"""quiver.exec.qplan — the Polars planner for the ISA v2 executor (docs/ISA2.md).

Emits tid-numbered instruction streams as Polars DataFrames: thread 0 is the
root (a mkdir preamble, then `spawn 1..N` / `join 1..N`); threads 1..N are the
per-file / per-member workers. `encode_stream` serialises a stream to the compact
binary the C `qvm` reads (fixed 88-byte records + a var-length heap).

This slice covers the CODEC=NONE modes the engine already runs:
  * plan_cp            — fs -> fs tree copy (copy_file_range), mkdir + setmeta
  * plan_pack_unc      — fs -> uncompressed tar (inline PAX header + copy_file_range
                         body at planner-computed offsets); zero-CPU pack

inflate/deflate + the sink lock (compressed pack/unpack/recompress) come next.
"""
from __future__ import annotations

import io
import os
import struct
import subprocess
import tempfile
import time

import numpy as np
import polars as pl

from .. import ipc
from ..nock.format import TarFormat, plan_layout
from ..nock import nockidx as _zf     # the nock footer-index layer (no old-engine dep)

# opcodes — mirror qvm.c
(OP_ALLOC, OP_FREE, OP_MOV, OP_MKDIR, OP_SETMETA, OP_SPAWN, OP_JOIN,
 OP_INFLATE, OP_DEFLATE, OP_CALL, OP_UNLINK, OP_RMDIR, OP_FBARRIER,
 OP_SRC_OPEN, OP_SRC_NEXT, OP_SRC_CLOSE, OP_TARSCAN, OP_SRC_SCAN,
 OP_SCANDIR, OP_CKSUM, OP_SINK_OPEN, OP_SINK_CLOSE) = range(1, 23)
# endpoint kinds
E_NONE, E_FS, E_BUF, E_INLINE, E_ARCH = range(5)
_BIG = 1 << 24                                # _sub for the frame's tail (deflate/free)

# the instruction word (one row = one instruction). Codec cols (sink/level/
# frame_id) are appended so the C buffer-index map for the first 15 is stable.
INSTR_COLS = {
    "tid": pl.Int64, "op": pl.UInt8, "src": pl.UInt8, "dst": pl.UInt8,
    "buf_id": pl.Int32, "buf_off": pl.Int64, "len": pl.Int64,
    "cap": pl.Int64, "lo": pl.Int64, "arch_off": pl.Int64,
    "path": pl.String, "dpath": pl.String, "payload": pl.Binary,
    "mode": pl.Int32, "mtime_ns": pl.Int64,
    "sink": pl.Int32, "level": pl.Int32, "frame_id": pl.Int64,
}
_DEFAULTS = {
    "src": 0, "dst": 0, "buf_id": -1, "buf_off": 0, "len": 0, "cap": 0,
    "lo": 0, "arch_off": 0, "path": "", "dpath": "", "payload": b"",
    "mode": -1, "mtime_ns": -1, "sink": 0, "level": 0, "frame_id": 0,
}


def _finalize(parts: list[pl.DataFrame]) -> pl.DataFrame:
    """Concat instruction fragments (diagonal — each sets only its columns),
    fill defaults, order by (tid, _sub), and cast to the instruction schema. The
    sort → fill → cast is one lazy pipeline, collected once."""
    cat = pl.concat(parts, how="diagonal_relaxed")
    have = set(cat.columns)
    df = (cat.lazy().sort(["tid", "_sub"])
            .with_columns(
              *[pl.col(c).fill_null(v) for c, v in _DEFAULTS.items() if c in have],
              *[pl.lit(v).alias(c) for c, v in _DEFAULTS.items() if c not in have])
            .select(*[pl.col(c).cast(t) for c, t in INSTR_COLS.items()])
            .collect())
    # rechunk so the stream serialises as ONE Arrow record batch (the C reader
    # takes the first batch); column order = INSTR_COLS drives the buffer map.
    return df.rechunk()


def _ancestors(rel: str):
    parts = rel.split("/")
    for k in range(1, len(parts)):
        yield "/".join(parts[:k])


def _dir_phases(dirs_depth, base_tid):
    """One MKDIR thread per directory (tids base_tid..), so mkdirs run in
    PARALLEL. Ordering is by depth epochs: thread 0 spawns each depth's tid
    range and joins before the next, so a parent always exists before its
    children. Returns (mkdir_df, root_phase_rows, next_tid, next_sub)."""
    dd = sorted(dirs_depth, key=lambda x: x[1])          # by depth
    m = len(dd)
    mkdir_rows = [{"tid": base_tid + i, "_sub": 0, "op": OP_MKDIR,
                   "path": p, "mode": 0o755} for i, (p, _) in enumerate(dd)]
    root_rows, sub, i = [], 0, 0
    while i < m:                                          # a spawn/join per depth
        depth, lo = dd[i][1], base_tid + i
        while i < m and dd[i][1] == depth:
            i += 1
        hi = base_tid + i - 1
        root_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN, "lo": lo, "cap": hi},
                      {"tid": 0, "_sub": sub + 1, "op": OP_JOIN, "lo": lo, "cap": hi}]
        sub += 2
    mkdir_df = (pl.DataFrame(mkdir_rows) if mkdir_rows
                else pl.DataFrame({"tid": [], "_sub": []}))
    return mkdir_df, root_rows, base_tid + m, sub


# --------------------------------------------------------------------- cp
def plan_cp(scan: pl.DataFrame, src_root: str, dst_root: str) -> pl.DataFrame:
    """fs -> fs: mkdir the directory set (depth order) in thread 0, spawn a
    thread per file that copy_file_ranges src->dst and sets its metadata, then a
    FINISH phase that restores each directory's mode+mtime. The dir mtimes must
    be re-set last: creating files/subdirs bumps a parent's mtime, so mkdir-time
    metadata would be clobbered. In batch mode all creation precedes any setmeta,
    so the finish order is irrelevant (setmeta touches only that dir's inode) —
    no deepest-first walk needed."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    n = files.height

    # real dirs from the scan (carry mode/mtime, include EMPTY dirs), unioned with
    # file ancestors so every parent exists even if the scan omitted it.
    dmeta = scan.filter(pl.col("is_dir")).sort("path")
    known = set(dmeta["path"])
    extra, seen = [], set(known)                 # ancestors not present as dir rows
    for p in files["path"]:
        for a in _ancestors(p):
            if a not in seen:
                seen.add(a); extra.append(a)
    # dst_root (depth 0), then every dir at its path depth; mkdirs run in parallel
    dd = ([(dst_root, 0)]
          + [(os.path.join(dst_root, d), d.count("/") + 1) for d in dmeta["path"]]
          + [(os.path.join(dst_root, d), d.count("/") + 1) for d in extra])
    mkdir_df, phase_rows, fbase, sub = _dir_phases(dd, 1)
    phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                    "lo": fbase, "cap": fbase + n - 1},
                   {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                    "lo": fbase, "cap": fbase + n - 1}]
    sub += 2

    f = files.with_row_index("k")
    tid = pl.col("k") + fbase                    # a thread per file, after mkdirs
    srcp = pl.lit(src_root.rstrip("/") + "/") + pl.col("path")
    dstp = pl.lit(dst_root.rstrip("/") + "/") + pl.col("path")
    mov = f.select(tid=tid, _sub=pl.lit(0), op=pl.lit(OP_MOV),
                   src=pl.lit(E_FS), dst=pl.lit(E_FS),
                   path=srcp, dpath=dstp, len=pl.col("size"),
                   mode=pl.col("mode") & 0o7777)
    meta = f.select(tid=tid, _sub=pl.lit(1), op=pl.lit(OP_SETMETA),
                    path=dstp, mode=pl.col("mode") & 0o7777,
                    mtime_ns=pl.col("mtime_ns"))

    # finish: restore dir mode+mtime, one thread per dir, after every file lands
    dbase = fbase + n
    nd = dmeta.height
    dir_meta = pl.DataFrame({"tid": [], "_sub": []})
    if nd:
        dm = dmeta.with_row_index("k")
        dir_meta = dm.select(
            tid=pl.col("k") + dbase, _sub=pl.lit(0), op=pl.lit(OP_SETMETA),
            path=pl.lit(dst_root.rstrip("/") + "/") + pl.col("path"),
            mode=pl.col("mode") & 0o7777, mtime_ns=pl.col("mtime_ns"))
        phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                        "lo": dbase, "cap": dbase + nd - 1},
                       {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                        "lo": dbase, "cap": dbase + nd - 1}]
    root_df = pl.DataFrame(phase_rows)
    return _finalize([root_df, mkdir_df, mov, meta, dir_meta])


# ------------------------------------------------------------------------ scan
def scan_stream(root: str, qvm_exe: str, on_chunk, threads: int = 8,
                chunk_rows: int = 1 << 16, transport: list | None = None) -> None:
    """STREAMING parallel scan (OP_SCANDIR): the VM walks the tree with a worker
    pool and PUSHES STAT rows in ~`chunk_rows` batches AS they are discovered, then
    a done marker. `on_chunk(df)` is called for each batch — so a caller can shard
    or pack files while the walk is still running (walk ‖ downstream work), one
    pass, without ever holding the whole member list. Columns: relative path,
    is_dir, size, mode, mtime_ns, uid, gid (root excluded; dirs + files incl. empty)."""
    root2 = root if transport else os.path.abspath(root)
    instr = _finalize([pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SCANDIR, "path": root2, "level": threads,
         "len": chunk_rows}])])

    def driver(vm):
        vm.push(instr)
        while True:
            m = vm._msg()
            if m is None:
                break
            kind, payload = m
            if kind == 2:                            # end-of-scan marker
                break
            df = ipc.read_all(payload)
            if df.height:
                on_chunk(df.with_columns(pl.col("is_dir").cast(pl.Boolean)))

    push_exec(driver, qvm_exe, "-", transport=transport)


def scan(root: str, qvm_exe: str, threads: int = 8,
         transport: list | None = None) -> pl.DataFrame:
    """Non-streaming convenience: collect the streaming scan into one DataFrame."""
    parts: list[pl.DataFrame] = []
    scan_stream(root, qvm_exe, parts.append, threads=threads, transport=transport)
    if not parts:
        return pl.DataFrame(schema={"path": pl.Utf8, "is_dir": pl.Boolean,
                                    "size": pl.Int64, "mode": pl.Int32,
                                    "mtime_ns": pl.Int64, "uid": pl.Int32,
                                    "gid": pl.Int32})
    return pl.concat(parts)


# ------------------------------------------------------------- teardown / durability
def plan_rm(files: list[str], dirs: list[str], root: str) -> pl.DataFrame:
    """Remove a set of entries under `root`: unlink files (one thread each, all
    parallel), then rmdir dirs DEEPEST-FIRST (a phase per depth, descending) so a
    directory is empty before it is removed. This is the teardown tail of a
    mirror/sync (ISA2 §5.1) — the dst entries not present in src."""
    root = root.rstrip("/")
    parts, phase_rows, sub, base = [], [], 0, 1
    nf = len(files)
    if nf:                                        # files: parallel unlink
        parts.append(pl.DataFrame({
            "tid": [base + i for i in range(nf)], "_sub": [0] * nf,
            "op": [OP_UNLINK] * nf, "path": [f"{root}/{p}" for p in files]}))
        phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN, "lo": base,
                        "cap": base + nf - 1},
                       {"tid": 0, "_sub": sub + 1, "op": OP_JOIN, "lo": base,
                        "cap": base + nf - 1}]
        sub += 2; base += nf
    if dirs:                                      # dirs: rmdir deepest-first
        byd: dict[int, list[str]] = {}
        for d in dirs:
            byd.setdefault(d.count("/"), []).append(d)
        rows = []
        for depth in sorted(byd, reverse=True):
            lo = base
            for d in byd[depth]:
                rows.append({"tid": base, "_sub": 0, "op": OP_RMDIR,
                             "path": f"{root}/{d}"}); base += 1
            phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN, "lo": lo,
                            "cap": base - 1},
                           {"tid": 0, "_sub": sub + 1, "op": OP_JOIN, "lo": lo,
                            "cap": base - 1}]
            sub += 2
        parts.append(pl.DataFrame(rows))
    root_df = pl.DataFrame(phase_rows) if phase_rows else \
        pl.DataFrame({"tid": [], "_sub": []})
    return _finalize([root_df] + parts)


def plan_fbarrier(paths: list[str]) -> pl.DataFrame:
    """Emit an fsync durability barrier for each path (thread 0, in order). An
    empty path "" fsyncs the output archive fd instead of a named file."""
    rows = [{"tid": 0, "_sub": i, "op": OP_FBARRIER, "path": p}
            for i, p in enumerate(paths)]
    return _finalize([pl.DataFrame(rows)]) if rows else _empty_batch()


# -------------------------------------------------------------- pack (uncompressed)
def plan_pack_unc(scan: pl.DataFrame, root: str
                  ) -> tuple[pl.DataFrame, pl.DataFrame, int]:
    """fs -> uncompressed tar: lay each member as [512-aligned PAX header |
    padded body] via inline->arch header + copy_file_range fs->arch body. Header
    bytes and offsets come from the vectorized layout planner (nock.format —
    ustar/PAX as Polars exprs, offsets a cum_sum): NO per-member Python loop.
    Returns (instr_df, footer_df, archive_size)."""
    plan = plan_layout(scan.lazy(), TarFormat(), sort=True).with_row_index("_k")
    n = plan.height
    total = int(plan["offset"][-1] + plan["block_len"][-1]) if n else 0
    tid = pl.col("_k") + 1

    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": n},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": n}])
    hdr = plan.select(                                 # inline PAX header -> arch
        tid=tid, _sub=pl.lit(0), op=pl.lit(OP_MOV),
        src=pl.lit(E_INLINE), dst=pl.lit(E_ARCH),
        arch_off=pl.col("offset"), payload=pl.col("header"))
    body = plan.select(                                # copy_file_range body -> arch
        tid=tid, _sub=pl.lit(1), op=pl.lit(OP_MOV),
        src=pl.lit(E_FS), dst=pl.lit(E_ARCH),
        arch_off=pl.col("data_offset"),
        path=pl.lit(root.rstrip("/") + "/") + pl.col("path"), len=pl.col("size"))
    instr = _finalize([root_df, hdr, body])

    footer = plan.select("path", "size", "mode", "mtime_ns", "data_offset",
                         read_size=pl.col("size"))
    return instr, footer, total


# directories in the nock footer: frame=-1 sentinel rows (no data, no frame).
# unpack reads them to mkdir + restore dir mode/mtime; old footers simply have
# none and fall back to ancestor-derived mkdirs without metadata.
def _dir_footer_rows(scan: pl.DataFrame) -> pl.DataFrame:
    d = scan.filter(pl.col("is_dir"))
    return d.select(
        path=pl.col("path"), size=pl.lit(0, pl.Int64),
        mode=pl.col("mode").cast(pl.Int32), mtime_ns=pl.col("mtime_ns").cast(pl.Int64),
        uid=pl.col("uid").cast(pl.Int32), gid=pl.col("gid").cast(pl.Int32),
        frame=pl.lit(-1, pl.Int32), frame_coff=pl.lit(-1, pl.Int64),
        frame_clen=pl.lit(-1, pl.Int64), in_off=pl.lit(-1, pl.Int64))


# -------------------------------------------------------------- pack (compressed)
def plan_pack(scan: pl.DataFrame, root: str, frame_bytes: int = 1 << 20,
              level: int = 6, npool: int = 16, predicate: pl.Expr | None = None,
              shard_by: pl.Expr | None = None, shards: int = 1,
              frame_base: int = 0
              ) -> tuple[pl.DataFrame, pl.DataFrame, int]:
    """fs -> compressed nock. Each frame is a thread that assembles [header|body]
    per member into a zeroed buffer and deflates it to its sink. `predicate`
    keeps only matching members (filter is just planning fewer). `shard_by`
    (an expr -> sink id, 0..shards-1) routes members to `shards` output sinks,
    each framed independently. Returns (instr_df, members_df, nsinks); members_df
    carries `sink` and joins the {frame,coff,clen} completions."""
    lf = scan.lazy().filter(~pl.col("is_dir")).sort("path")
    if predicate is not None:
        lf = lf.filter(predicate)
    df = (TarFormat().with_header_cols(lf)
          .with_columns(payload_len=((pl.col("size") + 511) // 512) * 512)
          .with_columns(block_len=pl.col("header_len") + pl.col("payload_len"))
          .collect())
    df = df.with_columns(sink=(shard_by.cast(pl.Int32) if shard_by is not None
                               else pl.lit(0, pl.Int32)))
    # frames pack by cumulative footprint WITHIN each sink; global dense frame id
    df = df.with_columns(
        _eo=pl.col("block_len").cum_sum().over("sink") - pl.col("block_len"))
    df = df.with_columns(_lf=(pl.col("_eo") // frame_bytes).cast(pl.Int64))
    fr = (df.select("sink", "_lf").unique().sort("sink", "_lf")
            .with_row_index("frame"))
    df = df.join(fr, on=["sink", "_lf"])
    df = df.with_columns(_fs=pl.col("_eo").min().over("frame"))
    df = df.with_columns(
        local=pl.col("_eo") - pl.col("_fs"),
        in_off=pl.col("_eo") - pl.col("_fs") + pl.col("header_len"),
        mrank=pl.int_range(pl.len()).over("frame"))
    frames = (df.group_by("frame")
                .agg(clen=pl.col("block_len").sum(), sink=pl.col("sink").first())
                .sort("frame"))
    nf = frames.height
    last = frames.group_by("sink").agg(_last=pl.col("frame").max())   # EOF/sink
    frames = frames.join(last, on="sink").with_columns(
        dl=pl.col("clen") + pl.when(pl.col("frame") == pl.col("_last"))
                              .then(1024).otherwise(0),
        bufid=(pl.col("frame") % npool).cast(pl.Int32))
    df = df.join(frames.select("frame", "bufid"), on="frame") \
           .with_columns(tid=pl.col("frame") + 1)

    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}])
    ftid = pl.col("frame") + 1
    alloc = frames.select(tid=ftid, _sub=pl.lit(0), op=pl.lit(OP_ALLOC),
                          buf_id=pl.col("bufid"), cap=pl.col("dl"),
                          mode=pl.lit(1))          # zero: tar body padding must be NUL
    hdr = df.select(tid=pl.col("tid"), _sub=1 + 2 * pl.col("mrank"),
                    op=pl.lit(OP_MOV), src=pl.lit(E_INLINE), dst=pl.lit(E_BUF),
                    buf_id=pl.col("bufid"), buf_off=pl.col("local"),
                    payload=pl.col("header"))
    body = df.select(tid=pl.col("tid"), _sub=2 + 2 * pl.col("mrank"),
                     op=pl.lit(OP_MOV), src=pl.lit(E_FS), dst=pl.lit(E_BUF),
                     buf_id=pl.col("bufid"), buf_off=pl.col("in_off"),
                     path=pl.lit(root.rstrip("/") + "/") + pl.col("path"),
                     len=pl.col("size"))
    deflate = frames.select(tid=ftid, _sub=pl.lit(_BIG), op=pl.lit(OP_DEFLATE),
                            buf_id=pl.col("bufid"), buf_off=pl.lit(0),
                            len=pl.col("dl"), sink=pl.col("sink"),
                            level=pl.lit(level), mode=pl.lit(1),   # digest on
                            frame_id=pl.col("frame") + frame_base)  # global id
    free = frames.select(tid=ftid, _sub=pl.lit(_BIG + 1), op=pl.lit(OP_FREE),
                         buf_id=pl.col("bufid"))
    instr = _finalize([root_df, alloc, hdr, body, deflate, free])
    members = df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                        frame=pl.col("frame") + frame_base, in_off="in_off",
                        sink="sink")
    return instr, members, (shards if shard_by is not None else 1)


def pack_stream(scan: pl.DataFrame, root: str, out_path: str, qvm_exe: str,
                chunk_rows: int = 64, frame_bytes: int = 1 << 20, level: int = 6,
                npool: int = 16, nworkers: int = 8,
                transport: list | None = None) -> int:
    """Streaming pack: scan is split into chunks, each planned into its own batch
    (frame ids offset by a running base) and PUSHED to the one persistent qvm —
    chunk k+1 is planned + pushed while qvm packs chunk k (push is async), so
    discovery/planning overlaps execution. Completions accumulate; one footer at
    the end. Returns the member count."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    chunks = [files.slice(i, chunk_rows)
              for i in range(0, files.height, chunk_rows)]
    collected: list[pl.DataFrame] = []

    def driver(vm):                               # push each chunk; no feedback needed
        base = 0
        for ch in chunks:
            instr, members, _ = plan_pack(ch, root, frame_bytes, level, npool,
                                          frame_base=base)
            collected.append(members)
            base += members["frame"].n_unique() if members.height else 0
            vm.push(instr)

    open(out_path, "wb").close()
    comp = push_exec(driver, qvm_exe, "-", sinks=(out_path,), npool=npool,
                     nworkers=nworkers, want_comp=True, transport=transport)
    return _stream_footer(out_path, collected, comp)   # stream per-chunk parts


def pack_pipe(scan: pl.DataFrame, root: str, out_path: str, qvm_exe: str,
              frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
              nworkers: int = 8) -> int:
    """Compressed pack to a PIPE sink — the S3/TCP shape. Frames stream out a
    pipe (deflate holds the sink lock through each write, since a pipe has no
    pwrite); a reader thread forwards the bytes (here into `out_path`). The
    footer (from the completions) is written after, as it would be a sidecar /
    trailing object part. Returns the member count."""
    import threading
    instr, members, _ = plan_pack(scan, root, frame_bytes, level, npool)
    r, w = os.pipe()
    chunks: list[bytes] = []

    def reader():
        while True:
            c = os.read(r, 1 << 20)
            if not c:
                break
            chunks.append(c)                     # a real sink would upload here

    th = threading.Thread(target=reader); th.start()
    comp = run(instr, qvm_exe, "-", sinks=(f"fd:{w}",), npool=npool,
               nworkers=nworkers, want_comp=True)
    os.close(w)                                  # parent's copy → reader hits EOF
    th.join(); os.close(r)

    with open(out_path, "wb") as f:
        f.write(b"".join(chunks))                # streamed frame bytes, in order
    return _stream_footer(out_path, [members], comp)


def tar_scan(tar_path: str, qvm_exe: str | None = None) -> pl.DataFrame:
    """Decode a (uncompressed) tar into a member-row stream — path, size, mode,
    mtime_ns, uid, gid, plus the member's location (offset, header_len, range =
    header + padded body). With `qvm_exe`, the parse runs in C (`qvm tarscan`,
    ustar/PAX/GNU, no per-member Python) — far faster for many-member tars, so
    recompress can scan in C and gather from qvm's own buffer instead of shipping
    decoded windows through the pipe. Without it, the tarfile fallback."""
    if qvm_exe is not None:                        # OP_TARSCAN file mode (mmap + parse)
        instr = _finalize([pl.DataFrame([
            {"tid": 0, "_sub": 0, "op": OP_TARSCAN, "buf_id": -1,
             "path": os.path.abspath(tar_path)}])])
        return _push_scan(instr, qvm_exe)
    import tarfile
    rows = []
    with tarfile.open(tar_path, "r:") as tf:
        for m in tf.getmembers():
            if not m.isfile():
                continue
            hl = m.offset_data - m.offset
            rows.append({"path": m.name, "size": m.size,
                         "mode": m.mode, "mtime_ns": int(m.mtime) * 1_000_000_000,
                         "uid": m.uid, "gid": m.gid, "offset": m.offset,
                         "header_len": hl,
                         "range": hl + ((m.size + 511) // 512) * 512})
    return pl.DataFrame(rows)


# The multi-run GATHER core, shared by every tar→nock path (recompress, windowed,
# streaming). Members carry a buffer-relative offset `boff`; they are grouped into
# frames by cumulative footprint, each member gets its in-frame offset, and each
# frame's contiguous (off,len) runs are packed for the in-place deflate. The three
# planners differ ONLY in how the buffer is filled (whole tar / window load /
# inline) — the gather itself is identical.
def _gather_frames(df: pl.DataFrame, frame_bytes: int, buf_id: int = 0):
    """Returns (df + frame/in_off, per-frame clen table, {frame: packed runs}).
    Runs are (buf_id, off, len) i64 triples so a frame can gather across buffers.
    If `df` has a `buf` column (COALESCED windows — several member-aligned windows
    in DIFFERENT ring buffers planned as one gather), each run carries its own
    buffer id and a buffer change breaks the run; the rows are already in gather
    order (window order, tar order within), so no re-sort. Otherwise all runs use
    the single `buf_id` and rows are sorted by offset."""
    multi = "buf" in df.columns
    if not multi:
        df = df.sort("boff")                    # single-buffer gather order (one op)
    # the gather MATH runs in a fixed numpy kernel (no per-batch query plan); polars
    # only carries the string columns (path etc.) and, upstream, the predicate.
    frame, in_off, uf, clen, payloads = _np_gather(
        df["boff"].to_numpy(), df["range"].to_numpy(), df["header_len"].to_numpy(),
        frame_bytes, buf_id, df["buf"].to_numpy() if multi else None)
    df = df.with_columns(frame=pl.Series("frame", frame, pl.Int64),
                         in_off=pl.Series("in_off", in_off, pl.Int64))
    frames = pl.DataFrame({"frame": pl.Series(uf, dtype=pl.Int64),
                           "clen": pl.Series(clen, dtype=pl.Int64)})
    return df, frames, payloads


def _np_gather(boff, rng, hl, frame_bytes, buf_id, buf=None):
    """The gather kernel: from member (offset, footprint, header_len[, buffer])
    arrays IN GATHER ORDER, produce each member's frame + in-frame offset, each
    frame's compressed length, and the {frame: (buf,off,len) i64 triples} runs.
    Pure numpy — cumsum / floor-div / reduceat / boundary masks; no query plan."""
    n = len(boff)
    if n == 0:
        z = np.zeros(0, np.int64)
        return z, z, z, z, {}
    boff = np.asarray(boff, np.int64); rng = np.asarray(rng, np.int64)
    hl = np.asarray(hl, np.int64)
    c = np.cumsum(rng) - rng                            # footprint start (non-decreasing)
    frame = c // frame_bytes
    first = np.empty(n, bool); first[0] = True; first[1:] = frame[1:] != frame[:-1]
    fstart = np.flatnonzero(first)                      # first member index per frame
    fs = np.repeat(c[fstart], np.diff(np.append(fstart, n)))   # frame's min c, broadcast
    in_off = c - fs + hl
    uf = frame[fstart]                                  # unique frames (sorted)
    clen = np.add.reduceat(rng, fstart)                 # per-frame Σ footprint
    new = np.empty(n, bool); new[0] = True              # run boundary: offset jump,
    new[1:] = (boff[1:] != boff[:-1] + rng[:-1]) | (frame[1:] != frame[:-1])  # frame,
    if buf is not None:                                 # or (coalesced) buffer change
        buf = np.asarray(buf, np.int64); new[1:] |= buf[1:] != buf[:-1]
    rs = np.flatnonzero(new)                            # run starts
    r_frame = frame[rs]; r_off = boff[rs]
    r_len = np.add.reduceat(rng, rs)                    # per-run Σ footprint
    r_buf = buf[rs] if buf is not None else None
    frc = np.empty(len(rs), bool); frc[0] = True; frc[1:] = r_frame[1:] != r_frame[:-1]
    grp = np.flatnonzero(frc); gend = np.append(grp[1:], len(rs))
    payloads = {}
    for a, b in zip(grp, gend):                         # per frame, pack its runs
        t = np.empty(3 * (b - a), dtype="<i8")
        t[0::3] = r_buf[a:b] if r_buf is not None else buf_id
        t[1::3] = r_off[a:b]; t[2::3] = r_len[a:b]
        payloads[int(r_frame[a])] = t.tobytes()
    return frame, in_off, uf, clen, payloads


def _gather_deflate(frames: pl.DataFrame, payloads: dict, buf_id: int,
                    level: int, frame_base: int) -> pl.DataFrame:
    """Per-frame multi-run deflate rows (digest on) — one thread per frame."""
    return frames.with_columns(
        payload=pl.col("frame").replace_strict(payloads, return_dtype=pl.Binary)).select(
        tid=pl.col("frame") + 1, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
        buf_id=pl.lit(buf_id), buf_off=pl.lit(0), len=pl.col("clen"),
        sink=pl.lit(0), level=pl.lit(level), mode=pl.lit(1),   # digest on
        frame_id=pl.col("frame") + frame_base, payload=pl.col("payload"))


def _gather_members(df: pl.DataFrame, frame_base: int) -> pl.DataFrame:
    return df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                     frame=pl.col("frame") + frame_base, in_off="in_off")


def plan_recompress(members: pl.DataFrame, tar_path: str,
                    frame_bytes: int = 1 << 20, level: int = 6,
                    predicate: pl.Expr | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(uncompressed) tar -> compressed nock, from a tar_scan member stream.
    Thread 0 loads the whole tar into a shared window (buf 0), spawns a child per
    output frame that multi-run `deflate`s its members' byte RANGES straight from
    the window (a dropped member just yields non-contiguous runs), and frees it
    after the join. Returns (instr_df, members_df)."""
    tarsize = os.path.getsize(tar_path)
    df = members if predicate is None else members.filter(predicate)
    df = df.with_columns(boff=pl.col("offset"))            # buffer = the whole tar
    df, frames, payloads = _gather_frames(df, frame_bytes, 0)
    nf = frames.height
    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_ALLOC, "buf_id": 0, "cap": tarsize},
        {"tid": 0, "_sub": 1, "op": OP_MOV, "src": E_FS, "dst": E_BUF,
         "buf_id": 0, "buf_off": 0, "path": tar_path, "len": tarsize},
        {"tid": 0, "_sub": 2, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 3, "op": OP_JOIN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": _BIG, "op": OP_FREE, "buf_id": 0}])
    instr = _finalize([root_df, _gather_deflate(frames, payloads, 0, level, 0)])
    return instr, _gather_members(df, 0)


def recompress(tar_path: str, out_path: str, qvm_exe: str,
               frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
               nworkers: int = 8, predicate: pl.Expr | None = None) -> int:
    """Plan + run a tar->nock recompress (multi-run gather), then write the
    footer from the completions. Returns the member count."""
    instr, members = plan_recompress(tar_scan(tar_path, qvm_exe), tar_path, frame_bytes,
                                     level, predicate)
    open(out_path, "wb").close()
    comp = run(instr, qvm_exe, "-", sinks=(out_path,), npool=max(npool, 1),
               nworkers=nworkers, want_comp=True)
    return _stream_footer(out_path, [members], comp)


def plan_window_gather(wdf: pl.DataFrame, win_start: int, buf_id: int,
                       frame_bytes: int, level: int, frame_base: int
                       ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The gather batch for a window (used by recompress_zst_push): thread 0 spawns
    a child per frame, each multi-run `deflate`s its members' WINDOW-RELATIVE ranges
    from the buffer `buf_id` (the driver holds it). No alloc/free here — the driver
    owns the buffer. Returns (instr_df, members_df)."""
    df = wdf.with_columns(boff=pl.col("offset") - win_start)   # relative to window
    df, frames, payloads = _gather_frames(df, frame_bytes, buf_id)
    nf = frames.height
    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}])
    instr = _finalize([root_df, _gather_deflate(frames, payloads, buf_id, level, frame_base)])
    return instr, _gather_members(df, frame_base)


def recompress_zst(src_path: str, out_path: str, qvm_exe: str,
                   frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
                   nworkers: int = 8, predicate: pl.Expr | None = None) -> int:
    """Recompress a COMPRESSED foreign source (.tar.zstd) into a nock. The
    multi-run gather needs random access to the decoded stream, so this is the
    decode-scan path: streaming-decompress the source to a temp .tar (bounded
    memory, single decode), discover members from it, then recompress via the
    multi-run gather. The fully-streaming one-window-at-a-time loop that never
    materializes (legacy zstream's ZMETA/PLAN feedback) is the remaining
    optimization for very large sources. Returns the member count."""
    import zstandard
    fd, tmp = tempfile.mkstemp(suffix=".tar"); os.close(fd)
    try:
        with open(src_path, "rb") as fi, open(tmp, "wb") as fo:
            zstandard.ZstdDecompressor().copy_stream(fi, fo,
                                                     read_size=1 << 20,
                                                     write_size=1 << 20)
        return recompress(tmp, out_path, qvm_exe, frame_bytes, level, npool,
                          nworkers, predicate)
    finally:
        os.unlink(tmp)


def recompress_zst_push(src_path: str, out_path: str, qvm_exe: str,
                       frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
                       nworkers: int = 8, predicate: pl.Expr | None = None) -> int:
    """PUSH-driven recompress of a single-frame .tar.zstd → nock, with NO Python
    decode, NO temp file, and NO shipped bytes. The VM streaming-decompresses the
    source straight into ONE pool buffer, scans the tar there (OP_TARSCAN), and
    PUSHES the member rows to us on the DATA channel; we plan the multi-run gather
    from those rows and push it back, so only member metadata crosses the pipe.
    Bounded-memory windowing is recompress_zst_window_push; for a multi-frame or
    size-unknown source, falls back to the decode-scan path. Member count."""
    import zstandard
    with open(src_path, "rb") as f:
        dsize = zstandard.get_frame_parameters(f.read(64)).content_size
    if not dsize or dsize == (1 << 64) - 1:        # size not stored → can't size buf
        return recompress_zst(src_path, out_path, qvm_exe, frame_bytes, level,
                              npool, nworkers, predicate)
    # entry: decode whole tar into buf 0, scan it (→ DATA rows). buf 0 stays
    # allocated across batches (the pool persists) so the pushed gather can read it.
    entry = _finalize([pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SRC_OPEN, "lo": 0, "path": src_path},
        {"tid": 0, "_sub": 1, "op": OP_ALLOC, "buf_id": 0, "cap": dsize},
        {"tid": 0, "_sub": 2, "op": OP_SRC_NEXT, "lo": 0, "buf_id": 0,
         "buf_off": 0, "len": dsize},
        {"tid": 0, "_sub": 3, "op": OP_SRC_CLOSE, "lo": 0},
        {"tid": 0, "_sub": 4, "op": OP_TARSCAN, "buf_id": 0, "buf_off": 0, "len": 0}])])
    st = {"members": pl.DataFrame(schema={"frame": pl.Int64})}

    def driver(vm):
        vm.push(entry)                             # decode + scan → pushes rows
        wdf = vm.read_rows()                        # OP_TARSCAN's member rows
        if predicate is not None and wdf.height:
            wdf = wdf.filter(predicate)
        if wdf.height == 0:
            return
        instr, members = plan_window_gather(wdf, 0, 0, frame_bytes, level, 0)
        st["members"] = members
        vm.push(instr)                             # gather reads the still-live buf 0

    open(out_path, "wb").close()
    comp = push_exec(driver, qvm_exe, "-", sinks=(out_path,), npool=max(npool, 1),
                     nworkers=nworkers, want_comp=True)
    return _stream_footer(out_path, [st["members"]], comp)


def recompress_zst_window_push(src_path: str, out_path: str, qvm_exe: str,
                              window_bytes: int = 8 << 20, frame_bytes: int = 1 << 20,
                              level: int = 6, npool: int = 4, nworkers: int = 8,
                              predicate: pl.Expr | None = None, depth: int = 6,
                              coalesce: int = 4) -> int:
    """BOUNDED-memory push recompress: the VM decodes ONE member-aligned window at
    a time (OP_SRC_SCAN — cut at the last complete member, partial tail carried in
    the source) and pushes that window's rows + a done flag on the DATA channel.

    COALESCED + PIPELINED over a ring of buffers. `coalesce` windows are decoded
    into distinct ring slots and their rows planned as ONE gather (the multi-run
    triples carry a per-run buffer id, so a frame spans slots) — so Python compiles
    the gather query once per `coalesce` windows instead of once per window,
    amortizing the (fixed) per-batch polars cost. Scanning still runs ahead of the
    gather, so decode ‖ compress ‖ plan overlap. Peak memory ≈ ring × window_bytes;
    buffer-slot backpressure holds it there. `window_bytes` must exceed the largest
    single member. Works on multi-frame sources (no size hint). Returns member count."""
    C = max(1, coalesce)                             # windows planned per gather
    R = max(depth, C + 2)                            # ring: a group + scan-ahead
    cap = window_bytes
    members: list[pl.DataFrame] = []
    frame_base = [0]

    scan_cache: dict[int, bytes] = {}                # slot → encoded frame (constant)

    def scan_batch(slot, first=False):               # ALLOC buf[slot] + SRC_SCAN
        rows = ([{"tid": 0, "_sub": 0, "op": OP_SRC_OPEN, "lo": 0, "path": src_path}]
                if first else [])
        b = 1 if first else 0
        rows += [{"tid": 0, "_sub": b, "op": OP_ALLOC, "buf_id": slot, "cap": cap},
                 {"tid": 0, "_sub": b + 1, "op": OP_SRC_SCAN, "lo": 0, "buf_id": slot,
                  "len": cap}]
        return _finalize([pl.DataFrame(rows)])

    def push_scan(vm, slot, first=False):            # a scan is constant per slot →
        if first:                                     # build+encode once, then reuse
            vm.push(scan_batch(slot, first=True)); return
        if slot not in scan_cache:
            scan_cache[slot] = vm.frame(scan_batch(slot))
        vm.push_raw(scan_cache[slot])

    def gather_group(group, last):                   # ONE gather over `group` windows
        # group: [(slot, rows)]; tag each window's rows with its slot and coalesce.
        rows = pl.concat([r.with_columns(buf=pl.lit(slot, pl.Int32),
                                         boff=pl.col("offset")) for slot, r in group]) \
            if group else pl.DataFrame()
        if predicate is not None and rows.height:
            rows = rows.filter(predicate)
        gd, nf = None, 0
        if rows.height:
            df, frames, payloads = _gather_frames(rows, frame_bytes)   # per-run bufid
            members.append(_gather_members(df, frame_base[0]))
            gd = _gather_deflate(frames, payloads, 0, level, frame_base[0])
            frame_base[0] += frames.height; nf = frames.height
        head = ([{"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
                 {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}] if nf else [])
        free = [{"tid": 0, "_sub": 2 + i, "op": OP_FREE, "buf_id": slot}
                for i, (slot, _) in enumerate(group)]     # free every window's slot
        if last:
            free.append({"tid": 0, "_sub": 2 + len(group), "op": OP_SRC_CLOSE, "lo": 0})
        parts = [pl.DataFrame(head + free)] + ([gd] if gd is not None else [])
        return _finalize(parts)

    def driver(vm):
        push_scan(vm, 0, first=True)                 # open + scan window 0
        rows = vm.read_rows(); done = vm.read_done()
        k, group = 0, []
        while True:
            group.append((k % R, rows))              # this window's slot + rows
            if not done:
                push_scan(vm, (k + 1) % R)           # scan the next window (overlap)
            if done or len(group) == C:              # group full → ONE coalesced gather
                vm.push(gather_group(group, last=done))
                group = []
            if done:
                break
            rows = vm.read_rows(); done = vm.read_done()
            k += 1

    open(out_path, "wb").close()
    comp = push_exec(driver, qvm_exe, "-", sinks=(out_path,), npool=max(npool, R + 1),
                     nworkers=nworkers, want_comp=True)
    if not members:
        members.append(pl.DataFrame(schema={"frame": pl.Int64}))
    return _stream_footer(out_path, members, comp)


def recompress_sharded(src_path: str, out_dir: str, qvm_exe: str, shard_key,
                       prefix: str = "shard", window_bytes: int = 8 << 20,
                       frame_bytes: int = 1 << 20, level: int = 6,
                       shard_bytes: int = 5 << 30, nworkers: int = 8,
                       source_fd: int | None = None) -> list[tuple]:
    """VM-NATIVE sharded recompress: the VM decodes the source (a tar / .tar.gz — a
    file path OR an inherited pipe fd, e.g. an HTTP stream Python pumps in),
    member-aligns it (OP_SRC_SCAN), and PUSHES member rows; Python routes each by
    `shard_key(path)` and pushes a gather that deflates that group's members STRAIGHT
    to the group's dynamic output sink (OP_SINK_OPEN/CLOSE). The member bytes never
    leave the VM — Python only sees metadata to route on. Each group rotates to a new
    shard past ~shard_bytes. Returns a manifest of (shard, group, index, members, bytes)."""
    os.makedirs(out_dir, exist_ok=True)
    cap = window_bytes
    src = f"fd:{source_fd}" if source_fd is not None else os.path.abspath(src_path)
    G: dict[str, dict] = {}                          # group → live shard state
    frame_base = [0]
    manifest: list[tuple] = []

    def gstate(gk):
        if gk not in G:
            G[gk] = {"slot": len(G), "idx": 0, "bytes": 0, "open": False,
                     "members": [], "frames": [], "comp": None}
        return G[gk]

    def shard_path(gk, i):
        return os.path.join(out_dir, f"{prefix}-{gk}-flac-{i:06d}.nock")

    def _deflate_rows(frames, payloads, sink, fb, tb):   # like _gather_deflate, tid/sink offset
        return frames.with_columns(
            payload=pl.col("frame").replace_strict(payloads, return_dtype=pl.Binary)).select(
            tid=pl.col("frame") + tb, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
            buf_id=pl.lit(0), buf_off=pl.lit(0), len=pl.col("clen"), sink=pl.lit(sink),
            level=pl.lit(level), mode=pl.lit(1),
            frame_id=pl.col("frame") + fb, payload=pl.col("payload"))

    def build(rows, done, scan_next):
        r = rows.with_columns(boff=pl.col("offset"),
                              _g=pl.col("path").map_elements(shard_key, return_dtype=pl.Utf8))
        opens, gds, subs, tid = [], [], [], 1
        closes, rotated = [], []                      # SINK_CLOSE + shards to footer
        for (gk,), part in r.partition_by("_g", as_dict=True).items():
            st = gstate(gk)
            if not st["open"]:                        # first members → open the shard file
                opens.append({"op": OP_SINK_OPEN, "sink": st["slot"],
                              "path": shard_path(gk, st["idx"])})
                st["open"] = True
            df, frames, pays = _gather_frames(part.drop("_g"), frame_bytes, 0)
            nf = frames.height
            gds.append(_deflate_rows(frames, pays, st["slot"], frame_base[0], tid))
            st["members"].append(_gather_members(df, frame_base[0]))
            st["frames"].extend(range(frame_base[0], frame_base[0] + nf))
            st["bytes"] += int(part["size"].sum())
            frame_base[0] += nf; tid += nf
            if st["bytes"] >= shard_bytes:            # shard full → close + rotate
                rotated.append((gk, st["slot"], st["idx"], pl.concat(st["members"]),
                                list(st["frames"])))
                closes.append(st["slot"])
                st.update(idx=st["idx"] + 1, bytes=0, open=False, members=[], frames=[])
        ntot = tid - 1
        # thread 0, in order: OPEN new shard sinks → spawn/join the deflates → free the
        # window buffer → CLOSE any filled shards → alloc + scan the next window.
        rowset, sub = [], 0
        for o in opens:
            rowset.append({"tid": 0, "_sub": sub, "op": OP_SINK_OPEN,
                           "sink": o["sink"], "path": o["path"]}); sub += 1
        if ntot:
            rowset += [{"tid": 0, "_sub": sub, "op": OP_SPAWN, "lo": 1, "cap": ntot},
                       {"tid": 0, "_sub": sub + 1, "op": OP_JOIN, "lo": 1, "cap": ntot}]
            sub += 2
        rowset.append({"tid": 0, "_sub": sub, "op": OP_FREE, "buf_id": 0}); sub += 1
        for slot in closes:
            rowset.append({"tid": 0, "_sub": sub, "op": OP_SINK_CLOSE, "sink": slot}); sub += 1
        if scan_next:
            rowset += [{"tid": 0, "_sub": sub, "op": OP_ALLOC, "buf_id": 0, "cap": cap},
                       {"tid": 0, "_sub": sub + 1, "op": OP_SRC_SCAN, "lo": 0,
                        "buf_id": 0, "len": cap}]
        else:
            rowset.append({"tid": 0, "_sub": sub, "op": OP_SRC_CLOSE, "lo": 0})
        return _finalize([pl.DataFrame(rowset), *gds]), rotated

    def driver(vm):
        vm.push(_finalize([pl.DataFrame([
            {"tid": 0, "_sub": 0, "op": OP_SRC_OPEN, "lo": 0, "path": src},
            {"tid": 0, "_sub": 1, "op": OP_ALLOC, "buf_id": 0, "cap": cap},
            {"tid": 0, "_sub": 2, "op": OP_SRC_SCAN, "lo": 0, "buf_id": 0, "len": cap}])]))
        rows = vm.read_rows(); done = vm.read_done()
        while True:
            batch, rotated = build(rows, done, scan_next=not done)
            for gk, slot, idx, mem, frames in rotated:
                manifest.append([shard_path(gk, idx), gk, idx, mem, frames])
            vm.push(batch)
            if done:
                break
            rows = vm.read_rows(); done = vm.read_done()
        for gk, st in G.items():                     # final open shards
            if st["members"]:
                manifest.append([shard_path(gk, st["idx"]), gk, st["idx"],
                                 pl.concat(st["members"]), list(st["frames"])])

    comp = push_exec(driver, qvm_exe, "-", npool=max(16, len(G) + 2),
                     nworkers=nworkers, want_comp=True,
                     extra_fds=(source_fd,) if source_fd is not None else ())
    out = []
    for shard, gk, idx, mem, frames in manifest:     # per-shard footer from its frames
        c = comp.filter(pl.col("frame").is_in(frames)) if comp is not None else None
        n = _stream_footer(shard, [mem], c)
        out.append((shard, gk, idx, n, int(mem["size"].sum())))
    return out


def _stream_footer(out_path: str, parts, comp: pl.DataFrame | None,
                   dirs: pl.DataFrame | None = None, force_sidecar: bool = False,
                   chunk_rows: int = 1 << 20) -> int:
    """Write the nock footer by joining + serializing it in COLUMNAR CHUNKS
    (one Arrow record batch per `chunk_rows`) instead of one big
    members⋈completions join + full serialization. This avoids a second
    whole-index copy (the join) and streams the write, so peak footer overhead is
    ~chunk_rows rows rather than the entire (100M-row) index materialized twice.
    Stays in Arrow/Polars throughout — no per-row Python objects. `parts` is an
    iterable of member frames (path,size,mode,mtime_ns,uid,gid,frame,in_off);
    `comp` carries the frame completions; `dirs` are frame=-1 directory rows.
    Returns the file count (dirs excluded)."""
    _pl = {"large_string": pl.Utf8, "i64": pl.Int64, "i32": pl.Int32}
    fs = {c: _pl[t] for c, t in _zf._FOOTER_IPC}
    fcols = [c for c, _ in _zf._FOOTER_IPC]
    # carry a per-frame content digest (xxh64) when the completions have one — an
    # extra footer column for integrity; readers without it just ignore it.
    has_dg = comp is not None and comp.height and "digest" in comp.columns
    if has_dg:
        fcols = fcols + ["frame_digest"]; fs["frame_digest"] = pl.Int64
    if comp is not None and comp.height:
        sel = dict(frame=pl.col("frame").cast(pl.Int64),
                   frame_coff="frame_coff", frame_clen="frame_clen")
        if has_dg:
            sel["frame_digest"] = pl.col("digest")
        comp = comp.select(**sel)
    ftmp = tempfile.TemporaryFile()
    total = 0
    for part in parts:
        if part is None or part.height == 0:
            continue
        for off in range(0, part.height, chunk_rows):
            sl = part.slice(off, chunk_rows).with_columns(
                pl.col("frame").cast(pl.Int64))
            j = (sl.join(comp, on="frame", how="left") if comp is not None
                 else sl.with_columns(frame_coff=pl.lit(-1, pl.Int64),
                                      frame_clen=pl.lit(-1, pl.Int64)))
            j = j.select([pl.col(c).cast(fs[c]) for c in fcols])
            ipc.write_batch(ftmp, j)          # columnar batch, streamed
            total += j.height
    if dirs is not None and dirs.height:          # directory rows: frame=-1, no data
        cols = dict(
            path="path", size=pl.lit(0, pl.Int64), mode="mode", mtime_ns="mtime_ns",
            uid="uid", gid="gid", frame=pl.lit(-1, pl.Int32),
            frame_coff=pl.lit(-1, pl.Int64), frame_clen=pl.lit(-1, pl.Int64),
            in_off=pl.lit(-1, pl.Int64))
        if has_dg:
            cols["frame_digest"] = pl.lit(-1, pl.Int64)
        d = dirs.select(**cols).select([pl.col(c).cast(fs[c]) for c in fcols])
        ipc.write_batch(ftmp, d)
    ipc.write_eos(ftmp)
    _zf._write_footer(out_path, ftmp, force_sidecar)
    ftmp.close()
    return total


def _shard_paths(out_path: str, n: int) -> list[str]:
    if n == 1:
        return [out_path]
    if "%d" in out_path:
        return [out_path % s for s in range(n)]
    stem = out_path[:-len(".tar.zstd")] if out_path.endswith(".tar.zstd") else out_path
    return [f"{stem}.shard{s}.tar.zstd" for s in range(n)]


def pack(scan: pl.DataFrame, root: str, out_path: str, qvm_exe: str,
         frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
         nworkers: int = 8, predicate: pl.Expr | None = None,
         shard_by: pl.Expr | None = None, shards: int = 1) -> int:
    """Plan + run a compressed nock pack, then write per-sink footers from the
    frame completions. With shard_by, fans out to `shards` self-contained nock
    archives (out_path %-pattern or `.shardN.tar.zstd`). Returns member count."""
    instr, members, nsinks = plan_pack(scan, root, frame_bytes, level, npool,
                                       predicate, shard_by, shards)
    outs = _shard_paths(out_path, nsinks)
    for o in outs:
        open(o, "wb").close()
    comp = run(instr, qvm_exe, "-", sinks=tuple(outs), npool=npool,
               nworkers=nworkers, want_comp=True)
    dirs = _dir_footer_rows(scan)                 # dir metadata (frame=-1 rows)
    total = 0
    for s, o in enumerate(outs):
        pf = members.filter(pl.col("sink") == s) if nsinks > 1 else members
        # a single archive records the WHOLE dir tree (incl. empty dirs); each
        # reshard shard records only its own files' ancestor dirs (self-contained)
        sd = dirs
        if nsinks > 1 and dirs.height:
            anc = set()
            for p in pf["path"]:
                anc.update(_ancestors(p))
            sd = dirs.filter(pl.col("path").is_in(list(anc)))
        # stream the footer (members⋈completions done row-group-wise, not one
        # big join+serialize) so a 100M-member index never fully materializes
        total += _stream_footer(o, [pf], comp, dirs=sd)
    return total


# ---------------------------------------------------------------- WAL / resume
def _wal_read(wal_path: str) -> dict:
    """Committed frames from a qvm WAL: {frame: (coff, clen, digest)} from the
    32-byte (i64×4) records qvm appends as each deflate lands."""
    if not os.path.exists(wal_path):
        return {}
    raw = open(wal_path, "rb").read()
    n = len(raw) // 32
    if not n:
        return {}
    a = np.frombuffer(raw, dtype="<i8", count=n*4).reshape(n, 4)
    return {int(f): (int(co), int(cl), int(dg)) for f, co, cl, dg in a}


def pack_wal(scan: pl.DataFrame, root: str, out_path: str, qvm_exe: str,
             wal_path: str, frame_bytes: int = 1 << 20, level: int = 6,
             npool: int = 16, nworkers: int = 8,
             predicate: pl.Expr | None = None) -> int:
    """WAL-resumable pack (single archive): qvm fsyncs each committed frame's
    (frame,coff,clen,digest) to `wal_path` as it lands. The plan is
    deterministic, so on resume we replan, DROP every already-committed frame
    (remapping the survivors to a contiguous thread range), truncate the sink to
    its committed high-water, and append only the un-committed tail — re-doing
    just the work the crash lost. The footer is built from the full WAL. Returns
    the file-member count."""
    instr, members, _ = plan_pack(scan, root, frame_bytes, level, npool, predicate)
    committed = _wal_read(wal_path)
    allframes = sorted(int(f) for f in members["frame"].unique()) if members.height else []
    remaining = [f for f in allframes if f not in committed]
    hw = max((co + cl for co, cl, _ in committed.values()), default=0)

    if committed and remaining:                   # resume: keep only un-committed
        newtid = {f + 1: i + 1 for i, f in enumerate(remaining)}  # old tid → new
        body = (instr.filter(pl.col("tid") >= 1)
                     .filter(pl.col("tid").is_in(list(newtid)))
                     .with_columns(pl.col("tid").replace(newtid)))
        root_df = _finalize([pl.DataFrame([
            {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": len(remaining)},
            {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": len(remaining)}])])
        instr = pl.concat([root_df, body])
    elif committed:                               # everything already durable
        instr = _empty_batch()
    else:                                         # fresh run
        open(out_path, "wb").close()

    run(instr, qvm_exe, "-", sinks=(out_path,), npool=npool, nworkers=nworkers,
        env={"QVM_WAL": wal_path, "QVM_SINK_STARTS": str(hw)})

    allc = _wal_read(wal_path)                     # committed + newly appended
    comp = pl.DataFrame(
        {"frame": list(allc), "frame_coff": [v[0] for v in allc.values()],
         "frame_clen": [v[1] for v in allc.values()],
         "digest": [v[2] for v in allc.values()]},
        schema={"frame": pl.Int64, "frame_coff": pl.Int64,
                "frame_clen": pl.Int64, "digest": pl.Int64})
    return _stream_footer(out_path, [members], comp, dirs=_dir_footer_rows(scan))


# --------------------------------------------------------------------- unpack
def plan_unpack(archive: str, dest: str, npool: int = 16,
                idx: pl.DataFrame | None = None) -> pl.DataFrame:
    """compressed nock -> fs: one thread per frame — alloc, inflate the frame
    from the archive, scatter its members to files, free. mkdir preamble in
    thread 0, then a FINISH phase restoring dir mode+mtime from the footer's
    directory rows (frame=-1). Reads the footer via the shared nock reader, or
    uses a caller-supplied `idx` subset (a frame partition, for distributed
    unpack — must include the dir rows the subset needs)."""
    full = idx if idx is not None else _zf.read_index(archive)
    dmeta = full.filter(pl.col("frame") < 0).sort("path")   # dir rows (metadata)
    idx = full.filter(pl.col("frame") >= 0).sort(["frame", "in_off"])   # files
    frames = (idx.group_by("frame").agg(
                  coff=pl.col("frame_coff").first(),
                  clen=pl.col("frame_clen").first(),
                  dlen=(pl.col("in_off") + pl.col("size")).max())
                .sort("frame").with_row_index("frank"))
    nf = frames.height
    frames = frames.with_columns(
        cap=((pl.col("dlen") + 511) // 512) * 512 + 2048,        # inflate headroom
        bufid=(pl.col("frank") % npool).cast(pl.Int32))
    # mkdir set: footer dir paths ∪ ancestors of every path (parents always exist)
    dirs, seen = [], set()
    for p in list(dmeta["path"]) + list(idx["path"]):
        for a in list(_ancestors(p)) + ([p] if p in set(dmeta["path"]) else []):
            if a not in seen:
                seen.add(a); dirs.append(a)
    dd = [(dest, 0)] + [(os.path.join(dest, d), d.count("/") + 1) for d in dirs]
    mkdir_df, phase_rows, fbase, sub = _dir_phases(dd, 1)
    phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                    "lo": fbase, "cap": fbase + nf - 1},
                   {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                    "lo": fbase, "cap": fbase + nf - 1}]
    sub += 2

    idx = idx.join(frames.select("frame", "frank", "bufid"), on="frame") \
             .with_columns(tid=pl.col("frank") + fbase,
                           mrank=pl.int_range(pl.len()).over("frame"))
    ftid = pl.col("frank") + fbase               # a thread per frame, after mkdirs
    alloc = frames.select(tid=ftid, _sub=pl.lit(0),
                          op=pl.lit(OP_ALLOC), buf_id=pl.col("bufid"),
                          cap=pl.col("cap"))
    inflate = frames.select(tid=ftid, _sub=pl.lit(1),
                            op=pl.lit(OP_INFLATE), buf_id=pl.col("bufid"),
                            buf_off=pl.lit(0), arch_off=pl.col("coff"),
                            len=pl.col("clen"))
    scatter = idx.select(tid=pl.col("tid"), _sub=2 + pl.col("mrank"),
                         op=pl.lit(OP_MOV), src=pl.lit(E_BUF), dst=pl.lit(E_FS),
                         buf_id=pl.col("bufid"), buf_off=pl.col("in_off"),
                         len=pl.col("size"),
                         path=pl.lit(dest.rstrip("/") + "/") + pl.col("path"),
                         mode=pl.col("mode") & 0o7777)
    free = frames.select(tid=ftid, _sub=pl.lit(_BIG),
                         op=pl.lit(OP_FREE), buf_id=pl.col("bufid"))

    # finish: restore dir mode+mtime, one thread per footer dir, after all frames
    dbase = fbase + nf
    nd = dmeta.height
    dir_meta = pl.DataFrame({"tid": [], "_sub": []})
    if nd:
        dm = dmeta.with_row_index("k")
        dir_meta = dm.select(
            tid=pl.col("k") + dbase, _sub=pl.lit(0), op=pl.lit(OP_SETMETA),
            path=pl.lit(dest.rstrip("/") + "/") + pl.col("path"),
            mode=pl.col("mode") & 0o7777, mtime_ns=pl.col("mtime_ns"))
        phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                        "lo": dbase, "cap": dbase + nd - 1},
                       {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                        "lo": dbase, "cap": dbase + nd - 1}]
    root_df = pl.DataFrame(phase_rows)
    return _finalize([root_df, mkdir_df, alloc, inflate, scatter, free, dir_meta])


def unpack(archive: str, dest: str, qvm_exe: str, npool: int = 16,
           nworkers: int = 8) -> None:
    os.makedirs(dest, exist_ok=True)
    instr = plan_unpack(archive, dest, npool)
    run(instr, qvm_exe, archive, npool=npool, nworkers=nworkers)


# --------------------------------------------------------------- merge (reduce)
def merge(shards: list[str], out_path: str) -> int:
    """The distributed reduce, ZERO-COPY: join N shard footers into one index
    tagged with shard_id, keeping each shard's frame offsets local — no byte
    movement. The merged archive is a manifest [u32 hlen][shard paths][footer
    ipc]; on extract it is byte-indistinguishable from a single-node run over the
    union. Returns the file-member count."""
    parts, fbase = [], 0
    for k, shard in enumerate(shards):
        idx = _zf.read_index(shard)
        parts.append(idx.with_columns(
            pl.when(pl.col("frame") >= 0).then(pl.col("frame") + fbase)
              .otherwise(-1).cast(pl.Int32).alias("frame"),
            pl.lit(k, dtype=pl.Int32).alias("shard_id")))
        fbase += int(idx.filter(pl.col("frame") >= 0)["frame"].n_unique())
    merged = (pl.concat(parts, how="vertical_relaxed") if parts
              else pl.DataFrame(schema={**_zf._ZF_SCHEMA, "shard_id": pl.Int32}))
    header = ("\n".join(os.path.abspath(s) for s in shards) + "\n").encode()
    with open(out_path, "wb") as f:                 # [u32 hlen][paths][footer ipc]
        f.write(struct.pack("<I", len(header))); f.write(header)
        ipc.write_all(f, merged)
    return int(merged.filter(pl.col("frame") >= 0).height)


def read_merged(out_path: str) -> tuple[list[str], pl.DataFrame]:
    """Return (shard_paths, joined_index) from a merge manifest."""
    with open(out_path, "rb") as f:
        (hlen,) = struct.unpack("<I", f.read(4))
        shards = [ln for ln in f.read(hlen).decode().split("\n") if ln]
        raw = f.read()
    idx = ipc.read_all(raw)
    return shards, idx


def unpack_merged(manifest: str, dest: str, qvm_exe: str, npool: int = 16,
                  nworkers: int = 8, workers: int = 4) -> int:
    """Unpack a merged shard-set: each shard is a standalone nock, so unpack them
    all into `dest` (parallel WITHIN a shard via the fiber scheduler, and up to
    `workers` shards concurrently). Overlapping mkdirs are benign (EEXIST).
    Returns the number of shards unpacked."""
    import concurrent.futures
    shards, _ = read_merged(manifest)
    os.makedirs(dest, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda sh: unpack(sh, dest, qvm_exe, npool, nworkers), shards))
    return len(shards)


def unpack_distributed(path: str, dest: str, qvm_exe: str, executors: int = 4,
                       npool: int = 16, nworkers: int = 8,
                       predicate: pl.Expr | None = None,
                       transports: list | None = None) -> int:
    """Distributed unpack — partition the FRAME set across executors and decode in
    parallel with NO reduce, since each member scatters to its own dest file
    (disjoint outputs on shared storage). Frames are the unit (a decode group
    can't split). A merged manifest round-robins whole SHARDS; a single archive
    round-robins its frames, each executor a separate qvm over its subset (+ all
    dir rows, so every executor materializes the tree). With `transports` (a list
    of argv prefixes, e.g. [["ssh","n1"],["ssh","n2"]]) each executor runs on a
    node via the direct (fd-passing-free) path over shared storage; without it,
    executors are `executors` local processes. Returns the file-member count."""
    import concurrent.futures
    n = len(transports) if transports else executors
    tp = (lambda i: transports[i % len(transports)]) if transports else (lambda i: None)
    os.makedirs(dest, exist_ok=True)
    if path.endswith(".nockm"):                   # merged: distribute whole shards
        shards, _ = read_merged(path)
        groups = [shards[i::n] for i in range(n)]

        def run_group(i):
            for sh in groups[i]:
                run(plan_unpack(sh, dest, npool), qvm_exe, sh,
                           npool=npool, nworkers=nworkers, transport=tp(i))
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
            list(ex.map(run_group, range(n)))
        return sum(_zf.read_index(s).filter(pl.col("frame") >= 0).height
                   for s in shards)
    idx = _zf.read_index(path)
    if predicate is not None:
        idx = idx.filter(predicate)
    dirs = idx.filter(pl.col("frame") < 0)
    files = idx.filter(pl.col("frame") >= 0)
    fids = sorted(files["frame"].unique().to_list())
    node = {f: i % n for i, f in enumerate(fids)}   # round-robin frames

    def run_one(i):
        keep = [f for f in fids if node[f] == i]
        if not keep:
            return
        sub = pl.concat([files.filter(pl.col("frame").is_in(keep)), dirs])
        run(plan_unpack(path, dest, npool, idx=sub), qvm_exe, path,
                   npool=npool, nworkers=nworkers, transport=tp(i))

    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(run_one, range(n)))
    return files.height


def verify(archive: str, qvm_exe: str, npool: int = 16, nworkers: int = 8
           ) -> tuple[int, list[int]]:
    """Integrity check: re-inflate every frame with the xxh64 DIGEST flag and
    compare each against the frame_digest the packer stored in the footer. Only
    decodes (no scatter) so it is cheap. Returns (frames_checked, mismatched
    frame ids) — an empty list means the archive is intact."""
    idx = _zf.read_index(archive)
    if "frame_digest" not in idx.columns:
        raise ValueError("archive carries no digests (packed without integrity)")
    frames = (idx.filter(pl.col("frame") >= 0).group_by("frame").agg(
                  coff=pl.col("frame_coff").first(), clen=pl.col("frame_clen").first(),
                  dg=pl.col("frame_digest").first(),
                  dlen=(pl.col("in_off") + pl.col("size")).max())
                .sort("frame").with_row_index("frank"))
    nf = frames.height
    frames = frames.with_columns(cap=((pl.col("dlen") + 511) // 512) * 512 + 2048,
                                 bufid=(pl.col("frank") % npool).cast(pl.Int32))
    ftid = pl.col("frank") + 1
    root = pl.DataFrame([{"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
                         {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}])
    alloc = frames.select(tid=ftid, _sub=pl.lit(0), op=pl.lit(OP_ALLOC),
                          buf_id=pl.col("bufid"), cap=pl.col("cap"))
    inflate = frames.select(tid=ftid, _sub=pl.lit(1), op=pl.lit(OP_INFLATE),
                            buf_id=pl.col("bufid"), buf_off=pl.lit(0),
                            arch_off=pl.col("coff"), len=pl.col("clen"),
                            mode=pl.lit(1), frame_id=pl.col("frame"))  # digest+report
    free = frames.select(tid=ftid, _sub=pl.lit(_BIG), op=pl.lit(OP_FREE),
                         buf_id=pl.col("bufid"))
    instr = _finalize([root, alloc, inflate, free])
    comp = run(instr, qvm_exe, archive, npool=npool, nworkers=nworkers, want_comp=True)
    got = {int(f): int(d) for f, d in zip(comp["frame"], comp["digest"])}
    exp = {int(f): int(d) for f, d in zip(frames["frame"], frames["dg"])}
    bad = [f for f in exp if got.get(f) != exp[f]]
    return nf, sorted(bad)


# ------------------------------------------------------------------------ S3
def s3_etags(paths: list[str], qvm_exe: str, part_size: int = 8 << 20,
             threads: int = 8) -> pl.DataFrame:
    """Local S3-compatible ETags (+ CRC64NVME) computed by qvm (OP_CKSUM): single
    PutObject (<= part_size) → MD5(file); multipart → MD5(concat of per-part
    MD5s)+"-N". The newline-joined paths ride in the op's payload; the VM hashes
    them in parallel and pushes {path, etag, cksum} back on the DATA channel — the
    basis for content-addressed S3 sync."""
    if not paths:
        return pl.DataFrame(schema={"path": pl.Utf8, "etag": pl.Utf8,
                                    "cksum": pl.Int64})
    instr = _finalize([pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_CKSUM, "payload": "\n".join(paths).encode(),
         "cap": part_size, "level": threads}])])
    return _push_scan(instr, qvm_exe)


def _s3_put(client, bucket, key, local, size, part_size):
    if size <= part_size:
        with open(local, "rb") as f:
            client.put_object(Bucket=bucket, Key=key, Body=f)
        return
    mp = client.create_multipart_upload(Bucket=bucket, Key=key); parts = []
    with open(local, "rb") as f:
        pn = 1
        while (chunk := f.read(part_size)):
            r = client.upload_part(Bucket=bucket, Key=key, UploadId=mp["UploadId"],
                                   PartNumber=pn, Body=chunk)
            parts.append({"PartNumber": pn, "ETag": r["ETag"]}); pn += 1
    client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=mp["UploadId"],
                                     MultipartUpload={"Parts": parts})


def rsync_to_s3(src_root: str, client, bucket: str, qvm_exe: str, prefix: str = "",
                delete: bool = True, part_size: int = 8 << 20, threads: int = 8
                ) -> dict:
    """Content-addressed sync of a local tree → S3, driven entirely by qvm: scan
    the tree, compute expected ETags, and upload ONLY files whose listed ETag
    differs (zero HEADs, no mtime heuristics; multipart above part_size). Deletes
    remote objects absent locally when `delete`. Requires ETag=MD5 semantics
    (no SSE-KMS/SSE-C). Returns {"put": n, "delete": m}."""
    src = scan(src_root, qvm_exe, threads).filter(~pl.col("is_dir"))
    paths = src["path"].to_list()
    local = s3_etags([os.path.join(src_root, p) for p in paths], qvm_exe,
                     part_size, threads).with_columns(path=pl.Series("path", paths))
    dst_rows = []
    for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            dst_rows.append((o["Key"][len(prefix):], o["ETag"].strip('"')))
    dst = (pl.DataFrame({"path": [r[0] for r in dst_rows],
                         "etag_d": [r[1] for r in dst_rows]}) if dst_rows
           else pl.DataFrame(schema={"path": pl.String, "etag_d": pl.String}))
    j = (local.join(src.select("path", "size"), on="path")
              .join(dst, on="path", how="full", coalesce=True))
    ups = j.filter(pl.col("etag").is_not_null()
                   & (pl.col("etag_d").is_null() | (pl.col("etag") != pl.col("etag_d"))))
    dels = j.filter(pl.col("etag").is_null()) if delete else j.clear()
    for r in ups.to_dicts():
        _s3_put(client, bucket, prefix + r["path"],
                os.path.join(src_root, r["path"]), r["size"], part_size)
    for r in dels.to_dicts():
        client.delete_object(Bucket=bucket, Key=prefix + r["path"])
    return {"put": ups.height, "delete": dels.height}


# ------------------------------------------------------------------- encoding
def encode_stream(instr: pl.DataFrame) -> bytes:
    """The instruction stream IS an Arrow-IPC batch — one serialization path
    with the rest of the system (quiver.ipc, compat=oldest so the C reader gets
    large_utf8/large_binary + i64 offsets), produced vectorized by Polars. No
    hand-rolled encoding: the 'heap' is Arrow's own string/binary data buffer."""
    buf = io.BytesIO()
    ipc.write_all(buf, instr)
    return buf.getvalue()


def _find_liburing():
    """Locate a built liburing (header + static lib) — the optional io_uring
    backend. Returns (include_dir, lib_a) or None."""
    import glob
    for h in glob.glob("/tmp/claude-*/**/liburing.h", recursive=True):
        for a in glob.glob(os.path.join(os.path.dirname(h), "..", "**",
                                        "liburing.a"), recursive=True):
            return os.path.dirname(h), os.path.abspath(a)
    return None


def build_qvm(dest: str, src: str | None = None, uring: bool = False):
    """Compile the qvm executor (links libzstd). Prefers the static libzstd.a
    from the conda pkgs dir; falls back to a dynamic -lzstd. With uring=True,
    compiles the optional io_uring backend (-DQVM_URING + liburing) — returns
    None if liburing can't be found so callers can skip."""
    here = os.path.dirname(os.path.abspath(__file__))
    src = src or os.path.join(here, "qvm.c")
    md5 = os.path.join(here, "md5.c")             # vendored MD5 for the etag mode
    zp = "/mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0"
    cmd = ["cc", "-O2", "-pthread", "-o", dest, src, md5]
    cmd += [f"-I{zp}/include", f"{zp}/lib/libzstd.a"] \
        if os.path.exists(f"{zp}/lib/libzstd.a") else ["-lzstd"]
    cmd += ["-lz"]                                # gzip source codec (zlib)
    if uring:
        lu = _find_liburing()
        if not lu:
            return None
        inc, lib = lu
        cmd = cmd[:6] + ["-DQVM_URING", f"-I{inc}"] + cmd[6:] + [lib]
    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def _readn(fd: int, n: int) -> bytes:
    """Read exactly n bytes, tolerating short reads (ssh may fragment the
    back-channel). Returns < n only at EOF."""
    buf = b""
    while len(buf) < n:
        c = os.read(fd, n - len(buf))
        if not c:
            break
        buf += c
    return buf


class _PushVM:
    """Handle a push driver uses to talk to the running qvm: PUSH instruction
    batches on the VM's stdin, and READ the DATA the VM streams back on stdout
    (member rows, kind 0; a 1-byte done flag, kind 2). Lock-step — the driver
    reads a batch's rows before pushing the next — so no reader thread and no
    full-duplex deadlock: the VM's reader always drains stdin."""

    def __init__(self, proc):
        self.p = proc
        self.out = proc.stdout.fileno()
        self.waits = []                          # (t0_ns, t1_ns) blocked-on-read spans

    def push(self, instr: pl.DataFrame) -> None:
        data = encode_stream(instr)
        self.push_raw(struct.pack("<I", len(data)) + data)

    def push_raw(self, framed: bytes) -> None:   # a pre-encoded [len][batch] frame
        self.p.stdin.write(framed); self.p.stdin.flush()

    @staticmethod
    def frame(instr: pl.DataFrame) -> bytes:     # encode + length-prefix, for caching
        data = encode_stream(instr)
        return struct.pack("<I", len(data)) + data

    def _msg(self):                              # one DATA message → (kind, payload)
        t0 = time.monotonic_ns()                 # a read BLOCKS here until the VM emits
        tag = _readn(self.out, 1)
        if not tag:
            self.waits.append((t0, time.monotonic_ns()))
            return None                          # VM closed stdout
        assert tag[0] == 1, f"unexpected stdout tag {tag[0]}"
        kind = _readn(self.out, 1)[0]
        (n,) = struct.unpack("<I", _readn(self.out, 4))
        m = (kind, _readn(self.out, n))
        self.waits.append((t0, time.monotonic_ns()))
        return m

    def read_rows(self) -> pl.DataFrame:         # kind 0: a member-row batch
        m = self._msg(); assert m and m[0] == 0, m
        return ipc.read_all(m[1])

    def read_done(self) -> bool:                 # kind 2: the window done flag
        m = self._msg(); assert m and m[0] == 2, m
        return bool(m[1][0]) if m[1] else True


def push_exec(driver, qvm_exe: str, arch_path: str = "-",
              sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
              want_comp: bool = False, env: dict | None = None,
              transport: list | None = None, extra_fds: tuple[int, ...] = ()):
    """Run a PUSH driver against qvm. Python PUSHES instruction batches on the
    VM's stdin; the VM streams member rows on stdout and exits when stdin closes
    (Python-initiated termination). `driver(vm)` orchestrates the pushes/reads via
    a `_PushVM`; when it returns, stdin is closed and the VM drains. Just stdin +
    stdout, so `transport=["ssh", host]` runs it on a node unchanged (fd: pipe
    sinks still need pass_fds → local). Returns the {frame, coff, clen} completions
    if want_comp."""
    penv = {**os.environ, **env} if env else None
    comp_path = "-"
    if want_comp:
        fd, comp_path = tempfile.mkstemp(prefix="qvm_comp_"); os.close(fd)
    argv = (transport or []) + [qvm_exe, "qvm", arch_path, str(npool),
            str(nworkers), comp_path, "1", *sinks]      # call_fd = 1 (stdout DATA)
    pass_fds = [int(s[3:]) for s in sinks if s.startswith("fd:")]  # local pipe sinks
    pass_fds += list(extra_fds)                                    # e.g. a source pipe
    t_spawn = time.monotonic_ns()                 # ~qvm start (for the Python trace)
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         bufsize=0, pass_fds=pass_fds, env=penv)
    vm = _PushVM(p)
    err = None
    try:
        driver(vm)
    except Exception as e:                        # surface after the VM is reaped
        err = e
    try:
        p.stdin.close()                           # EOF → VM drains + exits
    except BrokenPipeError:
        pass
    while vm._msg() is not None:                   # drain any trailing stdout
        pass
    rc = p.wait()
    pytr = os.environ.get("QVM_PYTRACE")          # optional Python-side trace
    if pytr:                                       # plan spans (CPU) between reads,
        spans, cursor = [], t_spawn                # wait spans = blocked on the VM
        for w0, w1 in vm.waits:
            if w0 > cursor:
                spans.append((cursor, w0, 0))     # 0 = plan (Python computing a batch)
            spans.append((w0, w1, 1)); cursor = w1  # 1 = wait (blocked on VM)
        with open(pytr, "wb") as tf:
            tf.write(struct.pack("<I", len(spans)))
            for a, b, k in spans:
                tf.write(struct.pack("<qqq", a, b, k))
    if err is not None:
        if want_comp:
            os.unlink(comp_path)
        raise err
    if rc != 0:
        if want_comp:
            os.unlink(comp_path)
        raise RuntimeError(f"qvm exited {rc}")
    if not want_comp:
        return None
    with open(comp_path, "rb") as cf:
        raw = cf.read()
    os.unlink(comp_path)
    comp = ipc.read_all(raw)                       # Arrow-IPC (frame, coff, clen)
    return comp.rename({"coff": "frame_coff", "clen": "frame_clen"})


def run(instr: pl.DataFrame, qvm_exe: str, arch_path: str = "-",
        sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
        want_comp: bool = False, env: dict | None = None,
        transport: list | None = None):
    """Drive the C `qvm` for a SINGLE batch: push it, close stdin, collect the
    {frame, coff, clen} completions if want_comp. `sinks` are deflate outputs."""
    return push_exec(lambda vm: vm.push(instr), qvm_exe, arch_path, sinks, npool,
                     nworkers, want_comp, env=env, transport=transport)


def _push_scan(instr: pl.DataFrame, qvm_exe: str,
               transport: list | None = None) -> pl.DataFrame:
    """Push a single scanning-op batch (OP_SCANDIR / OP_CKSUM / OP_TARSCAN) and
    return the one member/stat batch the VM pushes back on the DATA channel."""
    out = {}
    push_exec(lambda vm: (vm.push(instr), out.__setitem__("df", vm.read_rows())),
              qvm_exe, "-", transport=transport)
    return out["df"]


def _empty_batch() -> pl.DataFrame:
    # a 0-instruction batch: qvm builds one thread with an empty program that
    # completes immediately (a no-op pushed batch). Fully typed so encode_stream
    # emits a valid (empty) Arrow batch.
    return pl.DataFrame(schema=INSTR_COLS)

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

import numpy as np
import polars as pl

from .. import ipc
from ..nock.format import TarFormat, plan_layout
from ..nock import zframe as _zf

# opcodes — mirror qvm.c
(OP_ALLOC, OP_FREE, OP_MOV, OP_MKDIR, OP_SETMETA, OP_SPAWN, OP_JOIN,
 OP_INFLATE, OP_DEFLATE, OP_CALL, OP_UNLINK, OP_RMDIR, OP_FBARRIER) = range(1, 14)
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
    fill defaults, order by (tid, _sub), and cast to the instruction schema."""
    df = pl.concat(parts, how="diagonal_relaxed").sort(["tid", "_sub"])
    df = df.with_columns(
        *[pl.col(c).fill_null(v) for c, v in _DEFAULTS.items() if c in df.columns],
        *[pl.lit(v).alias(c) for c, v in _DEFAULTS.items() if c not in df.columns])
    # rechunk so the stream serialises as ONE Arrow record batch (the C reader
    # takes the first batch); column order = INSTR_COLS drives the buffer map.
    return df.select(*[pl.col(c).cast(t) for c, t in INSTR_COLS.items()]).rechunk()


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
def scan(root: str, qvm_exe: str, threads: int = 8) -> pl.DataFrame:
    """Parallel filesystem scan by qvm itself (no dependency on the old scanner):
    `qvm scan <root>` walks the tree with a worker pool and emits one Arrow batch
    — relative path, is_dir, size, mode, mtime_ns, uid, gid (root excluded, dirs +
    files incl. empty). Drop-in for wire.scan for the columns the planner uses."""
    p = subprocess.run([qvm_exe, "scan", os.path.abspath(root), str(threads)],
                       stdout=subprocess.PIPE, check=True)
    df = ipc.read_all(p.stdout)
    return df.with_columns(pl.col("is_dir").cast(pl.Boolean))


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
                npool: int = 16, nworkers: int = 8) -> int:
    """Streaming pack with Python feedback: scan is split into chunks, each
    planned into its own instruction batch (frame ids offset by a running base)
    and STREAMED to the one persistent qvm — batch k+1 is planned while qvm packs
    batch k. The generator plans lazily, so discovery/planning overlaps
    execution. Completions accumulate; one footer at the end. Member count."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    chunks = [files.slice(i, chunk_rows)
              for i in range(0, files.height, chunk_rows)]
    collected: list[pl.DataFrame] = []
    state = {"base": 0}

    def handler(cid):
        if cid == -1:                             # entry: a driver of K CALLs
            rows = [{"tid": 0, "_sub": k, "op": OP_CALL, "frame_id": k}
                    for k in range(len(chunks))]
            return _finalize([pl.DataFrame(rows)]) if rows else _empty_batch()
        instr, members, _ = plan_pack(chunks[cid], root, frame_bytes, level,
                                      npool, frame_base=state["base"])
        collected.append(members)
        state["base"] += members["frame"].n_unique() if members.height else 0
        return instr

    open(out_path, "wb").close()
    comp = run_calls(handler, qvm_exe, "-", sinks=(out_path,), npool=npool,
                     nworkers=nworkers, want_comp=True)
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


def tar_scan(tar_path: str) -> pl.DataFrame:
    """Decode a (uncompressed) tar into a member-row stream — the same schema as
    the filesystem scan (path, is_dir, size, mode, mtime_ns, uid, gid) plus the
    member's location in the archive (offset, header_len, range = header + padded
    body). So tar decode and fs scan are interchangeable discovery front-ends."""
    import tarfile
    rows = []
    with tarfile.open(tar_path, "r:") as tf:
        for m in tf.getmembers():
            if not m.isfile():
                continue
            hl = m.offset_data - m.offset
            rows.append({"path": m.name, "is_dir": False, "size": m.size,
                         "mode": m.mode, "mtime_ns": int(m.mtime) * 1_000_000_000,
                         "uid": m.uid, "gid": m.gid, "offset": m.offset,
                         "header_len": hl,
                         "range": hl + ((m.size + 511) // 512) * 512})
    return pl.DataFrame(rows)


def plan_recompress(members: pl.DataFrame, tar_path: str,
                    frame_bytes: int = 1 << 20, level: int = 6,
                    predicate: pl.Expr | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(uncompressed) tar -> compressed nock, from a tar_scan member stream.
    Thread 0 loads the whole tar into a shared window (buf 0), then spawns a
    child per output frame; each child multi-run `deflate`s its members' byte
    RANGES straight from the window (no re-copy), so a filter that drops members
    just yields non-contiguous runs. The window is freed after the join (shared
    read-only, lifetime = the scope). Returns (instr_df, members_df)."""
    tarsize = os.path.getsize(tar_path)
    df = members
    if predicate is not None:
        df = df.filter(predicate)
    df = df.sort("offset")
    df = df.with_columns(_c=pl.col("range").cum_sum() - pl.col("range"))
    df = df.with_columns(frame=(pl.col("_c") // frame_bytes).cast(pl.Int64))
    df = df.with_columns(_fs=pl.col("_c").min().over("frame"))
    df = df.with_columns(in_off=pl.col("_c") - pl.col("_fs") + pl.col("header_len"))
    # runs: a new run whenever the frame changes or the tar is non-contiguous
    df = df.with_columns(_pe=(pl.col("offset") + pl.col("range")).shift(1),
                         _pf=pl.col("frame").shift(1))
    df = df.with_columns(_new=((pl.col("offset") != pl.col("_pe"))
                               | (pl.col("frame") != pl.col("_pf"))).fill_null(True))
    df = df.with_columns(run=pl.col("_new").cum_sum())
    runs = (df.group_by("run").agg(frame=pl.col("frame").first(),
                                   off=pl.col("offset").min(),
                                   rlen=pl.col("range").sum())
              .sort("off"))                       # gather order = tar order
    frames = df.group_by("frame").agg(clen=pl.col("range").sum()).sort("frame")
    nf = frames.height
    rbf = runs.group_by("frame", maintain_order=True).agg(pl.col("off"),
                                                          pl.col("rlen"))
    payloads = {}
    for r in rbf.iter_rows(named=True):
        a = np.empty(2 * len(r["off"]), dtype="<i8")
        a[0::2] = r["off"]; a[1::2] = r["rlen"]
        payloads[int(r["frame"])] = a.tobytes()

    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_ALLOC, "buf_id": 0, "cap": tarsize},
        {"tid": 0, "_sub": 1, "op": OP_MOV, "src": E_FS, "dst": E_BUF,
         "buf_id": 0, "buf_off": 0, "path": tar_path, "len": tarsize},
        {"tid": 0, "_sub": 2, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 3, "op": OP_JOIN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": _BIG, "op": OP_FREE, "buf_id": 0}])
    deflate = frames.with_columns(
        payload=pl.col("frame").replace_strict(payloads, return_dtype=pl.Binary)).select(
        tid=pl.col("frame") + 1, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
        buf_id=pl.lit(0), buf_off=pl.lit(0), len=pl.col("clen"),
        sink=pl.lit(0), level=pl.lit(level), mode=pl.lit(1), frame_id=pl.col("frame"),
        payload=pl.col("payload"))
    instr = _finalize([root_df, deflate])
    members = df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                        "frame", "in_off")
    return instr, members


def recompress(tar_path: str, out_path: str, qvm_exe: str,
               frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
               nworkers: int = 8, predicate: pl.Expr | None = None) -> int:
    """Plan + run a tar->nock recompress (multi-run gather), then write the
    footer from the completions. Returns the member count."""
    instr, members = plan_recompress(tar_scan(tar_path), tar_path, frame_bytes,
                                     level, predicate)
    open(out_path, "wb").close()
    comp = run(instr, qvm_exe, "-", sinks=(out_path,), npool=max(npool, 1),
               nworkers=nworkers, want_comp=True)
    return _stream_footer(out_path, [members], comp)


def plan_window_gather(wdf: pl.DataFrame, win_start: int, buf_id: int,
                       frame_bytes: int, level: int, frame_base: int
                       ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The gather batch a window's OP_CALL returns: thread 0 spawns a child per
    frame, each multi-run `deflate`s its members' WINDOW-RELATIVE ranges from the
    window buffer `buf_id` (the driver holds it). No alloc/free here — the driver
    owns the buffer. Returns (instr_df, members_df)."""
    df = wdf.sort("offset").with_columns(rel=pl.col("offset") - win_start)
    df = df.with_columns(_c=pl.col("range").cum_sum() - pl.col("range"))
    df = df.with_columns(frame=(pl.col("_c") // frame_bytes).cast(pl.Int64))
    df = df.with_columns(_fs=pl.col("_c").min().over("frame"))
    df = df.with_columns(in_off=pl.col("_c") - pl.col("_fs") + pl.col("header_len"))
    df = df.with_columns(_pe=(pl.col("rel") + pl.col("range")).shift(1),
                         _pf=pl.col("frame").shift(1))
    df = df.with_columns(_new=((pl.col("rel") != pl.col("_pe"))
                               | (pl.col("frame") != pl.col("_pf"))).fill_null(True))
    df = df.with_columns(run=pl.col("_new").cum_sum())
    runs = (df.group_by("run").agg(frame=pl.col("frame").first(),
                                   off=pl.col("rel").min(),
                                   rlen=pl.col("range").sum()).sort("off"))
    frames = df.group_by("frame").agg(clen=pl.col("range").sum()).sort("frame")
    nf = frames.height
    rbf = runs.group_by("frame", maintain_order=True).agg(pl.col("off"), pl.col("rlen"))
    payloads = {}
    for r in rbf.iter_rows(named=True):
        a = np.empty(2 * len(r["off"]), dtype="<i8")
        a[0::2] = r["off"]; a[1::2] = r["rlen"]
        payloads[int(r["frame"])] = a.tobytes()
    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}])
    deflate = frames.with_columns(
        payload=pl.col("frame").replace_strict(payloads, return_dtype=pl.Binary)).select(
        tid=pl.col("frame") + 1, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
        buf_id=pl.lit(buf_id), buf_off=pl.lit(0), len=pl.col("clen"),
        sink=pl.lit(0), level=pl.lit(level), mode=pl.lit(1),   # digest on
        frame_id=pl.col("frame") + frame_base, payload=pl.col("payload"))
    instr = _finalize([root_df, deflate])
    members = df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                        frame=pl.col("frame") + frame_base, in_off="in_off")
    return instr, members


def plan_window_gather_inline(rows: list, win_start: int, win_bytes: bytes,
                              frame_bytes: int, level: int, frame_base: int,
                              predicate: pl.Expr | None = None
                              ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """A self-contained window gather whose bytes arrive INLINE (no seekable
    source): thread 0 allocs buf 0, inline-`mov`s the whole window into it, spawns
    a `deflate` child per frame (multi-run gather of window-relative ranges), and
    frees. Used by the fully-streaming compressed recompress, where each window's
    raw bytes are decoded in Python and carried in the instruction stream. Returns
    (instr_df, members_df); an all-filtered window yields an empty no-op batch."""
    df = pl.DataFrame(rows)
    if predicate is not None:
        df = df.filter(predicate)
    if df.height == 0:
        return _empty_batch(), pl.DataFrame(schema={"frame": pl.Int64})
    df = df.sort("offset").with_columns(rel=pl.col("offset") - win_start)
    df = df.with_columns(_c=pl.col("range").cum_sum() - pl.col("range"))
    df = df.with_columns(frame=(pl.col("_c") // frame_bytes).cast(pl.Int64))
    df = df.with_columns(_fs=pl.col("_c").min().over("frame"))
    df = df.with_columns(in_off=pl.col("_c") - pl.col("_fs") + pl.col("header_len"))
    df = df.with_columns(_pe=(pl.col("rel") + pl.col("range")).shift(1),
                         _pf=pl.col("frame").shift(1))
    df = df.with_columns(_new=((pl.col("rel") != pl.col("_pe"))
                               | (pl.col("frame") != pl.col("_pf"))).fill_null(True))
    df = df.with_columns(run=pl.col("_new").cum_sum())
    runs = (df.group_by("run").agg(frame=pl.col("frame").first(),
                                   off=pl.col("rel").min(),
                                   rlen=pl.col("range").sum()).sort("off"))
    frames = df.group_by("frame").agg(clen=pl.col("range").sum()).sort("frame")
    nf = frames.height
    rbf = runs.group_by("frame", maintain_order=True).agg(pl.col("off"), pl.col("rlen"))
    payloads = {}
    for r in rbf.iter_rows(named=True):
        a = np.empty(2 * len(r["off"]), dtype="<i8")
        a[0::2] = r["off"]; a[1::2] = r["rlen"]
        payloads[int(r["frame"])] = a.tobytes()
    wlen = len(win_bytes)
    root = pl.concat([
        pl.DataFrame([{"tid": 0, "_sub": 0, "op": OP_ALLOC, "buf_id": 0, "cap": wlen}]),
        pl.DataFrame({"tid": [0], "_sub": [1], "op": [OP_MOV], "src": [E_INLINE],
                      "dst": [E_BUF], "buf_id": [0], "buf_off": [0],
                      "payload": [win_bytes]}),
        pl.DataFrame([
            {"tid": 0, "_sub": 2, "op": OP_SPAWN, "lo": 1, "cap": nf},
            {"tid": 0, "_sub": 3, "op": OP_JOIN, "lo": 1, "cap": nf},
            {"tid": 0, "_sub": 4, "op": OP_FREE, "buf_id": 0}]),
    ], how="diagonal_relaxed")
    deflate = frames.with_columns(
        payload=pl.col("frame").replace_strict(payloads, return_dtype=pl.Binary)).select(
        tid=pl.col("frame") + 1, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
        buf_id=pl.lit(0), buf_off=pl.lit(0), len=pl.col("clen"),
        sink=pl.lit(0), level=pl.lit(level), mode=pl.lit(1),   # digest on
        frame_id=pl.col("frame") + frame_base, payload=pl.col("payload"))
    instr = _finalize([root, deflate])
    members = df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                        frame=pl.col("frame") + frame_base, in_off="in_off")
    return instr, members


def _tar_window_stream(src_path: str, window_bytes: int):
    """Stream-decode a .tar.zstd ONCE and yield (win_start, raw_window_bytes,
    member_rows) windows cut on cumulative member footprint — never materializing
    the whole decompressed tar. A tee records the decoded bytes as tarfile reads
    them (bounded to the current window, trimmed as windows are emitted); a
    member's padding lands in the tee only once tarfile advances past it, so a
    closed window is sliced one member late."""
    import zstandard, tarfile

    class _Tee:
        def __init__(self, src): self.src = src; self.buf = bytearray(); self.base = 0
        def read(self, n):
            c = self.src.read(n); self.buf.extend(c); return c
        def end(self): return self.base + len(self.buf)
        def sl(self, a, b): return bytes(self.buf[a - self.base:b - self.base])
        def trim(self, upto): del self.buf[:upto - self.base]; self.base = upto

    zr = zstandard.ZstdDecompressor().stream_reader(open(src_path, "rb"))
    tee = _Tee(zr)
    tf = tarfile.open(fileobj=tee, mode="r|")
    acc, wc, wstart, pend = [], 0, None, None
    for m in tf:
        if pend and tee.end() >= pend[1]:         # prior window's bytes now complete
            yield pend[0], tee.sl(pend[0], pend[1]), pend[2]
            tee.trim(pend[1]); pend = None
        if not m.isfile():
            continue
        hl = m.offset_data - m.offset
        rng = hl + ((m.size + 511) // 512) * 512
        row = {"path": m.name, "size": m.size, "mode": m.mode,
               "mtime_ns": int(m.mtime) * 1_000_000_000, "uid": m.uid, "gid": m.gid,
               "offset": m.offset, "header_len": hl, "range": rng}
        if wstart is None:
            wstart = m.offset
        acc.append(row); wc += rng
        if wc >= window_bytes:
            pend = (wstart, m.offset + rng, acc); acc, wc, wstart = [], 0, None
    if pend:                                       # EOF: trailing bytes all in tee
        yield pend[0], tee.sl(pend[0], pend[1]), pend[2]; tee.trim(pend[1])
    if acc:
        we = acc[-1]["offset"] + acc[-1]["range"]
        yield acc[0]["offset"], tee.sl(acc[0]["offset"], we), acc


def recompress_zst_stream(src_path: str, out_path: str, qvm_exe: str,
                          window_bytes: int = 8 << 20, frame_bytes: int = 1 << 20,
                          level: int = 6, npool: int = 4, nworkers: int = 8,
                          predicate: pl.Expr | None = None, chunk: int = 32) -> int:
    """Fully-streaming recompress of a COMPRESSED foreign source (.tar.zstd) into
    a nock (ISA2 §5.5 feedback mode): decode the source ONCE with a streaming
    decompressor, cutting windows on the fly, and recompress each as it is
    produced — NEVER materializing the whole decompressed tar (bounded to one
    window of raw bytes, carried inline in the instruction stream). Windows are
    driven by a chunked non-recursive driver: thread 0 issues `chunk` window
    CALLs sequentially (each window batch retires, freeing its inline bytes),
    then a continuation CALL fetches the next chunk — so peak memory is one
    window, not the archive. Returns the member count."""
    gen = _tar_window_stream(src_path, window_bytes)
    collected: list[pl.DataFrame] = []
    st = {"base": 0, "next": 0, "done": False}
    CONT = -2

    def driver_chunk():
        start = st["next"]; st["next"] += chunk
        rows = [{"tid": 0, "_sub": i, "op": OP_CALL, "frame_id": start + i}
                for i in range(chunk)]
        rows.append({"tid": 0, "_sub": chunk, "op": OP_CALL, "frame_id": CONT})
        return _finalize([pl.DataFrame(rows)])

    def handler(cid):
        if cid == -1 or cid == CONT:              # entry / continuation: next chunk
            return _empty_batch() if st["done"] else driver_chunk()
        if st["done"]:                            # a filler CALL past end-of-stream
            return _empty_batch()
        try:
            win_start, win_bytes, rows = next(gen)
        except StopIteration:
            st["done"] = True
            return _empty_batch()
        instr, members = plan_window_gather_inline(
            rows, win_start, win_bytes, frame_bytes, level, st["base"], predicate)
        collected.append(members)
        st["base"] += members["frame"].n_unique() if members.height else 0
        return instr

    open(out_path, "wb").close()
    comp = run_calls(handler, qvm_exe, "-", sinks=(out_path,), npool=npool,
                     nworkers=nworkers, want_comp=True)
    return _stream_footer(out_path, collected, comp)   # stream per-window parts


def recompress_windowed(tar_path: str, out_path: str, qvm_exe: str,
                        window_bytes: int = 8 << 20, frame_bytes: int = 1 << 20,
                        level: int = 6, nworkers: int = 8, depth: int = 2,
                        predicate: pl.Expr | None = None) -> int:
    """Windowed streaming recompress via OP_CALL — bounded to `depth` windows.
    The driver fiber PREFETCHES: before CALLing Python to gather window k, it
    spawns a loader fiber that reads window k+1 into the next buffer, so the load
    of the next window overlaps Python's planning and the current window's
    compression. `depth` window buffers ring the pool (2 = double-buffer); the
    pool's alloc backpressure keeps at most `depth` resident. This needs the
    caller batch to have in-flight work at the CALL — hence epoch-routed
    completions in the C scheduler."""
    members = tar_scan(tar_path)
    if predicate is not None:
        members = members.filter(predicate)
    members = members.sort("offset").with_columns(
        _c=pl.col("range").cum_sum() - pl.col("range"))
    members = members.with_columns(win=(pl.col("_c") // window_bytes))
    windows = members.partition_by("win", maintain_order=True)

    win_info = []
    for wdf in windows:
        ws = int(wdf["offset"].min())
        we = int((wdf["offset"] + wdf["range"]).max())
        win_info.append((ws, we - ws, wdf))
    K = len(win_info)

    # One window-fiber per window (tid w+1): alloc a buffer, load the window,
    # CALL Python to plan+gather it, free. The driver spawns them ALL at once; the
    # pool's alloc backpressure admits at most `depth` at a time (buf[w%depth]).
    # Because OP_CALL is async and its response is read OFF the scheduler thread,
    # window w+1's load and its Python planning run concurrently with window w's
    # compression — a genuine load ‖ plan ‖ compress pipeline. depth==1 degrades
    # to sequential (all fibers contend for buf 0) with no deadlock.
    fibers = []
    for w, (ws, wlen, _w) in enumerate(win_info):
        fibers += [
            {"tid": w + 1, "_sub": 0, "op": OP_ALLOC, "buf_id": w % depth,
             "cap": wlen},
            {"tid": w + 1, "_sub": 1, "op": OP_MOV, "src": E_FS, "dst": E_BUF,
             "buf_id": w % depth, "buf_off": 0, "path": tar_path,
             "arch_off": ws, "len": wlen},
            {"tid": w + 1, "_sub": 2, "op": OP_CALL, "frame_id": w},
            {"tid": w + 1, "_sub": 3, "op": OP_FREE, "buf_id": w % depth}]
    drv = ([{"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": K},
            {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": K}] if K else [])
    driver = _finalize([pl.DataFrame(drv + fibers)]) if fibers else _empty_batch()

    collected: list[pl.DataFrame] = []
    state = {"base": 0}

    def handler(cid):
        if cid == -1:                                # entry: the window driver
            return driver
        ws, wlen, wdf = win_info[cid]
        g, wmem = plan_window_gather(wdf, ws, cid % depth, frame_bytes, level,
                                     state["base"])
        collected.append(wmem)
        state["base"] += wmem["frame"].n_unique() if wmem.height else 0
        return g

    open(out_path, "wb").close()
    comp = run_calls(handler, qvm_exe, "-", sinks=(out_path,), npool=depth,
                     nworkers=nworkers, want_comp=True)
    return _stream_footer(out_path, collected, comp)   # stream per-window parts


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
                       predicate: pl.Expr | None = None) -> int:
    """Distributed unpack — partition the FRAME set across `executors` and decode
    in parallel with NO reduce, since each member scatters to its own dest file
    (disjoint outputs on shared storage). Frames are the unit (a decode group
    can't split). A merged manifest round-robins whole SHARDS; a single archive
    round-robins its frames, each executor a separate qvm process over its subset
    (+ all dir rows, so every executor materializes the tree). Executors here are
    local processes; prefixing their argv with ["ssh", host] would place them on
    nodes. Returns the file-member count decoded."""
    import concurrent.futures
    if path.endswith(".nockm"):                   # merged: distribute whole shards
        shards, _ = read_merged(path)
        os.makedirs(dest, exist_ok=True)
        groups = [shards[i::executors] for i in range(executors)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=executors) as ex:
            list(ex.map(lambda g: [unpack(s, dest, qvm_exe, npool, nworkers)
                                   for s in g], groups))
        return sum(_zf.read_index(s).filter(pl.col("frame") >= 0).height
                   for s in shards)
    idx = _zf.read_index(path)
    if predicate is not None:
        idx = idx.filter(predicate)
    dirs = idx.filter(pl.col("frame") < 0)
    files = idx.filter(pl.col("frame") >= 0)
    os.makedirs(dest, exist_ok=True)
    fids = sorted(files["frame"].unique().to_list())
    node = {f: i % executors for i, f in enumerate(fids)}   # round-robin frames

    def run_one(i):
        keep = [f for f in fids if node[f] == i]
        if not keep:
            return
        sub = pl.concat([files.filter(pl.col("frame").is_in(keep)), dirs])
        run(plan_unpack(path, dest, npool, idx=sub), qvm_exe, path,
            npool=npool, nworkers=nworkers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=executors) as ex:
        list(ex.map(run_one, range(executors)))
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
    src = src or os.path.join(os.path.dirname(os.path.abspath(__file__)), "qvm.c")
    zp = "/mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0"
    cmd = ["cc", "-O2", "-pthread", "-o", dest, src]
    cmd += [f"-I{zp}/include", f"{zp}/lib/libzstd.a"] \
        if os.path.exists(f"{zp}/lib/libzstd.a") else ["-lzstd"]
    if uring:
        lu = _find_liburing()
        if not lu:
            return None
        inc, lib = lu
        cmd = cmd[:6] + ["-DQVM_URING", f"-I{inc}"] + cmd[6:] + [lib]
    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def run(instr: pl.DataFrame, qvm_exe: str, arch_path: str = "-",
        sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
        want_comp: bool = False):
    """Encode and drive the C `qvm` executor. `sinks` are deflate output files;
    `want_comp` collects the {frame, coff, clen} completions (via a temp fd) and
    returns them as a DataFrame."""
    return run_calls(lambda cid: instr, qvm_exe, arch_path, sinks, npool,
                     nworkers, want_comp)


def run_calls(handler, qvm_exe: str, arch_path: str = "-",
              sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
              want_comp: bool = False):
    """CALL is qvm's SOLE entry point: qvm boots with one CALL(-1) and pulls
    every instruction batch by CALLing into Python. `handler(call_id)` answers
    each CALL — id -1 is the entry program; a driver's own CALLs pass their
    frame_id. Because a returned batch runs nested in the caller's scope, buffers
    the caller holds are available to it (bounded, lexical resource management).
    Completions accumulate across every (nested) batch."""
    comp_path = "-"
    if want_comp:
        fd, comp_path = tempfile.mkstemp(prefix="qvm_comp_"); os.close(fd)
    cr_r, cr_w = os.pipe()                        # qvm -> Python OP_CALL requests
    argv = [qvm_exe, "qvm", arch_path, str(npool), str(nworkers), comp_path,
            str(cr_w), *sinks]
    pass_fds = [cr_w] + [int(s[3:]) for s in sinks if s.startswith("fd:")]
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, pass_fds=pass_fds)
    os.close(cr_w)                                # qvm holds its own copy
    try:
        while True:
            req = os.read(cr_r, 8)               # a CALL request (blocks)
            if len(req) < 8:
                break                            # qvm closed the call fd → done
            (cid,) = struct.unpack("<q", req)
            data = encode_stream(handler(cid))
            p.stdin.write(struct.pack("<I", len(data)) + data); p.stdin.flush()
    finally:
        try:
            p.stdin.close()
        except BrokenPipeError:
            pass
        os.close(cr_r)
    rc = p.wait()
    if rc != 0:
        if want_comp:
            os.unlink(comp_path)
        raise RuntimeError(f"qvm exited {rc}")
    if not want_comp:
        return None
    with open(comp_path, "rb") as cf:
        raw = cf.read()
    os.unlink(comp_path)
    comp = ipc.read_all(raw)                      # Arrow-IPC (frame, coff, clen)
    return comp.rename({"coff": "frame_coff", "clen": "frame_clen"})


def _empty_batch() -> pl.DataFrame:
    # a 0-instruction batch: qvm builds one thread with an empty program that
    # completes immediately, resuming the CALLer. Fully typed so encode_stream
    # emits a valid (empty) Arrow batch.
    return pl.DataFrame(schema=INSTR_COLS)

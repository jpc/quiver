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
 OP_INFLATE, OP_DEFLATE) = range(1, 10)
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
    """fs -> fs: mkdir the ancestor set (depth order) in thread 0, then spawn a
    thread per file that copy_file_ranges src->dst and sets its metadata."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    n = files.height

    dirs, seen = [], set()                       # ancestor set
    for p in files["path"]:
        for a in _ancestors(p):
            if a not in seen:
                seen.add(a); dirs.append(a)
    # dst_root (depth 0), then each ancestor at its depth; mkdirs run in parallel
    dd = [(dst_root, 0)] + [(os.path.join(dst_root, d), d.count("/") + 1)
                            for d in dirs]
    mkdir_df, phase_rows, fbase, sub = _dir_phases(dd, 1)
    phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                    "lo": fbase, "cap": fbase + n - 1},
                   {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                    "lo": fbase, "cap": fbase + n - 1}]
    root_df = pl.DataFrame(phase_rows)

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
    return _finalize([root_df, mkdir_df, mov, meta])


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
                          buf_id=pl.col("bufid"), cap=pl.col("dl"))
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
                            level=pl.lit(level),
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
    collected: list[pl.DataFrame] = []

    def batches():
        base = 0
        for i in range(0, files.height, chunk_rows):
            sub = files.slice(i, chunk_rows)
            instr, members, _ = plan_pack(sub, root, frame_bytes, level, npool,
                                          frame_base=base)
            collected.append(members)
            base = int(members["frame"].max()) + 1 if members.height else base
            yield instr

    open(out_path, "wb").close()
    comp = run_stream(batches(), qvm_exe, "-", sinks=(out_path,), npool=npool,
                      nworkers=nworkers, want_comp=True)
    members = (pl.concat(collected) if collected
               else pl.DataFrame(schema={"frame": pl.Int64}))
    footer = (members.join(comp, on="frame", how="left")
                     .select([c for c, _ in _zf._FOOTER_IPC]))
    ftmp = tempfile.TemporaryFile()
    ipc.write_all(ftmp, footer)
    _zf._write_footer(out_path, ftmp, False); ftmp.close()
    return footer.height


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
    footer = (members.join(comp, on="frame", how="left")
                     .select([c for c, _ in _zf._FOOTER_IPC]))
    ftmp = tempfile.TemporaryFile()
    ipc.write_all(ftmp, footer)
    _zf._write_footer(out_path, ftmp, False); ftmp.close()
    return footer.height


def plan_recompress(tar_path: str, frame_bytes: int = 1 << 20, level: int = 6,
                    predicate: pl.Expr | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(uncompressed) tar -> compressed nock. Thread 0 loads the whole tar into a
    shared window (buf 0), then spawns a child per output frame; each child
    multi-run `deflate`s its members' byte RANGES straight from the window (no
    re-copy), so a filter that drops members just yields non-contiguous runs.
    The window is freed after the join (shared read-only, lifetime = the scope).
    Returns (instr_df, members_df)."""
    import tarfile
    rows = []
    with tarfile.open(tar_path, "r:") as tf:
        for m in tf.getmembers():
            if not m.isfile():
                continue
            hl = m.offset_data - m.offset
            rng = hl + ((m.size + 511) // 512) * 512      # header + padded body
            rows.append({"path": m.name, "offset": m.offset, "header_len": hl,
                         "size": m.size, "range": rng, "mode": m.mode,
                         "mtime_ns": int(m.mtime) * 1_000_000_000,
                         "uid": m.uid, "gid": m.gid})
    tarsize = os.path.getsize(tar_path)
    df = pl.DataFrame(rows)
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
        payload=pl.col("frame").map_elements(lambda f: payloads[int(f)],
                                             return_dtype=pl.Binary)).select(
        tid=pl.col("frame") + 1, _sub=pl.lit(0), op=pl.lit(OP_DEFLATE),
        buf_id=pl.lit(0), buf_off=pl.lit(0), len=pl.col("clen"),
        sink=pl.lit(0), level=pl.lit(level), frame_id=pl.col("frame"),
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
    instr, members = plan_recompress(tar_path, frame_bytes, level, predicate)
    open(out_path, "wb").close()
    comp = run(instr, qvm_exe, "-", sinks=(out_path,), npool=max(npool, 1),
               nworkers=nworkers, want_comp=True)
    footer = (members.join(comp, on="frame", how="left")
                     .select([c for c, _ in _zf._FOOTER_IPC]))
    ftmp = tempfile.TemporaryFile()
    ipc.write_all(ftmp, footer)
    _zf._write_footer(out_path, ftmp, False); ftmp.close()
    return footer.height


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
    joined = members.join(comp, on="frame", how="left")
    total = 0
    for s, o in enumerate(outs):
        part = (joined.filter(pl.col("sink") == s) if nsinks > 1 else joined) \
            .select([c for c, _ in _zf._FOOTER_IPC])
        ftmp = tempfile.TemporaryFile()
        ipc.write_all(ftmp, part)
        _zf._write_footer(o, ftmp, False); ftmp.close()
        total += part.height
    return total


# --------------------------------------------------------------------- unpack
def plan_unpack(archive: str, dest: str, npool: int = 16) -> pl.DataFrame:
    """compressed nock -> fs: one thread per frame — alloc, inflate the frame
    from the archive, scatter its members to files, free. mkdir preamble in
    thread 0. Reads the footer via the shared nock reader."""
    idx = _zf.read_index(archive).sort(["frame", "in_off"])
    frames = (idx.group_by("frame").agg(
                  coff=pl.col("frame_coff").first(),
                  clen=pl.col("frame_clen").first(),
                  dlen=(pl.col("in_off") + pl.col("size")).max())
                .sort("frame").with_row_index("frank"))
    nf = frames.height
    frames = frames.with_columns(
        cap=((pl.col("dlen") + 511) // 512) * 512 + 2048,        # inflate headroom
        bufid=(pl.col("frank") % npool).cast(pl.Int32))
    dirs, seen = [], set()
    for p in idx["path"]:
        for a in _ancestors(p):
            if a not in seen:
                seen.add(a); dirs.append(a)
    dd = [(dest, 0)] + [(os.path.join(dest, d), d.count("/") + 1) for d in dirs]
    mkdir_df, phase_rows, fbase, sub = _dir_phases(dd, 1)
    phase_rows += [{"tid": 0, "_sub": sub, "op": OP_SPAWN,
                    "lo": fbase, "cap": fbase + nf - 1},
                   {"tid": 0, "_sub": sub + 1, "op": OP_JOIN,
                    "lo": fbase, "cap": fbase + nf - 1}]
    root_df = pl.DataFrame(phase_rows)

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
    return _finalize([root_df, mkdir_df, alloc, inflate, scatter, free])


def unpack(archive: str, dest: str, qvm_exe: str, npool: int = 16,
           nworkers: int = 8) -> None:
    os.makedirs(dest, exist_ok=True)
    instr = plan_unpack(archive, dest, npool)
    run(instr, qvm_exe, archive, npool=npool, nworkers=nworkers)


# ------------------------------------------------------------------- encoding
def encode_stream(instr: pl.DataFrame) -> bytes:
    """The instruction stream IS an Arrow-IPC batch — one serialization path
    with the rest of the system (quiver.ipc, compat=oldest so the C reader gets
    large_utf8/large_binary + i64 offsets), produced vectorized by Polars. No
    hand-rolled encoding: the 'heap' is Arrow's own string/binary data buffer."""
    buf = io.BytesIO()
    ipc.write_all(buf, instr)
    return buf.getvalue()


def build_qvm(dest: str, src: str | None = None) -> str:
    """Compile the qvm executor (links libzstd). Prefers the static libzstd.a
    from the conda pkgs dir; falls back to a dynamic -lzstd."""
    src = src or os.path.join(os.path.dirname(os.path.abspath(__file__)), "qvm.c")
    zp = "/mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0"
    if os.path.exists(f"{zp}/lib/libzstd.a"):
        cmd = ["cc", "-O2", "-pthread", f"-I{zp}/include", "-o", dest, src,
               f"{zp}/lib/libzstd.a"]
    else:
        cmd = ["cc", "-O2", "-pthread", "-o", dest, src, "-lzstd"]
    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def run(instr: pl.DataFrame, qvm_exe: str, arch_path: str = "-",
        sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
        want_comp: bool = False):
    """Encode and drive the C `qvm` executor. `sinks` are deflate output files;
    `want_comp` collects the {frame, coff, clen} completions (via a temp fd) and
    returns them as a DataFrame."""
    return run_stream([instr], qvm_exe, arch_path, sinks, npool, nworkers,
                      want_comp)


def run_stream(batches, qvm_exe: str, arch_path: str = "-",
               sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
               want_comp: bool = False):
    """Drive qvm with an INCREMENTAL stream of instruction batches (each a
    length-framed Arrow batch: [u32 len][bytes]). qvm's scheduler is persistent
    — buffers a batch allocates survive into later batches — so `batches` may be
    a lazy generator that plans the next batch while qvm runs the current one
    (the Python-feedback loop). Completions accumulate across batches."""
    comp_path = "-"
    if want_comp:
        fd, comp_path = tempfile.mkstemp(prefix="qvm_comp_"); os.close(fd)
    argv = [qvm_exe, "qvm", arch_path, str(npool), str(nworkers), comp_path,
            *sinks]
    pass_fds = [int(s[3:]) for s in sinks if s.startswith("fd:")]  # inherited pipes
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, pass_fds=pass_fds)
    try:
        for instr in batches:
            data = encode_stream(instr)
            p.stdin.write(struct.pack("<I", len(data)) + data)
            p.stdin.flush()
    finally:
        p.stdin.close()
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
    (n,) = struct.unpack_from("<I", raw, 0)
    a = np.frombuffer(raw, dtype="<i8", offset=4, count=3 * n).reshape(n, 3)
    return pl.DataFrame({"frame": a[:, 0], "frame_coff": a[:, 1],
                         "frame_clen": a[:, 2]})

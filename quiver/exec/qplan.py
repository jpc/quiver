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


# --------------------------------------------------------------------- cp
def plan_cp(scan: pl.DataFrame, src_root: str, dst_root: str) -> pl.DataFrame:
    """fs -> fs: mkdir the ancestor set (depth order) in thread 0, then spawn a
    thread per file that copy_file_ranges src->dst and sets its metadata."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    n = files.height

    dirs, seen = [], set()                       # ancestor set, depth-sorted
    for p in files["path"]:
        for a in _ancestors(p):
            if a not in seen:
                seen.add(a); dirs.append(a)
    dirs.sort(key=lambda d: d.count("/"))

    # dst_root itself first (mkdir -p), then each ancestor under it
    paths = [dst_root] + [os.path.join(dst_root, d) for d in dirs]
    root = [{"tid": 0, "_sub": i, "op": OP_MKDIR, "path": p, "mode": 0o755}
            for i, p in enumerate(paths)]
    root += [{"tid": 0, "_sub": len(paths), "op": OP_SPAWN, "lo": 1, "cap": n},
             {"tid": 0, "_sub": len(paths) + 1, "op": OP_JOIN, "lo": 1, "cap": n}]
    root_df = pl.DataFrame(root) if root else pl.DataFrame({"tid": [], "_sub": []})

    f = files.with_row_index("k")
    tid = pl.col("k") + 1
    srcp = pl.lit(src_root.rstrip("/") + "/") + pl.col("path")
    dstp = pl.lit(dst_root.rstrip("/") + "/") + pl.col("path")
    mov = f.select(tid=tid, _sub=pl.lit(0), op=pl.lit(OP_MOV),
                   src=pl.lit(E_FS), dst=pl.lit(E_FS),
                   path=srcp, dpath=dstp, len=pl.col("size"),
                   mode=pl.col("mode") & 0o7777)
    meta = f.select(tid=tid, _sub=pl.lit(1), op=pl.lit(OP_SETMETA),
                    path=dstp, mode=pl.col("mode") & 0o7777,
                    mtime_ns=pl.col("mtime_ns"))
    return _finalize([root_df, mov, meta])


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
              level: int = 6, npool: int = 16) -> tuple[pl.DataFrame, pl.DataFrame]:
    """fs -> compressed nock: group members into ~frame_bytes frames; each frame
    is a thread that assembles [header|body] per member into a zeroed buffer and
    deflates it to sink 0. Returns (instr_df, members_df); members_df joins the
    {frame,coff,clen} completions to build the footer. Buffer ids ring over
    `npool` (must match the runner) so at most npool frames pack concurrently."""
    df = (TarFormat().with_header_cols(
              scan.lazy().filter(~pl.col("is_dir")).sort("path"))
          .with_columns(payload_len=((pl.col("size") + 511) // 512) * 512)
          .with_columns(block_len=pl.col("header_len") + pl.col("payload_len"))
          .collect())
    df = df.with_columns(_eo=pl.col("block_len").cum_sum() - pl.col("block_len"))
    df = df.with_columns(frame=(pl.col("_eo") // frame_bytes).cast(pl.Int64))
    df = df.with_columns(_fs=pl.col("_eo").min().over("frame"))
    df = df.with_columns(
        local=pl.col("_eo") - pl.col("_fs"),
        in_off=pl.col("_eo") - pl.col("_fs") + pl.col("header_len"),
        mrank=pl.int_range(pl.len()).over("frame"))
    frames = (df.group_by("frame").agg(clen=pl.col("block_len").sum())
                .sort("frame").with_row_index("frank"))
    nf = frames.height
    frames = frames.with_columns(
        dl=pl.col("clen") + pl.when(pl.col("frank") == nf - 1)
                              .then(1024).otherwise(0),          # tar EOF in last
        bufid=(pl.col("frank") % npool).cast(pl.Int32))
    df = df.join(frames.select("frame", "frank", "bufid"), on="frame") \
           .with_columns(tid=pl.col("frank") + 1)

    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": nf},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": nf}])
    alloc = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(0),
                          op=pl.lit(OP_ALLOC), buf_id=pl.col("bufid"),
                          cap=pl.col("dl"))
    hdr = df.select(tid=pl.col("tid"), _sub=1 + 2 * pl.col("mrank"),
                    op=pl.lit(OP_MOV), src=pl.lit(E_INLINE), dst=pl.lit(E_BUF),
                    buf_id=pl.col("bufid"), buf_off=pl.col("local"),
                    payload=pl.col("header"))
    body = df.select(tid=pl.col("tid"), _sub=2 + 2 * pl.col("mrank"),
                     op=pl.lit(OP_MOV), src=pl.lit(E_FS), dst=pl.lit(E_BUF),
                     buf_id=pl.col("bufid"), buf_off=pl.col("in_off"),
                     path=pl.lit(root.rstrip("/") + "/") + pl.col("path"),
                     len=pl.col("size"))
    deflate = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(_BIG),
                            op=pl.lit(OP_DEFLATE), buf_id=pl.col("bufid"),
                            buf_off=pl.lit(0), len=pl.col("dl"), sink=pl.lit(0),
                            level=pl.lit(level), frame_id=pl.col("frame"))
    free = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(_BIG + 1),
                         op=pl.lit(OP_FREE), buf_id=pl.col("bufid"))
    instr = _finalize([root_df, alloc, hdr, body, deflate, free])
    members = df.select("path", "size", "mode", "mtime_ns", "uid", "gid",
                        "frame", "in_off")
    return instr, members


def pack(scan: pl.DataFrame, root: str, out_path: str, qvm_exe: str,
         frame_bytes: int = 1 << 20, level: int = 6, npool: int = 16,
         nworkers: int = 8) -> int:
    """Plan + run a compressed nock pack, then write the footer from the frame
    completions. Returns the member count."""
    instr, members = plan_pack(scan, root, frame_bytes, level, npool)
    open(out_path, "wb").close()
    comp = run(instr, qvm_exe, "-", sinks=(out_path,), npool=npool,
               nworkers=nworkers, want_comp=True)
    footer = (members.join(comp, on="frame", how="left")
                     .select([c for c, _ in _zf._FOOTER_IPC]))
    ftmp = tempfile.TemporaryFile()
    ipc.write_all(ftmp, footer)
    _zf._write_footer(out_path, ftmp, False); ftmp.close()
    return footer.height


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
    idx = idx.join(frames.select("frame", "frank", "bufid"), on="frame") \
             .with_columns(tid=pl.col("frank") + 1,
                           mrank=pl.int_range(pl.len()).over("frame"))

    dirs, seen = [], set()
    for p in idx["path"]:
        for a in _ancestors(p):
            if a not in seen:
                seen.add(a); dirs.append(a)
    dirs.sort(key=lambda d: d.count("/"))
    paths = [dest] + [os.path.join(dest, d) for d in dirs]
    root = [{"tid": 0, "_sub": i, "op": OP_MKDIR, "path": p, "mode": 0o755}
            for i, p in enumerate(paths)]
    root += [{"tid": 0, "_sub": len(paths), "op": OP_SPAWN, "lo": 1, "cap": nf},
             {"tid": 0, "_sub": len(paths) + 1, "op": OP_JOIN, "lo": 1, "cap": nf}]
    root_df = pl.DataFrame(root)

    alloc = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(0),
                          op=pl.lit(OP_ALLOC), buf_id=pl.col("bufid"),
                          cap=pl.col("cap"))
    inflate = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(1),
                            op=pl.lit(OP_INFLATE), buf_id=pl.col("bufid"),
                            buf_off=pl.lit(0), arch_off=pl.col("coff"),
                            len=pl.col("clen"))
    scatter = idx.select(tid=pl.col("tid"), _sub=2 + pl.col("mrank"),
                         op=pl.lit(OP_MOV), src=pl.lit(E_BUF), dst=pl.lit(E_FS),
                         buf_id=pl.col("bufid"), buf_off=pl.col("in_off"),
                         len=pl.col("size"),
                         path=pl.lit(dest.rstrip("/") + "/") + pl.col("path"),
                         mode=pl.col("mode") & 0o7777)
    free = frames.select(tid=pl.col("frank") + 1, _sub=pl.lit(_BIG),
                         op=pl.lit(OP_FREE), buf_id=pl.col("bufid"))
    return _finalize([root_df, alloc, inflate, scatter, free])


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


def run(instr: pl.DataFrame, qvm_exe: str, arch_path: str = "-",
        sinks: tuple[str, ...] = (), npool: int = 16, nworkers: int = 8,
        want_comp: bool = False):
    """Encode and drive the C `qvm` executor. `sinks` are deflate output files;
    `want_comp` collects the {frame, coff, clen} completions (via a temp fd) and
    returns them as a DataFrame."""
    data = encode_stream(instr)
    comp_path = "-"
    if want_comp:
        fd, comp_path = tempfile.mkstemp(prefix="qvm_comp_"); os.close(fd)
    argv = [qvm_exe, "qvm", arch_path, str(npool), str(nworkers), comp_path,
            *sinks]
    p = subprocess.Popen(argv, stdin=subprocess.PIPE)
    p.stdin.write(data); p.stdin.close()
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

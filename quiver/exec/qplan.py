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
import subprocess

import polars as pl

from .. import ipc
from ..nock.format import TarFormat, plan_layout

# opcodes — mirror qvm.c
OP_ALLOC, OP_FREE, OP_MOV, OP_MKDIR, OP_SETMETA, OP_SPAWN, OP_JOIN = range(1, 8)
# endpoint kinds
E_NONE, E_FS, E_BUF, E_INLINE, E_ARCH = range(5)

# the instruction word (one row = one instruction)
INSTR_COLS = {
    "tid": pl.Int64, "op": pl.UInt8, "src": pl.UInt8, "dst": pl.UInt8,
    "buf_id": pl.Int32, "buf_off": pl.Int64, "len": pl.Int64,
    "cap": pl.Int64, "lo": pl.Int64, "arch_off": pl.Int64,
    "path": pl.String, "dpath": pl.String, "payload": pl.Binary,
    "mode": pl.Int32, "mtime_ns": pl.Int64,
}
_DEFAULTS = {
    "src": 0, "dst": 0, "buf_id": -1, "buf_off": 0, "len": 0, "cap": 0,
    "lo": 0, "arch_off": 0, "path": "", "dpath": "", "payload": b"",
    "mode": -1, "mtime_ns": -1,
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
        npool: int = 16, nworkers: int = 8) -> None:
    """Encode and drive the C `qvm` executor over a pipe."""
    data = encode_stream(instr)
    p = subprocess.Popen([qvm_exe, "qvm", arch_path, str(npool), str(nworkers)],
                         stdin=subprocess.PIPE)
    p.stdin.write(data); p.stdin.close()
    rc = p.wait()
    if rc != 0:
        raise RuntimeError(f"qvm exited {rc}")

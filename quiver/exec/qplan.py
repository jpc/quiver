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

import os
import struct
import subprocess
import tarfile

import numpy as np
import polars as pl

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
    return df.select(*[pl.col(c).cast(t) for c, t in INSTR_COLS.items()])


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
    padded body] at a planner-computed offset, header via inline->arch, body via
    copy_file_range fs->arch. Returns (instr_df, footer_df, archive_size)."""
    files = scan.filter(~pl.col("is_dir")).sort("path")
    rows = files.to_dicts()
    headers, hoff, boff = [], [], []
    cursor = 0
    for r in rows:
        ti = tarfile.TarInfo(r["path"])
        ti.size = r["size"]; ti.mode = r["mode"] & 0o7777
        ti.mtime = r["mtime_ns"] // 1_000_000_000
        ti.uid = r.get("uid", 0) or 0; ti.gid = r.get("gid", 0) or 0
        h = ti.tobuf(format=tarfile.PAX_FORMAT)
        headers.append(h)
        hoff.append(cursor); boff.append(cursor + len(h))
        cursor += len(h) + ((r["size"] + 511) // 512) * 512
    n = len(rows)

    root_df = pl.DataFrame([
        {"tid": 0, "_sub": 0, "op": OP_SPAWN, "lo": 1, "cap": n},
        {"tid": 0, "_sub": 1, "op": OP_JOIN, "lo": 1, "cap": n}])

    tids = pl.arange(1, n + 1, eager=True, dtype=pl.Int64)
    hdr = pl.DataFrame({
        "tid": tids, "_sub": pl.zeros(n, pl.Int64, eager=True),
        "op": pl.repeat(OP_MOV, n, eager=True),
        "src": pl.repeat(E_INLINE, n, eager=True), "dst": pl.repeat(E_ARCH, n, eager=True),
        "arch_off": pl.Series(hoff, dtype=pl.Int64),
        "payload": pl.Series(headers, dtype=pl.Binary)})
    bpaths = [root.rstrip("/") + "/" + r["path"] for r in rows]
    body = pl.DataFrame({
        "tid": tids, "_sub": pl.ones(n, pl.Int64, eager=True),
        "op": pl.repeat(OP_MOV, n, eager=True),
        "src": pl.repeat(E_FS, n, eager=True), "dst": pl.repeat(E_ARCH, n, eager=True),
        "arch_off": pl.Series(boff, dtype=pl.Int64),
        "path": pl.Series(bpaths),
        "len": pl.Series([r["size"] for r in rows], dtype=pl.Int64)})
    instr = _finalize([root_df, hdr, body])

    footer = pl.DataFrame({
        "path": files["path"], "size": files["size"], "mode": files["mode"],
        "mtime_ns": files["mtime_ns"],
        "data_offset": pl.Series(boff, dtype=pl.Int64),
        "read_size": files["size"]})
    return instr, footer, cursor


# ------------------------------------------------------------------- encoding
_REC = np.dtype([
    ("tid", "<u4"), ("op", "u1"), ("src", "u1"), ("dst", "u1"), ("pad", "u1"),
    ("buf_id", "<i4"), ("mode", "<i4"),
    ("buf_off", "<i8"), ("len", "<i8"), ("cap", "<i8"), ("lo", "<i8"),
    ("arch_off", "<i8"), ("mtime_ns", "<i8"),
    ("path_off", "<u4"), ("path_len", "<u4"),
    ("dpath_off", "<u4"), ("dpath_len", "<u4"),
    ("payload_off", "<u4"), ("payload_len", "<u4")])
assert _REC.itemsize == 88


def encode_stream(instr: pl.DataFrame) -> bytes:
    """Serialise to [u32 N][u64 heap_len][N×88-byte records][heap]. Var-length
    path/dpath/payload live in the heap (each \\0-terminated so C can point in
    for strings); records carry (offset,len) into it."""
    n = instr.height
    a = np.zeros(n, dtype=_REC)
    for f in ("tid", "op", "src", "dst", "buf_id", "buf_off", "len", "cap",
              "lo", "arch_off", "mode", "mtime_ns"):
        a[f] = instr[f].to_numpy()
    heap = bytearray()

    def put(b: bytes):
        off = len(heap); heap.extend(b); heap.append(0); return off, len(b)

    paths = instr["path"].to_list()
    dpaths = instr["dpath"].to_list()
    payloads = instr["payload"].to_list()
    for i in range(n):
        a["path_off"][i], a["path_len"][i] = put((paths[i] or "").encode())
        a["dpath_off"][i], a["dpath_len"][i] = put((dpaths[i] or "").encode())
        a["payload_off"][i], a["payload_len"][i] = put(payloads[i] or b"")
    return struct.pack("<IQ", n, len(heap)) + a.tobytes() + bytes(heap)


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

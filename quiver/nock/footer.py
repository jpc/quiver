"""
quiver.nock.footer — the index: write, locate, read, retrofit.

Placement (the slack rule): raw → EOF; tar → after the end-of-archive
marker; Arrow IPC host → spliced before the Arrow Footer. All IO through
pupyarrow; polars never touches pyarrow here.
"""

from __future__ import annotations

import os
import struct
import tarfile as _tarfile

import numpy as np
import polars as pl

from ..pupyarrow import ArrowType, BytesReader, FeatherFile
from ..pupyarrow.writer import write_feather
from .format import BLOCK, RawFormat

MAGIC = b"NOCKIDX1"
TRAILER_LEN = 8 + len(MAGIC)
ARROW_MAGIC = b"ARROW1"

FOOTER_COLS = ["path", "offset", "data_offset", "size", "read_size",
               "mtime_ns", "mode", "uid", "gid"]

_PL2W = {pl.String: "large_string", pl.Binary: "large_binary",
         pl.Int64: "i64", pl.Int32: "i32", pl.UInt64: "u64",
         pl.UInt8: "u8", pl.Float64: "f64",
         pl.Boolean: "u8", pl.UInt32: "i64"}


def _feather_bytes(df: pl.DataFrame, meta: dict[str, str]) -> bytes:
    import io
    schema, cols = [], []
    for name, dt in df.schema.items():
        w = _PL2W[dt.base_type()]
        schema.append((name, w))
        s = df[name]
        if dt == pl.Boolean:
            cols.append(s.cast(pl.UInt8).to_numpy())
        elif w in ("large_string", "large_binary"):
            cols.append(s.to_list())
        else:
            cols.append(s.to_numpy())
    buf = io.BytesIO()
    write_feather(buf, schema, cols, meta=meta)
    return buf.getvalue()


def _df_from_feather(data: bytes) -> tuple[pl.DataFrame, dict]:
    ff = FeatherFile(BytesReader(data))
    cols: dict = {c: [] for c in ff.schema.names}
    for bi in range(ff.num_record_batches):
        rb = ff.record_batch(bi)
        for f in ff.schema.fields:
            a = rb.column(f.name)
            if f.type_id in (ArrowType.LargeUtf8, ArrowType.Utf8):
                cols[f.name].append(pl.Series(a.to_list()))
            elif f.type_id in (ArrowType.LargeBinary, ArrowType.Binary):
                cols[f.name].append(pl.Series(
                    [bytes(v.read()) if hasattr(v, "read") else bytes(v)
                     for v in a.to_list()], dtype=pl.Binary))
            else:
                cols[f.name].append(pl.Series(a.to_numpy()))
    df = pl.DataFrame({k: pl.concat(v) for k, v in cols.items()})
    return df, ff.schema.custom_metadata


def write_footer(afd: int, df: pl.DataFrame, fmt_name: str,
                 end: int, eof_marker: bytes) -> None:
    os.pwrite(afd, eof_marker, end)
    ipc = _feather_bytes(df, {"nock_version": "1", "nock_host": fmt_name})
    off = end + len(eof_marker)
    os.pwrite(afd, ipc, off)
    os.pwrite(afd, struct.pack("<Q", len(ipc)) + MAGIC, off + len(ipc))


def finish_archive(afd: int, plan: pl.DataFrame, read_size: dict[int, int],
                   failed: set[int], fmt: RawFormat) -> pl.DataFrame:
    rows = plan.with_row_index("_i")
    ok = rows.filter(~pl.col("_i").is_in(list(failed)))
    df = ok.select(
        *[c for c in FOOTER_COLS if c != "read_size"]).with_columns(
        read_size=pl.Series([read_size[i] for i in ok["_i"]],
                            dtype=pl.Int64)).select(FOOTER_COLS)
    end = int(plan["offset"][-1] + plan["block_len"][-1]) if len(plan) else 0
    write_footer(afd, df, fmt.name, end, fmt.eof_marker)
    return df


def _locate_trailer(f) -> tuple[int, int]:
    f.seek(0, os.SEEK_END)
    end = f.tell()
    f.seek(end - TRAILER_LEN)
    tail = f.read(TRAILER_LEN)
    if tail[8:] == MAGIC:
        (n,) = struct.unpack("<Q", tail[:8])
        return end - TRAILER_LEN - n, n
    f.seek(end - 10)
    fl = f.read(10)
    if fl[4:] == ARROW_MAGIC:
        (flen,) = struct.unpack("<i", fl[:4])
        fstart = end - 10 - flen
        f.seek(fstart - TRAILER_LEN)
        tail = f.read(TRAILER_LEN)
        assert tail[8:] == MAGIC, "no nock index in this Arrow file"
        (n,) = struct.unpack("<Q", tail[:8])
        return fstart - TRAILER_LEN - n, n
    raise ValueError("no nock index found")


def read_index(path: str) -> pl.DataFrame:
    with open(path, "rb") as f:
        off, n = _locate_trailer(f)
        f.seek(off)
        data = f.read(n)
    return _df_from_feather(data)[0]


def extract(path: str, dest: str,
            predicate: pl.Expr | None = None) -> list[str]:
    from pathlib import Path
    idx = read_index(path)
    if predicate is not None:
        idx = idx.filter(predicate)
    idx = idx.sort("data_offset")
    out = []
    with open(path, "rb") as f:
        for r in idx.to_dicts():
            f.seek(r["data_offset"])
            data = f.read(r["read_size"])
            p = Path(dest) / r["path"]
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            if "mode" in r:
                os.chmod(p, r["mode"])
            out.append(r["path"])
    return out


def index_tar(tar_path: str, extra_cols=None) -> pl.DataFrame:
    """Retrofit ('nock') an existing tar / WebDataset shard: append the
    index without touching a single original byte."""
    rows = []
    with _tarfile.open(tar_path, "r:") as tf:
        for m in tf:
            if m.isfile():
                rows.append((m.name, m.offset, m.offset_data, m.size,
                             m.size, int(m.mtime) * 1_000_000_000,
                             m.mode, m.uid, m.gid))
        data_end = tf.fileobj.tell()
    df = pl.DataFrame([list(c) for c in zip(*rows)],
                      schema=list(zip(FOOTER_COLS,
                                      (pl.String,) + (pl.Int64,) * 5
                                      + (pl.Int32,) * 3)))
    if extra_cols is not None:
        df = extra_cols(df)
    ipc = _feather_bytes(df, {"nock_version": "1", "nock_host": "tar",
                              "nock_source": "index_tar"})
    with open(tar_path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        pad = b"\0" * (2 * BLOCK) if end == data_end else b""
        f.write(pad + ipc + struct.pack("<Q", len(ipc)) + MAGIC)
    return df


def index_arrow_shard(shard_path: str, data_column: str,
                      key_column: str | None = None) -> pl.DataFrame:
    """Retrofit an Arrow IPC shard: per-row byte ranges of data_column
    from pupyarrow lazy-buffer metadata, index spliced before the Arrow
    Footer so pyarrow/polars readers are unaffected."""
    rows = []
    with FeatherFile(shard_path) as ff:
        row = 0
        for bi in range(ff.num_record_batches):
            rb = ff.record_batch(bi)
            col = rb.column(data_column)
            assert getattr(col, "compression", None) in (None, "none"), \
                "compressed IPC buffers are not range-addressable"
            keys = rb.column(key_column).to_list() if key_column else None
            for i in range(rb.num_rows):
                buf = col._get_single(i)
                name = (keys[i] if keys else f"row{row:08d}") \
                       + "." + data_column
                rows.append((name, buf.offset, buf.offset, buf.length,
                             buf.length, 0, 0o644, 0, 0))
                row += 1
    df = pl.DataFrame([list(c) for c in zip(*rows)],
                      schema=list(zip(FOOTER_COLS,
                                      (pl.String,) + (pl.Int64,) * 5
                                      + (pl.Int32,) * 3)))
    ipc = _feather_bytes(df, {"nock_version": "1",
                              "nock_host": "arrow",
                              "nock_data_column": data_column})
    with open(shard_path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        f.seek(end - 10)
        (flen,) = struct.unpack("<i", f.read(4))
        tail_start = end - 10 - flen
        f.seek(tail_start)
        arrow_tail = f.read()
        f.seek(tail_start)
        f.write(ipc + struct.pack("<Q", len(ipc)) + MAGIC + arrow_tail)
    return df

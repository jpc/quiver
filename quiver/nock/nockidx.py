"""quiver.nock.nockidx — the nock footer-index layer.

The on-disk nock format: an ordinary multi-frame tar.zstd, with the member index
appended as one or more zstd *skippable* frames (so standard tools ignore it),
self-locating from EOF by a trailer. This module is the read/write of that index
— the ONLY part of the old zframe engine the qvm executor depends on, factored
out here so qvm needs nothing from the executor half. zframe re-exports these for
backward compatibility.
"""
from __future__ import annotations

import io
import os
import struct

import numpy as np
import polars as pl

from . import footer as _footer
from .. import ipc

SKIP_MAGIC = 0x184D2A50           # zstd skippable-frame magic (base .0-.F)
# per-skippable-frame payload cap (u32 length, minus room for the trailer);
# a module constant so tests can force the multi-frame path on small footers.
_SKIP_CHUNK = 0xFFFF0000

# the footer index schema — one row per member (frame → in-frame offset)
_ZF_SCHEMA = {
    "path": pl.String, "size": pl.Int64, "mode": pl.Int32,
    "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32,
    "frame": pl.Int32, "frame_coff": pl.Int64,
    "frame_clen": pl.Int64, "in_off": pl.Int64}
_FOOTER_IPC = [("path", "large_string"), ("size", "i64"), ("mode", "i32"),
               ("mtime_ns", "i64"), ("uid", "i32"), ("gid", "i32"),
               ("frame", "i32"), ("frame_coff", "i64"),
               ("frame_clen", "i64"), ("in_off", "i64")]
_FNUM_DT = [np.int64, np.int32, np.int64, np.int32, np.int32,
            np.int32, np.int64, np.int64, np.int64]   # the 9 numeric cols
_FOOTER_PL = dict(_ZF_SCHEMA)                          # polars footer schema


def _footer_bytes(path: str) -> bytes:
    """Return the raw footer IPC-stream bytes: from a .nock sidecar, a single
    embedded skippable frame (NOCKIDX1), or several skippable frames
    (NOCKIDXM, for footers larger than one frame's u32 length)."""
    side = path + ".nock"
    if os.path.exists(side):
        with open(side, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            f.seek(end - _footer.TRAILER_LEN)
            (flen,) = struct.unpack("<Q", f.read(8))
            f.seek(0)
            return f.read(flen)
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        f.seek(end - _footer.TRAILER_LEN)
        tail = f.read(_footer.TRAILER_LEN)
        if tail[8:] == _footer.MAGIC_MULTI:       # multi-frame: walk & concat
            (span,) = struct.unpack("<Q", tail[:8])
            f.seek(end - span)
            data = bytearray()
            while f.tell() < end:
                _sm, clen = struct.unpack("<II", f.read(8))  # skippable header
                data += f.read(clen)
            return bytes(data[:-_footer.TRAILER_LEN])         # drop the trailer
        off, n = _footer._locate_trailer(f)       # NOCKIDX1 single frame
        f.seek(off)
        return f.read(n)


def _write_footer(out_path: str, ftmp, force_sidecar: bool) -> None:
    """Finalize the nock footer, self-locating from EOF. It fits in one zstd
    skippable frame → single frame `[len][NOCKIDX1]`. Larger than one frame's
    u32 length → split across several skippable frames (each still skipped by
    standard tools), trailer `[span][NOCKIDXM]`; the archive stays a clean
    multi-frame tar.zstd with the index embedded, no sidecar. `force_sidecar`
    still writes a <archive>.nock sidecar (e.g. for append targets)."""
    flen = ftmp.tell()
    ftmp.seek(0)
    if force_sidecar:
        with open(out_path + ".nock", "wb") as side:
            while (c := ftmp.read(1 << 20)):
                side.write(c)
            side.write(struct.pack("<Q", flen) + _footer.MAGIC)
        return

    def _copy(dst, n):
        while n:
            c = ftmp.read(min(1 << 20, n))
            dst.write(c); n -= len(c)

    tlen = _footer.TRAILER_LEN
    with open(out_path, "ab") as fo:
        if flen + tlen <= 0xFFFFFFFF:              # one skippable frame
            fo.write(struct.pack("<II", SKIP_MAGIC, flen + tlen))
            _copy(fo, flen)
            fo.write(struct.pack("<Q", flen) + _footer.MAGIC)
        else:                                      # several skippable frames
            nframes = (flen + _SKIP_CHUNK - 1) // _SKIP_CHUNK
            span = 8 * nframes + flen + tlen       # total footer-region bytes
            remaining = flen
            for i in range(nframes):
                take = min(_SKIP_CHUNK, remaining)
                last = i == nframes - 1
                fo.write(struct.pack("<II", SKIP_MAGIC,
                                     take + (tlen if last else 0)))
                _copy(fo, take)
                remaining -= take
                if last:
                    fo.write(struct.pack("<Q", span) + _footer.MAGIC_MULTI)


def read_index(path: str) -> pl.DataFrame:
    """Footer frame (member → frame + in-frame offset), read from the streamed
    IPC footer via Polars (concatenating its batches)."""
    data = _footer_bytes(path)
    dfs = list(ipc.Reader(io.BytesIO(data)))
    return (pl.concat(dfs) if dfs else pl.DataFrame(schema=_ZF_SCHEMA))

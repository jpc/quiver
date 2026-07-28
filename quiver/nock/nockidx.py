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


# ------------------------------------------------------------ chunked footer (current)
def merkle_root(leaves):
    """Binary Merkle tree over 32B leaf hashes (BLAKE2b-256); odd node promoted.
    Root of [] is H(b"") — a defined empty-tree value."""
    import hashlib
    if not leaves:
        return hashlib.blake2b(b"", digest_size=32).digest()
    lvl = list(leaves)
    while len(lvl) > 1:
        nxt = []
        for i in range(0, len(lvl) - 1, 2):
            nxt.append(hashlib.blake2b(lvl[i] + lvl[i + 1], digest_size=32).digest())
        if len(lvl) % 2:
            nxt.append(lvl[-1])
        lvl = nxt
    return lvl[0]


def pack_footer(df, base_off: int, rows_per_batch: int = 1 << 20, level: int = 3,
                prev_off: int | None = None, reuse_ents=None, forced_cuts=None,
                snap_time_ns: int | None = None, prev_commit: bytes | None = None) -> bytes:
    """Build the whole footer REGION for STAT `df`, to be pwritten at file offset `base_off`.
    The table splits into ~`rows_per_batch`-row Arrow-IPC batches — cut only at FRAME
    boundaries so a frame never straddles two batches (a frame is <= frame_bytes, i.e. a few
    thousand rows, so snapping barely moves the cut). Each batch is independently
    zstd-compressed (shared-prefix paths ~15x) in its OWN skippable frame; a final skippable
    frame holds an uncompressed DIRECTORY (nbatch, then per batch: absolute payload offset,
    compressed len, first row, nrow). Trailer `[u64 dir_clen][NOCKZC01]`. Frame-aligned
    batches let the reader scatter each batch's whole frames with no cross-batch carry."""
    import io as _io
    import zstandard as _z
    import numpy as np
    cctx = _z.ZstdCompressor(level=level)
    n = df.height
    if n == 0:
        spans = [] if reuse_ents else [(0, 0)]
    else:
        frame = df["frame"].to_numpy()
        starts = np.flatnonzero(np.concatenate(([True], frame[1:] != frame[:-1])))   # frame-start rows
        targets = np.arange(rows_per_batch, n, rows_per_batch)                        # ~1M, ~2M, ...
        idx = np.searchsorted(starts, targets)                                        # snap FORWARD to a frame start
        fc = np.asarray([c for c in (forced_cuts or []) if 0 < c < n], dtype=np.int64)
        cuts = np.unique(np.concatenate(([0], starts[idx[idx < len(starts)]], fc, [n])))
        spans = list(zip(cuts[:-1].tolist(), cuts[1:].tolist()))
    import hashlib
    out, dirents, leaves = bytearray(), [], []
    for e in (reuse_ents or []):                          # (off, clen, first, nrow[, leaf32])
        dirents.append(tuple(e[:4]))
        leaves.append(e[4] if len(e) > 4 and e[4] else None)
    # reuse_ents: directory entries REFERENCING BATCHES OF OLDER FOOTERS in the append-only
    # chain (absolute offsets — the batches are immutable skippable frames, still in the
    # file/address space). Snapshot N's footer then rewrites only the batches whose rows
    # changed: the bup-tree-sharing analog, and S3-layerable (offsets survive the object map).
    for lo, hi in spans:
        buf = _io.BytesIO(); df[lo:hi].write_ipc(buf)
        comp = cctx.compress(buf.getvalue())
        payload_off = base_off + len(out) + 8            # past this frame's skip header
        out += struct.pack("<II", SKIP_MAGIC, len(comp)) + comp
        dirents.append((payload_off, len(comp), lo, hi - lo))
        leaves.append(hashlib.blake2b(comp, digest_size=32).digest())
    d = bytearray(struct.pack("<I", len(dirents)))
    for off, clen, first, nrow in dirents:
        d += struct.pack("<QQQQ", off, clen, first, nrow)
    if prev_off is not None or snap_time_ns is not None:  # SNAPSHOT CHAIN metadata (old readers
        d += struct.pack("<Q", prev_off or 0)             # ignore trailing bytes): prev = file size
        d += struct.pack("<q", snap_time_ns or 0)         # at the PREVIOUS snapshot; then ctime ns
        if all(l_ is not None for l_ in leaves):          # MERKLE: root + commit + batch leaf hashes
            root = merkle_root(leaves)                    # (leaf = BLAKE2b-256 of the compressed
            commit = hashlib.blake2b(                     # payload; commit chains the history)
                root + (prev_commit or b"\0" * 32) + struct.pack("<q", snap_time_ns or 0),
                digest_size=32).digest()
            d += root + commit + b"".join(leaves)
    out += struct.pack("<II", SKIP_MAGIC, len(d)) + bytes(d)
    out += struct.pack("<Q", len(d)) + _footer.MAGIC_CHUNK
    return bytes(out)


def read_directory(path: str, at: int | None = None, with_prev: bool = False):
    """Return the footer batch directory [(off, clen, first_row, nrow), ...], or None if
    there is no chunked footer. Reads only ~32 B/batch — no batch is decompressed.
    `at` = the file size AT SNAPSHOT TIME (its trailer end) to read an OLDER snapshot's
    footer from the append-only chain; None = the latest (file end). `with_prev` also
    returns the previous snapshot's `at` (or None)."""
    with open(path, "rb") as f:
        end = at if at is not None else os.fstat(f.fileno()).st_size
        f.seek(end - _footer.TRAILER_LEN)
        dir_clen, magic = struct.unpack("<Q", f.read(8))[0], f.read(len(_footer.MAGIC))
        if magic != _footer.MAGIC_CHUNK:
            return (None, None) if with_prev else None
        f.seek(end - _footer.TRAILER_LEN - dir_clen)
        d = f.read(dir_clen)
    nb = struct.unpack_from("<I", d, 0)[0]
    ents = [struct.unpack_from("<QQQQ", d, 4 + i * 32) for i in range(nb)]
    if not with_prev:
        return ents
    base = 4 + nb * 32
    prev = struct.unpack_from("<Q", d, base)[0] if len(d) >= base + 8 else None
    tns = struct.unpack_from("<q", d, base + 8)[0] if len(d) >= base + 16 else None
    root = commit = leaves = None
    if len(d) >= base + 16 + 64 + nb * 32:                # merkle present
        root = bytes(d[base + 16:base + 48])
        commit = bytes(d[base + 48:base + 80])
        leaves = [bytes(d[base + 80 + i * 32:base + 112 + i * 32]) for i in range(nb)]
    return ents, prev, (tns or None), root, commit, leaves


def iter_batches(path: str, batches=None, at: int | None = None):
    """Yield STAT batches (polars frames) from a chunked footer, inflating ONE at a time
    (bounded memory). `batches` = an iterable of batch indices to restrict to. `at` reads
    an older snapshot's footer (see read_directory)."""
    ents = read_directory(path, at=at)
    if ents is None:
        raise ValueError(f"{path}: no chunked (NOCKZC01) footer")
    import zstandard as _z
    dctx = _z.ZstdDecompressor()
    want = None if batches is None else set(batches)
    with open(path, "rb") as f:
        for i, (off, clen, first, nrow) in enumerate(ents):
            if want is not None and i not in want:
                continue
            f.seek(off)
            yield pl.read_ipc(io.BytesIO(dctx.decompress(f.read(clen))))


def read_footer(path: str, at: int | None = None) -> pl.DataFrame:
    """Full STAT table from a chunked footer (concatenates all batches)."""
    dfs = list(iter_batches(path, at=at))
    return pl.concat(dfs) if dfs else pl.DataFrame()


def snapshots(path: str):
    """Walk the append-only snapshot chain newest-first:
    [{"at": file-size-at-snapshot, "time_ns": ctime-or-None}, ...].
    Restore snapshot k by passing its "at" to iter_batches/read_footer/unpack."""
    out, at = [], None
    while True:
        r = read_directory(path, at=at, with_prev=True)
        if r is None or r[0] is None:
            break
        out.append({"at": at if at is not None else os.path.getsize(path), "time_ns": r[2],
                    "root": r[3].hex() if r[3] else None,
                    "commit": r[4].hex() if r[4] else None})
        at = r[1]
        if at is None or at <= 0:
            break
    return out


def _footer_bytes(path: str) -> bytes:
    """LEGACY reader (raw NOCKIDX1/M, retired for writing) — kept only so `migrate` can
    read a pre-chunked archive's footer once and rewrite it. Not on any hot path."""
    side = path + ".nock"
    if os.path.exists(side):
        with open(side, "rb") as f:
            f.seek(-_footer.TRAILER_LEN, os.SEEK_END)
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

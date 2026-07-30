"""quiver.nock.nockidx — the nock footer-index layer.

The on-disk nock format: an ordinary multi-frame tar.zstd, with the member index
appended as one or more zstd *skippable* frames (so standard tools ignore it),
self-locating from EOF by a trailer: [u64 dir_clen]["NOCKZC01"]. This module is the
read/write of that index — the directory of footer batches, the batches themselves,
the snapshot chain, and the Merkle root over them.
""" 
from __future__ import annotations

import io
import os
import struct

import numpy as np
import polars as pl

# The nock trailer, self-locating from EOF: [u64 dir_clen]["NOCKZC01"]. The pre-chunked
# NOCKIDX1/NOCKIDXM layouts are gone — nothing reads or writes them any more.
MAGIC_CHUNK = b"NOCKZC01"
TRAILER_LEN = 8 + len(MAGIC_CHUNK)                     # every magic was 8 bytes

SKIP_MAGIC = 0x184D2A50           # zstd skippable-frame magic (base .0-.F)
# per-skippable-frame payload cap (u32 length, minus room for the trailer);
# a module constant so tests can force the multi-frame path on small footers.
_SKIP_CHUNK = 0xFFFF0000

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
    batches let the reader scatter each batch's whole frames with no cross-batch carry.

    `rows_per_batch` is a RANDOM-ACCESS knob, not a compression one: reaching one member
    costs exactly one batch inflate, so 1M-row batches made `extract --glob` pay ~288 ms to
    reach a single member vs ~1.5 ms at 10k rows (192x), and they put a ~200 MB/nock floor
    under any streaming reader. The compression they buy is nearly nothing — measured on
    real EVI STAT, 12.9x at 1M vs 12.8x at 100k vs 11.9x at 10k, i.e. 8% more footer bytes
    at 10k, ~0.002% of the archive."""
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
    # PARALLEL PER BATCH. Each batch is already independently zstd-compressed into its own
    # skippable frame, so serializing/compressing/hashing them is embarrassingly parallel --
    # only the offset accumulation is sequential, and that is done after, in span order, so
    # the bytes are identical to the serial path. Measured 29.2 s of a 63.2 s footer build on
    # a whole-tree run, which is fully SERIAL after packing. ZstdCompressor is not safe to
    # share across threads, so each task makes its own.
    def _one(span):
        lo, hi = span
        buf = _io.BytesIO(); df[lo:hi].write_ipc(buf)
        comp = _z.ZstdCompressor(level=level).compress(buf.getvalue())
        return lo, hi, comp, hashlib.blake2b(comp, digest_size=32).digest()

    if len(spans) > 4 and not os.environ.get("QUIVER_FOOTER_SERIAL"):
        from concurrent.futures import ThreadPoolExecutor
        _nt = min(len(spans), (os.cpu_count() or 8))
        with ThreadPoolExecutor(max_workers=_nt) as _ex:
            done = list(_ex.map(_one, spans))
    else:
        done = [_one(sp) for sp in spans]
    for lo, hi, comp, leaf in done:
        payload_off = base_off + len(out) + 8            # past this frame's skip header
        out += struct.pack("<II", SKIP_MAGIC, len(comp)) + comp
        dirents.append((payload_off, len(comp), lo, hi - lo))
        leaves.append(leaf)
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
    out += struct.pack("<Q", len(d)) + MAGIC_CHUNK
    return bytes(out)


def footer_path(path: str) -> str:
    """Where this store's footer lives. A single-file nock keeps it appended to the data, so
    the file stays self-describing (`zstd -dc | tar` still works, standard tools skip the
    index). A SHARDED store already spans several files, so nothing is lost by giving the
    footer its own -- and a lot is gained: shard 0 stops being the one shard that is both data
    and index, and the footer no longer waits on one specific shard's last frame."""
    side = path + ".footer"
    return side if os.path.exists(side) else path


def read_directory(path: str, at: int | None = None, with_prev: bool = False):
    """Return the footer batch directory [(off, clen, first_row, nrow), ...], or None if
    there is no chunked footer. Reads only ~32 B/batch — no batch is decompressed.
    `at` = the file size AT SNAPSHOT TIME (its trailer end) to read an OLDER snapshot's
    footer from the append-only chain; None = the latest (file end). `with_prev` also
    returns the previous snapshot's `at` (or None)."""
    with open(footer_path(path), "rb") as f:
        end = at if at is not None else os.fstat(f.fileno()).st_size
        f.seek(end - TRAILER_LEN)
        dir_clen, magic = struct.unpack("<Q", f.read(8))[0], f.read(len(MAGIC_CHUNK))
        if magic != MAGIC_CHUNK:
            return (None, None) if with_prev else None
        f.seek(end - TRAILER_LEN - dir_clen)
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
    with open(footer_path(path), "rb") as f:
        for i, (off, clen, first, nrow) in enumerate(ents):
            if want is not None and i not in want:
                continue
            f.seek(off)
            yield pl.read_ipc(io.BytesIO(dctx.decompress(f.read(clen))))


# Columns added to the STAT schema after stores existed in the wild, with the value that
# reproduces the old behaviour. An append-only chain REUSES the previous snapshot's batches by
# reference, so one incremental written by newer code leaves a footer holding both widths --
# and a plain concat then refuses the whole store. That is exactly what happened to a 4.9 TB
# home backup: two incrementals completed reporting `lost 0`, and every read of the result
# failed with "unable to append to a DataFrame of width 14 with a DataFrame of width 15".
# `shard` is 0 rather than null because a pre-sharding store had exactly one sink.
_SCHEMA_BACKFILL = {"shard": (pl.Int64, 0)}


def _align_batches(dfs):
    """Make batches from different schema eras concatenable, widest-schema-wins."""
    if len(dfs) < 2:
        return dfs
    cols = max((d.columns for d in dfs), key=len)
    if all(d.columns == cols for d in dfs):
        return dfs
    out = []
    for d in dfs:
        missing = [c for c in cols if c not in d.columns]
        unknown = [c for c in missing if c not in _SCHEMA_BACKFILL]
        if unknown:                                       # refuse rather than invent values
            raise ValueError(f"{'/'.join(unknown)}: batch is missing columns with no known "
                             f"backfill; this store was written by an incompatible version")
        if missing:
            dt, val = zip(*((_SCHEMA_BACKFILL[c][0], _SCHEMA_BACKFILL[c][1]) for c in missing))
            d = d.with_columns([pl.lit(v, t).alias(c) for c, t, v in zip(missing, dt, val)])
        out.append(d.select(cols))
    return out


def read_footer(path: str, at: int | None = None) -> pl.DataFrame:
    """Full STAT table from a chunked footer (concatenates all batches).

    Batches may span schema eras: the snapshot chain reuses older batches by reference, so a
    store incrementally backed up by newer code holds both. See _align_batches."""
    dfs = _align_batches(list(iter_batches(path, at=at)))
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
        out.append({"at": at if at is not None else os.path.getsize(footer_path(path)),
                    "time_ns": r[2],
                    "root": r[3].hex() if r[3] else None,
                    "commit": r[4].hex() if r[4] else None})
        at = r[1]
        if at is None or at <= 0:
            break
    return out



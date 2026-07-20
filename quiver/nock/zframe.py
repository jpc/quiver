"""
quiver.nock.zframe — per-batch-frame zstd archives (Path C).

A whole-tar .zstd is one monolithic frame: no random access, serial
decompress. This re-frames the tar into independent zstd frames, each
covering a BATCH of whole tar members, and parks a nock footer in a
trailing zstd skippable frame:

    [frame 0: zstd(members batch 0)] ... [frame N] [skippable: nock footer]

Consequences:
  - random access at BATCH granularity: to read a member, seek to its
    frame's compressed range, decompress that one frame, slice it out;
  - parallel decompress: frames are independent, so extraction fans out
    across the pool (the monolithic frame allowed neither);
  - ratio ≈ whole-stream (batches are large enough to share context);
  - still a valid .tar.zstd: the frames decompress in order to the
    original tar, and standard zstd skips the trailing skippable frame.

Merging multiple inputs into one archive is native — a frame never
spans a source, so N source decompressors feed the framer in parallel
(the input-side parallelism). Frame indices and compressed offsets are
global across sources.

PROTOTYPE: reads whole inputs into memory and compresses frames
single-shot. Production wants a streaming member reader (the 666 GB
source can't be materialized) and a compression-thread pool; the format
and footer are the durable part.
"""

from __future__ import annotations

import io
import os
import struct
import tarfile

import polars as pl
import zstandard as zstd

from . import footer as _footer

SKIP_MAGIC = 0x184D2A50           # zstd skippable-frame magic (base .0-.F)

# footer columns: nock member fields + the frame-locating pair
ZFRAME_COLS = ["path", "size", "mode", "mtime_ns", "uid", "gid",
               "frame", "frame_coff", "frame_clen", "in_off"]


def _read_tar_bytes(src: str) -> bytes:
    """Whole tar bytes (decompressing .zstd). Prototype-only — production
    streams members instead of materializing the tar."""
    if src.endswith((".zst", ".zstd")):
        with open(src, "rb") as f:
            return zstd.ZstdDecompressor().stream_reader(f).read()
    with open(src, "rb") as f:
        return f.read()


def _member_end(data: bytes) -> int:
    """Offset just past the last real member (before the trailing zero
    blocks), so concatenating multiple tars stays one valid tar."""
    tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:")
    ms = tf.getmembers()
    if not ms:
        return 0
    last = ms[-1]
    body = last.offset_data + (last.size + 511) // 512 * 512
    return body


def recompress(inputs, out_path: str, batch_bytes: int = 16 << 20,
               level: int = 10, threads: int = 0,
               keep_tar_valid: bool = True) -> pl.DataFrame:
    """Merge tar (or tar.zstd) `inputs` into one per-batch-frame archive
    at `out_path`. Returns the footer frame. `threads` is per-frame zstd
    worker count (0 = auto)."""
    cctx = zstd.ZstdCompressor(level=level, threads=threads)
    rows, coff, fidx = [], 0, 0
    with open(out_path, "wb") as fout:
        for si, src in enumerate(inputs):
            data = _read_tar_bytes(src)
            tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:")
            members = tf.getmembers()
            # frames end at the trailing zeros of the LAST input only, so
            # a multi-input archive is still one clean tar stream.
            end = (len(data) if (si == len(inputs) - 1 or not keep_tar_valid)
                   else _member_end(data))

            # cut points: member offsets where the running batch crosses
            # batch_bytes (always at a member boundary → whole members)
            cuts, bstart = [0], 0
            for m in members:
                if m.offset > bstart and m.offset - bstart >= batch_bytes:
                    cuts.append(m.offset)
                    bstart = m.offset
            cuts.append(end)

            for fi in range(len(cuts) - 1):
                s, e = cuts[fi], cuts[fi + 1]
                comp = cctx.compress(data[s:e])
                fout.write(comp)
                for m in members:
                    if m.isfile() and s <= m.offset < e:
                        rows.append({
                            "path": m.name, "size": m.size,
                            "mode": m.mode, "mtime_ns": int(m.mtime) * 10**9,
                            "uid": m.uid, "gid": m.gid,
                            "frame": fidx, "frame_coff": coff,
                            "frame_clen": len(comp),
                            "in_off": m.offset_data - s,
                        })
                coff += len(comp)
                fidx += 1

        df = pl.DataFrame(rows, schema={
            "path": pl.String, "size": pl.Int64, "mode": pl.Int32,
            "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32,
            "frame": pl.Int32, "frame_coff": pl.Int64,
            "frame_clen": pl.Int64, "in_off": pl.Int64})
        feat = _footer._feather_bytes(df, {"nock_version": "1",
                                           "nock_host": "zframe"})
        # skippable frame wrapping the nock footer + its self-locating
        # trailer, so nock.read_index finds it from EOF unchanged.
        payload = feat + struct.pack("<Q", len(feat)) + _footer.MAGIC
        fout.write(struct.pack("<II", SKIP_MAGIC, len(payload)))
        fout.write(payload)
    return df


def read_index(path: str) -> pl.DataFrame:
    """Footer frame (member → frame + in-frame offset). Reuses nock's
    EOF-anchored trailer locator — the skippable-frame prefix sits before
    the feather and is ignored."""
    return _footer.read_index(path)


def extract(path: str, dest: str, predicate: pl.Expr | None = None):
    """Prototype extractor: group members by frame, decompress each
    needed frame once, slice members out. The C OP_EXTRACT decompress
    path replaces this for the parallel version."""
    idx = read_index(path)
    if predicate is not None:
        idx = idx.filter(predicate)
    dctx = zstd.ZstdDecompressor()
    out = []
    with open(path, "rb") as f:
        for (coff, clen), grp in idx.group_by(
                ["frame_coff", "frame_clen"], maintain_order=True):
            f.seek(coff)
            raw = dctx.decompress(f.read(clen))     # one batch decompressed
            for r in grp.iter_rows(named=True):
                data = raw[r["in_off"]: r["in_off"] + r["size"]]
                p = os.path.join(dest, r["path"])
                os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
                with open(p, "wb") as w:
                    w.write(data)
                os.chmod(p, r["mode"] & 0o7777)
                out.append(r["path"])
    return out

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
spans a source (each source ends on a frame boundary), so frame indices
and compressed offsets are global across sources and the sources could
be decompressed in parallel (the input-side parallelism).

The reader STREAMS: one member at a time, holding only the current batch
buffer (~batch_bytes) plus the current member, so it runs against
multi-hundred-GB sources without materializing them (verified against
the 666 GB / 110 GB production shards). Still open for full scale: a
frame-level compression pool (zstd's internal `threads` parallelizes
within a frame today), an incremental Arrow-batch footer writer for
hundred-million-member corpora (footer rows are in memory for now), and
the C OP_EXTRACT libzstd path for parallel extraction. The format and
footer are the durable part.
"""

from __future__ import annotations

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


def _tar_stream(src: str):
    """A streaming tar reader over `src`, decompressing .zstd on the fly.
    Never materializes the archive — reads member by member."""
    f = open(src, "rb")
    raw = (zstd.ZstdDecompressor().stream_reader(f)
           if src.endswith((".zst", ".zstd")) else f)
    return tarfile.open(fileobj=raw, mode="r|"), (f, raw)


_ZF_SCHEMA = {
    "path": pl.String, "size": pl.Int64, "mode": pl.Int32,
    "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32,
    "frame": pl.Int32, "frame_coff": pl.Int64,
    "frame_clen": pl.Int64, "in_off": pl.Int64}


def recompress(inputs, out_path: str, batch_bytes: int = 16 << 20,
               level: int = 10, threads: int = 0,
               limit: int | None = None,
               tar_format: int = tarfile.PAX_FORMAT) -> pl.DataFrame:
    """Stream `inputs` (tar or tar.zstd) into one per-batch-frame archive.

    Bounded memory: at most one batch buffer (~batch_bytes) plus the
    current member is held, so this runs against multi-hundred-GB sources
    without materializing them. Re-emits a continuous tar stream (PAX
    headers) and cuts frames at member boundaries; each source ends on a
    frame boundary so sources stay independently framed (future parallel
    input). `limit` caps members per source (sampling / testing).

    Footer rows accumulate in memory — fine up to millions of members;
    hundreds of millions (a full 2 TB text corpus) want the incremental
    Arrow-batch footer writer (a follow-up; the format is unchanged).

    `threads` is zstd's per-frame worker count (0 = auto)."""
    cctx = zstd.ZstdCompressor(level=level, threads=threads)
    rows: list[dict] = []
    coff = fidx = 0
    buf = bytearray()
    frame_base = 0        # continuous-stream offset of buf[0]
    stream_off = 0        # total tar bytes emitted so far
    pending: list[tuple[dict, int]] = []   # (row, data_off) for current buf
    fout = open(out_path, "wb")

    def flush():
        nonlocal coff, fidx, buf, frame_base, pending
        if not buf:
            return
        comp = cctx.compress(bytes(buf))
        fout.write(comp)
        for row, doff in pending:
            row.update(frame=fidx, frame_coff=coff, frame_clen=len(comp),
                       in_off=doff - frame_base)
            rows.append(row)
        coff += len(comp)
        fidx += 1
        frame_base += len(buf)
        buf = bytearray()
        pending = []

    try:
        for src in inputs:
            tin, handles = _tar_stream(src)
            try:
                for k, m in enumerate(tin):
                    if limit is not None and k >= limit:
                        break
                    body = tin.extractfile(m).read() if m.isfile() else b""
                    hdr = m.tobuf(format=tar_format)
                    pad = (-len(body)) % 512
                    data_off = stream_off + len(hdr)
                    buf += hdr
                    buf += body
                    if pad:
                        buf += b"\x00" * pad
                    stream_off += len(hdr) + len(body) + pad
                    if m.isfile():
                        pending.append((
                            {"path": m.name, "size": m.size, "mode": m.mode,
                             "mtime_ns": int(m.mtime) * 10**9,
                             "uid": m.uid, "gid": m.gid}, data_off))
                    if len(buf) >= batch_bytes:
                        flush()
            finally:
                tin.close()
                for h in handles:
                    h.close()
            flush()                       # source boundary = frame boundary
        buf += b"\x00" * 1024             # end-of-archive marker
        flush()

        df = pl.DataFrame(rows, schema=_ZF_SCHEMA)
        feat = _footer._feather_bytes(df, {"nock_version": "1",
                                           "nock_host": "zframe"})
        payload = feat + struct.pack("<Q", len(feat)) + _footer.MAGIC
        fout.write(struct.pack("<II", SKIP_MAGIC, len(payload)))
        fout.write(payload)
    finally:
        fout.close()
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

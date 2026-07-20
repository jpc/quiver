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
the 666 GB / 110 GB production shards). Frames compress on a worker pool
while the single source decompressor streams — a 6x win on
compression-bound (audio) shards (30 -> 180 MB/s at 16 workers).

Two bottlenecks, both addressed: compression-bound inputs (large,
~incompressible members like WAV) by the frame pool; small-member text
by a lean raw-copy parser (`_iter_raw`) that copies each member's exact
tar bytes and parses only name/size from fixed offsets instead of
building tarfile objects and re-emitting headers — 6x on text (6k ->
35k members/s) and byte-preserving. Still open: an incremental
Arrow-batch footer writer for hundred-million-member corpora (footer
rows are in memory now), and the C OP_EXTRACT libzstd path for parallel
extraction. The format and footer are the durable part.
"""

from __future__ import annotations

import collections
import concurrent.futures as cf
import os
import struct
import time

import polars as pl
import zstandard as zstd

from . import footer as _footer

SKIP_MAGIC = 0x184D2A50           # zstd skippable-frame magic (base .0-.F)

# footer columns: nock member fields + the frame-locating pair
ZFRAME_COLS = ["path", "size", "mode", "mtime_ns", "uid", "gid",
               "frame", "frame_coff", "frame_clen", "in_off"]


def _open_stream(src: str):
    """Raw decompressed byte reader over `src` (.zstd on the fly)."""
    f = open(src, "rb")
    raw = (zstd.ZstdDecompressor().stream_reader(f)
           if src.endswith((".zst", ".zstd")) else f)
    return raw, (f, raw)


_BLK = 512
_ZERO = bytes(_BLK)


def _octal(b: bytes) -> int:
    """tar numeric field: octal ASCII, or GNU base-256 for big values."""
    if b[0] & 0x80:
        n = b[0] & 0x7f
        for c in b[1:]:
            n = (n << 8) | c
        return n
    b = b.strip(b"\x00 ")
    return int(b, 8) if b else 0


def _iter_raw(reader, limit=None):
    """Lean streaming tar parser: yield
        (name|None, size, mode, mtime, uid, gid, raw, body_off)
    per member. `raw` is the member's EXACT tar bytes (any PAX/GNU
    extension blocks + header + padded body) — copied, never re-emitted,
    so the output stays byte-identical and there is no tobuf cost.
    `body_off` locates the file body inside `raw`. name is None for
    non-file entries (still copied, no footer row). Handles ustar(+prefix
    split), PAX 'x'/'g', and GNU 'L' long names; unknown typeflags are
    treated as opaque sized members."""
    ext = b""            # accumulated extension-header bytes for next real hdr
    pax: dict = {}
    gnu_name = None
    n = 0
    read = reader.read
    while True:
        hdr = read(_BLK)
        if len(hdr) < _BLK or hdr == _ZERO:
            return
        typ = hdr[156]
        size = _octal(hdr[124:136])
        blen = (size + 511) // _BLK * _BLK
        if typ == 0x78 or typ == 0x67:              # 'x' / 'g' — PAX record
            body = read(blen)
            for line in body[:size].split(b"\n"):
                if not line:
                    continue
                kv = line[line.index(b" ") + 1:]     # "LEN key=value"
                eq = kv.index(b"=")
                pax[kv[:eq].decode()] = kv[eq + 1:].rstrip(b"\n").decode()
            ext += hdr + body
            continue
        if typ == 0x4C:                             # 'L' — GNU long name
            body = read(blen)
            gnu_name = body[:size].split(b"\x00", 1)[0]
            ext += hdr + body
            continue
        if "path" in pax:
            name = pax["path"].encode()
        elif gnu_name is not None:
            name = gnu_name
        else:
            nm = hdr[0:100].split(b"\x00", 1)[0]
            pre = hdr[345:500].split(b"\x00", 1)[0]
            name = pre + b"/" + nm if pre else nm
        rsize = int(pax["size"]) if "size" in pax else size
        rblen = (rsize + 511) // _BLK * _BLK
        body = read(rblen)
        raw = ext + hdr + body if ext else hdr + body
        body_off = len(ext) + _BLK
        if typ == 0x30 or typ == 0x00:              # '0' / NUL — regular file
            n += 1
            yield (name, rsize, _octal(hdr[100:108]), _octal(hdr[136:148]),
                   _octal(hdr[108:116]), _octal(hdr[116:124]), raw, body_off)
            if limit is not None and n >= limit:
                return
        else:
            yield (None, rsize, 0, 0, 0, 0, raw, body_off)
        ext = b""
        pax = {}
        gnu_name = None


_ZF_SCHEMA = {
    "path": pl.String, "size": pl.Int64, "mode": pl.Int32,
    "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32,
    "frame": pl.Int32, "frame_coff": pl.Int64,
    "frame_clen": pl.Int64, "in_off": pl.Int64}


def recompress(inputs, out_path: str, batch_bytes: int = 16 << 20,
               level: int = 10, workers: int | None = None,
               limit: int | None = None,
               progress=None, progress_every: float = 2.0) -> pl.DataFrame:
    """Stream `inputs` (tar or tar.zstd) into one per-batch-frame archive.

    Bounded memory: at most one batch buffer (~batch_bytes) plus the
    current member is held, so this runs against multi-hundred-GB sources
    without materializing them. Copies each member's raw tar bytes
    (byte-preserving) and cuts frames at member boundaries; each source
    ends on a frame boundary so sources stay independently framed (future
    parallel input). `limit` caps file members per source (sampling).

    Footer rows accumulate in memory — fine up to millions of members;
    hundreds of millions (a full 2 TB text corpus) want the incremental
    Arrow-batch footer writer (a follow-up; the format is unchanged).

    Frames compress on a `workers`-wide pool while the (single, serial)
    source decompressor streams — so throughput is bounded by the source
    decompress rate, not by one compressor. Each worker uses its own
    compressor (safe); GIL is released in zstd, giving real parallelism.
    """
    workers = workers or min(16, (os.cpu_count() or 4))
    rows: list[dict] = []
    coff = fidx = nmembers = 0
    buf = bytearray()
    frame_base = 0        # continuous-stream offset of the NEXT frame
    stream_off = 0        # total tar bytes emitted so far
    pending: list[tuple[dict, int]] = []   # (row, data_off) for current buf
    fout = open(out_path, "wb")
    pool = cf.ThreadPoolExecutor(max_workers=workers)
    inflight: collections.deque = collections.deque()  # (future, rows, base)

    def _compress(data: bytes) -> bytes:
        return zstd.ZstdCompressor(level=level).compress(data)  # own ctx/thread
    # progress is by COMPRESSED bytes consumed from the sources — the one
    # total we know up front (single-frame zstd doesn't store the
    # decompressed size). cin_done = finished sources; f.tell() = current.
    cin_total = sum(os.path.getsize(s) for s in inputs)
    t0 = last = time.time()
    cin_done = 0

    def report(f, force=False):
        nonlocal last
        if progress is None:
            return
        now = time.time()
        if not force and now - last < progress_every:
            return
        last = now
        progress({"members": nmembers,
                  "cin": cin_done + (f.tell() if f else 0),
                  "cin_total": cin_total, "cout": coff,
                  "decompressed": stream_off, "frames": fidx,
                  "elapsed": now - t0})

    def drain_one():
        nonlocal coff, fidx
        fut, rws, base = inflight.popleft()
        comp = fut.result()                 # waits for this frame's compress
        fout.write(comp)
        for row, doff in rws:
            row.update(frame=fidx, frame_coff=coff, frame_clen=len(comp),
                       in_off=doff - base)
            rows.append(row)
        coff += len(comp)
        fidx += 1

    def flush():
        """Hand the current batch to the compress pool (in order); drain
        finished frames, bounding in-flight memory."""
        nonlocal buf, frame_base, pending
        if not buf:
            return
        inflight.append((pool.submit(_compress, bytes(buf)), pending,
                         frame_base))
        frame_base += len(buf)
        buf = bytearray()
        pending = []
        while len(inflight) >= 2 * workers:      # keep the pool fed, not flooded
            drain_one()

    try:
        for src in inputs:
            reader, handles = _open_stream(src)
            raw_f = handles[0]            # underlying file for compressed tell()
            try:
                for (name, size, mode, mtime, uid, gid, mraw, boff) in \
                        _iter_raw(reader, limit):
                    data_off = stream_off + boff    # file body in the stream
                    buf += mraw
                    stream_off += len(mraw)
                    if name is not None:
                        pending.append((
                            {"path": name.decode("utf-8", "surrogateescape"),
                             "size": size, "mode": mode,
                             "mtime_ns": mtime * 10**9,
                             "uid": uid, "gid": gid}, data_off))
                        nmembers += 1
                    if len(buf) >= batch_bytes:
                        flush()
                    report(raw_f)
            finally:
                for h in handles:
                    h.close()
            cin_done += os.path.getsize(src)
            flush()                       # source boundary = frame boundary
        buf += b"\x00" * 1024             # end-of-archive marker
        flush()
        while inflight:                   # drain the compress pool, in order
            drain_one()
        report(None, force=True)

        df = pl.DataFrame(rows, schema=_ZF_SCHEMA)
        feat = _footer._feather_bytes(df, {"nock_version": "1",
                                           "nock_host": "zframe"})
        payload = feat + struct.pack("<Q", len(feat)) + _footer.MAGIC
        fout.write(struct.pack("<II", SKIP_MAGIC, len(payload)))
        fout.write(payload)
    finally:
        pool.shutdown(wait=True)
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

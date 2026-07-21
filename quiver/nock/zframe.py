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
import io
import queue
import os
import struct
import tempfile
import threading
import time

import numpy as np
import polars as pl
import zstandard as zstd

from . import footer as _footer
from ..pupyarrow.writer import StreamReader, StreamWriter

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

_FOOTER_IPC = [("path", "large_string"), ("size", "i64"), ("mode", "i32"),
               ("mtime_ns", "i64"), ("uid", "i32"), ("gid", "i32"),
               ("frame", "i32"), ("frame_coff", "i64"),
               ("frame_clen", "i64"), ("in_off", "i64")]
_FNUM_DT = [np.int64, np.int32, np.int64, np.int32, np.int32,
            np.int32, np.int64, np.int64, np.int64]   # the 9 numeric cols

Result = collections.namedtuple("Result", "members frames")


class _FooterStream:
    """Streams footer rows to an Arrow IPC stream (pupyarrow StreamWriter),
    flushing a record batch every `flush_rows` — so only one batch of
    footer rows lives in memory, not the whole (potentially 100M-row)
    index. Columns are accumulated as Python lists and converted to numpy
    at flush (strings stay lists)."""
    def __init__(self, fileobj, flush_rows: int = 1_000_000):
        self.sw = StreamWriter(fileobj, _FOOTER_IPC)
        self.flush_rows = flush_rows
        self.paths: list[str] = []
        self.nums: list[list] = [[] for _ in range(9)]
        self.n = self.members = 0

    def add(self, path, size, mode, mtime_ns, uid, gid,
            frame, coff, clen, in_off):
        self.paths.append(path)
        for col, v in zip(self.nums, (size, mode, mtime_ns, uid, gid,
                                      frame, coff, clen, in_off)):
            col.append(v)
        self.n += 1
        self.members += 1
        if self.n >= self.flush_rows:
            self.flush()

    def flush(self):
        if not self.n:
            return
        cols = [self.paths] + [np.asarray(self.nums[i], dtype=_FNUM_DT[i])
                               for i in range(9)]
        self.sw.write_batch(cols)
        self.paths = []
        self.nums = [[] for _ in range(9)]
        self.n = 0

    def close(self):
        self.flush()
        self.sw.close()


def recompress(inputs, out_path: str, batch_bytes: int = 16 << 20,
               level: int = 10, workers: int | None = None,
               limit: int | None = None,
               progress=None, progress_every: float = 2.0) -> pl.DataFrame:
    """Stream `inputs` (tar or tar.zstd) into one per-batch-frame archive.

    Parallel input: `workers` producer threads each own a subset of the
    sources and run the FULL pipeline — decompress, raw-copy parse
    (`_iter_raw`), and compress each batch frame inline — then serialize
    only the frame write + footer append under one lock. zstd releases
    the GIL, so decompress and compress genuinely overlap across
    producers; profiling the sequential version showed one producer left
    the compressors ~52% idle, which this fills.

    Frames from different producers interleave in the output; that's
    fine — each frame is a whole number of members with no interior
    zero blocks (only the final end-of-archive frame has them), so the
    concatenation is still one valid tar, and the footer records every
    frame's real offset. in_off is the member's offset WITHIN its frame,
    so frames are self-contained (no global stream offset needed).

    Bounded memory: ~workers x batch_bytes of live buffers plus the
    streaming footer. Copies raw member bytes (byte-preserving); cuts
    frames at member boundaries; each source ends on a frame boundary.
    `limit` caps file members per source (sampling)."""
    inputs = list(inputs)
    workers = workers or (os.cpu_count() or 4)       # compress-pool size
    # A few reader threads decompress+parse sources in parallel and feed
    # ONE shared compress pool. So all cores keep compressing even when a
    # single huge source (the 666 GB one) is all that's left — the pure
    # inline-compress producer would serialize that tail. A semaphore
    # bounds outstanding frames (memory).
    readers = max(1, min(12, len(inputs)))
    fout = open(out_path, "wb")
    ftmp = tempfile.TemporaryFile()        # footer IPC stream, spills to disk
    fw = _FooterStream(ftmp)
    wlock = threading.Lock()               # serializes writes + footer + stats
    st = {"coff": 0, "fidx": 0, "members": 0}
    cin_total = sum(os.path.getsize(s) for s in inputs)
    cin_done = 0
    partial = [0] * readers                # each reader's in-progress tell()
    t0 = time.time()
    last = [t0]
    errors: list = []
    pool = cf.ThreadPoolExecutor(max_workers=workers)
    slots = threading.Semaphore(workers * 2)   # bound in-flight frames
    tls = threading.local()

    def maybe_report(force=False):
        if progress is None:
            return
        now = time.time()
        if not force and now - last[0] < progress_every:
            return
        last[0] = now
        progress({"members": st["members"], "cin": cin_done + sum(partial),
                  "cin_total": cin_total, "cout": st["coff"],
                  "decompressed": 0, "frames": st["fidx"],
                  "elapsed": now - t0})

    def write_frame(comp: bytes, rows: list):
        """Serialize one compressed frame to the output + footer."""
        with wlock:
            fout.write(comp)
            coff, fidx, clen = st["coff"], st["fidx"], len(comp)
            for (path, size, mode, mtime, uid, gid, in_off) in rows:
                fw.add(path, size, mode, mtime, uid, gid,
                       fidx, coff, clen, in_off)
            st["coff"] += clen
            st["fidx"] += 1
            st["members"] += len(rows)
            maybe_report()

    def _compress_write(data: bytes, rows: list):
        try:
            c = getattr(tls, "c", None)
            if c is None:
                c = tls.c = zstd.ZstdCompressor(level=level)   # per worker
            write_frame(c.compress(data), rows)
        finally:
            slots.release()

    def _submit(data: bytes, rows: list):
        slots.acquire()                    # backpressure on fast readers
        pool.submit(_compress_write, data, rows)

    srcq: queue.Queue = queue.Queue()
    for s in inputs:
        srcq.put(s)

    def reader(rid: int):
        nonlocal cin_done
        while True:
            try:
                src = srcq.get_nowait()
            except queue.Empty:
                return
            try:
                rd, handles = _open_stream(src)
                raw_f = handles[0]
                buf = bytearray()
                rows: list = []
                try:
                    for (name, size, mode, mtime, uid, gid, mraw, boff) in \
                            _iter_raw(rd, limit):
                        in_off = len(buf) + boff       # offset within THIS frame
                        buf += mraw
                        if name is not None:
                            rows.append((
                                name.decode("utf-8", "surrogateescape"),
                                size, mode, mtime * 10**9, uid, gid, in_off))
                        if len(buf) >= batch_bytes:
                            _submit(bytes(buf), rows)
                            buf, rows = bytearray(), []
                            partial[rid] = raw_f.tell()
                    if buf:                            # source's final frame
                        _submit(bytes(buf), rows)
                finally:
                    for h in handles:
                        h.close()
            except Exception as e:                     # isolate a bad source
                errors.append((src, e))
            with wlock:
                cin_done += os.path.getsize(src)
                partial[rid] = 0

    try:
        threads = [threading.Thread(target=reader, args=(i,), daemon=True)
                   for i in range(readers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        pool.shutdown(wait=True)           # drain outstanding compress+writes
        if errors:
            raise RuntimeError(f"{len(errors)} source(s) failed; "
                               f"first: {errors[0][0]}: {errors[0][1]!r}")
        # end-of-archive marker as its own final frame
        write_frame(zstd.ZstdCompressor(level=level).compress(b"\x00" * 1024),
                    [])
        maybe_report(force=True)

        # footer is now a finished IPC stream on `ftmp` (bounded memory).
        fw.close()
        flen = ftmp.tell()
        # trailer: [len][MAGIC], self-locating from EOF like every nock host.
        # ≤4 GB → one zstd skippable frame (standard tools skip it); larger
        # → a .nock sidecar (the archive stays a clean multi-frame tar.zstd).
        trailer = struct.pack("<Q", flen) + _footer.MAGIC
        if flen + len(trailer) <= 0xFFFFFFFF:
            fout.write(struct.pack("<II", SKIP_MAGIC, flen + len(trailer)))
            ftmp.seek(0)
            while True:
                chunk = ftmp.read(1 << 20)
                if not chunk:
                    break
                fout.write(chunk)
            fout.write(trailer)
        else:
            with open(out_path + ".nock", "wb") as side:
                ftmp.seek(0)
                while True:
                    chunk = ftmp.read(1 << 20)
                    if not chunk:
                        break
                    side.write(chunk)
                side.write(trailer)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        ftmp.close()
        fout.close()
    return Result(members=fw.members, frames=st["fidx"])


def _footer_bytes(path: str) -> bytes:
    """Return the raw footer IPC-stream bytes, from the embedded skippable
    frame or the .nock sidecar."""
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
        off, n = _footer._locate_trailer(f)   # EOF-anchored, ignores skip hdr
        f.seek(off)
        return f.read(n)


def _write_footer(out_path: str, ftmp, force_sidecar: bool) -> None:
    """Finalize the nock footer: [len][MAGIC], self-locating from EOF. ≤4 GB
    embeds in one zstd skippable frame (standard tools skip it); larger goes
    to a <archive>.nock sidecar, keeping the archive a clean multi-frame
    tar.zstd (a skippable frame's length field is u32)."""
    flen = ftmp.tell()
    trailer = struct.pack("<Q", flen) + _footer.MAGIC
    ftmp.seek(0)
    if flen + len(trailer) <= 0xFFFFFFFF and not force_sidecar:
        with open(out_path, "ab") as fo:
            fo.write(struct.pack("<II", SKIP_MAGIC, flen + len(trailer)))
            while (c := ftmp.read(1 << 20)):
                fo.write(c)
            fo.write(trailer)
    else:
        with open(out_path + ".nock", "wb") as side:
            while (c := ftmp.read(1 << 20)):
                side.write(c)
            side.write(trailer)


def _ingest_footer(f, finalize, progress, progress_every, cin_total,
                   cout_fn=None):
    """Read the per-member footer records streamed by zpack/zexec (identical
    60B-tail format, sink-tagged), build one footer per sink, and hand each
    finished footer temp file to `finalize(sink, ftmp)`. Returns
    (members, frames, sinks)."""
    import time
    rec = struct.Struct("<qqiiiiqqqi")          # 60B tail: adds i32 sink
    footers = {}                                # sink -> (ftmp, _FooterStream)

    def fw_for(sink):
        if sink not in footers:
            ft = tempfile.TemporaryFile()
            footers[sink] = (ft, _FooterStream(ft))
        return footers[sink][1]

    buf = b""
    frames = members = 0
    t0 = last = time.time()
    while True:
        chunk = f.read(1 << 20)
        if not chunk:
            break
        buf += chunk
        i, n = 0, len(buf)
        while True:
            if i + 2 > n:
                break
            plen = buf[i] | (buf[i + 1] << 8)
            if i + 2 + plen + 60 > n:
                break
            path = buf[i + 2:i + 2 + plen].decode("utf-8", "surrogateescape")
            (size, mtime, mode, uid, gid, frame, coff, clen, in_off, sink) = \
                rec.unpack_from(buf, i + 2 + plen)
            fw_for(sink).add(path, size, mode, mtime, uid, gid,
                             frame, coff, clen, in_off)
            members += 1
            if frame + 1 > frames:
                frames = frame + 1
            i += 2 + plen + 60
        buf = buf[i:]
        now = time.time()
        if progress and now - last >= progress_every:
            last = now
            cout = cout_fn(footers) if cout_fn else 0
            progress({"members": members, "frames": frames, "cout": cout,
                      "cin_total": cin_total, "elapsed": now - t0})
    for sink, (ft, fw) in footers.items():
        fw.close()
        finalize(sink, ft)
        ft.close()
    return members, frames, sorted(footers)


def _footer_payloads(ftmp):
    """The footer bytes to place after a sink's frames. ≤4 GB → an embedded
    zstd skippable frame appended to the archive object; larger → the ipc
    stream for a separate <key>.nock sidecar object. Returns
    (embed_bytes_or_None, sidecar_bytes_or_None)."""
    flen = ftmp.tell()
    ftmp.seek(0)
    body = ftmp.read()
    trailer = struct.pack("<Q", flen) + _footer.MAGIC
    if flen + len(trailer) <= 0xFFFFFFFF:
        return (struct.pack("<II", SKIP_MAGIC, flen + len(trailer))
                + body + trailer), None
    return None, body + trailer


def _zscan(inputs, readers):
    """scan: quiver-exec decompresses + parses each source in C (off the GIL)
    and streams member metadata (path, stat, source_id, ordinal). No bytes
    are compressed — this is the cheap pass that feeds the Polars planner."""
    import subprocess
    import numpy as np
    from ..wire import EXE
    proc = subprocess.Popen([EXE, "zscan", str(readers), *inputs],
                            stdout=subprocess.PIPE, bufsize=1 << 22)
    tail = struct.Struct("<qqiiiii")            # size,mtime,mode,uid,gid,src,ord
    f = proc.stdout
    paths, sizes, mtimes = [], [], []
    modes, uids, gids, srcs, ords = [], [], [], [], []
    buf = b""
    while True:
        chunk = f.read(1 << 22)
        if not chunk:
            break
        buf += chunk
        i, n = 0, len(buf)
        while True:
            if i + 2 > n:
                break
            plen = buf[i] | (buf[i + 1] << 8)
            if i + 2 + plen + 36 > n:
                break
            paths.append(buf[i + 2:i + 2 + plen].decode("utf-8", "surrogateescape"))
            (sz, mt, mo, ui, gi, sr, od) = tail.unpack_from(buf, i + 2 + plen)
            sizes.append(sz); mtimes.append(mt); modes.append(mo)
            uids.append(ui); gids.append(gi); srcs.append(sr); ords.append(od)
            i += 2 + plen + 36
        buf = buf[i:]
    if proc.wait() != 0:
        raise RuntimeError(f"zscan exited {proc.returncode}")
    return pl.DataFrame({
        "source_id": np.array(srcs, dtype=np.int32),
        "ordinal": np.array(ords, dtype=np.int32),
        "path": paths,
        "size": np.array(sizes, dtype=np.int64),
        "mode": np.array(modes, dtype=np.int32),
        "mtime_ns": np.array(mtimes, dtype=np.int64),
        "uid": np.array(uids, dtype=np.int32),
        "gid": np.array(gids, dtype=np.int32)})


def _glob_to_re(g):
    """glob → regex without look-around (Polars' Rust engine rejects the
    look-ahead fnmatch.translate emits). * → .*, ? → ., [..] preserved."""
    import re
    out, i, n = [], 0, len(g)
    while i < n:
        c = g[i]
        if c == "*":
            out.append(".*")
        elif c == "?":
            out.append(".")
        elif c == "[":
            j = i + 1
            if j < n and g[j] in "!^":
                j += 1
            if j < n and g[j] == "]":
                j += 1
            while j < n and g[j] != "]":
                j += 1
            if j >= n:
                out.append(r"\[")
            else:
                cls = g[i + 1:j]
                out.append("[" + ("^" + cls[1:] if cls[:1] == "!" else cls) + "]")
                i = j
        else:
            out.append(re.escape(c))
        i += 1
    return "".join(out)


def _globs_re(globs):
    return "^(?:" + "|".join(_glob_to_re(g) for g in globs) + ")$"


def plan_frames(df, include=None, exclude=None, min_size=0,
                batch_bytes=16 << 20, shard_by=None, shards=None):
    """The plan seam: given scanned member metadata, apply globs/size filters,
    optionally assign each member to a `sink` (attribute-based resharding),
    and pack surviving members into frames (greedy fill by raw tar size,
    within-source stream order, never spanning a source or sink). Returns the
    plan with global `frame` and `sink`. Arbitrary Polars policy lives here.

    shard_by: a pl.Expr over the member columns yielding a sink id, or the
    string "hash" (with `shards=N`) for an even N-way split by path."""
    d = df.sort(["source_id", "ordinal"])
    if min_size:
        d = d.filter(pl.col("size") >= min_size)
    if include:
        d = d.filter(pl.col("path").str.contains(_globs_re(include)))
    if exclude:
        d = d.filter(~pl.col("path").str.contains(_globs_re(exclude)))
    # sink assignment (default: everything to sink 0)
    if shard_by is None:
        d = d.with_columns(pl.lit(0, dtype=pl.Int32).alias("sink"))
    else:
        if isinstance(shard_by, str) and shard_by == "hash":
            if not shards:
                raise ValueError("shards=N required for hash sharding")
            expr = pl.col("path").hash() % shards
        elif isinstance(shard_by, pl.Expr):
            expr = shard_by
        else:
            raise TypeError("shard_by must be a pl.Expr or 'hash'")
        d = d.with_columns(expr.cast(pl.Int32).alias("sink"))
    # raw tar footprint of each member: 512 header + padded body
    d = d.with_columns(
        (512 + ((pl.col("size") + 511) // 512) * 512).alias("raw"))
    # greedy fill within each (source, sink) block, in ordinal order
    d = d.with_columns(
        ((pl.col("raw").cum_sum().over(["source_id", "sink"]) - pl.col("raw"))
         // batch_bytes).alias("lframe"))
    # globally unique frame ids by offsetting each (source, sink) block
    per = (d.group_by(["source_id", "sink"])
           .agg((pl.col("lframe").max() + 1).alias("nf"))
           .sort(["source_id", "sink"]))
    per = per.with_columns((pl.col("nf").cum_sum() - pl.col("nf")).alias("base"))
    d = d.join(per.select(["source_id", "sink", "base"]),
               on=["source_id", "sink"], how="left")
    return d.with_columns(
        (pl.col("lframe") + pl.col("base")).cast(pl.Int32).alias("frame")
    ).sort(["source_id", "ordinal"])


def _write_plan(plan_df, nsrc, path):
    """Plan file for zexec: [i32 nsrc][i32 nsink][i32 counts[nsrc]][ents],
    ents being per-source (i32 ordinal, i32 frame, i32 sink) sorted by ordinal
    in source order — C merge-joins it against the re-parsed stream. Returns
    the sink count."""
    import numpy as np
    d = plan_df.sort(["source_id", "ordinal"])
    nsink = int(d["sink"].max()) + 1 if d.height else 1
    counts = [0] * nsrc
    for sid, c in d.group_by("source_id").agg(pl.len().alias("c")).iter_rows():
        counts[sid] = c
    ords = d["ordinal"].to_numpy().astype(np.int32)
    frames = d["frame"].to_numpy().astype(np.int32)
    sinks = d["sink"].to_numpy().astype(np.int32)
    ent = np.empty(len(ords) * 3, dtype=np.int32)
    ent[0::3] = ords
    ent[1::3] = frames
    ent[2::3] = sinks
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", nsrc, nsink))
        f.write(np.array(counts, dtype=np.int32).tobytes())
        f.write(ent.tobytes())
    return nsink


def _shard_pattern(out_path):
    """Turn an output path into a printf pattern with %d for the sink."""
    if "%d" in out_path:
        return out_path
    if out_path.endswith(".tar.zstd"):
        return out_path[:-len(".tar.zstd")] + ".shard%d.tar.zstd"
    return out_path + ".shard%d"


def _parse_s3(url):
    rest = url[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _s3_upload_sink(client, bucket, key, fifo_path, footer_box, done_evt,
                    part_size=8 << 20):
    """Stream one sink's frames from its FIFO straight into an S3 multipart
    upload; when the frames end, append the footer (an embedded skippable
    frame, or a separate <key>.nock object for >4 GB) and complete. Runs off
    C's own output — no local staging of the 2 TB body."""
    mpu = client.create_multipart_upload(Bucket=bucket, Key=key)
    uid = mpu["UploadId"]
    parts, pnum, buf = [], 1, bytearray()

    def put_part(data):
        nonlocal pnum
        r = client.upload_part(Bucket=bucket, Key=key, UploadId=uid,
                               PartNumber=pnum, Body=bytes(data))
        parts.append({"ETag": r["ETag"], "PartNumber": pnum})
        pnum += 1
    try:
        with open(fifo_path, "rb") as fr:              # blocks until C opens it
            while (chunk := fr.read(1 << 20)):
                buf += chunk
                while len(buf) >= part_size:           # non-last parts ≥ 5 MB
                    put_part(buf[:part_size]); del buf[:part_size]
        done_evt.wait()                                # footer now available
        embed, sidecar = footer_box[0]
        if embed:
            buf += embed
        while len(buf) >= part_size:
            put_part(buf[:part_size]); del buf[:part_size]
        if buf or not parts:
            put_part(buf)                              # final (any size) part
        client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=uid,
            MultipartUpload={"Parts": parts})
        if sidecar is not None:
            client.put_object(Bucket=bucket, Key=key + ".nock", Body=sidecar)
    except Exception:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)
        raise


def _recompress_s3(inputs, s3_url, level, batch_bytes, readers, compressors,
                   progress, progress_every, include, exclude, min_size,
                   shard_by, shards, cin_total):
    """Reshard/recompress with the outputs streamed straight into S3: C writes
    each sink's frames sequentially to a FIFO, a per-sink uploader thread
    multipart-uploads them, and the footer is appended once the frames end."""
    import subprocess
    import threading
    import shutil
    import boto3
    from ..wire import EXE
    bucket, key = _parse_s3(s3_url)
    planned = include or exclude or min_size or shard_by is not None
    tmpplan = None
    if planned:
        plan = plan_frames(_zscan(inputs, readers), include, exclude,
                           min_size, batch_bytes, shard_by, shards)
        with tempfile.NamedTemporaryFile(suffix=".plan", delete=False) as pf:
            tmpplan = pf.name
        nsink = _write_plan(plan, len(inputs), tmpplan)
        present = (sorted({int(x) for x in plan["sink"].unique()})
                   if plan.height else [])
    else:
        nsink, present = 1, [0]
    sink_key = ((lambda s: _shard_pattern(key) % s) if nsink > 1
                else (lambda s: key))

    tmpdir = tempfile.mkdtemp(prefix="quiver-s3-")
    fifopat = os.path.join(tmpdir, "s%d.fifo")
    client = boto3.client("s3")
    boxes = {s: [None] for s in present}
    evts = {s: threading.Event() for s in present}
    threads = {}
    for s in present:
        os.mkfifo(fifopat % s)
        t = threading.Thread(target=_s3_upload_sink, daemon=True,
                             args=(client, bucket, sink_key(s), fifopat % s,
                                   boxes[s], evts[s]))
        t.start(); threads[s] = t

    def finalize(sink, ftmp):
        boxes[sink][0] = _footer_payloads(ftmp)
        evts[sink].set()
    try:
        if planned:
            cmd = [EXE, "zexec", tmpplan, fifopat, str(level),
                   str(readers), str(compressors), *inputs]
        else:
            cmd = [EXE, "zpack", fifopat % 0, str(level), str(batch_bytes),
                   str(readers), str(compressors), *inputs]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1 << 22)
        members, frames, _ = _ingest_footer(proc.stdout, finalize, progress,
                                            progress_every, cin_total)
        rc = proc.wait()
        for s in present:                      # unblock any sink with no records
            if not evts[s].is_set():
                boxes[s][0] = (b"", None); evts[s].set()
        for t in threads.values():
            t.join()
        if rc != 0:
            raise RuntimeError(f"exec exited {rc}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        if tmpplan:
            os.unlink(tmpplan)
    return Result(members=members, frames=frames)


def recompress_c(inputs, out_path: str, level: int = 6,
                 batch_bytes: int = 16 << 20, readers: int = 8,
                 compressors: int | None = None, progress=None,
                 progress_every: float = 2.0, _force_sidecar: bool = False,
                 include=None, exclude=None, min_size: int = 0,
                 shard_by=None, shards=None) -> Result:
    """The fold. With no filter or shard, the fused `zpack` fast path
    decompresses, parses, batches, compresses and appends all in C (option A).
    With any glob/size filter or a shard, it splits into scan → Polars plan →
    exec (option B): the plan seam where filters, frame policy and sink
    routing live, at the cost of one extra decompress-only pass (~3% of
    level-10 compress). Sharding fans out to `out_path` templated with the
    sink id ('{shard}'/'%d', else `.shardN` inserted) in a single pass —
    each shard a self-contained tar.zstd (+ .nock)."""
    import subprocess
    from ..wire import EXE
    compressors = compressors or (os.cpu_count() or 8)
    cin_total = sum(os.path.getsize(p) for p in inputs)

    if out_path.startswith("s3://"):                        # stream into S3
        return _recompress_s3(inputs, out_path, level, batch_bytes, readers,
                              compressors, progress, progress_every, include,
                              exclude, min_size, shard_by, shards, cin_total)

    planned = include or exclude or min_size or shard_by is not None

    if planned:                                             # B: scan → plan → exec
        df = _zscan(inputs, readers)
        plan = plan_frames(df, include, exclude, min_size, batch_bytes,
                           shard_by, shards)
        with tempfile.NamedTemporaryFile(suffix=".plan", delete=False) as pf:
            plan_path = pf.name
        nsink = _write_plan(plan, len(inputs), plan_path)
        pattern = _shard_pattern(out_path) if nsink > 1 else out_path
        sink_path = ((lambda s: pattern % s) if nsink > 1
                     else (lambda s: out_path))

        def fin(sink, ftmp):
            _write_footer(sink_path(sink), ftmp, _force_sidecar)

        def cout(footers):
            return sum(os.path.getsize(sink_path(s)) for s in footers
                       if os.path.exists(sink_path(s)))
        try:
            proc = subprocess.Popen(
                [EXE, "zexec", plan_path, pattern, str(level),
                 str(readers), str(compressors), *inputs],
                stdout=subprocess.PIPE, bufsize=1 << 22)
            members, frames, _ = _ingest_footer(
                proc.stdout, fin, progress, progress_every, cin_total, cout)
            if proc.wait() != 0:
                raise RuntimeError(f"zexec exited {proc.returncode}")
        finally:
            os.unlink(plan_path)
        return Result(members=members, frames=frames)

    proc = subprocess.Popen(                                 # A: fused fast path
        [EXE, "zpack", out_path, str(level), str(batch_bytes),
         str(readers), str(compressors), *inputs],
        stdout=subprocess.PIPE, bufsize=1 << 22)
    members, frames, _ = _ingest_footer(
        proc.stdout, lambda sink, ftmp: _write_footer(out_path, ftmp,
                                                      _force_sidecar),
        progress, progress_every, cin_total,
        lambda footers: (os.path.getsize(out_path)
                         if os.path.exists(out_path) else 0))
    if proc.wait() != 0:
        raise RuntimeError(f"zpack exited {proc.returncode}")
    return Result(members=members, frames=frames)


def read_index(path: str) -> pl.DataFrame:
    """Footer frame (member → frame + in-frame offset), read from the
    streamed IPC footer via pupyarrow (concatenating its batches)."""
    data = _footer_bytes(path)
    dfs = [pl.DataFrame(b) for b in StreamReader(io.BytesIO(data))]
    return (pl.concat(dfs) if dfs else pl.DataFrame(schema=_ZF_SCHEMA))


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

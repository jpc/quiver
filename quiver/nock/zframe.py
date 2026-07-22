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


_ZMETA_SCHEMA = {"path": pl.String, "source_id": pl.Int32, "ordinal": pl.Int32,
                 "size": pl.Int64, "mode": pl.Int32, "mtime_ns": pl.Int64,
                 "uid": pl.Int32, "gid": pl.Int32}


def _zscan(inputs, readers):
    """scan: quiver-exec decompresses + parses each source in C (off the GIL)
    and streams member metadata as ZMETA Arrow-IPC batches — read here through
    the same StreamReader the planner uses for scan. No bytes are compressed;
    this is the cheap pass that feeds the Polars planner."""
    import subprocess
    from ..wire import EXE, _to_pl
    from ..pupyarrow.writer import StreamReader
    proc = subprocess.Popen([EXE, "zscan", str(readers), *inputs],
                            stdout=subprocess.PIPE, bufsize=1 << 22)
    dfs = [_to_pl(b) for b in StreamReader(proc.stdout)]
    if proc.wait() != 0:
        raise RuntimeError(f"zscan exited {proc.returncode}")
    return pl.concat(dfs) if dfs else pl.DataFrame(schema=_ZMETA_SCHEMA)


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


def _write_plan(plan_df, nsrc, path, nsink=None):
    """Write the plan as an OP_COMPRESS command stream (Arrow IPC, CMD schema),
    sorted by (source_id, ordinal). The four plan operands ride existing command
    columns — source_id→data_offset, ordinal→size, frame→dep_group,
    sink→parent_row — and zexec reads it with the normal command reader. Returns
    the sink count (nsink and resume offsets are execution params, passed on
    argv, not in the stream)."""
    import zstandard as zstd
    from ..wire import cmd_df, _df_cols, CMD_SCHEMA, OP_COMPRESS
    from ..pupyarrow.writer import StreamWriter
    d = plan_df.sort(["source_id", "ordinal"])
    if nsink is None:
        nsink = int(d["sink"].max()) + 1 if d.height else 1
    n = d.height
    # whole-stream zstd: the command word is wide but sparse (an OP_COMPRESS row
    # uses 5 of 15 columns), so the constant/zero buffers collapse across the
    # stream — far better than per-buffer compression. zexec streams-decompresses.
    with open(path, "wb") as f, \
            zstd.ZstdCompressor(level=3).stream_writer(f, closefd=False) as zf:
        w = StreamWriter(zf, CMD_SCHEMA)
        if n:
            cmds = cmd_df(n).with_columns(
                pl.lit(OP_COMPRESS, dtype=pl.UInt8).alias("opcode"),
                d["source_id"].cast(pl.Int64).alias("data_offset"),
                d["ordinal"].cast(pl.Int64).alias("size"),
                d["frame"].cast(pl.Int64).alias("dep_group"),
                d["sink"].cast(pl.Int64).alias("parent_row"))
            for s in range(0, n, 1 << 20):
                w.write_batch(_df_cols(cmds[s:s + (1 << 20)]))
        w.close()
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


def _retry(fn, attempts=5, base=0.5):
    """Retry a transient S3 op with exponential backoff."""
    import time
    for i in range(attempts):
        try:
            return fn()
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(base * (2 ** i))


def _make_submit(executor, sem):
    """Submit an upload task bounded by `sem` in-flight slots — this is the
    backpressure: when the cap is reached, submit() blocks, the sink stops
    draining its FIFO, and C blocks writing that sink (per-sink lock, so the
    others keep going). Memory is bounded to sem_size × part_size globally."""
    def submit(fn):
        sem.acquire()

        def wrapped():
            try:
                return fn()
            finally:
                sem.release()
        return executor.submit(wrapped)
    return submit


def _s3_upload_sink(client, bucket, key, fifo_path, footer_box, done_evt,
                    submit, part_size=8 << 20, retries=5):
    """Stream one sink's frames from its FIFO straight into an S3 multipart
    upload; when the frames end, append the footer (an embedded skippable
    frame, or a separate <key>.nock object for >4 GB) and complete. Parts are
    uploaded through a bounded, retrying pool so reads and uploads overlap
    without unbounded memory. No local staging of the body."""
    mpu = _retry(lambda: client.create_multipart_upload(Bucket=bucket, Key=key),
                 retries)
    uid = mpu["UploadId"]
    futures, pnum, buf = [], 1, bytearray()

    def send(data, n):                                 # S3 assembles by number,
        futures.append((n, submit(lambda: _retry(     # so out-of-order is fine
            lambda: client.upload_part(
                Bucket=bucket, Key=key, UploadId=uid,
                PartNumber=n, Body=data), retries)["ETag"])))
    try:
        with open(fifo_path, "rb") as fr:              # blocks until C opens it
            while (chunk := fr.read(1 << 20)):
                buf += chunk
                while len(buf) >= part_size:           # non-last parts ≥ 5 MB
                    send(bytes(buf[:part_size]), pnum); pnum += 1
                    del buf[:part_size]
        done_evt.wait()                                # footer now available
        embed, sidecar = footer_box[0]
        if embed:
            buf += embed
        while len(buf) >= part_size:
            send(bytes(buf[:part_size]), pnum); pnum += 1; del buf[:part_size]
        if buf or not futures:
            send(bytes(buf), pnum); pnum += 1          # final (any size) part
        parts = [{"ETag": fut.result(), "PartNumber": n}
                 for n, fut in sorted(futures)]
        _retry(lambda: client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=uid,
            MultipartUpload={"Parts": parts}), retries)
        if sidecar is not None:
            _retry(lambda: client.put_object(
                Bucket=bucket, Key=key + ".nock", Body=sidecar), retries)
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)
        except Exception:
            pass
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
    from concurrent.futures import ThreadPoolExecutor
    import boto3
    from ..wire import EXE
    bucket, key = _parse_s3(s3_url)
    inflight = max(4, (compressors or 8) // 4)     # bounded parts in flight
    executor = ThreadPoolExecutor(max_workers=inflight)
    submit = _make_submit(executor, threading.Semaphore(inflight))
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
                                   boxes[s], evts[s], submit))
        t.start(); threads[s] = t

    def finalize(sink, ftmp):
        boxes[sink][0] = _footer_payloads(ftmp)
        evts[sink].set()
    try:
        if planned:
            cmd = [EXE, "zexec", tmpplan, fifopat, str(level),
                   str(readers), str(compressors), str(nsink), "-", *inputs]
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
        executor.shutdown(wait=True)
        shutil.rmtree(tmpdir, ignore_errors=True)
        if tmpplan:
            os.unlink(tmpplan)
    return Result(members=members, frames=frames)


_WAL_REC = struct.Struct("<qqiiiiqqqi")             # matches the 60B footer tail


def _wal_iter(path):
    """Stream committed footer records from the WAL (raw 60B-tail records, the
    exact bytes zexec emits). Yields (path, size, mtime, mode, uid, gid, frame,
    coff, clen, in_off, sink). A torn trailing record is ignored."""
    buf = b""
    with open(path, "rb") as f:
        while (chunk := f.read(1 << 20)):
            buf += chunk
            i, n = 0, len(buf)
            while i + 2 <= n:
                plen = buf[i] | (buf[i + 1] << 8)
                if i + 2 + plen + 60 > n:
                    break
                p = buf[i + 2:i + 2 + plen].decode("utf-8", "surrogateescape")
                yield (p,) + _WAL_REC.unpack_from(buf, i + 2 + plen)
                i += 2 + plen + 60
            buf = buf[i:]


def _ingest_wal(f, walf, progress, progress_every, cin_total):
    """Persist each newly committed record to the WAL (fsync'd on the progress
    tick — a frame's bytes are already in the sink before its record, so the
    WAL is the source of truth for what's durable). Returns new (members)."""
    import time
    buf = b""
    members = 0
    t0 = last = time.time()
    while (chunk := f.read(1 << 20)):
        buf += chunk
        i, n = 0, len(buf)
        while i + 2 <= n:
            plen = buf[i] | (buf[i + 1] << 8)
            if i + 2 + plen + 60 > n:
                break
            walf.write(buf[i:i + 2 + plen + 60])
            members += 1
            i += 2 + plen + 60
        buf = buf[i:]
        now = time.time()
        if now - last >= progress_every:
            last = now
            walf.flush(); os.fsync(walf.fileno())
            if progress:
                progress({"members": members, "frames": 0, "cout": 0,
                          "cin_total": cin_total, "elapsed": now - t0})
    return members


def _wal_finalize(wal_path, sink_path, force_sidecar):
    """Build each sink's footer from the full WAL (committed + newly appended)
    and write it. Returns (members, frames, sinks)."""
    foot = {}
    members, maxframe = 0, -1
    for (p, size, mtime, mode, uid, gid, frame, coff, clen, in_off, sink) \
            in _wal_iter(wal_path):
        if sink not in foot:
            ft = tempfile.TemporaryFile()
            foot[sink] = (ft, _FooterStream(ft))
        foot[sink][1].add(p, size, mode, mtime, uid, gid,
                          frame, coff, clen, in_off)
        members += 1
        if frame > maxframe:
            maxframe = frame
    for sink, (ft, fw) in foot.items():
        fw.close()
        _write_footer(sink_path(sink), ft, force_sidecar)
        ft.close()
    return members, maxframe + 1, sorted(foot)


def _recompress_wal(inputs, out_path, level, batch_bytes, readers, compressors,
                    progress, progress_every, force_sidecar, include, exclude,
                    min_size, shard_by, shards, wal_path, cin_total):
    """WAL-resumable recompress (local). The plan is deterministic, so on
    resume we re-scan/re-plan, drop every already-committed frame, and tell
    zexec to append after each sink's high-water — re-decompressing to fast-
    forward (cheap) while skipping the compression that's already done."""
    import subprocess
    from ..wire import EXE
    plan = plan_frames(_zscan(inputs, readers), include, exclude, min_size,
                       batch_bytes, shard_by, shards)
    nsink = int(plan["sink"].max()) + 1 if plan.height else 1
    pattern = _shard_pattern(out_path) if nsink > 1 else out_path
    sink_path = (lambda s: pattern % s) if nsink > 1 else (lambda s: out_path)

    done_frames, hw = set(), {}
    if os.path.exists(wal_path):                    # resume
        for r in _wal_iter(wal_path):
            done_frames.add(r[6])
            end = r[7] + r[8]                       # coff + clen
            if end > hw.get(r[10], 0):
                hw[r[10]] = end
        if done_frames:
            plan = plan.filter(~pl.col("frame").is_in(list(done_frames)))
    with tempfile.NamedTemporaryFile(suffix=".plan", delete=False) as pf:
        plan_path = pf.name
    _write_plan(plan, len(inputs), plan_path, nsink=nsink)
    starts = ",".join(str(hw.get(s, 0)) for s in range(nsink))  # resume offsets

    walf = open(wal_path, "ab")
    try:
        proc = subprocess.Popen(
            [EXE, "zexec", plan_path, pattern, str(level),
             str(readers), str(compressors), str(nsink), starts, *inputs],
            stdout=subprocess.PIPE, bufsize=1 << 22)
        _ingest_wal(proc.stdout, walf, progress, progress_every, cin_total)
        rc = proc.wait()
        walf.flush(); os.fsync(walf.fileno())
    finally:
        walf.close()
        os.unlink(plan_path)
    if rc != 0:
        raise RuntimeError(f"zexec exited {rc} (WAL kept at {wal_path})")
    members, frames, _ = _wal_finalize(wal_path, sink_path, force_sidecar)
    os.unlink(wal_path)                             # retire WAL on success
    return Result(members=members, frames=frames)


def recompress_c(inputs, out_path: str, level: int = 6,
                 batch_bytes: int = 16 << 20, readers: int = 8,
                 compressors: int | None = None, progress=None,
                 progress_every: float = 2.0, _force_sidecar: bool = False,
                 include=None, exclude=None, min_size: int = 0,
                 shard_by=None, shards=None, wal=None) -> Result:
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

    if wal is not None:                                     # WAL-resumable (local)
        if out_path.startswith("s3://"):
            raise NotImplementedError("WAL resume is local-only for now")
        return _recompress_wal(inputs, out_path, level, batch_bytes, readers,
                               compressors, progress, progress_every,
                               _force_sidecar, include, exclude, min_size,
                               shard_by, shards, wal, cin_total)

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
                 str(readers), str(compressors), str(nsink), "-", *inputs],
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


_MERGED_IPC = _FOOTER_IPC + [("shard_id", "i32")]


def _schema_cols(df, schema):
    """Columns of `df` in `schema` order, in the form StreamWriter wants
    (lists for strings/binary, numpy for fixed-width)."""
    return [df[name].to_list() if t in ("large_string", "large_binary")
            else df[name].to_numpy() for name, t in schema]


def merge(shards, out_path: str) -> int:
    """The distributed reduce, logical (zero-copy): join N shard footers into
    one index tagged with shard_id, keeping each shard's frame offsets local.
    No byte movement — the merged archive is the shard files plus this manifest
    (shard paths + the joined footer). Byte-indistinguishable, on extract, from
    a single-node run over the union of the sources. Returns member count."""
    parts, fbase = [], 0
    for k, shard in enumerate(shards):
        idx = read_index(shard)
        parts.append(idx.with_columns(
            (pl.col("frame") + fbase).cast(pl.Int32).alias("frame"),
            pl.lit(k, dtype=pl.Int32).alias("shard_id")))
        fbase += int(idx["frame"].n_unique())
    merged = (pl.concat(parts) if parts
              else pl.DataFrame(schema={**_ZF_SCHEMA, "shard_id": pl.Int32}))
    header = ("\n".join(os.path.abspath(s) for s in shards) + "\n").encode()
    with open(out_path, "wb") as f:                 # [u32 hlen][paths][footer ipc]
        f.write(struct.pack("<I", len(header)))
        f.write(header)
        w = StreamWriter(f, _MERGED_IPC)
        for s in range(0, merged.height, 1 << 20):
            w.write_batch(_schema_cols(merged[s:s + (1 << 20)], _MERGED_IPC))
        w.close()
    return merged.height


def read_merged(out_path: str):
    """Return (shard_paths, joined_index) from a merge manifest."""
    with open(out_path, "rb") as f:
        (hlen,) = struct.unpack("<I", f.read(4))
        shards = [ln for ln in f.read(hlen).decode().split("\n") if ln]
        dfs = [pl.DataFrame(b) for b in StreamReader(f)]
    idx = (pl.concat(dfs) if dfs
           else pl.DataFrame(schema={**_ZF_SCHEMA, "shard_id": pl.Int32}))
    return shards, idx


def extract_merged(out_path: str, dest: str, predicate=None):
    """Extract from a merged shard-set: resolve each member's frame in its own
    shard file, decompress it once, slice — the same loop as extract() with a
    shard_id lookup in front."""
    shards, idx = read_merged(out_path)
    if predicate is not None:
        idx = idx.filter(predicate)
    dctx = zstd.ZstdDecompressor()
    fh, out = {}, []
    for (sid, coff, clen), grp in idx.group_by(
            ["shard_id", "frame_coff", "frame_clen"], maintain_order=True):
        f = fh.get(sid) or fh.setdefault(sid, open(shards[sid], "rb"))
        f.seek(coff)
        raw = dctx.decompress(f.read(clen))
        for r in grp.iter_rows(named=True):
            data = raw[r["in_off"]: r["in_off"] + r["size"]]
            p = os.path.join(dest, r["path"])
            os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
            with open(p, "wb") as w:
                w.write(data)
            os.chmod(p, r["mode"] & 0o7777)
            out.append(r["path"])
    for f in fh.values():
        f.close()
    return out


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


def _unpack(idx, dest, shard_of, workers):
    """Parallel unpack: decode each frame once in a thread pool — both the zstd
    decode and the file writes release the GIL, so this genuinely parallelizes
    — and scatter its members to files (the extract() loop, fanned out). The C
    OP_EXTRACT+ZSTD_D path (docs/UNPACK.md) is the max-throughput successor;
    this is the portable version. `shard_of(shard_id)` resolves the source file
    — a constant for a linear nock, the shard list for a sharded one."""
    workers = workers or (os.cpu_count() or 8)
    has_shard = "shard_id" in idx.columns
    keys = (["shard_id"] if has_shard else []) + ["frame_coff", "frame_clen"]
    tl = threading.local()

    def do(key, grp):
        sid, coff, clen = key if has_shard else (0, key[0], key[1])
        if not hasattr(tl, "d"):
            tl.d = zstd.ZstdDecompressor()
        with open(shard_of(sid), "rb") as f:
            f.seek(coff)
            raw = tl.d.decompress(f.read(clen))        # one frame, GIL released
        for r in grp.iter_rows(named=True):
            p = os.path.join(dest, r["path"])
            parent = os.path.dirname(p)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(p, "wb") as w:
                w.write(raw[r["in_off"]: r["in_off"] + r["size"]])
            os.chmod(p, r["mode"] & 0o7777)
            os.utime(p, ns=(r["mtime_ns"], r["mtime_ns"]))
        return grp.height

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(do, k, g)
                for k, g in idx.group_by(keys, maintain_order=False)]
        return sum(f.result() for f in futs)


def _unpack_plan(idx, dest, shard_of=None):
    """Compile a nock index into the buffer-machine command stream (docs/ISA.md
    §2-§4): MKDIR epoch, then a decode epoch of INFLATE-headed groups, then a
    SETMETA epoch. Each group is one INFLATE row (decode the frame from
    [frame_coff, frame_clen] into a worker buffer; header_offset carries K =
    member count) immediately followed by its K EXTRACT rows, each scattering
    buffer slice [in_off, +size] to a file. `shard_of` (shard_id→file) makes it
    sharded: the INFLATE's `path` names the shard file to read the frame from
    (§10.5), and frames are keyed/sorted by (shard_id, frame_coff) since offsets
    repeat across shards. Returns (cmds, group start-indices, paths)."""
    from ..wire import OP_INFLATE, OP_EXTRACT, OP_MKDIR, OP_SETMETA, cmd_df
    sharded = shard_of is not None and "shard_id" in idx.columns
    fkeys = (["shard_id"] if sharded else []) + ["frame_coff"]
    idx = idx.sort(fkeys + ["in_off"]).with_columns(
        _fi=(pl.struct(fkeys).rle_id() if sharded
             else pl.col("frame_coff").rle_id()))     # 0..F-1 (sorted ⇒ runs)
    idx = idx.with_columns(_mp=pl.int_range(pl.len()).over("_fi"))  # pos in frame

    dirs = (idx.select(dir=pl.col("path").str.extract(r"^(.*)/", 1))
               .drop_nulls().unique())
    seen, alld = set(), []                    # mkdir -p every ancestor
    for d in dirs["dir"]:
        parts = d.split("/")
        for k in range(1, len(parts) + 1):
            pd = "/".join(parts[:k])
            if pd not in seen:
                seen.add(pd); alld.append(pd)
    alld.sort(key=lambda d: d.count("/"))
    maxd = max((d.count("/") for d in alld), default=-1)
    dg = maxd + 1
    n = idx.height

    frames = (idx.group_by(fkeys + ["frame_clen"], maintain_order=True)
                 .agg(k=pl.len()))
    nF = frames.height
    # sharded: INFLATE selects its source by shard_id in pad_align; the shard
    # files are opened once at startup (passed to the executor on argv).
    sid = (frames["shard_id"].cast(pl.Int64) if sharded
           else pl.Series([0] * nF, dtype=pl.Int64))
    inflate = cmd_df(nF, opcode=[OP_INFLATE] * nF,
                     dep_group=pl.Series([dg] * nF, dtype=pl.Int64),
                     pad_align=sid,
                     data_offset=frames["frame_coff"], size=frames["frame_clen"],
                     header_offset=frames["k"]).with_columns(
        _fi=pl.int_range(nF, dtype=pl.Int64),
        _sub=pl.lit(0, dtype=pl.Int64), _mp=pl.lit(0, dtype=pl.Int64))
    members = cmd_df(n, opcode=[OP_EXTRACT] * n,
                     dep_group=pl.Series([dg] * n, dtype=pl.Int64),
                     path=[os.path.join(dest, p) for p in idx["path"]],
                     header_offset=idx["in_off"], size=idx["size"],
                     mode=(idx["mode"] & 0o7777).cast(pl.Int32)).with_columns(
        _fi=idx["_fi"].cast(pl.Int64),
        _sub=pl.lit(1, dtype=pl.Int64), _mp=idx["_mp"].cast(pl.Int64))
    decode = (pl.concat([inflate, members])
                .sort(["_fi", "_sub", "_mp"]).drop(["_fi", "_sub", "_mp"]))

    mkdir = cmd_df(len(alld), opcode=[OP_MKDIR] * len(alld),
                   dep_group=pl.Series([d.count("/") for d in alld],
                                       dtype=pl.Int64),
                   path=[os.path.join(dest, d) for d in alld])
    setmeta = cmd_df(n, opcode=[OP_SETMETA] * n,
                     dep_group=pl.Series([maxd + 2] * n, dtype=pl.Int64),
                     path=[os.path.join(dest, p) for p in idx["path"]],
                     mtime_ns=idx["mtime_ns"])
    cmds = pl.concat([mkdir, decode, setmeta]).with_columns(
        user_data=pl.int_range(len(alld) + nF + 2 * n, dtype=pl.UInt64))
    # valid chunk starts: any row that isn't a decode-group member (EXTRACT).
    boundaries = np.flatnonzero(cmds["opcode"].to_numpy() != OP_EXTRACT)
    return cmds, boundaries, idx["path"].to_list()


def _unpack_exec(idx, dest, afd_path, shard_of, engine, batch_rows):
    """Drive the decode-group plan through one executor. afd_path is the fd the
    executor opens for the linear case; for the sharded case the shard files are
    passed as `sources` and opened once at startup, selected per-frame by
    shard_id (docs/ISA.md §10.5)."""
    from ..wire import PipeExecutor
    from ..tools import _check
    cmds, boundaries, paths = _unpack_plan(idx, dest, shard_of)
    ex = PipeExecutor(afd_path, engine=engine,
                      sources=list(shard_of) if shard_of is not None else None)
    try:
        comp = ex.execute(cmds, batch_rows=batch_rows, boundaries=boundaries)
    finally:
        assert ex.close() == 0
    _check(comp, cmds)
    return len(paths)


def unpack(path, dest, predicate=None, workers=None, engine="auto",
           batch_rows=4096):
    """Parallel unpack of a linear nock archive → dest. engine=None keeps the
    portable Python thread-pool decoder (the oracle); otherwise members are
    decoded through the executor's buffered decode groups (INFLATE + scatter,
    docs/ISA.md) — one decode per frame, no cache. `batch_rows` bounds the
    Arrow batch; group-boundary chunking keeps each frame's group whole."""
    idx = read_index(path)
    if predicate is not None:
        idx = idx.filter(predicate)
    os.makedirs(dest, exist_ok=True)
    if engine is None or not idx.height:
        return _unpack(idx, dest, lambda sid: path, workers)
    return _unpack_exec(idx, dest, path, None, engine, batch_rows)


def recompress_stream(inputs, out_path, level=10, window_bytes=256 << 20,
                      frame_bytes=16 << 20, compressors=None, readers=None):
    """One-pass planned recompress via the `zstream` port (docs/ISA.md §5) — the
    last de-fork. C decompresses each source ONCE into a live buffer (a large
    `window`) and yields member metadata (ZMETA); this driver plans the window
    into `frame_bytes`-sized frames and sends the plan back (PLAN); C's
    compressor pool compresses the planned slices from the *same* buffer in
    parallel (keeping the sink fed) and returns (frame→coff,clen) as COMP. The
    footer is built from ZMETA + plan + COMP — the 60-byte record retired, both
    directions on standard schemas. in_off is computed exactly from buf_span
    (C's per-member buffer span, which absorbs interleaved dir/PAX/GNU blocks).

    Single reader + synchronous per-window (parallel compress within a window);
    multi-reader parallel decode + async double-buffering are follow-ups."""
    import subprocess
    from ..wire import EXE, CMD_SCHEMA, cmd_df, _df_cols, OP_COMPRESS, _to_pl
    compressors = compressors or (os.cpu_count() or 8)
    readers = readers or min(24, len(inputs))          # one decode stream/source
    r_comp, w_comp = os.pipe()
    argv = [EXE, "zstream", str(w_comp), out_path, str(level), str(window_bytes),
            str(compressors), str(readers), *[str(i) for i in inputs]]
    proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            pass_fds=(w_comp,))
    os.close(w_comp)
    zmeta = StreamReader(proc.stdout)
    comp_f = os.fdopen(r_comp, "rb")
    comp = StreamReader(comp_f)
    plan_w = StreamWriter(proc.stdin, CMD_SCHEMA)
    proc.stdin.flush()

    ftmp = tempfile.TemporaryFile()
    fw = _FooterStream(ftmp)
    base = 0                                        # global frame id offset
    instr_path = os.environ.get("QUIVER_TRACE_INSTR")   # capture a sample window
    for zb in zmeta:                                # one batch per window
        m = pl.DataFrame(zb)
        n = m.height
        if not n:
            continue
        # Assign members to frames within the window: greedy fill by buffer
        # span, cutting every frame_bytes. Frame id (dep_group) must be locally
        # 0..F-1 (C groups the PLAN by it) but globally unique in the footer.
        span = m["buf_span"]
        entry_off = span.cum_sum() - span                  # offset within window
        lframe = (entry_off // frame_bytes).cast(pl.Int64) # 0..F-1
        padbody = ((m["size"] + 511) // 512) * 512
        m = m.with_columns(_lf=lframe, _eo=entry_off, _pb=padbody, _sp=span)
        m = m.with_columns(_fs=pl.col("_eo").min().over("_lf"))  # frame start
        # in_off is relative to the member's FRAME: (entry within frame) + hdrlen,
        # where hdrlen = span - padded_body (body sits at the end of the region).
        in_off = (m["_eo"] - m["_fs"] + m["_sp"] - m["_pb"]).to_list()
        lf = m["_lf"].to_list()
        cmds = cmd_df(n, opcode=[OP_COMPRESS] * n, dep_group=m["_lf"])
        plan_w.write_batch(_df_cols(cmds)); proc.stdin.flush()
        c = _to_pl(comp.read_batch())                      # COMP: per-frame
        # COMP rows: user_data = local frame id, read_size = coff, cksum = clen
        ud = c["user_data"].to_list(); co = c["read_size"].to_list()
        cl = c["cksum"].to_list()
        coff = {int(u): int(o) for u, o in zip(ud, co)}
        clen = {int(u): int(x) for u, x in zip(ud, cl)}
        paths = m["path"].to_list(); szs = m["size"].to_list()
        modes = m["mode"].to_list(); mts = m["mtime_ns"].to_list()
        uids = m["uid"].to_list(); gids = m["gid"].to_list()
        # instruction-stream preview: capture the three streams for one window
        # (truncated) — the ZMETA in, PLAN out, and COMP back, as the executor
        # and planner actually exchanged them.
        if instr_path and base:                     # skip window 0 (tiny warmup)
            import json as _json
            K = 12
            frames_out = sorted(coff)
            _json.dump({
                "window": {"members": n, "frames": len(coff),
                           "frame_bytes": frame_bytes},
                "zmeta": {"cols": ["path", "size", "mode", "mtime_ns",
                                   "uid", "gid", "buf_span"],
                          "rows": [[paths[i], szs[i], modes[i], mts[i],
                                    uids[i], gids[i], int(span[i])]
                                   for i in range(min(K, n))], "total": n},
                "plan": {"cols": ["opcode", "member", "→ frame (dep_group)",
                                  "in_off"],
                         "rows": [["COMPRESS", i, base + lf[i], in_off[i]]
                                  for i in range(min(K, n))], "total": n},
                "comp": {"cols": ["frame", "coff", "clen"],
                         "rows": [[base + f, coff[f], clen[f]]
                                  for f in frames_out[:K]],
                         "total": len(coff)},
            }, open(instr_path, "w"), indent=0)
            instr_path = None                       # once
        for i in range(n):
            f = lf[i]
            fw.add(paths[i], szs[i], modes[i], mts[i], uids[i], gids[i],
                   base + f, coff[f], clen[f], in_off[i])
        base += len(coff)
    try:
        proc.stdin.close()
    except BrokenPipeError:
        pass
    comp_f.close()
    fw.close()
    _write_footer(out_path, ftmp, False); ftmp.close()
    assert proc.wait() == 0
    return Result(members=fw.members, frames=base)


def unpack_distributed(path, dest, transports, predicate=None, engine="auto",
                       batch_rows=4096):
    """Distributed unpack (docs/ISA.md §10.5, UNPACK.md): partition the frame
    set across N executors and decode in parallel — no reduce, since each
    member scatters to its own dest file (disjoint outputs on shared storage).
    Frames are the unit (a decode group can't split across nodes); round-robin
    them across transports, balanced by frame. Sharded nock passes every shard
    as a source to each node (weka-shared; a node reads only its frames' shards;
    shard_id stays a global index). Returns total members unpacked."""
    is_manifest = str(path).endswith(".nockset")
    if is_manifest:
        shards, idx = read_merged(path); shard_of = list(shards); afd = "-"
    else:
        idx = read_index(path); shard_of = None; afd = path
    if predicate is not None:
        idx = idx.filter(predicate)
    os.makedirs(dest, exist_ok=True)
    n = len(transports)
    if not idx.height:
        return 0
    fkeys = (["shard_id"] if shard_of else []) + ["frame_coff"]
    idx = idx.sort(fkeys).with_columns(
        (pl.struct(fkeys).rle_id() % n).alias("_node"))   # frame → node
    parts = idx.partition_by("_node", as_dict=True)

    def run_one(k):
        sub = parts.get((k,))
        if sub is None or not sub.height:
            return 0
        from ..tools import _check
        cmds, boundaries, paths = _unpack_plan(sub.drop("_node"), dest, shard_of)
        ex = transports[k].executor(afd, engine=engine,
                                    sources=shard_of if shard_of else None)
        try:
            comp = ex.execute(cmds, batch_rows=batch_rows, boundaries=boundaries)
        finally:
            assert ex.close() == 0
        _check(comp, cmds)
        return len(paths)

    with cf.ThreadPoolExecutor(max_workers=n) as pool:
        return sum(pool.map(run_one, range(n)))


def _pack_fs_scan(root, batch_bytes, predicate, engine, threads):
    """Shared front-end: scan the tree, filter, greedy-fill frames by raw tar
    footprint (512 header + padded body), within path order."""
    from .. import wire
    df = wire.scan(root, engine=engine or "auto", threads=threads)
    files = df.filter(~pl.col("is_dir"))
    if predicate is not None:
        files = files.filter(predicate)
    files = files.sort("path").with_columns(
        (512 + ((pl.col("size") + 511) // 512) * 512).alias("raw"))
    return files.with_columns(
        ((pl.col("raw").cum_sum() - pl.col("raw")) // batch_bytes).alias("frame"))


def _pack_fs_plan(files, root, level):
    """Compile scanned files into the buffer-machine encode-group command stream
    (docs/ISA.md §3-§4): per frame one DEFLATE row (size = assembled frame
    length, header_offset = K members, pad_align = level) followed by its K
    gather rows — each carrying its PAX header INLINE at header_offset and its
    file body (pread of `path`) at data_offset. Returns (cmds, boundaries,
    frames) where frames[fid] = footer tuples (path,size,mode,mtime,uid,gid,
    in_off) and the fid-th DEFLATE row is the fid-th boundary."""
    import tarfile
    from ..wire import OP_DEFLATE, OP_COPY, cmd_df
    opc, hdr, hoff, doff, sz, pth, pad = [], [], [], [], [], [], []
    frames = []
    for _, grp in files.group_by("frame", maintain_order=True):
        cursor = 0
        members = []
        rows = []
        for r in grp.iter_rows(named=True):
            ti = tarfile.TarInfo(r["path"])
            ti.size = r["size"]; ti.mode = r["mode"] & 0o7777
            ti.mtime = r["mtime_ns"] // 1_000_000_000
            ti.uid = r["uid"]; ti.gid = r["gid"]
            h = ti.tobuf(format=tarfile.PAX_FORMAT)
            ho, do = cursor, cursor + len(h)
            cursor = do + ((r["size"] + 511) // 512) * 512     # padded body
            rows.append((h, ho, do, r["size"], os.path.join(root, r["path"])))
            members.append((r["path"], r["size"], r["mode"], r["mtime_ns"],
                            r["uid"], r["gid"], do))            # in_off = body
        opc.append(OP_DEFLATE); hdr.append(b""); hoff.append(len(rows))
        doff.append(0); sz.append(cursor); pth.append(""); pad.append(level)
        for (h, ho, do, s, rp) in rows:
            opc.append(OP_COPY); hdr.append(h); hoff.append(ho)
            doff.append(do); sz.append(s); pth.append(rp); pad.append(1)
        frames.append(members)
    n = len(opc)
    cmds = cmd_df(n, opcode=pl.Series(opc, dtype=pl.UInt8),
                  header=pl.Series(hdr, dtype=pl.Binary),
                  header_offset=pl.Series(hoff, dtype=pl.Int64),
                  data_offset=pl.Series(doff, dtype=pl.Int64),
                  size=pl.Series(sz, dtype=pl.Int64), path=pth,
                  pad_align=pl.Series(pad, dtype=pl.Int64))
    boundaries = np.flatnonzero(cmds["opcode"].to_numpy() == OP_DEFLATE)
    return cmds, boundaries, frames


def pack_fs(root, out_path, batch_bytes=16 << 20, level=6, workers=None,
            predicate=None, engine="auto", threads=8, batch_rows=4096):
    """Parallel pack a filesystem tree into a per-batch-frame zstd nock — the
    encode-group dual of unpack (docs/ISA.md): each frame is a DEFLATE group
    that gathers PAX headers (INLINE, planner-formatted) + file bodies into a
    worker buffer and compresses it. engine=None keeps the pure-Python
    thread-pool packer (the oracle). Output is unpackable by unpack()."""
    files = _pack_fs_scan(root, batch_bytes, predicate, engine, threads)
    if engine is None or not files.height:
        return _pack_fs_py(files, root, out_path, level, workers)
    from ..wire import PipeExecutor
    from ..tools import _check
    open(out_path, "wb").close()                       # fresh append target
    cmds, boundaries, frames = _pack_fs_plan(files, root, level)
    ex = PipeExecutor(out_path, engine=engine)
    try:
        comp = ex.execute(cmds, batch_rows=batch_rows, boundaries=boundaries)
    finally:
        assert ex.close() == 0
    _check(comp, cmds)
    coff = dict(zip(comp["user_data"].to_list(), comp["read_size"].to_list()))
    clen = dict(zip(comp["user_data"].to_list(), comp["cksum"].to_list()))
    uds = cmds["user_data"].to_list()          # boundaries[fid] = DEFLATE row
    ftmp = tempfile.TemporaryFile()
    fw = _FooterStream(ftmp)
    for fid, members in enumerate(frames):
        ud = uds[int(boundaries[fid])]
        for (path, size, mode, mtime, uid, gid, in_off) in members:
            fw.add(path, size, mode, mtime, uid, gid, fid, coff[ud], clen[ud],
                   in_off)
    fw.close()
    _write_footer(out_path, ftmp, False)
    ftmp.close()
    return Result(members=fw.members, frames=len(frames))


def _pack_fs_py(files, root, out_path, level, workers):
    """Oracle: the pure-Python thread-pool packer (file read + zstd both release
    the GIL). Superseded by the executor encode-group path; kept as the test
    reference."""
    import tarfile
    workers = workers or (os.cpu_count() or 8)
    lock = threading.Lock()
    st = {"off": 0}
    fd = os.open(out_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)

    def work(grp):
        buf, members = bytearray(), []
        for r in grp.iter_rows(named=True):
            with open(os.path.join(root, r["path"]), "rb") as f:
                data = f.read()
            ti = tarfile.TarInfo(r["path"])
            ti.size = len(data); ti.mode = r["mode"] & 0o7777
            ti.mtime = r["mtime_ns"] // 1_000_000_000
            ti.uid = r["uid"]; ti.gid = r["gid"]
            hdr = ti.tobuf(format=tarfile.PAX_FORMAT)
            in_off = len(buf) + len(hdr)
            buf += hdr; buf += data; buf += b"\x00" * ((-len(data)) % 512)
            members.append((r["path"], len(data), r["mode"], r["mtime_ns"],
                            r["uid"], r["gid"], in_off))
        comp = zstd.ZstdCompressor(level=level).compress(bytes(buf))
        with lock:
            coff = st["off"]; st["off"] += len(comp)
        os.pwrite(fd, comp, coff)
        return coff, len(comp), members

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        results = [f.result() for f in
                   [ex.submit(work, g)
                    for _, g in files.group_by("frame", maintain_order=True)]]
    os.close(fd)
    ftmp = tempfile.TemporaryFile()
    fw = _FooterStream(ftmp)
    for fid, (coff, clen, members) in enumerate(results):
        for (path, size, mode, mtime, uid, gid, in_off) in members:
            fw.add(path, size, mode, mtime, uid, gid, fid, coff, clen, in_off)
    fw.close()
    _write_footer(out_path, ftmp, False)
    ftmp.close()
    return Result(members=fw.members, frames=len(results))


def unpack_merged(manifest, dest, predicate=None, workers=None, engine="auto",
                  batch_rows=4096):
    """Parallel unpack of a sharded nock: each member resolved to its shard file
    (docs/ISA.md §10.5) — the only difference from linear is that each INFLATE
    names its shard as its source. engine=None keeps the Python oracle."""
    shards, idx = read_merged(manifest)
    if predicate is not None:
        idx = idx.filter(predicate)
    os.makedirs(dest, exist_ok=True)
    if engine is None or not idx.height:
        return _unpack(idx, dest, lambda sid: shards[sid], workers)
    return _unpack_exec(idx, dest, "-", list(shards), engine, batch_rows)

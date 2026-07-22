"""Runtime Arrow IPC via Polars (native Rust).

The hand-rolled `pupyarrow` reader/writer is now confined to the **compiler**
(`compiler/gen_templates.py`, which bakes the C emit templates at build time)
and **debug/introspection**. Everything on a live data path — the executor
command/completion streams, the zstream ZMETA/PLAN/COMP exchange, footer and
scan IO — serializes and parses through Polars here:

- **read**: frame each Arrow-IPC message off the pipe (cheap Python: 8-byte
  header + metadata length), then hand `schema + batch + EOS` to
  `pl.read_ipc_stream` (native). C emits one schema then batches, so we capture
  the first message as the schema and reuse it.
- **write**: `DataFrame.write_ipc_stream(compat_level=oldest)` — `oldest`
  forces the classic Arrow types (large_utf8 / large_binary, i64 offsets) that
  the C hand-parser expects, instead of Polars' default Utf8View. We strip the
  trailing EOS so batches concatenate into one stream; the C readers skip the
  repeated schema message (`meta.header_type == Schema`) and stop on EOS/EOF.
"""
from __future__ import annotations

import io
import struct

import polars as pl

_CONT = 0xFFFFFFFF
EOS = struct.pack("<II", _CONT, 0)
_OLDEST = pl.CompatLevel.oldest()

# Arrow IPC Message flatbuffer fields we need to FRAME a stream (split it into
# messages) — the header type (Schema=1 / RecordBatch=3) and the body length.
# Only the envelope is parsed here; column data is decoded by Polars. Inlined
# so this module has no dependency on the hand-rolled flatbuffer reader.
_HDR_SCHEMA, _HDR_BATCH = 1, 3


def _msg_fields(meta: bytes):
    """(header_type, body_length) from an Arrow IPC Message metadata buffer."""
    root = int.from_bytes(meta[0:4], "little")
    vt = root - int.from_bytes(meta[root:root + 4], "little", signed=True)
    vt_len = int.from_bytes(meta[vt:vt + 2], "little")

    def field(fid):                              # table field offset, 0 if absent
        off = 4 + 2 * fid
        if off + 2 > vt_len:
            return 0
        fo = int.from_bytes(meta[vt + off:vt + off + 2], "little")
        return root + fo if fo else 0

    htp, blp = field(1), field(3)                # HEADER_TYPE, BODY_LENGTH
    htype = meta[htp] if htp else 0
    blen = (int.from_bytes(meta[blp:blp + 8], "little", signed=True)
            if blp else 0)
    return htype, blen


def write_batch(sink, df: pl.DataFrame) -> None:
    """Append one record batch (schema + batch, no EOS) to a streaming sink."""
    buf = io.BytesIO()
    df.write_ipc_stream(buf, compat_level=_OLDEST)
    sink.write(buf.getvalue()[:-8])              # strip the trailing EOS marker


def write_eos(sink) -> None:
    sink.write(EOS)


def write_all(sink, df: pl.DataFrame) -> None:
    """Write a DataFrame as one complete Arrow-IPC stream (schema + batches +
    EOS). For files/blobs read back whole with `read_all`/`pl.read_ipc_stream`
    — NOT for the pipe protocol, where `write_batch` (no per-chunk schema) is
    used so the C reader can skip repeated schema messages."""
    df.write_ipc_stream(sink, compat_level=_OLDEST)


def _read_exact(f, n: int) -> bytes | None:
    out = b""
    while len(out) < n:
        c = f.read(n - len(out))
        if not c:
            return None
        out += c
    return out


def _raw_msg(f):
    """One framed Arrow-IPC message: (header_type, raw_bytes), or None at
    EOS/EOF. Only the envelope (header type + body length) is parsed — no
    column decoding, that's Polars' job."""
    hdr = _read_exact(f, 8)
    if hdr is None:
        return None
    cont, mlen = struct.unpack("<II", hdr)
    if mlen == 0:
        return None                              # EOS
    meta = _read_exact(f, mlen)
    if meta is None:
        return None
    htype, blen = _msg_fields(meta)
    body = _read_exact(f, blen) if blen else b""
    return htype, hdr + meta + body


class Reader:
    """Read a streaming Arrow-IPC pipe as Polars DataFrames. The first schema
    message is captured and prepended to each batch for the native reader; any
    repeated schema messages (a streaming writer that re-emits the schema per
    batch) are skipped, so both schema-once and schema-per-batch producers read
    correctly."""

    def __init__(self, fileobj):
        self.f = fileobj
        self.schema = None

    def read(self) -> pl.DataFrame | None:
        """Next record batch as a DataFrame, or None at EOS/EOF."""
        while True:
            m = _raw_msg(self.f)
            if m is None:
                return None
            htype, raw = m
            if htype == _HDR_SCHEMA:
                if self.schema is None:
                    self.schema = raw            # first schema; reuse for parse
                continue                          # skip (repeated) schema msgs
            if self.schema is None:
                return None                       # batch before any schema
            return pl.read_ipc_stream(self.schema + raw + EOS)

    def __iter__(self):
        while (df := self.read()) is not None:
            yield df


def read_all(data: bytes) -> pl.DataFrame:
    """Parse a complete in-memory Arrow-IPC stream (all batches → one frame)."""
    return pl.read_ipc_stream(io.BytesIO(data))

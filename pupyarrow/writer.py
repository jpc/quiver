"""pupyarrow writer + stream reader. numpy-only.

StreamWriter/write_feather emit Arrow IPC that pyarrow's verifier
accepts (conformance-tested); StreamReader is the wire-protocol
counterpart for reading executor/scanner output from a pipe."""
from __future__ import annotations
import struct
import numpy as np
from . import fb
from .fb import (Builder, MSG, BATCH, SCHEMA, FIELD, INT, FLOAT, KV, FOOTER,
                HEADER_SCHEMA, HEADER_BATCH, V5,
                TYPE_INT, TYPE_FLOAT, TYPE_LARGE_BINARY, TYPE_LARGE_UTF8)

CONT = 0xFFFFFFFF
ARROW_MAGIC = b"ARROW1"
EOS = struct.pack("<II", CONT, 0)

TYPES = {
    "u8":  (TYPE_INT, {"bits": 8,  "signed": False}, np.uint8, "fixed"),
    "i32": (TYPE_INT, {"bits": 32, "signed": True},  np.int32, "fixed"),
    "i64": (TYPE_INT, {"bits": 64, "signed": True},  np.int64, "fixed"),
    "u64": (TYPE_INT, {"bits": 64, "signed": False}, np.uint64, "fixed"),
    "f64": (TYPE_FLOAT, {"precision": 2}, np.float64, "fixed"),
    "large_string": (TYPE_LARGE_UTF8, {}, None, "varlen"),
    "large_binary": (TYPE_LARGE_BINARY, {}, None, "varlen"),
}

def _pad8(n): return (n + 7) & ~7

def _kv_vec(b: Builder, meta: dict[str, str]) -> int:
    offs = []
    for k, v in meta.items():
        ko, vo = b.create_string(k), b.create_string(v)
        b.start_object(2)
        b.slot_offset(KV.KEY, ko); b.slot_offset(KV.VALUE, vo)
        offs.append(b.end_object())
    return b.offsets_vector(offs)

def _type_table(b: Builder, tid: int, p: dict) -> int:
    if tid == TYPE_INT:
        b.start_object(2)
        b.slot_scalar(INT.BIT_WIDTH, "<i", 4, p["bits"])
        b.slot_scalar(INT.IS_SIGNED, "<B", 1, int(p["signed"]))
    elif tid == TYPE_FLOAT:
        b.start_object(1)
        b.slot_scalar(FLOAT.PRECISION, "<h", 2, p["precision"])
    else:
        b.start_object(0)
    return b.end_object()

def _schema_table(b: Builder, schema, meta) -> int:
    fields = []
    for name, t in schema:
        tid, params, _, _ = TYPES[t]
        no = b.create_string(name)
        to = _type_table(b, tid, params)
        b.start_object(7)
        b.slot_offset(FIELD.NAME, no)
        b.slot_scalar(FIELD.NULLABLE, "<B", 1, 1)
        b.slot_scalar(FIELD.TYPE_TYPE, "<B", 1, tid)
        b.slot_offset(FIELD.TYPE, to)
        fields.append(b.end_object())
    fvec = b.offsets_vector(fields)
    mvec = _kv_vec(b, meta) if meta else 0
    b.start_object(4)
    b.slot_offset(SCHEMA.FIELDS, fvec)
    if mvec: b.slot_offset(SCHEMA.METADATA, mvec)
    return b.end_object()

def _message(header_type: int, build, body_len: int = 0) -> bytes:
    b = Builder()
    h = build(b)
    b.start_object(5)
    b.slot_scalar(MSG.VERSION, "<h", 2, V5)
    b.slot_scalar(MSG.HEADER_TYPE, "<B", 1, header_type)
    b.slot_offset(MSG.HEADER, h)
    b.slot_scalar(MSG.BODY_LENGTH, "<q", 8, body_len)
    out = b.finish(b.end_object())
    return out + b"\0" * (_pad8(len(out)) - len(out))

def schema_message(schema, meta=None) -> bytes:
    return _message(HEADER_SCHEMA, lambda b: _schema_table(b, schema, meta))

def _cols_to_buffers(schema, cols):
    assert len(schema) == len(cols), (len(schema), len(cols))
    n, bufs = None, []
    for (name, t), col in zip(schema, cols):
        _, _, dt, layout = TYPES[t]
        if layout == "fixed":
            arr = np.ascontiguousarray(col, dtype=dt)
            if n is None: n = len(arr)
            bufs += [b"", arr.tobytes()]
        else:
            data = [(v.encode() if isinstance(v, str) else bytes(v)) for v in col]
            if n is None: n = len(data)
            offs = np.zeros(len(data) + 1, dtype=np.int64)
            np.cumsum([len(d) for d in data], out=offs[1:])
            bufs += [b"", offs.tobytes(), b"".join(data)]
    return bufs, n or 0

def batch_message(schema, cols) -> tuple[bytes, bytes]:
    bufs, n = _cols_to_buffers(schema, cols)
    def build(b: Builder):
        b.start_vector(16, len(schema), 8)
        for _ in range(len(schema)):
            b.struct_2i64(n, 0)          # FieldNode(length, null_count)
        nvec = b.end_vector(len(schema))
        pos, placed = 0, []
        for buf in bufs:
            placed.append((pos, len(buf))); pos += _pad8(len(buf))
        b.start_vector(16, len(placed), 8)
        for off, ln in reversed(placed):
            b.struct_2i64(off, ln)       # Buffer(offset, length)
        bvec = b.end_vector(len(placed))
        b.start_object(4)
        b.slot_scalar(BATCH.LENGTH, "<q", 8, n)
        b.slot_offset(BATCH.NODES, nvec)
        b.slot_offset(BATCH.BUFFERS, bvec)
        return b.end_object()
    body = b"".join(x + b"\0" * (_pad8(len(x)) - len(x)) for x in bufs)
    return _message(HEADER_BATCH, build, len(body)), body

def _frame(m: bytes) -> bytes:
    return struct.pack("<II", CONT, len(m)) + m

class StreamWriter:
    def __init__(self, f, schema, meta=None, write_schema=True):
        self.f, self.schema = f, schema
        if write_schema:               # False: appending to an existing
            f.write(_frame(schema_message(schema, meta)))  # stream file
    def write_batch(self, cols):
        m, body = batch_message(self.schema, cols)
        self.f.write(_frame(m) + body)
    def close(self):
        self.f.write(EOS); self.f.flush()

def write_feather(f, schema, cols, meta=None, batch_rows=None):
    f.write(ARROW_MAGIC + b"\0\0")
    f.write(_frame(schema_message(schema, meta)))
    n = len(cols[0]); blocks = []
    step = batch_rows or max(n, 1)
    for s in range(0, max(n, 1), step):
        m, body = batch_message(schema, [c[s:s+step] for c in cols])
        blocks.append((f.tell(), 8 + len(m), len(body)))
        f.write(_frame(m) + body)
    f.write(EOS)
    b = Builder()
    so = _schema_table(b, schema, meta)
    b.start_vector(24, len(blocks), 8)
    for off, ml, bl in reversed(blocks):
        b.struct_block(off, ml, bl)
    rvec = b.end_vector(len(blocks))
    b.start_object(5)
    b.slot_scalar(FOOTER.VERSION, "<h", 2, V5)
    b.slot_offset(FOOTER.SCHEMA, so)
    b.slot_offset(FOOTER.BATCHES, rvec)
    fbb = b.finish(b.end_object())
    f.write(fbb + struct.pack("<i", len(fbb)) + ARROW_MAGIC)


class StreamReader:
    """Incremental Arrow IPC stream reader for the quiver wire protocol:
    flat schemas, no nulls. Yields dict[str, np.ndarray | list] per
    batch. Reads from any file-like with .read()."""

    _NP = {(TYPE_INT, 8, False): np.uint8, (TYPE_INT, 32, True): np.int32,
           (TYPE_INT, 64, True): np.int64, (TYPE_INT, 64, False): np.uint64,
           (TYPE_FLOAT, 0, False): np.float64}

    def __init__(self, f):
        self.f = f
        m = self._read_msg()
        assert m is not None, "stream closed before schema"
        msg = fb.Tbl.root(m[0])
        assert msg.u8(fb.MSG.HEADER_TYPE) == HEADER_SCHEMA
        self.fields = []          # (name, kind, np_dtype)
        for ft in msg.union(fb.MSG.HEADER).vector_tables(fb.SCHEMA.FIELDS):
            tid = ft.u8(fb.FIELD.TYPE_TYPE)
            name = ft.string(fb.FIELD.NAME)
            if tid in (TYPE_LARGE_UTF8, TYPE_LARGE_BINARY):
                self.fields.append((name, "varlen",
                                    tid == TYPE_LARGE_UTF8))
            else:
                tt = ft.table(fb.FIELD.TYPE)
                key = (tid, tt.i32(fb.INT.BIT_WIDTH) if tid == TYPE_INT else 0,
                       tt.boolean(fb.INT.IS_SIGNED) if tid == TYPE_INT
                       else False)
                self.fields.append((name, "fixed", self._NP[key]))

    def _read_exact(self, n: int) -> bytes | None:
        out = b""
        while len(out) < n:
            chunk = self.f.read(n - len(out))
            if not chunk:
                return None
            out += chunk
        return out

    def _read_msg(self):
        hdr = self._read_exact(8)
        if hdr is None:
            return None
        cont, mlen = struct.unpack("<II", hdr)
        assert cont == CONT, hex(cont)
        if mlen == 0:
            return None                       # EOS
        meta = self._read_exact(mlen)
        msg = fb.Tbl.root(meta)
        blen = msg.i64(fb.MSG.BODY_LENGTH)
        body = self._read_exact(blen) if blen else b""
        return meta, body

    def read_batch(self) -> dict | None:
        m = self._read_msg()
        if m is None:
            return None
        meta, body = m
        msg = fb.Tbl.root(meta)
        rb = msg.union(fb.MSG.HEADER)
        n = rb.i64(fb.BATCH.LENGTH)
        bstart, _ = rb.vector(fb.BATCH.BUFFERS)
        bi = 0

        def buf(i):
            o, l = struct.unpack_from("<qq", meta, bstart + 16 * i)
            return body[o:o + l]

        out = {}
        for name, kind, p in self.fields:
            if kind == "fixed":
                out[name] = np.frombuffer(buf(bi + 1), dtype=p, count=n)
                bi += 2
            else:
                offs = np.frombuffer(buf(bi + 1), dtype=np.int64,
                                     count=n + 1)
                data = buf(bi + 2)
                vals = [data[offs[i]:offs[i + 1]] for i in range(n)]
                out[name] = ([v.decode() for v in vals] if p else vals)
                bi += 3
        return out

    def __iter__(self):
        while (b := self.read_batch()) is not None:
            yield b

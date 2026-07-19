"""
fb.py — the flatbuffers subset pupyarrow actually uses, in one module.

Proposed as pupyarrow/fb.py when it becomes a standalone project.
Replaces both the `flatbuffers` pip dependency and the 50-file generated
flatbuf/ tree with ~300 lines: a slot-offset reader (`Tbl`) and a
minimal builder ported from the reference algorithm (Apache-2.0,
google/flatbuffers — keep the attribution).

Builder scope: tables of scalars/offsets, vectors of offsets and of
inline structs, strings. No vtable deduplication (marginally larger
metadata, meaningfully simpler code); alignment rules are kept exactly,
which is what Arrow C++'s flatbuffers verifier actually checks — the
conformance bar is "pyarrow opens every stream we emit".

Arrow field-slot constants live here too, replacing the codegen: they
are the stable public ABI of Message.fbs/Schema.fbs/File.fbs.
"""

from __future__ import annotations

import struct

# ── Arrow flatbuffers schema constants (slot ids from the .fbs files) ────

class MSG:      VERSION, HEADER_TYPE, HEADER, BODY_LENGTH, METADATA = range(5)
class BATCH:    LENGTH, NODES, BUFFERS, COMPRESSION = range(4)
class SCHEMA:   ENDIANNESS, FIELDS, METADATA = range(3)
class FIELD:    NAME, NULLABLE, TYPE_TYPE, TYPE, DICT, CHILDREN, METADATA = range(7)
class INT:      BIT_WIDTH, IS_SIGNED = range(2)
class FLOAT:    PRECISION = 0
class KV:       KEY, VALUE = range(2)
class TS:       UNIT, TIMEZONE = range(2)
class FSB:      BYTE_WIDTH = 0
class FSL:      LIST_SIZE = 0
class DATE:     UNIT = 0
class TIME:     UNIT, BIT_WIDTH = range(2)
class DECIMAL:  PRECISION, SCALE, BIT_WIDTH = range(3)
class DURATION: UNIT = 0
class INTERVAL: UNIT = 0
class MAP:      KEYS_SORTED = 0
class UNION:    MODE, TYPE_IDS = range(2)
class BODYCOMP: CODEC, METHOD = range(2)
class FOOTER:   VERSION, SCHEMA, DICTS, BATCHES, METADATA = range(5)

HEADER_SCHEMA, HEADER_DICT, HEADER_BATCH = 1, 2, 3
TYPE_INT, TYPE_FLOAT, TYPE_BOOL = 2, 3, 6
TYPE_LARGE_BINARY, TYPE_LARGE_UTF8 = 19, 20
V5 = 4

# ── reader ───────────────────────────────────────────────────────────────

def _u16(b, o): return struct.unpack_from("<H", b, o)[0]
def _i32(b, o): return struct.unpack_from("<i", b, o)[0]
def _u32(b, o): return struct.unpack_from("<I", b, o)[0]


class Tbl:
    """A flatbuffers table position; every accessor is explicit offset
    math — no per-field object allocation (see bench: this is what makes
    it faster than generated accessors on many-message streams)."""

    __slots__ = ("b", "pos")

    def __init__(self, b, pos):
        self.b, self.pos = b, pos

    @classmethod
    def root(cls, b, offset=0):
        return cls(b, offset + _u32(b, offset))

    def _slot(self, fid) -> int:
        vt = self.pos - _i32(self.b, self.pos)
        s = 4 + 2 * fid
        if s >= _u16(self.b, vt):
            return 0
        v = _u16(self.b, vt + s)
        return self.pos + v if v else 0

    def scalar(self, fid, fmt, default=0):
        p = self._slot(fid)
        return struct.unpack_from(fmt, self.b, p)[0] if p else default

    def u8(self, fid, d=0):  return self.scalar(fid, "<B", d)
    def i16(self, fid, d=0): return self.scalar(fid, "<h", d)
    def i32(self, fid, d=0): return self.scalar(fid, "<i", d)
    def i64(self, fid, d=0): return self.scalar(fid, "<q", d)
    def boolean(self, fid, d=False): return bool(self.scalar(fid, "<B", int(d)))

    def slot_pos(self, fid) -> int:
        """Absolute byte offset of a scalar field's value (patch slots)."""
        return self._slot(fid)

    def table(self, fid) -> "Tbl | None":
        p = self._slot(fid)
        return Tbl(self.b, p + _u32(self.b, p)) if p else None

    union = table   # header union value: same indirection

    def string(self, fid) -> str | None:
        p = self._slot(fid)
        if not p:
            return None
        v = p + _u32(self.b, p)
        n = _u32(self.b, v)
        return bytes(self.b[v + 4: v + 4 + n]).decode()

    def vector(self, fid) -> tuple[int, int]:
        """→ (absolute offset of element 0, length). Struct/scalar
        elements are inline at stride; table elements are uoffsets."""
        p = self._slot(fid)
        if not p:
            return 0, 0
        v = p + _u32(self.b, p)
        return v + 4, _u32(self.b, v)

    def vector_i32(self, fid) -> list[int]:
        start, n = self.vector(fid)
        return [struct.unpack_from("<i", self.b, start + 4 * i)[0]
                for i in range(n)]

    def vector_tables(self, fid):
        start, n = self.vector(fid)
        for i in range(n):
            e = start + 4 * i
            yield Tbl(self.b, e + _u32(self.b, e))


def parse_schema(schema_tbl: Tbl) -> list[tuple[str, int, dict]]:
    """→ [(name, type_id, params)], covering pupyarrow's flat types.
    Demonstrates the codegen-free read path for the FeatherFile port."""
    out = []
    for f in schema_tbl.vector_tables(SCHEMA.FIELDS):
        tid = f.u8(FIELD.TYPE_TYPE)
        t = f.table(FIELD.TYPE)
        params = {}
        if tid == TYPE_INT:
            params = {"bits": t.i32(INT.BIT_WIDTH),
                      "signed": t.boolean(INT.IS_SIGNED)}
        elif tid == TYPE_FLOAT:
            params = {"precision": t.i16(FLOAT.PRECISION)}
        out.append((f.string(FIELD.NAME), tid, params))
    return out


# ── builder (ported subset of the reference algorithm) ───────────────────

class Builder:
    def __init__(self, initial: int = 1024):
        self.buf = bytearray(initial)
        self.head = initial
        self.minalign = 1
        self.vt: list[int] | None = None
        self.object_end = 0

    def offset(self) -> int:
        return len(self.buf) - self.head

    def _grow(self):
        old = len(self.buf)
        new = bytearray(max(old * 2, 1))
        new[len(new) - old:] = self.buf
        self.head += len(new) - old
        self.buf = new

    def _pad(self, n: int):
        self.head -= n
        self.buf[self.head:self.head + n] = b"\0" * n

    def prep(self, size: int, additional: int):
        if size > self.minalign:
            self.minalign = size
        align = (~(len(self.buf) - self.head + additional) + 1) & (size - 1)
        while self.head < align + size + additional:
            self._grow()
            align = (~(len(self.buf) - self.head + additional) + 1) & (size - 1)
        self._pad(align)

    def _place(self, fmt: str, size: int, x):
        self.head -= size
        struct.pack_into(fmt, self.buf, self.head, x)

    def prepend(self, fmt: str, size: int, x):
        self.prep(size, 0)
        self._place(fmt, size, x)

    def prepend_uoffset(self, off: int):
        self.prep(4, 0)
        assert off <= self.offset()
        self._place("<I", 4, self.offset() - off + 4)

    def create_string(self, s: str | bytes) -> int:
        b = s.encode() if isinstance(s, str) else bytes(s)
        self.prep(4, len(b) + 1)
        self.head -= len(b) + 1
        self.buf[self.head:self.head + len(b)] = b
        self.buf[self.head + len(b)] = 0
        self._place("<I", 4, len(b))
        return self.offset()

    # tables
    def start_object(self, nslots: int):
        self.vt = [0] * nslots
        self.object_end = self.offset()

    def slot_scalar(self, fid: int, fmt: str, size: int, x, default=0):
        if x != default:
            self.prepend(fmt, size, x)
            self.vt[fid] = self.offset()

    def slot_offset(self, fid: int, off: int):
        if off:
            self.prepend_uoffset(off)
            self.vt[fid] = self.offset()

    def end_object(self) -> int:
        self.prepend("<i", 4, 0)                     # soffset placeholder
        obj = self.offset()
        vt = self.vt
        i = len(vt) - 1
        while i >= 0 and vt[i] == 0:
            i -= 1
        for elem in reversed(vt[:i + 1]):
            self._place("<H", 2, obj - elem if elem else 0)
        self._place("<H", 2, obj - self.object_end)  # table size
        self._place("<H", 2, (i + 3) * 2)            # vtable size
        struct.pack_into("<i", self.buf, len(self.buf) - obj,
                         self.offset() - obj)        # patch soffset
        self.vt = None
        return obj

    # vectors
    def start_vector(self, elem_size: int, n: int, alignment: int):
        self.prep(4, elem_size * n)
        self.prep(alignment, elem_size * n)

    def end_vector(self, n: int) -> int:
        self._place("<I", 4, n)
        return self.offset()

    def offsets_vector(self, offs: list[int]) -> int:
        self.start_vector(4, len(offs), 4)
        for o in reversed(offs):
            self.prepend_uoffset(o)
        return self.end_vector(len(offs))

    # inline structs (Arrow: FieldNode, Buffer = 2×i64; Block = i64,i32,pad,i64)
    def struct_2i64(self, a: int, b: int):
        self.prep(8, 16)
        self._place("<q", 8, b)
        self._place("<q", 8, a)

    def struct_block(self, off: int, meta_len: int, body_len: int):
        self.prep(8, 24)
        self._place("<q", 8, body_len)
        self._pad(4)
        self._place("<i", 4, meta_len)
        self._place("<q", 8, off)

    def finish(self, root: int) -> bytes:
        self.prep(self.minalign, 4)
        self.prepend_uoffset(root)
        return bytes(self.buf[self.head:])

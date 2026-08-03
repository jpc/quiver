import sys, os, random, subprocess
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
import polars as pl
import zstandard as zstd
from quiver.exec.blocks import _ipc_bytes

NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK = range(6)
E_FS, E_VAL, E_INLINE, E_SINK = range(4)

rows = []
def emit(tid, op, k1=0, k2=0, a=0, b=0, c=0, d=0, path="", payload=b""):
    rows.append(dict(tid=tid, op=op, k1=k1, k2=k2, a=a, b=b, c=c, d=d,
                     path=path, payload=payload))

random.seed(9)
files = []
for i in range(6):
    fn = os.path.abspath(f"g{i}.bin")
    open(fn, "wb").write(random.randbytes(random.randrange(1000, 2 << 20)))
    files.append(fn)

out = os.path.abspath("stream.pack")
# fiber 0: open sink, create vals, spawn workers, join
emit(0, SINK, a=0, path=out)
for i, fn in enumerate(files):
    emit(0, NEWVAL, a=i, b=1, c=3)                     # vid=i, zstd, level 3
emit(0, SPAWN, a=1, b=len(files))
emit(0, JOIN, a=1, b=len(files))
# fiber 1+i: header -> val, body -> val, close, drain to sink
for i, fn in enumerate(files):
    hdr = f"HDR:{os.path.basename(fn)}".encode().ljust(64, b"\0")
    emit(1 + i, MOV, k1=E_INLINE, k2=E_VAL, d=i, payload=hdr)
    emit(1 + i, MOV, k1=E_FS, k2=E_VAL, a=0, b=-1, d=i, path=fn)
    emit(1 + i, CLOSE, a=i)
    emit(1 + i, MOV, k1=E_VAL, k2=E_SINK, a=i, c=-1, d=0)

df = pl.DataFrame(rows).select(
    tid=pl.col("tid").cast(pl.UInt32), op=pl.col("op").cast(pl.UInt8),
    k1=pl.col("k1").cast(pl.UInt8), k2=pl.col("k2").cast(pl.UInt8),
    a=pl.col("a").cast(pl.Int64), b=pl.col("b").cast(pl.Int64),
    c=pl.col("c").cast(pl.Int64), d=pl.col("d").cast(pl.Int64),
    path=pl.col("path").cast(pl.Utf8), payload=pl.col("payload").cast(pl.Binary))
open("prog.arrow", "wb").write(_ipc_bytes(df))

r = subprocess.run([_R + "/quiver/exec/qvm2", "run", "prog.arrow"],
                   capture_output=True, text=True, timeout=120)
print("rc:", r.returncode, r.stderr.strip()[:200])
assert r.returncode == 0

# verify: walk zstd frames in drain order, match by header, compare bodies
blob = open(out, "rb").read()
seen = {}
off = 0
while off < len(blob):
    n = zstd.ZstdDecompressor()  # noqa
    flen = zstd.frame_content_size(blob[off:off + 18])
    import zstandard
    csize = zstandard.get_frame_parameters(blob[off:off + 18])
    # find compressed frame size by trial: decompress with stream reader
    dctx = zstandard.ZstdDecompressor()
    # use decompressobj to find frame end
    dobj = dctx.decompressobj()
    dec = dobj.decompress(blob[off:])
    used = len(blob) - off - len(dobj.unused_data)
    off += used
    name = dec[:64].split(b"\0")[0].decode().removeprefix("HDR:")
    seen[name] = dec[64:]
bad = 0
for fn in files:
    body = open(fn, "rb").read()
    got = seen.get(os.path.basename(fn))
    if got != body:
        bad += 1; print("MISMATCH", fn, len(got or b""), len(body))
print("STREAM-M0 PASS" if not bad and len(seen) == len(files) else f"STREAM-M0 FAIL bad={bad} seen={len(seen)}")
sys.exit(1 if bad else 0)

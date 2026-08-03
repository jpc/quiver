import sys, os, random, struct, subprocess, shutil, hashlib
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
import polars as pl
from quiver.exec.blocks import _ipc_bytes
import blake3

NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT = range(7)
E_FS, E_VAL, E_INLINE, E_SINK = range(4)
DIG = 0x80
QVM2 = _R + "/quiver/exec/qvm2"

def prog(rows, path):
    df = pl.DataFrame(rows).select(
        tid=pl.col("tid").cast(pl.UInt32), op=pl.col("op").cast(pl.UInt8),
        k1=pl.col("k1").cast(pl.UInt8), k2=pl.col("k2").cast(pl.UInt8),
        a=pl.col("a").cast(pl.Int64), b=pl.col("b").cast(pl.Int64),
        c=pl.col("c").cast(pl.Int64), d=pl.col("d").cast(pl.Int64),
        path=pl.col("path").cast(pl.Utf8), payload=pl.col("payload").cast(pl.Binary))
    open(path, "wb").write(_ipc_bytes(df))

def E(rows, tid, op, k1=0, k2=0, a=0, b=0, c=0, d=0, path="", payload=b""):
    rows.append(dict(tid=tid, op=op, k1=k1, k2=k2, a=a, b=b, c=c, d=d, path=path, payload=payload))

random.seed(17)
files = []
for i in range(8):
    fn = os.path.abspath(f"m1_{i}.bin")
    open(fn, "wb").write(random.randbytes(random.randrange(1000, 3 << 20)))
    files.append(fn)

# ---------- PACK: header+body -> codec val -> sink0; DIGEST bodies; EMIT records -> sink1
out, rec = os.path.abspath("m1.pack"), os.path.abspath("m1.rec")
rows = []
E(rows, 0, SINK, a=0, path=out)
E(rows, 0, SINK, a=1, path=rec)
for i in range(len(files)):
    E(rows, 0, NEWVAL, a=i, b=1, c=3)                     # zstd frame val
E(rows, 0, SPAWN, a=1, b=len(files))
E(rows, 0, JOIN, a=1, b=len(files))
for i, fn in enumerate(files):
    hdr = f"HDR:{os.path.basename(fn)}".encode().ljust(64, b"\0")
    E(rows, 1 + i, MOV, k1=E_INLINE, k2=E_VAL, d=i, payload=hdr)
    E(rows, 1 + i, MOV, k1=E_FS | DIG, k2=E_VAL, a=0, b=-1, d=i, path=fn)  # DIGEST body
    E(rows, 1 + i, CLOSE, a=i)
    E(rows, 1 + i, MOV, k1=E_VAL, k2=E_SINK, a=i, b=0, c=-1, d=0)
    E(rows, 1 + i, EMIT, a=1)
prog(rows, "m1_pack.arrow")
r = subprocess.run([QVM2, "run", "m1_pack.arrow"], capture_output=True, text=True, timeout=120)
assert r.returncode == 0, r.stderr

# read the EMIT records: {u32 tid, u32 pad, i64 base, i64 len, i64 digest}
recs = {}
buf = open(rec, "rb").read()
for off in range(0, len(buf), 32):
    tid, _pad, base, ln, dg = struct.unpack_from("<IIqqq", buf, off)
    recs[tid] = (base, ln, dg)
assert len(recs) == len(files)
dig_bad = 0
for i, fn in enumerate(files):
    want = int.from_bytes(blake3.blake3(open(fn, "rb").read()).digest(8), "little", signed=True)
    got = recs[1 + i][2]
    if want != got: dig_bad += 1; print("DIGEST MISMATCH", fn)

# ---------- UNPACK from the records: two fibers per file —
#   producer: mov fs:pack@base±len -> w (CODEC unzstd, STREAM), close
#   consumer: mov w@64±(-1) -> fs:restored TRUNC   (chases the decoder)
rdir = os.path.abspath("m1_restored"); shutil.rmtree(rdir, ignore_errors=True); os.makedirs(rdir)
rows = []
for i, fn in enumerate(files):
    E(rows, 0, NEWVAL, a=i, b=2, c=0, d=1)                # unzstd, STREAM
E(rows, 0, SPAWN, a=1, b=2 * len(files))
E(rows, 0, JOIN, a=1, b=2 * len(files))
for i, fn in enumerate(files):
    base, ln, _dg = recs[1 + i]
    dst = os.path.join(rdir, os.path.basename(fn))
    E(rows, 1 + 2 * i, MOV, k1=E_FS, k2=E_VAL, a=base, b=ln, d=i, path=out)   # producer
    E(rows, 1 + 2 * i, CLOSE, a=i)
    E(rows, 2 + 2 * i, MOV, k1=E_VAL, k2=E_FS, a=i, b=64, c=-1, d=-1, path=dst)  # consumer chases
prog(rows, "m1_unpack.arrow")
r = subprocess.run([QVM2, "run", "m1_unpack.arrow"], capture_output=True, text=True, timeout=120)
assert r.returncode == 0, r.stderr

body_bad = 0
for fn in files:
    a = hashlib.md5(open(fn, "rb").read()).hexdigest()
    b = hashlib.md5(open(os.path.join(rdir, os.path.basename(fn)), "rb").read()).hexdigest()
    if a != b: body_bad += 1; print("BODY MISMATCH", fn)
ok = not dig_bad and not body_bad
print("M1 ROUND-TRIP PASS (8 files: digests + streamed unpack byte-exact)" if ok
      else f"M1 FAIL dig_bad={dig_bad} body_bad={body_bad}")
sys.exit(0 if ok else 1)

import sys, os, random, struct, subprocess, shutil, hashlib
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
import polars as pl
from quiver.exec.blocks import _ipc_bytes

(NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT,
 MKDIR, SYMLINK, LINK, SETMETA, UNLINK, RMDIR, FENCE) = range(14)
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

# ---- source tree: files in nested dirs (restrictive modes), symlinks, a hardlink
random.seed(23)
src = os.path.abspath("m2_src"); shutil.rmtree(src, ignore_errors=True)
files, dirs = [], []
for i in range(6):
    d = f"{src}/d{i}/sub"
    os.makedirs(d)
    fn = f"{d}/f{i}.bin"
    open(fn, "wb").write(random.randbytes(random.randrange(5000, 1 << 20)))
    files.append(fn)
dmeta = {}
for r, ds, _f in os.walk(src, topdown=False):
    for dd in ds:
        p = os.path.join(r, dd)
        m = random.choice([0o700, 0o755, 0o555, 0o500]); mt = random.randrange(15, 17) * 10**17
        dmeta[os.path.relpath(p, src)] = (m, mt)

# ---- the §4.6 unpack shape, straight to a fresh dest (bodies from the SOURCE
# files stand in for frame scatter — M1 proved codec scatter; M2 gates ORDERING)
dst = os.path.abspath("m2_dst"); shutil.rmtree(dst, ignore_errors=True)
rows = []
nfil = len(files)
# scope 1: files (mkparents implicit) + explicit dirs interleaved
E(rows, 0, SPAWN, a=1, b=nfil + len(dmeta))
E(rows, 0, JOIN, a=1, b=nfil + len(dmeta))
t = 1
for fn in files:
    rel = os.path.relpath(fn, src)
    E(rows, t, MOV, k1=E_FS | DIG, k2=E_FS, a=0, b=-1, d=-1, path2=None) if False else None
    # val-free file copy: fs->val(plain)->fs would need a val; simplest: fs->val->fs
    E(rows, t, NEWVAL, a=100 + t, b=0, c=0, d=1)          # plain STREAM val
    E(rows, t, MOV, k1=E_FS, k2=E_VAL, a=0, b=-1, d=100 + t, path=fn)
    E(rows, t, CLOSE, a=100 + t)
    E(rows, t, MOV, k1=E_VAL, k2=E_FS, a=100 + t, b=0, c=-1, d=-1, path=os.path.join(dst, rel))
    t += 1
for rel in dmeta:
    E(rows, t, MKDIR, path=os.path.join(dst, rel)); t += 1
# scope 2: symlinks + hardlink (need targets on disk)
E(rows, 0, SPAWN, a=t, b=t + 2)
E(rows, 0, JOIN, a=t, b=t + 2)
E(rows, t, SYMLINK, b=16 * 10**17, path=os.path.join(dst, "d0/rel_link"), payload=b"sub/f0.bin"); t += 1
E(rows, t, SYMLINK, path=os.path.join(dst, "dangling"), payload=b"no/where"); t += 1
E(rows, t, LINK, path=os.path.join(dst, "d1/hard"),
  payload=os.path.join(dst, "d1/sub/f1.bin").encode()); t += 1
# scope 3: restrictive dir metadata LAST
E(rows, 0, SPAWN, a=t, b=t + len(dmeta) - 1)
E(rows, 0, JOIN, a=t, b=t + len(dmeta) - 1)
for rel, (m, mt) in dmeta.items():
    E(rows, t, SETMETA, a=m, b=mt, path=os.path.join(dst, rel)); t += 1
prog(rows, "m2.arrow")
r = subprocess.run([QVM2, "run", "m2.arrow"], capture_output=True, text=True, timeout=120)
assert r.returncode == 0, r.stderr

bad = 0
for fn in files:
    rel = os.path.relpath(fn, src)
    a = hashlib.md5(open(fn, "rb").read()).hexdigest()
    b = hashlib.md5(open(os.path.join(dst, rel), "rb").read()).hexdigest()
    if a != b: bad += 1; print("BODY", rel)
for rel, (m, mt) in dmeta.items():
    st = os.lstat(os.path.join(dst, rel))
    if (st.st_mode & 0o7777) != m or st.st_mtime_ns != mt:
        bad += 1; print("DIRMETA", rel, oct(st.st_mode & 0o7777), oct(m), st.st_mtime_ns, mt)
if os.readlink(os.path.join(dst, "d0/rel_link")) != "sub/f0.bin": bad += 1; print("SYMLINK")
if os.lstat(os.path.join(dst, "d0/rel_link")).st_mtime_ns != 16 * 10**17: bad += 1; print("SYMLINK MTIME")
if os.readlink(os.path.join(dst, "dangling")) != "no/where": bad += 1; print("DANGLING")
if os.stat(os.path.join(dst, "d1/hard")).st_ino != os.stat(os.path.join(dst, "d1/sub/f1.bin")).st_ino:
    bad += 1; print("HARDLINK")
print("M2 §4.6 SHAPE PASS (files+dirs interleaved, links fenced by scope, restrictive modes last)"
      if not bad else f"M2 FAIL bad={bad}")
sys.exit(1 if bad else 0)

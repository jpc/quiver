"""M6: qplan2 pack/unpack, cross-engine gated against bvm/blocks."""
import sys, os, shutil, random, hashlib, time
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
from quiver import qplan2
from quiver.exec import blocks

BVM = _R + "/quiver/exec/bvm"

def tree_md5(root):
    out = {}
    for r, ds, fs in os.walk(root):
        for f in fs:
            p = os.path.join(r, f)
            out[os.path.relpath(p, root)] = hashlib.md5(open(p, "rb").read()).hexdigest()
    return out

base = os.path.abspath("m6"); shutil.rmtree(base, ignore_errors=True)
src = f"{base}/src"
random.seed(47)
for i in range(12):
    d = f"{src}/d{i:02}/sub"
    os.makedirs(d)
    for k in range(random.randrange(2, 8)):
        open(f"{d}/f{k}.bin", "wb").write(random.randbytes(random.randrange(100, 1 << 20)))
for r, ds, _f in os.walk(src, topdown=False):
    for dd in ds:
        p = os.path.join(r, dd)
        os.utime(p, ns=(16 * 10**17, 16 * 10**17)); os.chmod(p, 0o755)
truth = tree_md5(src)
bad = []

# ---- gate A: qplan2.pack -> blocks.verify + blocks.unpack
t0 = time.time()
r = qplan2.pack(src, f"{base}/q.nock", level=3)
tp = time.time() - t0
v = blocks.verify(f"{base}/q.nock", sample=64)
if v["truncated"] or v["bad_frames"]:
    bad.append(f"verify: {v}")
d1 = f"{base}/u1"
blocks.unpack(f"{base}/q.nock", d1, BVM, nworkers=8)
if tree_md5(d1) != truth: bad.append("bvm-unpack(qplan2-pack) tree mismatch")

# ---- gate B: bvm pack -> qplan2.unpack
blocks.pack_fs_c(src, f"{base}/b.nock", f"{base}/b.wal", BVM, nworkers=8, level=1)
d2 = f"{base}/u2"
t0 = time.time()
n2 = qplan2.unpack(f"{base}/b.nock", d2)
tu = time.time() - t0
if tree_md5(d2) != truth: bad.append("qplan2-unpack(bvm-pack) tree mismatch")
# dir metadata through qplan2.unpack
for r_, ds, _f in os.walk(src):
    for dd in ds:
        sp = os.path.join(r_, dd); rp = os.path.relpath(sp, src)
        st = os.lstat(os.path.join(d2, rp))
        if (st.st_mode & 0o7777) != 0o755 or st.st_mtime_ns != 16 * 10**17:
            bad.append(f"dirmeta {rp}"); break

# ---- gate C: qplan2 -> qplan2
d3 = f"{base}/u3"
qplan2.unpack(f"{base}/q.nock", d3)
if tree_md5(d3) != truth: bad.append("qplan2 self round-trip mismatch")

print(f"pack {r} ({tp:.2f}s)  unpack n={n2} ({tu:.2f}s)")
print("M6 CROSS-ENGINE PASS" if not bad else f"M6 FAIL: {bad}")
sys.exit(1 if bad else 0)

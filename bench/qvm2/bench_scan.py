import sys, os, time
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from quiver.exec import blocks

ROOT = sys.argv[1]
BVM = _R + "/quiver/exec/bvm"

def bvm_leg(tag):
    t0 = time.time()
    b = blocks._Bvm(BVM, 32)
    b.scan_fs(0, ROOT)
    n = 0
    while True:
        t, pld = b.read()
        if t is None or t == 1: break
        if t == 0:
            n += blocks._stat_df(pld)[2].height
    b.finish()
    w = time.time() - t0
    print(f"  bvm[{tag}]: {n:,} rows in {w:.2f}s ({n/w:,.0f}/s)", flush=True)
    return n, w

import drive_m5
def qvm2_leg():
    df, vm_w, wall, rc = drive_m5.cscan(ROOT, "/tmp/qvm2bench.emit", walkers=32)
    assert rc == 0
    print(f"  qvm2: {df.height:,} entries in {wall:.2f}s (vm {vm_w:.2f}s, {df.height/wall:,.0f}/s)", flush=True)
    return df.height, wall

print(f"=== scan benchmark on {ROOT}", flush=True)
n1, w1 = bvm_leg("cold")
n2, w2 = qvm2_leg()
n3, w3 = bvm_leg("warm")
print(f"SUMMARY root={ROOT} bvm_cold={w1:.2f}s qvm2={w2:.2f}s bvm_warm={w3:.2f}s "
      f"rows bvm={n1:,} qvm2={n2:,} ratio_vs_warm={w2/w3:.2f}x", flush=True)

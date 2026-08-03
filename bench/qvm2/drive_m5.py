"""I_SCAN generator leaf: one instruction, whole tree as Arrow batches."""
import sys, os, time, subprocess
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
sys.path.insert(0, ".")
import polars as pl
import pyarrow as pa
from drive_m4 import ibatch, irows, _msg_size, ISCH
(NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT,
 MKDIR, SYMLINK, LINK, SETMETA, UNLINK, RMDIR, FENCE, READDIR, STATB, SCAN) = range(17)
QVM2 = _R + "/quiver/exec/qvm2"

def cscan(root, emitf, walkers=32):
    open(emitf, "wb").close()
    proc = subprocess.Popen([QVM2, "stream"], stdin=subprocess.PIPE)
    t0 = time.time()
    proc.stdin.write(ibatch(pl.concat([
        irows(tid=0, op=SINK, a=0, b=1, path=emitf),
        irows(tid=1, op=SCAN, a=0, b=walkers, path=root),
        irows(tid=0, op=SPAWN, a=1, b=1),
        irows(tid=0, op=JOIN, a=1, b=1)])))
    proc.stdin.close()
    rc = proc.wait(timeout=1800)
    vm_w = time.time() - t0
    # one vectorized parse of the whole stream
    buf = open(emitf, "rb").read()
    pos, msgs, schema = 0, [], None
    while True:
        tot, isb = _msg_size(buf, pos)
        if tot is None: break
        if schema is None and not isb and tot > 8: schema = buf[pos:pos+tot]
        elif isb: msgs.append(buf[pos:pos+tot])
        pos += tot
    blob = schema + b"".join(msgs) + b"\xff\xff\xff\xff\x00\x00\x00\x00"
    df = pl.from_arrow(pa.ipc.open_stream(pa.py_buffer(blob)).read_all())
    df = df.filter(pl.col("kind") != 255)
    return df, vm_w, time.time() - t0, rc

if __name__ == "__main__":
    root = os.path.abspath(sys.argv[1])
    df, vm_w, wall, rc = cscan(root, "/tmp/m5.emit")
    print(f"rc={rc} entries={df.height:,} vm={vm_w:.2f}s total={wall:.2f}s")
    truth_n = sum(len(ds) + len(fs) for _r, ds, fs in os.walk(root))
    ok = rc == 0 and df.height == truth_n
    if ok:
        bad = 0
        for row in df.sample(min(200, df.height), seed=3).iter_rows(named=True):
            st = os.lstat(os.path.join(root, row["name"]))
            if (st.st_mode & 0o7777) != (row["mode"] & 0o7777): bad += 1
        ok = bad == 0
    print("M5 C-WALKER LEAF PASS" if ok else f"M5 FAIL entries={df.height} truth={truth_n}")
    sys.exit(0 if ok else 1)

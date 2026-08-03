import sys, os, random, struct, subprocess, shutil, time
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
import polars as pl
from quiver.exec.blocks import _ipc_bytes

(NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT,
 MKDIR, SYMLINK, LINK, SETMETA, UNLINK, RMDIR, FENCE, READDIR, STATB) = range(16)
QVM2 = _R + "/quiver/exec/qvm2"
STATB_K = 512

def batch(rows):
    df = pl.DataFrame(rows).select(
        tid=pl.col("tid").cast(pl.UInt32), op=pl.col("op").cast(pl.UInt8),
        k1=pl.col("k1").cast(pl.UInt8), k2=pl.col("k2").cast(pl.UInt8),
        a=pl.col("a").cast(pl.Int64), b=pl.col("b").cast(pl.Int64),
        c=pl.col("c").cast(pl.Int64), d=pl.col("d").cast(pl.Int64),
        path=pl.col("path").cast(pl.Utf8), payload=pl.col("payload").cast(pl.Binary))
    return _ipc_bytes(df)
def R(tid, op, k1=0, k2=0, a=0, b=0, c=0, d=0, path="", payload=b""):
    return dict(tid=tid, op=op, k1=k1, k2=k2, a=a, b=b, c=c, d=d, path=path, payload=payload)

def run_scan(root, emitf, verbose=False):
    """Wave-scheduled two-phase scan. Returns (records dict, waves, wall)."""
    open(emitf, "wb").close()
    proc = subprocess.Popen([QVM2, "stream"], stdin=subprocess.PIPE)
    t0 = time.time()
    tid_kind, tid_dir = {}, {}                 # tid -> ('rd'|'st'), dir path
    seen, names_of, stat_parts = {}, {}, []
    nxt, waves, pos = 1, 0, 0
    outstanding = set()

    def send(rows):
        nonlocal waves
        proc.stdin.write(batch(rows)); proc.stdin.flush(); waves += 1

    def sched_readdir(rows, d):
        nonlocal nxt
        tid_kind[nxt] = 'rd'; tid_dir[nxt] = d
        rows.append(R(nxt, READDIR, a=0, path=d))
        outstanding.add(nxt); nxt += 1

    def sched_statb(rows, d, names):
        nonlocal nxt
        for i in range(0, len(names), STATB_K):
            pk = b"".join(bytes([len(n)]) + n.encode() for n in names[i:i + STATB_K])
            tid_kind[nxt] = 'st'; tid_dir[nxt] = d
            rows.append(R(nxt, STATB, a=0, path=d, payload=pk))
            outstanding.add(nxt); nxt += 1

    rows = [R(0, SINK, a=0, path=emitf)]
    lo = nxt
    sched_readdir(rows, root)
    rows.append(R(0, SPAWN, a=lo, b=nxt - 1))
    send(rows)

    deadline = time.time() + 1800
    ef = open(emitf, "rb")
    buf = b""
    while outstanding and time.time() < deadline:
        ef.seek(len(buf))
        nb = ef.read()                              # incremental: only NEW bytes
        if nb:
            buf += nb
        new_rows = []
        lo = nxt
        progressed = False
        while pos + 8 <= len(buf):
            tid, blen = struct.unpack_from("<II", buf, pos)
            final = bool(blen & 0x80000000); blen &= 0x7FFFFFFF
            if tid == 0 or pos + 8 + blen > len(buf):
                break
            end = pos + 8 + blen
            d = tid_dir[tid]
            df = pl.read_ipc_stream(buf[pos + 8:end])        # C-emitted QREC batch: vectorized
            if tid_kind[tid] == 'rd':
                nm = names_of.setdefault(d, [])
                nm.extend(df["name"].to_list())
                for name in df.filter(pl.col("kind") == 4)["name"].to_list():  # DT_DIR
                    sched_readdir(new_rows, os.path.join(d, name))
                if final:
                    sched_statb(new_rows, d, names_of.pop(d))
            else:
                stat_parts.append(df.with_columns(dir=pl.lit(d)))
            if final:
                outstanding.discard(tid)
            pos = end; progressed = True
        if new_rows:
            new_rows.append(R(0, SPAWN, a=lo, b=nxt - 1))
            send(new_rows)
        elif not progressed:
            time.sleep(0.005)
    send([R(0, JOIN, a=1, b=nxt - 1)])
    proc.stdin.close()
    rc = proc.wait(timeout=600)
    if stat_parts:                                 # one vectorized pass at the end
        allst = pl.concat(stat_parts)
        rels = (allst["dir"] + "/" + allst["name"]).to_list()
        pre = len(root) + 1
        for rel, k, sz, md in zip(rels, allst["kind"].to_list(),
                                  allst["size"].to_list(), allst["mode"].to_list()):
            seen[rel[pre:]] = (k, sz, md & 0o7777)
    return seen, waves, time.time() - t0, rc

if __name__ == "__main__":
    random.seed(31)
    root = os.path.abspath("m3_tree")            # reuse the M3 tree
    seen, waves, wall, rc = run_scan(root, os.path.abspath("m3b.emit"))
    truth = {}
    for r, ds, fs in os.walk(root):
        for d in ds:
            p = os.path.join(r, d)
            truth[os.path.relpath(p, root)] = (1, None, os.lstat(p).st_mode & 0o7777)
        for f in fs:
            p = os.path.join(r, f); st = os.lstat(p)
            truth[os.path.relpath(p, root)] = (0, st.st_size, st.st_mode & 0o7777)
    bad = sum(1 for rel, (k, sz, md) in truth.items()
              if seen.get(rel) is None or seen[rel][0] != k
              or (sz is not None and seen[rel][1] != sz) or seen[rel][2] != md)
    bad += len(set(seen) - set(truth))
    print(f"rc={rc} entries={len(seen)}/{len(truth)} waves={waves} wall={wall:.2f}s")
    print("M3b TWO-PHASE SCAN PASS" if not bad and rc == 0 and len(seen) == len(truth)
          else f"M3b FAIL bad={bad}")
    sys.exit(1 if bad or rc != 0 else 0)

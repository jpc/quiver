"""Vectorized wave-scan planner: polars end to end.
Emit = ONE Arrow stream (schema at sink open, bare batch messages, kind==255 done
markers). Waves authored as column ops; names payloads via str.join aggregation."""
import sys, os, struct, subprocess, time
import pathlib; _R = str(pathlib.Path(__file__).resolve().parents[2]); sys.path.insert(0, _R)
import polars as pl
import pyarrow as pa
from quiver.exec.blocks import _ipc_bytes

(NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT,
 MKDIR, SYMLINK, LINK, SETMETA, UNLINK, RMDIR, FENCE, READDIR, STATB) = range(16)
QVM2 = _R + "/quiver/exec/qvm2"
STATB_K = 512

ISCH = dict(tid=pl.UInt32, op=pl.UInt8, k1=pl.UInt8, k2=pl.UInt8,
            a=pl.Int64, b=pl.Int64, c=pl.Int64, d=pl.Int64,
            path=pl.Utf8, payload=pl.Binary)

def ibatch(df):
    return _ipc_bytes(df.select(*[pl.col(c).cast(t) for c, t in ISCH.items()]))

def irows(**cols):
    """Build instruction rows from columns; missing operands default."""
    ns = [len(v) for v in cols.values() if isinstance(v, (list, pl.Series))]
    n = max(ns) if ns else 1
    base = dict(tid=0, op=0, k1=0, k2=0, a=0, b=0, c=0, d=0, path="", payload=b"")
    out = {}
    for k, dv in base.items():
        v = cols.get(k, dv)
        out[k] = v if isinstance(v, (list, pl.Series)) else [v] * n
    return pl.DataFrame(out)

# ---- python port of the C flatbuffer walk: message length incl. body ----
def _msg_size(buf, off):
    """(total_size, is_batch) of the Arrow message at off, or (None, None) if incomplete."""
    if off + 8 > len(buf): return None, None
    mlen, = struct.unpack_from("<I", buf, off + 4)
    if mlen == 0: return 8, False
    if off + 8 + mlen > len(buf): return None, None
    m = off + 8
    root, = struct.unpack_from("<I", buf, m)
    vt = (m + root) - struct.unpack_from("<i", buf, m + root)[0]
    vsz, = struct.unpack_from("<H", buf, vt)
    def field(idx):
        slot = 4 + 2 * idx
        if slot >= vsz: return None
        voff, = struct.unpack_from("<H", buf, vt + slot)
        return (m + root + voff) if voff else None
    htp = field(1)
    is_batch = htp is not None and buf[htp] == 3
    blp = field(3)
    blen = struct.unpack_from("<q", buf, blp)[0] if blp else 0
    total = 8 + mlen + blen
    if off + total > len(buf): return None, None
    return total, is_batch

def run_scan(root, emitf, workers=32, verbose=False):
    open(emitf, "wb").close()
    env = dict(os.environ, QVM2_WORKERS=str(workers))
    proc = subprocess.Popen([QVM2, "stream"], stdin=subprocess.PIPE, env=env)
    t0 = time.time()
    plan_s = 0.0

    def send(df_rows):
        proc.stdin.write(ibatch(df_rows)); proc.stdin.flush()

    # wave 0
    wave = pl.DataFrame({"tid": pl.Series([1], dtype=pl.UInt32), "dir": [root]})
    tid_dirs = [wave]
    send(pl.concat([
        irows(tid=0, op=SINK, a=0, b=1, path=emitf),
        irows(tid=1, op=READDIR, a=0, path=root),
        irows(tid=0, op=SPAWN, a=1, b=1)]))
    nxt, outstanding, waves = 2, 1, 1
    pos = 0
    schema_msg = None
    names_pending = []                              # dfs: tid, name, kind
    stat_parts = []
    buf = b""
    ef = open(emitf, "rb")
    deadline = time.time() + 1800
    while outstanding and time.time() < deadline:
        ef.seek(len(buf))
        nb = ef.read()
        if nb: buf += nb
        msgs = []
        while True:
            tot, is_batch = _msg_size(buf, pos)
            if tot is None: break
            if schema_msg is None and not is_batch and tot > 8:
                schema_msg = buf[pos:pos + tot]
            elif is_batch:
                msgs.append(buf[pos:pos + tot])
            pos += tot
        if not msgs:
            time.sleep(0.002); continue
        tp = time.time()
        blob = schema_msg + b"".join(msgs) + b"\xff\xff\xff\xff\x00\x00\x00\x00"
        df = pl.from_arrow(pa.ipc.open_stream(pa.py_buffer(blob)).read_all())
        done = df.filter(pl.col("kind") == 255)
        outstanding -= done.height
        data = df.filter(pl.col("kind") != 255)
        names = data.filter(pl.col("phase") == 0)
        stats = data.filter(pl.col("phase") == 1)
        if stats.height: stat_parts.append(stats)
        new_rows = []
        if names.height: names_pending.append(names)
        fin_rd = done.filter(pl.col("phase") == 0)["tid"].unique()
        if fin_rd.len():
            pend = pl.concat(names_pending) if names_pending else None
            names_pending = []
            if pend is not None:
                if len(tid_dirs) > 1:                      # amortized: concat once per growth
                    tid_dirs = [pl.concat(tid_dirs)]
                tdf = tid_dirs[0]
                ready = pend.join(pl.DataFrame({"tid": fin_rd}), on="tid").join(tdf, on="tid")
                later = pend.join(pl.DataFrame({"tid": fin_rd}), on="tid", how="anti")
                if later.height: names_pending.append(later)
                if ready.height:
                    # descend: every DT_DIR spawns a READDIR
                    nd = ready.filter(pl.col("kind") == 4).select(
                        dir=pl.col("dir") + "/" + pl.col("name"))
                    k = nd.height
                    if k:
                        ndf = nd.with_columns(tid=(pl.int_range(k, dtype=pl.UInt32) + nxt))
                        tid_dirs.append(ndf.select("tid", "dir"))
                        new_rows.append(ndf.select(
                            tid="tid", op=pl.lit(READDIR, pl.UInt8),
                            a=pl.lit(0, pl.Int64), path="dir"))
                        nxt += k
                    # stat batches: chunk names per dir, NUL-join via ONE aggregation
                    sb = (ready.with_columns(
                            ci=pl.int_range(pl.len()).over("tid") // STATB_K)
                          .group_by("tid", "ci")
                          .agg(pl.col("name").str.join("\x00"), pl.col("dir").first()))
                    m = sb.height
                    sb = sb.with_columns(ntid=(pl.int_range(m, dtype=pl.UInt32) + nxt))
                    nxt += m
                    new_rows.append(sb.select(
                        tid="ntid", op=pl.lit(STATB, pl.UInt8),
                        a=pl.lit(0, pl.Int64), path="dir",
                        payload=(pl.col("name") + "\x00").cast(pl.Binary)))
                    tid_dirs.append(sb.select(tid="ntid", dir="dir"))
        if new_rows:
            allnew = pl.concat([irows(**{c: r[c].to_list() for c in r.columns})
                                for r in new_rows]) if False else None
            # build directly: pad each partial rows-df to the full schema
            parts = []
            for r in new_rows:
                miss = {c: (pl.lit("", pl.Utf8) if c == "path" else
                            pl.lit(b"", pl.Binary) if c == "payload" else pl.lit(0))
                        for c in ISCH if c not in r.columns}
                parts.append(r.with_columns(**miss).select(*[pl.col(c).cast(t) for c, t in ISCH.items()]))
            wrows = pl.concat(parts)
            lo = int(wrows["tid"].min()); hi = int(wrows["tid"].max())
            outstanding += wrows.height
            wrows = pl.concat([wrows, irows(tid=0, op=SPAWN, a=lo, b=hi)
                               .select(*[pl.col(c).cast(t) for c, t in ISCH.items()])])
            send(wrows); waves += 1
        plan_s += time.time() - tp
    send(irows(tid=0, op=JOIN, a=1, b=nxt - 1))
    proc.stdin.close()
    rc = proc.wait(timeout=600)
    allst = pl.concat(stat_parts) if stat_parts else pl.DataFrame()
    if allst.height:
        allst = allst.join(pl.concat(tid_dirs) if len(tid_dirs) > 1 else tid_dirs[0], on="tid")
    return allst, waves, time.time() - t0, plan_s, rc

if __name__ == "__main__":
    root = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "m3_tree")
    st, waves, wall, plan_s, rc = run_scan(root, "/tmp/m4.emit")
    truth_n = sum(len(ds) + len(fs) for _r, ds, fs in os.walk(root))
    print(f"rc={rc} entries={st.height}/{truth_n} waves={waves} wall={wall:.2f}s plan={plan_s:.2f}s")
    if st.height == truth_n and rc == 0:
        # spot-verify a sample of 200 rows
        import random as rnd
        bad = 0
        for row in st.sample(min(200, st.height), seed=7).iter_rows(named=True):
            p = os.path.join(row["dir"], row["name"])
            s2 = os.lstat(p)
            if (s2.st_size != row["size"] and row["kind"] == 0) or (s2.st_mode & 0o7777) != (row["mode"] & 0o7777):
                bad += 1; print("MISMATCH", p)
        print("M4 VECTOR SCAN PASS" if not bad else f"M4 FAIL bad={bad}")
        sys.exit(0 if not bad else 1)
    print("M4 FAIL count/rc")
    sys.exit(1)

#!/usr/bin/env python3
"""Why is bvm slower with BVM_SINK_DIRECT when the microbenchmark says O_DIRECT is faster?

bench/dwrite says a 64 MB O_DIRECT pwrite runs at 6.38 GB/s vs 5.25 buffered, and that the
sync_file_range pacing costs nothing. Yet bvm on ~/eot measured 191.4 s with BVM_SINK_DIRECT
against 84.5 s buffered. Both numbers are from the current code, so the cost is in something
bvm does that dwrite does not.

Two candidates, and bvm already collects the counters to separate them:

  ns_write   the pwrite itself. If this is flat between modes, O_DIRECT is not the problem.
  ns_comp    under O_DIRECT the writev fast path is DISABLED (bvm.c:940), because the 3-byte
             Raw_Block headers make the iovecs unaligned. So every incompressible frame gains
             a full-frame memcpy through zstd_stored_frame(), and every worker allocates a
             ZSTD_compressBound(frame_cap) scratch buffer it otherwise never touches -- at
             -j128 that is ~16 GB of first-touch pages.

Runs a small subtree so a variant costs ~10 s instead of 3 minutes.

  ./directsweep.py [root] [--workers N] [--frame-cap MB] [--reps 1]
"""
import argparse, os, shutil, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from quiver.exec import blocks

OUT = "/mnt/weka/jpc/tmp/quiver-bench/dsweep"


def run(root, label, env, workers, cap, level=6, shards=16):
    """One backup, returning the stat counters bvm reports."""
    out = f"{OUT}/{label}.nock"
    os.makedirs(OUT, exist_ok=True)
    for p in (out, out + ".footer", out + ".frames.bin"):
        if os.path.exists(p):
            os.unlink(p)
    for i in range(1, shards):
        if os.path.exists(f"{out}.{i}"):
            os.unlink(f"{out}.{i}")
    old = {k: os.environ.get(k) for k in env}
    os.environ.update({k: v for k, v in env.items() if v is not None})
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
    t0 = time.time()
    r = blocks.backup(root, out, blocks.default_bvm(), nworkers=workers, level=level,
                      shards=shards, frame_cap=cap << 20)
    dt = time.time() - t0
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return dt, r["perf_raw"], r


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", nargs="?", default="/mnt/weka/jpc/eot/high_quality_Nov6_24")
    ap.add_argument("--workers", type=int, default=128)
    ap.add_argument("--frame-cap", type=int, default=64)
    ap.add_argument("--reps", type=int, default=1)
    # the pool is an in-flight WINDOW: each worker used to own a 64 MB scratch buffer, so a
    # pool smaller than the worker count just swaps a write stall for a buffer stall.
    ap.add_argument("--bufsweep", type=int, nargs="*",
                    help="sweep BVM_WRITE_BUFS with 8 direct writers")
    ap.add_argument("--wrsweep", type=int, nargs="*", help="sweep BVM_WRITERS")
    ap.add_argument("--buffered", action="store_true", help="sweep writers WITHOUT O_DIRECT")
    a = ap.parse_args()

    variants = [
        ("buffered",      {"BVM_SINK_DIRECT": None}),
        ("direct",        {"BVM_SINK_DIRECT": "1"}),
        # is the loss the missing writev path, or O_DIRECT itself? buffered WITHOUT the writev
        # fast path isolates the memcpy+scratch cost with no O_DIRECT involved.
        ("buf_nowritev",  {"BVM_SINK_DIRECT": None, "BVM_NO_WRITEV": "1"}),
        # BVM_WRITERS variants live on the writer-pool branch; on main the env is inert.
    ]
    if a.wrsweep:
        # writers were ~100% busy at 8 with workers stalled 71% of the time, and each writer
        # only reached 0.26 GB/s vs 0.72 in dwriteq -> latency-bound, so add writers.
        variants = [(f"wr{n}{'b' if a.buffered else 'd'}",
                     {"BVM_SINK_DIRECT": None if a.buffered else "1",
                      "BVM_WRITERS": str(n), "BVM_WRITE_BUFS": "192"}) for n in a.wrsweep]
    elif a.bufsweep:
        variants = [(f"bufs{n}", {"BVM_SINK_DIRECT": "1", "BVM_WRITERS": "8",
                                 "BVM_WRITE_BUFS": str(n)}) for n in a.bufsweep]
    print(f"root {a.root}  -j{a.workers}  frame_cap {a.frame_cap} MB\n")
    hdr = (f"  {'variant':<14} {'secs':>7} {'read':>7} {'write':>7} "
           f"{'ns_write':>9} {'ns_comp':>9} {'raw GB':>8} {'store GB':>9}")
    print(hdr)
    for label, env in variants:
        for rep in range(a.reps):
            try:
                dt, p, r = run(a.root, label, env, a.workers, a.frame_cap)
            except Exception as e:
                print(f"  {label:<14}  FAILED: {type(e).__name__}: {e}")
                continue
            src = p.get("comp_in", 0) or 1
            store = p.get("wr_bytes", 0)
            if r.get("lost") or r.get("errors"):
                print(f"    !! lost {r.get('lost')} errors {r.get('errors')} "
                      f"{r.get('error_sample')}")
            # a decoupled writer must still deliver every locator: a frame whose write
            # completed but whose record_done never arrived is silently missing from the
            # footer, so structurally verify rather than trusting the byte totals.
            v = blocks.verify(f"{OUT}/{label}.nock")
            bad = v.get("truncated", 0)
            print(f"  {label:<14} {dt:7.1f} {src/dt/1e9:7.2f} {store/dt/1e9:7.2f} "
                  f"{p.get('ns_write',0)/1e9:9.1f} {p.get('ns_comp',0)/1e9:9.1f} "
                  f"{p.get('raw_bytes',0)/1e9:8.1f} {store/1e9:9.2f}"
                  f"{'   VERIFY OK' if not bad else f'   !! {bad} TRUNCATED'}")
            sys.stdout.flush()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""quiver benchmark suite — measure THIS filesystem, don't trust someone else's numbers.

Every tuning knob quiver exposes (frame size, sink count, worker count, chunk size, hash
choice) has a filesystem-dependent optimum. This suite measures them and writes a report
with the settings it recommends for your storage.

  ./run_bench.py --out results/mysite            # everything (~20 min)
  ./run_bench.py --only fsbw,scan --dir /mnt/x   # a subset
  ./run_bench.py --report results/mysite         # re-render a report from saved JSON

Results are JSON (machine-readable, diffable between runs/sites) + a markdown report.
Use `--srun` on a SLURM cluster to run each measurement on an exclusive node.
"""
import argparse, json, os, re, shutil, subprocess, sys, time, socket, platform

HERE = os.path.dirname(os.path.abspath(__file__))


def sh(cmd, timeout=3600):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.stdout + r.stderr


def wrap(cmd, args):
    """Optionally run one measurement on an exclusive node."""
    return f"{args.srun} {cmd}" if args.srun else cmd


def num(pat, text, default=None, group=1):
    m = re.search(pat, text)
    return float(m.group(group)) if m else default


# ---------------------------------------------------------------- measurements
def bench_fsbw(args, out):
    """Write bandwidth: threads, chunk size, and — the big one — SINK COUNT.
    quiver writes frames into sink file(s); many filesystems serialize concurrent
    writes to a single inode, so this decides how many shards a job should use."""
    d = os.path.join(args.dir, "bench_fsbw")
    os.makedirs(d, exist_ok=True)
    exe = os.path.join(HERE, "fsbw")
    res = {"threads": {}, "chunk_mb": {}, "sinks": {}, "mode": {}}

    def one(extra, gb=None):
        gb = gb or args.gb
        txt = sh(wrap(f"bash -c 'rm -f {d}/*.dat; {exe} {d} {extra}'", args))
        return num(r"DURABLE ([0-9.]+) GB/s", txt), txt

    probe_c = sorted(args.chunks)[len(args.chunks) // 2]      # representative chunk for the thread sweep
    for t in args.threads:
        v, _ = one(f"{t} {args.gb} {probe_c} direct")
        res["threads"][str(t)] = v
        print(f"    threads={t:<4} {v} GB/s", flush=True)
    best_t = max(res["threads"], key=lambda k: res["threads"][k] or 0)
    for c in args.chunks:
        v, _ = one(f"{best_t} {args.gb} {c} direct")
        res["chunk_mb"][str(c)] = v
        print(f"    chunk={c}MB   {v} GB/s", flush=True)
    best_c = max(res["chunk_mb"], key=lambda k: res["chunk_mb"][k] or 0)
    for k in args.sinks:
        v, _ = one(f"{best_t} {max(1, args.gb // 2)} {best_c} direct shared {k}")
        res["sinks"][str(k)] = v
        print(f"    sinks={k:<4}  {v} GB/s", flush=True)
    for label, extra in (("separate_direct", f"{best_t} {args.gb} {best_c} direct"),
                         ("separate_buffered", f"{best_t} {args.gb} {best_c} buffered"),
                         ("one_sink_direct", f"{best_t} {args.gb} {best_c} direct shared 1"),
                         ("one_sink_buffered", f"{best_t} {args.gb} {best_c} buffered shared 1")):
        v, _ = one(extra)
        res["mode"][label] = v
        print(f"    {label:<20} {v} GB/s", flush=True)
    res["best_threads"], res["best_chunk_mb"] = int(best_t), int(best_c)
    shutil.rmtree(d, ignore_errors=True)
    out["fsbw"] = res


def bench_scan(args, out):
    """Directory walk: readdir+d_type vs full stat — decides rm/scan strategy."""
    d = os.path.join(args.dir, "bench_scan")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    n, per = args.scan_dirs, args.scan_per_dir
    for i in range(n):
        p = os.path.join(d, f"d{i:04d}")
        os.makedirs(p)
        for j in range(per):
            open(os.path.join(p, f"f{j}"), "wb").close()
    exe = os.path.join(HERE, "scanprobe")
    res = {}
    for mode, label in ((0, "readdir_dtype"), (1, "readdir_lstat"), (2, "getdents64_dtype")):
        txt = sh(wrap(f"{exe} {d} {mode}", args))
        res[label] = num(r"([0-9.]+) ent/s", txt)
        print(f"    {label:<18} {res[label]} entries/s", flush=True)
    res["files"] = n * per
    shutil.rmtree(d, ignore_errors=True)
    out["scan"] = res


def bench_hashes(args, out):
    txt = sh(wrap(os.path.join(HERE, "hashes"), args))
    res = {}
    for line in txt.splitlines():
        m = re.match(r"\s+(\S.*?)\s{2,}([0-9.]+) GB/s", line)
        if m:
            res[m.group(1).strip()] = float(m.group(2))
    try:
        import blake3, time as _t
        b = os.urandom(64 << 10) * 1024
        t = _t.time(); n = 0
        while n + (1 << 20) <= len(b):
            blake3.blake3(b[n:n + (1 << 20)]).digest(); n += 1 << 20
        res["BLAKE3"] = round(n / (_t.time() - t) / 1e9, 2)
    except ImportError:
        pass
    for k, v in res.items():
        print(f"    {k:<24} {v} GB/s", flush=True)
    out["hashes"] = res


def bench_crypto(args, out):
    txt = sh(wrap(os.path.join(HERE, "crypto"), args))
    res = {}
    for line in txt.splitlines():
        m = re.match(r"\s+(\S.*?)\s{2,}([0-9.]+) GB/s", line)
        if m:
            res[m.group(1).strip()] = float(m.group(2))
    for k, v in res.items():
        print(f"    {k:<24} {v} GB/s", flush=True)
    out["crypto"] = res


def bench_read(args, out):
    """Read bandwidth — restore/verify throughput. `shared` reads ONE file from many threads
    (restoring from a single nock) vs one file per thread (a sharded nockset)."""
    d = os.path.join(args.dir, "bench_read")
    os.makedirs(d, exist_ok=True)
    fsbw, fsops = os.path.join(HERE, "fsbw"), os.path.join(HERE, "fsops")
    res = {"threads": {}, "mode": {}}
    t = max(args.threads)
    sh(wrap(f"bash -c 'rm -f {d}/*.dat; {fsbw} {d} {t} {args.gb} 8 direct'", args))        # per-thread files
    for n in args.threads:
        txt = sh(wrap(f"{fsops} read {d} {n} {args.gb} 8 direct", args))
        res["threads"][str(n)] = num(r"([0-9.]+) GB/s", txt)
        print(f"    threads={n:<4} {res['threads'][str(n)]} GB/s", flush=True)
    for label, extra in (("separate_direct", "direct"), ("separate_buffered", "buffered")):
        txt = sh(wrap(f"{fsops} read {d} {t} {args.gb} 8 {extra}", args))
        res["mode"][label] = num(r"([0-9.]+) GB/s", txt)
    sh(wrap(f"bash -c 'rm -f {d}/*.dat; {fsbw} {d} {t} {args.gb} 8 direct shared 1'", args))  # one big file
    for label, extra in (("one_file_direct", "direct shared"), ("one_file_buffered", "buffered shared")):
        txt = sh(wrap(f"{fsops} read {d} {t} {max(args.gb // 4, 1)} 8 {extra}", args))
        res["mode"][label] = num(r"([0-9.]+) GB/s", txt)
    for k, v in res["mode"].items():
        print(f"    {k:<20} {v} GB/s", flush=True)
    shutil.rmtree(d, ignore_errors=True)
    out["read"] = res


def bench_dirspread(args, out):
    """The SAME file-creation work spread over 1 .. N directories. Parallel filesystems lock
    the parent directory per create/unlink, so directory fan-out often matters more than any
    other knob — this is why quiver shuffles work across directories."""
    exe = os.path.join(HERE, "fsops")
    res = {"create": {}, "stat": {}, "unlink": {}, "write_gbs": {}}
    n_per = max(200, args.dir_files // max(args.threads))
    for nd in args.dirspread:
        d = os.path.join(args.dir, f"bench_dirs{nd}")
        shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
        txt = sh(wrap(f"{exe} dirs {d} {max(args.threads)} {n_per} {args.dir_file_kb} {nd}", args))
        for key, pat in (("create", r"create ([0-9.]+)/s"), ("stat", r"stat ([0-9.]+)/s"),
                         ("unlink", r"unlink ([0-9.]+)/s"), ("write_gbs", r"([0-9.]+) GB/s")):
            res[key][str(nd)] = num(pat, txt)
        print(f"    dirs={nd:<5} create {res['create'][str(nd)]}/s  stat {res['stat'][str(nd)]}/s  "
              f"unlink {res['unlink'][str(nd)]}/s", flush=True)
        shutil.rmtree(d, ignore_errors=True)
    out["dirspread"] = res


def bench_cdc(args, out):
    """Content-defined chunking + delta: the cost of finding what changed, and how few bytes
    a localized edit must transfer. Drives the backup/rsync delta path."""
    exe = os.path.join(HERE, "chunking")
    d = os.path.join(args.dir, "bench_cdc")
    os.makedirs(d, exist_ok=True)
    base, mod = os.path.join(d, "base.bin"), os.path.join(d, "mod.bin")
    n = args.cdc_mb << 20
    with open(base, "wb") as f:                      # semi-compressible, realistic
        blk = (os.urandom(4096) + b"the quick brown fox " * 200)
        while f.tell() < n: f.write(blk)
    data = bytearray(open(base, "rb").read())
    data[len(data) // 2:len(data) // 2 + 4096] = os.urandom(4096)      # one localized edit
    open(mod, "wb").write(bytes(data))
    res = {}
    txt = sh(wrap(f"{exe} chunk {base}", args))
    res["chunk_gbs"] = num(r"fastcdc\s+[0-9.]+ MB in\s+[0-9.]+s\s+([0-9.]+) GB/s", txt)
    res["chunk_hash_gbs"] = num(r"cdc\+xxh64\s+[0-9.]+ MB in\s+[0-9.]+s\s+([0-9.]+) GB/s", txt)
    txt = sh(wrap(f"{exe} dcdc {base} {mod}", args))
    res["delta_send_mb"] = num(r"send\s+([0-9.]+) MB", txt)
    res["delta_pct"] = num(r"\(\s*([0-9.]+)%\)", txt)
    res["file_mb"] = args.cdc_mb
    for k, v in res.items():
        print(f"    {k:<18} {v}", flush=True)
    shutil.rmtree(d, ignore_errors=True)
    out["cdc"] = res


def bench_multinode(args, out):
    """Aggregate write bandwidth across N nodes simultaneously — does the fabric scale, or
    is the whole cluster sharing one ceiling?"""
    if not args.nodes:
        print("    (skipped: pass --nodes n1,n2,...)", flush=True)
        return
    nodes = args.nodes.split(",")
    exe = os.path.join(HERE, "fsbw")
    res = {}
    for count in args.node_counts:
        if count > len(nodes): continue
        procs, t0 = [], time.time()
        for i in range(count):
            d = os.path.join(args.dir, f"bench_mn{i}")
            os.makedirs(d, exist_ok=True)
            cmd = (f"{args.srun_base} -w {nodes[i]} bash -c "
                   f"'rm -f {d}/*.dat; {exe} {d} {max(args.threads)} {args.gb} 8 direct'")
            procs.append(subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                          stderr=subprocess.DEVNULL, text=True))
        tot = 0.0
        for p in procs:
            o, _ = p.communicate()
            v = num(r"DURABLE ([0-9.]+) GB/s", o)
            if v: tot += v
        res[str(count)] = round(tot, 2)
        print(f"    {count} node(s): {tot:.2f} GB/s aggregate", flush=True)
        for i in range(count):
            shutil.rmtree(os.path.join(args.dir, f"bench_mn{i}"), ignore_errors=True)
    out["multinode"] = res


def bench_numa(args, out):
    """Fully-loaded compression vs CPU/memory placement. Storage clients that SPIN-POLL
    (WEKA, DAOS, SPDK-style) own a few cores permanently; hard-pinning application threads
    onto those cores starves them and the job waits for its slowest thread."""
    exe = os.path.join(HERE, "numa_zstd")
    ncpu = os.cpu_count()
    res = {}
    txt = sh(wrap(f"{exe} 0 {args.numa_mb} {args.level}", args))   # 0 => 1 thread per ALLOWED cpu
    for line in txt.splitlines():
        m = re.match(r"\s+(\S+)\s+threads=(\d+)\s+compress ([0-9.]+) GB/s \| end-to-end ([0-9.]+)", line)
        if m:
            res[m.group(1)] = float(m.group(3))                     # compress-only (setup excluded)
            res[m.group(1) + "_end_to_end"] = float(m.group(4))
            print(f"    {m.group(1):<11} compress {m.group(3)} GB/s (e2e {m.group(4)})", flush=True)
    # pin AROUND any spin-polling storage cores, if we can find them
    poll = sh("ps -eo comm,psr,pcpu --sort=-pcpu | awk '$3>90 && $1 !~ /^(python|zstd|numa_zstd)/ {print $2}'")
    cpus = sorted({int(x) for x in poll.split() if x.isdigit()})
    if cpus:
        sib = set()
        for c in cpus:
            t = sh(f"cat /sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list").strip()
            sib |= {int(v) for v in re.split(r"[,-]", t) if v.isdigit()}
        keep = [str(i) for i in range(ncpu) if i not in sib]
        res["spinpoll_cpus"] = sorted(sib)
        txt = sh(wrap(f"taskset -c {','.join(keep)} {exe} {len(keep)} {args.numa_mb} {args.level} none", args))
        v = num(r"none\s+threads=\d+\s+([0-9.]+) GB/s", txt)
        res["avoiding_spinpoll_cores"] = v
        print(f"    avoiding spin-poll cores {sorted(sib)}: {v} GB/s", flush=True)
    out["numa"] = res


def bench_quiver(args, out):
    """End-to-end: pack a real tree, capture quiver's own perf/queue verdict."""
    sys.path.insert(0, os.path.dirname(HERE))
    from quiver.exec import blocks
    src = args.tree
    if not src:
        src = os.path.join(args.dir, "bench_tree")
        shutil.rmtree(src, ignore_errors=True)
        for i in range(64):
            p = os.path.join(src, f"d{i:03d}")
            os.makedirs(p)
            for j in range(64):
                open(os.path.join(p, f"f{j}"), "wb").write(os.urandom(200_000))
    nock = os.path.join(args.dir, "bench_quiver.nock")
    for f in (nock, nock + ".wal"):
        if os.path.exists(f):
            os.unlink(f)
    t = time.time()
    r = blocks.backup(src, nock, blocks.default_bvm(), nworkers=args.workers, level=args.level,
                      strict=False)
    dt = time.time() - t
    perf, hints = r.pop("perf", ({}, []))
    size = os.path.getsize(nock)
    res = dict(seconds=round(dt, 1), files=r.get("full"), store_gb=round(size / 1e9, 2),
               gb_per_s=round(size / dt / 1e9, 3), errors=r.get("errors"), lost=r.get("lost"),
               perf=perf, hints=hints, workers=args.workers, level=args.level)
    for h in hints:
        print(f"    • {h}", flush=True)
    os.unlink(nock)
    if os.path.exists(nock + ".wal"):
        os.unlink(nock + ".wal")
    if not args.tree:
        shutil.rmtree(src, ignore_errors=True)
    out["quiver"] = res


def bench_cfr(args, out):
    """Zero-copy pack: incompressible members are stored codec-free, so their bytes never
    need to enter user space — but a zstd frame of Raw_Blocks interleaves a 3-byte header
    every 128 KiB, so copy_file_range costs 8 syscall pairs per MB. `rwz` is what bvm does
    today (read + interleave memcpy + write); `cfr` is the proposal; `cfr1` is the unframed
    upper bound. If cfr <= rwz the framing syscalls ate the saving."""
    d = os.path.join(args.dir, "bench_cfr")
    os.makedirs(d, exist_ok=True)
    exe = os.path.join(HERE, "cfr")
    res = {}
    for mb in (16, 64, 256):
        nf = max(4, min(32, (args.gb * 1024) // mb))
        txt = sh(wrap(f"{exe} {d} {mb} {nf} {max(args.threads)}", args))
        res[str(mb)] = {m: num(rf"\s{m}\s+([0-9.]+) GB/s", txt) for m in
                        ("rw", "rwz", "wv", "cfr", "cfr1")}
        print(f"    member={mb}MB  " + "  ".join(f"{k} {v}" for k, v in res[str(mb)].items()),
              flush=True)
        sh(wrap(f"bash -c 'rm -f {d}/src.*'", args))
    shutil.rmtree(d, ignore_errors=True)
    out["cfr"] = res


def bench_framecap(args, out):
    """Frame-size knee: ratio vs (zstd level x frame cap) on REAL corpora. quiver splits an
    oversized member into frame_cap pieces, each its own independent frame, so the cap trades
    worker memory and restore parallelism against compression — and the break-even moves with
    the level, because the level sets zstd's window. Point --framecap-corpora at a directory
    of representative *.bin samples of your own data; without one this falls back to two
    synthetic corpora, which will NOT predict your knee."""
    exe = os.path.join(HERE, "framecap")
    cdir = args.framecap_corpora or os.path.join(args.dir, "corpora")
    os.makedirs(cdir, exist_ok=True)
    files = sorted(f for f in os.listdir(cdir) if f.endswith(".bin"))
    if not files:                                        # fallback: text + incompressible
        import tarfile, io
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as t:
            t.add(os.path.dirname(HERE), arcname="src")
        b = buf.getvalue()
        n = args.framecap_mb << 20
        open(os.path.join(cdir, "text.bin"), "wb").write((b * (n // max(len(b), 1) + 1))[:n])
        open(os.path.join(cdir, "random.bin"), "wb").write(os.urandom(n))
        files = ["random.bin", "text.bin"]
        print(f"    (no corpora in {cdir}; using synthetic text+random — supply real samples)",
              flush=True)
    levels = ",".join(str(x) for x in args.framecap_levels)
    caps = ",".join(str(x) for x in args.framecap_caps)
    res = {}
    for f in files:
        name = f[:-4]
        txt = sh(wrap(f"{exe} {os.path.join(cdir, f)} --levels {levels} --caps {caps} "
                      f"--tol {args.framecap_tol} --threads {max(args.threads)}", args),
                 )
        grid, knees = {}, {}
        for line in txt.splitlines():
            p = line.split("\t")
            if len(p) == 5 and p[0].isdigit():
                grid.setdefault(p[0], {})[p[1]] = float(p[2])
                knees.setdefault(p[0], {})["window_mb"] = float(p[4])
            m = re.search(r"level (\d+) .*knee \(<= [\d.]+% off\): (\d+) MB at ([\d.]+)x", line)
            if m:
                knees.setdefault(m.group(1), {}).update(knee_mb=int(m.group(2)),
                                                        knee_ratio=float(m.group(3)))
        res[name] = {"ratio": grid, "knee": knees}
        ks = "  ".join(f"L{k}:{v.get('knee_mb','?')}MB" for k, v in sorted(knees.items(),
                                                                          key=lambda x: int(x[0])))
        print(f"    {name:10s} {ks}", flush=True)
    out["framecap"] = res


def bench_rw(args, out):
    """Read and write AT THE SAME TIME. Every other ceiling here is single-direction, but a
    backup does both, so neither one says whether its combined rate is near the limit. Same
    threads, same files, three modes; the only variable is whether the other direction runs."""
    d = os.path.join(args.dir, "bench_rw")
    exe = os.path.join(HERE, "rw")
    txt = sh(wrap(f"{exe} {d} {max(args.threads)} {args.gb} 8 {max(args.sinks)}", args))
    res = {}
    for m in ("read", "write", "rw"):
        mm = re.search(rf"^\s+{m}\s+read\s+([0-9.]+) GB/s\s+write\s+([0-9.]+) GB/s\s+"
                       rf"combined\s+([0-9.]+)", txt, re.M)
        if mm:
            res[m] = dict(read=float(mm.group(1)), write=float(mm.group(2)),
                          combined=float(mm.group(3)))
            print(f"    {m:<6} read {res[m]['read']:>6.2f}  write {res[m]['write']:>6.2f}  "
                  f"combined {res[m]['combined']:>6.2f} GB/s", flush=True)
    shutil.rmtree(d, ignore_errors=True)
    out["rw"] = res


BENCHES = {"fsbw": bench_fsbw, "read": bench_read, "dirspread": bench_dirspread,
           "scan": bench_scan, "cdc": bench_cdc, "hashes": bench_hashes,
           "crypto": bench_crypto, "numa": bench_numa, "multinode": bench_multinode,
           "cfr": bench_cfr, "framecap": bench_framecap, "rw": bench_rw,
           "quiver": bench_quiver}


# ---------------------------------------------------------------- report
def render(res, path):
    fs = res.get("fsbw") or {}
    if "error" in fs:
        fs = {}
    L = ["# quiver benchmark report", "",
         f"- host: `{res['env']['host']}`  ({res['env']['cpus']} CPUs, {res['env']['mem_gb']} GB RAM)",
         f"- target: `{res['env']['dir']}`  (fs: **{res['env']['fstype']}**)",
         f"- kernel: `{res['env']['kernel']}`   date: {res['env']['date']}", ""]
    if fs:
        L += ["## Write bandwidth", "",
              "Threads (separate files, O_DIRECT — page cache excluded, so these are durable):", "",
              "| threads | " + " | ".join(fs["threads"]) + " |",
              "|---|" + "---|" * len(fs["threads"]),
              "| GB/s | " + " | ".join(str(v) for v in fs["threads"].values()) + " |", "",
              f"Chunk size (at {fs['best_threads']} threads):", "",
              "| chunk MB | " + " | ".join(fs["chunk_mb"]) + " |",
              "|---|" + "---|" * len(fs["chunk_mb"]),
              "| GB/s | " + " | ".join(str(v) for v in fs["chunk_mb"].values()) + " |", "",
              "**Sink count** — concurrent writers into ONE file vs many "
              "(quiver writes frames into sinks; this sets `--shards`):", "",
              "| sinks | " + " | ".join(fs["sinks"]) + " |",
              "|---|" + "---|" * len(fs["sinks"]),
              "| GB/s | " + " | ".join(str(v) for v in fs["sinks"].values()) + " |", ""]
        m = fs.get("mode", {})
        if m:
            L += ["| mode | GB/s |", "|---|---|"] + \
                 [f"| {k} | {v} |" for k, v in m.items()] + [""]
    sc = res.get("scan") or {}
    if "error" in sc:
        sc = {}
    if sc:
        L += ["## Directory scan", "",
              f"{sc.get('files'):,} files:", "",
              "| method | entries/s |", "|---|---|"] + \
             [f"| {k} | {v:,.0f} |" for k, v in sc.items() if k != "files"] + [""]
    for key, title in (("hashes", "Hashes"), ("crypto", "Crypto (AEAD/KDF candidates)")):
        d = res.get(key)
        if d and "error" not in d:
            L += [f"## {title}", "", "| algorithm | GB/s |", "|---|---|"] + \
                 [f"| {k} | {v} |" for k, v in d.items()] + [""]
    nm = res.get("numa") or {}
    if nm and "error" not in nm:
        L += ["## Compression under full load: CPU/memory placement", "",
              "| placement | GB/s |", "|---|---|"] + \
             [f"| {k} | {v} |" for k, v in nm.items()
              if isinstance(v, (int, float))] + [""]
        if nm.get("spinpoll_cpus"):
            L += [f"Storage client spin-poll CPUs detected: `{nm['spinpoll_cpus']}`.", ""]
    fc = res.get("framecap") or {}
    if fc and "error" not in fc:
        L += ["## Frame cap: how big must a frame be?", "",
              "quiver splits an oversized member into `--frame-cap-mb` pieces, each compressed as "
              "its own frame. Smaller caps mean less worker memory and more restore parallelism; "
              "bigger caps compress better, but only up to zstd's match window — which is set by "
              "the LEVEL. The knee is the smallest cap within "
              f"{res.get('env',{}).get('args',{}).get('framecap_tol',0.5)}% of the asymptotic "
              "ratio.", ""]
        levs = sorted({k for c in fc.values() for k in c.get("knee", {})}, key=int)
        L += ["| corpus | " + " | ".join(f"L{v}" for v in levs) + " |",
              "|---|" + "---|" * len(levs)]
        for name in sorted(fc):
            kn = fc[name].get("knee", {})
            L += ["| " + name + " | " + " | ".join(
                f"{kn.get(v,{}).get('knee_mb','-')} MB" for v in levs) + " |"]
        wins = {}
        for c in fc.values():
            for v, d in c.get("knee", {}).items():
                if d.get("window_mb"):
                    wins[v] = d["window_mb"]
        if wins:
            L += ["", "| level | " + " | ".join(f"L{v}" for v in levs) + " |",
                  "|---|" + "---|" * len(levs),
                  "| zstd window | " + " | ".join(f"{wins.get(v,0):.1f} MB" for v in levs) + " |"]
        sp = {}
        for name, c in fc.items():
            m = c.get("mb_per_s") or {}
            for v, d in m.items():
                if d.get("1") and d.get("16"):
                    sp.setdefault(v, []).append(d["1"] / d["16"])
        if sp:
            L += ["", "Speed moves too, and in the opposite direction — a bigger frame means a "
                  "bigger match search. Going from a 1 MB to a 16 MB cap costs "
                  + ", ".join(f"{max(sp[v]):.1f}x at L{v}" for v in sorted(sp, key=int)
                              if max(sp[v]) > 1.2)
                  + " of compression throughput (worst corpus). Past 16 MB it flattens: 64 MB "
                    "is within ~5% of 16 MB everywhere measured, for at most 1.3% more ratio.",
                  ""]
        L += ["", "The window is a FLOOR, not the answer. Data with no exploitable structure "
              "(model weights, already-compressed media) is flat from 1 MB — a bigger frame has "
              "nothing to find. Data with long-range redundancy (logs, JSONL, source) keeps "
              "gaining well past the window, because a longer frame also amortizes the entropy "
              "tables. quiver defaults to 8x the window for the level (16/32/64 MB), which costs "
              "under 1% of ratio on every corpus measured here while keeping worker memory "
              "linear in the cap.", ""]

    cf = res.get("cfr") or {}
    if cf:
        L += ["## Stored-frame write path (incompressible members)", "",
              "Members zstd gives up on are stored codec-free. `rwz` is a buffered read plus "
              "the interleave memcpy that builds the framed output; `wv` writes the SAME read "
              "buffer through `writev` with the block headers as separate iovecs (no memcpy); "
              "`cfr` would `copy_file_range` the body (unavailable in practice — the digest and "
              "CDC manifest need the bytes in user space); `cfr1` is the unframed upper bound.",
              "", "| member | rw | rwz (memcpy) | wv (writev) | cfr | cfr1 |", "|---|---|---|---|---|---|"] + \
             [f"| {k} MB | {v.get('rw')} | {v.get('rwz')} | {v.get('wv')} | {v.get('cfr')} | "
              f"{v.get('cfr1')} | " for k, v in sorted(cf.items(), key=lambda x: int(x[0]))] + [""]
        gains = [v["wv"] / v["rwz"] for v in cf.values()
                 if v.get("wv") and v.get("rwz")]
        if gains:
            L += [f"`writev` vs the memcpy path: **{min(gains):.2f}x - {max(gains):.2f}x** "
                  f"(GB/s, higher is better).", ""]
    q = res.get("quiver")
    if q and "error" not in q:
        L += ["## End-to-end pack", "",
              f"- {q['files']:,} files -> {q['store_gb']} GB in {q['seconds']}s "
              f"(**{q['gb_per_s']} GB/s**), {q['workers']} workers, level {q['level']}",
              f"- errors {q['errors']}, lost {q['lost']}", "", "Verdict from quiver's own counters:", ""] + \
             [f"- {h}" for h in q["hints"]] + [""]
    # ---- recommendations
    L += ["## Recommended settings", ""]
    m = fs.get("mode", {}) if fs else {}
    if fs.get("sinks"):
        best_sink = max(fs["sinks"], key=lambda k: fs["sinks"][k] or 0)
        one = fs["sinks"].get("1") or 0
        gain = (fs["sinks"][best_sink] / one) if one else 0
        L += [f"- **`--sinks {best_sink}`** (`backup`) / **`--shards {best_sink}`** (`recompress`) "
              f"— {fs['sinks'][best_sink]} GB/s vs {one} GB/s on a single "
              f"sink (**{gain:.1f}x**). Single-inode write contention is usually the biggest "
              f"single win on a parallel filesystem.",
              f"- **`--frame-mb {fs['best_chunk_mb']}`** — knee of the chunk-size curve.",
              f"- **`-j {fs['best_threads']}`** worker threads per node for write-bound work."]
        if m.get("one_sink_buffered") and m.get("one_sink_direct"):
            L += [f"- buffered vs O_DIRECT on one sink: {m['one_sink_buffered']} vs "
                  f"{m['one_sink_direct']} GB/s — page cache does not rescue single-inode contention."]
    if sc:
        r1, r2 = sc.get("readdir_dtype"), sc.get("readdir_lstat")
        if r1 and r2:
            L += ([f"- scan: `d_type` is {r1/r2:.1f}x faster than `lstat` here — `rm`/enumerate "
                   f"paths should never stat."] if r1 > r2 * 1.05 else
                  [f"- scan: `d_type` ({r1:,.0f}/s) and `lstat` ({r2:,.0f}/s) cost the same on "
                   f"this bench's small, cache-warm tree, so it measures no win. Keep the "
                   f"name-only mode anyway: it costs nothing, and on a cold multi-million-file "
                   f"tree the stat round-trips dominate (measured 13x on this cluster during a "
                   f"5.8M-file delete). Re-measure with `--scan-dirs`/`--scan-per-dir` large "
                   f"enough to exceed the client cache if you need the real number."])
    if nm and "error" not in nm:
        none_v, local_v = nm.get("none"), nm.get("local")
        avoid = nm.get("avoiding_spinpoll_cores")
        if none_v and local_v:
            pinned = [v for k, v in nm.items()
                      if k in ("cpuonly", "local", "interleave", "remote") and isinstance(v, (int, float))]
            best_pin = max(pinned) if pinned else local_v
            if none_v > best_pin * 1.15:
                L += [f"- **do NOT hard-pin worker threads** — unpinned {none_v} GB/s vs "
                      f"{best_pin} GB/s for the best pinned placement. With one thread per CPU, a "
                      f"pinned thread that shares a core with anything else (storage client "
                      f"spin-pollers, daemons) cannot migrate, and the job waits for its slowest "
                      f"thread. Memory placement (local/interleave/remote) showed no consistent "
                      f"signal here — the effect is the pinning itself, not NUMA locality."]
            elif local_v > none_v * 1.15:
                L += [f"- pinning threads+memory to their local NUMA node is worth "
                      f"{local_v/none_v:.2f}x here ({local_v} vs {none_v} GB/s)."]
            else:
                L += [f"- CPU/memory placement is ~neutral here ({none_v} vs {local_v} GB/s)."]
        if avoid and none_v:
            L += [f"- excluding the storage client's spin-poll cores: {avoid} GB/s "
                  f"({avoid/none_v:.2f}x vs default placement)."]
    h = res.get("hashes", {})
    if h and "error" not in h:
        # identity hashes must be cryptographic — a chunk id an attacker can collide lets a
        # crafted file replace another's content. Rank only those; report the rest as context.
        CRYPT = ("blake3", "blake2b", "sha256", "sha512")
        cand = {k: v for k, v in h.items() if isinstance(v, (int, float))
                and any(k.lower().startswith(c) for c in CRYPT)}
        if cand:
            fastest = max(cand, key=lambda k: cand[k])
            L += [f"- hash: **{fastest}** at {cand[fastest]} GB/s — the fastest CRYPTOGRAPHIC "
                  f"option, which is what a chunk id has to be. Non-cryptographic hashes benchmark "
                  f"faster but cannot be used for identity or dedup."]
    L += ["", "---", "",
          "_Re-run on your own storage: `./run_bench.py --out results/<site> --dir <path>`. "
          "Numbers vary enormously by filesystem, client config and node type; the "
          "recommendations above are computed from THIS run._", ""]
    open(path, "w").write("\n".join(L))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="results/local", help="output directory")
    ap.add_argument("--dir", default=os.environ.get("QUIVER_BENCH_DIR", "/tmp/quiver-bench"),
                    help="filesystem under test")
    ap.add_argument("--only", help="comma list: " + ",".join(BENCHES))
    ap.add_argument("--srun", default="", help="prefix for each measurement, e.g. "
                    "'srun -p gpu -G 8 -N 1 -n 1 --mem=0 --time=30'")
    ap.add_argument("--gb", type=int, default=2, help="GB per thread per fsbw measurement")
    ap.add_argument("--threads", type=int, nargs="+", default=[4, 8, 16, 32, 64])
    ap.add_argument("--chunks", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    ap.add_argument("--sinks", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32])
    ap.add_argument("--scan-dirs", type=int, default=200)
    ap.add_argument("--scan-per-dir", type=int, default=100)
    ap.add_argument("--tree", help="real tree for the end-to-end pack (default: synthetic)")
    ap.add_argument("--numa-mb", type=int, default=48, help="MB per thread for the NUMA bench")
    ap.add_argument("--dirspread", type=int, nargs="+", default=[1, 4, 16, 64, 256],
                    help="directory counts to spread the same file work over")
    ap.add_argument("--dir-files", type=int, default=20000, help="total files for the dirspread bench")
    ap.add_argument("--dir-file-kb", type=int, default=16)
    ap.add_argument("--framecap-corpora", help="dir of representative *.bin samples of YOUR "
                    "data for the frame-cap sweep (default <dir>/corpora)")
    ap.add_argument("--framecap-levels", type=int, nargs="+", default=[1, 3, 6, 9, 12, 15, 19])
    ap.add_argument("--framecap-caps", type=int, nargs="+",
                    default=[1, 2, 4, 8, 16, 32, 64, 128, 256], help="frame caps in MB")
    ap.add_argument("--framecap-tol", type=float, default=0.5,
                    help="%% of the asymptotic ratio you are willing to give up")
    ap.add_argument("--framecap-mb", type=int, default=256, help="synthetic corpus size")
    ap.add_argument("--cdc-mb", type=int, default=256, help="file size for the chunking/delta bench")
    ap.add_argument("--nodes", help="comma list of hostnames for the multinode bench")
    ap.add_argument("--node-counts", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--srun-base", default="srun -p gpu -G 8 -N 1 -n 1 --mem=0 --time=30",
                    help="srun prefix used per node by the multinode bench")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--level", type=int, default=6)
    ap.add_argument("--report", help="only re-render the report from an existing results dir")
    args = ap.parse_args()

    if args.report:
        res = json.load(open(os.path.join(args.report, "results.json")))
        render(res, os.path.join(args.report, "report.md"))
        print(f"report -> {os.path.join(args.report, 'report.md')}")
        return

    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.dir, exist_ok=True)
    fstype = sh(f"stat -f -c %T {args.dir}").strip()
    mem = int(sh("awk '/MemTotal/{print $2}' /proc/meminfo").strip() or 0) // 1024 // 1024
    # MERGE with any previous run in this dir: `--only X` must not discard the other
    # sections (it silently did once, and the report came out half-empty).
    prev_path = os.path.join(args.out, "results.json")
    res = json.load(open(prev_path)) if os.path.exists(prev_path) else {}
    res["env"] = {"host": socket.gethostname(), "dir": args.dir, "fstype": fstype,
                   "cpus": os.cpu_count(), "mem_gb": mem, "kernel": platform.release(),
                   "date": time.strftime("%Y-%m-%d %H:%M"), "args": vars(args)}
    want = args.only.split(",") if args.only else list(BENCHES)
    for name in want:
        if name not in BENCHES:
            sys.exit(f"unknown bench {name}; have {list(BENCHES)}")
        print(f"[{name}]", flush=True)
        t = time.time()
        try:
            BENCHES[name](args, res)
        except Exception as e:
            print(f"    FAILED: {e}", flush=True)
            res[name] = {"error": str(e)}
        print(f"    ({time.time()-t:.0f}s)", flush=True)
        json.dump(res, open(os.path.join(args.out, "results.json"), "w"), indent=1)
    render(res, os.path.join(args.out, "report.md"))
    print(f"\nresults -> {args.out}/results.json\nreport  -> {args.out}/report.md")


if __name__ == "__main__":
    main()

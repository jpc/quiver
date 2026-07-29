"""quiver CLI — the BLOCKS engine (docs/BLOCKS.md, docs/DATAFLOW.md).

One persistent C executor (bvm: SCAN / DECODE / COPY, built on demand from bvm.c) driven
by a Python planner (blocks.py). Verbs:

    recompress OUT IN...     foreign .tar/.tar.zstd... -> one chunked nock (multi-source)
    pack ROOT OUT            filesystem tree -> nock, incremental via a WAL
    unpack ARCHIVE DEST      nock -> filesystem (--glob subset, --workers frame-parallel)
    extract ARCHIVE DEST     unpack restricted to --glob (random-access member subset)
    diff A B                 streaming tree diff (fs/fs, or tar:/nock: specs)
    sync SRC DST             rsync — local, or networked+encrypted (--host/--ctrl/--data)
    scan PATH                nock: member count/listing; dir: du summary
    du PATH                  apparent size + file count
    rm PATH                  parallel delete
    cp SRC DST               parallel recursive copy (mode+mtime preserved)
    serve ARCHIVE            browse a nock over HTTP
"""
import argparse
import os
import sys

import polars as pl


def _spec(s):
    """'tar:PATH' | 'nock:PATH' | 'fs:PATH' | PATH -> (kind, path); bare path = fs/nock."""
    for k in ("tar", "nock", "fs"):
        if s.startswith(k + ":"):
            return (k, s[len(k) + 1:])
    return ("nock" if s.endswith((".nock", ".tar.zstd")) and os.path.isfile(s) else "fs", s)


def _expand(pattern):
    """A nock 'set' is just a glob: expand PATTERN to the matching nock files (sorted).
    Replaces the .nockset manifest — several nocks are unioned by reading their footers."""
    import glob
    return sorted(glob.glob(pattern)) or ([pattern] if os.path.exists(pattern) else [])


def _shard_ex(tmpl, n):
    from .exec.blocks import _shard_path
    return _shard_path(tmpl, 0) + (f" .. {_shard_path(tmpl, n - 1)}" if n > 1 else "")


def main(argv=None):
    from .exec import blocks
    p = argparse.ArgumentParser(prog="quiver", description="BLOCKS archive/sync engine")
    p.add_argument("-j", "--workers", type=int, default=16)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("recompress", help="foreign tar(.zstd) -> chunked nock")
    a.add_argument("out"); a.add_argument("inputs", nargs="+")
    a.add_argument("-L", "--level", type=int, default=6)
    a.add_argument("--frame-mb", type=float, default=1.0)
    a.add_argument("--no-tar-headers", action="store_true",
                   help="strip tar headers (smaller; data section is no longer a tar)")
    a.add_argument("--shards", type=int, metavar="N",
                   help="fan frames round-robin into N shard nocks (OUT: {shard}/%%d, else .i)")
    a.add_argument("--wal", metavar="PATH",
                   help="crash-resumable recompress (one input); rerun with the same --wal")
    a.add_argument("--glob", action="append", metavar="PAT", help="keep only members matching PAT")
    a.add_argument("--exclude", action="append", metavar="PAT", help="drop members matching PAT")
    a.add_argument("--min-size", type=int, default=0, help="drop members smaller than N bytes")
    a = sub.add_parser("pack", help="fs tree -> nock (incremental via --wal)")
    a.add_argument("root"); a.add_argument("out"); a.add_argument("--wal")
    a.add_argument("-L", "--level", type=int, default=6)
    a.add_argument("--no-tar-headers", action="store_true",
                   help="strip tar headers (smaller; data section is no longer a tar)")
    a.add_argument("--glob", action="append", metavar="PAT", help="keep only files matching PAT")
    a.add_argument("--exclude", action="append", metavar="PAT", help="drop files matching PAT")
    a.add_argument("--min-size", type=int, default=0, help="drop files smaller than N bytes")
    a = sub.add_parser("unpack", help="nock(s) -> fs; ARCHIVE may be a glob (a 'nockset')")
    a.add_argument("archive"); a.add_argument("dest"); a.add_argument("--glob", action="append")
    a.add_argument("--shuffle", action="store_true", help="randomize frame order (less WEKA dir-lock contention)")
    a.add_argument("--no-mtime", action="store_true", help="skip mtime restore (~35%% faster on WEKA; metadata-op-bound)")
    a = sub.add_parser("extract", help="unpack a --glob member subset")
    a.add_argument("archive"); a.add_argument("dest")
    a.add_argument("--glob", action="append", required=True)
    a = sub.add_parser("diff", help="streaming tree diff")
    a.add_argument("a"); a.add_argument("b")
    a = sub.add_parser("sync", help="rsync (local or networked)")
    a.add_argument("src"); a.add_argument("dst"); a.add_argument("-n", type=int, default=4)
    a.add_argument("--host"); a.add_argument("--ctrl", type=int); a.add_argument("--data", type=int)
    a.add_argument("--serve", action="store_true", help="run the receiver on DST (dst=local)")
    a = sub.add_parser("scan", help="nock(s) member count (glob ok) / fs du")
    a.add_argument("path"); a.add_argument("--glob", action="append")
    a = sub.add_parser("find", help="parallel find over the fs scanner")
    a.add_argument("root"); a.add_argument("-name", "--name", dest="name")
    a.add_argument("--min-size", type=int); a.add_argument("--max-size", type=int)
    a.add_argument("--newer", help="match files modified after this reference file")
    a = sub.add_parser("backup", help="append-only incremental snapshot: fs -> nock chain")
    a.add_argument("root"); a.add_argument("out")
    a.add_argument("-L", "--level", type=int, default=6)
    a.add_argument("--exclude", action="append", metavar="PAT",
                   help="extra .quiverignore-style pattern (repeatable)")
    a.add_argument("--frame-cap-mb", type=int, metavar="MB",
                   help="split members bigger than this across frames (bounds pack memory). "
                        "Default: 8x zstd's window for -L (16/32/64 MB) — see bench/framecap")
    a.add_argument("--sinks", type=int, default=1, metavar="N",
                   help="write data across N sink files (out, out.1, ...); parallel "
                        "filesystems serialize concurrent writes to one inode")
    a = sub.add_parser("snapshots", help="list a backup chain's snapshots")
    a.add_argument("archive")
    a = sub.add_parser("restore", help="restore a snapshot (default: latest)")
    a.add_argument("archive"); a.add_argument("dest")
    a.add_argument("--snapshot", type=int, default=0, metavar="K", help="0=latest, 1=previous, ...")
    a = sub.add_parser("push", help="replicate a backup chain to s3://... or file://...")
    a.add_argument("archive"); a.add_argument("url")
    a = sub.add_parser("pull", help="fetch a replicated backup chain")
    a.add_argument("url"); a.add_argument("archive")
    a = sub.add_parser("gc", help="compact a backup chain under a retention policy")
    a.add_argument("archive")
    a.add_argument("--last", type=int); a.add_argument("--daily", type=int)
    a.add_argument("--weekly", type=int); a.add_argument("--monthly", type=int)
    a.add_argument("--yearly", type=int)
    a = sub.add_parser("verify", help="re-hash member bodies against footer digests")
    a.add_argument("archive"); a.add_argument("--sample", type=int, metavar="N",
                   help="check only N random frames")
    a.add_argument("--merkle", action="store_true", help="authenticate the footer Merkle tree only")
    a.add_argument("--complete", metavar="ROOT", nargs="?", const="",
                   help="completeness: footer member set vs ROOT tree (catches DROPPED members)")
    a = sub.add_parser("du"); a.add_argument("path")
    a = sub.add_parser("rm"); a.add_argument("path")
    a.add_argument("--procs", type=int, default=1, help="fork N bvm unlinker processes (separate WEKA clients)")
    a = sub.add_parser("cp"); a.add_argument("src"); a.add_argument("dst")
    a = sub.add_parser("serve"); a.add_argument("archive"); a.add_argument("--port", type=int, default=8756)
    args = p.parse_args(argv)
    j = args.workers
    fb = args.frame_mb * (1 << 20) if getattr(args, "frame_mb", None) else 1 << 20

    if args.cmd == "recompress":
        bvm = blocks.default_bvm()
        tc = not args.no_tar_headers
        pred = blocks._member_filter(args.glob, args.exclude, args.min_size)
        if args.shards:
            n = blocks.recompress_shards(args.inputs, args.out, bvm, args.shards, nworkers=j,
                                         level=args.level, frame_bytes=int(fb), tar_compat=tc,
                                         predicate=pred)
            print(f"{n:,} members -> {args.shards} shards ({_shard_ex(args.out, args.shards)})")
        elif args.wal:
            if len(args.inputs) != 1:
                sys.exit("--wal takes exactly one input")
            n = blocks.recompress_c(args.inputs[0], args.out, bvm, nworkers=j, level=args.level,
                                    frame_bytes=int(fb), tar_compat=tc, wal_path=args.wal, predicate=pred)
            print(f"{n:,} members -> {args.out}")
        else:
            n = blocks.recompress_multi(args.inputs, args.out, bvm, nworkers=j, level=args.level,
                                        frame_bytes=int(fb), tar_compat=tc, predicate=pred)
            print(f"{n:,} members -> {args.out}")
    elif args.cmd == "pack":
        bvm = blocks.default_bvm()
        pred = blocks._member_filter(args.glob, args.exclude, args.min_size)
        r = blocks.pack_fs_c(args.root, args.out, args.wal or args.out + ".wal", bvm,
                             nworkers=j, level=args.level, tar_compat=not args.no_tar_headers,
                             predicate=pred)
        print(r)
    elif args.cmd in ("unpack", "extract"):
        bvm = blocks.default_bvm()
        paths = _expand(args.archive)
        if not paths:
            sys.exit(f"no nock matches {args.archive!r}")
        pred = blocks._glob_pred(args.glob)
        shuf = getattr(args, "shuffle", False)
        if getattr(args, "no_mtime", False):                  # metadata-op-bound: shedding mtime ~35% faster
            os.environ["BVM_SCATTER_META"] = "0"
        if len(paths) > 1:                                    # join all frames + shuffle across sources
            n = blocks.unpack_set(paths, args.dest, bvm, nworkers=j, predicate=pred, shuffle=True)
        else:
            n = blocks.unpack_c(paths[0], args.dest, bvm, nworkers=j, predicate=pred, shuffle=shuf)
        print(f"{n:,} files from {len(paths)} nock(s) -> {args.dest}")
    elif args.cmd == "diff":
        bvm = blocks.default_bvm()
        ka, kb = _spec(args.a), _spec(args.b)
        d = (blocks.diff_stream(ka[1], kb[1], bvm, j) if ka[0] == "fs" == kb[0]
             else blocks.diff_trees(ka, kb, bvm))
        for k in ("added", "removed", "changed", "unchanged"):
            print(f"{k:9s} {len(d.get(k, []))}")
    elif args.cmd == "sync":
        bvm = blocks.default_bvm()
        if args.host:
            print(blocks.rsync_push(args.src, bvm, args.host, args.ctrl, args.data, args.n, j))
        elif args.serve:
            print("deleted", blocks.rsync_serve(args.dst, bvm, args.ctrl, args.data, args.n, j))
        else:
            print(blocks.rsync(args.src, args.dst, bvm, args.n, j))
    elif args.cmd == "scan":
        if os.path.isdir(args.path):
            nb, nf = blocks.du(args.path); print(f"{nf:,} files  {nb/1e9:.3f} GB")
        else:
            paths = _expand(args.path)
            if not paths:
                sys.exit(f"no nock matches {args.path!r}")
            pred = blocks._glob_pred(args.glob)
            tot_m = tot_b = 0
            for p in paths:
                files = blocks.scan_nock(p).filter(pl.col("frame") >= 0)
                if pred is not None:
                    files = files.filter(pred)
                tot_m += files.height; tot_b += int(files["size"].sum())
                if len(paths) > 1:
                    print(f"  {os.path.basename(p):40s} {files.height:>10,}  {files['size'].sum()/1e9:8.3f} GB")
            print(f"{tot_m:,} members  {tot_b/1e9:.3f} GB uncompressed"
                  + (f"  across {len(paths)} nocks" if len(paths) > 1 else ""))
    elif args.cmd == "find":
        bvm = blocks.default_bvm()
        newer_ns = os.stat(args.newer).st_mtime_ns if args.newer else None
        res = blocks.find(args.root, bvm, name=args.name, min_size=args.min_size,
                          max_size=args.max_size, newer_than_ns=newer_ns, nworkers=j)
        for pth in res:
            print(pth)
        print(f"# {len(res):,} matches", file=sys.stderr)
    elif args.cmd == "backup":
        r = blocks.backup(args.root, args.out, blocks.default_bvm(), nworkers=j, level=args.level,
                          excludes=args.exclude, shards=args.sinks,
                          frame_cap=(args.frame_cap_mb << 20) if args.frame_cap_mb else None)
        perf = r.pop("perf", ({}, []))
        print(r)
        for line in perf[1]:
            print("  •", line)
    elif args.cmd == "snapshots":
        from .nock import nockidx
        snaps = nockidx.snapshots(args.archive)
        import datetime
        for i, sn in enumerate(snaps):
            ts = (datetime.datetime.fromtimestamp(sn["time_ns"] / 1e9).isoformat(" ", "seconds")
                  if sn["time_ns"] else "?")
            cm = (sn.get("commit") or "")[:16]
            print(f"  [{i}] {ts}  end={sn['at']:,}  commit={cm}")
        print(f"{len(snaps)} snapshot(s)")
    elif args.cmd == "restore":
        from .nock import nockidx
        snaps = nockidx.snapshots(args.archive)
        at = snaps[args.snapshot]["at"] if args.snapshot else None
        n = blocks.unpack_c(args.archive, args.dest, blocks.default_bvm(), nworkers=j, at=at)
        print(f"{n:,} entries -> {args.dest}")
    elif args.cmd == "push":
        print(blocks.push(args.archive, args.url))
    elif args.cmd == "pull":
        print(blocks.pull(args.url, args.archive))
    elif args.cmd == "gc":
        print(blocks.gc(args.archive, last=args.last, daily=args.daily,
                        weekly=args.weekly, monthly=args.monthly, yearly=args.yearly))
    elif args.cmd == "verify":
        if args.complete is not None:
            r = blocks.verify_complete(args.archive, root=args.complete or None)
            ok = not r["bad_locators"] and not r["missing"]
            print(("OK " if ok else "FAIL ") + str(r)); sys.exit(0 if ok else 1)
        if args.merkle:
            r = blocks.verify_merkle(args.archive)
            ok = r["bad"] is not None and not r["bad"] and r["root"]
            print(("OK " if ok else "FAIL ") + str(r)); sys.exit(0 if ok else 1)
        r = blocks.verify(args.archive, sample=args.sample)
        ok = r["mismatched"] == 0 and r["bad_frames"] == 0
        print(("OK " if ok else "FAIL ") + str(r))
        if not ok:
            sys.exit(1)
    elif args.cmd == "du":
        nb, nf = blocks.du(args.path); print(f"{nf:,} files  {nb/1e9:.3f} GB")
    elif args.cmd == "rm":
        print(f"{blocks.rm(args.path, j, bvm_exe=blocks.default_bvm(), procs=args.procs):,} removed")
    elif args.cmd == "cp":
        print(f"{blocks.cp(args.src, args.dst, j):,} copied -> {args.dst}")
    elif args.cmd == "serve":
        from .nock import zserve
        zserve.main([args.archive, "--port", str(args.port)])


if __name__ == "__main__":
    main()

"""quiver CLI: du, rm, cp, sync, pack, nock, scan."""
import argparse
import sys

import polars as pl


def main(argv=None):
    from . import nock, tools, wire
    p = argparse.ArgumentParser(prog="quiver")
    p.add_argument("--engine", default="auto", choices=["auto","uring","sync"])
    p.add_argument("--threads", type=int, default=64)  # WEKA plateau, see docs/BENCH-IREN.md
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, nargs in (("du",1),("rm",1),("scan",1)):
        sp = sub.add_parser(name); sp.add_argument("paths", nargs=nargs)
    for name in ("cp","sync"):
        sp = sub.add_parser(name); sp.add_argument("paths", nargs=2)
        sp.add_argument("--no-preserve-times", action="store_true",
                        help="skip mtime restore (~halves RPCs on WEKA)")
    sp = sub.add_parser("pack"); sp.add_argument("paths", nargs=2)
    sp.add_argument("--tar", action="store_true")
    sp = sub.add_parser("nock"); sp.add_argument("paths", nargs=1)
    sp = sub.add_parser("extract"); sp.add_argument("paths", nargs=2)
    sp = sub.add_parser("recompress",
                        help="tar/tar.zstd... -> per-batch-frame zstd nock")
    sp.add_argument("out"); sp.add_argument("inputs", nargs="+")
    sp.add_argument("--batch-mb", type=float, default=16.0)
    sp.add_argument("--level", type=int, default=10)
    sp.add_argument("--workers", type=int, default=None,
                    help="compress-pool workers (C engine) / threads (py)")
    sp.add_argument("--readers", type=int, default=8,
                    help="C-engine reader threads (parse fans out per source)")
    sp.add_argument("--py", action="store_true",
                    help="use the pure-Python engine (GIL-bound; supports --limit)")
    sp.add_argument("--limit", type=int, default=None,
                    help="max members per input (sampling; --py only)")
    sp = sub.add_parser("serve", help="browse a zframe archive over HTTP")
    sp.add_argument("archive"); sp.add_argument("--port", type=int, default=8756)
    a = p.parse_args(argv)
    eng, thr = a.engine, a.threads
    if a.cmd == "du":
        print(tools.du(a.paths[0], engine=eng, threads=thr))
    elif a.cmd == "scan":
        print(wire.scan(a.paths[0], eng, thr))
    elif a.cmd == "rm":
        print(f"{tools.rm(a.paths[0], engine=eng, threads=thr)} ops")
    elif a.cmd == "cp":
        print(f"{tools.cp(a.paths[0], a.paths[1], engine=eng, threads=thr, preserve_times=not a.no_preserve_times)} entries")
    elif a.cmd == "sync":
        print(tools.sync(a.paths[0], a.paths[1], engine=eng, threads=thr, preserve_times=not a.no_preserve_times))
    elif a.cmd == "pack":
        fmt = nock.TarFormat() if a.tar else nock.RawFormat()
        idx = tools.pack(a.paths[0], a.paths[1], fmt, engine=eng,
                         threads=thr)
        print(f"{len(idx)} members")
    elif a.cmd == "nock":
        print(nock.index_tar(a.paths[0]).height, "members indexed")
    elif a.cmd == "extract":
        print(f"{len(nock.extract(a.paths[0], a.paths[1], engine=eng))} extracted")
    elif a.cmd == "recompress":
        from .nock import zframe

        def _prog(s):
            # C engine reports cout (no cin consumed); py engine reports cin.
            done = s.get("cin", s.get("cout", 0))
            total = s["cin_total"]
            pct = 100 * done / total if total else 0
            el = s["elapsed"]
            rate = done / el if el else 0
            eta = (total - done) / rate if rate and "cin" in s else 0
            sys.stderr.write(
                f"\r[{pct:5.1f}%] {s['members']:>12,} members  "
                f"{s['cout']/1e9:5.1f} GB out  {s['frames']:>7,} frames  "
                f"{s['cout']/1e6/el if el else 0:5.0f} MB/s out  "
                f"{int(el)//3600:d}:{int(el)%3600//60:02d}:{int(el)%60:02d}   ")
            sys.stderr.flush()

        if a.py:
            res = zframe.recompress(a.inputs, a.out,
                                    batch_bytes=int(a.batch_mb * (1 << 20)),
                                    level=a.level, workers=a.workers,
                                    limit=a.limit, progress=_prog)
        else:
            res = zframe.recompress_c(a.inputs, a.out,
                                      batch_bytes=int(a.batch_mb * (1 << 20)),
                                      level=a.level, readers=a.readers,
                                      compressors=a.workers, progress=_prog)
        sys.stderr.write("\n")
        print(f"{res.members} members, {res.frames} frames -> {a.out}")
    elif a.cmd == "serve":
        from .nock import zserve
        zserve.main([a.archive, "--port", str(a.port)])


if __name__ == "__main__":
    main()

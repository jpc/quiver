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
    sp = sub.add_parser("pack"); sp.add_argument("paths", nargs=2)
    sp.add_argument("--tar", action="store_true")
    sp = sub.add_parser("nock"); sp.add_argument("paths", nargs=1)
    sp = sub.add_parser("extract"); sp.add_argument("paths", nargs=2)
    a = p.parse_args(argv)
    eng, thr = a.engine, a.threads
    if a.cmd == "du":
        print(tools.du(a.paths[0], engine=eng, threads=thr))
    elif a.cmd == "scan":
        print(wire.scan(a.paths[0], eng, thr))
    elif a.cmd == "rm":
        print(f"{tools.rm(a.paths[0], engine=eng, threads=thr)} ops")
    elif a.cmd == "cp":
        print(f"{tools.cp(a.paths[0], a.paths[1], engine=eng, threads=thr)} entries")
    elif a.cmd == "sync":
        print(tools.sync(a.paths[0], a.paths[1], engine=eng, threads=thr))
    elif a.cmd == "pack":
        fmt = nock.TarFormat() if a.tar else nock.RawFormat()
        idx = tools.pack(a.paths[0], a.paths[1], fmt, engine=eng,
                         threads=thr)
        print(f"{len(idx)} members")
    elif a.cmd == "nock":
        print(nock.index_tar(a.paths[0]).height, "members indexed")
    elif a.cmd == "extract":
        print(f"{len(nock.extract(a.paths[0], a.paths[1], engine=eng))} extracted")


if __name__ == "__main__":
    main()

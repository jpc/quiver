"""Benchmark harness: quiver vs coreutils on a local tree. Numbers here
are sanity floor; the interesting runs are WEKA/IREN (P2 exit)."""
import os, random, shutil, subprocess, tempfile, time
from pathlib import Path

import polars as pl

from quiver import nock, tools, wire


def t(fn):
    t0 = time.perf_counter(); fn(); return time.perf_counter() - t0


def main(n_files=20000):
    tmp = Path(tempfile.mkdtemp())
    src = tmp / "tree"
    random.seed(3)
    for i in range(n_files):
        p = src / f"a{i%20}" / f"b{i%40}" / f"f{i:05d}"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(os.urandom(random.randrange(0, 2048)))
    rows = []

    rows.append(("scan (uring t8)", t(lambda: wire.scan(str(src), "uring", 8))))
    # -printf %s forces a stat per entry — the honest baseline for a
    # statx scan; bare `find` is dirent-only and ~2-4x cheaper than
    # anything that actually stats.
    rows.append(("find -printf (stats)", t(lambda: subprocess.run(
        f"find {src} -printf '%s\\n' | wc -l", shell=True,
        capture_output=True))))
    rows.append(("find names-only", t(lambda: subprocess.run(
        f"find {src} | wc -l", shell=True, capture_output=True))))
    rows.append(("du -s", t(lambda: subprocess.run(
        ["du", "-s", str(src)], capture_output=True))))
    rows.append(("quiver du", t(lambda: tools.du(str(src)))))
    rows.append(("quiver pack tar", t(lambda: tools.pack(
        str(src), str(tmp/"q.tar"), nock.TarFormat(), engine="uring"))))
    rows.append(("GNU tar -cf", t(lambda: subprocess.run(
        ["tar", "-cf", str(tmp/"g.tar"), "-C", str(src), "."],
        capture_output=True))))
    for label, kw in (("rm refcount", dict(scheduler="refcount")),
                      ("rm epochs", dict(scheduler="epochs"))):
        clone = tmp / label.replace(" ", "_")
        shutil.copytree(src, clone)
        rows.append((f"quiver {label}", t(lambda c=clone, k=kw:
                     tools.rm(str(c), engine="uring", **k))))
    clone = tmp / "rmrf"; shutil.copytree(src, clone)
    rows.append(("rm -rf", t(lambda: subprocess.run(["rm", "-rf",
                                                     str(clone)]))))
    print(pl.DataFrame({"op": [r[0] for r in rows],
                        "seconds": [round(r[1], 3) for r in rows]}))
    shutil.rmtree(tmp)


if __name__ == "__main__":
    main()

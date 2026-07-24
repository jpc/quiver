#!/usr/bin/env python3
"""One-pass streaming split of a LibriLight subset into content-addressed shards,
isolating the single biggest speaker (6454 in `large`) into its own shard set.

LibriLight lays a subset out as  {root}/{speaker}/{book}/*.flac . The naming on
our copy (librilight-large-6454-flac-* vs librilight-large-wo6454-flac-*) is
exactly this split: speaker 6454 dominates `large`, so folding it into the normal
shards would make one shard enormous and skew balance — it gets its own shards.

The split is ONE PASS: it drives the STREAMING scanner (qplan.scan_stream /
OP_SCANDIR), which pushes STAT rows in chunks AS the parallel walk discovers them,
and routes each file by speaker on the fly. It never holds the whole member list —
only the current, partially-filled shard per group. When a group reaches ~shard
size it is packed (compressed nock) and the next shard begins, while the walk keeps
running. So: walk once, route as you go, emit shards as they fill.

    python scripts/librilight_split.py <root> <out_dir> --qvm <qvm> \
        [--subset large] [--big 6454] [--shard-gib 5]
    python scripts/librilight_split.py --selftest --qvm <qvm>
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import polars as pl
from quiver.exec import qplan


def librilight_split(root: str, out_dir: str, qvm_exe: str, subset: str = "large",
                     big_speaker: str = "6454", shard_bytes: int = 5 << 30,
                     speaker_depth: int = 0, frame_bytes: int = 1 << 20, level: int = 6,
                     nworkers: int = 8, on_shard=None) -> list[tuple]:
    """Stream-split `{root}` into shards. `speaker_depth` is the path component that
    is the speaker id (0 when `root` IS the subset dir, so paths are speaker/book/...).
    Files from `big_speaker` go to `{subset}-{big_speaker}` shards, everyone else to
    `{subset}-wo{big_speaker}`; each shard fills to ~`shard_bytes` of source audio
    then packs. Returns a manifest of (shard_path, group, index, members, src_bytes)."""
    os.makedirs(out_dir, exist_ok=True)
    big_g, rest_g = f"{subset}-{big_speaker}", f"{subset}-wo{big_speaker}"
    acc: dict[str, list] = {big_g: [], rest_g: []}   # pending rows per group (one shard)
    sz = {big_g: 0, rest_g: 0}                        # pending source bytes per group
    idx = {big_g: 0, rest_g: 0}                       # next shard index per group
    manifest: list[tuple] = []
    speaker = pl.col("path").str.split("/").list.get(speaker_depth)

    def _pack_shard(g: str, rows: pl.DataFrame) -> None:
        shard = os.path.join(out_dir, f"librilight-{g}-flac-{idx[g]:06d}.nock")
        n = qplan.pack(rows, root, shard, qvm_exe, frame_bytes=frame_bytes,
                       level=level, nworkers=nworkers)
        rec = (shard, g, idx[g], n, int(rows["size"].sum()))
        manifest.append(rec)
        if on_shard:
            on_shard(rec)
        idx[g] += 1

    def flush(g: str, force: bool = False) -> None:
        if not acc[g] or (not force and sz[g] < shard_bytes):
            return
        rows = pl.concat(acc[g]); acc[g] = []; sz[g] = 0
        csum = np.cumsum(rows["size"].to_numpy())     # greedily cut ~shard_bytes shards
        start = 0
        while start < rows.height:
            base = int(csum[start - 1]) if start else 0
            end = int(np.searchsorted(csum, base + shard_bytes)) + 1
            if end > rows.height:
                end = rows.height
            if end == rows.height and not force and csum[-1] - base < shard_bytes:
                rem = rows.slice(start)                # remainder < a shard: carry it
                acc[g] = [rem]; sz[g] = int(csum[-1] - base)
                return
            _pack_shard(g, rows.slice(start, end - start))
            start = end

    def route(df: pl.DataFrame) -> None:              # one streamed STAT chunk
        f = df.filter(~pl.col("is_dir"))
        if not f.height:
            return
        is_big = speaker == big_speaker
        for g, part in ((big_g, f.filter(is_big)), (rest_g, f.filter(~is_big))):
            if not part.height:
                continue
            acc[g].append(part)
            sz[g] += int(part["size"].sum())
            flush(g)                                  # emit the shard if it is full

    qplan.scan_stream(root, qvm_exe, route, threads=nworkers)   # ← the single pass
    flush(big_g, force=True)                          # tail shards
    flush(rest_g, force=True)
    return manifest


def librilight_split_http(url: str, out_dir: str, qvm_exe: str, subset: str = "large",
                          big_speaker: str = "6454", shard_bytes: int = 5 << 30,
                          speaker_depth: int = 1, level: int = 3, frame_bytes: int = 1 << 20,
                          nworkers: int = 8, on_shard=None, _stream=None) -> list[tuple]:
    """STREAM-download a subset tar(.gz) over HTTP and repack it on the fly — never
    landing the full archive (or the extracted files) on disk. The `large` subset is
    a single tar.gz; `r|*` streams AND gzip-decompresses it sequentially (no seek),
    so only the HTTP stream flows and gzip runs once over it. Each member (path
    {subset}/{speaker}/{book}/*.flac → speaker at `speaker_depth`) is routed to its
    group's OPEN shard tar; when a group hits ~`shard_bytes` that shard tar is
    recompressed to a nock shard and a fresh one begins. Peak disk is ~one shard per
    group. `_stream` overrides the source (a file object) for testing. Returns the
    same manifest as librilight_split.

    Note: a single tar.gz is ONE gzip stream, so decompression is single-threaded
    (network- and gzip-bound); swap in isal/igzip for the fastest zlib path."""
    import io
    import tarfile
    import tempfile
    import urllib.request

    os.makedirs(out_dir, exist_ok=True)
    big_g, rest_g = f"{subset}-{big_speaker}", f"{subset}-wo{big_speaker}"
    open_tar: dict[str, tuple] = {}                   # group → (tarfile, temp path, bytes)
    idx = {big_g: 0, rest_g: 0}
    manifest: list[tuple] = []

    def shard_open(g: str):
        fd, tmp = tempfile.mkstemp(prefix=f"ll_{g}_", suffix=".tar", dir=out_dir)
        os.close(fd)
        open_tar[g] = (tarfile.open(tmp, "w"), tmp, 0)

    def shard_close(g: str) -> None:                  # recompress the group's shard tar
        tf, tmp, nbytes = open_tar.pop(g)
        tf.close()
        if nbytes == 0:
            os.unlink(tmp); return
        shard = os.path.join(out_dir, f"librilight-{g}-flac-{idx[g]:06d}.nock")
        n = qplan.recompress(tmp, shard, qvm_exe, frame_bytes=frame_bytes,
                             level=level, nworkers=nworkers)
        os.unlink(tmp)
        rec = (shard, g, idx[g], n, nbytes)
        manifest.append(rec)
        if on_shard:
            on_shard(rec)
        idx[g] += 1

    src = _stream if _stream is not None else urllib.request.urlopen(url)
    with tarfile.open(fileobj=src, mode="r|*") as tar:  # STREAM + gunzip, no seek
        for m in tar:
            if not m.isfile():
                continue
            parts = m.name.split("/")
            spk = parts[speaker_depth] if len(parts) > speaker_depth else parts[0]
            g = big_g if spk == big_speaker else rest_g
            if g not in open_tar:
                shard_open(g)
            data = tar.extractfile(m).read()          # this member's bytes, in order
            tf, tmp, nbytes = open_tar[g]
            mi = tarfile.TarInfo(m.name); mi.size = m.size; mi.mode = m.mode
            mi.mtime = m.mtime; mi.uid = m.uid; mi.gid = m.gid
            tf.addfile(mi, io.BytesIO(data))
            nbytes += m.size; open_tar[g] = (tf, tmp, nbytes)
            if nbytes >= shard_bytes:
                shard_close(g)
    for g in list(open_tar):                           # tail shards
        shard_close(g)
    return manifest


# --------------------------------------------------------------------- self-test
def _selftest(qvm_exe: str) -> None:
    import shutil, tempfile
    from quiver.nock import nockidx as _zf

    root = tempfile.mkdtemp(prefix="ll_split_")
    files: dict[str, int] = {}                        # rel path -> size
    rng = __import__("numpy").random.default_rng(0)
    # 6454 = the big speaker (many files); a few small speakers alongside it
    plan = {"6454": 120, "100": 20, "200": 25, "1088": 18}
    for spk, nfile in plan.items():
        for b in range(3):
            d = os.path.join(root, spk, f"book{b}")
            os.makedirs(d, exist_ok=True)
            for u in range(nfile):
                sz = int(rng.integers(2000, 40000))
                rel = f"{spk}/book{b}/{spk}-{b}-{u:04d}.flac"
                with open(os.path.join(root, rel), "wb") as fo:
                    fo.write(bytes(rng.integers(0, 256, sz, dtype="uint8")))
                files[rel] = sz
    total_big = sum(v for k, v in files.items() if k.startswith("6454/"))
    print(f"synthetic tree: {len(files)} files, 6454 = {total_big/1e6:.2f} MB")

    out = tempfile.mkdtemp(prefix="ll_shards_")
    # small shard so BOTH groups split into several shards in one pass
    shard_bytes = total_big // 3
    man = librilight_split(root, out, qvm_exe, subset="large", big_speaker="6454",
                           shard_bytes=shard_bytes, frame_bytes=64 << 10, nworkers=4)
    for shard, g, i, n, sb in man:
        print(f"  {g}-{i:03d}: {n:5d} files, {sb/1e6:6.2f} MB  {os.path.basename(shard)}")

    # ---- validate: every file in exactly one shard, groups pure, round-trips ----
    seen: dict[str, str] = {}
    big_shards = rest_shards = 0
    for shard, g, i, n, sb in man:
        is_rest = g.endswith("wo6454")
        big_shards += not is_rest
        rest_shards += is_rest
        idx = _zf.read_index(shard).filter(pl.col("frame") >= 0)
        for p in idx["path"]:
            assert p not in seen, f"{p} in two shards"
            seen[p] = g
            spk = p.split("/")[0]
            if is_rest:
                assert spk != "6454", f"6454 file {p} leaked into wo6454 shard"
            else:
                assert spk == "6454", f"non-6454 file {p} in 6454 shard"
    assert set(seen) == set(files), \
        f"coverage: {len(seen)} shard files vs {len(files)} scanned"
    # round-trip one 6454 shard byte-exact
    s0 = next(s for s, g, *_ in man if not g.endswith("wo6454"))
    xd = tempfile.mkdtemp()
    qplan.unpack(s0, xd, qvm_exe, npool=8)
    for p in _zf.read_index(s0).filter(pl.col("frame") >= 0)["path"]:
        assert open(os.path.join(xd, p), "rb").read() == \
            open(os.path.join(root, p), "rb").read()
    print(f"PASS (fs): {len(files)} files → {big_shards} large-6454 + {rest_shards} "
          f"large-wo6454 shards, groups pure, complete, byte-exact round-trip")

    # ---- HTTP path: tar up the tree as .tar.gz, serve it, stream-download + repack ----
    import gzip, http.server, socketserver, tarfile, threading
    tgz = os.path.join(tempfile.mkdtemp(), "large.tar.gz")
    with tarfile.open(tgz, "w:gz") as tf:             # single tar.gz, like the real large
        tf.add(root, arcname="large", recursive=True)
    srvdir = os.path.dirname(tgz)
    os.chdir(srvdir)
    httpd = socketserver.TCPServer(("127.0.0.1", 0), http.server.SimpleHTTPRequestHandler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    out2 = tempfile.mkdtemp(prefix="ll_http_")
    # paths in the tar are large/{speaker}/... → speaker at depth 1
    man2 = librilight_split_http(f"http://127.0.0.1:{port}/large.tar.gz", out2, qvm_exe,
                                 subset="large", big_speaker="6454",
                                 shard_bytes=total_big // 3, speaker_depth=1,
                                 level=3, frame_bytes=64 << 10, nworkers=4)
    httpd.shutdown()
    seen2, pure = set(), True
    for shard, g, i, n, sb in man2:
        for p in _zf.read_index(shard).filter(pl.col("frame") >= 0)["path"]:
            spk = p.split("/")[1]                     # large/{speaker}/...
            pure = pure and ((spk == "6454") != g.endswith("wo6454"))
            seen2.add(p[len("large/"):])
    assert pure, "HTTP split leaked a speaker across groups"
    assert seen2 == set(files), f"HTTP coverage {len(seen2)} vs {len(files)}"
    print(f"PASS (http): streamed .tar.gz → {len(man2)} shards, groups pure, complete "
          f"(never staged the full archive)")
    for d in (root, out, xd, srvdir, out2):
        shutil.rmtree(d, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", nargs="?", help="subset dir (paths are speaker/book/*.flac)")
    ap.add_argument("out_dir", nargs="?")
    ap.add_argument("--qvm", required=True)
    ap.add_argument("--subset", default="large")
    ap.add_argument("--big", default="6454", help="speaker id to isolate")
    ap.add_argument("--shard-gib", type=float, default=5.0)
    ap.add_argument("--speaker-depth", type=int, default=0)
    ap.add_argument("--level", type=int, default=6)
    ap.add_argument("--nworkers", type=int, default=16)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        _selftest(a.qvm)
        return
    if not a.root or not a.out_dir:
        ap.error("root and out_dir required (or --selftest)")
    man = librilight_split(a.root, a.out_dir, a.qvm, subset=a.subset, big_speaker=a.big,
                           shard_bytes=int(a.shard_gib * (1 << 30)),
                           speaker_depth=a.speaker_depth, level=a.level,
                           nworkers=a.nworkers,
                           on_shard=lambda r: print(f"shard {r[1]}-{r[2]:06d}: "
                                                    f"{r[3]} files, {r[4]/1e9:.2f} GB"))
    big = sum(1 for _, g, *_ in man if not g.endswith(f"wo{a.big}"))
    print(f"\n{len(man)} shards ({big} {a.subset}-{a.big}, {len(man)-big} "
          f"{a.subset}-wo{a.big}), {sum(r[3] for r in man)} files")


if __name__ == "__main__":
    main()

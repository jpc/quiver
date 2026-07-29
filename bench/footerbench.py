#!/usr/bin/env python3
"""Replay THE WHOLE footer phase from an existing store, so it can be optimized on its own.

On a whole-tree run the footer is serial after all packing and took 1093 s of 2357 (46%).
Iterating on that through 40-minute backups is not viable, and timing hand-picked stages was
worse than useless -- every stage I guessed at came back near zero while the real cost sat
somewhere I had not thought to look. So this reconstructs the planner's inputs from a store's
own footer and calls blocks.assemble_footer() itself: same function, same row counts, same
shapes, no backup and no cluster.

    ./footerbench.py /path/to/store.nock                    # replay, timed
    ./footerbench.py /path/to/store.nock --rows 300000      # subset, for fast iteration
    ./footerbench.py /path/to/store.nock --profile          # cProfile the phase

What is reconstructed, and how faithfully:
  pdf/bounds/pre  exact -- the member table and frame cuts come straight from the footer
  locs/shard_of   exact -- every frame's locator is in the footer
  allst           SHAPED -- bvm emits one Arrow batch per frame, so the batches are rebuilt
                  by grouping members on frame. Row counts and batch counts match the real
                  run, which is what the cost depends on.
"""
import argparse, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import polars as pl
from quiver.exec import blocks


def rebuild(store, rows=None):
    """Store footer -> the arguments assemble_footer() takes."""
    df = blocks.scan_nock(store)
    if "shard" not in df.columns:
        df = df.with_columns(shard=pl.lit(0, pl.Int64))
    if rows:
        df = df.head(rows)
    files = df.filter(pl.col("frame") >= 0).sort(["frame", "in_off"])
    ext = df.filter(pl.col("frame") == -4)
    dirs = df.filter(pl.col("frame") == -1)
    links = df.filter(pl.col("frame").is_in([-2, -3]))

    # the plan: direct members, plus one row per PIECE of every split member
    piece = []
    for r in ext.iter_rows(named=True):
        for coff, clen, io_, ln, oo, sh in blocks._parse_extents(r["extents"]):
            piece.append(dict(path=r["path"], size=ln, mode=r["mode"], mtime_ns=r["mtime_ns"],
                              uid=r["uid"], gid=r["gid"], in_off=0, link="", type=0,
                              src_off=oo, coff=coff, clen=clen, shard=sh))
    direct = files.select(
        path="path", size="size", mode="mode", mtime_ns="mtime_ns", uid="uid", gid="gid",
        in_off="in_off", link=pl.lit("", pl.Utf8), type=pl.lit(0, pl.UInt8),
        src_off=pl.lit(0, pl.Int64), coff="coff", clen="clen", shard="shard")
    dirrows = dirs.select(
        path="path", size=pl.lit(0, pl.Int64), mode="mode", mtime_ns="mtime_ns", uid="uid",
        gid="gid", in_off=pl.lit(0, pl.Int64), link=pl.lit("", pl.Utf8),
        type=pl.lit(5, pl.UInt8), src_off=pl.lit(0, pl.Int64),
        coff=pl.lit(0, pl.Int64), clen=pl.lit(0, pl.Int64), shard=pl.lit(0, pl.Int64))
    pf = pl.DataFrame(piece).select(direct.columns).cast(direct.schema) if piece else None
    typed = pl.concat([x for x in (dirrows, direct, pf) if x is not None and x.height])

    # frames: one per distinct (shard, coff, clen), in plan order
    key = typed.select("shard", "coff", "clen")
    newf = ((key["coff"] != key["coff"].shift(1)) | (key["shard"] != key["shard"].shift(1))
            | (key["clen"] != key["clen"].shift(1))).fill_null(True).to_numpy()
    fid = np.cumsum(newf) - 1
    typed = typed.with_columns(fid=pl.Series(fid.astype(np.int64)))
    bounds = [0] + (np.flatnonzero(newf)[1:].tolist()) + [typed.height]
    nframes = len(bounds) - 1
    first = typed.filter(pl.Series(newf))
    locs = {i: (int(c), int(l)) for i, (c, l) in
            enumerate(zip(first["coff"].to_list(), first["clen"].to_list()))}
    shard_of = np.asarray(first["shard"].to_list(), dtype=np.int64)

    sizes = np.where(typed["type"].to_numpy() == 0, typed["size"].to_numpy(), 0)
    pre = np.zeros(sizes.size + 1, np.int64); np.cumsum(sizes, out=pre[1:])

    cap = 16 << 20
    big = ext.select("path", "size", "mode", "mtime_ns", "uid", "gid")
    small = files.select("path", "size", "mode", "mtime_ns", "uid", "gid")

    # allst: bvm sends ONE Arrow batch per frame; rebuild that shape by grouping on frame
    st = (files.select("path", "digest", "chunks", "frame")
          .with_columns(fid=pl.col("frame").cast(pl.Int64)))
    return dict(pdf=typed.drop("coff", "clen", "shard"), bounds=bounds, pre=pre, locs=locs,
                shard_of=shard_of, big=big, small=small, links=links, allst=st,
                paths=blocks.store_files(store), frame_cap=cap, nframes=nframes)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("store")
    ap.add_argument("--rows", type=int)
    ap.add_argument("--rows-per-batch", type=int, default=1 << 12)
    ap.add_argument("--profile", action="store_true")
    ap.add_argument("--out", default="/tmp/footerbench.footer")
    a = ap.parse_args()

    t0 = time.time()
    A = rebuild(a.store, a.rows)
    print(f"  rebuilt inputs in {time.time()-t0:.1f}s: {A['pdf'].height:,} plan rows, "
          f"{A['nframes']:,} frames, {A['big'].height:,} split members, "
          f"{A['allst'].height:,} stat rows", flush=True)

    def go():
        return blocks.assemble_footer(a.out, A["pdf"], A["bounds"], A["pre"], A["locs"],
                                      A["shard_of"], A["big"], A["small"], A["links"],
                                      A["allst"], A["paths"], A["frame_cap"],
                                      rows_per_batch=a.rows_per_batch)
    if a.profile:
        import cProfile, pstats, io as _io
        pr = cProfile.Profile(); pr.enable()
        t0 = time.time(); r = go(); dt = time.time() - t0
        pr.disable()
        s = _io.StringIO(); pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(18)
        print(s.getvalue())
    else:
        t0 = time.time(); r = go(); dt = time.time() - t0

    print(f"  assemble_footer: {dt:.2f}s   footer {r['footer_bytes']/1e6:.1f} MB   "
          f"{r['extent_rows']:,} extent rows   {len(r['lost_paths']):,} lost")
    if A["nframes"]:
        print(f"  scaled to 556,370 frames: {dt * 556370 / A['nframes']:.0f}s")


if __name__ == "__main__":
    main()

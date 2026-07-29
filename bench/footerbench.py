#!/usr/bin/env python3
"""Isolate footer assembly so it can be optimized without re-running a backup.

The footer phase is now the single largest part of a whole-tree run -- 1093 s of 2357, 46% --
and it is entirely serial after packing. Iterating on it through 40-minute backups is not
viable, so this reconstructs the planner's inputs from an EXISTING store's footer and replays
each stage under a timer. Same row counts, same shapes, same code; no cluster, no data moved.

    ./footerbench.py /path/to/store.nock            # breakdown by stage
    ./footerbench.py /path/to/store.nock --rows 400000   # a fast subset while iterating

Stages timed separately, because "the footer is slow" was never actionable:
  load        read the existing footer (setup, not part of the phase)
  packed      build the direct-member rows + join their digests
  extents     _split_extent_rows: EXTENT rows for split members
  pack_footer nockidx.pack_footer: batch, zstd-compress, build the directory
  telemetry   frame_costs + write_parquet -- added for diagnosis, now on the critical path
"""
import argparse, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from quiver.exec import blocks
from quiver.nock import nockidx


class T:
    def __init__(self): self.t = {}
    def __call__(self, name):
        self.name = name; return self
    def __enter__(self): self.t0 = time.time(); return self
    def __exit__(self, *a):
        self.t[self.name] = time.time() - self.t0
        print(f"  {self.name:<12}{self.t[self.name]:>9.2f}s", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("store")
    ap.add_argument("--rows", type=int, help="subset the member table (fast iteration)")
    ap.add_argument("--rows-per-batch", type=int, default=1 << 12)
    ap.add_argument("--level", type=int, default=3, help="footer batch zstd level")
    ap.add_argument("--no-telemetry", action="store_true")
    a = ap.parse_args()
    t = T()

    with t("load"):
        df = blocks.scan_nock(a.store)
        if a.rows:
            df = df.head(a.rows)
    files = df.filter(pl.col("frame") >= 0)
    ext = df.filter(pl.col("frame") == -4)
    dirs = df.filter(pl.col("frame") == -1)
    links = df.filter(pl.col("frame").is_in([-2, -3]))
    print(f"  ({files.height:,} direct, {ext.height:,} extent, {dirs.height:,} dirs, "
          f"{links.height:,} links)")

    C13 = blocks.STAT_COLS + ["chunks", "extents", "shard"]
    for c, dt in (("chunks", pl.Binary), ("extents", pl.Binary), ("shard", pl.Int64)):
        for nm in ("files", "ext", "dirs", "links"):
            v = locals()[nm]
            if c not in v.columns:
                locals()[nm] = v.with_columns(pl.lit(None, dt).alias(c))

    # --- the stages, as backup_multi runs them ---------------------------------------
    with t("packed"):
        digs = files.select("path", "digest", "chunks").unique(subset="path", keep="last")
        packed = (files.with_columns(digest=pl.lit(-1, pl.Int64))
                  .drop("digest").join(digs, on="path", how="left", maintain_order="left")
                  .with_columns(digest=pl.col("digest").fill_null(-1)))
        packed = packed.select([c for c in C13 if c in packed.columns])

    with t("extents"):
        if ext.height:
            # rebuild the piece table the way the planner has it, then run the real builder
            rows = []
            for r in ext.head(2000).iter_rows(named=True):
                for i, (coff, clen, io_, ln, oo, sh) in enumerate(
                        blocks._parse_extents(r["extents"])):
                    rows.append(dict(path=r["path"], src_off=oo, coff=coff, clen=clen,
                                     in_off_out=io_, shard=sh, fid=i, size=ln, type=0))
            fall = pl.DataFrame(rows) if rows else pl.DataFrame()
            big = ext.head(2000).select("path", "size", "mode", "mtime_ns", "uid", "gid")
            if fall.height:
                r_, l_ = blocks._split_extent_rows(big, fall, set(), None, 16 << 20)
                print(f"    ({len(r_):,} extent rows rebuilt from {fall.height:,} pieces)")

    with t("pack_footer"):
        parts = [p for p in (packed,) if p.height]
        stat = pl.concat(parts, how="vertical_relaxed")
        disk = stat.rename({x: y for x, y in (("coff", "frame_coff"), ("clen", "frame_clen"))
                            if x in stat.columns})
        blob = nockidx.pack_footer(disk, 0, rows_per_batch=a.rows_per_batch, level=a.level)
        print(f"    ({len(blob)/1e6:.1f} MB footer, {a.rows_per_batch} rows/batch, "
              f"level {a.level})")

    if not a.no_telemetry:
        fb = a.store + ".frames.0.bin"
        if os.path.exists(fb):
            with t("telemetry"):
                fc = blocks.read_frames_live(fb)
                if fc.height:
                    fc.write_parquet("/tmp/footerbench.parquet")
                    print(f"    ({fc.height:,} frame records -> parquet)")

    tot = sum(v for k, v in t.t.items() if k != "load")
    print(f"\n  total (excluding load): {tot:.2f}s")
    for k, v in sorted(t.t.items(), key=lambda kv: -kv[1]):
        if k != "load":
            print(f"    {k:<12}{100*v/tot:>6.1f}%")


if __name__ == "__main__":
    main()

"""
quiver.tools — cp, rm, du, sync, pack as queries over the scan stream,
executed as epoch-ordered intent logs by artar_exec.

Every tool has the same shape:
    scan (C, io_uring statx) → Polars plan → command DataFrame with
    dep_group epochs → PipeExecutor → completions.

Epoch conventions (executor barriers between distinct dep_group values):
    rm:     files @0, dirs @ 1 + (max_depth - depth)   (deepest first)
    cp:     dirs @ depth (shallowest first), files @ max_depth + 1
    rsync:  mkdirs @ depth, copies @ D+1, unlinks @ D+2,
            rmdirs @ D+3 + (max_depth - depth)          (delete-after)

du needs no executor at all — it is pure producer + planner.
"""

from __future__ import annotations

import os

import polars as pl

from .wire import (OP_COPY, OP_FBARRIER, OP_MKDIR, OP_RMDIR,
                   OP_SETMETA, OP_UNLINK,
                   cmd_df, run_commands, scan, scan_lazyframe)
from . import nock

DEPTH = (pl.col("path").str.count_matches("/")).alias("depth")


def _run(cmds: pl.DataFrame, engine: str, wal_path: str | None,
         archive: str = "-", distributed=None) -> pl.DataFrame | None:
    """Execute directly, across nodes, or via a resumable WAL.
    `distributed` = (root, transports, affinity_col) fans the plan over
    nodes by subtree affinity (batch only — no WAL combination yet)."""
    if distributed is not None and wal_path is None:
        from .remotes.multi import run_distributed
        root, transports, aff = distributed
        comp = run_distributed(cmds, root, transports, aff, archive, engine)
        _check(comp, cmds)
        return comp
    if wal_path is None:
        comp = run_commands(cmds, archive, engine)
        _check(comp, cmds)
        return comp
    from . import wal as _wal
    _wal.write(wal_path, cmds)
    r = _wal.execute(wal_path, archive, engine)
    if r["failed"][0]:
        raise OSError(f"{r['failed'][0]} operations failed; "
                      f"inspect wal.failures({wal_path!r}) and rerun")
    return None


def _check(comp: pl.DataFrame, cmds: pl.DataFrame) -> None:
    bad = comp.filter(pl.col("res") < 0)
    if len(bad):
        detail = bad.join(cmds, on="user_data").select(
            "user_data", "res", "opcode", "path")
        raise OSError(f"{len(bad)} operations failed:\n{detail}")


# ── du ────────────────────────────────────────────────────────────────────

def du(root: str, depth: int = 1, apparent: bool = False,
       engine: str = "auto", threads: int = 8) -> pl.DataFrame:
    """Disk usage by prefix at `depth`. Matches du semantics: st_blocks
    based (unless apparent=True), hardlinks counted once, directory
    inodes included. Pure scan + group_by — no execution engine."""
    df = scan(root, engine, threads)
    usage = (pl.col("size") if apparent else pl.col("blocks") * 512)
    files = df.filter(~pl.col("is_dir")).unique(subset="ino")  # hardlinks
    entries = pl.concat([files, df.filter(pl.col("is_dir"))])
    per = (entries
           .with_columns(prefix=pl.col("path").str.split("/")
                         .list.slice(0, depth).list.join("/"))
           .group_by("prefix")
           .agg(bytes=usage.sum(), files=(~pl.col("is_dir")).sum(),
                dirs=pl.col("is_dir").sum())
           .sort("bytes", descending=True))
    total = entries.select(prefix=pl.lit("<total>"), bytes=usage.sum(),
                           files=(~pl.col("is_dir")).sum(),
                           dirs=pl.col("is_dir").sum())
    return pl.concat([per, total])


# ── rm -r ─────────────────────────────────────────────────────────────────

def rm(root: str, select: pl.Expr | None = None,
       engine: str = "auto", wal: str | None = None,
       scheduler: str = "refcount", threads: int = 8,
       transports=None) -> int:
    """Recursive delete of root's contents (and root itself unless a
    `select` filter keeps survivors). Files in epoch 0, rmdirs deepest-
    first — the executor barrier makes -ENOTEMPTY structurally impossible."""
    if transports is not None:
        # refcount orders via parent_row (a positional index into the
        # whole frame); subtree partitioning re-indexes each shard, so
        # those indices break. Depth epochs are position-independent and
        # partition cleanly — use them for distributed rm.
        scheduler = "epochs"
    df = scan(root, engine, threads)
    if select is not None:
        df = df.filter(select)
        remove_root = False
    else:
        remove_root = True
    df = df.with_columns(DEPTH)
    maxd = df["depth"].max() if len(df) else 0
    if scheduler == "refcount":
        # forest deps instead of depth barriers: a rmdir becomes ready
        # the moment ITS children finish; independent subtrees never
        # wait for each other. Emission order is post-order (children
        # first) so the sync engine's sequential run stays valid.
        df = df.sort("depth", descending=True) \
               .sort("is_dir", maintain_order=True)   # files, then dirs deep→shallow
        # stripe the file block across parent dirs (unlink takes the
        # parent's i_rwsem exclusively — see _stripe_dirs); dirs keep
        # their positions and parent_row is computed after the permute
        df = pl.concat([_stripe_dirs(df.filter(~pl.col("is_dir"))),
                        df.filter(pl.col("is_dir"))])
        paths = df["path"].to_list()
        rows = {}
        base = len(paths)
        order = list(range(base)) + ([base] if remove_root else [])
        all_paths = [os.path.join(root, p) for p in paths]
        ops = [OP_RMDIR if d else OP_UNLINK for d in df["is_dir"]]
        dir_row = {p: i for i, (p, d) in enumerate(zip(paths, df["is_dir"]))
                   if d}
        parent = []
        for p in paths:
            pp = os.path.dirname(p)
            parent.append(dir_row.get(pp, base if remove_root else -1)
                          if pp or remove_root else
                          (base if remove_root else -1))
        if remove_root:
            all_paths.append(os.path.abspath(root))
            ops.append(OP_RMDIR)
            parent.append(-1)
        n = len(all_paths)
        cmds = cmd_df(n, opcode=ops, path=all_paths,
                      parent_row=pl.Series(parent, dtype=pl.Int64))
    else:
        # rm = sync from emptiness (S8), plus the root itself
        cmds, _ = sync_cmds(empty_stat(),
                            df.drop("depth").with_columns(DEPTH),
                            root, root, delete=True)
        n = len(cmds) + (1 if remove_root else 0)
        if remove_root:
            root_row = cmd_df(
                1, user_data=pl.Series([len(cmds)], dtype=pl.UInt64),
                opcode=[OP_RMDIR],
                dep_group=pl.Series([int(cmds["dep_group"].max() or 0) + 1],
                                    dtype=pl.Int64),
                path=[os.path.abspath(root)])
            cmds = pl.concat([cmds, root_row])
    dist = (os.path.abspath(root), transports, "path") if transports else None
    _run(cmds, engine, wal, distributed=dist)
    return n


# ── cp -r ─────────────────────────────────────────────────────────────────

def _setmeta_cmds(dst_root: str, df: pl.DataFrame,
                  dep: int) -> pl.DataFrame:
    """SETMETA epoch: mtimes restored by the executor's sync pool
    (utimensat has no io_uring opcode) — the Python metadata tail from
    the prototype is gone."""
    n = len(df)
    return cmd_df(n, opcode=[OP_SETMETA] * n,
                  dep_group=pl.Series([dep] * n, dtype=pl.Int64),
                  path=[os.path.join(dst_root, p) for p in df["path"]],
                  mtime_ns=df["mtime_ns"])


def cp(src_root: str, dst_root: str, engine: str = "auto",
       wal: str | None = None, threads: int = 8,
       preserve_times: bool = True, transports=None) -> int:
    """cp -r = sync into emptiness (S8)."""
    src = scan(src_root, engine, threads).with_columns(DEPTH)
    os.makedirs(dst_root, exist_ok=True)
    cmds, _ = sync_cmds(src, empty_stat(), src_root, dst_root,
                        delete=False, preserve_times=preserve_times)
    if len(cmds):
        dist = (os.path.abspath(dst_root), transports, "dst_path") \
            if transports else None
        _run(cmds, engine, wal, distributed=dist)
    return len(src)

def empty_stat() -> pl.DataFrame:
    from .wire import SCAN_PL
    return pl.DataFrame(schema=SCAN_PL)


def _stripe_dirs(df: pl.DataFrame) -> pl.DataFrame:
    """Round-robin rows across parent directories. The kernel holds the
    parent dir's i_rwsem exclusively for every create/unlink, so
    dir-major order convoys the executor's open pool and io-wq on one
    lock (measured on WEKA: ~2 effective openats in flight out of 16).
    Striping spreads concurrent ops across directory locks."""
    if not len(df):
        return df
    return (df.with_columns(_d=pl.col("path").str.extract(r"^(.*)/", 1)
                            .fill_null(""))
              .with_columns(_r=pl.int_range(pl.len()).over("_d"))
              .sort(["_r", "_d"]).drop(["_d", "_r"]))


def sync_cmds(src: pl.DataFrame, dst: pl.DataFrame, src_root: str,
              dst_root: str, delete: bool = True,
              preserve_times: bool = True
              ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """THE planner (S8): full outer join on path, (size, mtime_ns)
    delta, epoch ladder mkdir < copy < setmeta < unlink < rmdir(deep
    first). cp is this with an empty dst; rm is this with an empty src.
    Returns (cmds, op summary).

    preserve_times=False drops the SETMETA mtime epoch. BPF profiling on
    WEKA showed setattr is a full ~1.5 ms RPC per file — the same cost
    as the copy — so skipping it ~halves cp/sync RPC traffic. Safe for
    content-addressed sync, which never consults mtime."""
    j = src.join(dst, on="path", how="full", suffix="_d", coalesce=True) \
           .with_columns(DEPTH)
    in_src, in_dst = pl.col("is_dir").is_not_null(), \
                     pl.col("is_dir_d").is_not_null()
    if len(j.filter(in_src & in_dst
                    & (pl.col("is_dir") != pl.col("is_dir_d")))):
        raise NotImplementedError("file<->dir type conflict")

    maxd = int(j["depth"].max() or 0)
    mkdirs = j.filter(in_src & ~in_dst & pl.col("is_dir"))
    copies = _stripe_dirs(
        j.filter(in_src & ~pl.col("is_dir").fill_null(False) & (
            ~in_dst | (pl.col("size") != pl.col("size_d"))
                    | (pl.col("mtime_ns") != pl.col("mtime_ns_d")))))
    unlinks = _stripe_dirs(j.filter(in_dst & ~in_src
                                    & ~pl.col("is_dir_d"))) \
        if delete else j.clear()
    rmdirs = j.filter(in_dst & ~in_src & pl.col("is_dir_d")) \
        if delete else j.clear()

    cmds = pl.concat([
        cmd_df(len(mkdirs), opcode=[OP_MKDIR] * len(mkdirs),
               dep_group=mkdirs["depth"],
               path=[os.path.join(dst_root, p) for p in mkdirs["path"]],
               mode=(mkdirs["mode"] & 0o7777).cast(pl.Int32)),
        cmd_df(len(copies), opcode=[OP_COPY] * len(copies),
               dep_group=pl.Series([maxd + 1] * len(copies)),
               path=[os.path.join(src_root, p) for p in copies["path"]],
               dst_path=[os.path.join(dst_root, p) for p in copies["path"]],
               size=copies["size"],
               mode=(copies["mode"] & 0o7777).cast(pl.Int32)),
        _setmeta_cmds(dst_root, copies, maxd + 2) if preserve_times
        else cmd_df(0),
        cmd_df(len(unlinks), opcode=[OP_UNLINK] * len(unlinks),
               dep_group=pl.Series([maxd + 3] * len(unlinks)),
               path=[os.path.join(dst_root, p) for p in unlinks["path"]]),
        cmd_df(len(rmdirs), opcode=[OP_RMDIR] * len(rmdirs),
               dep_group=(maxd + 4 + maxd - rmdirs["depth"]),
               path=[os.path.join(dst_root, p) for p in rmdirs["path"]]),
    ]).with_columns(user_data=pl.int_range(
        len(mkdirs) + len(copies) * (2 if preserve_times else 1)
        + len(unlinks) + len(rmdirs), dtype=pl.UInt64))
    summary = pl.DataFrame({
        "op": ["mkdir", "copy", "unlink", "rmdir"],
        "count": [len(mkdirs), len(copies), len(unlinks), len(rmdirs)]})
    return cmds, summary


def sync(src_root: str, dst_root: str, delete: bool = True,
         engine: str = "auto", wal: str | None = None,
         threads: int = 8, preserve_times: bool = True) -> pl.DataFrame:
    """One-way sync src → dst; idempotent (mtimes preserved unless
    preserve_times=False)."""
    src = scan(src_root, engine, threads).with_columns(DEPTH)
    os.makedirs(dst_root, exist_ok=True)
    dst = scan(dst_root, engine, threads)
    cmds, summary = sync_cmds(src, dst, src_root, dst_root, delete,
                              preserve_times=preserve_times)
    if len(cmds):
        _run(cmds, engine, wal)
    return summary


# ── ducl migration: emit pwalk2's Feather schema from the new scanner ─────

def ducl_frame(root: str, engine: str = "auto",
               threads: int = 8) -> pl.DataFrame:
    """Produce a DataFrame matching ducl's Feather SCHEMA (schema.py) from
    one artar_exec scan — replaces the pwalk2 CSV → pyarrow.csv hop.
    Faithful to pw2_output.c: mode as '%07o', times in seconds, dirs at
    depth-1, child_count/dir_total_size = direct children (files) only,
    files get child_count=-1 / dir_total_size=0."""
    df = scan(root, engine, threads)
    absroot = os.path.abspath(root)
    per_dir = (df.filter(~pl.col("is_dir"))
                 .group_by("parent_ino")
                 .agg(child_count=pl.len().cast(pl.Int64),
                      dir_total_size=pl.col("size").sum()))
    mode_oct = pl.concat_str(
        [(pl.col("mode") // (8 ** k) % 8).cast(pl.String)
         for k in range(6, -1, -1)]).alias("mode")
    return (df.join(per_dir, left_on="ino", right_on="parent_ino",
                    how="left")
              .select(
        inode_id=pl.col("ino"),
        parent_inode_id=pl.col("parent_ino"),
        depth=(pl.col("depth") - 1).cast(pl.Int32),  # pwalk2: parent depth
        path=pl.lit(absroot + "/") + pl.col("path"),
        ext=pl.col("path").str.extract(r"\.([^./\\]+)$", 1).fill_null(""),
        uid=pl.col("uid").cast(pl.UInt32),
        gid=pl.col("gid").cast(pl.UInt32),
        size=pl.col("size"),
        fs_id=pl.col("dev"),
        blocks=pl.col("blocks"),
        nlink=pl.col("nlink").cast(pl.UInt32),
        mode=mode_oct,
        atime=pl.col("atime_ns") // 1_000_000_000,
        mtime=pl.col("mtime_ns") // 1_000_000_000,
        ctime=pl.col("ctime_ns") // 1_000_000_000,
        child_count=pl.when(pl.col("is_dir"))
                      .then(pl.col("child_count").fill_null(0))
                      .otherwise(-1),
        dir_total_size=pl.when(pl.col("is_dir"))
                         .then(pl.col("dir_total_size").fill_null(0))
                         .otherwise(0),
    ))



def pack(root: str, archive_path: str, fmt=None, engine: str = "auto",
         threads: int = 8, select: pl.Expr | None = None) -> pl.DataFrame:
    """Create a nock archive (raw or tar host): C scan → layout query →
    unified COPY commands targeting the archive fd → footer commit."""
    fmt = fmt or nock.RawFormat()
    lf = scan_lazyframe(root, engine, threads)
    if select is not None:
        lf = lf.filter(select)
    plan = nock.plan_layout(lf, fmt)
    has_hdr = "header" in plan.columns
    n = len(plan)
    cmds = cmd_df(
        n, opcode=[OP_COPY] * n,
        path=[os.path.join(root, p) for p in plan["path"]],
        header=plan["header"] if has_hdr
               else pl.Series([b""] * n, dtype=pl.Binary),
        header_offset=plan["offset"] if has_hdr
                      else pl.zeros(n, pl.Int64, eager=True),
        data_offset=plan["data_offset"], size=plan["size"],
        pad_align=pl.Series([fmt.align] * n),
        mode=plan["mode"].cast(pl.Int32))
    # durability epoch: payloads must be stable before the footer commits
    cmds = pl.concat([cmds, cmd_df(1, user_data=pl.Series([n],
                                   dtype=pl.UInt64),
                                   opcode=[OP_FBARRIER],
                                   dep_group=pl.Series([1],
                                                       dtype=pl.Int64))])
    afd = os.open(archive_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        comp = run_commands(cmds, archive_path, engine)
        assert comp.filter(pl.col("user_data") == n)["res"][0] == 0, \
            "durability barrier failed"
        comp = comp.filter(pl.col("user_data") < n)
        rs = dict(zip(comp["user_data"], comp["read_size"]))
        failed = set(comp.filter(pl.col("res") < 0)["user_data"])
        idx = nock.finish_archive(afd, plan, rs, failed, fmt)
        os.fsync(afd)                       # footer/trailer durable too
        return idx
    finally:
        os.close(afd)

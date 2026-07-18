"""
quiver.stream — pipelined tools: execute while still scanning.

Ordering model (learned the hard way): per *worker*, a directory's row
precedes its children's rows; across workers there is NO ordering — a
child's chunk can flush before its parent's row does. So the streaming
planner must be monotone against out-of-order arrival. The trick: never
wait for a dir's row to create the dir. Each batch derives the ancestor
directories it needs from its own paths (mkdir -p semantics, dedup'd by
a created-set), so any arrival order works; the real dir rows only
contribute metadata, applied in a SETMETA tail after the stream drains
(which also restores directory mtimes — deepest first, so parent
mtimes aren't re-dirtied — something the batch cp never did).

The planner classification this module embodies (mirrors classic query
engines): a per-batch plan pipelines iff it is *monotone* — each input
row's commands depend only on rows already seen. Monotone: mkdir+copy
(cp), file unlinks (rm), layout with a running offset (pack,
arrival-order), aggregation folds (du). Pipeline breakers: rmdir
(needs subtree completeness), sync's full outer join (needs the other
side — unless the other side is a manifest lookup), and sort policies
(need the full set). The breakers run as a small tail after the stream
drains; everything else overlaps the scan.
"""

from __future__ import annotations

import os

import polars as pl

from . import nock
from .tools import DEPTH, _setmeta_cmds
from .wire import (OP_COPY, OP_FBARRIER, OP_MKDIR, OP_RMDIR, OP_UNLINK,
                   PipeExecutor, cmd_df, scan_iter)

_MODE = pl.col("mode") & 0o7777


def _check(comp: pl.DataFrame) -> None:
    bad = comp.filter(pl.col("res") < 0)
    assert not len(bad), bad


def _ancestors(path: str):
    parts = path.split("/")
    for k in range(1, len(parts) + 1):
        yield "/".join(parts[:k])


def stream_cp(src_root: str, dst_root: str, engine: str = "auto",
              threads: int = 8) -> int:
    """cp that copies while the scan is still running."""
    os.makedirs(dst_root, exist_ok=True)
    ex = PipeExecutor("-", engine=engine)
    created: set[str] = set()
    dir_meta = []                    # real dir rows → metadata tail
    total = 0
    try:
        for b in scan_iter(src_root, engine, threads):
            b = b.with_columns(_MODE)
            dir_meta.append(b.filter(pl.col("is_dir"))
                            .select("path", "mode", "mtime_ns", "depth"))
            files = b.filter(~pl.col("is_dir"))
            need: list[str] = []
            for p in b["path"]:
                d = p if p in set(b.filter(pl.col("is_dir"))["path"]) \
                    else os.path.dirname(p)
                for a in _ancestors(d) if d else ():
                    if a not in created:
                        created.add(a)
                        need.append(a)
            maxd = max((a.count("/") for a in need), default=0) + 1
            cmds = pl.concat([
                cmd_df(len(need), opcode=[OP_MKDIR] * len(need),
                       dep_group=pl.Series([a.count("/") for a in need],
                                           dtype=pl.Int64),
                       path=[os.path.join(dst_root, a) for a in need]),
                cmd_df(len(files), opcode=[OP_COPY] * len(files),
                       dep_group=pl.Series([maxd] * len(files),
                                           dtype=pl.Int64),
                       path=[os.path.join(src_root, p)
                             for p in files["path"]],
                       dst_path=[os.path.join(dst_root, p)
                                 for p in files["path"]],
                       size=files["size"],
                       mode=files["mode"].cast(pl.Int32)),
                _setmeta_cmds(dst_root, files, maxd + 1),
            ]).with_columns(user_data=pl.int_range(
                len(need) + 2 * len(files), dtype=pl.UInt64)).sort(
                "dep_group", maintain_order=True)
            if len(cmds):
                _check(ex.execute(cmds))
            total += len(b)
        # metadata tail: dir modes + mtimes, deepest first
        dirs = pl.concat(dir_meta) if dir_meta else None
        if dirs is not None and len(dirs):
            dirs = dirs.sort("depth", descending=True)
            n = len(dirs)
            cmds = cmd_df(n, opcode=[8] * n,   # OP_SETMETA
                          path=[os.path.join(dst_root, p)
                                for p in dirs["path"]],
                          mode=dirs["mode"].cast(pl.Int32),
                          mtime_ns=dirs["mtime_ns"])
            _check(ex.execute(cmds))
    finally:
        assert ex.close() == 0
    return total


def stream_rm(root: str, engine: str = "auto", threads: int = 8,
              remove_root: bool = True) -> int:
    """rm that unlinks files while the scan is still running (the bulk
    of the ops); the rmdir tail — the only pipeline breaker — drains
    after the stream ends."""
    ex = PipeExecutor("-", engine=engine)
    n = 0
    dir_frames = []
    try:
        for b in scan_iter(root, engine, threads):
            dir_frames.append(b.filter(pl.col("is_dir")))
            files = b.filter(~pl.col("is_dir"))
            if len(files):
                cmds = cmd_df(len(files), opcode=[OP_UNLINK] * len(files),
                              path=[os.path.join(root, p)
                                    for p in files["path"]])
                _check(ex.execute(cmds))
                n += len(files)
        dirs = (pl.concat(dir_frames).with_columns(DEPTH)
                if dir_frames else None)
        tail = []
        if dirs is not None and len(dirs):
            maxd = dirs["depth"].max()
            tail.append(cmd_df(
                len(dirs), opcode=[OP_RMDIR] * len(dirs),
                dep_group=(maxd - dirs["depth"]).cast(pl.Int64),
                path=[os.path.join(root, p) for p in dirs["path"]]))
            n += len(dirs)
        if remove_root:
            base = sum(len(t) for t in tail)
            tail.append(cmd_df(1, opcode=[OP_RMDIR],
                               dep_group=pl.Series([10**6], dtype=pl.Int64),
                               path=[os.path.abspath(root)]))
            n += 1
        if tail:
            cmds = pl.concat(tail).with_columns(
                user_data=pl.int_range(sum(len(t) for t in tail),
                                       dtype=pl.UInt64)).sort(
                "dep_group", maintain_order=True)
            _check(ex.execute(cmds))
    finally:
        assert ex.close() == 0
    return n


def stream_pack(root: str, archive_path: str, fmt=None,
                engine: str = "auto", threads: int = 8) -> pl.DataFrame:
    """pack that writes the archive while the scan is still running.
    Layout is a running prefix-sum (arrival order, not sorted — the
    sort policy is the one pipeline breaker this mode gives up); the
    footer accumulates and commits after an FBARRIER, same durability
    ordering as batch pack."""
    fmt = fmt or nock.RawFormat()
    afd = os.open(archive_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    ex = PipeExecutor(archive_path, engine=engine)
    run = 0
    footer_parts = []
    try:
        for b in scan_iter(root, engine, threads):
            plan = nock.plan_layout(b.with_columns(_MODE).lazy(), fmt,
                                    base_offset=run, sort=False)
            if not len(plan):
                continue
            run = int(plan["offset"][-1] + plan["block_len"][-1])
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
            comp = ex.execute(cmds)
            _check(comp)
            rs = dict(zip(comp["user_data"], comp["read_size"]))
            footer_parts.append(plan.with_columns(
                read_size=pl.Series([rs[i] for i in range(n)],
                                    dtype=pl.Int64)))
        _check(ex.execute(cmd_df(1, opcode=[OP_FBARRIER])))
    finally:
        assert ex.close() == 0
    try:
        footer = (pl.concat(footer_parts)
                  .select(nock.footer.FOOTER_COLS)
                  if footer_parts else None)
        if footer is not None:
            nock.write_footer(afd, footer, fmt.name, run, fmt.eof_marker)
            os.fsync(afd)
        return footer if footer is not None else pl.DataFrame()
    finally:
        os.close(afd)

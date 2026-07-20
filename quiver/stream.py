"""
quiver.stream — one streaming framework, tools expressed as (step, finish).

The whole design rests on the project's single ordering primitive: a
command batch is a barrier (the executor drains it before the next).
So batched and streamed execution are the *same computation at different
granularities* — plan the whole scan as one batch, or plan each scan
batch as it arrives. `drive()` is that one loop; a tool is a `Plan` with:

    step(batch, state) -> cmds | None    # monotone: uses only rows seen
    finish(state)      -> cmds | None    # the breaker tail

`step` output executes immediately (overlapping the still-running scan);
`finish` output runs once after the stream drains. A tool is *monotone*
(fully streaming) iff each row's commands depend only on rows already
seen — mkdir+copy (cp), file unlinks (rm), arrival-order layout (pack).
The *breakers* live in `finish`: rmdir (needs subtree completeness),
sort policies (need the full set), and sync's outer join (needs the
other side). Feed a `Plan` `scan_iter(...)` for streaming or a single
collected scan for batch mode — identical code, the barrier does the
ordering either way.

Ordering caveat the multithreaded scanner forces: parent-before-child
holds per *worker*, not globally — a child's chunk can arrive before its
parent's row. So `step` must be monotone against out-of-order arrival:
never wait for a dir's row to create the dir. cp derives the ancestors
it needs from each batch's own paths (mkdir -p, deduped by a created
set); real dir rows only contribute metadata, applied deepest-first in
the finish tail (which also restores directory mtimes).
"""

from __future__ import annotations

import os

import polars as pl

from . import nock
from .tools import DEPTH, _setmeta_cmds
from .wire import (OP_COPY, OP_FBARRIER, OP_MKDIR, OP_RMDIR, OP_SETMETA,
                   OP_UNLINK, PipeExecutor, cmd_df, scan, scan_iter)

_MODE = pl.col("mode") & 0o7777


def _check(comp: pl.DataFrame) -> None:
    bad = comp.filter(pl.col("res") < 0)
    assert not len(bad), bad


def _ancestors(path: str):
    parts = path.split("/")
    for k in range(1, len(parts) + 1):
        yield "/".join(parts[:k])


class Plan:
    """A tool as a per-batch step + a breaker finish. Default finish is
    empty (fully monotone tools)."""
    archive = "-"

    def init(self) -> dict:
        return {}

    def step(self, batch: pl.DataFrame, state: dict):
        return None

    def finish(self, state: dict):
        return None


def drive(plan: Plan, source, engine: str = "auto") -> dict:
    """Run `plan` over a sequence of scan batches through one persistent
    executor. `source` is `scan_iter(...)` (streaming) or `[scan(...)]`
    (batch) — the loop is identical; the batch barrier orders both."""
    ex = PipeExecutor(plan.archive, engine=engine)
    state = plan.init()
    try:
        for batch in source:
            cmds = plan.step(batch, state)
            if cmds is not None and len(cmds):
                _check(ex.execute(cmds))
        cmds = plan.finish(state)
        if cmds is not None and len(cmds):
            _check(ex.execute(cmds))
    finally:
        assert ex.close() == 0
    return state


# ── cp: mkdir-p + copy (monotone), dir metadata in the finish tail ─────────

class CpPlan(Plan):
    def __init__(self, src_root: str, dst_root: str):
        self.src_root, self.dst_root = src_root, dst_root
        os.makedirs(dst_root, exist_ok=True)

    def init(self):
        return {"created": set(), "dir_meta": [], "total": 0}

    def step(self, batch, state):
        batch = batch.with_columns(_MODE)
        dirs_here = set(batch.filter(pl.col("is_dir"))["path"])
        state["dir_meta"].append(
            batch.filter(pl.col("is_dir"))
                 .select("path", "mode", "mtime_ns", "depth"))
        files = batch.filter(~pl.col("is_dir"))
        need: list[str] = []
        for p in batch["path"]:
            d = p if p in dirs_here else os.path.dirname(p)
            for a in (_ancestors(d) if d else ()):
                if a not in state["created"]:
                    state["created"].add(a)
                    need.append(a)
        maxd = max((a.count("/") for a in need), default=0) + 1
        state["total"] += len(batch)
        return pl.concat([
            cmd_df(len(need), opcode=[OP_MKDIR] * len(need),
                   dep_group=pl.Series([a.count("/") for a in need],
                                       dtype=pl.Int64),
                   path=[os.path.join(self.dst_root, a) for a in need]),
            cmd_df(len(files), opcode=[OP_COPY] * len(files),
                   dep_group=pl.Series([maxd] * len(files), dtype=pl.Int64),
                   path=[os.path.join(self.src_root, p)
                         for p in files["path"]],
                   dst_path=[os.path.join(self.dst_root, p)
                             for p in files["path"]],
                   size=files["size"], mode=files["mode"].cast(pl.Int32)),
            _setmeta_cmds(self.dst_root, files, maxd + 1),
        ]).with_columns(user_data=pl.int_range(
            len(need) + 2 * len(files), dtype=pl.UInt64)).sort(
            "dep_group", maintain_order=True)

    def finish(self, state):
        if not state["dir_meta"]:
            return None
        dirs = pl.concat(state["dir_meta"])
        if not len(dirs):
            return None
        dirs = dirs.sort("depth", descending=True)   # deepest first
        n = len(dirs)
        return cmd_df(n, opcode=[OP_SETMETA] * n,
                      path=[os.path.join(self.dst_root, p)
                            for p in dirs["path"]],
                      mode=dirs["mode"].cast(pl.Int32),
                      mtime_ns=dirs["mtime_ns"])


# ── rm: streaming forest-refcount over the scan's close-events ─────────────

class RmPlan(Plan):
    """Files unlink as they arrive; a directory is rmdir'd the instant
    its emitted-child count (the scan close-event) is fully accounted
    for by removals — mid-stream, not in a finish tail. Pure path
    arithmetic: removing a child 'd/f' decrements the parent 'd'; the
    root's children have dirname '' which is the root's own rel, so the
    refcount closes at the top too. Needs scan(closes=True).

    Ordering inside one frame: unlinks at epoch 0, cascade-ready rmdirs
    at depth-ordered later epochs (deeper first). The executor's
    per-epoch barrier makes every unlink complete before any rmdir and
    every child-dir rmdir before its parent's — so no completion
    feedback loop is needed; readiness is decided at emit time because
    emission guarantees completion before any later epoch."""
    MAXD = 1 << 20                       # depth ceiling for epoch layering

    def __init__(self, root: str, remove_root: bool = True):
        self.root, self.remove_root = root, remove_root

    def init(self):
        import collections
        return {"gone": collections.defaultdict(int),  # child removals emitted
                "total": {},                            # child_count once closed
                "done": set(), "n": 0}

    def _drain(self, state) -> list[str]:
        """Dirs now rmdir-able, cascading bottom-up (a rmdir frees its
        parent's slot, which may make the parent ready in the same pass)."""
        gone, total, done = state["gone"], state["total"], state["done"]
        out, changed = [], True
        while changed:
            changed = False
            for d in list(total):
                if d in done or gone[d] != total[d]:
                    continue
                done.add(d)
                if d == "" and not self.remove_root:
                    continue                      # keep the root
                out.append(d)
                changed = True
                if d != "":
                    gone[os.path.dirname(d)] += 1  # this dir's own removal
        return out

    def _emit(self, state, files: list[str], rmdirs: list[str]):
        n = len(files) + len(rmdirs)
        if not n:
            return None
        state["n"] += n
        rmdirs = sorted(rmdirs, key=lambda d: -d.count("/"))   # deepest first
        parts = [
            cmd_df(len(files), opcode=[OP_UNLINK] * len(files),
                   dep_group=pl.zeros(len(files), pl.Int64, eager=True),
                   path=[os.path.join(self.root, p) for p in files]),
            cmd_df(len(rmdirs), opcode=[OP_RMDIR] * len(rmdirs),
                   dep_group=pl.Series([1 + self.MAXD - d.count("/")
                                        for d in rmdirs], dtype=pl.Int64),
                   path=[os.path.abspath(self.root) if d == ""
                         else os.path.join(self.root, d) for d in rmdirs]),
        ]
        return pl.concat(parts).with_columns(
            user_data=pl.int_range(n, dtype=pl.UInt64)).sort(
            "dep_group", maintain_order=True)

    def step(self, batch, state):
        cl = batch.filter(pl.col("child_count") >= 0)      # close-events
        for p, c in zip(cl["path"], cl["child_count"]):
            state["total"][p] = int(c)
        files = batch.filter((pl.col("child_count") < 0)
                             & ~pl.col("is_dir"))["path"].to_list()
        for p in files:
            state["gone"][os.path.dirname(p)] += 1
        return self._emit(state, files, self._drain(state))

    def finish(self, state):
        return self._emit(state, [], self._drain(state))    # root, late dirs


# pack is the one tool that does NOT fit (step, finish): it consumes the
# completion frames (each COPY reports read_size, which the footer needs
# to record truth when a file shrank between stat and read) and writes a
# trailer to an fd it owns after the barrier. drive() executes and
# discards completions, so stream_pack keeps its own loop below — the
# honest edge of the abstraction, not a wart to paper over.


# ── public entry points: stream via scan_iter, batch via [scan] ────────────

def _source(root: str, engine: str, threads: int, streaming: bool,
            closes: bool = False):
    """The only difference between streaming and batch: many scan
    batches, or one. The Plan and the driver are identical."""
    if streaming:
        return scan_iter(root, engine, threads, closes=closes)
    return [scan(root, engine, threads, closes=closes)]


def stream_cp(src_root: str, dst_root: str, engine: str = "auto",
              threads: int = 8, streaming: bool = True) -> int:
    plan = CpPlan(src_root, dst_root)
    return drive(plan, _source(src_root, engine, threads, streaming),
                 engine)["total"]


def stream_rm(root: str, engine: str = "auto", threads: int = 8,
              remove_root: bool = True, streaming: bool = True) -> int:
    plan = RmPlan(root, remove_root)
    return drive(plan, _source(root, engine, threads, streaming,
                               closes=True), engine)["n"]


def stream_pack(root: str, archive_path: str, fmt=None,
                engine: str = "auto", threads: int = 8) -> pl.DataFrame:
    """pack while scanning. Layout is a running prefix-sum (arrival
    order — the sort policy is the one breaker this mode gives up); the
    footer accumulates and commits after the FBARRIER, same durability
    ordering as batch pack. read_size is captured per batch by running
    the executor directly here (pack needs completions, which drive()
    discards), so this keeps its own loop rather than using drive()."""
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
        footer = (pl.concat(footer_parts).select(nock.footer.FOOTER_COLS)
                  if footer_parts else None)
        if footer is not None:
            nock.write_footer(afd, footer, fmt.name, run, fmt.eof_marker)
            os.fsync(afd)
        return footer if footer is not None else pl.DataFrame()
    finally:
        os.close(afd)

"""
quiver.wal — write-ahead intent log with resume.

The command frame *is* the WAL (that was the original point of the
intent-log design): persist it before executing, append completions as
they drain, and resume is a filter. Files:

    <wal>        the full command frame (nock feather, inspectable —
                 dry-run is `wal.status(path)`)
    <wal>.done   append-only IPC stream of completion batches

Crash anywhere is safe: an unrecorded chunk simply re-executes, and
every opcode is idempotent under replay — COPY re-pwrites the same
offsets, MKDIR tolerates EEXIST, UNLINK/RMDIR treat ENOENT as done
(the work already happened), SETMETA/CKSUM/FBARRIER are naturally
re-runnable. `--retry-failed` is not a mode: failed rows are simply
never marked done, so rerunning the WAL retries them.
"""

from __future__ import annotations

import errno
import os

import polars as pl

from .nock.footer import _df_from_feather, _feather_bytes
from .pupyarrow.writer import StreamReader, StreamWriter
from .wire import (CMD_PL, OP_RMDIR, OP_UNLINK, PipeExecutor, cmd_df)

_COMP_SCHEMA = [("user_data", "u64"), ("res", "i32")]

# errno values that mean "the intended state already holds" per opcode
_IDEMPOTENT_OK = {OP_UNLINK: {-errno.ENOENT}, OP_RMDIR: {-errno.ENOENT}}


def write(path: str, cmds: pl.DataFrame) -> None:
    """Persist the intent log (sorted by dep_group — execution order)."""
    cmds = cmds.sort("dep_group", maintain_order=True)
    with open(path, "wb") as f:
        f.write(_feather_bytes(cmds, {"nock_version": "1",
                                      "quiver_wal": "1"}))


def load(path: str) -> pl.DataFrame:
    df, meta = _df_from_feather(open(path, "rb").read())
    assert meta.get("quiver_wal") == "1", "not a quiver WAL"
    return df.cast(dict(CMD_PL))


def _load_log(path: str) -> pl.DataFrame:
    """All recorded completions, last record per user_data wins (a later
    successful retry supersedes an earlier failure)."""
    done_path = path + ".done"
    if not os.path.exists(done_path):
        return pl.DataFrame(schema={"user_data": pl.UInt64,
                                    "res": pl.Int32})
    frames = []
    with open(done_path, "rb") as f:
        try:
            for b in StreamReader(f):
                frames.append(pl.DataFrame(b))
        except (AssertionError, ValueError):
            pass          # torn tail from a crash mid-append: ignore it
    if not frames:
        return pl.DataFrame(schema={"user_data": pl.UInt64,
                                    "res": pl.Int32})
    return (pl.concat(frames).cast({"user_data": pl.UInt64,
                                    "res": pl.Int32})
            .unique(subset="user_data", keep="last"))


def _done_set(path: str) -> set[int]:
    log = _load_log(path)
    cmds = load(path).select("user_data", "opcode")
    j = log.join(cmds, on="user_data")
    ok = [r["res"] == 0
          or r["res"] in _IDEMPOTENT_OK.get(r["opcode"], ())
          for r in j.to_dicts()]
    return set(j.filter(pl.Series(ok))["user_data"]) if len(j) else set()


def failures(path: str) -> pl.DataFrame:
    """Error table: latest-known failures joined with their commands.
    Rerunning the WAL retries exactly these rows."""
    log = _load_log(path)
    cmds = load(path)
    j = log.join(cmds, on="user_data")
    bad = [r["res"] < 0
           and r["res"] not in _IDEMPOTENT_OK.get(r["opcode"], ())
           for r in j.to_dicts()]
    return (j.filter(pl.Series(bad)) if len(j) else j).select(
        "user_data", "res", "opcode", "path", "dst_path")


def status(path: str) -> pl.DataFrame:
    """Dry-run / progress view: the intent log joined with done-ness."""
    cmds = load(path)
    done = _done_set(path)
    return cmds.with_columns(
        done=pl.col("user_data").is_in(list(done)))


def execute(path: str, archive_path: str = "-", engine: str = "auto",
            batch_rows: int = 4096) -> pl.DataFrame:
    """(Re)execute a WAL: skip rows already completed, append newly
    completed rows to the done-log after every chunk. Chunks never
    straddle a dep_group boundary beyond what sorting guarantees, and
    every chunk boundary is a barrier."""
    cmds = load(path)
    done = _done_set(path)
    keep = (~pl.col("user_data").is_in(list(done)))
    # parent_row references positions in the persisted frame; filtering
    # for resume shifts positions, so remap: parents that are already
    # done need no notification (-1), surviving parents get their new
    # position.
    import numpy as np
    mask = cmds.select(keep.alias("k"))["k"].to_numpy()
    new_pos = np.full(len(cmds), -1, dtype=np.int64)
    new_pos[mask] = np.arange(int(mask.sum()))
    pr = cmds["parent_row"].to_numpy()
    remapped = np.where(pr >= 0, new_pos[np.clip(pr, 0, None)], -1)
    todo = (cmds.with_columns(parent_row=pl.Series(remapped,
                                                   dtype=pl.Int64))
            .filter(pl.Series(mask)))
    if not len(todo):
        return pl.DataFrame({"executed": [0], "failed": [0]})

    done_path = path + ".done"
    fresh = not os.path.exists(done_path)
    dl = open(done_path, "ab")
    dw = StreamWriter(dl, _COMP_SCHEMA, write_schema=fresh)

    ex = PipeExecutor(archive_path, engine=engine)
    executed = failed = 0
    try:
        for start in range(0, len(todo), batch_rows):
            chunk = todo[start:start + batch_rows]
            comp = ex.execute(chunk, batch_rows=batch_rows)
            j = comp.join(chunk.select("user_data", "opcode"),
                          on="user_data")
            okmask = [r["res"] == 0
                      or r["res"] in _IDEMPOTENT_OK.get(r["opcode"], ())
                      for r in j.to_dicts()]
            executed += sum(okmask)
            failed += len(j) - sum(okmask)
            # record everything; _done_set filters, failures() surfaces
            dw.write_batch([j["user_data"].to_numpy(),
                            j["res"].to_numpy()])
            dl.flush()
            os.fsync(dl.fileno())
    finally:
        try:
            ex.close()
        except (BrokenPipeError, OSError):
            pass                      # executor died; done-log is truth
        dl.close()
    return pl.DataFrame({"executed": [executed], "failed": [failed]})

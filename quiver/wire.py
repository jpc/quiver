"""
quiver.wire — the pipe protocol to quiver-exec, pyarrow-free.

CMD frames go out through pupyarrow's StreamWriter; COMP and STAT frames
come back through its StreamReader. polars only ever sees plain
numpy/list columns.
"""

from __future__ import annotations

import os
import subprocess

import numpy as np
import polars as pl

from .pupyarrow.writer import StreamReader, StreamWriter

OP_UNLINK, OP_RMDIR, OP_MKDIR, OP_COPY, OP_CKSUM = 2, 3, 4, 5, 6
OP_FBARRIER, OP_SETMETA, OP_EXTRACT = 7, 8, 9

CMD_SCHEMA = [
    ("user_data", "u64"), ("opcode", "u8"), ("dep_group", "i64"),
    ("path", "large_string"), ("dst_path", "large_string"),
    ("header", "large_binary"), ("header_offset", "i64"),
    ("data_offset", "i64"), ("size", "i64"), ("pad_align", "i64"),
    ("mode", "i32"), ("mtime_ns", "i64"), ("uid", "i32"), ("gid", "i32"),
    ("parent_row", "i64"),
]
CMD_PL = pl.Schema({
    "user_data": pl.UInt64, "opcode": pl.UInt8, "dep_group": pl.Int64,
    "path": pl.String, "dst_path": pl.String, "header": pl.Binary,
    "header_offset": pl.Int64, "data_offset": pl.Int64, "size": pl.Int64,
    "pad_align": pl.Int64, "mode": pl.Int32, "mtime_ns": pl.Int64,
    "uid": pl.Int32, "gid": pl.Int32, "parent_row": pl.Int64,
})

EXE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "exec", "quiver-exec")


def _engine(e: str) -> str:
    """QUIVER_FORCE_ENGINE overrides all engine choices — the CI escape
    hatch for environments where io_uring is seccomp-blocked."""
    return os.environ.get("QUIVER_FORCE_ENGINE") or e


def cmd_df(n: int, **cols) -> pl.DataFrame:
    base = {
        "user_data": pl.int_range(n, dtype=pl.UInt64, eager=True),
        "opcode": pl.zeros(n, pl.UInt8, eager=True),
        "dep_group": pl.zeros(n, pl.Int64, eager=True),
        "path": pl.Series([""] * n), "dst_path": pl.Series([""] * n),
        "header": pl.Series([b""] * n, dtype=pl.Binary),
        "header_offset": pl.zeros(n, pl.Int64, eager=True),
        "data_offset": pl.zeros(n, pl.Int64, eager=True),
        "size": pl.zeros(n, pl.Int64, eager=True),
        "pad_align": pl.ones(n, pl.Int64, eager=True),
        # -1 sentinels: "leave alone" for SETMETA, "default" for COPY/MKDIR
        "mode": pl.Series([-1] * n, dtype=pl.Int32),
        "mtime_ns": pl.Series([-1] * n, dtype=pl.Int64),
        "uid": pl.Series([-1] * n, dtype=pl.Int32),
        "gid": pl.Series([-1] * n, dtype=pl.Int32),
        "parent_row": pl.Series([-1] * n, dtype=pl.Int64),
    }
    base.update({k: (v if isinstance(v, pl.Series) else pl.Series(v))
                 for k, v in cols.items()})
    return pl.DataFrame(base).cast(dict(CMD_PL))


def _df_cols(df: pl.DataFrame):
    out = []
    for name, t in CMD_SCHEMA:
        s = df[name]
        out.append(s.to_list() if t in ("large_string", "large_binary")
                   else s.to_numpy())
    return out


def _to_pl(batch: dict) -> pl.DataFrame:
    return pl.DataFrame({k: (v if isinstance(v, list) else v)
                         for k, v in batch.items()})


def _popen_spawn(argv: list[str]):
    return subprocess.Popen(argv, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)


class PipeExecutor:
    """Drives one quiver-exec over stdin/stdout Arrow frames. The
    protocol is transport-agnostic bytes, so where the executor RUNS is
    decoupled from how it is DRIVEN:

    - `transport` is an argv prefix — [] local, ["ssh", host], or
      ["srun", "-w", node] — spawned with Popen (the default `spawn`).
    - `spawn` overrides process creation entirely for backends that are
      not argv-prefixable (e.g. a Modal Sandbox). It takes the argv and
      returns a handle exposing `.stdin` (write bytes), `.stdout` (read
      bytes) and `.wait() -> int`. subprocess.Popen already satisfies
      this; a Modal adapter wraps a ContainerProcess to match."""

    def __init__(self, archive_path: str = "-", engine: str = "auto",
                 exe: str = EXE, transport: list[str] | None = None,
                 spawn=None):
        argv = (transport or []) + [exe, "exec", archive_path,
                                    _engine(engine)]
        self.proc = (spawn or _popen_spawn)(argv)
        self.writer = StreamWriter(self.proc.stdin, CMD_SCHEMA)
        self.proc.stdin.flush()
        self.reader = StreamReader(self.proc.stdout)

    def execute(self, cmds: pl.DataFrame,
                batch_rows: int = 4096) -> pl.DataFrame:
        """parent_row values are positions within `cmds`. Chunking is
        safe because every batch boundary is a barrier: a parent landing
        in a later chunk starts after all earlier chunks completed, so
        its in-chunk refcount correctly ignores already-finished
        children. We rebase to chunk-local indices here (-1 when the
        parent lies outside the chunk)."""
        outs = []
        for start in range(0, len(cmds), batch_rows):
            chunk = cmds[start:start + batch_rows]
            end = start + len(chunk)
            pr = pl.col("parent_row")
            chunk = chunk.with_columns(
                pl.when((pr >= start) & (pr < end)).then(pr - start)
                  .otherwise(-1).alias("parent_row"))
            self.writer.write_batch(_df_cols(chunk))
            self.proc.stdin.flush()
            outs.append(_to_pl(self.reader.read_batch()))
        return pl.concat(outs) if outs else pl.DataFrame(
            schema={"user_data": pl.UInt64, "res": pl.Int32,
                    "read_size": pl.Int64, "cksum": pl.UInt64,
                    "etag": pl.Binary, "parts": pl.Int32})

    def close(self) -> int:
        self.writer.close()
        self.proc.stdin.close()
        self.reader.read_batch()
        return self.proc.wait()


def run_commands(cmds: pl.DataFrame, archive_path: str = "-",
                 engine: str = "auto") -> pl.DataFrame:
    ex = PipeExecutor(archive_path, engine=engine)
    try:
        return ex.execute(cmds.sort("dep_group", maintain_order=True))
    finally:
        assert ex.close() == 0


SCAN_PL = pl.Schema({
    "path": pl.String, "size": pl.Int64, "blocks": pl.Int64,
    "mtime_ns": pl.Int64, "atime_ns": pl.Int64, "ctime_ns": pl.Int64,
    "ino": pl.UInt64, "parent_ino": pl.UInt64, "dev": pl.UInt64,
    "mode": pl.Int32, "uid": pl.Int32, "gid": pl.Int32, "nlink": pl.Int32,
    "depth": pl.Int32, "is_dir": pl.Boolean, "child_count": pl.Int64})


def scan_iter(root: str, engine: str = "auto", threads: int = 8,
              prefix: str = "", glob: str = "", exe: str = EXE,
              transport: list[str] | None = None, closes: bool = False):
    """Streaming scan: yields one stat frame per IPC batch as the
    scanner produces them. The stream invariant that makes pipelined
    execution correct: a directory's row is always emitted in an
    earlier batch than any of its children's rows (a child chunk exists
    only because its parent was already processed and queued)."""
    root = root if transport else os.path.abspath(root)
    proc = subprocess.Popen((transport or []) + [exe, "scan", root,
                            _engine(engine), str(threads), prefix, glob,
                            "1" if closes else "0"],
                            stdout=subprocess.PIPE)
    try:
        for b in StreamReader(proc.stdout):
            yield _to_pl(b).with_columns(
                pl.col("is_dir").cast(pl.Boolean))
    finally:
        assert proc.wait() == 0, "scanner failed"


def scan(root: str, engine: str = "auto", threads: int = 8,
         prefix: str = "", glob: str = "",
         exe: str = EXE, transport: list[str] | None = None,
         closes: bool = False) -> pl.DataFrame:
    """quiver-exec scan → stat frame. `prefix` prunes whole subtrees
    (stage-1 pushdown: their getdents/statx never happen); `glob`
    filters basenames and skips statx for known-regular misses
    (stage 2)."""
    root2 = root if transport else os.path.abspath(root)
    proc = subprocess.Popen((transport or []) + [exe, "scan", root2,
                            _engine(engine), str(threads), prefix, glob,
                            "1" if closes else "0"],
                            stdout=subprocess.PIPE)
    dfs = [_to_pl(b) for b in StreamReader(proc.stdout)]
    assert proc.wait() == 0, "scanner failed"
    if not dfs:
        return pl.DataFrame(schema=SCAN_PL)
    return pl.concat(dfs).with_columns(pl.col("is_dir").cast(pl.Boolean))


def scan_lazyframe(root: str, engine: str = "auto",
                   threads: int = 8) -> pl.LazyFrame:
    return (scan(root, engine, threads)
            .with_columns(mode=pl.col("mode") & 0o7777).lazy())

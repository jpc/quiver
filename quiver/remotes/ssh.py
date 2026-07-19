"""
quiver.remotes.ssh — remote execution over ssh.

Design: nothing new crosses the wire except bytes the protocol already
carries. `ssh host quiver-exec …` is driven verbatim: remote scan is
the STAT stream over ssh stdout; remote execution is the CMD stream
over ssh stdin. The data plane for sync rides *inside* the protocol —
the `header` column is arbitrary bytes written at `header_offset`, so a
changed file becomes a chain of inline-payload COPY rows (chunk 0 owns
the truncate; parent_row chains order the chunks). Verification pulls
only hashes back across the wire: the remote executor CKSUMs, the local
one CKSUMs, and the comparison is a join.
"""

from __future__ import annotations

import os

import polars as pl

from ..tools import DEPTH, _setmeta_cmds, sync_cmds
from ..wire import (OP_CKSUM, OP_COPY, PipeExecutor, cmd_df, scan)

CHUNK = 1 << 20


class Ssh:
    """A remote quiver endpoint. exe is the path of quiver-exec on the
    remote (deploy: scp one static binary)."""

    def __init__(self, host: str, exe: str = "quiver-exec",
                 ssh_opts: list[str] | None = None):
        self.transport = ["ssh", *(ssh_opts or []), host]
        self.exe = exe

    def scan(self, root: str, engine: str = "auto",
             threads: int = 8, **kw) -> pl.DataFrame:
        return scan(root, engine, threads, exe=self.exe,
                    transport=self.transport, **kw)

    def executor(self, archive: str = "-",
                 engine: str = "auto") -> PipeExecutor:
        return PipeExecutor(archive, engine, exe=self.exe,
                            transport=self.transport)

    def _inline_copy_rows(self, src_root: str, copies: pl.DataFrame,
                          dst_root: str, epoch: int) -> pl.DataFrame:
        """Payload chunks as COPY rows: header=bytes, header_offset=
        file offset. Chunks of one file are ordered by a parent_row
        chain (k depends on k-1); chunk 0's offset-0 write owns the
        truncate."""
        rows = {k: [] for k in ("path", "dst_path", "header",
                                "header_offset", "mode", "parent_row")}
        idx = 0
        for r in copies.to_dicts():
            data = open(os.path.join(src_root, r["path"]), "rb").read()
            n_chunks = max(1, (len(data) + CHUNK - 1) // CHUNK)
            for k in range(n_chunks):
                rows["path"].append("")
                rows["dst_path"].append(
                    os.path.join(dst_root, r["path"]))
                rows["header"].append(data[k * CHUNK:(k + 1) * CHUNK])
                rows["header_offset"].append(k * CHUNK)
                rows["mode"].append(r["mode"] & 0o7777)
                # chunk k is a child of chunk k+1: parent waits for it
                rows["parent_row"].append(idx + 1 if k < n_chunks - 1
                                          else -1)
                idx += 1
        n = idx
        return cmd_df(
            n, opcode=[OP_COPY] * n,
            dep_group=pl.Series([epoch] * n, dtype=pl.Int64),
            path=rows["path"], dst_path=rows["dst_path"],
            header=pl.Series(rows["header"], dtype=pl.Binary),
            header_offset=pl.Series(rows["header_offset"],
                                    dtype=pl.Int64),
            mode=pl.Series(rows["mode"], dtype=pl.Int32),
            parent_row=pl.Series(rows["parent_row"], dtype=pl.Int64))

    def sync_to(self, src_root: str, dst_root: str,
                engine: str = "auto", delete: bool = True,
                local_engine: str = "auto") -> pl.DataFrame:
        """Local tree → remote tree. Plan locally from both scans; the
        remote executor runs mkdirs/deletes/setmeta verbatim and
        receives changed payloads inline."""
        src = scan(src_root, local_engine).with_columns(DEPTH)
        ex = self.executor(engine=engine)
        try:
            ex.execute(cmd_df(1, opcode=[4],          # OP_MKDIR dst root
                              path=[dst_root]))
            dst = self.scan(dst_root, engine)
            cmds, summary = sync_cmds(src, dst, src_root, dst_root,
                                      delete)
            copies = cmds.filter((pl.col("opcode") == OP_COPY))
            others = cmds.filter(pl.col("opcode") != OP_COPY)
            if len(copies):
                # replace path-based copies with inline chunks at the
                # same epoch (the source paths don't exist remotely)
                epoch = int(copies["dep_group"][0])
                src_files = src.filter(
                    pl.col("path").is_in([
                        os.path.relpath(p, dst_root)
                        for p in copies["dst_path"]]))
                inline = self._inline_copy_rows(src_root, src_files,
                                                dst_root, epoch)
                # parent_row is positional: after concat the inline
                # block starts at len(others) (concat is stable and the
                # subsequent sort is too, with others' epochs ≤ copy
                # epoch), so shift the chain indices by that base
                base = len(others.filter(
                    pl.col("dep_group") < epoch))
                inline = inline.with_columns(
                    pl.when(pl.col("parent_row") >= 0)
                      .then(pl.col("parent_row") + base)
                      .otherwise(-1).alias("parent_row"))
                pre = others.filter(pl.col("dep_group") < epoch)
                post = others.filter(pl.col("dep_group") > epoch)
                # stable sort: pre-block rows all < epoch, inline rows
                # == epoch, so the inline block's start (= len(pre))
                # survives sorting and the parent_row chain stays valid
                cmds = pl.concat([pre, inline, post]).sort(
                    "dep_group", maintain_order=True)
            cmds = cmds.sort("dep_group", maintain_order=True) \
                       .with_columns(user_data=pl.int_range(
                           len(cmds), dtype=pl.UInt64))
            if len(cmds):
                comp = ex.execute(cmds)
                bad = comp.filter(pl.col("res") < 0)
                assert not len(bad), bad
            return summary
        finally:
            assert ex.close() == 0

    def verify(self, src_root: str, dst_root: str,
               engine: str = "auto", part_size: int = 5 << 20,
               local_engine: str = "auto") -> pl.DataFrame:
        """Content verification: both sides CKSUM, only hashes cross
        the wire. Returns mismatching paths (empty = trees identical)."""
        def cksums(frame, root, executor):
            files = frame.filter(~pl.col("is_dir"))
            n = len(files)
            if n == 0:
                return pl.DataFrame(schema={"path": pl.String,
                                            "cksum": pl.UInt64})
            comp = executor.execute(cmd_df(
                n, opcode=[OP_CKSUM] * n,
                path=[os.path.join(root, p) for p in files["path"]],
                pad_align=pl.Series([part_size] * n, dtype=pl.Int64)))
            return pl.DataFrame({"path": files["path"],
                                 "cksum": comp.sort("user_data")["cksum"]})
        lex = PipeExecutor("-", local_engine)
        rex = self.executor(engine=engine)
        try:
            a = cksums(scan(src_root, local_engine), src_root, lex)
            b = cksums(self.scan(dst_root, engine), dst_root, rex)
        finally:
            assert lex.close() == 0 and rex.close() == 0
        j = a.join(b, on="path", how="full", suffix="_r", coalesce=True)
        return j.filter(pl.col("cksum").is_null()
                        | pl.col("cksum_r").is_null()
                        | (pl.col("cksum") != pl.col("cksum_r")))

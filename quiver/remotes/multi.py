"""
quiver.remotes.multi — run a command plan across N nodes.

The filesystem dependency graph is a forest: operations in disjoint
top-level subtrees are independent, so each node runs its whole shard
(all epochs) locally with no cross-node barrier. The only global
serialization point is the root — created before the fan-out (cp),
removed after the join (rm). Keeping each subtree on one client also
avoids the cross-client rmdir visibility stall we measured; the lone
cross-client op is the root rmdir, which carries the ENOTEMPTY retry.

Transports decouple WHERE the executor runs from HOW it is driven. The
Arrow-over-pipe protocol is identical for all of them:

  - Ssh / Slurm: an argv prefix ("ssh host" / "srun -w node") in front
    of quiver-exec, spawned with Popen. Both IREN nodes are separate
    WEKA clients over the same /mnt/weka — the FE-count ceiling we
    measured means N clients ≈ N× metadata throughput.
  - Modal: not argv-prefixable, so it overrides PipeExecutor's `spawn`
    to exec quiver-exec inside a Modal Sandbox with a Volume mounted,
    wrapping the ContainerProcess streams to look like pipes. (Draft —
    see ModalTransport; validate against Modal's stream API before use.)

Partitioning is cheap in Polars: derive a subtree key (one vectorized
string pass), size subtrees (one group_by), greedily bin-pack the
DISTINCT subtrees across nodes (tiny Python loop — never over rows),
map back (vectorized replace), split with one partition_by.
"""

from __future__ import annotations

import concurrent.futures as cf
import os
import subprocess

import polars as pl

from ..wire import EXE, OP_MKDIR, PipeExecutor


# ── partitioning ───────────────────────────────────────────────────────────

def subtree_key(path_col: str, root: str) -> pl.Expr:
    """First path component under `root` — the affinity bucket. The row
    that IS the root maps to '' (handled on the coordinator)."""
    prefix = root.rstrip("/") + "/"
    return (pl.col(path_col).str.strip_prefix(prefix)
            .str.split("/").list.first().fill_null(""))


def partition_plan(cmds: pl.DataFrame, root: str, n: int,
                   affinity_col: str = "path"
                   ) -> tuple[pl.DataFrame, list[pl.DataFrame]]:
    """Split `cmds` into (root_ops, [shard_0 .. shard_{n-1}]), balanced
    by op count via greedy longest-processing-time bin-packing of the
    distinct subtrees. Empty shards come back as empty frames so callers
    can zip 1:1 with executors."""
    keyed = cmds.with_columns(_sub=subtree_key(affinity_col, root))
    root_ops = keyed.filter(pl.col("_sub") == "").drop("_sub")
    body = keyed.filter(pl.col("_sub") != "")
    if not len(body):
        return root_ops, [cmds.clear() for _ in range(n)]

    sizes = body.group_by("_sub").len().sort("len", descending=True)
    load = [0] * n
    assign: dict[str, int] = {}
    for sub, cnt in sizes.iter_rows():
        k = min(range(n), key=load.__getitem__)   # least-loaded bin
        assign[sub] = k
        load[k] += cnt

    body = body.with_columns(
        _node=pl.col("_sub").replace_strict(assign, return_dtype=pl.UInt32))
    parts = body.partition_by("_node", as_dict=True)
    shards = [(parts[(i,)].drop("_sub", "_node") if (i,) in parts
               else cmds.clear()) for i in range(n)]
    return root_ops, shards


# ── transports: where the executor runs ────────────────────────────────────

class Transport:
    """Produces a PipeExecutor bound to one node/container."""
    def executor(self, archive: str = "-",
                 engine: str = "auto") -> PipeExecutor:
        raise NotImplementedError


class LocalTransport(Transport):
    def __init__(self, exe: str = EXE):
        self.exe = exe

    def executor(self, archive="-", engine="auto"):
        return PipeExecutor(archive, engine, exe=self.exe)


class SshTransport(Transport):
    def __init__(self, host: str, opts=(), exe: str = EXE):
        self.host, self.opts, self.exe = host, list(opts), exe

    def executor(self, archive="-", engine="auto"):
        return PipeExecutor(archive, engine, exe=self.exe,
                            transport=["ssh", *self.opts, self.host])


class SlurmTransport(Transport):
    """Runs quiver-exec on a node of the CURRENT allocation via srun
    --overlap (no re-queue). Requires running inside salloc/sbatch."""
    def __init__(self, node: str, exe: str = EXE,
                 srun_opts=("--overlap", "-N1", "-n1")):
        self.node, self.exe, self.srun_opts = node, exe, list(srun_opts)

    def executor(self, archive="-", engine="auto"):
        return PipeExecutor(archive, engine, exe=self.exe,
                            transport=["srun", *self.srun_opts,
                                       "-w", self.node])


class ModalTransport(Transport):
    """DRAFT — untested (needs a Modal account + the `modal` package).

    Spawns quiver-exec inside a Modal Sandbox with `volume` mounted at
    `mount`, and adapts the ContainerProcess streams to the byte-pipe
    interface PipeExecutor expects. Modal's exec streams differ from
    file objects (write()+drain / read(n)); the adapter below is the
    shape to validate against the installed Modal version."""
    def __init__(self, app, image, volume, mount: str = "/mnt/data",
                 exe: str = "quiver-exec"):
        self.app, self.image = app, image
        self.volume, self.mount, self.exe = volume, mount, exe

    def _spawn(self, argv):
        import modal
        sb = modal.Sandbox.create(app=self.app, image=self.image,
                                  volumes={self.mount: self.volume})
        # argv is [exe, "exec", archive, engine]; run it in the sandbox
        proc = sb.exec(*argv)

        class _Handle:                       # adapt to .stdin/.stdout/.wait
            def __init__(h):
                h.stdin = proc.stdin         # write bytes; flush() may no-op
                h.stdout = proc.stdout       # read(n) bytes
                h._sb, h._p = sb, proc
            def wait(h):
                h._p.wait(); h._sb.terminate(); return h._p.returncode
        return _Handle()

    def executor(self, archive="-", engine="auto"):
        return PipeExecutor(archive, engine, exe=self.exe, spawn=self._spawn)


def ssh_transports(hosts, opts=(), exe: str = EXE) -> list[SshTransport]:
    return [SshTransport(h, opts, exe) for h in hosts]


def slurm_transports(exe: str = EXE) -> list[SlurmTransport]:
    """One transport per node of the current SLURM allocation."""
    nodelist = os.environ["SLURM_JOB_NODELIST"]
    hosts = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                           capture_output=True, text=True,
                           check=True).stdout.split()
    return [SlurmTransport(h, exe=exe) for h in hosts]


# ── the distributed executor ────────────────────────────────────────────────

class MultiExecutor:
    """Fans a plan across N transports by subtree affinity. Duck-types
    the PipeExecutor execute/close contract, so run_commands drives it
    unchanged. Batch mode (the whole plan is available to partition and
    balance); execs[0] is the coordinator that owns the root ops."""

    def __init__(self, root: str, transports: list[Transport],
                 archive: str = "-", engine: str = "auto",
                 affinity_col: str = "path"):
        self.root, self.affinity_col = root, affinity_col
        self.execs = [t.executor(archive, engine) for t in transports]

    def _run(self, ex, shard):
        return ex.execute(shard.sort("dep_group", maintain_order=True))

    def execute(self, cmds: pl.DataFrame) -> pl.DataFrame:
        root_ops, shards = partition_plan(
            cmds, self.root, len(self.execs), self.affinity_col)
        pre = root_ops.filter(pl.col("opcode") == OP_MKDIR)   # dst_root first
        post = root_ops.filter(pl.col("opcode") != OP_MKDIR)  # root rmdir last
        comps = []
        if len(pre):
            comps.append(self._run(self.execs[0], pre))
        with cf.ThreadPoolExecutor(max(1, len(self.execs))) as tp:
            futs = [tp.submit(self._run, ex, s)
                    for ex, s in zip(self.execs, shards) if len(s)]
            comps += [f.result() for f in futs]     # join == global barrier
        if len(post):
            comps.append(self._run(self.execs[0], post))
        return (pl.concat(comps) if comps
                else cmds.clear().select("user_data"))

    def close(self) -> int:
        rc = 0
        for ex in self.execs:
            rc |= ex.close()
        return rc


def run_distributed(cmds: pl.DataFrame, root: str,
                    transports: list[Transport], affinity_col: str = "path",
                    archive: str = "-", engine: str = "auto") -> pl.DataFrame:
    """Batch analogue of wire.run_commands, spread across `transports`."""
    ex = MultiExecutor(root, transports, archive, engine, affinity_col)
    try:
        return ex.execute(cmds)
    finally:
        ex.close()

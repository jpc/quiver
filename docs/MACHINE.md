# quiver as a virtual machine for IO

quiver is easiest to reason about not as a bag of file tools but as a **virtual
machine**: a compiler front-end (Polars) lowers high-level intent into an
**instruction stream**, and a back-end (`quiver-exec`) executes it, retiring
**completions**. Naming that makes most of the architecture — and the pending
refactor — fall out as consequences rather than choices.

## The mapping

| VM concept | quiver |
|---|---|
| ISA / opcodes | `OP_UNLINK … OP_COMPRESS` (`quiver/opcodes.py`) |
| Instruction word | a command row: `CMD_SCHEMA` columns are the operand fields |
| Program / bytecode | the command DataFrame, as an Arrow-IPC stream |
| Compiler front-end | the Polars planner; each tool (`rm`/`cp`/`pack`/recompress) is a source language that lowers to opcodes |
| Interpreter loop | `quiver-exec`'s dispatch (`row_sync` + the ring scheduler) |
| Functional units | two execution ports — the io_uring **ring** (metadata: unlink/mkdir/rmdir/fsync) and the 64-worker **pool** (bytes: copy/cksum/compress) |
| Retirement / result bus | the completion stream (`COMP`: `res`, `read_size`, `cksum`, …) |
| Reorder-buffer tag | `user_data` — completions come back out of order and re-associate to their issuing row |
| Memory-ordering model | `dep_group` (epochs) + `OP_FBARRIER` (a fence instruction) |
| Address space + relocation | the archive body + the **nock footer** (member → byte range); `extract` is a deref |
| Linker | writing the footer — resolving logical names to physical offsets after layout |
| Multicore / cluster | `MultiExecutor` sharding the stream across nodes; the spawn seam (ssh/SLURM/Modal) is the interconnect |
| Journal / checkpoint | the WAL — replay the instructions whose completion never landed |

## What the lens explains

**io_uring is itself an IO VM — quiver is a portable one, one level up.** SQEs are
instructions, CQEs completions, the submission queue an instruction stream with
ordering flags. quiver lowers its ISA *onto* io_uring when it's available and
onto the thread **pool** when it isn't — the ring is the JIT, the pool is the
portable interpreter, and `QUIVER_FORCE_ENGINE` / `ring == NULL` is "run the
interpreter." A VM's defining property is that the ISA decouples the program
from the machine; that is exactly why the same command stream runs on a laptop,
over ssh, or under SLURM without the planner knowing.

**The plan/execute split is the static/dynamic-value boundary.** Whatever the
compiler can know it puts in the instruction (paths, member→frame assignment);
whatever only exists at run time it reads back from retirement. `OP_COMPRESS`
returns its frame's `(coff, clen)` in the completion because those are computed
by the execution unit, then the linker writes them into the footer. The
"plannable vs un-plannable" halves are just compile-time vs run-time values.

**The ISA is data-parallel, deliberately.** A command stream is a vector of
lanes; the pool runs them in parallel; `dep_group` is predication/ordering
across lanes. There is no control flow in the ISA — loops and branches live in
the compiler (Polars). That's a feature, the same bet a GPU kernel or a database
physical plan makes: straight-line, divergence-free work vectorizes and
distributes. quiver is a **vector/dataflow IO machine**, not a general-purpose
one.

**Distribution and journaling are free.** Because the interface is an
instruction stream over a bus, sharding it across machines (`MultiExecutor`) and
logging it for replay (WAL) are operations *on the stream*, not features bolted
onto each tool.

## What the lens says about the refactor

The recompress/reshard fold (`zpack`/`zscan`/`zexec`) **forked the ISA**: it
stood up a second machine with its own instruction encoding (the plan file),
its own result encoding (60-byte records), and its own bus, running beside the
real one. That is the anomaly `docs/INTERFACES.md` is about, and the VM lens
gives the principle behind fixing it and the acceptance test for future work:

> A new capability should be an **instruction** in the one ISA, executed by the
> one interpreter over the one bus — not a new *mode* with its own encodings.

So the remaining refactor stages are precisely "re-merge the forked machine":
the plan becomes an `OP_COMPRESS` command stream, the footer records become
`COMP` completions, and resume becomes the existing WAL replaying uncommitted
instructions. Stage 1 already did this for the front-end (`zscan` now speaks the
same `ZMETA`/`StreamReader` path as `scan`) and unified the opcode namespace so
the two planes can't define different ISAs.

## Where the analogy strains

It is not Turing-complete and shouldn't be: no branches, no loops, no
mutable in-ISA state. It's closer to a database's physical-plan executor or a
GPU command buffer than to a CPU. The complementary database lens fits just as
well — planner → vectorized physical operators → an indexed store — and both
lenses agree on the same discipline: one planner, one physical ISA, one
execution engine, one index.

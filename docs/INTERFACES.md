# Component interfaces

quiver is a **Polars control plane** driving a **C data plane** (`quiver-exec`)
over pipes, with a self-describing on-disk index (the nock footer). Almost the
whole system is expressible through a handful of universal contracts. This
doc names them, points out where the zframe/recompress fold grew parallel
bespoke formats, and sketches how to collapse them back onto the universal
ones.

## The two universal interfaces

Everything the control plane asks of the data plane, and everything it learns
back, is one of two Arrow-IPC streams over a pipe (`quiver/wire.py`):

1. **Command stream — Python → C.** A Polars DataFrame with `CMD_SCHEMA`
   columns (`opcode`, `path`, `dst_path`, `header`, `data_offset`, `size`,
   `pad_align`, `mode`/`mtime_ns`/`uid`/`gid`, `dep_group`, `parent_row`, …),
   serialized by pupyarrow's `StreamWriter` into the executor's stdin. Every
   tool — `rm`, `cp`, `sync`, `pack`, `extract` — *compiles to a command
   DataFrame* and hands it to `PipeExecutor.execute`. `opcode` (the `OP_*`
   enum) selects the executor path; the other columns are its arguments.

2. **Result stream — C → Python.** Arrow-IPC batches back through
   `StreamReader`:
   - **COMP** (completions): `user_data` (row id), `read_size`, `cksum`,
     `errno` — one row per executed command.
   - **STAT** (scan rows): `path`, `size`, `mode`, `mtime_ns`, `uid`, `gid`,
     `is_dir`, `child_count`, … — the `scan` output the planner sees as a
     plain DataFrame.

The planner never parses bytes: it builds DataFrames and reads DataFrames.
That is the whole contract.

## Supporting universal contracts

- **Opcode enum** (`OP_UNLINK … OP_EXTRACT`). One small integer namespace
  shared by `wire.py` and the C `enum`. The dispatch key of interface (1).
- **The pipe seam** (`PipeExecutor` `spawn`). The command/result streams don't
  care *where* the executor runs — `_popen_spawn` is overridable, so the same
  two streams run over a local pipe, ssh, `srun --overlap`, or a Modal
  sandbox. `quiver/remotes/multi.py` shards a command DataFrame across several
  of these by subtree affinity; each shard is still just interface (1)+(2).
- **The nock footer.** An Arrow-IPC stream parked at EOF with a self-locating
  `NOCKIDX1` trailer, spliced into any host (a tar, a raw blob, a zstd
  skippable frame, or a `.nock` sidecar). The universal *index*: member →
  byte range. Written once, read by `read_index`/`extract` regardless of host.
- **Format adapters** (`TarFormat`, `RawFormat`). Pluggable host layout for
  `pack`; the executor only sees `header` bytes + offsets, never the format.
- **The WAL** (`quiver/wal.py`). Persisted command batches for crash-resumable
  execution (`test_wal_resume`): log the commands, replay the unfinished ones.

## Where the fold grew parallel formats

The recompress/reshard path (`zpack`/`zscan`/`zexec` in `quiver-exec.c`,
`quiver/nock/zframe.py`) was built fast and, for the **metadata plane**,
reinvented each universal contract as a bespoke binary format:

| Fold format | Bytes | Universal analogue it duplicates |
|---|---|---|
| `zscan` member record | `[u16 plen][path][36B tail]` | **STAT** stream |
| plan file | `[nsrc][nsink][start[]][counts[]][ents]` | **command** stream |
| footer record (`zpack`/`zexec` → Python) | `[u16 plen][path][60B tail]` | **COMP** stream + nock footer |
| recompress WAL | raw footer records appended | `wal.py` |

Plus two smaller drifts:

- `OP_COMPRESS = 10` exists in the C `enum` but **not** in `wire.py`'s opcode
  list — the shared enum has already split.
- The **byte plane** (compressed frames) legitimately needs its own
  high-bandwidth path — `pwrite` to a file, or a FIFO to an S3 uploader — and
  that is *fine*; it is separate from the metadata plane and should stay
  separate. The debt is only in the metadata plane above.

## How to simplify: collapse onto the universal interfaces

The fold's three bespoke metadata formats can each become the universal one,
deleting ~200 lines of hand-rolled parsing and making recompress "just another
Polars-planned, Arrow-wired tool". Staged so the proven `zpack` byte path is
touched last:

1. **`zscan` → the STAT stream.** ✅ *Done.* `zscan` emits a `ZMETA` Arrow-IPC
   batch (`path`, `source_id`, `ordinal`, stat) via the same template
   machinery as scan; `_zscan` reads it through `StreamReader` into a Polars
   DataFrame — no bespoke 36B record, no manual column building. This path
   never touched `zpack`'s output, so it was the safe first stage.

2. **plan file → the command stream.** *Next.* The plan is a command DataFrame:
   `opcode = OP_COMPRESS`, one row per kept member carrying `source_id`,
   `ordinal`, `frame`, `sink`, and the per-sink `start` — reusing `cmd_df`
   with a few added columns (or overloading `dep_group`/`parent_row`).
   `zexec` stops being a bespoke plan-file loader and becomes the executor
   running `OP_COMPRESS`, fed through the normal `StreamWriter`.

3. **footer records → the COMP stream + nock footer.** `zpack`/`zexec` emit
   Arrow COMP batches (member fields + `frame`/`coff`/`clen`/`sink`).
   `_ingest_footer` becomes "read completions → write the nock footer," which
   is what the footer writer already does everywhere else.

4. **recompress WAL → `wal.py`.** Log the `OP_COMPRESS` command batches (and
   completions) with the existing, tested WAL. Resume = replay the commands
   whose completion never landed — the same mechanism as `rm`/`cp`, no
   separate append format.

5. **One opcode source of truth.** ✅ *Done.* Opcodes live in
   `quiver/opcodes.py`; `wire.py` derives its `OP_*` from it and
   `gen_templates.py` emits `#define OP_* n` into `ipc_gen.h`, so the C data
   plane and Python control plane can no longer drift (`OP_COMPRESS` was
   C-only before this).

### Why this is a win, not just tidier

- **Less code, one mental model.** Recompress joins every other tool: build a
  command DataFrame, read a result DataFrame. No new wire formats to learn or
  keep in sync.
- **Actually faster.** Batched columnar Arrow IPC moves member metadata more
  efficiently than the per-record, length-prefixed encoding the fold uses
  (one framed batch vs. millions of tiny records).
- **Free features.** Distributed execution (`multi.py`), the tested WAL, and
  the spawn-seam transports all operate on the command stream — unifying the
  fold onto it means reshard-to-S3 and crash resume inherit multi-node
  execution instead of needing bespoke plumbing.

### What deliberately stays separate

The **byte plane** is not metadata and should not go through Arrow IPC: the
compressed frames flow `pwrite`→file or FIFO→S3-uploader at GB/s, sized to the
work, with per-sink backpressure. The unification above is strictly about the
*control* and *index* planes — commands in, completions/scan out, footer at
rest — which is where uniformity pays off.

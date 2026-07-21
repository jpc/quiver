# The quiver IO ISA

Every existing and planned feature recast as operations of one machine, so that
implementation adds *instructions and addressing modes*, not *modes*. Read
`docs/MACHINE.md` first for the framing; this is the instruction-set detail.

## 0. Instructions vs programs vs services

Three levels get confused, so pin them down:

- **Instructions** — what the machine executes. Only these are the ISA. There
  are four classes (§1).
- **Programs** — compiler front-ends that lower to an instruction stream, living
  entirely in Polars: `du`, `rm`, `cp`, `sync`/`rsync`, `pack`, `extract`,
  `recompress`, `reshard`. The machine never sees them; they emit instructions.
  (`du` emits *none* — it's a pure query over a generator's output.)
- **Services** — runtime/µarch machinery that isn't an instruction: the linker
  (footer writer), the WAL journal, the S3 uploader (a sink's implementation),
  the dependency scheduler. They operate *on* the streams, not *in* them.

So `rm` and `rsync` are **programs**, not instructions. `scan` is an
**instruction** — but a generator (§1), not a transformer.

## 1. Instruction classes

Two ports and four classes:

- **Generators** (read an address space → a row stream; *1 request → N rows*,
  no command input). `SCAN` reads a namespace subtree via getdents+statx and
  streams `STAT` rows; `ZSCAN` reads a `tar.zstd` via decompress+parse and
  streams `ZMETA` rows. These are the machine's **read-state** side — the
  operand supply the compiler plans over. They correspond to reading the `FILE`
  and `STREAM` source address spaces (§2) *as a table*. They run on the **scan
  port** (root in → rows out); everything below runs on the **execute port**
  (command stream in → completion stream out).
- **Control-path** transformers (namespace + metadata; latency-bound → io_uring
  ring): `MKDIR`, `UNLINK`, `RMDIR`, `SETMETA` (and the natural future `LINK`,
  `SYMLINK`, `RENAME`). Names and inode attributes, never bulk bytes.
- **Data-path** transformers (bytes; throughput-bound → 64-worker pool): `COPY`,
  `CKSUM`, `EXTRACT`, `COMPRESS` — *one instruction family*, see §2.
- **Ordering**: `FBARRIER` — a fence + durability point, issuable to either
  transformer unit.

Transformers are *1 command → 1 completion*; generators are *1 → N*; that
cardinality difference is why `scan`/`zscan` have their own invocation (the
scan port) rather than riding the command stream.

## 2. The data-path is one instruction, parameterized

`COPY`/`EXTRACT`/`COMPRESS`/`CKSUM` are the same operation — **move a byte
range from a source, through a transform, to a sink, and report a result** —
differing only in three operand fields. Naming those fields *is* the
simplification; new features become new field values, not new opcodes.

**Source addressing modes** — where the operand bytes come from:
| mode | operand | used by |
|---|---|---|
| `FILE(path, off, len)` | a filesystem byte range | COPY, CKSUM |
| `ARCHIVE(off, len)` | a range of the nock host | EXTRACT |
| `INLINE(header)` | bytes carried in the instruction | small members in pack |
| `STREAM(source_id, member)` | a member of a `tar.zstd` input | recompress |

`STREAM` is special: the bytes don't exist until decoded, so it needs a **fetch
stage** — the decompress+parse "reader" — that gathers members into frame
operands. That fetch stage is the one genuinely new execution hardware the fold
requires; everything else is a field value.

**Transforms** — applied to the operand in flight:
`IDENTITY` (copy), `ZSTD_C(level)` (compress), `ZSTD_D` (decompress),
`CKSUM` (CRC-64, produce no output).

**Sink addressing modes** — where the result bytes go:
| mode | operand | used by |
|---|---|---|
| `FILE(path, off)` | a filesystem file | cp, extract |
| `ARCHIVE_APPEND` | append to the host, offset assigned at run time | pack, recompress |
| `STREAM(sink_id)` | an external stream (a FIFO → S3 multipart) | reshard-to-S3 |
| `NULL` | discard (CKSUM only) | verify |

So: `COPY = XFER(FILE→FILE, IDENTITY)`, `EXTRACT = XFER(ARCHIVE→FILE)`,
`COMPRESS = XFER(STREAM/INLINE→ARCHIVE_APPEND, ZSTD_C)`,
`CKSUM = XFER(FILE→NULL, CKSUM)`. Reshard is just the sink `sink_id`; S3 is a
sink mode; decompress-on-extract is a transform. None of these need a new
machine.

## 3. Static vs dynamic destinations — the plan/link boundary

A sink offset is either **compile-time** (the planner knows the layout because
member sizes are known — pack lays out sequential offsets) or **run-time** (the
size isn't known until the transform runs — `ZSTD_C` output length). The latter
is why `ARCHIVE_APPEND` assigns the offset at retirement and reports it in the
completion. The **linker** (footer writer) merges both into one relocation
table: static offsets from the plan, dynamic offsets from completions. This is
exactly quiver's "plannable / un-plannable" split, now named.

## 4. Ordering model

Independent instructions run in parallel (max ILP). Ordering is expressed two
ways today, both lowerings of one partial order:
- `dep_group` (epochs): instructions in a group are a scheduling barrier — used
  by `rm` to put a directory's children in an earlier epoch than the `RMDIR`.
- `parent_row` (refcount): a dependency edge — a child decrements its parent's
  count; the parent issues when it hits zero.
- `FBARRIER`: a full fence + fsync.

The clean statement: **each instruction may declare dependencies; the scheduler
executes the DAG.** Epochs and refcounts are two encodings; a future ISA can
pick one.

## 5. Result / completion model

Each instruction retires a completion tagged by `user_data` (the reorder-buffer
tag — completions arrive out of order and re-associate). The result register
file is small and currently *role-overloaded*:
`res` (status/errno), `read_size` (bytes written **or** `coff`), `cksum`
(checksum **or** `clen`), `etag`/`parts` (S3 multipart). A clean ISA names
result fields by role (`out_offset`, `out_len`, `checksum`, `status`) instead
of reusing `read_size`/`cksum`.

## 6. Every tool and feature, recast

| feature | as a VM program |
|---|---|
| `scan` | read machine state: stream `STAT` rows (operand fetch for the compiler) |
| `du` | a Polars **query** (aggregation) over the `scan` table — no instructions |
| `rm` | compile `UNLINK`+`RMDIR` with child→parent ordering |
| `cp` | compile `MKDIR`+`COPY`+`SETMETA`, dirs scheduled before files |
| `sync` | compile a reconciliation program from a `scan`×`scan` diff |
| `pack` | compile `COPY→ARCHIVE_APPEND` with **static** offsets; link the footer |
| `extract` | resolve names via the footer, compile `EXTRACT`+`MKDIR`+`SETMETA` |
| **recompress** | `zscan` = fetch `STREAM` state → plan → `COMPRESS` (`STREAM→ARCHIVE_APPEND`, `ZSTD_C`) → link from completions |
| **reshard** | recompress with a per-instruction `sink_id` (fan the sink) |
| **S3 stream** | sink mode `STREAM(sink_id)` → FIFO → multipart uploader |
| **WAL resume** | journal retired instructions; on restart the compiler elides the ones already committed |
| **distribute** | shard the instruction stream across interpreters by subtree affinity; merge results |

The point of the table: recompress is *structurally pack* — same program shape
— with source mode `STREAM` instead of `FILE` and transform `ZSTD_C` instead of
`IDENTITY`. It became a separate machine only by accident.

## 7. Current warts (what the ISA cleanup removes)

- **Overloaded `pad_align`**: it's both copy alignment and `ZSTD_C` level. →
  a dedicated `xform_param`.
- **Overloaded results**: `read_size`=`coff`, `cksum`=`clen`. → role-named
  result fields.
- **Implicit addressing modes**: source/sink modes are baked into the opcode
  rather than being operand fields. → explicit `src_mode`/`sink_mode`.
- **The forked ISA**: recompress's plan file + 60-byte records + bespoke WAL
  are a parallel encoding of the command stream, completion stream, and
  `wal.py`. → re-merge (this is the pending refactor).

## 8. What this means for implementation (before writing it)

The refactor is "lower recompress onto the one ISA," and the ISA view fixes its
shape:

1. **The plan is a command stream, at member granularity.** Each row is a
   placement instruction — `opcode=COMPRESS`, operands `(source_id, ordinal,
   frame, sink, level)` — carried in `CMD_SCHEMA` (adding `frame`/`sink` or
   mapping `frame→dep_group`, `sink→a sink field`, `level→xform_param`). The
   executor reads it with the normal `StreamReader`, not a bespoke plan file.
2. **`STREAM` source ⇒ a fetch stage.** The executor keeps the decompress+parse
   reader, but now as the operand-fetch for `COMPRESS`: it gathers a frame's
   members (by `ordinal`, per the command rows) and hands the frame to the
   `ZSTD_C` unit. Instructions are per-member (micro-ops); `COMPRESS` retires
   per-frame — a legitimate decode-into-µops story, made explicit.
3. **Completions carry the dynamic offset.** `COMPRESS` retires `(coff, clen,
   sink)` in the completion stream (Arrow `COMP`), and the linker writes the
   footer from it — deleting the 60-byte record format.
4. **Resume is the existing WAL.** Journal the `COMPRESS` completions; on
   restart the planner drops committed frames — the same "replay uncommitted
   instructions" the executor WAL already does for `rm`.

Acceptance test for all of it: after the refactor there is **one** instruction
schema in, **one** completion schema out, **one** WAL, and recompress differs
from pack only by field values (`STREAM`/`ZSTD_C`) — no second machine.

## 10. Instruction encoding: narrow the word, don't pad it

The command word is **wide** (`CMD_SCHEMA`, 15 columns) but any one opcode uses
few — `OP_COMPRESS` uses 5 (`source_id`, `ordinal`, `frame`, `sink`, `level`).
The other 10 are dead weight: ~100 B/row vs the ~24 B the operands need, and the
empty `path`/`dst_path`/`header` columns still carry `n×8` offset buffers. On a
33 M-member plan that's a multi-GB stream — and it now *travels to nodes*
(`docs/DISTRIBUTED.md`), so compactness matters. Three ways to shrink it:

1. **Nullable columns.** Mark the unused columns null: an all-null column
   collapses to a validity bitmap (or omits its buffers entirely), and the
   empty-string offset buffers disappear. Arrow IPC also allows per-buffer
   LZ4/ZSTD. *Caveat:* pupyarrow and `parse_cmd_batch` assume every column's
   buffers are fully materialized at fixed indices, so nulls/compression need
   the reader to handle validity and absent buffers — real work, and it keeps
   the wide word.
2. **Narrow schema per *stream*** *(recommended — with a caveat).* Give a
   compact schema to a stream that is *intrinsically one opcode*. This is the
   important correction: batches are **not** opcode-homogeneous in general.
   `row_sync` dispatches **per row** (`switch (c->opcode[i])`), `PipeExecutor`
   chunks by **row range**, and the tools emit **mixed** batches (`sync` =
   `MKDIR`+`COPY`+`UNLINK`+`RMDIR` in one DataFrame, ordered by `dep_group`
   epochs that interleave opcodes). The wide word is precisely what lets any
   opcode sit in any row — so a narrow per-opcode schema is *not* a free swap for
   the general exec stream.

   But the **recompress plan is intrinsically homogeneous** — every row is
   `OP_COMPRESS` — and it rides its *own* stream (`zexec` reads it; `exec` reads
   mixed commands). So it can carry a narrow `ZPLAN` schema (`{source_id,
   ordinal, frame, sink, level}`) without touching the mixed path at all. The
   executor already reads a schema message at the head of each stream; that
   header says which encoding a stream uses — one general wide format for mixed
   batches, a packed format for a single-opcode stream, like a CPU with a
   general encoding plus a SIMD one. Needs the small `ZPLAN` reader (deferred in
   stage 2), no validity machinery.
3. **Just compress on the wire.** zstd the plan stream for transfer — the
   constant/zero columns vanish under general compression. Cheap and immediate,
   but the *parse* cost (materializing full buffers in C) is unchanged.

So: **yes, nulls work**, but the cleaner answer is a narrow schema on the
homogeneous *plan* stream (not opcode-grouping the general path, which fights
per-row dispatch and epoch interleaving). Stage 2 took the wide word (reusing
`parse_cmd_batch`) to land the lowering with zero new C; the narrow `ZPLAN`
schema on the plan stream is the compacting follow-up, and it composes with the
mixed exec stream because they're different streams with different schema
headers.

## 9. Instruction granularity: fuse the architecture, pipeline the µarch

Should the data-path instruction split into separate `LOAD` (fetch/decompress
the operand), `XFORM` (de/compress), and `SINK` (store) instructions? The
stages are real — they already exist as the fold's reader threads, compress
pool, and per-sink writers, with wildly different cost profiles (`ZSTD_D`
~1 GB/s, `ZSTD_C(10)` ~25 MB/s/core, sink = disk or network). So the question
is whether to make them **architectural** (three instructions in the stream) or
keep one fused `COMPRESS` and leave the stages **microarchitectural**.

**Keep one architectural instruction; crack it into a 3-stage pipeline in the
executor.** Three reasons, in order of weight:

1. **The intermediate value is bulk bytes.** In a CPU ISA the value between
   instructions is a register (8 bytes); here it's a ~16 MB frame moving at
   GB/s. Splitting makes that buffer a first-class ISA value the *instruction
   stream* must name and the scheduler must allocate and lifetime-manage —
   which drags the **byte plane into the control plane**, the one boundary the
   whole design defends. Fused, the control plane says "compress member M into
   frame F of sink S" and never touches a byte; the buffer stays a
   microarchitectural detail.
2. **Fusion is where the pipelining lives.** A fused op keeps the frame buffer
   local to one dataflow — gathered, compressed, and handed to its sink without
   a scheduler round-trip. This is why GPUs fuse kernels and databases fuse
   operators rather than materializing intermediates; it's also literally why
   the fold hits 540 MB/s. Splitting would materialize the intermediate through
   a scheduler for no gain on one node.
3. **The fetch is shared, not per-instruction.** One decompression pass of a
   source supplies the operands for *all* its frames — you must not `LOAD` per
   frame. So the `STREAM` fetch is a per-source **operand supply bound to the
   addressing mode** (the reader / `zscan`), feeding many `COMPRESS` ops — a
   shared front-end resource, not a `LOAD` instruction. That already argues
   against a symmetric LOAD/XFORM/SINK split.

The analogy is exact: x86 `add [mem], reg` is **one** architectural instruction
the microarchitecture cracks into load + ALU + store µops. We do the same —
`COMPRESS` is one instruction; the executor's decompress→compress→sink threads
are its µops, tuned independently (`readers`/`compressors` counts, per-sink
backpressure) without appearing in the ISA.

**The one case that flips it: independent *placement*.** Promote the µops to
architectural instructions only when a stage needs to run on a *different
machine* — decode near storage, `ZSTD_C` on compute nodes, `SINK` near S3 — i.e.
a distributed pipeline where the intermediate crosses a network anyway (so
materializing it is no longer free). Single-node throughput doesn't need this
today, so keep it in the back pocket: build `COMPRESS` fused, but keep the three
stages cleanly separable in the executor so a future distributed dataflow can
cut between them. (Checkpointing between stages is *not* a reason — §8.4's WAL
re-decodes to fast-forward, and decode is the cheap 3%.)

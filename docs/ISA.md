# The quiver IO ISA

Every existing and planned feature recast as operations of one machine, so that
implementation adds *instructions and addressing modes*, not *modes*. Read
`docs/MACHINE.md` first for the framing; this is the instruction-set detail.

## 1. Instruction classes

Two functional units, one fence — the whole ISA sorts into three classes:

- **Control-path** (namespace + metadata; latency-bound → io_uring ring):
  `MKDIR`, `UNLINK`, `RMDIR`, `SETMETA` (and the natural future `LINK`,
  `SYMLINK`, `RENAME`). These touch names and inode attributes, never bulk
  bytes.
- **Data-path** (bytes; throughput-bound → 64-worker pool): `COPY`, `CKSUM`,
  `EXTRACT`, `COMPRESS`. These are *one instruction family* — see §2.
- **Ordering**: `FBARRIER` — a fence + durability point, issuable to either
  unit.

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

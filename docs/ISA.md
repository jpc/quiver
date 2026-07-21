# The quiver IO ISA — a buffer machine

Every existing and planned feature recast as operations of one machine, so that
implementation adds *instructions and addressing modes*, not *modes*. Read
`docs/MACHINE.md` first for the framing; this is the instruction-set detail.

This is the **buffer-machine** revision. The earlier ISA had one fused
`COMPRESS` superinstruction that internally fetched, transformed, and sank —
and it argued *against* an explicit intermediate buffer on the grounds that
naming a 16 MB value in the control plane "drags the byte plane into the
control plane." That objection is wrong, and the frame-cache we built to make
the fused form work (a runtime LRU with single-flight and refcounts) is the
proof: it was a *runtime* mechanism rediscovering something the planner already
knows — which members share a decompressed frame, hence exactly when each
buffer is born and dies. **Buffer lifetimes are known ahead of time.** So the
buffer becomes a first-class, planner-allocated ISA value, and the machine
becomes small composable ops over explicit buffers.

## 0. Instructions vs programs vs services

Three levels, pinned down:

- **Instructions** — what the machine executes. Only these are the ISA (§2).
- **Programs** — compiler front-ends that lower to an instruction stream, living
  entirely in Polars: `du`, `rm`, `cp`, `sync`/`rsync`, `pack`, `extract`,
  `recompress`, `reshard`, `pack_fs`, `unpack`. The machine never sees them.
- **Services** — runtime machinery that isn't an instruction: the linker (footer
  writer), the WAL journal, the S3 uploader (a sink's implementation), the
  dependency scheduler, the **buffer allocator**. They operate *on* the streams,
  not *in* them.

## 1. State and ports

- **Generators** (read an address space → a row stream; *1 request → N rows*).
  `SCAN` streams `STAT` rows from a namespace subtree; `ZSCAN` streams `ZMETA`
  rows from a `tar.zstd`; the nock **footer** is a generator with no work at all
  (the whole member→frame table is already on disk). Generators are the operand
  supply the compiler plans over. They run on the **scan port**; everything
  below runs on the **execute port** (command stream in → completion stream out).
- **The execute port** processes the instruction stream (§2) against two
  resources: the **archive fd(s)** / remote sinks, and a **fixed pool of memory
  buffers** — the only managed resource the whole design has.

## 2. The data ISA: three composable ops

The law that shapes everything: **a (de)compression boundary is the only thing
that forces bytes into a managed buffer; nothing else does.** File↔file,
archive-range↔file, archive↔archive, ↔S3 never need one — the consumer pulls the
byte range straight from the source (`pread`/`pwrite`/`splice`, zero-copy). zstd
alone needs contiguous memory. So the data ISA is:

| op | signature | crosses zstd? | touches a buffer? |
|----|-----------|:---:|:---:|
| **COPY** | region → region, identity | no | move-through only |
| **INFLATE** | compressed src-range → `BUF[k]` | decode | **writes** a buffer |
| **DEFLATE** | `BUF[k]` → sink-append | encode | **consumes** a buffer |
| **CKSUM** | region → (checksum, no bytes) | no | no |

A **region** is one of:

| region | operand | zero-copy? |
|---|---|:---:|
| `FILE(path[, off, len])` | a filesystem byte range | yes |
| `ARCHIVE(off, len)` | a range of the nock host fd | yes |
| `S3(key[, range])` | a remote object / part | yes (server-side) |
| `INLINE(bytes)` | bytes carried in the instruction | n/a (already in-stream) |
| `BUF(k, off, len)` | a slice of managed buffer *k* | memcpy |

**COPY is the dumb mover.** It never transforms; it moves region A → region B,
zero-copy whenever neither end is a `BUF`. It subsumes today's `COPY`
(file→archive), `EXTRACT` (archive→file), buffer **scatter** (`BUF→file`) and
**gather** (`file→BUF`), and inline header emission (`INLINE→BUF`). **INFLATE**
and **DEFLATE** are the *only* ops that cross the zstd boundary and are the only
ops that create/retire a buffer. That is the entire architecture:

```
raw extract        COPY  ARCHIVE(o,l) → FILE(p)                 # zero-copy
cp                 COPY  FILE(s) → FILE(d)                      # zero-copy
merge / concat     COPY  ARCHIVE_a(o,l) → ARCHIVE_m            # zero-copy
verify             CKSUM FILE(p) → ⊘
unpack a frame     INFLATE ARCHIVE(coff,clen) → BUF[k]          # + per member:
                   COPY  BUF[k](in_off,size) → FILE(path)       #   scatter
pack a frame       COPY  INLINE(paxhdr) → BUF[k](hoff)          # + per member:
                   COPY  FILE(path) → BUF[k](boff,size)         #   gather
                   DEFLATE BUF[k] → ARCHIVE_APPEND              # then encode
```

**Sinks** for COPY/DEFLATE are `FILE`, `ARCHIVE_APPEND` (offset assigned at
retirement — §7), `S3(sink_id)` (→ multipart uploader), or `⊘` (CKSUM). Reshard
is just which `sink_id` a group's DEFLATE targets; S3 is a sink mode. No new
machine for either.

## 3. Buffers: explicit, planner-allocated, group-scoped

Bytes crossing the zstd boundary live in a **buffered group** — the unit of
dispatch:

```
decode group:   INFLATE →BUF[k];  COPY BUF[k]→…  (× members)
encode group:   COPY …→BUF[k]  (× members);  DEFLATE BUF[k]→sink
```

One worker owns one buffer and runs a group start-to-finish in listed order,
then reuses the slot for its next group. This is what makes the schedule AOT:

- **Buffer lifetime = group extent.** The planner emits INFLATE-then-scatters
  (or gathers-then-DEFLATE) contiguously; the intra-group order *is* the
  dependency, satisfied by the owning worker running sequentially. No LRU, no
  refcount, no single-flight — the step-1 frame cache disappears.
- **Buffer count = worker count** `W` (×2 if we want decode/encode-ahead
  double-buffering). The allocator is a register-allocation pass over `W` slots;
  a group waits for a free slot exactly as an instruction waits for a free
  register. Because a frame is never split across workers, no buffer is ever
  shared and no lock guards buffer contents.
- **Sizes are known**, so slots are pre-sized and groups can be bucketed to a
  few fixed capacities:
  - **nock** → frame uncompressed length from the footer (`max(in_off+size)`
    over the frame, or stored `frame_ulen`);
  - **pack / pack_fs** → `Σ (512 + padded_body)` over the frame's members;
  - **legacy monolithic `tar.zstd`** → bounded by the streaming block (§5).

Contrast the old fused `COMPRESS`: it kept the buffer microarchitectural
*because the control plane couldn't size or place it*. Once the planner sizes
and places it, explicit is strictly better — it also makes `INFLATE` and
`DEFLATE` independently schedulable (decode near storage, encode on compute)
the day we want a distributed pipeline, with the buffer as the cut point.

## 4. Two dispatch classes

The executor has exactly two data schedulers, split by whether an op touches a
buffer:

- **Bufferless** — zero-copy `COPY`, `CKSUM`, and all metadata ops
  (`MKDIR`/`UNLINK`/`RMDIR`/`SETMETA`/`FBARRIER`). Independent, dispatched
  **per row** on the ring/pool, ordered by `dep_group` epochs — the plane that
  already exists.
- **Buffered groups** — `INFLATE…COPY*` and `COPY*…DEFLATE`. Dispatched **per
  group** to a worker that owns one of the `W` buffers for the group's duration.
  This is the zpack compress-pool generalized (that pool was already
  buffer-per-worker — which is exactly why the *compress* side was never the
  problem; only the per-row `EXTRACT` dispatch was).

Unifying these two retired schedulers into one buffered-group scheduler is the
core of the de-fork (§11).

## 5. Streaming: plan a block at a time

Only **nock** is fully AOT — its footer is the whole plan, no execution
feedback. Every other source is discovered as you go, so the machine **streams**:
the planner consumes the source in **blocks**, plans each block, and feeds the
executor, overlapping planning of block *n+1* with execution of block *n*.

- **live FS** (`pack_fs`, `cp`, `rm`, `rsync`) — the scan itself can take a
  while, so block on `scan_iter` output; plan each batch of `STAT` rows as it
  arrives. (`rsync` = block the `scan × scan` diff.)
- **legacy monolithic `tar.zstd`** (`recompress`) — the one genuinely
  bidirectional case; see "Streaming recompress" below.
- **uncompressed tar** — a quick scan of the 512-byte headers up front, then
  plan.

The **only** constraint on block size is amortizing the Python↔executor call
overhead: blocks must be big enough that per-batch fixed cost is negligible, and
they need not be the whole dataset. This is the existing `(step, finish)` Plan
framework and `drive()` loop — batched and streamed execution are the same code
at different granularities; the block size is the single knob.

**Measured round-trip floor.** One command-batch → completion exchange costs
**~1.7 ms** (≈570 round-trips/s) — Arrow encode/decode over the pipe plus a
minimal op. Keeping that under 10 % of wall time (block ≳ 9·L·T) puts the useful
block at **~7 MB** (compress ~0.54 GB/s) to **~20 MB** (decode ~1.5 GB/s) — i.e.
one frame-batch, exactly the natural streaming grain. *This depends on the pool
being persistent:* spawning the worker pool per batch (the earlier design) cost
~12 ms/round-trip (≈80 rt/s) and forced ~100 MB blocks. The pool is now created
once per `exec` session and reused across every batch and epoch (one unified
pool dispatching WK_ROW / WK_DECODE / WK_ENCODE items), so the floor is the
protocol, not thread churn.

### Streaming recompress: decompress once, plan the union, keep the sinks fed

Recompressing a monolithic `tar.zstd` is the only case where the layout isn't
knowable without decompressing — so it's the one **bidirectional** flow, and the
clean form is *one pass* (not the two-pass `zscan→plan→zexec`, not the
plan-less fused `zpack` — it subsumes both):

```
readers (INFLATE + parse) ─► metadata union ─► planner (Polars: filter/assign/in_off)
                                                       │
                                                       ▼
                          frame-job queue ─► sinks (DEFLATE a live-buffer slice, append)
  ▲ bounded buffer pool = backpressure       keep this queue non-empty = full throughput
  └───────────── buffers recycled once all their frames retire ◄──────────────┘
```

Wire (its own `zstream` port — the `exec` port is unidirectional):
`C→Py ZMETA(buf_id, members)` · `Py→C PLAN(buf_id, member→frame/sink/keep)` ·
`C→Py COMP(frame→coff,clen)`. The footer is `ZMETA` (metadata) + `PLAN` (frame,
`in_off` computed in Python) + `COMP` (coff, clen) — the 60-byte record retired.

Four properties make it correct and fast:

1. **Decompress once.** A frame is a contiguous run of the original tar, so the
   sink `DEFLATE`s a slice of the *live* decompressed buffer — no re-decompress,
   no gather. Same throughput as fused `zpack`, plus the plan step it lacked.
2. **Plan the union, not one buffer.** Several readers decompress in parallel;
   the planner drains whatever metadata is available across *all* of them and
   plans it in one round-trip. This amortizes the 1.7 ms round-trip across many
   buffers and hands Polars a big batch. Frames never span a buffer (buffer
   boundary = frame boundary), so the union is only a *planning* batch — each
   frame is still assigned within its own buffer.
3. **The objective is sink non-idle.** Compression is the bottleneck
   (~25 MB/s/core); readers (~1.5 GB/s) and the planner (Polars) are far ahead,
   so full throughput ≡ the `DEFLATE` queue never empties. Everything upstream
   exists only to keep that queue backed up; a 2–3 deep buffer queue does it.
4. **Buffers are the backpressure + the recycling unit.** The fixed-size pool
   (§ below) bounds resident memory to `queue_depth × buf` and is returned for
   more decompression once a buffer's frames retire — the same `bp_*` pool.

This is `zpack`'s reader+compressor pools (already persistent, already the right
shape) with the plan stage spliced between them — the last fork closed.

### Frame buffers are fixed-size and member-aligned

Two questions the streaming encoder settles:

- **Do entries cross buffer boundaries?** No — and this is the load-bearing
  invariant. A frame's cut is only ever tested *after* a whole member has landed
  in the buffer, so **a frame is always a whole number of members** and a member
  never straddles a frame. (This is also what makes frames independently
  decodable and merge zero-copy — nock's frame-never-spans-a-member rule.) So
  "crossing" is a non-problem by construction; you never split a member, you
  choose the cut at a member boundary.
- **Fixed-size buffers?** Yes. Because frames are member-aligned and target
  `batch` bytes, the assembly buffer is a fixed quantum (`cap = batch + slack`)
  in the common case, so the executor **recycles a fixed pool** of them
  (`bp_acquire`/`bp_release`) instead of `malloc`/`free` per ~16 MB frame, and
  resident memory is bounded to `max_live × cap`. The one exception — a single
  member larger than `cap` — makes *its* frame oversized: the buffer is grown in
  place for that frame and freed on release rather than recycled. So "fixed"
  means fixed for the overwhelming common case, with an oversized member falling
  out naturally as its own solo frame. (The pool backs both the fused reader and
  the plan-driven multi-sink reader; each sink a reader fills holds one buffer.)

## 6. Ordering

Independent instructions run in parallel (max ILP). Ordering is a partial order
with two encodings today, both retained:

- `dep_group` (epochs) — a scheduling barrier between groups of rows (`rm` puts
  a directory's children in an earlier epoch than its `RMDIR`; `cp` puts
  `MKDIR` before `COPY`).
- `parent_row` (refcount) — a dependency edge; a child decrements its parent's
  count, the parent issues at zero.
- `FBARRIER` — a full fence + fsync; the footer commits after it (nock §3.3).

**Intra-group ordering needs neither** — the worker runs a group's ops in listed
order, so INFLATE-before-scatter and gather-before-DEFLATE are free. Cross-group
and metadata ordering use the epochs/refcounts above.

## 7. Static vs dynamic destinations — the linker

A sink offset is either **compile-time** (member sizes known → `pack`/`COPY`
lay out sequential offsets) or **run-time** (`DEFLATE` output length is unknown
until it runs). `ARCHIVE_APPEND` assigns the offset at retirement and reports it
in the completion. The **linker** merges both into one footer relocation table:
static offsets from the plan, dynamic offsets from `DEFLATE` completions. Each
instruction retires a completion tagged by `user_data` (completions arrive out
of order and re-associate); result fields are role-named — `out_offset`,
`out_len`, `checksum`, `status` — not the current overloaded
`read_size`/`cksum`.

## 8. Every tool and feature, recast

| feature | lowering |
|---|---|
| `scan` | generator: stream `STAT` rows |
| `du` | Polars **query** over the `scan` table — no instructions |
| `rm` | `UNLINK`+`RMDIR`, child→parent ordering (streamed over scan-close events) |
| `cp` | `MKDIR`+`COPY`(zero-copy)+`SETMETA` |
| `rsync` | reconciliation program from a streamed `scan × scan` diff |
| `pack` | `COPY FILE→ARCHIVE_APPEND` (static offsets); link the footer |
| `extract` | footer → `COPY ARCHIVE→FILE` (zero-copy) + `MKDIR` + `SETMETA` |
| **unpack** (nock) | footer → per frame: `INFLATE`, then `COPY BUF→FILE` per member |
| **pack_fs** | scan → per frame: `COPY INLINE(pax)→BUF` + `COPY FILE→BUF`, then `DEFLATE` |
| **recompress** | stream-`INFLATE` source → plan cut points → `DEFLATE` live slices |
| **reshard** | per-group `sink_id` on the `DEFLATE` (fan the sink) |
| **S3 stream** | sink mode `S3(sink_id)` → FIFO → multipart uploader |
| **merge** | zero-copy `COPY ARCHIVE→ARCHIVE` (or logical: manifest only) |
| **WAL resume** | journal `DEFLATE` completions; planner elides committed frames |
| **distribute** | shard the stream by subtree / frame-range affinity; merge results |

The PAX header lives in exactly one place — the planner's `tobuf(PAX_FORMAT)`,
carried `INLINE`. The executor has **no** tar knowledge: it moves bytes, decodes
frames, encodes buffers. That is the whole point.

## 9. Encoding: narrow the word per stream

The command word is **wide** (`CMD_SCHEMA`, 15 columns) but any op uses few. The
general exec stream is **not** opcode-homogeneous — `sync` interleaves
`MKDIR`+`COPY`+`UNLINK`+`RMDIR` by epoch, and `row_sync` dispatches per row — so
the wide word is what lets any op sit in any row; keep it for the mixed stream.
But a **buffered-group plan stream** is intrinsically a handful of opcodes
(`INFLATE`/`COPY`/`DEFLATE`) and rides its own stream with its own schema
header, so it can carry a packed schema (the head-of-stream schema message says
which encoding, like a general vs SIMD encoding on a CPU). And the whole plan
stream zstd-compresses on the wire — the constant/zero columns vanish (measured
1.7 B/row). Both compose because they are different streams.

## 10. What this replaces — the migration

The old ISA's forked artifacts (the fused `COMPRESS`, the bespoke plan file +
60-byte records, the runtime frame cache, the separate zpack/zexec/exec
schedulers) collapse into: **three data ops over planner-allocated buffers, two
dispatch classes, one command schema, one completion schema, one WAL.**

Re-sequenced de-fork (supersedes `docs/DEFORK.md`'s original order):

1. **Buffered-group scheduler in the executor** — `W` buffers, a group queue;
   a worker owns a buffer for a group's extent. Replaces the per-row `EXTRACT`
   dispatch *and* the zpack pool with one mechanism.
2. **Rework unpack onto it** — `INFLATE` + `COPY BUF→FILE` group; **delete the
   step-1 frame cache** (`g_fc`/`fc_get`/`fc_release`). Oracle: current
   `unpack(engine=None)`.
3. **pack_fs** — `COPY INLINE→BUF` + `COPY FILE→BUF` gather group + `DEFLATE`;
   PAX header inline from the planner. Oracle: current `pack_fs`.
4. **recompress** — fold `zexec`/`zpack` into a streaming (§5) `INFLATE`→plan→
   `DEFLATE`-live-slice group; retire the plan-file + 60-byte records to the
   command/completion streams.
5. **Sharded / multi-file source + merge** fall out of the `sink_id` field and
   a `shard_id → fd` table. The table is **AOT**, not a runtime cache: the
   planner knows the shard set, so the files are declared on the executor's argv
   and opened once at startup; an `INFLATE` selects its source by index
   (`shard_id` in `pad_align`), carrying no per-row path. This is the same
   AOT-over-runtime discipline as buffers — the planner knows the resource set,
   so acquire it up front, don't rediscover it behind a mutex per frame.

Acceptance: one instruction schema in, one completion schema out, one footer,
one WAL; every tool differs from the others only by op + region fields; every
existing zframe test (now an oracle) still passes byte-for-byte.

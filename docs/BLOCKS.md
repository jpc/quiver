# BLOCKS: a residence/STAT-table abstraction for qvm

## Why

qvm's ISA v2 is a pile of manual byte bookkeeping — `ALLOC`/`FREE` a buffer slot,
`MOV` bytes between typed endpoints at explicit offsets, pack `(buf,off,len)`
gather triples per frame, thread a `frame_base` counter, carry partial members
across windows, spawn one fiber per frame. Almost every correctness bug this
codebase has hit is a bookkeeping bug in that layer:

| bug (this session) | root cause (low-level bookkeeping) |
|---|---|
| null `frame_coff` (0.037% members unextractable) | sparse `frame = c // frame_bytes` ids vs. `frame_base += count` assuming dense |
| flagship phantom members (19.6k vs 18.4k) | partial-member **carry** across coalesced windows mis-resyncs |
| `-EMSGSIZE` oversize | a member larger than the fixed **window** buffer |
| `read_done` returned a tuple → truncated runs | hand-rolled DATA done-marker protocol |
| QCAP fiber-flood **segfault** on 140k-frame unpack | one fiber per frame, unbounded task submission |
| dir-entry-as-0-byte-member | members and directories not modeled uniformly |

They are all the same mistake at different addresses: the VM manipulates *buffers
and offsets*, and the planner re-derives *which bytes are where* by hand. Raise
the abstraction so the VM manipulates **files whose bytes have a residence**, and
these bug classes stop being expressible.

The pieces already exist, un-unified: `enum { E_FS, E_BUF, E_INLINE, E_ARCH }` is a
proto-residence, `MOV`'s task kinds (`TK_FS_TO_BUF`, `TK_BUF_TO_ARCH`, …) are a
proto-`COPY`, and the nock footer `_FOOTER_IPC = [path,size,mode,mtime_ns,uid,gid,
frame,frame_coff,frame_clen,in_off]` is *already* a STAT table + a compressed-MEM
locator. This design promotes them to first class.

## The model

### STAT table — the one currency

Every op consumes and/or produces an Arrow **STAT batch**: rows of
`{path, size, mode, mtime_ns, uid, gid}` plus a **locator** saying where that
file's bytes currently live. The file list, the in-flight work, and the nock
footer are all the same object at different stages. Planner logic (filter by
glob/regex, partition into frames/shards) is pure STAT-table manipulation in
Polars/numpy; the VM never re-derives byte positions — it reads the locator.

### Residence — where the bytes are

A batch is homogeneous in residence (keeps `COPY` a single kind). Three
residences, each with its locator:

- **FS** — the bytes are the file at `path`. Locator: none (path is enough).
- **ARCHIVE(fd)** — packed as one contiguous range in an *uncompressed* archive
  file (a tar). Locator: `(archive_id, offset, len)`. **No copy into memory** — a
  seekable uncompressed source is read in place on demand.
- **MEM(block)** — a contiguous range in a memory block (from decompression, a
  network stream, a raw decode). Locator: `(block_id, offset, len)`.

A fourth is really a *transform over* a residence, not a residence: a
**compressed** range (a nock frame: `frame_coff, frame_clen`) is decoded by a
codec on `COPY`, producing MEM. That is why `INFLATE`/`DEFLATE` fold into `COPY`.

### Block + page — members never split

A MEM block is a refcounted buffer, filled **whole-member at a time**: place a
member's bytes, advance; when the next member would overflow the block, close the
block (its STAT batch = the whole members it holds) and start a new one. A member
too big for the default block size gets a block sized to it. Consequences:

- **the block boundary is always a member boundary** → no partial-member carry →
  the flagship phantom-member and window-carry classes vanish;
- **no member exceeds its block** → the `-EMSGSIZE` class vanishes;
- the "page" view lets a whole-file archive map in directly: for an *uncompressed*
  tar the member ranges are already contiguous pages in the file, so they stay in
  ARCHIVE residence (mmap/read on demand) and are never copied to MEM at all.

### Buffer lifecycle — refcount by batch retirement, no FREE

A block's refcount = number of live STAT batches whose rows point into it.
Partition/filter of a batch splits the references; when a batch is fully consumed
by its downstream op and retired, it drops its references; at refcount 0 the block
frees. There is **no `OP_FREE`, no `buf_id` ring, no `npool % buf`**. Bounded
memory is a **block budget** in the scheduler: a producer (`DECODE`/`COPY→MEM`)
blocks when total live block bytes exceed the budget and resumes as downstream
retirement frees blocks — the windowing we hand-code today, now automatic and
correct by construction.

## The reduced ISA

Two verbs plus a thin fs-structure set, replacing ~22 opcodes:

- **`SCAN(residence, source) → STAT stream`** — enumerate a residence into STAT
  batches, streaming (emit as discovered, for pipelining):
  - `SCAN(FS, root)` = parallel walk (today's `SCANDIR`).
  - `SCAN(ARCHIVE, tar_fd)` = parse tar headers → member ranges (today's `TARSCAN`).
  - `SCAN(NOCK, path)` = read the footer (it already *is* a STAT table).
- **`DECODE(compressed_source) → STAT stream over MEM`** — inflate a stream
  (`.tar.zstd`/gzip, or one nock frame) into page-aligned MEM blocks and parse
  members. This is `SCAN`-over-compressed = today's `SRC_SCAN`, minus the carry.
- **`COPY(STAT batch, dst_residence [, codec]) → relocated STAT [+ completions]`**
  — move each file's bytes from its current residence to the destination; the
  optional codec is the byte transform. One op subsumes:
  - `COPY(FS→MEM)` read files into a block (pack read)
  - `COPY(MEM→FS)` scatter members to files (unpack)
  - `COPY(FS→ARCHIVE)` synthesize tar header+body (pack)
  - `COPY(MEM→SINK, zstd)` compress a batch into a frame, emit `(coff,clen)` (deflate)
  - `COPY(NOCK→MEM, inflate)` decode a frame (unpack decode)
  - `COPY(FS→FS)` `copy_file_range` (cp)
- **fs-structure** — `MKDIR`, `SETMETA`, `UNLINK`, `RMDIR` over STAT rows (the
  directory rows of a STAT batch). `COPY(→FS)` materializes the tree from them.

`SPAWN`/`JOIN`/`CALL`/`ALLOC`/`FREE`/`SINK_OPEN`/`SINK_CLOSE` all disappear:
parallelism is *implicit* (a `COPY` is data-parallel over its batch, bounded by
the scheduler), a sink is just a residence target, and blocks self-free.

## Pipelines, rewritten

```
pack        SCAN(FS) → partition→frames → COPY(→SINK, tar+zstd) → footer=STAT⋈completions
recompress  DECODE(.tar.zstd→MEM) → [filter STAT] → [repartition] → COPY(→SINK, zstd) → footer
unpack      SCAN(NOCK) → group by frame → COPY(NOCK→MEM, inflate) → COPY(→FS)
shard       DECODE → partition STAT by shard_key → COPY(group→shard_sink, zstd)   (sink = residence)
cp          SCAN(FS) → COPY(→FS)
distributed partition the STAT (by frame) across nodes → each node runs its COPYs   (frames independent)
```

The footer is a serialized STAT table with NOCK-frame locators; reading a nock is
`SCAN(NOCK)`; writing one is "serialize the final STAT." The format collapses to
*compressed byte ranges + a STAT table over them*. The per-frame distributed
unpack we just measured (4.65× on 4 nodes) is the generic "partition the STAT,
run COPYs" — no special path.

## What each bug becomes

- sparse frame-id / null_coff — **gone**: a frame is a STAT partition; its id is
  assigned once and its completion keyed by the partition. No `frame_base`.
- flagship phantom members / carry — **gone**: page-aligned blocks, member = page,
  no cross-block carry.
- `-EMSGSIZE` — **gone**: a block is sized to hold its largest member.
- `read_done` protocol — **gone**: the wire is STAT-batch streaming, not a
  hand-rolled done marker.
- QCAP segfault — **gone**: `COPY` submits bounded batch-parallel work; there is no
  one-fiber-per-member explosion. Block budget + bounded fan-out replace it.
- dir-as-member — **gone**: dirs are STAT rows with an FS-structure locator,
  materialized by `COPY(→FS)`; never a 0-byte data member.

## Planner simplification

The Python side becomes pure STAT algebra: `SCAN → filter (polars glob/regex on
path) → partition (numpy cumsum → frame/shard ids) → emit COPYs`. The
`_np_gather` kernel and its `(buf,off,len)` triples are **deleted** — the STAT
locator *is* the gather; the VM reads it. `frame_base` threading is **deleted** —
ids are assigned once at partition. This is exactly the "Polars-filter +
fixed-numpy-compile" split from earlier, now the native contract.

## Migration

1. Land the residence type + locator in the STAT/footer schema (footer already
   carries it; formalize `res` + `loc`).
2. Implement refcounted blocks + the block budget in the scheduler; keep `ALLOC`/
   `FREE` working as a compatibility path.
3. Implement `COPY(residence_pair [,codec])` as one op dispatching to the existing
   `TK_*` task kinds; port `INFLATE`/`DEFLATE`/`MOV` callers onto it.
4. Implement `DECODE` with page/member-aligned blocks (replaces `SRC_SCAN` carry);
   validate recompress byte-exact — this alone kills the phantom-member and
   `-EMSGSIZE` bugs.
5. Fold `SCANDIR`/`TARSCAN`/footer-read into `SCAN`.
6. Delete `_np_gather`, `frame_base`, `SINK_OPEN/CLOSE`, `SPAWN/JOIN` from the
   planner; delete the dead opcodes from the VM.

## Decisions (locked)

1. **The frame is the universal unit.** A block == a compression frame == a group
   of **whole members**. Legacy archives (uncompressed tar, fs pack, streams) get
   **whole-member frames synthesized on the fly** as they are read — same code path
   as a nock frame. Oversize member → its own frame.
2. **Nock is lazy.** `SCAN(NOCK)` returns the STAT table from the footer and decodes
   **nothing**. A frame is inflated only when a `COPY` request references members in
   it (decode-on-demand); the frame is the decode group its STAT sub-batch aligns to.
3. **This is a rewrite** — a clean new executor core around STAT/residence/frame/
   COPY, not a retrofit of the 22 opcodes. (Reference core in Python first as the
   executable spec + correctness proof on the large-member source that broke the old
   paths; the C port follows the same contract.)
4. **Simplified WAL on top.** Not baked into the core: a thin layer that, on each
   `COPY(→SINK)` frame commit, appends `(frame_id, coff, clen, digest)` + the frame's
   STAT partition to a WAL; resume replays the WAL → skip committed frames, truncate
   the sink to the last durable offset, continue. A committed frame is just a retired
   STAT partition with a durable locator.

### Still to pin during the build
- Refcount across split/merge of STAT batches (batch→block graph must be exact).
- Sink offset assignment for `COPY(→SINK)` — keep the reserve-cursor-off-lock design.

## Streaming planning + the WAL-as-STAT (dedup / change-detection)

Two properties, both because STAT is the currency and both composing into an
incremental, content-addressed archiver:

**Streaming planning.** The planner PUSHES STAT/COPY batches on the VM's stdin
incrementally (the push model), rather than handing over one job blob. `SCAN`
(fs walk / archive decode / net stream) emits STAT batches as it discovers rows;
the planner diffs + partitions each batch and pushes its COPYs; the VM executes
them as they arrive; completions stream back. So SCAN ‖ plan ‖ compress ‖ write
overlap and the full STAT table is never materialized — you work on data as it
streams in. (bvm moves from a job-file to a framed stdin reader; blocks.py's
`SCAN` generators already model it.)

**The WAL is a durable STAT table.** On each `COPY(→SINK)` commit, append the
frame's STAT partition — `{path, size, mtime_ns, digest, frame, coff, clen}` — to
the WAL. It is not a bespoke log; it is the same STAT object, persisted. That one
fact gives three things at once:

- **Resume** (original goal): committed frames are STAT partitions with durable
  locators; skip them, truncate the sink to the last durable offset, continue.
- **Change-detection / incremental** — re-`SCAN` the source and JOIN the new
  scan-STAT against the WAL on `path`:
  - present + `(size,mtime)` unchanged → **UNCHANGED**: reuse the WAL locator, never
    read or compress the file;
  - present + differ → **CHANGED**: re-read + re-frame;
  - scan-only → **ADDED**; WAL-only → **DELETED**.
  Only CHANGED+ADDED bytes are read and compressed — rsync/backup semantics, as a
  STAT join.
- **Dedup (content-addressed)** — carry a per-member content `digest`; JOIN
  CHANGED+ADDED against the WAL's `digest→locator` index: a match (a renamed or
  duplicated file, or an unchanged body a cheap stat missed) **reuses the existing
  compressed bytes** — the content is stored once. New digests get packed into new
  frames and their `digest→locator` recorded.

The output footer is the merged STAT (UNCHANGED+dedup reused ⊕ CHANGED+ADDED new);
DELETED rows drop. The whole thing streams: `scan → diff(WAL) → COPY(delta) →
append(WAL)`, overlapped. The WAL thus doubles as the manifest of "what content
exists and where," i.e. the seed of a content-addressed store shared across runs.

### The C↔Py contract: C streams STAT up, Py streams the plan down

The division of labor is fixed: **C is the sensor + actuator, Py is the brain.** C
does the fast, parallel, I/O-bound work — SCAN a filesystem, DECODE an archive,
execute COPY — and it is C that *discovers* members, so C STREAMS the STAT up to Py
incrementally. Py holds the policy — glob/regex filter, framing, shard routing,
WAL-diff/dedup — and STREAMS the plan (COPY ops referencing that STAT) back down.
C executes and streams the result STAT (frame locators) up; Py writes the footer +
WAL. Both directions flow batch-by-batch so scan ‖ plan ‖ copy overlap and nothing
is materialized whole. Wire, full-duplex over the bvm pipe:

```
C -> Py (stdout):  STAT(rows)          members C discovered (scan/decode), + source locator
                   SCAN_EOF            source exhausted
                   DONE(frame,coff,clen,digest)   a COPY(->SINK) completion
Py -> C (stdin):   COPY_FRAME(frame,level,members)  compress/scatter these; C assigns offsets
                   PLAN_EOF            no more plans
```

So `bvm bpack <root> <out>` walks the tree and streams STAT; Py WAL-diffs + frames
the delta and sends COPY_FRAMEs; C reads + compresses + streams DONE; Py assembles
the footer (reused locators ⊕ new) and appends the WAL. Unpack is the mirror
(C SCANs the nock footer and streams it; Py partitions frames and sends COPY→FS) —
the current unpack has Py reading the footer, which is a shortcut to fix once bscan
handles the footer too.

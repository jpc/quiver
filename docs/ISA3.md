# ISA v3 — fibers over dynamic dataflow values

> Design. Merges bvm's coarse wire ops (the shipped BLOCKS executor) with qvm's
> fiber scheduler (ISA2.md, `git show 07c6157`), replacing the one part of v2
> that failed in production: static, planner-owned buffer management. ISA2.md
> stays as the record of the fiber model; MACHINE.md's VM framing still applies.

## 0. Post-mortem: what static memory cost us

v2 put memory in the instruction set: `alloc buf, cap` / `free buf`, where
`buf_id == physical slot — the planner draws ids from a ring of pool size`
(qvm.c:668) and `cap` is computed at plan time. Every fact the planner could
not know at compile time became an edge case with its own machinery:

- **Member larger than the window** → `oversized ⟹ -EMSGSIZE` (qvm.c:546) — a
  hard runtime error the *harness* worked around by retrying whole sources up a
  `WIN_LADDER = [128 MB, 512 MB, 2 GB]`. Retries left frame-id gaps; the sparse
  ids produced the `null_coff` footer corruption. One static cap, three layers
  of scar tissue.
- **bvm inherited the disease in softer form**: `blk_add` needs a special
  sole-runner rule so an oversize member with an empty pool doesn't deadlock;
  J_PACK reserves `est*2 + 16 MB` and a giant member that fails to buffer once
  produced a 9-byte empty frame claiming 55.7 TB (the verify structural pass
  exists because of it); the planner must pre-split big members into
  `frame_cap` EXTENT pieces *for RAM safety*, entangling a layout policy with
  an allocator concern.
- The pattern generalizes: this codebase's worst bugs (fd-exhaustion subtree
  loss, DONE-flush locator drop, pwrite 2 GiB truncation, null_coff) are all
  static bookkeeping meeting a runtime scale it didn't anticipate.

v3 keeps everything else from v2 — fibers, spawn/join structured concurrency,
the completion scheduler (io_uring + compute pool via eventfd), the
sink-as-async-mutex — and removes `alloc`/`free`/`buf_id`/`cap` from the ISA
entirely. Memory becomes what the data actually is: a runtime-managed value.

## 1. The dataflow model

**Values.** A `val` is an anonymous, immutable-once-written byte sequence
produced by exactly one instruction and consumed by one or more. Instructions
name values by small per-fiber registers (SSA-ish: `v0, v1, …` local to the
tid), never by pool slot. The runtime owns placement, growth, and lifetime.

**Representation: chunk list, not flat buffer.** A val is a sequence of fixed
chunks (default 4 MB). The producer appends; capacity is *discovered*, never
declared. `realloc`-into-place and "member > window" cease to exist — the
qvm.c:546 oversize flag and the WIN_LADDER are deleted concepts.

**Two consumption modes**, chosen per edge by the expander:

- `RANDOM` — consumers start after the producer finishes; arbitrary reads
  (`val@off±len`). This is v2's join-bounded window: gather-deflate over member
  runs, scatter out of an inflated frame.
- `STREAM` — consumers chase the producer with a bounded lead (a chunk-queue
  window); chunks release as the *last* consumer passes them. A 137 GB member
  flows through a fixed footprint. This subsumes the giant-member problem
  structurally: EXTENT splitting remains as a *layout policy* (random access,
  parallel unpack), but RAM safety no longer depends on the planner doing it.

**Admission: one global byte budget.** Chunk appends charge the budget; a
producer blocks when over. The sole-runner rule is core, not a patch: a fiber
that holds no chunks while nothing else is live may always proceed — a single
value larger than the whole budget degrades to STREAM pacing instead of
deadlocking or erroring. (This is bvm's `blk_add` wait-condition, promoted from
comment to invariant.)

**Lifetime = dataflow, bracketed by scope.** A val dies when its last consumer
retires, and at scope exit (`join`) regardless — the v2 insight ("the join is
the refcount") kept, with the runtime counting consumers *inside* the scope so
fan-out reads (one decode window feeding N deflates) need no SEAL, no refcount
in the ISA, and no planner-visible free.

**Codecs are val properties, not instructions.** `v = new val CODEC(zstd,lvl)`
compresses every byte `mov`'d in — the val's content IS the frame, closed when its
last producer retires; `CODEC(unzstd)` decodes symmetrically. A fiber's in-order
`mov`s into a codec val give the sequential feed a zstd stream needs (compression
of one frame was never parallel — it runs on one compute worker exactly as an
instruction-form deflate would). The zero-copy gather of ISA2 §5.5 survives as
`mov w@run → z` sequences; `deflate`/`inflate` leave the ISA entirely.

**Digest rides the mov.** The `DIGEST(blake3|cdc)` flag hashes bytes in passing —
bvm already hashes while reading (pack), so the flag form is the honest one. A
"pure digest" is `mov src → null DIGEST` (MANIFEST is exactly this); no standalone
digest op exists.

**Zeroing is an op property, not an alloc flag** (pack's tar padding zeroes at
fill time on the worker, first-touch faulting off the scheduler thread —
qvm.c:813's lesson carries over).

## 2. Instructions

Leaf ops (a fiber executes its list in order, suspending on each completion):

Ten ops. Codecs and hashing are val/edge properties (§1), not instructions.

| instr | backend | notes |
|---|---|---|
| `mov src → dst` | ring / compute | endpoints: `fs:path`, `arch:fd@off±len`, `val@off±len`, `inline:bytes`, `sink:N`, `pipe:N`, `null`. `fs→fs` = copy_file_range. Into a `CODEC` val → runs on a compute worker. Flags: `DIGEST(blake3|cdc)` hash-in-passing, `TRUNC(fsize)`. |
| `mkdir p` `symlink l→p` `link t→p` `setmeta p` `unlink p` `rmdir p` | ring/wq | today's META op types + UNLINK, one row each; mkparents-on-ENOENT semantics |
| `reserve sink, n → off` | — | the async-mutex sink of ISA2 §2 (FILE releases before positioned writes; PIPE holds through) |
| `emit stream, bytes` | — | STAT / ZMETA / DONE / control, VM → planner/footer |
| `spawn lo,hi` / `join lo,hi` | control | structured concurrency; ranges of tids |
| `fence` | control | drain the scope's outstanding namespace ops; kept for wire-level phase cuts |

**There is no `recv`.** All planner → VM flow is an asynchronous batch-spawn into
an open scope (below); the only synchronization a fiber ever does is `join`.

**Planner → VM is always an async stream.** A scope may be *subscribed* to a wire
stream: each arriving batch expands (in C) to child fibers spawned into it; the
scope joins when the stream closes and the children drain. The recompress
feedback loop loses its synchronous `recv`: the decode fiber emits ZMETA for
window k and immediately starts window k+1 — when k's plan arrives it spawns
deflate children against k's val, which dies at their join. No new throttle is
needed: **the byte budget self-clocks decode-ahead** (a window that can't admit
chunks parks its producer), which is exactly the backpressure a blocking recv
was hand-implementing.

**Wire form: bvm's, unchanged.** The planner keeps sending coarse Arrow-batch
verbs — SCAN_*, OPEN_TAR, PACK_FILES df, SCATTER df, META df, UNLINK, MANIFEST.
Each verb is a **macro**: a C expander unfolds the batch into a fiber tree
(exactly ISA2 §5.5's "the C reader expands it into the fiber tree", generalized
to every op). Amortization stays (one message ≈ 1M rows); the planner API and
`_Bvm` do not change on day one; raw fiber programs are a later, additive wire
op for shapes the macros don't cover (rsync pipelines first).

## 3. Sinks and the footer

Unchanged from ISA2 §2/§6 plus what this week settled: frames land in
completion order at `reserve`d offsets; `emit(DONE)` fires at reservation; the
footer fiber joins plan rows with DONE completions and writes **sidecar
footers for any sharded store** (`out.footer`, offset 0, global frame ids,
`shard` column — no per-shard dense renumbering, no shard that is both data
and index), inline footers for single-file nocks. `_pwrite_all` discipline is
the sink's, not the caller's.

## 4. Every existing operation as a v3 program

Notation as ISA2 §4. `F` = frame group, `m` = member. Each entry names the bvm
mechanism it replaces and the edge case that dissolves.

### 4.1 scan_fs / scan_names / scan_dirs
Unchanged as a generator service emitting STAT/BSTAT batches — it is the
best-measured part of the engine (8% faster than pwalk2 at 61% RAM) and already
internally fiber-shaped (walker pool, per-walker rings, EMFILE re-queue).
v3 treats it as a `spawn`-fan producing `emit` rows; no rewrite, just the same
scheduler underneath eventually.

### 4.2 pack (pack_fs_c / backup, whole members)
```
scope ROOT (over scan stream, planner cuts frames):
  spawn per frame F {m0..mk}:
    v = new val                            ; RANDOM (assembly window)
    spawn per m in F:                      ; IO fan: today's fd-cache/ring prefetch
      mov inline:hdr(m) → v@ho(m)
      mov fs:m.path     → v@do(m)  DIGEST  ; hash-while-reading (delta manifests)
    join
    c = new val CODEC(zstd,lvl)
    mov v → c                              ; the frame compresses as it streams in
    off = reserve(sink:F.sink, len(c)); mov c → arch:sink@off; emit DONE
  join
  finish: mov inline:ZEROS → sink; footer fiber
```
Replaces J_PACK_FILES. The `est*2 + 16 MB` reservation guess and the
frame-lost/zero-fill discipline become chunk-budget admission; padding zeroes
at fill.

### 4.3 large members (today: planner frame_cap EXTENT pieces)
```
    v = new val STREAM
    mov fs:m.path → v DIGEST     ; producer (whole-file hash in passing)
    spawn per piece p (layout policy, unchanged):
      c = new val CODEC(zstd,lvl)
      mov v@p.run → c            ; consumers chase; chunks release behind them
      off = reserve(...); mov c → arch@off; emit DONE
    join
```
EXTENT rows stay (random access wants pieces); the *RAM* coupling is gone. The
empty-frame-claiming-55.7 TB failure mode cannot be expressed: there is no
single giant materialization to fail.

### 4.4 delta backup (MANIFEST + literals)
MANIFEST = `spawn per path: mov fs:path → null DIGEST(cdc)` emitting manifest
rows — today's J_MANIFEST, a flagged mov. Delta literals are 4.2 with `v` filled from
changed ranges only; carried rows never touch the VM (footer-only, as today).

### 4.5 recompress (OPEN_TAR + COPY_BLOCK / COPY_MEMBERS)
```
scope per source (streaming, subscribed to the plan stream):
  loop windows k:
    wₖ = new val CODEC(unzstd)             ; decode window — GROWS to member
    mov arch:src → wₖ                      ;   alignment; carry is a val view;
    emit ZMETA(members(wₖ))                ;   "member > window" cannot happen
    ; NO recv — decode of window k+1 starts now; budget self-clocks the lead
  on plan batch for window k (async):
    spawn per frame F:
      c = new val CODEC(zstd,lvl)
      mov wₖ@F.span → c                            ; COPY_BLOCK: one run
      | mov inline:hdr ⊕ wₖ@run → c per member     ; COPY_MEMBERS: gather
      off = reserve(sink:F.sink, len(c)); mov c → arch@off; emit DONE
    join → wₖ dies
```
J_COPY_BLOCK and J_COPY_MEMBERS collapse into one program differing in one
gather list — the review's riskiest duplication, dissolved rather than
refactored. The block ownership machinery (`blk_ref/retire`, owner-refs,
`g_fatal` on use-after-retire) is replaced by scope lifetime.

### 4.6 unpack (SCATTER, incl. EXTENT stitch and stored frames)
```
scope ROOT (over footer stream):
  spawn per frame F @ (shard,coff,clen):
    c = new val; mov arch:shard@coff±clen → c
    w = inflate c                          ; stored frames: skip inflate,
    spawn per piece m in F:                ;   or fs→fs copy_file_range path
      mov w@in_off(m)±len → fs:m.dst@out_off(m) TRUNC(fsize) ; mkparents on ENOENT
      setmeta m.dst                        ; file meta, as today
    join
  join
  finish scope:                            ; ORDER BY STRUCTURE, not sessions:
    spawn per hardlink: link t→p           ;   after all file fibers joined
    join
    spawn per dir: setmeta d               ;   restrictive modes last
    join
```
Replaces J_SCATTER **and the three-session fence dance** (files → hardlinks →
dir meta became three `_Bvm` processes this week; here it is one program's
finish scope). Dirs/symlinks interleave with file fibers exactly as the new
META op does; the 4-node rank-0 long pole (17 min of serial Python) is a spawn
fan.

### 4.7 rm
```
scope ROOT: spawn per file: unlink p; join
  finish: spawn per dir (deepest-first waves): rmdir p; join   ; ENOTEMPTY retry stays
```
Today's UNLINK op + Python depth batching; the depth ladder becomes nested
scopes. io_uring batching unchanged underneath.

### 4.8 rsync (tx/rx) — the shape that justifies fibers
Per-partition pipelines with real dependency structure:
```
scope per partition P:
  spawn scan(P.src) ‖ scan(P.dst)          ; two generators
  join → diff rows
  spawn per changed m: (4.2 body) → pipe:data   ; frames to the socket
  join; emit partition-done → ctrl
```
Receiver: today's rx_reader/rx_apply_meta become the mirror program over
`pipe:` sources — same leaf ops, no bespoke receiver code path.
`diff_stream`'s partition bookkeeping moves from Python into scopes.

### 4.9 push / pull (object store)
`pipe:` sinks with multipart semantics (ISA2 §5.7): reserve→write holds the
pipe lock through the serial write; segments are vals; `_pwrite_all`-class
bugs are impossible at the ISA level (the sink loops).

### 4.10 verify
Today planner-side; optionally a program (`spawn per frame: mov arch@coff±18 →
val; digest sampled frames`) — worthwhile only if the structural pass's 36 min
single-thread wall matters; it parallelizes trivially here.

## 5. What v3 does not change (measured, this week)

- WEKA metadata RPCs per **client instance** are the ceiling (`rm procs=4` =
  4 mounts, not 4x threads); fibers do not multiply clients.
- A single-frame zstd source decodes on one core regardless of scheduler
  (714.8 GB monster: 22 min, hidden inside an 80 min pack).
- io_uring has no chmod/chown/utimensat — `setmeta` still runs on blocking
  threads (io-wq or pool).
- Single-inode write bandwidth, node skew (12.4 vs 18.4 min on identical
  work), and the scan core's numbers: all untouched.

v3's yield is structural: the five bug classes above become inexpressible, the
executor's job-kind monoliths become ~10 leaf ops + expanders, and the rsync
direction gets a native execution model instead of Python orchestration.

## 6. Migration

1. **FENCE in bvm now** (~30 lines): kills the unpack session dance without
   any scheduler work; also the acceptance test for scope semantics.
2. **Scheduler core under the existing jobs**: J_* become single-instruction
   fibers over the completion scheduler (qvm.c's Thread/ready-queue/eventfd
   core, ~400 lines, revived from `git show fb9231e^:quiver/exec/qvm.c`);
   behavior identical, jobs just get tids.
3. **Expanders, one wire op at a time**, each gated byte-exact against the
   current engine on the test suite + an EVI-subset round-trip (the
   reference-core method that validated BLOCKS itself). Order: META (simplest,
   newest), SCATTER (kills the sessions), PACK/COPY_MEMBERS (kills the
   duplication), OPEN_TAR windows last (kills the block store).
4. **Delete the J_* monoliths and the block ownership machinery** once every
   verb expands.
5. Raw fiber-program wire op + the rsync pipeline as its first user.

Each phase keeps the planner API frozen; nothing on disk changes format
(footers already settled sidecar-for-sharded this week).

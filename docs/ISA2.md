# quiver executor ISA v2 — fibers over a completion scheduler

> Design proposal. Supersedes the FILL/EMIT draft and the group/epoch model in
> ISA.md once built. ISA.md still documents the *current* executor.

## 1. Execution model

A **completion-driven scheduler** over two backends:

- **io_uring** — FS/IO ops (`mov`, namespace ops). Cheap ops complete inline;
  blocking ones offload to `io-wq`. ~0 initiator CPU.
- **compute pool** (~`ncores` threads) — `inflate`/`deflate`. One core each.
  Workers signal completion through an `eventfd` registered in the ring, so a
  single `io_uring_wait_cqe` loop reaps IO and codec completions alike.

A codec is not a special instruction — it's a long-running async op that runs on
a core instead of the device. The scheduling core is free during it, exactly as
during IO. The only asymmetry is capacity: ring depth (large) vs core count
(small); codec ops additionally gate on holding a buffer.

**Fibers (pseudo-threads).** Every instruction carries a `tid`. Same `tid` runs
sequentially (issue order = execution order); different `tid`s run in parallel,
bounded by resource caps. A fiber **suspends on each op's completion without
holding a core**, so thousands are cheap — only ops *in flight* consume
resources.

**Threads are numbered; only thread 0 starts.** Every instruction carries a
`tid`; a thread's program is its instructions in order. At launch **only thread
0 is runnable** — every other thread is inert until spawned. `spawn lo, hi`
activates a **contiguous range** of thread ids (compact: two integers spawn a
whole fan-out); `join lo, hi` waits for that range. Ranges are natural because
the planner numbers work items (frames, members) sequentially.

**Structured concurrency: `spawn`/`join`.** A fiber opens a scope, spawns a
range of child threads, joins, continues. This one primitive subsumes:

- **dependencies** — fork is the parent→child edge, join the children→continuation edge. The fiber tree *is* the DAG.
- **fan-out / scatter** — spawn N children, each a plain 1:1 op.
- **buffer lifetime** — `alloc` before `spawn`, `free` after `join`. The join is the "refcount", but lexically scoped and runtime-counted, not a field on the buffer. **No refcount, no `SEAL`.**
- **the namespace ladder** — sequential `spawn`/`join` phases; but streaming (§6), `mkdir -p` inlines per data fiber (dedup, `EEXIST` benign) and `rmdir` is a finish scope. The old `dep_group` epochs are the batch-mode shadow of this.

**Backpressure.** `alloc` blocks when the buffer pool has no free slot. Spawned
children are admitted by capacity (pool slots / compute cores / ring depth). A
parent that spawns 10⁴ children doesn't blow up — they queue.

## 2. Endpoints & the sink

`mov` moves bytes between endpoints:

- `fs:path` — a filesystem file (whole `[0,size]`).
- `arch:@off±len` — a byte range in an archive fd.
- `buf:id@off±len` — a pooled buffer.
- `inline:<bytes>` — a planner-supplied constant (e.g. a PAX header).
- `sink:N` / `pipe:N` — an append target.

`fs → fs` with no buffer specialises to **`copy_file_range`** (reflink / server
offload where the mount supports it; ring fallback on `EXDEV`).

**A sink is an unordered async-mutex-guarded cursor** — *not* an actor fiber.
The fiber holding a compressed frame (or packing an uncompressed member) does:

```
acquire(sink)                              ; async mutex — parks the fiber, not a core
region = cursor; cursor += N               ; reserve N bytes (one frame's clen, or hdr+body)
emit(footer, {tag, coff=region, clen=N})   ; completion known the instant we reserve
FILE:  release(sink); <positioned writes into region>   ; writes concurrent, off-lock
PIPE:  <serial write of region>; release(sink)          ; no pwrite → hold through write
```

- **Order is not required.** Tar is order-agnostic and every frame decodes to
  whole records, so any frame order is a valid tar; the footer maps
  `frame→coff` / `member→(frame,in_off)` for random access regardless. Frames
  land in completion order. *Opt-in* frame ordering (a turnstile on `next_expected`)
  exists only for a strict sequential consumer or whole-stream byte-identity.
- **Header sticks to data** for free: compressed → the PAX header is `mov`'d into
  the assembly buffer adjacent to the body and they `deflate` into one
  inseparable frame; uncompressed → the reservation covers the whole
  `[512-aligned header + padded body]` region, then two positioned writes.
- **Per-sink lock** ⇒ N sinks = N concurrent streams; only writers to the *same*
  target serialise (inherent for a pipe).

## 3. Instruction set

| instr | backend | notes |
|---|---|---|
| `alloc buf, cap` / `free buf` | pool | `alloc` is the backpressure point |
| `mov src → dst` | io_uring | `fs`/`buf`/`pipe`/`inline` → `fs`/`buf`/`pipe`; `fs→fs` = `copy_file_range` |
| `inflate src → buf` | compute | decompress a frame |
| `deflate src[runs] → dst` | compute | compress; `dst` a `buf` (then `mov` to sink) |
| `mkdir` `unlink` `rmdir` `setmeta` `fbarrier` | io_uring | namespace / durability |
| `spawn { … }` / `join` | control | structured concurrency |
| `cksum` | either | standalone, or a `DIGEST` flag on `mov`/`inflate`/`deflate` (hash-while-moving) |

Wire row: `tid`, `opcode`, `flags` (`SRC`·`DST`·`CODEC`·`DIGEST`·`TRUNC`),
`buf_id`, `src_id` (sink/source fd), `path`, `off`, `len`, `mode/mtime/uid/gid`,
`payload` (inline bytes **or** packed `(off,len)` runs). The recompress feedback
mode keeps its compact 2-column `(frame,sink)` plan (0.2 ms/window); the C reader
expands it into the fiber tree in §5.5.

## 4. Notation for the streams

`scope`/indentation = a fiber scope; `spawn Xᵢ:` opens a child fiber per item and
`join` waits for all; leaf ops as in §3; `;` comments. `hdr(m)` = the PAX header
bytes, `ho/do/in_off` = header/body offsets.

## 5. Example instruction streams

### 5.1 cp  (fs → fs, no buffers — the `copy_file_range` fast path)

```
scope ROOT  (nursery over the scan stream):
  spawn per file f:
    mkdir -p parents(f.dst)              ; dedup via shared set, EEXIST ok
    mov  fs:f.src → fs:f.dst             ; copy_file_range — no buffer touched
    setmeta f.dst {mode, mtime}
  join
  finish: setmeta dirs (deepest-first)   ; sync also: unlink/rmdir tail
```

### 5.2 pack  (fs → compressed nock)

```
scope ROOT  (over scan stream, members grouped into frames by raw footprint):
  spawn per frame F {m₀..mₖ}:
    alloc win, cap(F)
    spawn per member m in F:             ; concurrent header+body fills (IO fan)
      mov inline:hdr(m) → buf:win@ho(m)  ; header … (sticks to body: same buffer)
      mov fs:m.path     → buf:win@do(m)  ; … body, adjacent
    join                                 ; window assembled
    deflate buf:win[0..end] → cbuf       ; compute: one frame
    mov cbuf → sink:0                     ; reserve coff, write, emit {F→coff,clen}, free cbuf
    free win
  join
  finish:
    mov inline:ZEROS(2×512) → sink:0      ; tar EOF, physically last
    <footer fiber> writes member⋈frame index → sink:0   ; streamed + skippable trailer
```

### 5.3 pack uncompressed  (fs → uncompressed tar nock — zero-CPU `clone`)

```
scope ROOT  (over scan stream; for already-compressed payloads — audio/video):
  spawn per member m:
    region = reserve(sink:0, 512·⌈hlen⌉ + pad512(m.size))   ; ONE cursor bump (lock)
    mov inline:hdr(m) → arch:sink@region.hdr                ; positioned pwrite
    mov fs:m.path     → arch:sink@region.body               ; copy_file_range — no buffer!
    emit footer {m → data_offset=region.body, size}
  join
  finish: ZEROS; footer                                     ; random-access index, no frames
```

### 5.4 unpack  (nock → fs)

```
scope ROOT  (nursery over the FOOTER stream, frame-grouped, batch by batch):
  spawn per frame F {m₀..mₖ} @ (coff,clen):
    alloc win, dlen(F)
    mov arch:@coff±clen → cbuf                ; read the compressed frame
    inflate cbuf → win ;  free cbuf
    spawn per member m in F:                  ; concurrent scatter (IO fan-out)
      mkdir -p parents(m.dst)
      mov buf:win@in_off(m)±size → fs:m.dst   ; scatter one member
      setmeta m.dst {mode, mtime}
    join                                      ; all members written
    free win                                  ; ← the old refcount, now a join
  join
  finish: dir mtimes (deepest-first)
```

### 5.5 recompress  (foreign tar.zstd → nock — the streaming-feedback mode)

The one mode whose members are discovered by decoding, so it keeps the compact
`(frame,sink)` plan. The reader expands it into this tree per window:

```
scope ROOT  (per source, streaming):
  loop windows:
    alloc win, window_cap
    mov arch:src-stream → cbuf ; inflate cbuf → win   ; decode ONCE; discover members,
                                                       ; record buf_spans (implicit fill)
    emit  ZMETA(window members) → planner              ; feedback  C → planner
    recv  PLAN(member → frame,sink)                     ; planner → C  (compact 2-col)
    spawn per frame F in window:                        ; F parallel compressions…
      deflate buf:win[runs(F)] → cbuf                   ; …zero-copy gather from the SHARED win
      mov cbuf → sink:F.sink                            ; reserve, write, {F→coff,clen}, free cbuf
    join                                                ; all F done ⇒ win drainable
    free win
  finish: ZEROS; footer (member ⋈ frame)
```

`win` is one buffer read concurrently by F `deflate` children (the case that used
to need a refcount) and freed at the join — no refcount, no `SEAL`.

### 5.6 reshard  (→ N sinks) — delta on 5.2/5.5

The plan assigns each frame a `sink ∈ 0..N-1`; the only change is
`mov cbuf → sink:F.sink`. N sinks = N locks = N independent nock archives, each
with its own footer written in `finish`. Nothing else moves.

### 5.7 S3 / network  (→ pipe sinks) — delta on 5.6

`sink:F.sink` is a `pipe:` (socket / S3 multipart). `mov cbuf → pipe:sink` holds
the lock **through** the serial write (no pwrite). The footer is appended at
stream end (S3 object) or sent as a trailer / sidecar. Fan-out across N pipes =
N saturated connections; only same-pipe writers serialise.

## 6. Streaming, footer, and what's shared

- **Everything streams.** A top-level nursery consumes the plan stream (statx /
  footer batches / ZMETA) and spawns per-frame fibers as batches arrive, joining
  at end-of-stream. Wall-clock is `max(scan, move)`. The footer is itself read
  batch-by-batch (skippable-frame chunks, headers skipped transparently) so a
  multi-GB index never materialises; it must be **frame-primary ordered** to keep
  decode monotone (which is the order pack/recompress emit).
- **The footer is a sink.** A footer fiber joins plan-side member rows with the
  `{frame→coff,clen}` completions from the sinks and appends index batches, then
  writes the skippable-frame trailer + magic in the finish scope.
- **One engine.** cp, pack, pack-uncompressed, unpack, recompress, reshard, and
  S3/network are all fiber shapes over the same ~10 leaf ops + `spawn`/`join`.
  `copy_file_range` (5.1, 5.3) is the only path that skips the buffer pool; every
  other mode is `alloc → fill → codec → drain → free`, bracketed by a scope.
```

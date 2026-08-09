# qvm2 — the fiber-VM architecture

> How the shipped executor (`quiver/exec/qvm2.c`) and its planner
> (`quiver/qplan2.py`) actually work. `docs/ISA3.md` is the design rationale and
> the post-mortem of the static-memory model this replaced; `docs/MACHINE.md` is
> the VM-lens framing. This document is the *built* system, with diagrams.

## 1. The three layers

quiver is a virtual machine for IO: a **compiler** (Polars) lowers high-level
intent into a narrow instruction stream, an **executor** (qvm2, C) runs it over
io_uring + a thread pool, and **completions** flow back as Arrow record batches.

```mermaid
flowchart TB
    subgraph P["PLANNER — quiver/qplan2.py (Polars)"]
        V["verbs: scan / pack / unpack<br/>each a DataFrame → instruction-table macro"]
    end
    subgraph W["WIRE — narrow Arrow IPC over stdin/pipe"]
        I["instruction batches (10-col schema)"]
        R["record batches (QREC schema)"]
    end
    subgraph X["EXECUTOR — quiver/exec/qvm2.c"]
        S["fiber scheduler (1 thread)<br/>completion-driven"]
        RING["io_uring<br/>reads · writes · openat"]
        POOL["compute/blocking pool<br/>zstd · statx · namespace syscalls"]
    end
    FS[("filesystem<br/>(WEKA / local)")]
    P -->|instructions| I --> S
    S -->|"emit"| R -->|"read_ipc_stream (vectorized)"| P
    S <--> RING <--> FS
    S <-->|"eventfd"| POOL <--> FS
```

The wire is deliberately coarse-in, coarse-out: one instruction batch carries
~10⁵ rows, one record batch the same, so the Polars planner pays per-*batch*
cost, never per-*member*. Everything below the wire is C.

## 2. The scheduler: fibers over completions

A **fiber** is a lightweight sequential program (a list of `Instr`) with a
program counter and one **cursor** of in-flight transfer state. Same `tid` runs
in order; different `tid`s run concurrently, bounded by resources. A fiber
**suspends on each op's completion without holding a thread** — thousands are
cheap; only ops *in flight* consume anything.

```mermaid
stateDiagram-v2
    [*] --> INERT: spawned but inert
    INERT --> READY: parent SPAWN
    READY --> READY: fib_step (advance until it parks)
    READY --> WAIT_CQE: submitted a ring op (read/write/open)
    READY --> WAIT_JOB: queued a pool job (codec/statx/nsop)
    READY --> WAIT_VAL: RANDOM val not closed / STREAM chase
    READY --> WAIT_BUDGET: byte budget full / open-gate full
    READY --> WAIT_JOIN: joining a tid range not yet done
    READY --> WAIT_STREAM: tid 0, program drained, wire still open
    WAIT_CQE --> READY: cqe reaped
    WAIT_JOB --> READY: eventfd → done-list drained
    WAIT_VAL --> READY: val closed / grew
    WAIT_BUDGET --> READY: bytes freed / open slot freed
    WAIT_JOIN --> READY: countdown hit zero
    WAIT_STREAM --> READY: new instruction batch arrived
    READY --> DONE: pc past end of program
    DONE --> [*]
```

**One scheduler thread** owns all fiber state and both queues. Workers touch
only their own job plus the eventfd. The loop is: run every ready fiber to its
next park, then block for exactly one completion (a ring cqe, or the eventfd
signalling pool completions), re-ready whoever it unblocks, repeat.

```mermaid
flowchart TB
    A["ready fibers?"] -->|yes| B["fib_step each until it parks or DONE"]
    B --> A
    A -->|"no (all parked)"| C["g_nlive_fibers == 0<br/>&& stream EOF?"]
    C -->|yes| Z["exit"]
    C -->|no| D["io_uring_wait_cqe — ONE completion"]
    D --> E{"cqe source"}
    E -->|"ring op"| F["resume owning fiber<br/>(read landed / write done / open landed)"]
    E -->|"eventfd"| G["drain done-list:<br/>ready each fiber, wake its val"]
    F --> A
    G --> A
```

> **Two bugs this shape hid, both O(n·events) and both fixed to counters**
> ([`docs/ISA3.md` history]): the DONE check scanned every fiber per completion,
> and the liveness test scanned every fiber slot per reap iteration — 64% of a
> 1M-fiber unpack's cycles (perf-witnessed). `g_nlive_fibers` and a `JoinWait`
> countdown replaced both: 494 s → 53 s on the 500k-entry local unpack.

## 3. The value model — dynamic, not static

This is the core departure from v2 (qvm), whose `alloc buf, cap` put memory in
the ISA and made every un-plannable size an edge case. A **val** is an anonymous
chunk-list byte sequence produced by one instruction, consumed by others, named
by a planner-assigned id. Capacity is *discovered*; the runtime owns lifetime.

```mermaid
flowchart LR
    subgraph VAL["Val (id N)"]
        direction LR
        H["chunk 4MB"] --> C2["chunk 4MB"] --> C3["chunk …"]
    end
    PROD["producer fiber<br/>mov fs:path → val"] -->|append| VAL
    CODEC{"codec?<br/>zstd / unzstd"}
    PROD -.-> CODEC -.-> VAL
    VAL -->|"RANDOM: wait close"| CON1["consumer:<br/>mov val@off±len → fs"]
    VAL -->|"STREAM: chase + release-behind"| CON2["consumer<br/>(bounded footprint)"]
```

- **Chunk list, not flat buffer** → `realloc`-into-place and "member > window"
  cease to exist. A codec is a *val property* (`CODEC(zstd,lvl)` / `unzstd`), not
  an instruction: every `mov` into the val streams through it, so one frame = one
  val, and `deflate`/`inflate` leave the ISA.
- **RANDOM vs STREAM**: RANDOM consumers wait for close (gather-deflate, scatter
  from an inflated frame); STREAM consumers chase the producer, freeing chunks
  behind them — a 137 GB member flows through a fixed footprint.
- **One global byte budget** gates admission; a val that alone exceeds it
  degrades to STREAM pacing (the sole-runner rule) rather than deadlocking. This
  is bvm's `blk_add` wait-condition, promoted from comment to invariant.
- **Lifetime** = dataflow: a val dies when its last consumer retires. v1 makes
  this explicit (`FREE`, authored after the last consumer); scope-based freeing
  is the planned successor.

## 4. The instruction set (18 ops) & endpoints

A fiber runs its ops in order; each advances by **exactly one quantum** (one
cqe, one pool job, or one chunk) then retires or stays at `pc`. The **cursor** is
the only in-flight state — zeroed on every retirement, so resumption is uniform.

```mermaid
flowchart LR
    subgraph DATA["data plane"]
        NEWVAL["NEWVAL id,codec,stream"]
        MOV["MOV src→dst  (+DIGEST +TRUNC)"]
        CLOSE["CLOSE id"]
        FREE["FREE id"]
    end
    subgraph CTRL["control"]
        SPAWN["SPAWN lo,hi"]
        JOIN["JOIN lo,hi"]
        FENCE["FENCE"]
    end
    subgraph NS["namespace (blocking pool)"]
        MKDIR["MKDIR"]
        SYMLINK["SYMLINK"]
        LINK["LINK"]
        SETMETA["SETMETA"]
        UNLINK["UNLINK"]
        RMDIR["RMDIR"]
    end
    subgraph SCANOPS["scan"]
        SCAN["SCAN (C-walker generator leaf)"]
        READDIR["READDIR / STATB (wave leaves)"]
    end
    subgraph IO["sink / feedback"]
        SINK["SINK n,path"]
        EMIT["EMIT n"]
    end
```

**`mov`** moves bytes between endpoints; the endpoint matrix is the whole data
plane:

| src ↓ \ dst → | `val` | `fs` | `sink` |
|---|---|---|---|
| **`fs:path`** | read file → val (async openat, then ring reads) | — | — |
| **`val@off±len`** | (gather) | **scatter** file (async open, ring writes, TRUNC) | reserve+write a frame |
| **`inline:bytes`** | planner constant → val (e.g. a header) | — | — |

Flags ride `mov`: `DIGEST` hashes bytes in passing (blake3), `TRUNC` sets the
output file size. A codec on the *destination val* makes the transfer a
compress/decompress. `fs→fs` would specialize to `copy_file_range` (planned).

## 5. Scan: the granularity boundary, made concrete

The one place the fiber model *loses*, measured and designed around. Two forms
coexist in the same ISA:

```mermaid
flowchart TB
    subgraph BAD["per-directory WAVE fibers (READDIR/STATB)"]
        direction TB
        d1["dir fiber"] --> d2["dir fiber"] --> d3["… 10⁵ fibers"]
        note1["10⁵ items × µs work each<br/>= scheduler-bound<br/>measured 85× off a flat walker"]
    end
    subgraph GOOD["SCAN — C walker generator LEAF"]
        direction TB
        w["one I_SCAN instruction"] --> pool["walker-thread pool<br/>shared dir queue, ring-batched opens,<br/>EMFILE re-queue holds NO fd"]
        pool --> qrec["QREC Arrow batches → sink"]
        note2["the VM sees ONE op;<br/>parity-or-better vs bvm<br/>(0.96× on WEKA, 1.45M rows)"]
    end
```

**The law** (`docs/ISA3.md` §4.1, and memory `qvm2-scan-granularity`): scheduling
pays for *planner-scale* fan-out (10²–10⁴ items with real work each — frames,
stat batches) and loses by orders for *filesystem-scale* fan-out (10⁵⁺ items,
microseconds each). So a filesystem walk is a **generator leaf** — bvm's walker
revived inside qvm2, emitting QREC batches the planner reads vectorized — while
waves stay for coarse fan-out. Both are the same wire, the same VM.

## 6. Worked example: `unpack` as a fiber tree

The ISA3 §4.6 shape, as authored by `qplan2.unpack`. Structured concurrency
(`spawn`/`join`) *is* the dependency DAG; scopes replace the old session fences.

```mermaid
flowchart TB
    ROOT["tid 0 (root)"]
    ROOT --> S1["scope 1: dirs"]
    S1 --> mk["MKDIR × N<br/>(depth-sorted, then hash-SHUFFLED<br/>to de-contend per-parent create locks)"]
    ROOT --> S2["scope 2: frames"]
    subgraph FR["per frame F"]
        prod["producer fiber:<br/>NEWVAL unzstd → MOV arch@coff±clen → val → CLOSE"]
        prod --> cons["consumer fibers, ≤512 members each<br/>(share the RANDOM val):<br/>MOV val@in_off±size → fs:dst TRUNC<br/>SETMETA dst"]
    end
    S2 --> FR
    ROOT --> S3["scope 3: FREE vals"]
    ROOT --> S4["scope 4: dir metadata LAST<br/>(restrictive modes can't lock earlier writes out)"]
```

Every ordering constraint that used to need a separate `_Bvm` process is now a
scope boundary: dirs before files (mkparents fallback covers stratum-edge
races), files before restrictive dir modes, all frames before the FREE sweep.
The member-cap on consumers exists because a byte-capped frame of thousands of
tiny members would otherwise serialize every scatter in one fiber.

## 7. The planner: verbs are Polars expressions

No C expanders. Each verb turns a data table into an instruction table, entirely
in Polars — the compiler front-end MACHINE.md always described, taken literally.

```mermaid
flowchart LR
    subgraph PACK["qplan2.pack"]
        sc["scan(root) → DataFrame"] --> fr["frame = cumsum(size)//frame_bytes<br/>(vectorized)"]
        fr --> rows["select(tid, op, a, b, …)<br/>NEWVAL / MOV / CLOSE / MOV→sink / EMIT<br/>per fiber, as column ops"]
        rows --> ord["order: [setup] [fiber bodies] [SPAWN/JOIN tail]<br/>(spawn AFTER feed — the wire ordering contract)"]
        ord --> run["→ qvm2 stream"]
        run --> foot["EMIT records → NOCKZC01 footer<br/>via blocks.write_footer"]
    end
```

Because the footer is written with the *same* `nockidx` machinery bvm uses,
every qplan2-packed store is a first-class nock: `blocks.verify` and
`blocks.unpack` read it unmodified. That **cross-engine gate** — qvm2 packs,
bvm's readers verify, and vice versa, byte-exact — is the correctness contract,
the same reference-core method that validated the BLOCKS engine.

## 8. What the wire carries

```mermaid
flowchart LR
    subgraph INSTR["instruction row (10 cols)"]
        direction LR
        i["tid u32 · op u8 · k1 u8 · k2 u8<br/>a,b,c,d i64 · path str · payload bin"]
    end
    subgraph QREC["QREC record row (scan/emit)"]
        direction LR
        q["name str · kind u8 · mode i32 · size i64<br/>mtime_ns i64 · uid i32 · gid i32<br/>tid i32 · final u8 · phase u8"]
    end
```

Both are generated as compile-time flatbuffer templates
(`quiver/compiler/gen_qvm2.py`, sibling of bvm's `gen_templates.py`) so C emits
them by patching buffer offsets — no per-row serialization — and Polars reads
them with one `read_ipc_stream`. A schema is written once at sink open; batches
are bare messages; a `kind==255` row marks a producer done.

## 9. Status & provenance

- **Correctness**: the EVI cross-engine gate passes — a 5.1 GB / 112,819-file
  corpus subset, qplan2-packed, bvm-verified, qplan2-unpacked, `diff` byte-exact.
- **Performance** (EVI subset unpack, single 64-worker node): 1465 s → **477 s**,
  every step a profiled single-variable fix (two quadratic scans, per-parent
  create-lock shuffle, opens off the scheduler, member-capped fibers). The
  residual is filesystem-bound: WEKA directory *creation* at ~535/s per client
  (per-parent serialization; only multi-node helps) and wekafs completing
  `openat` inline. Forced io-wq offload (`QVM2_ASYNC_OPEN`, opt-in) stalls wekafs
  and is off by default.
- **Scope**: v1 packs/unpacks files + dirs. Symlinks/hardlinks/delta/tar_compat
  are refused loudly, not silently — features on the roadmap alongside
  scope-based val lifetime and multi-node via the wave layer.

Every performance claim here is a same-node, same-allocation measurement; see
the `qvm2-*` memory notes for the falsified theories each fix had to survive.

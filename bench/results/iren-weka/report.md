# quiver benchmark report

- host: `pg11b-5-5-hpc6`  (64 CPUs, 251 GB RAM)
- target: `/mnt/weka/jpc/tmp/quiver-bench`  (fs: **UNKNOWN (0x18031977)**)
- kernel: `5.15.0-177-generic`   date: 2026-07-28 16:39

## Write bandwidth

Threads (separate files, O_DIRECT — page cache excluded, so these are durable):

| threads | 4 | 8 | 16 | 32 | 64 |
|---|---|---|---|---|---|
| GB/s | 1.89 | 3.43 | 5.79 | 6.65 | 7.3 |

Chunk size (at 64 threads):

| chunk MB | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| GB/s | 7.43 | 7.28 | 7.18 | 7.09 | 7.07 |

**Sink count** — concurrent writers into ONE file vs many (quiver writes frames into sinks; this sets `--shards`):

| sinks | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| GB/s | 1.81 | 2.52 | 2.84 | 3.63 | 4.58 | 5.83 |

| mode | GB/s |
|---|---|
| separate_direct | 7.28 |
| separate_buffered | 5.64 |
| one_sink_direct | 1.8 |
| one_sink_buffered | 1.47 |

## Directory scan

80,000 files:

| method | entries/s |
|---|---|
| readdir_dtype | 74,254 |
| readdir_lstat | 86,389 |
| getdents64_dtype | 83,054 |

## Hashes

| algorithm | GB/s |
|---|---|
| xxh64 two-seed (128b) | 4.89 |
| BLAKE2b-128 (sodium) | 0.92 |
| BLAKE2b-256 (sodium) | 0.92 |
| SHA-256 (sodium) | 0.3 |
| SipHash64 (reference) | 2.15 |
| BLAKE3 | 2.06 |

## Crypto (AEAD/KDF candidates)

| algorithm | GB/s |
|---|---|
| keyed-BLAKE2b-256 | 0.92 |
| XChaCha20-Poly1305 | 1.3 |
| AES-256-GCM (NI) | 1.98 |

## Compression under full load: CPU/memory placement

| placement | GB/s |
|---|---|
| none | 3.5 |
| none_end_to_end | 2.19 |
| cpuonly | 1.15 |
| cpuonly_end_to_end | 0.57 |
| local | 1.32 |
| local_end_to_end | 0.62 |
| interleave | 1.56 |
| interleave_end_to_end | 0.76 |
| remote | 1.95 |
| remote_end_to_end | 0.86 |

## Frame cap: how big must a frame be?

quiver splits an oversized member into `--frame-cap-mb` pieces, each compressed as its own frame. Smaller caps mean less worker memory and more restore parallelism; bigger caps compress better, but only up to zstd's match window — which is set by the LEVEL. The knee is the smallest cap within 0.5% of the asymptotic ratio.

| corpus | L1 | L3 | L6 | L9 | L12 | L15 | L19 |
|---|---|---|---|---|---|---|---|
| audio | 1 MB | 1 MB | 1 MB | 2 MB | 4 MB | 4 MB | 4 MB |
| code | 4 MB | 8 MB | 16 MB | 16 MB | 32 MB | 32 MB | 64 MB |
| csv | 2 MB | 2 MB | 8 MB | 16 MB | 16 MB | 32 MB | 64 MB |
| jsonl | 8 MB | 64 MB | 64 MB | 64 MB | 64 MB | 64 MB | 64 MB |
| mixed | 1 MB | 2 MB | 8 MB | 16 MB | 16 MB | 16 MB | 32 MB |
| weights | 1 MB | 1 MB | 1 MB | 1 MB | 1 MB | 1 MB | 16 MB |

| level | L1 | L3 | L6 | L9 | L12 | L15 | L19 |
|---|---|---|---|---|---|---|---|
| zstd window | 0.5 MB | 2.1 MB | 2.1 MB | 4.2 MB | 4.2 MB | 4.2 MB | 8.4 MB |

The window is a FLOOR, not the answer. Data with no exploitable structure (model weights, already-compressed media) is flat from 1 MB — a bigger frame has nothing to find. Data with long-range redundancy (logs, JSONL, source) keeps gaining well past the window, because a longer frame also amortizes the entropy tables. quiver defaults to 8x the window for the level (16/32/64 MB), which costs under 1% of ratio on every corpus measured here while keeping worker memory linear in the cap.

## End-to-end pack

- 4,096 files -> 0.82 GB in Nones (**None GB/s**), 64 workers, level 6
- errors 0, lost 0

Verdict from quiver's own counters:

- worker utilization 79% of 64 workers; busy split: write 92%  hash 3%  open/create 3%
- BANDWIDTH-BOUND: read 0.42 GB/s, write 0.01 GB/s (per-op avg)
- METADATA-BOUND: 4,096 opens avg 705us, 14% >500us (backend RPC / dir-lock contention) -> more parallel dirs, or fewer+larger files
- scan: 4,160 stats avg 467us (cold metadata)
- job queue full 30%+ of the time: workers saturated (this is good scaling)
- incompressible fast-path: 819 raw frames, 0.8 GB stored codec-free (unpack = pure fs-to-fs)
- queues: jobq 68% full (47% saturated, 22% empty) | workers busy 51.4/64 (80%) | scanq 0 dirs | packbuf 1% | blockbuf 0%
- BOTTLENECK = WORKERS (80% engaged, queue saturated 47%): this node is the limit — ADD NODES (per-node fabric/CPU is saturated, more threads won't help)

## Recommended settings

- **`--sinks 32`** (`backup`) / **`--shards 32`** (`recompress`) — 5.83 GB/s vs 1.81 GB/s on a single sink (**3.2x**). Single-inode write contention is usually the biggest single win on a parallel filesystem.
- **`--frame-mb 1`** — knee of the chunk-size curve.
- **`-j 64`** worker threads per node for write-bound work.
- buffered vs O_DIRECT on one sink: 1.47 vs 1.8 GB/s — page cache does not rescue single-inode contention.
- scan: `d_type` (74,254/s) and `lstat` (86,389/s) cost the same on this bench's small, cache-warm tree, so it measures no win. Keep the name-only mode anyway: it costs nothing, and on a cold multi-million-file tree the stat round-trips dominate (measured 13x on this cluster during a 5.8M-file delete). Re-measure with `--scan-dirs`/`--scan-per-dir` large enough to exceed the client cache if you need the real number.
- **do NOT hard-pin worker threads** — unpinned 3.5 GB/s vs 1.95 GB/s for the best pinned placement. With one thread per CPU, a pinned thread that shares a core with anything else (storage client spin-pollers, daemons) cannot migrate, and the job waits for its slowest thread. Memory placement (local/interleave/remote) showed no consistent signal here — the effect is the pinning itself, not NUMA locality.
- hash: **BLAKE3** at 2.06 GB/s — the fastest CRYPTOGRAPHIC option, which is what a chunk id has to be. Non-cryptographic hashes benchmark faster but cannot be used for identity or dedup.

---

_Re-run on your own storage: `./run_bench.py --out results/<site> --dir <path>`. Numbers vary enormously by filesystem, client config and node type; the recommendations above are computed from THIS run._

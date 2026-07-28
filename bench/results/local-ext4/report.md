# quiver benchmark report

- host: `pg11b-5-5-hpc6`  (64 CPUs, 251 GB RAM)
- target: `/tmp/claude-50001/-mnt-weka-jpc-src-quiver/94f8ec2d-13d4-4bcc-be57-98b70204e1a8/scratchpad/localbench`  (fs: **ext2/ext3**)
- kernel: `5.15.0-177-generic`   date: 2026-07-28 11:17

## Write bandwidth

Threads (separate files, O_DIRECT — page cache excluded, so these are durable):

| threads | 4 | 8 | 16 | 32 |
|---|---|---|---|---|
| GB/s | 0.56 | 0.6 | 0.59 | 0.55 |

Chunk size (at 8 threads):

| chunk MB | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| GB/s | 0.52 | 0.53 | 0.51 | 0.53 |

**Sink count** — concurrent writers into ONE file vs many (quiver writes frames into sinks; this sets `--shards`):

| sinks | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| GB/s | 0.52 | 0.55 | 0.59 | 0.55 | 0.57 |

| mode | GB/s |
|---|---|
| separate_direct | 0.64 |
| separate_buffered | 0.65 |
| one_sink_direct | 0.68 |
| one_sink_buffered | 0.47 |

## Directory scan

20,000 files:

| method | entries/s |
|---|---|
| readdir_dtype | 1,029,537 |
| readdir_lstat | 285,685 |
| getdents64_dtype | 1,071,032 |

## Hashes

| algorithm | GB/s |
|---|---|
| xxh64 two-seed (128b) | 3.89 |
| BLAKE2b-128 (sodium) | 0.86 |
| BLAKE2b-256 (sodium) | 0.86 |
| SHA-256 (sodium) | 0.28 |
| SipHash64 (reference) | 1.99 |
| BLAKE3 | 2.12 |

## Crypto (AEAD/KDF candidates)

| algorithm | GB/s |
|---|---|
| keyed-BLAKE2b-256 | 0.86 |
| XChaCha20-Poly1305 | 1.24 |
| AES-256-GCM (NI) | 1.81 |

## Compression under full load: CPU/memory placement

| placement | GB/s |
|---|---|
| none | 1.29 |
| none_end_to_end | 0.83 |
| cpuonly | 1.25 |
| cpuonly_end_to_end | 0.88 |
| local | 1.39 |
| local_end_to_end | 0.79 |
| interleave | 1.48 |
| interleave_end_to_end | 0.74 |
| remote | 1.46 |
| remote_end_to_end | 0.84 |

## End-to-end pack

- 4,096 files -> 0.82 GB in 4.4s (**0.187 GB/s**), 16 workers, level 6
- errors 0, lost 0

Verdict from quiver's own counters:

- worker utilization 18% of 16 workers; busy split: write 78%  hash 12%  read 7%  compress 3%
- BANDWIDTH-BOUND: read 1.42 GB/s, write 0.13 GB/s (per-op avg)
- scan: 4,160 stats avg 3us
- job queue full 30%+ of the time: workers saturated (this is good scaling)
- incompressible fast-path: 819 raw frames, 0.8 GB stored codec-free (unpack = pure fs-to-fs)
- queues: jobq 50% full (46% saturated, 48% empty) | workers busy 9.7/16 (61%) | scanq 0 dirs | packbuf 0% | blockbuf 0%
- BOTTLENECK = WORKERS (61% engaged, queue saturated 46%): this node is the limit — ADD NODES (per-node fabric/CPU is saturated, more threads won't help)

## Recommended settings

- **`--sinks 4`** (`backup`) / **`--shards 4`** (`recompress`) — 0.59 GB/s vs 0.52 GB/s on a single sink (**1.1x**). Single-inode write contention is usually the biggest single win on a parallel filesystem.
- **`--frame-mb 2`** — knee of the chunk-size curve.
- **`-j 8`** worker threads per node for write-bound work.
- buffered vs O_DIRECT on one sink: 0.47 vs 0.68 GB/s — page cache does not rescue single-inode contention.
- scan: `d_type` is 3.6x faster than `lstat` here — `rm`/enumerate paths should never stat.
- CPU/memory placement is ~neutral here (1.29 vs 1.39 GB/s).
- hash: **BLAKE3** at 2.12 GB/s — the fastest CRYPTOGRAPHIC option, which is what a chunk id has to be. Non-cryptographic hashes benchmark faster but cannot be used for identity or dedup.

---

_Re-run on your own storage: `./run_bench.py --out results/<site> --dir <path>`. Numbers vary enormously by filesystem, client config and node type; the recommendations above are computed from THIS run._

# IREN/WEKA benchmark — 2026-07-19

The P2 exit criterion from PLAN §9: quiver against pwalk2 and coreutils
on real WEKA trees. Node `pg12b-4-4-hpc` (24 cores, kernel
5.15.0-136), WEKA-mounted `/mnt/weka`.

Kernel note: 5.15 lacks deferred fixed-file assignment (5.17), so COPY
chains run through the executor's sync fallback — copy numbers below
are the *floor* for quiver; scan/rm/du run native uring.

## Scan — real audio tree, 365k files (`diverse-speakers/youtube-nocc`)

Full statx of every entry. Two passes each; pass 1 ≈ coldest cache,
differences between passes were <10% throughout (WEKA metadata is
RPC-bound either way).

| tool                    | threads | seconds | vs find |
|-------------------------|--------:|--------:|--------:|
| find -printf (1 stat/s) |       1 |    54.5 |      1× |
| quiver scan (uring)     |       8 |     5.1 |     11× |
| quiver scan (sync)      |       8 |     4.9 |     11× |
| quiver scan (uring)     |      16 |     2.6 |     21× |
| pwalk2                  |      16 |     2.6 |     21× |
| quiver scan (uring)     |      32 |     1.4 |     39× |
| quiver scan (uring)     |      64 |     1.0 |     52× |
| pwalk2                  |      64 |     1.1 |     50× |
| quiver scan (uring)     |     128 |     1.0 |     52× |
| pwalk2                  |     128 |     1.1 |     50× |

Findings:

- **quiver == pwalk2 thread-for-thread** (2.6 vs 2.6 at t16, 1.0 vs
  1.1 at t64). The ported worker model reproduces the original's
  performance while emitting Arrow instead of CSV.
- Both plateau at ~350k stats/s around 64 threads — the WEKA metadata
  backend, not the client, is the wall. Default scan threads should be
  raised from 8 to ~64 for WEKA targets.
- uring vs sync engine at equal thread count is a wash *for scan* on
  this kernel: concurrency width (threads × in-flight statx) is what
  the RPC round-trips reward, exactly as PLAN §1 predicted.
- The dataset survey also surfaced a pathological target for a future
  round: a single flat directory holding 40.8M WAVs
  (`data2/multimodal/core/granary/.../audios/en`), where subtree
  parallelism is zero and only statx pipelining can help.

## du — same tree

| tool                     | seconds |
|--------------------------|--------:|
| quiver du (t16,incl. ~1s Python startup) | 6.0 |
| du -sb                   |    54.0 |

9×, and quiver's number is mostly the t16 scan — at t64 the scan
portion drops to ~1s.

## cp / sync / rm — synthetic 20k × 1 KB tree on WEKA

| operation                  | quiver | coreutils |
|----------------------------|-------:|----------:|
| cp (20k files, 200 dirs)   |   66.3 |     108.3 |
| sync (no-op, both sides scanned) | 0.9 | — |
| rm -r                      |    4.1 |      32.6 |

- **rm is 8×**: unlinkat has a native uring opcode, so deletes run at
  full ring concurrency even on 5.15, epoch barriers included.
- **cp is 1.6× despite serialized copies** (5.15 fallback): the mkdir
  epoch runs concurrent on the ring and the sync path spends fewer
  syscalls per file than cp -a. On a ≥5.17 kernel the copy chains go
  concurrent; re-measure then.
- No-op sync converges in 0.9s: two 20k-entry scans + join + empty
  plan.

## Reproduction

`tests/bench-weka.sh` (this run's harness):
scan/du against a real tree read-only; cp/sync/rm against a synthetic
tree created under the runner's own space; every number is a single
`BENCH name seconds` line for easy diffing between runs.

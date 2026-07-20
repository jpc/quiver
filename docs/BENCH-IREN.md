# IREN/WEKA benchmark — 2026-07-19

The P2 exit criterion from PLAN §9: quiver against pwalk2, dust, ducl,
and coreutils on real WEKA trees. Node `pg12b-4-4-hpc` (24 cores,
kernel 5.15.0-136), WEKA-mounted `/mnt/weka`. All quiver numbers
include Python + polars startup (~0.4 s) unless marked as raw scanner.

Kernel note: 5.15 lacks deferred fixed-file assignment (5.17+), so the
executor's direct-fd chains can't run; COPY/CKSUM/SETMETA rows execute
on the 64-worker sync pool instead (see "what profiling found" below —
on wekafs that pool *beats* the chains).

## Scan — real audio tree, 365k files / 39k dirs

Full statx of every entry. Two passes; pass-to-pass spread was <10%.

| tool                    | threads | seconds | vs find |
|-------------------------|--------:|--------:|--------:|
| find -printf (stats)    |       1 |    54.5 |      1× |
| quiver scan (uring)     |       8 |     5.1 |     11× |
| quiver scan (uring)     |      16 |     2.6 |     21× |
| pwalk2                  |      16 |     2.6 |     21× |
| quiver scan (uring)     |      32 |     1.4 |     39× |
| quiver scan (uring)     |      64 |     1.0 |     52× |
| pwalk2                  |      64 |     1.1 |     50× |
| quiver scan (uring)     |     128 |     1.0 |     52× |

- **quiver == pwalk2 thread-for-thread**; both plateau at ~350k
  stats/s around 64 threads — the WEKA metadata backend is the wall.
  CLI default is now `--threads 64`.
- uring vs sync engine at equal width is a wash for scan on this
  kernel: statx punts to io-wq threads either way (wchan sampling
  shows workers in wekafs `commit_blocking_request`); concurrency
  width is everything, exactly as PLAN §1 predicted.

## du — same tree

| tool                  | seconds |
|-----------------------|--------:|
| quiver du (t64)       |     2.1 |
| dust 1.1.1            | 2.1–2.5 |
| ducl scan (pwalk2 t64)| 2.2–2.3 |
| du -sb                |    54.0 |

All three parallel tools sit at the same backend plateau; quiver's
edge is never touching CSV. (An earlier 6s reading was a CLI bug —
`--threads` parsed but not plumbed; found by cProfile, fixed.)

## cp / rm / sync — synthetic 20k × 1 KB tree, 200 dirs

| operation                | quiver | coreutils |  ratio |
|--------------------------|-------:|----------:|-------:|
| cp                       |    2.5 |     109.2 |    43× |
| rm -r                    |    1.6 |      32.2 |    20× |
| sync (no-op, both sides) |    0.9 |         — |      — |

What profiling found, in order (each step verified by re-benchmark):

1. **Directory-lock convoy** (wchan: `rwsem_down_write_slowpath`).
   The kernel takes the parent dir's i_rwsem exclusively per
   create/unlink; dir-major command order collapsed 16 concurrent
   opens to ~2. Fix: the planner stripes copies/unlinks round-robin
   across parent dirs. cp 56.8→36.9s, rm 4.1→1.6s.
2. **io-wq punting pathology** (decisive experiment: the same 20k
   copies as plain userspace threads run in 2.6s at t16 / 0.7s at
   t64, vs 36s as io_uring chains). On 5.15 every wekafs op is punted
   to io-wq threads anyway; whatever io-wq serializes on, userspace
   threads avoid. Fix: the executor's pool executes whole rows via
   row_sync at 64 workers. cp 36.9→2.5s.

(The `/proc/wekafs/stat` avg column suggested the SETMETA tail was
~17 µs noise. BPF kretprobes later showed the truth — setattr is a
full ~1.5 ms backend RPC; that avg was diluted by boot history. See
the BPF section below and the `--no-preserve-times` win.)

## tar — pack / list / extract, same 20k tree

| operation                        | quiver | GNU tar |
|----------------------------------|-------:|--------:|
| pack (create archive)            |    2.3 |     8.4 |
| list                             |    0.6 |    ~0.0 |
| extract (all 20k)                |    2.0 |    79.8 |
| selective extract (100 of 20k)   |    0.8 |     0.5 |

- **pack is 3.7×**: cum_sum layout → concurrent reads + offset writes
  into one archive fd beat tar's serial append. Output verified by
  GNU tar; 50 random members byte-identical.
- **extract is 40×**: planned as executor rows (OP_EXTRACT, the
  archive-read mirror of COPY) — mkdirs by depth, extract rows striped
  across parent dirs, SETMETA mtime tail. The serial-Python original
  tied GNU tar at 79s on the same create wall cp had; through the
  pooled executor it lands at 2.0s, byte-verified, mtimes restored.
- list/selective on a 20 MB page-cache-hot archive can't show the
  footer's advantage (tar full-scans 20 MB in noise time); the
  scale story — one footer read vs 10M header parses, ranged reads
  vs full scan — needs a multi-GB cold-cache run to demonstrate.

## Phase split — where each tool's time goes (2026-07-20, final code)

In-process (no Python startup), t64, warm-ish WEKA caches. wire.scan /
run_commands wrapped with timers; plan = total − scan − execute.

| tool                    | total | scan | plan | exec | coreutils |
|-------------------------|------:|-----:|-----:|-----:|----------:|
| du (real 365k tree)     |   1.5 |  1.1 |  0.3 |    — |      54.0 |
| cp 20k                  |   1.8 |  0.1 |  0.1 |  1.5 |     109.2 |
| sync no-op 20k          |   0.2 |  0.1 |  0.0 |  0.0 |         — |
| pack (tar) 20k          |   1.5 |  0.2 |  0.5 |  0.8 |       8.6 |
| extract 20k             |   1.5 |  0.0 |  0.0 |  1.4 |      79.8 |
| extract selective (100) |   0.2 |  0.0 |  0.0 |  0.2 |       0.5 |
| rm 20k                  |   1.0 |  0.1 |  0.0 |  0.9 |      32.2 |

Every tool is execute-bound at ~40–75 µs/file effective — the wall is
WEKA op throughput, not quiver structure (bare-copy floor at t64 was
0.7 s). Scan and plan are noise everywhere except pack's 0.5 s plan
(cum_sum layout + tar-header expressions) and du's 0.3 s aggregation.
Add ~0.4 s Python startup for one-shot CLI invocations.

## 10× scale — 200k files / 2000 dirs / 307 MiB archive

Same harness, same phase split. Scaling vs the 20k table is linear or
better everywhere; no phase goes super-linear.

| tool                    | total | scan | plan | exec | vs 20k |
|-------------------------|------:|-----:|-----:|-----:|-------:|
| scan 200k               |   0.9 |  0.9 |    — |    — |   ~5× |
| du 200k                 |   0.6 |  0.5 |  0.1 |    — |      — |
| cp 200k                 |  17.3 |  0.9 |  0.6 | 15.8 |   9.6× |
| sync no-op 200k         |   1.5 |  1.5 |  0.0 |  0.0 |   7.5× |
| pack (tar) 200k         |  13.9 |  2.1 |  4.3 |  7.6 |   9.3× |
| extract 200k            |  15.7 |  0.1 |  0.4 | 15.3 |  10.5× |
| extract selective (100) |   0.2 |  0.1 |  0.0 |  0.2 |     1× |
| rm 200k                 |  ~10  |  0.6 |  0.3 |  9.4 |    10× |

Coreutils at this scale: tar -cf 87.9 s (6.3×), rm -rf 322.7 s (31×);
cp -a extrapolates to ~18 min (~63×). Steady-state rates: ~12.7k
copies/s, ~20k unlinks/s, ~12.7k extracts/s. Selective extract stays
constant-time — the footer's point. pack's plan (header expressions +
cum_sum) is linear at ~21 µs/row and is now its largest
non-executor cost.

## Local NVMe (login node, ext4, warm cache, 20k files)

- Raw scanner: 22–29 ms at t8–16 — **3–4× faster than stat-forcing
  `find`** (80–100 ms). The old "find beats scan" reading compared a
  dirent-only `find` (no stats, 40 ms) against a full statx scan
  through the Python wire layer; bench.py now uses `-printf` for the
  honest baseline.
- wire.scan (Python end-to-end): ~53–69 ms — ~1.5 µs/row parse cost.
- rm: executor ≈ `rm -rf` per-op (uring 381 ms vs sync 481 ms for the
  execute phase; `rm -rf` 296 ms total); the remaining gap is the
  fixed scan+plan+spawn ~0.2 s, which amortizes with tree size.
- Python startup (~0.3–0.4 s) dominates only trivial jobs.

## Profiling toolkit

`perf_event_paranoid=4` blocks unprivileged perf, but the nodes grant
passwordless sudo, so kernel BPF is available: a static bpftrace
AppImage on weka + `sudo bpftrace` attaches kretprobes to the wekafs
module directly (see the BPF section). Unprivileged substitutes that
also carried real weight: strace -c (syscall time), /proc/PID/task/
*/wchan sampling (off-CPU wait channels), /proc/wekafs/stat (client
op counts — but its avg-latency column is a boot-lifetime average, so
trust counts, not the µs), cProfile (Python side), and targeted C
experiments. Each finding was confirmed by re-benchmark before acting.

## Reproduction

`quiver/tests/bench-weka.sh` — scan/du read-only against a real tree,
cp/sync/rm/pack against synthetic trees in the runner's own space.
Every number is a `BENCH name seconds` line for diffing between runs.

## BPF profiling of the WEKA client (2026-07-20, sudo + bpftrace)

kretprobe latency histograms on the wekafs kernel module during a live
cp+rm workload (556k RPCs sampled). Probe: `quiver/tests/wekaprobe.bt`.

Per-op client-side latency (the real distribution, not boot averages):

| wekafs op                | median    | note                        |
|--------------------------|-----------|-----------------------------|
| `lookup`                 | 128–256µs | cheap — cache/lease covered |
| `atomic_open` (create)   | 1–2 ms    | fused lookup+create, good   |
| `setattr` (mtime)        | 1–2 ms    | full RPC, = cost of a copy  |
| `unlink`                 | 1–2 ms    |                             |
| `commit_blocking_request`| bimodal   | 16–32µs (cached) / 1–2ms (backend) |

Three conclusions, two of them things NOT to do:

1. **RPC latency is bimodal** — a 16–32µs cached mode and a ~1.5 ms
   backend mode. Every metadata *mutation* pays the 1.5 ms. Throughput
   is therefore concurrency ÷ 1.5 ms; ~13k ops/s at 64 workers means
   the client's 4 frontend processes are the ceiling. Raising client FE
   count is the highest-value knob and it's a research-infra change,
   not a quiver one.
2. **Don't raise `dentry_max_age`** — lookups are 128–256µs, 5–10×
   cheaper than the mutations. Caching them harder saves nothing.
3. **Don't chase io-wq / io_uring for mutations** — the floor is
   backend RPC latency, which userspace threads already saturate.

The one quiver-side lever the histogram revealed: **`setattr` is a full
1.5 ms RPC**, so the mtime-restore epoch is pure overhead where mtimes
don't matter. `cp/sync(preserve_times=False)` (`--no-preserve-times`)
drops it: cp 2.3 s → 1.9 s (~17%; setattr is ~1/3 of the mutation
traffic, the copy's own create+write being the rest). Content-addressed
sync never reads mtime, so this is free there.

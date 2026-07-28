# quiver benchmark suite

Every performance knob quiver exposes has a **filesystem-dependent** optimum. The numbers
below from one cluster are not portable — on WEKA, splitting output across 32 sink files is
worth **3.9x**; on local ext4 the same change is worth **1.0x**. So measure your own storage:

```sh
make
./run_bench.py --out results/mysite --dir /path/on/the/fs/under/test
cat results/mysite/report.md
```

The report ends with **recommended settings computed from your run** (`--shards`,
`--frame-mb`, `-j`), so it is directly actionable.

## What it measures, and why quiver cares

| bench | measures | knob it informs |
|---|---|---|
| `fsbw` | write bandwidth vs threads, chunk size, **sink count**, buffered/O_DIRECT | `--shards`, `--frame-mb`, `-j` |
| `read` | read bandwidth vs threads; one file vs many | restore/verify; sharded nocksets |
| `dirspread` | the same create/stat/unlink work over 1..N **directories** | work shuffling, `rm` strategy |
| `scan` | `readdir`+`d_type` vs `lstat` vs raw `getdents64` | whether enumerate paths may stat |
| `cdc` | content-defined chunking rate + delta size for a localized edit | backup/rsync delta path |
| `hashes` | BLAKE3 / BLAKE2b / SHA-256 / SipHash | chunk-id + digest algorithm |
| `crypto` | AES-GCM / XChaCha20-Poly1305 / keyed BLAKE2b | encryption headroom |
| `cfr` | stored-frame write shapes: buffered copy vs `writev` vs `copy_file_range` | incompressible pack path |
| `framecap` | compression ratio vs (zstd level x frame cap) on YOUR corpora | `--frame-cap-mb` |
| `numa` | fully-loaded compression vs CPU/memory placement | whether to pin workers |
| `multinode` | aggregate bandwidth with N nodes writing at once | how many nodes to use |
| `quiver` | end-to-end pack of a real tree, incl. quiver's own queue-occupancy verdict | everything |

Multi-node needs hostnames:

```sh
./run_bench.py --only multinode --nodes n1,n2,n3,n4 --node-counts 1 2 4 \
  --srun-base "srun -p gpu -G 8 -N 1 -n 1 --mem=0 --time=30"
```

HTML reports (self-contained, no external assets):

```sh
./report_html.py results/siteA results/siteB --index results/index.html
```

`--tree /some/real/data` makes the end-to-end pack use your data instead of synthetic files;
that is the most representative single number.

## Reading the results

Three traps this suite is built to avoid — all of them cost us real debugging time:

1. **Buffered writes measure DRAM, not storage.** A buffered run "achieved" 20.9 GB/s here;
   the same work with `O_DIRECT` was 6.4 GB/s, and the buffered `fsync` then stalled 17s.
   Every headline number in the report is `O_DIRECT` (or durability-inclusive).
2. **Data generation must not be the bottleneck.** `fsbw` fills one buffer with xorshift128+
   *once* and reuses it hundreds of times; it prints the amortized cost (<0.001% of runtime).
3. **Concurrent writers to one file may not scale.** This is the single biggest effect we
   found, and it is invisible unless you test it explicitly.

`quiver`'s own counters also self-report a bottleneck verdict using queue occupancy
(Little's law): a bounded queue persistently *full* means its consumer is the constraint,
persistently *empty* means its producer is. The report includes that verdict verbatim.

## Cluster use

Pass `--srun` to run each measurement on an exclusive node — otherwise co-tenants skew
results (we measured a ~2x error from a busy node):

```sh
./run_bench.py --out results/mysite --dir /mnt/fs/scratch \
  --srun "srun -p gpu -G 8 -N 1 -n 1 --mem=0 --time=30"
```

Multi-node aggregate bandwidth (does the fabric scale?) is a separate question; run the
suite on N nodes simultaneously against the same filesystem and sum the `fsbw` numbers.

## Re-rendering / diffing

`results.json` is the machine-readable record; `./run_bench.py --report results/mysite`
re-renders the markdown from it. Diff two `results.json` files to compare sites, kernels,
client versions, or before/after a tuning change.

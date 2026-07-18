# quiver & nock

**quiver** — Unix file tooling rebuilt set-at-a-time: `scan`, `cp`, `rm`,
`du`, `sync`, `pack`. One relational planner (Polars), swappable
execution engines (io_uring / sync), Arrow record batches as the only
interchange.

**nock** — the index format: an Arrow IPC table of members parked in the
trailing slack of a host container (tar, raw, or Arrow IPC), making any
shard listable in one read and extractable by predicate.

Built for high-latency parallel filesystems (WEKA-class) and object
stores, where round-trips dominate and batching wins. On a warm local
fs, coreutils will beat it — that's expected and honest.

## The one idea

```
   producers                planner                  executors
┌──────────────┐   ┌───────────────────────┐   ┌──────────────────┐
│ fs scan (C,  │   │ Polars: join, delta,  │   │ fs exec (C,      │
│ io_uring)    │──▶│ cum_sum layout, epoch │──▶│ io_uring chains) │
│ nock footers │   │ assignment, header    │   │ s3 exec (boto3)  │
│ S3 listing   │   │ packing — all exprs   │   │ ssh remote exec  │
└──────────────┘   └───────────────────────┘   └──────────────────┘
```

Every tool is a query over stat frames that emits command frames.
`sync` is the general tool; `cp` is sync into an empty destination,
`rm` is sync from an empty source. tar headers are generated as Polars
expressions (byte-compatible with GNU tar, PAX included). Archives
carry their own index; retrofitting existing WebDataset tar shards or
Arrow shards touches zero payload bytes.

## Highlights

- **Zero-dependency runtime**: polars + numpy. pyarrow and flatbuffers
  are test-suite oracles only — `pupyarrow/` (vendored fork of
  [wsds](https://github.com/HumeAI/wsds)'s reader) gained a writer and
  a hand-rolled flatbuffers subset (`fb.py`, 12.6× faster metadata
  walks than generated accessors).
- **One C binary** (`quiver-exec`): io_uring scanner (pwalk2 worker
  model) + executor (linked SQE chains, refcount forest scheduler,
  direct descriptors) + hashing (part-parallel MD5 composite ETags,
  slice-by-8 CRC-64/NVME with GF(2) combine).
- **Predicate pushdown into the scanner**: path prefixes prune whole
  subtrees (no getdents), basename globs skip statx for known-regular
  misses.
- **Pipelined execution**: `quiver.stream` runs the executor while the
  scan is still going; monotone plans overlap, pipeline breakers
  (rmdir, joins, sorts) drain as a tail.
- **WAL / resume**: the command frame *is* the write-ahead log; crash
  anywhere, rerun the WAL — completed rows skip, failed rows retry.
- **ssh mode**: `ssh host quiver-exec` driven verbatim; changed
  payloads ride inside the protocol as inline chunk chains; `verify`
  sends only u64 hashes across the wire.
- **S3 sync**: content-addressed via listed ETags vs locally computed
  composite ETags; multipart, CRC64 end-to-end, moto-tested.

## Quickstart

```sh
apt install liburing-dev libssl-dev   # build deps
make -C quiver/exec                    # builds quiver-exec + templates
PYTHONPATH=. python3 -m quiver.tests.run_all   # 18 groups
PYTHONPATH=. python3 -m quiver.cli du /some/tree
PYTHONPATH=. python3 -m quiver.cli pack --tar /some/tree out.tar
python3 -c "from quiver import nock; print(nock.read_index('out.tar'))"
```

## Status

Prototype consolidated through P0–P2 of [docs/PLAN.md](docs/PLAN.md);
18 integration test groups (GNU tar interop, oracle scan parity,
crash-resume, live-sshd remote sync, moto S3). Not yet handled:
symlinks, hardlink preservation in cp, sparse files, xattrs — see the
plan before pointing it at trees it didn't create.

## Credits

`pupyarrow/` originates from HumeAI/wsds. `pupyarrow/fb.py` ports the
builder algorithm from google/flatbuffers (Apache-2.0) — see NOTICE.
The scanner's worker model follows HumeAI/ducl's pwalk2.

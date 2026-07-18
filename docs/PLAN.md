# quiver & nock — Plan

**quiver** — Unix file tooling rebuilt set-at-a-time: scan, cp, rm, du, sync,
pack. One relational planner, swappable execution engines.

**nock** — the index format: an Arrow IPC table of members parked in the
trailing slack of a host container (tar, raw, or Arrow IPC), making any
shard listable in one read and extractable by predicate.

Everything in the *Prototyped* column below has been implemented and
verified end-to-end (GNU tar interop, io_uring engines, moto S3, ducl
schema casts, pupyarrow round-trips). This document is the consolidation
plan: what the architecture simplifies down to, and what remains.

---

## 1. The one idea

Every tool is the same three-stage pipeline over Arrow record batches:

```
   producers                planner                  executors
┌──────────────┐   ┌───────────────────────┐   ┌──────────────────┐
│ fs scan (C,  │   │ Polars: join, delta,  │   │ fs exec (C,      │
│ io_uring)    │──▶│ cum_sum layout, epoch │──▶│ io_uring chains) │
│ nock footers │   │ assignment, header    │   │ s3 exec (boto3)  │
│ S3 listing   │   │ packing — all exprs   │   │ ssh remote exec  │
└──────────────┘   └───────────────────────┘   └──────────────────┘
```

Producers emit **stat frames**. The planner turns queries into **command
frames** with dependency epochs. Executors return **completion frames**.
The planner never knows or cares which producer or executor is on the
other side — filesystem, archive footer, object store, or a process on
the far side of an ssh pipe.

A tool is then just a query:

| tool          | plan                                                        |
|---------------|-------------------------------------------------------------|
| `quiver du`   | scan → group_by prefix (blocks·512, hardlink-dedup on ino)  |
| `quiver rm`   | scan → unlinks @0, rmdirs deepest-first epochs              |
| `quiver cp`   | scan → mkdirs by depth, copies after, metadata tail         |
| `quiver sync` | scan×2 → full outer join on path → delta → epoch ladder     |
| `quiver pack` | scan → cum_sum layout → header exprs → archive copy chains  |
| `quiver nock` | read existing container headers/metadata → append index     |

---

## 2. Simplifications (what this plan deletes)

The prototypes accumulated parallel mechanisms. Consolidate:

**S1 — One table schema, three roles.** The scan output, the nock
footer, and the sync manifest are the same logical table with role-
specific column subsets:

```
core:      path, size, mtime_ns, mode, uid, gid, is_dir
scan +:    blocks, atime_ns, ctime_ns, ino, parent_ino, dev, nlink, depth
footer +:  offset, data_offset, read_size, [codec], [header_len]
manifest+: hash_algo, hash, part_size, [etag]
```

Producers fill what they know; consumers select what they need. Nullable
columns beat three schemas. The wire STAT template covers the scan
subset; footers and manifests are written by Python/pupyarrow where
schema flexibility is free.

**S2 — Delete the Python Ring.** The in-process `SyncRing`/op-chain
layer was scaffolding to shape the io_uring seam. The pipe protocol *is*
the seam now. Python keeps zero execution machinery; the C executor's
`sync` engine is the portable fallback, and the Python walker survives
only inside the test suite as the scan-parity oracle.

**S3 — One sync delta.** Content-addressed only. The per-backend
listing already carries a content hash (AWS/B2 ETag with deterministic
`part_size`, GCS crc32c/md5 from JSON listing, nock footers carry their
own hash column); local expectations come from the executor's CKSUM op.
mtime survives solely as an optional pre-filter to skip hashing
obviously-unchanged files (size+mtime equal ⇒ assume clean unless
`--paranoid`). The metadata-mtime delta path is removed.

**S4 — One copy opcode.** `COPY_FILE(src, dst, dst_offset, header?,
pad_align)` subsumes archive-COPY: packing an archive is copying into
one big destination at planner-computed offsets with a header prefix.
The executor no longer special-cases the archive fd; it is passed per
batch (or opened per row via dst_path).

**S5 — One binary, three verbs.** `quiver-exec {scan|exec|cksum-bench}`.
Scanner and executor stay in one process image; remote deployment is
copying one static binary.

**S6 — pupyarrow gains a write path and owns the whole IPC surface.**
A minimal writer (flat schemas, no nulls/nesting/compression — exactly
the STAT/CMD/COMP/footer shapes) built purely on the flatbuffers codegen
already vendored in pupyarrow. Consequences: the schema compiler becomes
self-hosted (pupyarrow writes the reference messages *and* locates the
patch slots in its own bytes — pyarrow leaves the build chain entirely);
footers and manifests are written without pyarrow at runtime; remote
`apply` nodes need only the static binary plus a tiny Python env.
pyarrow survives solely as the test-suite conformance oracle ("pyarrow
must read every stream we emit").

**S8 — There is one tool.** `sync_cmds(src_frame, dst_frame)` is THE
planner: full outer join, delta, epoch ladder. cp is sync into an empty
dst frame; rm is sync from an empty src frame (plus the root rmdir);
sync is the general case. tools.py collapsed accordingly. Future: pack
against an existing archive's footer frame = incremental pack, for
free.

**S7 — No watchdog.** The pwalk2 stuck-op watchdog addressed a WEKA bug
that is fixed. Not ported. (Revisit only if D-state hangs reappear;
the hook point is the worker loop's blocking ops.)

---

## 3. nock format specification (v1)

### 3.1 Index placement — the slack rule

The index is a standard Arrow IPC file (schema metadata:
`nock_version`, `nock_host`, plus host-specific keys) followed by a
16-byte trailer `[u64 ipc_len]["ARTARIDX"→"NOCKIDX1"]`, parked where the
host container cannot see it:

| host      | placement                              | native readers      |
|-----------|----------------------------------------|---------------------|
| raw       | at EOF                                 | (none — index-only) |
| tar       | after the 2×512 end-of-archive marker  | any tar, WebDataset |
| arrow     | before the Arrow Footer flatbuffer     | pyarrow, polars, pupyarrow |

Locator: check EOF for nock trailer; else if EOF ends `ARROW1`, step
over the Arrow footer and check there. One function, all hosts.

### 3.2 Index contents

Core columns per §S1 footer role. Required: `path`, `data_offset`,
`size`, `read_size`. `offset`/`header_len` only for tar hosts. Domain
columns are open — wsds adds `key`, `ext`, `dur`, `spk`; the schema
compiler never sees these (footers are written in Python).

`codec` column reserves per-member compression (zstd per value), which
is the supported answer for compressed audio — IPC buffer compression is
rejected because it destroys range addressability.

### 3.3 Writing

- **New archives**: layout is a Polars query (`cum_sum` of
  `header_len + pad(size, align)`), tar/PAX headers are generated as
  Polars expressions (verified byte-compatible with GNU tar incl. PAX
  long names and the UTF-8 padding traps), execution is copy chains.
- **Retrofit (`quiver nock`)**: tar → one header pass, append; arrow →
  pupyarrow metadata pass, splice before Footer. Original bytes never
  move. On S3: `UploadPartCopy` + appended tail part; no data egress.
- **Durability**: footer write is the commit point. It happens only
  after all payload completions drain and an `FBARRIER` (fsync) epoch
  completes. Crash before footer ⇒ the container is a valid plain
  tar/arrow file with no index; re-nock is idempotent.

### 3.4 Versioning & hashes

`part_size` and `hash_algo` are recorded **per row**, never assumed
globally, so the checksum scheme can evolve without invalidating
history. v1 algorithms: `md5-composite` (S3 ETag semantics),
`crc64nvme` (composable, u64, S3 FULL_OBJECT verified on PUT).

---

## 4. Wire protocol (frozen at v1)

Arrow IPC streams over stdin/stdout. Three fixed schemas compiled to C
templates by the schema compiler (`gen_ipc_template.py`), which
navigates pyarrow's reference serialization with pupyarrow's vendored
flatbuffers codegen (hand-rolled navigator as fallback; both must agree
at build time).

**CMD** (planner → executor): `user_data u64, opcode u8, dep_group i64,
path, dst_path, header binary, header_offset i64, data_offset i64,
size i64, pad_align i64, mode i32`. Rows sorted by `dep_group`.

**COMP** (executor → planner): `user_data u64, res i32 (0/-errno),
read_size i64, cksum u64, etag binary, parts i32`.

**STAT** (scanner → planner): scan-role columns of §S1.

Opcodes v1: `COPY_FILE, UNLINK, RMDIR, MKDIR, CKSUM, FBARRIER`.
Reserved: `LINK, SYMLINK, SETMETA` (chown/chmod/utimes — no io_uring
opcodes exist; lands in the executor's sync pool, replacing today's
Python metadata tail).

Ordering contract: the executor barriers between distinct `dep_group`
values; each pipe batch is additionally an implicit barrier. Epochs
encode: rmdir-after-children (deepest-first), mkdir-before-copy
(shallowest-first), delete-after-copy, footer-after-fsync.

Transports: local pipes; `ssh host quiver-exec …` verbatim; a
socket/shm transport is an optimization, not a semantic change.

---

## 5. Executor engine (C, `quiver-exec`)

**exec**: per-row io_uring chains (`IOSQE_IO_LINK`) over direct-
descriptor slots; row-indexed completion accounting; MKDIR treats
-EEXIST as success; severed-link short reads replay via the sync path;
sync engine as runtime fallback and semantic reference.

**scan**: the pwalk2 worker model, ported faithfully — FIFO dir queue
with atomic in-flight termination, per-worker rings, double-buffered
getdents/statx pipeline (readdir batch B while the ring stats batch A),
getdents concurrency gate. Emits whole STAT batches under one mutex, so
worker output interleaves losslessly in a single IPC stream. Output
verified deterministic across engines and thread counts.

**cksum**: single-pass per-part MD5 → composite ETag (+`-N`), plus
full-object CRC64NVME. Verified against the spec check vector, hashlib,
and moto's real multipart ETags.

Planned engine work, in order of expected WEKA impact:

1. **Refcount scheduler** replacing epochs *inside* the executor:
   filesystem deps form a forest, so per-row `parent_row` + child
   counters (submit at zero) give maximal cross-subtree concurrency
   where depth-barriers idle the ring. Wire change: one column
   (`parent_row i64`), epochs remain the degenerate case.
2. **Predicate pushdown into scan**: planner decomposes conjuncts via
   `expr.meta`; path-prefix conjuncts prune subtrees (skip getdents),
   basename/d_type conjuncts skip statx, residual stat conjuncts filter
   post-CQE. Wire change: scan cmd rows gain prefix/glob columns.
3. **Cross-directory statx overlap**: workers currently drain per
   directory chunk; letting statx from queue-adjacent dirs share the
   ring window helps many-small-dirs trees.
4. **Hashing throughput — first tranche shipped**: CRC64 upgraded to
   slice-by-8 (word-at-a-time tables) with a GF(2) `crc64_combine`
   (zlib's crc32_combine generalized to the NVME poly), making CRC
   composable across parts; CKSUM of files > 2 parts now fans out to a
   4-thread part pool (independent MD5 per part is S3's own
   construction; per-part CRCs folded via combine) — verified
   bit-identical to sequential reference and the spec vector, ~0.4
   GB/s combined md5+crc in this container. Remaining: ISA-L `md5_mb`
   lanes for the many-small-files regime, PCLMULQDQ CRC64, thread-pool
   sizing from measurement, O_DIRECT reads for the hash path.
5. **Copy fast paths**: `copy_file_range`/reflink worker-thread branch
   keyed on a planner-computed `same_dev` column; registered buffers +
   O_DIRECT for the archive path.

**C vs Rust**: stays C while the protocol is frozen and the codebase is
one file (~1k lines). Documented triggers to rewrite in Rust: (a) the
executor needs to evaluate serialized Polars predicates (polars-rs), (b)
generic op graphs beyond the forest model, (c) the second
lifetime/aliasing bug of the pathbuf class reaches production.

### 5.1 Pipelined execution (execute while scanning)

`quiver.stream` runs the executor concurrently with the scanner: scan
batches stream in, per-batch command frames stream out, and pipe-batch
barriers do the cross-batch ordering for free. The planner
classification (same theory as query-engine pipeline breakers): a
per-batch plan pipelines iff it is **monotone** — each row's commands
depend only on rows already seen. Monotone: mkdir+copy (cp), file
unlinks (rm), layout with a running offset (pack in arrival order),
aggregation folds (du). Pipeline breakers, which run as a small tail
after the stream drains: rmdir (subtree completeness), sync's full
outer join (needs the other side — unless it's a manifest lookup, which
makes streaming sync a hash probe), and sort policies (pack gives up
the locality sort in streaming mode; offsets being a prefix-sum means
layout itself was never a breaker).

Ordering caveat that shaped the design: per scanner *worker*, parent
rows precede child rows; across workers there is no ordering (buffers
flush independently). Streaming plans are therefore written monotone
against out-of-order arrival — stream_cp derives needed ancestors from
each batch's own paths (mkdir -p semantics + created-set) and applies
real directory metadata in a deepest-first SETMETA tail, which also
restores directory mtimes (batch cp never did).

---

## 6. Cloud & remote

**Sources/sinks registry** (Python): `fs://`, `nock://shard-or-manifest`,
`s3://` (AWS, B2, GCS-interop via endpoint_url), `ssh://host/path`.
`quiver sync A B` resolves both, scans both, plans once.

**Object-store sync** is content-addressed (§S3): listing hashes vs
executor-computed expectations; multipart with pinned `part_size`;
S3-validated CRC64 on upload for end-to-end integrity. Delta shown to
skip touched-but-identical files and to converge idempotently (moto).

**Manifests**: per-prefix manifest object = as-synced stat frame
(footer-role columns + hashes). Refresh cycle: List (cheap, 1 req/1000
keys) reconciles `(key, size, etag)` against manifest rows; mismatches
demote to dirty. Manifest writes use conditional PUT (`If-Match` on the
manifest's own ETag) for multi-writer safety. **Super-manifest** for
sharded corpora: concatenated shard footers + shards table; `du`,
listing, and sync planning over an S3 archive become one GET. Later:
chunk the super-manifest and Merkle/prolly-hash the chunks so two sites
diff indexes in O(log n) exchanges — the control plane for multi-PB
remote sync.

**Remote data plane — shipped, and simpler than planned**: the wire
protocol already *is* a data plane. The `header` column writes
arbitrary bytes at `header_offset`, so a changed file crosses ssh as a
chain of inline-payload COPY rows — chunk 0 owns the truncate (C rule:
only an offset-0 write truncates), later chunks are ordered by
parent_row chains within the copy epoch. `quiver.remotes.ssh.Ssh`
drives `ssh host quiver-exec` verbatim: remote scan (STAT over ssh
stdout), remote execution (CMD over ssh stdin), `sync_to` (plan
locally from both scans, metadata ops verbatim, payloads inline), and
`verify` (both executors CKSUM, only u64 hashes cross the wire —
1-bit corruption detection tested against a live sshd). Deployment is
one static binary. The raw-archive `apply` envelope remains the design
for object-store transfer where there's no executor on the far side.

Hardening still open: ssh ControlMaster connection reuse, exe
bootstrap (scp on first use), host path quoting, resumable inline
transfer (WAL the chunk rows), and bandwidth-shaped chunk sizing.

---

## 7. Integrations

**wsds**: nock's scope in wsds is deliberately narrow — **audio shard
storage and WebDataset tar conversion**, nothing more. Tar hosts stay
WebDataset-compatible (training loaders stream them unchanged) and
`quiver nock` retrofits existing wds shards metadata-only, server-side
on B2; `.tar.gz` shards convert to feather-zstd instead. Rich-typed
shards remain plain Feather — pupyarrow reads them natively and they
need no nock index (the Arrow-host splice remains a general capability,
not a wsds requirement). The **wsds seek index stays**: it maps
timestamps to byte positions *within* members, a different granularity
than the nock footer's member→range mapping; the two compose (footer
finds the sample, seek index finds the moment).

**ducl**: `quiver-exec scan` replaces pwalk2 + the CSV hop. ducl adopts
the STAT schema directly (drop the `%07o`/seconds/depth−1 compat shims);
`child_count`/`dir_total_size` move into agg.py as `group_by(parent_ino)`;
`ducl update` becomes a path-prefix partition swap; `ducl dashboard`
pointed at a super-manifest is du-of-S3 without scanning S3.

---

## 8. Repository layout

```
quiver/
  exec/            quiver-exec.c, ipc_gen.h (generated), Makefile
  compiler/        gen_ipc_template.py  (pyarrow+pupyarrow, build-time)
  nock/            formats (raw/tar/arrow-host), planner exprs,
                   footer read/write, locator, retrofit converters
  tools/           du, rm, cp, sync, pack  (thin queries over nock/)
  remotes/         fs / s3 / ssh sources & sinks, manifests
  tests/           parity oracle (python walker), GNU tar interop,
                   moto suite, spec vectors, determinism matrix
```

---

## 9. Phases

**P0 — Consolidation (this plan).** Apply S1–S7 to the prototype code;
rename; freeze wire v1; port existing test matrix into `tests/`.
Exit: all current green tests pass under the new layout.

**P1 — Trust. ✅ shipped.** FBARRIER opcode (native IORING_OP_FSYNC on
the archive fd, path form for files/dirs); pack orders payload-barrier →
footer → trailer-fsync, making the footer the literal commit point.
SETMETA opcode (chmod/chown/utimensat in the executor sync pool, -1
sentinels) replaced the Python mtime tail in cp/sync. quiver/wal.py:
the command frame persists as a nock feather, completions append to an
idempotent done-log (torn tails tolerated), resume is a filter, dry-run
is `wal.status`, and retry is rerunning the WAL — failed rows are simply
never marked done (verified: crash-after-chunk-1 resume; premature
ENOTEMPTY rmdir succeeding on rerun).

**P2 — WEKA performance. ◐ in progress.** Shipped: scan pushdown
(stage-1 prefix subtree-prune — pruned subtrees never getdents; stage-2
basename glob — known-regular misses never get a statx SQE; both
verified equal to full-scan-then-filter, with a stats line proving the
pruning: dirs 23→5, statx 264→52 on the test tree) and the refcount
scheduler (`parent_row` forest deps in the executor; chunk boundaries
stay barriers, parent_row rebases per chunk, WAL resume remaps
positions after filtering — crash-resume tested cross-chunk). Epochs
remain the degenerate case. tests/bench.py is the local sanity floor
(coreutils wins on warm-cache local fs, as expected — the design pays
off where round-trips cost); the real exit criterion is unchanged:
benchmark on IREN/WEKA against pwalk2, GNU tar, rsync, mpiFileUtils.
Remaining: cross-directory statx overlap; the polars predicate→prefix
decomposer feeding pushdown automatically; hashing engines and copy
fast paths (P4).

**P3 — Scale-out sync.** Manifests + super-manifest + conditional
writes; `apply` mode; ssh transport hardening; wsds shard migration
tooling (`quiver nock --all` over a bucket) and ducl cutover.

**P4 — Throughput.** Hashing engines (md5_mb, PCLMUL), copy fast paths,
socket/shm transport if pipes measure as a bottleneck (they likely
won't — batches amortize).

---

## 10. Open questions

- **Raw-host alignment default**: 1 (dense) vs 4096 (O_DIRECT-friendly)
  for wsds audio shards; measure against B2 range-read behavior.
- **Deletion semantics in shard-land**: tombstone rows in the
  super-manifest vs compaction policy; interacts with training-set
  reproducibility (a manifest snapshot *is* a dataset version — cheap
  data versioning may fall out for free).
- **Inline small bodies** (payloads below ~4–16 KB as a footer binary
  column) — parked since the two-format split; revisit with real wsds
  size distributions.
- ~~Refcount wire format~~ resolved: `parent_row` is within-frame
  positional; the executor sees chunk-local indices (rebased by the
  wire layer), chunk boundaries are barriers, and WAL resume remaps
  positions after filtering. Stable ids only become necessary if chunks
  ever stop being barriers.
- **pupyarrow ownership**: promote out of wsds into `quiver/` (ducl and
  quiver both depend on it), or keep wsds as the home and depend on it?

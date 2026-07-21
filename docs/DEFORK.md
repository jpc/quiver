# The de-fork: collapse the bespoke modes/drivers into the one machine

**Why.** We designed one machine — `scan`/generators → Polars plan → the *one*
executor over a command stream → link the footer (`docs/ISA.md`,
`docs/MACHINE.md`). But the zframe work was shipped as a pile of **parallel
machines**, each re-implementing I/O + framing + append + footer:

- **C modes**: `zpack`, `zscan`, `zexec` (and the never-built `zunpack`) — each
  its own reader loop and wire handling.
- **Python thread-pool drivers**: `recompress_c`, `pack_fs`, `unpack`,
  `unpack_merged`, `merge`, and the reshard/S3/WAL variants.

Every one of these is really **one instruction with different operand fields**
(§2 addressing modes): `src ∈ {FILE, STREAM, ARCHIVE, INLINE}`, `transform ∈
{ZSTD_C, ZSTD_D, IDENTITY}`, `sink ∈ {ARCHIVE_APPEND, STREAM, FILE}`. `pack_fs`
is `COMPRESS(src=FILE)`, recompress is `COMPRESS(src=STREAM)`, unpack is
`EXTRACT(transform=ZSTD_D)`, reshard is a `sink` field. They should be **Polars
compiler passes emitting the one command stream**, not tools.

The model already exists in the tree: **`tools.pack` / `nock.extract` are the
consolidated form** — scan → plan → `OP_COPY`/`OP_EXTRACT` commands → the one
executor → footer. The zframe (compressed) path is the fork; make it look like
`tools.pack`.

## Plan (safe-first; each step keeps the suite green, old driver → oracle)

1. **`unpack` (linear) → `OP_EXTRACT` + `ZSTD_D` on the one executor.** Pure
   addition (no `zpack` to touch). The command carries `frame_coff` (=
   `data_offset`), `frame_clen`, `in_off`, `size`, `path`, `mode`, `mtime`; the
   executor decompresses the frame and slices `[in_off, size]`.
   - **KEY FINDING (verified in code):** the pool is a **shared LIFO work
     queue** — `open_worker` pulls `p->q[--p->qn]`, so rows are dispatched
     dynamically, **not** as contiguous per-worker ranges. A *thread-local*
     one-frame cache therefore gets poor hits (a frame's members scatter across
     workers, each re-decoding the 16 MB frame). So use a **shared bounded frame
     cache** (small LRU, mutex-protected: first worker to hit a frame decodes +
     inserts, others reuse) — or add frame-affinity scheduling. Sort the plan by
     `frame_coff`. Oracle: the current Python `unpack`/`extract`.
2. **`pack_fs` → `OP_COMPRESS(src=FILE)`.** The executor gains a FILE-source
   **fetch/gather**: read a frame's files by path, tar-format, gather into the
   frame buffer, `ZSTD_C`, append (offset from completion). This is `zexec`'s
   reader with pread instead of decompress. Oracle: the current `pack_fs`.
3. **recompress → `OP_COMPRESS(src=STREAM)`.** This is `zexec` already; reframe
   it as the executor's STREAM fetch (not a separate mode), so `zpack`/`zexec`
   collapse into one `COMPRESS` with a source-mode switch.
4. **Sharded extract (multi-file source).** The one executor currently opens a
   single archive fd; sharded unpack/extract needs a `shard_id → file` table.
   Add a source-file table to the command context; then `unpack_merged` and the
   distributed unpack fold in too.
5. **Retire** `zpack`/`zscan`/`zexec` modes and the Python thread-pool drivers
   to test oracles once each capability re-lands as an instruction.

## Acceptance (the test I wrote in INTERFACES.md and then ignored)

One instruction schema in, one completion schema out, one footer, one WAL; each
tool differs from the others only by field values; every existing zframe test
(now an oracle) still passes byte-for-byte.

## State at compaction

- Tree is **green: 28 test groups.** The prototypes (recompress_c, reshard, S3,
  WAL, merge, unpack, pack_fs) all work and are byte-exact — they are the
  oracles for this consolidation.
- Distributed pack run: SLURM array **260863** (8-way, level 6) still `PD
  (Resources)`; monitor `b87t9b6yx` will merge shards + report. `docs/` also
  has ISA/MACHINE/INTERFACES/DISTRIBUTED/UNPACK/model.py.
- Was mid-way starting step 1 (found the LIFO-queue fact above); nothing
  uncommitted.

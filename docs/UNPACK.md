# Unpacking the nock format (linear and sharded)

Unpacking is the **mirror of recompress**, and the footer is the pivot: recompress
*writes* it (member → byte range, offsets resolved from completions); unpack
*reads* it and turns each entry back into a file. Where recompress **gathers**
tar members into frames, unpack **scatters** members out of frames — same
frame-parallel structure, run backwards.

## It's a fully static plan

Recompress had an un-plannable half (`ZSTD_C` output size known only at
retirement). Unpack has none: the footer already knows every member's `(shard,
frame_coff, frame_clen, in_off, size, path, mode, mtime)`. So the **footer *is*
the plan** — a pure relocation table, already resolved. No dynamic offsets, no
linker, and — unlike distributed recompress — **no merge**: the outputs are
independent files. This is the read direction, and it's strictly simpler.

## The instruction: EXTRACT + ZSTD_D + a frame cache

Today `OP_EXTRACT` is `ARCHIVE(data_offset, size) → FILE(path)`, a raw
`pread`→write with no decode (§2's `ARCHIVE→FILE, IDENTITY`). zframe members
live *inside* a compressed frame, so unpack is the same instruction with two
additions:

- **transform `ZSTD_D`**: decompress the frame `[frame_coff, frame_clen]`, then
  slice `[in_off, in_off+size]`.
- **a thread-local one-frame cache**: if a row's frame equals the last one this
  worker decoded, reuse it. The planner sorts the command stream by
  `(shard_id, frame_coff)`, so a frame's members are consecutive and each frame
  is decompressed **once** — the natural batch unit, with no new opcode, just
  per-row dispatch plus a cache. (A frame holds ~a batch of members, so one
  decode feeds many writes.)

For the **sharded** format, `shard_id` selects the source file instead of the
single archive fd; everything else is identical. That's the only difference
between unpacking a linear and a sharded nock.

## Addressing modes (the mirror of pack)

`src = ARCHIVE(shard_id, frame_coff, frame_clen)` → `ZSTD_D` → **scatter** to
`FILE(path)` per member, plus `MKDIR` (deduped dirs) and `SETMETA` (mode/mtime)
— exactly `extract`'s existing `EXTRACT`+`MKDIR`+`SETMETA` shape (§6), now with
a decode. The sink generalizes like recompress's did: unpack to files, or
**re-pack** into another archive (`recompress_c`, which re-levels), or to `STREAM`
(→ S3 / a pipe) — same three sink modes.

## Local parallel unpack: the compress pool, backwards

Group selected members by frame; hand each frame to a worker that decompresses
it once and writes its members' files. That's the recompress compress-pool
**run in reverse** — a decompress pool over independent frames. It replaces the
current prototype (`extract`/`extract_merged` in `zframe.py` decode frames in
single-threaded Python; the docstring already flags this as the placeholder).

## Distributed unpack: partition frames, no reduce

Frames are independent, so unpack scatters across nodes with no coordination and
**no merge**:

- **Sharded nock** → assign **whole shard files** to nodes (data locality: a
  node reads the shard files it owns). Natural and IO-local.
- **Linear nock** → partition by **frame byte-range**: node *k* handles frames
  in `[a_k, b_k)`, reading only that slice of the one file (weka scales
  concurrent reads). 

Each node runs the local parallel unpack on its command shard; the destination
files are disjoint, so the job is done when the last node is — near-linear,
capped only by output IO. (Contrast recompress, whose reduce was the merge;
unpack's reduce is empty.)

## Selective unpack

A predicate on the footer (glob/size) filters members *before* planning, so only
frames containing a wanted member are read. The cost model to note: extraction
granularity is the **frame** — pulling one member still decodes its whole
~16 MB frame. For dense extraction that's ideal (amortized); for sparse
point-lookups it over-reads, and the mitigation is to choose frame batching at
*pack* time for the expected access pattern (small frames for random access,
large for bulk).

## Build steps

1. **Executor**: extend `OP_EXTRACT` with an optional `ZSTD_D` decode + in-frame
   slice (`frame_coff`/`frame_clen`/`in_off` ride existing command columns) and
   a `__thread` one-frame cache; open `shard_id`'s file for the sharded case.
2. **Planner**: `read_index`/`read_merged` → filter → sort by `(shard_id,
   frame_coff)` → emit the `EXTRACT`+`MKDIR`+`SETMETA` command stream (the same
   zstd-compressed command stream recompress uses).
3. **Distribute**: partition the frame set (by shard file, or by frame range for
   linear) across nodes via `MultiExecutor`; no merge.
4. Retire the Python `extract`/`extract_merged` prototypes once the C path
   lands, keeping them as the reference oracle in the test suite.

Acceptance test: unpack(pack(X)) == X, byte-exact and attribute-exact, and a
distributed unpack of a sharded nock equals a single-node unpack of the same
members.

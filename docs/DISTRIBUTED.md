# Distributed recompress: scatter · map · merge

Now that the plan is an `OP_COMPRESS` command stream (`docs/ISA.md` §8.1),
distributing it is map-reduce: **compile a different instruction stream per
node** (scatter), run each node's `zexec` independently (map), then **merge**
the shard outputs into one archive (reduce). The merge is the "unit" that takes
frames-with-footers and emits one frame stream with footers joined — and it's
cheap, for a specific structural reason.

## The shape

1. **Scatter (compile per node).** Partition the command stream by source
   affinity — node *k* gets whole sources (a frame never spans a source, so a
   node's work is self-contained). This is exactly what `MultiExecutor` already
   does to the command stream; each node gets a sub-plan.
2. **Map (execute).** Each node runs `zexec` on its sub-plan with **no
   coordination**, producing its own archive: `[frame₀…frameₘ][footer]`, with
   offsets local to that node's output.
3. **Reduce (merge).** Combine the *N* `(frames, footer)` shards into one
   `(frames, footer)`.

## The merge unit is the linker, across shards

The single-node linker already resolves each member's offset from its
`COMPRESS` completion and writes the footer. The merge is the same linker with
one extra term — a per-shard **base offset** — so it needs no recompression and
no reframing, only concatenation and arithmetic:

Let shard *k*'s frame region be `Sₖ = max(frame_coff + frame_clen)` over its
members (the compressed bytes of its frames, i.e. everything before its own
footer). Define `base[k] = Σⱼ<ₖ Sⱼ`. Then:

- **Frames** — concatenate the shard frame regions in order:
  `[shard0 frames][shard1 frames]…`. Because each shard is a run of complete,
  independent zstd frames, the concatenation is itself a valid zstd/`tar.zstd`
  stream — no decode, no re-encode.
- **Footer** — read each shard's footer, add `base[k]` to every `frame_coff`
  (and `Σⱼ<ₖ mⱼ` to the frame label for uniqueness), concatenate the rows, and
  write one nock footer at the end. Pure Polars:

  ```python
  parts = []
  base = 0
  for k, shard in enumerate(shards):
      idx = read_index(shard)                       # this shard's members
      parts.append(idx.with_columns(pl.col("frame_coff") + base))
      base += frame_region_size(shard)              # Sₖ
  footer = pl.concat(parts)                          # the joined index
  ```

That's the whole reduce: `cat` the frame regions, shift the offsets. No
`COMPRESS` runs in the merge.

## Why it's this cheap: the invariant

Everything above works only because **a frame never spans a source or a sink**
— the same invariant behind reshard and random-access extract. It makes each
shard's frames self-contained, so they concatenate byte-exact and their footers
compose by simple offset addition. Distributed merge is a concatenation, not a
recompression, *by construction*.

## Two merge modes

- **Physical (one file).** Copy the shard frame regions into one output and
  append the joined footer → a single valid `tar.zstd` (+ `.nock`). The copy is
  a ~`ΣSₖ`-byte, IO-bound pass — expressible as `COPY` instructions
  (`src=ARCHIVE(shard_k)` → `dst=ARCHIVE_APPEND`), so the merge *itself* is a
  small program in the same ISA, not a new opcode.
- **Logical (zero-copy manifest).** Don't move bytes: keep the *N* shard files
  and give the joined footer a `shard_id` column, leaving `frame_coff` local.
  `extract` reads shard[`shard_id`] at `frame_coff`. The "archive" is then a
  partitioned dataset (N files + one index) — the parquet-dataset shape — for
  when a single file isn't required.

## In machine terms

The merge is **not a new instruction** — it's the reduce half of distributed
execution: the linker (a service, in Polars) joining footers, optionally driving
`COPY` instructions to concatenate frames. So the full distributed recompress
reuses everything: `MultiExecutor` scatters the command stream, per-node `zexec`
maps, and the cross-shard linker reduces. It also closes a duality — a **sink**
can already be a node (scatter output to N nodes); the merge is the **gather**
that brings node outputs back into one index.

Properties that fall out: the reduce is **associative** (merge shards pairwise
or all at once) and **streaming** (a shard can be folded in as it completes,
accumulating `base` and appending its rebased footer rows), so a long
distributed run doesn't wait for all nodes before it starts linking.

## Sketch of the unit

```python
def merge(shards, out):                 # reduce: N (frames, footer) → 1
    base, frame_base, rows = 0, 0, []
    with open(out, "wb") as fo:
        for k, shard in enumerate(shards):
            idx = read_index(shard)
            rows.append(idx.with_columns(
                pl.col("frame_coff") + base,
                pl.col("frame")      + frame_base))
            copy_frame_region(shard, fo)            # append [0, Sₖ) bytes
            base       += frame_region_size(shard)  # Sₖ
            frame_base += idx["frame"].n_unique()
        write_footer(out, pl.concat(rows))          # one joined nock footer
```

Same acceptance test as the rest of the refactor: the merged archive is
byte-indistinguishable from a single-node run over the union of the sources —
`recompress` is still `pack`, just executed in pieces and linked.

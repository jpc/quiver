# Distributed streaming rsync: scan, reconcile, delta

rsync in quiver's model is a **program** (a Polars reconciliation over two scan
tables → COPY/UNLINK commands), not a new machine. What's new here is (1)
distributing the *scan itself* across machines as a streaming work queue, and
(2) a content layer — streaming rolling checksums — under the metadata check, so
we can do rsync's block-delta transfer, not just whole-file copy.

Three layers, cheapest first; each is optional and streams into the next:

```
  L0  size + mtime      metadata diff   (scan × scan)          — skip unchanged
  L1  whole-file cksum  content diff    (CKSUM, CRC-64/MD5)    — verify/undated mtimes
  L2  rolling block delta  sub-file diff (BLOCKSUM + DELTA)    — transfer only changed blocks
```

## 1. Distributed streaming scan — roots as a work queue

Scanning a 33 M-file tree is itself the long pole, and it parallelizes across
*subtrees* (the filesystem forest: disjoint top-level dirs are independent —
the same fact `MultiExecutor` and streaming `rm` already exploit). Instead of
one machine walking everything, distribute the **walk**:

- **Coordinator** walks the *top* of the tree (getdents at the root, cheap) and,
  as it discovers directory nodes at depth ≤ *d*, pushes each as a **root** onto
  a shared queue. It does not descend into them.
- **Workers** (the coordinator itself + remote nodes over `SlurmTransport`/
  `SshTransport`) pull roots off the queue and run the existing `scan_iter` on
  that subtree — a full recursive walk of just that subtree, streaming `STAT`
  rows back. A fast/idle node pulls more roots: **work-stealing**, so a skewed
  tree (one giant subtree) balances without a static partition.
- Each subtree scan ends with the **close-event** we already emit
  (`child_count ≥ 0` on the dir row at getdents-EOF, `scan(closes=True)`). That
  event is the synchronization primitive: it means "this subtree is fully
  enumerated", so its reconciliation can fire *now*, before the rest of the
  tree is scanned.

Both `src` and `dst` are scanned this way (same root queue, two scans per root),
so a subtree's src-listing and dst-listing complete together and reconcile as a
**small, independent chunk**.

```
 coordinator: getdents(root) ─► root queue ─┬─► worker A: scan_iter(subtreeᵢ) ─► STAT+close ─┐
                                            ├─► worker B: scan_iter(subtreeⱼ) ─► STAT+close ─┤
                                            └─► coordinator: scan_iter(subtreeₖ) ────────────┤
                                                                                            ▼
                                              per-subtree reconcile (src × dst) at its close-event
                                                                                            ▼
                                                COPY / UNLINK chunk ─► MultiExecutor (that node)
```

This overlaps three stages that today run in sequence: **scan** subtree A on
node W, **reconcile** subtree B on the coordinator, **execute** subtree C's
commands on node W′ — the `(step, finish)`/`drive()` streaming loop
(`stream.py`), now with the source being a *distributed* `scan_iter` and the
reconcile keyed on close-events (exactly how streaming `rm` fires `RMDIR`).

Locality: reconcile and execute a subtree on the **node that scanned it** (its
dcache is warm, and it's already a WEKA client frontend), so the COPY/UNLINK for
that subtree never crosses back to the coordinator. The only global serialization
is the root (created before fan-out, removed after join) — the `MultiExecutor`
root-op rule, unchanged.

## 2. L0 — size + mtime (the existing metadata diff)

Per subtree chunk: full-outer-join src×dst on the subtree-relative path; a file
needs transfer iff `dst` missing or `(size, mtime_ns)` differ; a `dst`-only file
is deleted (`--delete`). This is `sync_cmds` today, now applied per subtree at
its close-event instead of over the whole tree. Cheapest layer — one stat each,
no bytes read — and it clears the overwhelming majority (unchanged files).

## 3. L1/L2 — content: streaming rolling checksums

Metadata can lie (mtime preserved but content changed; or mtime unreliable), and
for *large* files a whole-file recopy is wasteful when a few blocks changed.
That's the rsync delta algorithm; it maps onto two streaming opcodes, both of
which extend the existing `CKSUM` (which already streams a file once computing
per-part MD5 + rolling CRC-64 for S3 ETags — the same single-pass shape).

### BLOCKSUM (receiver side, dst) — a streaming per-block checksum table

Read each candidate `dst` file **once**, in fixed blocks of *B* bytes, emitting
per block `(offset, weak, strong)`:
- **weak** = rsync's rolling checksum `a = Σ bᵢ mod 2¹⁶`, `b = Σ (n−i)·bᵢ mod
  2¹⁶`, packed `a | b<<16` — cheap, rollable.
- **strong** = a truncated BLAKE3/MD5 of the block — collision guard.

Embarrassingly parallel across blocks **and** files → the worker pool, one block
per job (the `CKSUM` per-part fan-out, at block granularity). Output is a small
checksum table streamed back, keyed by file.

### DELTA (sender side, src) — roll the weak sum in one streaming pass

For each changed `src` file, read it **streaming** (never materialize it) and
roll the weak checksum byte-by-byte — O(1) per byte:

```
a' = (a − b_out + b_in) mod 2¹⁶
b' = (b − n·b_out + a') mod 2¹⁶      # n = block length
weak' = a' | b'<<16
```

At each position, look up `weak'` in the dst block table (a hash set); on a hit,
confirm with the **strong** hash of the current window. A confirmed match emits
a **block-copy** (dst offset → out offset); the unmatched bytes between matches
emit a **literal** run. The result is a **delta script**: a column of
`(op ∈ {LITERAL, COPY}, src_off|dst_off, len)`. Sequential per file (the roll is
a recurrence), parallel across files (pool). One pass, bounded memory (a block
window + the current literal run) — the "streaming manner": it runs against
multi-hundred-GB files without buffering them, exactly like the zframe reader.

### Reconstruct — apply the delta

The delta script is itself a **command stream** for the one executor: `LITERAL`
rows are `COPY(src[a:b] → out)` (zero-copy range move, §ISA), `COPY` rows are
`COPY(dst_old[c:d] → out)` (reuse the receiver's existing bytes — the transfer
saving). So delta-apply is the buffer machine's `COPY` with two source regions
(the new file and the old dst file) — no new execution hardware, just a second
source fd in the AOT source table (§10.5, the same `shard_id → fd` mechanism).

## 4. Where each layer runs (cost model)

| layer | reads | parallelism | when |
|---|---|---|---|
| L0 size+mtime | stat only | across subtrees + files | always (clears ~all) |
| L1 whole-file cksum | whole file once | across files (pool) | mtime untrusted / verify |
| L2 block delta | changed files once | across files; roll seq. per file | large files, few changed blocks |

L2 pays for itself only when `(blocks changed × B) ≪ file size` and bytes are
dear (WAN/S3 egress). On the local WEKA fabric (§BENCH-IREN: ~1.6 GB/s/node) a
whole-file recopy at L1 often beats the delta's extra read of `dst`; L2 is the
**cross-site / S3** tool. The planner picks the layer per file from a size
threshold + transport cost, the same way pack chose frame size per access
pattern.

## 5. Build order (each a streaming stage, oracle = coreutils/rsync)

1. **Distributed streaming scan**: root queue + work-stealing over
   `MultiExecutor` transports; reconcile per subtree at its close-event (extend
   `stream.py`'s `drive()` with a distributed `scan_iter` source). Oracle: a
   single-node `sync` over the union equals the distributed run.
2. **BLOCKSUM opcode**: per-block `(weak, strong)` — the `CKSUM` fan-out at block
   granularity. Oracle: recompute in Python.
3. **DELTA opcode**: streaming rolling match → delta script. Oracle: `rsync
   --only-write-batch` / a Python reference roller; reconstruct(delta) == src,
   byte-exact.
4. **Reconstruct**: lower the delta script to `COPY` rows with a two-fd source
   table; fold into the reconcile chunk. Acceptance: distributed streaming rsync
   of a mutated tree == `rsync -a`, byte-exact, with L2 transferring only the
   changed blocks (measure literal vs copy bytes).

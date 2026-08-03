# qvm2 micro benchmarks / milestone gates

Each `drive_m*.py` is a milestone gate (see the header of `quiver/exec/qvm2.c`);
every one packs real bytes and self-verifies. `bench_scan.py <root>` runs the
three-leg scan A/B (bvm cold, qvm2 I_SCAN, bvm warm — same process, back to back;
only same-run numbers are comparable).

Build first: `make -C quiver/exec bvm qvm2` (override ZSTD/URING/BLAKE3 to your
static-lib prefixes). Gates create their trees in the CWD — run from a scratch dir:

    python bench/qvm2/drive_m6.py          # cross-engine pack/unpack gate
    python bench/qvm2/drive_m5.py <tree>   # I_SCAN leaf vs os.walk truth
    python bench/qvm2/bench_scan.py <tree> # the A/B

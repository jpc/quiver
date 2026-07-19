#!/bin/bash
# quiver WEKA benchmark — run on a GPU node via srun.
# Read-only scans on a real 365k-file audio tree; rm/cp on a synthetic
# tree under the runner's own space. Every timing printed as "BENCH <name> <seconds>".
set -u
TREE=/mnt/weka/jpc/diverse-speakers/youtube-nocc
PY=/mnt/weka/jpc/miniconda3/envs/env/bin/python3
Q=/mnt/weka/jpc/src/quiver
EXE=$Q/quiver/exec/quiver-exec
PWALK2=/mnt/weka/jpc/disk-usage/filesystem-reporting-tools/pwalk2
WORK=/mnt/weka/jpc/tmp/quiver-bench-$$
export PYTHONPATH=$Q

echo "== node: $(hostname)  kernel: $(uname -r)  cores: $(nproc)"

t() {  # t <name> <cmd...>
    local name=$1; shift
    local s=$(date +%s.%N)
    "$@" >/dev/null 2>/tmp/bench-err-$$
    local rc=$?
    local e=$(date +%s.%N)
    if [ $rc -ne 0 ]; then echo "BENCH $name FAILED rc=$rc"; head -3 /tmp/bench-err-$$;
    else echo "BENCH $name $(echo "$e $s" | awk '{printf "%.1f", $1-$2}')"; fi
}

echo "== A: scan of $TREE (365k files) — two passes, first is coldest =="
for pass in 1 2; do
    t scan-quiver-uring-t8-p$pass  $EXE scan $TREE uring 8
    t scan-quiver-uring-t16-p$pass $EXE scan $TREE uring 16
    t scan-quiver-sync-t8-p$pass   $EXE scan $TREE sync 8
    t scan-pwalk2-t64-p$pass       $PWALK2 --threads 64 $TREE
    t scan-pwalk2-t16-p$pass       $PWALK2 --threads 16 $TREE
done
t scan-find-stat-p1 find $TREE -printf "%s %T@\n"

echo "== B: du =="
t du-quiver-t16 $PY -m quiver.cli --threads 16 du $TREE
t du-coreutils  du -sb $TREE

echo "== C: synthetic 20k-file tree in $WORK =="
mkdir -p $WORK
$PY - "$WORK" <<'EOF'
import os, sys
w = sys.argv[1]
src = os.path.join(w, "src")
for d in range(200):
    dd = os.path.join(src, f"d{d:03d}")
    os.makedirs(dd, exist_ok=True)
    for f in range(100):
        with open(os.path.join(dd, f"f{f:03d}.dat"), "wb") as fh:
            fh.write(os.urandom(1024))
print("created 20k files")
EOF

t cp-quiver   $PY -m quiver.cli cp $WORK/src $WORK/dst-quiver
t cp-coreutils cp -a $WORK/src $WORK/dst-cp
t sync-quiver-noop $PY -m quiver.cli sync $WORK/src $WORK/dst-quiver
t rm-quiver   $PY -m quiver.cli rm $WORK/dst-quiver
t rm-coreutils rm -rf $WORK/dst-cp
rm -rf $WORK
echo "== done"

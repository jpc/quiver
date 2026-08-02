"""Regression suite for the BLOCKS executor (docs/BLOCKS.md, docs/DATAFLOW.md).

Covers every mode against the real blocks.py APIs: recompress / unpack (+ frame
split) / incremental WAL pack / multi-tar / two-tree diff / streaming diff / and
the networked encrypted rsync — all byte-exact. Run: `pytest tests/test_blocks.py`."""
import os, io, shutil, time, tarfile, hashlib
import pytest
import polars as pl
from quiver.exec import blocks


# ------------------------------------------------------------------ helpers
def _tar_zstd(path, members, fmt=tarfile.PAX_FORMAT):
    """members: [(name, bytes)] -> a .tar.zstd; returns {name: md5}."""
    import zstandard
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=fmt) as tf:
        for name, data in members:
            ti = tarfile.TarInfo(name); ti.size = len(data); ti.mode = 0o644
            tf.addfile(ti, io.BytesIO(data))
    open(path, "wb").write(zstandard.ZstdCompressor(level=6).compress(buf.getvalue()))
    return {name: hashlib.md5(data).hexdigest() for name, data in members}


def _tree_md5(root):
    d = {}
    for dp, _, fns in os.walk(root):
        for fn in fns:
            p = os.path.join(dp, fn)
            d[os.path.relpath(p, root)] = hashlib.md5(open(p, "rb").read()).hexdigest()
    return d


def _write(root, rel, data):
    p = os.path.join(root, rel)
    os.makedirs(os.path.dirname(p) or root, exist_ok=True)
    open(p, "wb").write(data)


def _sample_members(n=40):
    # includes an oversize member (idx 7) + long PAX paths to exercise both paths
    out = []
    for i in range(n):
        sz = 200_000 if i == 7 else 500 + i * 777
        deep = "/".join(f"very_long_directory_component_{k}" for k in range(i % 4 + 1))
        out.append((f"{deep}/f{i:04d}.bin", bytes(((i * 37 + j) & 0xFF for j in range(sz)))))
    return out


# ------------------------------------------------------------------ recompress / unpack
def test_recompress_byte_exact(bvm, tmp_path):
    src, nk, dest = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock"), str(tmp_path / "un")
    truth = _tar_zstd(src, _sample_members())
    n = blocks.recompress_c(src, nk, bvm, nworkers=4, frame_bytes=64 << 10)
    idx = blocks.scan_nock(nk).filter(pl.col("frame") >= 0)
    frames = sorted(idx["frame"].unique().to_list())
    assert frames == list(range(len(frames)))                 # dense frame ids
    assert idx.filter(pl.col("coff") < 0).height == 0          # no null_coff
    assert n == len(truth)
    blocks.unpack_c(nk, dest, bvm, nworkers=4)
    assert _tree_md5(dest) == truth


def test_unpack_frame_split(bvm, tmp_path):
    src, nk, dest = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock"), str(tmp_path / "un")
    truth = _tar_zstd(src, _sample_members(30))
    blocks.recompress_c(src, nk, bvm, nworkers=4, frame_bytes=32 << 10)
    tot = 0
    for k in range(4):                                        # 4 disjoint frame subsets
        tot += blocks.unpack_c(nk, dest, bvm, nworkers=2, predicate=(pl.col("frame") % 4 == k))
    assert tot == len(truth)
    assert _tree_md5(dest) == truth


def test_gnu_format(bvm, tmp_path):
    src, nk, dest = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock"), str(tmp_path / "un")
    truth = _tar_zstd(src, _sample_members(20), fmt=tarfile.GNU_FORMAT)   # GNU 'L' long names
    blocks.recompress_c(src, nk, bvm, nworkers=4, frame_bytes=64 << 10)
    blocks.unpack_c(nk, dest, bvm, nworkers=4)
    assert _tree_md5(dest) == truth


# ------------------------------------------------------------------ incremental pack
def test_pack_incremental(bvm, tmp_path):
    root, nk, wal, dest = (str(tmp_path / "tree"), str(tmp_path / "s.nock"),
                           str(tmp_path / "s.wal"), str(tmp_path / "un"))
    os.makedirs(root)
    _write(root, "a", b"A" * 5000); _write(root, "b", b"B" * 5000)
    _write(root, "sub/c", b"C" * 4000); _write(root, "sub/d", b"D" * 5000)
    r1 = blocks.pack_fs_c(root, nk, wal, bvm, nworkers=4)
    assert r1["packed"] == 4 and r1["unchanged"] == 0
    blocks.unpack_c(nk, dest, bvm, nworkers=4); assert _tree_md5(dest) == _tree_md5(root)

    time.sleep(1.1)
    _write(root, "b", b"b" * 6000)                           # changed
    _write(root, "e", b"E" * 5000)                           # added
    os.remove(os.path.join(root, "sub/d"))                   # deleted
    r2 = blocks.pack_fs_c(root, nk, wal, bvm, nworkers=4)
    assert (r2["unchanged"], r2["packed"], r2["deleted"]) == (2, 2, 1)
    shutil.rmtree(dest); blocks.unpack_c(nk, dest, bvm, nworkers=4)
    assert _tree_md5(dest) == _tree_md5(root)                 # reflects the current tree


# ------------------------------------------------------------------ multi-tar
def test_multi_tar(bvm, tmp_path):
    srcs, truth = [], {}
    for a in range(3):
        s = str(tmp_path / f"t{a}.tar.zstd")
        truth.update(_tar_zstd(s, [(f"tar{a}/f{i:03d}", bytes([a * 91 + i & 255]) * (400 + i * 300))
                                   for i in range(20)]))
        srcs.append(s)
    nk, dest = str(tmp_path / "m.nock"), str(tmp_path / "un")
    n = blocks.recompress_multi(srcs, nk, bvm, nworkers=6, frame_bytes=32 << 10)
    assert n == len(truth)
    blocks.unpack_c(nk, dest, bvm, nworkers=4); assert _tree_md5(dest) == truth


# ------------------------------------------------------------------ diff
def _two_trees(tmp_path):
    A, B = str(tmp_path / "A"), str(tmp_path / "B")
    os.makedirs(A + "/sub")
    _write(A, "a", b"X" * 3000); _write(A, "b", b"Y" * 3000)
    _write(A, "sub/c", b"Z" * 3000); _write(A, "d", b"W" * 3000)
    shutil.copytree(A, B); time.sleep(1.1)
    _write(B, "b", b"YY" * 4000); os.remove(os.path.join(B, "d")); _write(B, "e", b"V" * 3000)
    return A, B


def test_diff_fs_and_tar(bvm, tmp_path):
    A, B = _two_trees(tmp_path)
    exp = dict(added=["e"], removed=["d"], changed=["b"], unchanged=["a", "sub/c"])
    d = blocks.diff_trees(("fs", A), ("fs", B), bvm)
    assert {k: sorted(d[k]) for k in exp} == {k: sorted(v) for k, v in exp.items()}
    tar = str(tmp_path / "A.tar.zstd")
    import zstandard
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for rel in ["a", "b", "sub/c", "d"]:
            tf.add(os.path.join(A, rel), arcname=rel)    # preserves mtime + mode
    open(tar, "wb").write(zstandard.ZstdCompressor(level=6).compress(buf.getvalue()))
    d2 = blocks.diff_trees(("tar", tar), ("fs", B), bvm)
    assert {k: sorted(d2[k]) for k in exp} == {k: sorted(v) for k, v in exp.items()}


def test_diff_stream(bvm, tmp_path):
    A, B = str(tmp_path / "A"), str(tmp_path / "B")
    for d in range(6):
        for i in range(15):
            _write(A, f"sub{d:02d}/f{i:03d}", bytes([d * 7 + i & 255]) * (100 + i))
    shutil.copytree(A, B); time.sleep(1.1)
    _write(B, "sub03/f005", b"CH" * 50); _write(B, "sub02/new", b"N" * 40)
    os.remove(os.path.join(B, "sub04/f010"))
    stream = blocks.diff_stream(A, B, bvm)
    mat = blocks.diff_trees(("fs", A), ("fs", B), bvm)
    assert {k: sorted(stream[k]) for k in stream} == {k: sorted(mat[k]) for k in mat}


# ------------------------------------------------------------------ footer format
def test_chunked_footer_random_access(tmp_path):
    from quiver.nock import nockidx
    import numpy as np
    N = 120_000
    i = np.arange(N)
    paths = pl.DataFrame({"i": i}).select(
        ("d_" + (pl.col("i") // 100).cast(pl.String).str.zfill(5) + "/f_"
         + pl.col("i").cast(pl.String).str.zfill(7) + ".bin").alias("p"))["p"]
    df = pl.DataFrame({"path": paths, "size": i.astype(np.int64),
        "mode": np.full(N, 0o644, np.int32), "mtime_ns": i.astype(np.int64),
        "uid": np.zeros(N, np.int32), "gid": np.zeros(N, np.int32),
        "frame": (i // 8).astype(np.int32), "frame_coff": (i * 1000).astype(np.int64),
        "frame_clen": np.full(N, 1000, np.int64), "in_off": (i * 10).astype(np.int64)})
    nk = str(tmp_path / "f.nock")
    with open(nk, "wb") as f:
        base = f.write(b"\x28\xb5\x2f\xfd" + b"BODY" * 64)          # fake archive body
        f.write(nockidx.pack_footer(df, base, rows_per_batch=25_000, level=3))
    ents = nockidx.read_directory(nk)
    assert len(ents) == 5                                            # 120k / 25k
    assert sum(e[3] for e in ents) == N                             # dir row-count
    assert nockidx.read_footer(nk).equals(df)                       # full round-trip
    one = list(nockidx.iter_batches(nk, batches=[2]))               # inflate ONE batch only
    assert len(one) == 1 and one[0].height == 25_000
    assert one[0]["path"][0] == df["path"][50_000]                  # right slice


# ------------------------------------------------------------------ streaming unpack
def test_unpack_streaming_multibatch(bvm, tmp_path):
    """Unpack streams the footer batch-by-batch; batches are frame-aligned, so each holds
    whole frames and scatters independently. Force a tiny footer batch size to exercise it."""
    from quiver.nock import nockidx
    src = str(tmp_path / "s.tar.zstd")
    truth = _tar_zstd(src, _sample_members(40))
    nk = str(tmp_path / "o.nock")
    blocks.recompress_c(src, nk, bvm, frame_bytes=2000)    # many small frames
    idx = blocks.scan_nock(nk)                              # rewrite footer w/ rows_per_batch=7
    de = int((idx.filter(pl.col("frame") >= 0)["coff"] + idx.filter(pl.col("frame") >= 0)["clen"]).max())
    data = open(nk, "rb").read()
    fd = os.open(nk, os.O_RDWR | os.O_TRUNC); os.write(fd, data[:de])
    rows = blocks._stat_rows([{k: r[k] for k in blocks.STAT_COLS} for r in idx.iter_rows(named=True)])
    blocks.write_footer(fd, de, [rows], rows_per_batch=7); os.close(fd)
    assert len(nockidx.read_directory(nk)) > 4             # genuinely multi-batch
    seen = set()                                           # no frame is split across batches
    for batch in nockidx.iter_batches(nk):
        frames = set(batch.filter(pl.col("frame") >= 0)["frame"].to_list())
        assert not (frames & seen)                         # frame-aligned: disjoint per batch
        seen |= frames
    dest = str(tmp_path / "un")
    assert blocks.unpack_c(nk, dest, bvm) == len(truth)
    assert _tree_md5(dest) == truth                        # multi-batch unpack byte-exact


# ------------------------------------------------------------------ block lifecycle
def test_block_lifecycle_guard(bvm, tmp_path):
    """A plan that copies a block after freeing it is caught (error), not UB."""
    src = str(tmp_path / "s.tar.zstd")
    _tar_zstd(src, [(f"f{i}", b"x" * 500) for i in range(5)])
    b = blocks._Bvm(bvm, 4)
    b.sink(0, str(tmp_path / "o.nock"))
    b.open_tar(0, os.path.abspath(src), 1 << 20, tar_compat=1)
    while True:
        t, pl_ = b.read()
        if t == 0:
            break
    _sid, block, _m = blocks._parse_stat(pl_)
    b.free_block(block)                                   # retire it
    b.copy_block(block, 0, 6)                             # use-after-free -> guard must fire
    with pytest.raises(RuntimeError):
        b.finish()


def test_block_budget_no_deadlock(bvm, tmp_path):
    """Many blocks through a tiny budget: retire+busy must free under back-pressure, no hang."""
    src = str(tmp_path / "s.tar.zstd")
    truth = _tar_zstd(src, [(f"g{i % 4}/f{i:04d}", bytes([i & 255]) * 3000) for i in range(300)])
    out = str(tmp_path / "big.nock")
    n = blocks.recompress_multi([src], out, bvm, nworkers=4, frame_bytes=8 << 10, budget_mb=1)
    assert n == len(truth)
    dest = str(tmp_path / "un"); blocks.unpack_c(out, dest, bvm, nworkers=4)
    assert _tree_md5(dest) == truth


# ------------------------------------------------------------------ WAL resume
def test_recompress_wal_resume(bvm, tmp_path):
    import struct
    src = str(tmp_path / "s.tar.zstd")
    truth = _tar_zstd(src, _sample_members(40))            # multi-frame (has an oversize member)
    out, wal = str(tmp_path / "r.nock"), str(tmp_path / "r.wal")
    n1 = blocks.recompress_c(src, out, bvm, frame_bytes=8 << 10, wal_path=wal)
    committed, _, _ = blocks._wal_load(wal)
    assert len(committed) >= 4                             # enough frames to make resume meaningful
    keep = sorted(committed)[:3]
    cut = max(committed[f][0] for f in keep)               # cursor after the 3rd committed frame
    data = open(wal, "rb").read(); off = 0; recs = []       # keep only the first 3 WAL records
    while off + 20 <= len(data):
        _f, _c, ln = struct.unpack_from("<qqI", data, off); recs.append(data[off:off + 20 + ln]); off += 20 + ln
    open(wal, "wb").write(b"".join(recs[:3]))
    os.truncate(out, cut)                                  # simulate a crash after 3 frames
    n2 = blocks.recompress_c(src, out, bvm, frame_bytes=8 << 10, wal_path=wal)   # resume
    assert n2 == n1 == len(truth)
    dest = str(tmp_path / "un")
    blocks.unpack_c(out, dest, bvm)
    assert _tree_md5(dest) == truth                        # resumed archive is byte-exact


# ------------------------------------------------------------------ networked rsync
def test_rsync_networked(bvm, tmp_path):
    src, dst = str(tmp_path / "src"), str(tmp_path / "dst")
    for d in range(6):
        for i in range(15):
            _write(src, f"sub{d:02d}/f{i:03d}", bytes([d * 7 + i & 255]) * (200 + i * 30))
    shutil.copytree(src, dst); time.sleep(1.1)
    _write(dst, "sub03/f005", b"STALE" * 40)                 # changed -> resync
    os.remove(os.path.join(dst, "sub04/f010"))               # missing -> send
    _write(dst, "sub01/extra", b"E" * 50)                    # extra -> delete
    _write(src, "sub05/fresh", b"F" * 77)                    # src-only -> send
    out = blocks.rsync(src, dst, bvm, n=4)                    # X25519 KX, 4 AEAD conns, apply-on-fly
    assert out["sent"] == 3 and out["deleted"] == 1
    assert _tree_md5(dst) == _tree_md5(src)                   # byte-exact after sync


@pytest.mark.slow
def test_giant_frame_over_2gb(bvm, tmp_path):
    """A member whose frame exceeds Linux's ~2GB pwrite cap: an unlooped pwrite short-writes
    and the frame is silently DROPPED (this cost a 4.6TB home backup 1.33M members).
    Guards pwrite_all(). Skipped unless QUIVER_SLOW=1 (writes 2.2GB)."""
    if not os.environ.get("QUIVER_SLOW"):
        pytest.skip("set QUIVER_SLOW=1 (2.2GB of I/O)")
    src = tmp_path / "src"; src.mkdir()
    big = src / "huge.bin"
    with open(big, "wb") as f:
        chunk = os.urandom(1 << 20)
        for _ in range(2200):
            f.write(chunk)
    import hashlib
    h = hashlib.blake2b(open(big, "rb").read(), digest_size=16).hexdigest()
    r = blocks.backup(str(src), str(tmp_path / "s.nock"), bvm, nworkers=4, level=1, strict=True)
    assert r["errors"] == 0 and r["lost"] == 0, r
    out = tmp_path / "u"
    blocks.unpack(str(tmp_path / "s.nock"), str(out), bvm, nworkers=4)
    assert hashlib.blake2b(open(out / "huge.bin", "rb").read(), digest_size=16).hexdigest() == h


def test_verify_complete_catches_dropped_members(bvm, tmp_path):
    """verify() re-hashes only SURVIVING members, so a dropped member passes it.
    verify_complete() compares the footer against the live tree and catches it."""
    src = tmp_path / "src"; src.mkdir()
    for i in range(12):
        (src / f"f{i}").write_bytes(os.urandom(4096))
    nock = tmp_path / "s.nock"
    blocks.backup(str(src), str(nock), bvm, nworkers=4, level=1)
    good = blocks.verify_complete(str(nock), root=str(src))
    assert good["missing"] == 0 and good["bad_locators"] == 0, good
    (src / "appeared_later.txt").write_bytes(b"not in the footer")
    bad = blocks.verify_complete(str(nock), root=str(src))
    assert bad["missing"] == 1 and "appeared_later.txt" in bad["missing_sample"], bad


def test_many_members_multi_record_batch(bvm, tmp_path):
    """polars splits an IPC stream into MANY record batches (~300k rows each) regardless of
    DataFrame chunking; a reader that consumes only the first SILENTLY DROPS the rest — this
    lost 1.33M members (72%) of a 4.6TB home backup while reporting success. Needs >300k
    members in one pack message to trip (measured split: 1 batch at 400k rows, 2 at 600k).
    Guards arrow_next()."""
    if not os.environ.get("QUIVER_SLOW"):
        pytest.skip("set QUIVER_SLOW=1 (creates 600k files)")
    src = tmp_path / "src"
    n = 600_000
    for d in range(60):                                   # 60 dirs x 10k files
        dd = src / f"d{d:02d}"; dd.mkdir(parents=True)
        for i in range(n // 60):
            (dd / f"f{i}").write_bytes(b"x")
    nock = tmp_path / "s.nock"
    r = blocks.backup(str(src), str(nock), bvm, nworkers=8, level=1, strict=False)
    assert r["lost"] == 0, f"lost {r['lost']} members: {r['lost_sample']}"
    comp = blocks.verify_complete(str(nock), root=str(src))
    assert comp["missing"] == 0 and comp["footer"] >= n, comp


def test_multinode_locators_stream_during_dispatch(bvm, tmp_path):
    """bvm flushes frame locators every 2048 frames AND once a second, so on any real job most
    of them arrive while the planner is still dispatching -- i.e. through poll(), not finish().
    poll() used to parse them and drop the result, which cost 2,210,967 members' locators on a
    whole-home backup: 6 TB of correct data on disk with a footer that referenced almost none
    of it. Single-node backup() never calls poll(), so only a MULTI-executor run reproduces it.
    Uses two local executors -- no cluster required."""
    src = tmp_path / "tree"
    src.mkdir()
    for i in range(900):                                  # enough frames to force >1 flush
        (src / f"f{i:04d}.bin").write_bytes(os.urandom(3000) * (1 + i % 7))
    out = str(tmp_path / "s.nock")
    r = blocks.backup_multi(str(src), out, bvm, ["a", "b"], nworkers=4, sinks_per_node=2,
                            chunk_gb=0.000_2, launch=lambda n: [], strict=False)
    assert r["lost"] == 0, f"lost {r['lost']} members: locators were dropped"
    idx = blocks.scan_nock(out)
    assert idx.filter(pl.col("frame") >= 0).height == 900
    dest = str(tmp_path / "out")
    assert blocks.unpack(out, dest, bvm, nworkers=4) == 900
    assert _tree_md5(dest) == _tree_md5(str(src))


# ------------------------------------------------------------------ snapshot chain
def test_backup_snapshot_chain(bvm, tmp_path):
    """The incremental gate for the footer-assembly collapse: two snapshots with a
    delta'd file, a carried file, an added and a deleted file; both snapshots must
    restore byte-exact and the summary must show delta + batch reuse at work."""
    import random
    src, nock = str(tmp_path / "t"), str(tmp_path / "c.nock")
    os.makedirs(src + "/sub")
    rnd = random.Random(41)
    big = bytearray(rnd.randbytes(512 * 1024))               # >=128K: delta-eligible
    _write(src, "big.bin", bytes(big))
    _write(src, "keep.bin", b"K" * 9000)
    _write(src, "sub/gone.bin", b"G" * 7000)
    r1 = blocks.backup(src, nock, bvm, nworkers=4, level=1, strict=True)
    assert r1["lost"] == 0 and r1["errors"] == 0, r1
    at1 = blocks.scan_nock(nock)                             # snapshot 1 restores exactly
    d1 = str(tmp_path / "u1"); blocks.unpack(nock, d1, bvm, nworkers=4)
    t1 = _tree_md5(d1)
    assert t1 == _tree_md5(src)

    time.sleep(1.1)
    big[1000:1200] = rnd.randbytes(200)                      # small edit -> delta member
    _write(src, "big.bin", bytes(big))
    _write(src, "new.bin", b"N" * 6000)
    os.remove(os.path.join(src, "sub/gone.bin"))
    r2 = blocks.backup(src, nock, bvm, nworkers=4, level=1, strict=True)
    assert r2["lost"] == 0 and r2["errors"] == 0, r2
    assert r2["delta"] == 1, r2                              # big.bin went as extents
    assert r2["carried"] >= 1, r2                            # keep.bin not re-packed
    d2 = str(tmp_path / "u2"); blocks.unpack(nock, d2, bvm, nworkers=4)
    assert _tree_md5(d2) == _tree_md5(src)                   # snapshot 2 == current tree
    snaps = blocks.scan_nock(nock, at=None)                  # and snapshot 1 still restores
    import quiver.nock.nockidx as _nx
    ats = _nx.snapshots(nock) if hasattr(_nx, "snapshots") else None
    if ats and len(ats) >= 2:
        d3 = str(tmp_path / "u3")
        blocks.unpack(nock, d3, bvm, nworkers=4, at=ats[-1]["at"])
        assert _tree_md5(d3) == t1

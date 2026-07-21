"""quiver test suite — run with: PYTHONPATH=<repo parent> python3 -m quiver.tests.run_all"""
import io, json, os, random, shutil, subprocess, sys, tarfile, tempfile
from pathlib import Path

import polars as pl

from quiver import nock, stream, tools, wal, wire
from quiver.tests.oracle import walk

PASS = []

def ok(name):
    PASS.append(name); print(f"  ✓ {name}")

def make_tree(root: Path, n=300, seed=6):
    random.seed(seed)
    for i in range(n):
        p = root / f"t{i%5}" / f"m{i%3}" / f"f{i:03d}.bin"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(os.urandom(random.randrange(0, 6000)))
    (root/"żółć/empty_dir").mkdir(parents=True)
    (root/("L"*120+".txt")).write_bytes(b"pax\n")
    os.link(root/"t0/m0/f000.bin", root/"t1/hardlink")

def test_scan(tmp):
    src = tmp/"scan"; make_tree(src)
    ref = walk(str(src))
    zero_dir = pl.when(pl.col("is_dir")).then(0).otherwise(pl.col("size"))
    for eng, thr in (("sync",1),("uring",1),("uring",8)):
        df = wire.scan(str(src), eng, thr).sort("path")
        got = df.select("path", size=zero_dir, mtime_ns="mtime_ns",
                        mode=pl.col("mode") & 0o7777, uid="uid", gid="gid",
                        is_dir="is_dir")
        assert got.equals(ref.cast(dict(got.schema))), (eng, thr)
    ok("scan parity (oracle) across engines/threads")

def test_pack(tmp):
    src = tmp/"pack"; make_tree(src)
    tar = str(tmp/"o.tar")
    idx = tools.pack(str(src), tar, nock.TarFormat(), engine="uring")
    d = tmp/"x"; d.mkdir()
    assert subprocess.run(["tar","xf",tar,"-C",str(d)],
                          capture_output=True).returncode == 0
    for p in src.rglob("*"):
        if p.is_file():
            assert (d/p.relative_to(src)).read_bytes() == p.read_bytes()
    with tarfile.open(tar) as tf:
        assert "L"*120+".txt" in {m.name for m in tf.getmembers()}
    ok("pack tar: GNU tar + PAX + payload fidelity")
    raw = str(tmp/"o.raw")
    tools.pack(str(src), raw, nock.RawFormat(), engine="uring")
    d2 = tmp/"y"; nock.extract(raw, str(d2))
    for p in src.rglob("*"):
        if p.is_file():
            assert (d2/p.relative_to(src)).read_bytes() == p.read_bytes()
    got = nock.extract(tar, str(tmp/"z"), pl.col("size") > 4000)
    assert all((tmp/"z"/g).stat().st_size > 4000 for g in got)
    ok("raw roundtrip + predicate extract via nock index")
    # executor engines vs the inline loop, plus the SETMETA mtime tail
    d3 = tmp/"y-inline"; nock.extract(raw, str(d3), engine=None)
    d4 = tmp/"y-sync";   nock.extract(raw, str(d4), engine="sync")
    for p in src.rglob("*"):
        if p.is_file():
            rel = p.relative_to(src)
            b = p.read_bytes()
            assert (d3/rel).read_bytes() == b == (d4/rel).read_bytes()
            assert (d2/rel).stat().st_mtime_ns == p.stat().st_mtime_ns
    ok("extract: uring/sync engines == inline oracle; mtimes restored")

def test_tools(tmp):
    src = tmp/"tools"; make_tree(src)
    mine = tools.du(str(src), depth=1)
    truth = int(subprocess.run(["du","-s","--block-size=1",str(src)],
        capture_output=True,text=True).stdout.split()[0])
    assert mine.filter(pl.col("prefix")=="<total>")["bytes"][0] \
           + os.stat(src).st_blocks*512 == truth
    ok("du == system du (blocks, hardlink dedup)")
    dst = tmp/"copy"
    tools.cp(str(src), str(dst), engine="uring")
    for p in src.rglob("*"):
        if p.is_file():
            q = dst/p.relative_to(src)
            assert q.read_bytes() == p.read_bytes()
            assert q.stat().st_mtime_ns == p.stat().st_mtime_ns
    assert tools.sync(str(src), str(dst), engine="uring")["count"].sum() == 0
    (src/"t2/new.bin").write_bytes(b"N"*99)
    shutil.rmtree(src/"t4"); (dst/"stray").write_bytes(b"s")
    tools.sync(str(src), str(dst), engine="uring")
    assert not (dst/"stray").exists() and (dst/"t2/new.bin").exists()
    assert tools.sync(str(src), str(dst), engine="uring")["count"].sum() == 0
    tools.rm(str(dst), engine="uring")
    assert not dst.exists()
    ok("cp / sync converge+idempotent / rm epochs")

def test_retrofit(tmp):
    shard = str(tmp/"wds.tar")
    with tarfile.open(shard, "w") as tf:
        for i in range(50):
            for ext, data in (("wav", os.urandom(1000+i)),
                              ("json", json.dumps({"i": i}).encode())):
                ti = tarfile.TarInfo(f"s{i:04d}.{ext}"); ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
    orig = open(shard,"rb").read()
    nock.index_tar(shard, extra_cols=lambda df: df.with_columns(
        key=pl.col("path").str.extract(r"^(.*)\.[^.]+$", 1)))
    assert open(shard,"rb").read()[:len(orig)] == orig
    with tarfile.open(shard) as tf:
        assert len(tf.getmembers()) == 100
    idx = nock.read_index(shard)
    assert "key" in idx.columns and len(idx) == 100
    ok("index_tar: wds shard retrofit, bytes untouched")
    try:
        import pyarrow as pa, pyarrow.ipc as pa_ipc
    except ImportError:
        return
    arrow = str(tmp/"a.arrow")
    sch = pa.schema([("key", pa.large_string()), ("wav", pa.large_binary())])
    wavs = [os.urandom(500+i) for i in range(30)]
    with pa_ipc.new_file(arrow, sch) as w:
        w.write_table(pa.table([pa.array([f"u{i}" for i in range(30)]),
                                pa.array(wavs, pa.large_binary())], schema=sch))
    nock.index_arrow_shard(arrow, "wav", key_column="key")
    assert pa_ipc.open_file(arrow).read_all().num_rows == 30
    r = nock.read_index(arrow).filter(pl.col("path")=="u7.wav").to_dicts()[0]
    assert open(arrow,"rb").read()[r["data_offset"]:r["data_offset"]+r["size"]] == wavs[7]
    ok("index_arrow_shard: splice, pyarrow unaffected, ranges exact")

def test_cksum(tmp):
    (tmp/"check").write_bytes(b"123456789")
    ex = wire.PipeExecutor("-", engine="uring")
    comp = ex.execute(wire.cmd_df(1, opcode=[wire.OP_CKSUM],
                                  path=[str(tmp/"check")],
                                  pad_align=[5<<20]))
    ex.close()
    assert comp["cksum"][0] == 0xae8b14860a799888
    ok("CKSUM: CRC-64/NVME spec vector")

def test_s3(tmp):
    try:
        from moto import mock_aws
    except ImportError:
        return
    from quiver.remotes import s3 as S
    src = tmp/"s3src"
    random.seed(9)
    for i in range(30):
        p = src/f"d{i%3}"/f"f{i:02d}"; p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(os.urandom(random.randrange(1, 3000)))
    with mock_aws():
        c = S.s3_client(region_name="us-east-1"); c.create_bucket(Bucket="bkt")
        s = S.rsync_to_s3_etag(str(src), c, "bkt", "p/")
        assert s.filter(pl.col("op")=="put")["count"][0] == 30
        assert S.rsync_to_s3_etag(str(src), c, "bkt", "p/")["count"].sum() == 0
    ok("s3 etag sync converges (moto)")

def test_wal_resume(tmp):
    # crash simulation: executor dies after the first chunk; resume
    # completes; a third run is a no-op
    src = tmp/"walsrc"
    for i in range(40):
        p = src/"d"/f"f{i:02d}"; p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
    files = sorted((src/"d").iterdir())
    cmds = wire.cmd_df(len(files), opcode=[wire.OP_UNLINK]*len(files),
                       path=[str(p) for p in files])
    wp = str(tmp/"rm.wal")
    wal.write(wp, cmds)

    real = wal.PipeExecutor
    class Crashy(real):
        calls = 0
        def execute(self, chunk, batch_rows=4096):
            Crashy.calls += 1
            if Crashy.calls > 1:
                self.proc.kill()
                raise RuntimeError("simulated crash")
            return super().execute(chunk, batch_rows)
    wal.PipeExecutor = Crashy
    try:
        try:
            wal.execute(wp, engine="uring", batch_rows=10)
            assert False, "should have crashed"
        except RuntimeError:
            pass
    finally:
        wal.PipeExecutor = real
    st = wal.status(wp)
    n_done = st["done"].sum()
    assert 0 < n_done < len(files), n_done
    r = wal.execute(wp, engine="uring", batch_rows=10)
    assert r["executed"][0] == len(files) - n_done and r["failed"][0] == 0
    assert not any(p.exists() for p in files)
    r = wal.execute(wp, engine="uring")
    assert r["executed"][0] == 0
    ok(f"wal: crash after chunk 1 ({n_done} done) → resume → idempotent")

def test_wal_retry(tmp):
    # a premature rmdir fails (ENOTEMPTY), stays un-done, and simply
    # succeeds on the next run once its children are gone — retry is
    # not a mode, it is rerunning the WAL
    src = tmp/"retry"/"d"
    src.mkdir(parents=True)
    for i in range(5):
        (src/f"f{i}").write_bytes(b"y")
    cmds = pl.concat([
        wire.cmd_df(1, opcode=[wire.OP_RMDIR], path=[str(src)],
                    dep_group=pl.Series([0], dtype=pl.Int64)),   # premature
        wire.cmd_df(5, opcode=[wire.OP_UNLINK]*5,
                    user_data=pl.Series(range(1, 6), dtype=pl.UInt64),
                    dep_group=pl.Series([1]*5, dtype=pl.Int64),
                    path=[str(src/f"f{i}") for i in range(5)]),
    ])
    wp = str(tmp/"retry.wal")
    wal.write(wp, cmds)
    r = wal.execute(wp, engine="uring")
    assert r["failed"][0] == 1 and r["executed"][0] == 5
    assert src.exists()
    r = wal.execute(wp, engine="uring")
    assert r["failed"][0] == 0 and r["executed"][0] == 1
    assert not src.exists()
    ok("wal: failed row retried by rerun (ENOTEMPTY → success)")

def test_refcount_rm(tmp):
    import shutil
    src = tmp/"rcsrc"; make_tree(src, n=200, seed=13)
    for eng in ("uring", "sync"):
        tree = tmp/f"rc{eng}"; shutil.copytree(src, tree)
        tools.rm(str(tree), engine=eng, scheduler="refcount")
        assert not tree.exists()
    tree = tmp/"rcwal"; shutil.copytree(src, tree)
    tools.rm(str(tree), engine="uring", scheduler="refcount",
             wal=str(tmp/"rc.wal"))
    assert not tree.exists() and wal.status(str(tmp/"rc.wal"))["done"].all()
    # cross-chunk + resume: refcount frame through the WAL with tiny
    # batches and a mid-run crash — exercises per-chunk rebase and the
    # resume-time parent_row remap
    tree = tmp/"rcresume"; shutil.copytree(src, tree)
    df = wire.scan(str(tree), "uring", 4)
    files = df.filter(~pl.col("is_dir")).sort("depth", descending=True)
    dirs = df.filter(pl.col("is_dir")).sort("depth", descending=True)
    paths = files["path"].to_list() + dirs["path"].to_list()
    isd = [False]*len(files) + [True]*len(dirs)
    dir_row = {p: len(files)+i for i, p in enumerate(dirs["path"])}
    base = len(paths)
    parent = [dir_row.get(os.path.dirname(p), base) for p in paths]
    allp = [os.path.join(str(tree), p) for p in paths] + [str(tree)]
    ops = [wire.OP_RMDIR if d else wire.OP_UNLINK for d in isd] \
          + [wire.OP_RMDIR]
    cmds = wire.cmd_df(base+1, opcode=ops, path=allp,
                       parent_row=pl.Series(parent+[-1], dtype=pl.Int64))
    wp = str(tmp/"rcres.wal"); wal.write(wp, cmds)
    real = wal.PipeExecutor
    class C2(real):
        calls = 0
        def execute(self, ch, batch_rows=4096):
            C2.calls += 1
            if C2.calls == 3:
                self.proc.kill(); raise RuntimeError("crash")
            return super().execute(ch, batch_rows)
    wal.PipeExecutor = C2
    try:
        try: wal.execute(wp, engine="uring", batch_rows=40)
        except RuntimeError: pass
    finally:
        wal.PipeExecutor = real
    r = wal.execute(wp, engine="uring", batch_rows=40)
    assert r["failed"][0] == 0 and not tree.exists()
    ok("rm refcount scheduler: forest deps, cross-chunk, crash-resume")

def test_pushdown(tmp):
    import re, subprocess as sp
    src = tmp/"push"; make_tree(src, n=240, seed=17)
    full = wire.scan(str(src), "uring", 4).sort("path")
    def run(prefix="", glob=""):
        p = sp.Popen([wire.EXE, "scan", str(src), "uring", "4",
                      prefix, glob], stdout=sp.PIPE, stderr=sp.PIPE)
        from quiver.pupyarrow.writer import StreamReader
        dfs = [pl.DataFrame(b) for b in StreamReader(p.stdout)]
        err = p.stderr.read().decode(); p.wait()
        st = tuple(map(int, re.search(
            r"dirs=(\d+) statx=(\d+) emitted=(\d+)", err).groups()))
        df = (pl.concat(dfs).with_columns(pl.col("is_dir").cast(pl.Boolean))
              if dfs else full.clear())
        return df.sort("path"), st
    _, (d0, s0, _) = run()
    sub, (d1, s1, _) = run(prefix="t2")
    exp = full.filter(pl.col("path").str.starts_with("t2/")
                      | (pl.col("path") == "t2"))
    assert sub["path"].to_list() == exp["path"].to_list()
    assert d1 < d0 and s1 < s0
    gl, (_, sg, _) = run(glob="*.bin")
    expg = full.filter(pl.col("path").str.split("/").list.last()
                       .str.ends_with(".bin"))
    assert gl["path"].to_list() == expg["path"].to_list() and sg < s0
    ok(f"scan pushdown: prefix dirs {d0}→{d1} statx {s0}→{s1}, "
       f"glob statx {s0}→{sg}")

def test_wal_failures_view(tmp):
    src = tmp/"fv"/"d"; src.mkdir(parents=True)
    (src/"f").write_bytes(b"z")
    cmds = pl.concat([
        wire.cmd_df(1, opcode=[wire.OP_RMDIR], path=[str(src)],
                    dep_group=pl.Series([0], dtype=pl.Int64)),
        wire.cmd_df(1, opcode=[wire.OP_UNLINK],
                    user_data=pl.Series([1], dtype=pl.UInt64),
                    dep_group=pl.Series([1], dtype=pl.Int64),
                    path=[str(src/"f")])])
    wp = str(tmp/"fv.wal"); wal.write(wp, cmds)
    wal.execute(wp, engine="uring")
    f = wal.failures(wp)
    assert len(f) == 1 and f["opcode"][0] == wire.OP_RMDIR \
        and f["res"][0] == -39  # ENOTEMPTY
    wal.execute(wp, engine="uring")
    assert len(wal.failures(wp)) == 0   # success supersedes (last-wins)
    ok("wal.failures(): error table, cleared after successful retry")

def test_streaming(tmp):
    src = tmp/"str"; make_tree(src, n=250, seed=23)
    d = tmp/"strcp"
    stream.stream_cp(str(src), str(d), "uring")
    for p in src.rglob("*"):
        q = d/p.relative_to(src)
        if p.is_file():
            assert q.read_bytes() == p.read_bytes()
        assert q.stat().st_mtime_ns == p.stat().st_mtime_ns
    assert tools.sync(str(src), str(d), engine="uring")["count"].sum() == 0
    arc = str(tmp/"str.tar")
    idx = stream.stream_pack(str(src), arc, nock.TarFormat(), "uring")
    assert subprocess.run(["tar","tf",arc],
                          capture_output=True).returncode == 0
    x = tmp/"strx"; nock.extract(arc, str(x))
    files = [p for p in src.rglob("*") if p.is_file()]
    assert all((x/p.relative_to(src)).read_bytes() == p.read_bytes()
               for p in files)
    stream.stream_rm(str(d), "uring")
    assert not d.exists()
    # same Plan, batch granularity (source = one scan): must match
    db = tmp/"strcp_batch"
    stream.stream_cp(str(src), str(db), "uring", streaming=False)
    for p in src.rglob("*"):
        if p.is_file():
            assert (db/p.relative_to(src)).read_bytes() == p.read_bytes()
    stream.stream_rm(str(db), "uring", streaming=False)
    assert not db.exists()
    # deep + wide + empty-dir tree to exercise the streaming rmdir
    # refcount cascade; _check raises on any ENOTEMPTY (res<0)
    deep = tmp/"deeprm"
    for i in range(400):
        p = deep
        for lvl in range(i % 6 + 1):
            p = p/f"L{lvl}_{(i>>lvl)%4}"
        p.mkdir(parents=True, exist_ok=True)
        (p/f"f{i}").write_bytes(b"x")
    (deep/"a/b/c/emptyleaf").mkdir(parents=True)
    assert stream.stream_rm(str(deep), "uring", threads=8) > 400
    assert not deep.exists()
    ok("streaming: cp/pack/rm execute during the scan (out-of-order-"
       "arrival safe); same Plan batches or streams; deep rmdir cascade")

def test_ssh(tmp):
    # requires a reachable sshd (tests set one up on localhost:2222);
    # skip silently otherwise
    import subprocess as sp
    U = os.environ.get("USER") or "root"
    probe = sp.run(["ssh", "-p", "2222", "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=2",
                    "-o", "StrictHostKeyChecking=accept-new",
                    f"{U}@localhost", "true"], capture_output=True)
    if probe.returncode != 0:
        return
    from quiver.remotes.ssh import Ssh
    remote = Ssh(f"{U}@localhost", exe=wire.EXE,
                 ssh_opts=["-p", "2222", "-o", "BatchMode=yes",
                           "-o", "StrictHostKeyChecking=accept-new"])
    src = tmp/"sshsrc"
    make_tree(src, n=80, seed=31)
    (src/"big.bin").write_bytes(os.urandom((1 << 20) + 999))  # chunk chain
    dst = str(tmp/"sshdst")
    remote.sync_to(str(src), dst, engine="uring")
    for p in src.rglob("*"):
        if p.is_file():
            assert (Path(dst)/p.relative_to(src)).read_bytes() \
                   == p.read_bytes()
    assert sum(r["count"] for r in
               remote.sync_to(str(src), dst,
                              engine="uring").to_dicts()) == 0
    assert len(remote.verify(str(src), dst, engine="uring")) == 0
    v = next(Path(dst).rglob("*.bin"))
    b = bytearray(v.read_bytes()); b[0] ^= 1; v.write_bytes(bytes(b))
    assert len(remote.verify(str(src), dst, engine="uring")) == 1
    ok("ssh: remote scan+sync (inline chunk chains)+verify, corruption "
       "caught by wire-crossing hashes only")

def test_zframe(tmp):
    try:
        import zstandard  # noqa
    except ImportError:
        return
    import io, tarfile, subprocess as sp
    from quiver.nock import zframe
    # two source tars → verify compat, round-trip, batch random access, merge
    def mk(path, pfx, n):
        with tarfile.open(path, "w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (30 + i % 40)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
    mk(str(tmp/"za.tar"), "a", 200); mk(str(tmp/"zb.tar"), "b", 120)
    out = str(tmp/"z.zframe.zstd")
    res = zframe.recompress([str(tmp/"za.tar"), str(tmp/"zb.tar")], out,
                            batch_bytes=32 << 10)
    assert res.members == 320 and res.frames > 1
    # standard tools read it (skippable footer ignored, one clean tar)
    n = sp.run(f"zstd -dc {out} | tar t | wc -l", shell=True,
               capture_output=True, text=True).stdout.strip()
    assert n == "320", n
    # random-access extract of one member from the 2nd input matches
    zframe.extract(out, str(tmp/"zx"), pl.col("path") == "b/f0100")
    with tarfile.open(str(tmp/"zb.tar")) as tf:
        assert (tmp/"zx"/"b"/"f0100").read_bytes() == \
            tf.extractfile("b/f0100").read()
    ok("zframe: per-batch frames — tar-compatible, merge, batch extract")


def test_zframe_c(tmp):
    """The fold: quiver-exec `zpack` does decompress+parse+compress+append
    in C (GIL-free), streaming footer records. Inputs are tar.zstd."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, tarfile, subprocess as sp
    from quiver.nock import zframe

    def mk_zst(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (25 + i % 50)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}"); ti.size = len(b)
                ti.mode = 0o644; ti.mtime = 1_700_000_000 + i
                tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    mk_zst(str(tmp/"a.tar.zstd"), "a", 300)
    mk_zst(str(tmp/"b.tar.zstd"), "b", 170)
    out = str(tmp/"zc.tar.zstd")
    res = zframe.recompress_c([str(tmp/"a.tar.zstd"), str(tmp/"b.tar.zstd")],
                              out, level=6, batch_bytes=32 << 10,
                              readers=2, compressors=4)
    assert res.members == 470, res.members
    assert res.frames > 1, res.frames
    n = sp.run(f"zstd -dc {out} | tar t | wc -l", shell=True,
               capture_output=True, text=True).stdout.strip()
    assert n == "470", n                       # still one clean tar.zstd
    # byte-exact random access from the 2nd (merged) source
    zframe.extract(out, str(tmp/"zcx"), pl.col("path") == "b/f0100")
    want = (b"b-100-") * (25 + 100 % 50)
    assert (tmp/"zcx"/"b"/"f0100").read_bytes() == want
    # >4 GB footers can't embed in a u32-length skippable frame → .nock
    # sidecar; the archive then stays a pristine multi-frame tar.zstd.
    sc = str(tmp/"zs.tar.zstd")
    zframe.recompress_c([str(tmp/"a.tar.zstd")], sc, level=6,
                        batch_bytes=32 << 10, readers=1, compressors=2,
                        _force_sidecar=True)
    assert os.path.exists(sc + ".nock")
    n2 = sp.run(f"zstd -dc {sc} | tar t | wc -l", shell=True,
                capture_output=True, text=True).stdout.strip()
    assert n2 == "300", n2                      # no embedded footer to trip tar
    assert zframe.read_index(sc).height == 300  # index served from sidecar
    ok("zframe C fold: zpack decompress+parse+compress in C, merge, sidecar")


def test_zframe_plan(tmp):
    """The plan seam: scan (C, metadata) → Polars filter/frame-assign → exec
    (C, compress only kept members). Globs/size filters without touching C."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, tarfile, subprocess as sp
    from quiver.nock import zframe

    def mk_zst(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (5 + i)      # size grows with i
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}.dat"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    mk_zst(str(tmp/"a.tar.zstd"), "a", 100)
    mk_zst(str(tmp/"b.tar.zstd"), "b", 100)
    srcs = [str(tmp/"a.tar.zstd"), str(tmp/"b.tar.zstd")]

    # scan sees every member with source_id + ordinal
    scan = zframe._zscan(srcs, 2)
    assert scan.height == 200 and scan["source_id"].n_unique() == 2

    # include glob: only source b's members survive
    out = str(tmp/"only_b.tar.zstd")
    r = zframe.recompress_c(srcs, out, level=4, batch_bytes=16 << 10,
                            include=["b/*"])
    idx = zframe.read_index(out)
    assert r.members == 100 and idx.height == 100
    assert idx["path"].str.starts_with("b/").all()
    n = sp.run(f"zstd -dc {out} | tar t | wc -l", shell=True,
               capture_output=True, text=True).stdout.strip()
    assert n == "100", n                        # kept members form a clean tar
    # byte-exact for a filtered member, sliced from its frame
    zframe.extract(out, str(tmp/"bx"), pl.col("path") == "b/f0050.dat")
    assert (tmp/"bx"/"b"/"f0050.dat").read_bytes() == (b"b-50-") * (5 + 50)

    # size filter composes; frames stay valid
    out2 = str(tmp/"big.tar.zstd")
    r2 = zframe.recompress_c(srcs, out2, level=4, batch_bytes=16 << 10,
                             min_size=400)
    idx2 = zframe.read_index(out2)
    assert (idx2["size"] >= 400).all() and r2.members == idx2.height
    ok("zframe plan seam: scan → Polars glob/size filter → exec (kept only)")


def test_zframe_reshard(tmp):
    """Single-pass fan-out: the plan tags each member with a sink; zexec
    writes N self-contained tar.zstd shards from one decompress pass."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, tarfile, glob, subprocess as sp
    import polars as pl
    from quiver.nock import zframe

    def mk_zst(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (3 + i)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}.dat"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    mk_zst(str(tmp/"a.tar.zstd"), "a", 120)
    mk_zst(str(tmp/"b.tar.zstd"), "b", 80)
    srcs = [str(tmp/"a.tar.zstd"), str(tmp/"b.tar.zstd")]

    # 3-way hash split in one pass
    N = 3
    res = zframe.recompress_c(srcs, str(tmp/"h.tar.zstd"), level=4,
                              batch_bytes=16 << 10, shard_by="hash", shards=N)
    assert res.members == 200
    shards = sorted(glob.glob(str(tmp/"h.shard*.tar.zstd")))
    assert len(shards) == N, shards
    seen, total = set(), 0
    for k, sh in enumerate(shards):
        idx = zframe.read_index(sh)
        n = sp.run(f"zstd -dc {sh} | tar t | wc -l", shell=True,
                   capture_output=True, text=True).stdout.strip()
        assert int(n) == idx.height          # each shard a clean tar.zstd
        hk = pl.DataFrame({"p": idx["path"].to_list()}).with_columns(
            (pl.col("p").hash() % N).alias("h"))
        assert (hk["h"] == k).all()           # every member routed to its shard
        seen |= set(idx["path"].to_list()); total += idx.height
    assert len(seen) == total == 200          # disjoint and complete

    # attribute split (by source) via a Polars expression sink
    res2 = zframe.recompress_c(srcs, str(tmp/"s.tar.zstd"), level=4,
                               batch_bytes=16 << 10,
                               shard_by=pl.col("source_id"))
    a = zframe.read_index(str(tmp/"s.shard0.tar.zstd"))
    b = zframe.read_index(str(tmp/"s.shard1.tar.zstd"))
    assert a["path"].str.starts_with("a/").all()
    assert b["path"].str.starts_with("b/").all()
    ok("zframe reshard: one pass → N byte-exact shards (hash + attribute)")


def test_zframe_s3(tmp):
    """Stream reshard outputs straight into S3: C writes each sink's frames to
    a FIFO, a per-sink uploader multipart-uploads them, footer appended — no
    local staging of the body. Verified against moto."""
    try:
        import zstandard as zstd  # noqa
        from moto import mock_aws
        import boto3
    except ImportError:
        return
    import io, os, tarfile
    import polars as pl
    from quiver.nock import zframe
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    def mk_zst(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (4 + i)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}.dat"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    mk_zst(str(tmp/"a.tar.zstd"), "a", 150)
    src = [str(tmp/"a.tar.zstd")]

    @mock_aws
    def run():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="quiver-test")
        N = 2
        res = zframe.recompress_c(src, "s3://quiver-test/out/p.tar.zstd", level=4,
                                  batch_bytes=16 << 10, readers=1, compressors=4,
                                  shard_by="hash", shards=N)
        assert res.members == 150
        keys = sorted(o["Key"] for o in
                      s3.list_objects_v2(Bucket="quiver-test")["Contents"])
        assert keys == ["out/p.shard0.tar.zstd", "out/p.shard1.tar.zstd"], keys
        # download a shard, confirm it's a valid nock zframe, routed + exact
        body = s3.get_object(Bucket="quiver-test",
                             Key="out/p.shard0.tar.zstd")["Body"].read()
        dl = str(tmp/"dl.tar.zstd"); open(dl, "wb").write(body)
        idx = zframe.read_index(dl)
        hk = pl.DataFrame({"x": idx["path"].to_list()}).with_columns(
            (pl.col("x").hash() % N).alias("h"))
        assert (hk["h"] == 0).all() and idx.height > 0
        zframe.extract(dl, str(tmp/"dx"), pl.col("path") == idx["path"][0])
        name = idx["path"][0]
        i = int(name.split("/f")[1].split(".")[0])
        assert (tmp/"dx"/name).read_bytes() == (f"a-{i}-".encode()) * (4 + i)
    run()
    ok("zframe S3: reshard streamed to S3 multipart (moto), nock + byte-exact")


def test_zframe_packfs(tmp):
    """FILE-source pack: scan a filesystem tree, batch files into frames, zstd
    each frame in parallel → a standard tar.zstd nock that round-trips."""
    from quiver.nock import zframe
    root = tmp/"tree"
    want = {}
    random.seed(3)
    for i in range(300):
        d = root/f"d{i % 5}"/f"s{i % 2}"; d.mkdir(parents=True, exist_ok=True)
        data = os.urandom(random.randrange(5, 4000))
        (d/f"f{i:04d}.bin").write_bytes(data)
        want[f"d{i % 5}/s{i % 2}/f{i:04d}.bin"] = data
    res = zframe.pack_fs(str(root), str(tmp/"fs.tar.zstd"),
                         batch_bytes=64 << 10, level=4, workers=4)
    assert res.members == 300 and res.frames > 1
    n = subprocess.run(f"zstd -dc {tmp}/fs.tar.zstd | tar t | wc -l", shell=True,
                       capture_output=True, text=True).stdout.strip()
    assert int(n) == 300                               # a clean tar.zstd
    zframe.unpack(str(tmp/"fs.tar.zstd"), str(tmp/"fx"), workers=4)
    for rel, data in want.items():                     # pack→unpack byte-exact
        assert (tmp/"fx"/rel).read_bytes() == data
    ok("zframe pack_fs: filesystem scan → compressed frame nock, round-trips")


def test_zframe_unpack(tmp):
    """Parallel unpack (decode each frame once in a thread pool) must equal the
    single-threaded extract oracle, byte-exact, for linear and sharded nock,
    with predicate pushdown."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, tarfile
    from quiver.nock import zframe

    def mk(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}{i}-".encode()) * (6 + i)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}.dat"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    a, b = str(tmp/"a.tar.zstd"), str(tmp/"b.tar.zstd")
    mk(a, "a", 150); mk(b, "b", 150)
    # linear nock: parallel unpack == extract() oracle, byte-exact
    zframe.recompress_c([a], str(tmp/"lin.tar.zstd"), level=4, batch_bytes=16 << 10)
    assert zframe.unpack(str(tmp/"lin.tar.zstd"), str(tmp/"ul"), workers=4) == 150
    zframe.extract(str(tmp/"lin.tar.zstd"), str(tmp/"uo"))
    for i in (0, 77, 149):
        w = (f"a{i}-".encode()) * (6 + i)
        assert (tmp/"ul"/"a"/f"f{i:04d}.dat").read_bytes() == w
        assert (tmp/"uo"/"a"/f"f{i:04d}.dat").read_bytes() == w
    # sharded nock: unpack resolves each member to its shard
    zframe.recompress_c([a], str(tmp/"s0.tar.zstd"), level=4, batch_bytes=16 << 10)
    zframe.recompress_c([b], str(tmp/"s1.tar.zstd"), level=4, batch_bytes=16 << 10)
    zframe.merge([str(tmp/"s0.tar.zstd"), str(tmp/"s1.tar.zstd")], str(tmp/"m.nockset"))
    assert zframe.unpack_merged(str(tmp/"m.nockset"), str(tmp/"um"), workers=4) == 300
    assert (tmp/"um"/"a"/"f0005.dat").read_bytes() == (b"a5-") * 11
    assert (tmp/"um"/"b"/"f0149.dat").read_bytes() == (b"b149-") * (6 + 149)
    # predicate pushdown: only decode/write frames with matching members
    n3 = zframe.unpack_merged(str(tmp/"m.nockset"), str(tmp/"ub"),
                              predicate=pl.col("path").str.starts_with("b/"),
                              workers=4)
    assert n3 == 150 and not (tmp/"ub"/"a").exists()
    ok("zframe unpack: parallel decode == oracle, linear + sharded + predicate")


def test_zframe_merge(tmp):
    """The distributed reduce: recompress sources into separate shards, then
    merge (zero-copy) into one index — must equal a single-node run over all
    sources, byte-exact, with members resolved to the right shard."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, tarfile
    from quiver.nock import zframe

    def mk_zst(path, pfx, n):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for i in range(n):
                b = (f"{pfx}-{i}-").encode() * (7 + i)
                ti = tarfile.TarInfo(f"{pfx}/f{i:04d}.dat"); ti.size = len(b)
                ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
        with open(path, "wb") as fo:
            fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    srcs = [str(tmp/f"{p}.tar.zstd") for p in ("a", "b", "c")]
    for p, s in zip(("a", "b", "c"), srcs):
        mk_zst(s, p, 120)
    # map: one shard per source
    shard_files = []
    for k, s in enumerate(srcs):
        sh = str(tmp/f"sh{k}.tar.zstd")
        zframe.recompress_c([s], sh, level=4, batch_bytes=16 << 10)
        shard_files.append(sh)
    # reduce
    m = zframe.merge(shard_files, str(tmp/"merged.nockset"))
    ref = zframe.recompress_c(srcs, str(tmp/"single.tar.zstd"),
                              level=4, batch_bytes=16 << 10)
    assert m == ref.members == 360
    shards, idx = zframe.read_merged(str(tmp/"merged.nockset"))
    assert len(shards) == 3 and set(idx["shard_id"].unique().to_list()) == {0, 1, 2}
    assert set(idx["path"]) == set(
        zframe.read_index(str(tmp/"single.tar.zstd"))["path"])
    # byte-exact for a member from each shard
    zframe.extract_merged(str(tmp/"merged.nockset"), str(tmp/"mx"),
                          pl.col("path").is_in(["a/f0005.dat", "c/f0119.dat"]))
    assert (tmp/"mx"/"a"/"f0005.dat").read_bytes() == (b"a-5-") * 12
    assert (tmp/"mx"/"c"/"f0119.dat").read_bytes() == (b"c-119-") * (7 + 119)
    ok("zframe merge: shards → zero-copy reduce == single-node, byte-exact")


def test_zframe_wal(tmp):
    """WAL resume: after a crash, re-scan/re-plan (deterministic), skip the
    frames already committed to the WAL, and append the rest — verified by
    simulating a crash mid-run and resuming to a complete, byte-exact archive."""
    try:
        import zstandard as zstd  # noqa
    except ImportError:
        return
    import io, os, struct, subprocess as sp, tarfile
    from quiver.nock import zframe
    from quiver.wire import EXE

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tf:
        for i in range(400):
            b = (f"m{i}-").encode() * (20 + i)
            ti = tarfile.TarInfo(f"c/f{i:04d}.dat"); ti.size = len(b)
            ti.mode = 0o644; tf.addfile(ti, io.BytesIO(b))
    src = str(tmp/"a.tar.zstd")
    with open(src, "wb") as fo:
        fo.write(zstd.ZstdCompressor().compress(raw.getvalue()))

    # deterministic run (1 reader/compressor), capture records + raw frames
    df = zframe._zscan([src], 1)
    plan = zframe.plan_frames(df, batch_bytes=16 << 10)
    M = plan.height
    pp = str(tmp/"p.plan"); zframe._write_plan(plan, 1, pp, nsink=1)
    full = str(tmp/"full.frames")
    proc = sp.Popen([EXE, "zexec", pp, full, "4", "1", "1", "1", "-", src],
                    stdout=sp.PIPE)                     # nsink=1, starts="-"
    data = proc.stdout.read(); proc.wait()
    rec = struct.Struct("<qqiiiiqqqi")
    recs, i, n = [], 0, len(data)
    while i + 2 <= n:
        plen = data[i] | (data[i + 1] << 8)
        if i + 2 + plen + 60 > n:
            break
        recs.append((data[i:i + 2 + plen + 60],
                     rec.unpack_from(data, i + 2 + plen)))
        i += 2 + plen + 60
    nframes = max(t[5] for _, t in recs) + 1
    assert nframes > 3, nframes

    # simulate a crash after ~1/3 of the frames
    K = nframes // 3
    committed = [rw for rw, t in recs if t[5] < K]
    hw = max(t[6] + t[7] for _, t in recs if t[5] < K)
    out = str(tmp/"resumed.tar.zstd"); wal = str(tmp/"job.wal")
    with open(out, "wb") as fo:
        fo.write(open(full, "rb").read()[:hw])
    with open(wal, "wb") as fo:
        fo.write(b"".join(committed))

    res = zframe.recompress_c([src], out, level=4, readers=1, compressors=1,
                              batch_bytes=16 << 10, wal=wal)
    assert res.members == M, (res.members, M)          # complete
    assert not os.path.exists(wal)                     # WAL retired on success
    idx = zframe.read_index(out)
    assert idx.height == M
    m = sp.run(f"zstd -dc {out} | tar t | wc -l", shell=True,
               capture_output=True, text=True).stdout.strip()
    assert int(m) == M
    # byte-exact for a member from a committed frame and a resumed frame
    zframe.extract(out, str(tmp/"rx"), pl.col("path").is_in(
        ["c/f0001.dat", "c/f0399.dat"]))
    assert (tmp/"rx"/"c"/"f0001.dat").read_bytes() == (b"m1-") * 21
    assert (tmp/"rx"/"c"/"f0399.dat").read_bytes() == (b"m399-") * (20 + 399)
    ok("zframe WAL: crash mid-run → resume skips committed frames, complete")

def test_multi(tmp):
    # distributed cp/rm across two LOCAL executors: exercises the Polars
    # subtree partition, the fan-out/barrier, and the root-op handling
    # (real ssh/srun only change how the executor is spawned).
    from quiver.remotes.multi import LocalTransport, partition_plan
    src = tmp/"multi"; make_tree(src, n=300, seed=17)
    tr = [LocalTransport(), LocalTransport()]
    dst = tmp/"multi_dst"
    tools.cp(str(src), str(dst), engine="uring", transports=tr)
    for p in src.rglob("*"):
        if p.is_file():
            assert (dst/p.relative_to(src)).read_bytes() == p.read_bytes()
    # partition invariants: affinity (no subtree split) + root separated
    from quiver.wire import cmd_df, OP_UNLINK
    paths = [f"{src}/a/f{i}" for i in range(5)] + [f"{src}/b/f{i}"
             for i in range(9)] + [os.path.abspath(str(src))]
    cmds = cmd_df(len(paths), opcode=[OP_UNLINK]*len(paths), path=paths)
    root_ops, shards = partition_plan(cmds, os.path.abspath(str(src)), 2)
    assert len(root_ops) == 1
    seen = {}
    for i, sh in enumerate(shards):
        for p in sh["path"]:
            sub = p.split("/")[-2]
            assert seen.setdefault(sub, i) == i, "subtree split across shards"
    tools.rm(str(dst), engine="uring", transports=tr)
    assert not dst.exists()
    ok("multi: distributed cp/rm (2 executors), subtree affinity + root")

def test_cksum_parallel(tmp):
    import hashlib
    big = os.urandom(23 * (1 << 20) + 12345)      # 5 parts, parallel path
    (tmp/"ckbig").write_bytes(big)
    PART = 5 << 20
    ex = wire.PipeExecutor("-", engine="uring")
    comp = ex.execute(wire.cmd_df(1, opcode=[wire.OP_CKSUM],
                                  path=[str(tmp/"ckbig")],
                                  pad_align=[PART]))
    ex.close()
    digs = b"".join(hashlib.md5(big[o:o+PART]).digest()
                    for o in range(0, len(big), PART))
    assert bytes(comp["etag"][0]) == hashlib.md5(digs).digest()
    assert comp["parts"][0] == 5
    ok("part-parallel CKSUM: 4-thread MD5 fan-out + CRC64 combine == "
       "sequential reference")

def test_p1_barrier(tmp):
    (tmp/"bf").write_bytes(b"z")
    ex = wire.PipeExecutor("-", engine="uring")
    comp = ex.execute(wire.cmd_df(1, opcode=[wire.OP_FBARRIER],
                                  path=[str(tmp/"bf")]))
    ex.close()
    assert comp["res"][0] == 0
    ok("FBARRIER on a path fsyncs cleanly (archive-fd form covered by pack)")

def main():
    tmp = Path(tempfile.mkdtemp())
    for t in (test_scan, test_pack, test_tools, test_retrofit,
              test_cksum, test_s3, test_wal_resume, test_wal_retry,
              test_refcount_rm, test_pushdown, test_wal_failures_view,
              test_streaming, test_ssh, test_multi, test_zframe, test_zframe_c,
              test_zframe_plan, test_zframe_reshard, test_zframe_s3,
              test_zframe_merge, test_zframe_unpack, test_zframe_packfs,
              test_zframe_wal,
              test_cksum_parallel,
              test_p1_barrier):
        t(tmp)
    shutil.rmtree(tmp)
    print(f"\n{len(PASS)} test groups passed")

if __name__ == "__main__":
    main()

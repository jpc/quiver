"""End-to-end tests for the blocks-based `quiver` CLI (quiver/cli.py)."""
import io, os, sys, shutil, subprocess, tarfile, hashlib, time
import pytest
import zstandard


def _q(*args, cwd=None):
    r = subprocess.run([sys.executable, "-m", "quiver.cli", *map(str, args)],
                       capture_output=True, text=True, cwd=cwd,
                       env={**os.environ, "PYTHONPATH": _REPO})
    assert r.returncode == 0, f"quiver {args} failed:\n{r.stdout}\n{r.stderr}"
    return r.stdout + r.stderr


_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _tar_zstd(path, members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
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


def _members(n=60):
    return [(f"grp{i % 3}/f{i:04d}.bin", bytes(((i * 17 + j) & 255) for j in range(300 + i * 40)))
            for i in range(n)]


def test_cli_recompress_unpack_extract(bvm, tmp_path):
    src, nk = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock")
    truth = _tar_zstd(src, _members())
    out = _q("recompress", nk, src)
    assert f"{len(truth):,} members" in out
    assert "60 members" in _q("scan", nk)
    dest = str(tmp_path / "un")
    _q("unpack", nk, dest)
    assert _tree_md5(dest) == truth
    ex = str(tmp_path / "ex")                              # extract only grp0/*
    _q("extract", nk, ex, "--glob", "grp0/*")
    assert set(_tree_md5(ex)) == {k for k in truth if k.startswith("grp0/")}


def test_cli_pack_diff_sync(bvm, tmp_path):
    root = str(tmp_path / "tree")
    os.makedirs(root + "/sub")
    for i in range(20):
        open(f"{root}/sub/f{i}", "wb").write(bytes([i]) * (100 + i))
    nk = str(tmp_path / "p.nock")
    assert "'packed': 20" in _q("pack", root, nk)
    un = str(tmp_path / "un")
    _q("unpack", nk, un)
    assert _tree_md5(un) == _tree_md5(root)
    cpd = str(tmp_path / "cpd")                            # cp then diff -> identical
    _q("cp", root, cpd)
    d = _q("diff", root, cpd)
    assert "changed   0" in d and "unchanged 20" in d
    open(f"{cpd}/sub/f0", "wb").write(b"STALE")            # sync repairs it
    _q("sync", root, cpd, "-n", "3")
    assert _tree_md5(cpd) == _tree_md5(root)


def test_cli_find_du(bvm, tmp_path):
    root = str(tmp_path / "t")
    for d in range(4):
        for i in range(10):
            p = f"{root}/d{d}/f{i:02d}.bin"
            os.makedirs(os.path.dirname(p), exist_ok=True)
            open(p, "wb").write(b"x" * (1000 if i == 5 else 100))
    names = [l for l in _q("find", root, "-name", "f0*.bin").splitlines() if not l.startswith("#")]
    assert len(names) == 4 * 10                           # f00..f09 -> all match f0*
    big = [l for l in _q("find", root, "--min-size", "500").splitlines() if not l.startswith("#")]
    assert len(big) == 4                                  # the i==5 files, one per dir
    assert "40 files" in _q("du", root)


def test_cli_glob_set(bvm, tmp_path):
    truth = {}
    for a in range(3):                                    # 3 tars -> 3 separate nocks
        src = str(tmp_path / f"s{a}.tar.zstd")
        truth.update(_tar_zstd(src, [(f"set{a}/f{i:02d}", bytes([a * 50 + i]) * (100 + i))
                                     for i in range(15)]))
        _q("recompress", str(tmp_path / f"n{a}.nock"), src)
    pat = str(tmp_path / "n*.nock")                       # the glob IS the "nockset"
    out = _q("scan", pat)
    assert f"{len(truth)} members" in out and "across 3 nocks" in out
    dest = str(tmp_path / "un")
    assert "from 3 nock(s)" in _q("unpack", pat, dest)
    assert _tree_md5(dest) == truth                       # union of all 3 nocks


def test_cli_tar_compat(bvm, tmp_path):
    import polars as pl, zstandard, io, tarfile
    from quiver.exec import blocks
    src, nk = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock")
    truth = _tar_zstd(src, _members(30))
    _q("recompress", nk, src)                              # tar-compat is the default
    dest = str(tmp_path / "un")
    _q("unpack", nk, dest)
    assert _tree_md5(dest) == truth                        # blocks unpack still byte-exact
    # the data frames (everything before the footer) must concatenate into a valid tar
    idx = blocks.scan_nock(nk).filter(pl.col("frame") >= 0)
    dctx = zstandard.ZstdDecompressor()
    raw = bytearray()
    with open(nk, "rb") as f:
        for c, l in idx.select("coff", "clen").unique().sort("coff").iter_rows():
            f.seek(c); raw += dctx.decompress(f.read(l))
    got = {}
    with tarfile.open(fileobj=io.BytesIO(bytes(raw))) as tf:
        for m in tf:
            if m.isreg():
                got[m.name] = hashlib.md5(tf.extractfile(m).read()).hexdigest()
    assert got == truth                                    # data section IS a tar

    # --no-tar-headers keeps unpack working but the data is no longer a tar
    nk2 = str(tmp_path / "stripped.nock")
    _q("recompress", nk2, src, "--no-tar-headers")
    d2 = str(tmp_path / "un2")
    _q("unpack", nk2, d2)
    assert _tree_md5(d2) == truth
    assert os.path.getsize(nk2) < os.path.getsize(nk)      # smaller without headers


def _data_as_tar(nk):
    """Decode a nock's data frames and return {name: md5} read via tarfile."""
    import polars as pl, zstandard, io, tarfile
    from quiver.exec import blocks
    idx = blocks.scan_nock(nk).filter(pl.col("frame") >= 0)
    dctx = zstandard.ZstdDecompressor()
    raw = bytearray()
    with open(nk, "rb") as f:
        for c, l in idx.select("coff", "clen").unique().sort("coff").iter_rows():
            f.seek(c); raw += dctx.decompress(f.read(l))
    out = {}
    with tarfile.open(fileobj=io.BytesIO(bytes(raw))) as tf:
        for m in tf:
            if m.isreg():
                out[m.name] = hashlib.md5(tf.extractfile(m).read()).hexdigest()
    return out


def test_cli_pack_tar_compat(bvm, tmp_path):
    root = str(tmp_path / "tree")
    os.makedirs(root + "/sub")
    for i in range(25):
        open(f"{root}/sub/f{i:03d}", "wb").write(bytes([i]) * (100 + i * 7))
    long = root + "/" + "d/" * 20 + "longpath_member.bin"                 # >100 chars -> PAX
    os.makedirs(os.path.dirname(long)); open(long, "wb").write(b"L" * 900)
    nk = str(tmp_path / "p.nock")
    _q("pack", root, nk)                                                  # tar-compat default
    assert _data_as_tar(nk) == _tree_md5(root)                           # data section is a tar

    time.sleep(1.1)                                                      # incremental append
    open(f"{root}/sub/f000", "wb").write(b"CHANGED" * 40)
    open(f"{root}/added.bin", "wb").write(b"A" * 50)
    _q("pack", root, nk)                                                 # re-pack (overwrites old EOF frame)
    assert _data_as_tar(nk) == _tree_md5(root)                           # still a valid tar after append
    un = str(tmp_path / "un2")
    _q("unpack", nk, un)
    assert _tree_md5(un) == _tree_md5(root)


def test_cli_shards(bvm, tmp_path):
    import glob
    src = str(tmp_path / "s.tar.zstd")
    truth = _tar_zstd(src, [(f"g{i % 5}/f{i:04d}.bin", bytes([i & 255]) * (200 + i * 20))
                            for i in range(80)])
    out = str(tmp_path / "o.nock")
    o = _q("recompress", out, "--shards", "4", "--frame-mb", "0.03", src)
    assert "4 shards" in o
    assert len(glob.glob(out + ".*")) == 4                 # o.nock.0 .. o.nock.3
    pat = out + ".*"
    s = _q("scan", pat)
    assert f"{len(truth)} members" in s and "across 4 nocks" in s
    dest = str(tmp_path / "un")
    _q("unpack", pat, dest)
    assert _tree_md5(dest) == truth                        # union of shards == source


def test_cli_filter(bvm, tmp_path):
    members, keep = [], {}                                  # .keep/.drop, some small (<100B)
    for i in range(40):
        ext = "keep" if i % 2 == 0 else "drop"
        data = b"x" * (50 if i % 5 == 0 else 800)
        name = f"g{i % 3}/f{i:03d}.{ext}"
        members.append((name, data))
        if ext == "keep":
            keep[name] = hashlib.md5(data).hexdigest()
    src = str(tmp_path / "s.tar.zstd"); _tar_zstd(src, members)

    nk = str(tmp_path / "k.nock")                           # recompress: member filter (tar source)
    _q("recompress", nk, "--glob", "*.keep", "--frame-mb", "0.02", src)
    d = str(tmp_path / "kun"); _q("unpack", nk, d)
    assert _tree_md5(d) == keep
    assert _data_as_tar(nk) == keep                        # filtered nock is still a valid tar

    root = str(tmp_path / "tree")                           # pack: the SAME filter (fs source)
    for name, data in members:
        p = os.path.join(root, name); os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "wb").write(data)
    pk = str(tmp_path / "p.nock"); _q("pack", root, pk, "--glob", "*.keep")
    d2 = str(tmp_path / "pun"); _q("unpack", pk, d2)
    assert _tree_md5(d2) == keep

    small_keep = sum(1 for i in range(40) if i % 2 == 0 and i % 5 == 0)   # exclude + min-size
    out = _q("recompress", str(tmp_path / "c.nock"), "--exclude", "*.drop",
             "--min-size", "100", "--frame-mb", "0.02", src)
    assert f"{len(keep) - small_keep} members" in out


def test_cli_unpack_fidelity(bvm, tmp_path):
    root = str(tmp_path / "tree"); os.makedirs(root + "/sub")
    for i in range(20):
        p = f"{root}/sub/f{i:02d}"; open(p, "wb").write(bytes([i]) * (100 + i))
        os.chmod(p, 0o640 if i % 2 else 0o644)
    time.sleep(1.1)                                        # mtimes distinctly in the past
    nk = str(tmp_path / "p.nock"); _q("pack", root, nk)
    un = str(tmp_path / "un"); _q("unpack", nk, un)
    assert _tree_md5(un) == _tree_md5(root)
    d = _q("diff", root, un)                               # mtime+mode restored -> nothing changed
    assert "changed   0" in d and "unchanged 20" in d
    a, b = os.stat(f"{root}/sub/f01"), os.stat(f"{un}/sub/f01")
    assert (a.st_mode & 0o777) == (b.st_mode & 0o777) == 0o640
    assert int(a.st_mtime) == int(b.st_mtime)


def test_cli_symlinks(bvm, tmp_path):
    root = str(tmp_path / "tree"); os.makedirs(root + "/sub")
    open(f"{root}/target.txt", "wb").write(b"content here")
    open(f"{root}/sub/data", "wb").write(b"x" * 300)
    links = {"rel": "target.txt", "abs": "/etc/hostname",       # relative, absolute,
             "sub/deep": "sub/data", "dangling": "nope"}        # in-subdir, dangling
    for name, tgt in links.items():
        os.symlink(tgt, os.path.join(root, name))
    time.sleep(1.1)
    nk = str(tmp_path / "p.nock")
    assert "'symlinks': 4" in _q("pack", root, nk)
    un = str(tmp_path / "un"); _q("unpack", nk, un)
    for name, tgt in links.items():                            # reconstructed with exact targets
        p = os.path.join(un, name)
        assert os.path.islink(p) and os.readlink(p) == tgt
    for f in ["target.txt", "sub/data"]:                       # regular files intact
        assert open(os.path.join(un, f), "rb").read() == open(os.path.join(root, f), "rb").read()
    d = _q("diff", root, un)                                   # symlinks + files all unchanged
    assert "changed   0" in d and "unchanged 6" in d


def _tar_zstd_syms(path, files, links):
    import zstandard
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for name, data in files:
            ti = tarfile.TarInfo(name); ti.size = len(data); ti.mode = 0o644
            tf.addfile(ti, io.BytesIO(data))
        for name, tgt in links.items():
            ti = tarfile.TarInfo(name); ti.type = tarfile.SYMTYPE; ti.linkname = tgt; ti.mode = 0o777
            tf.addfile(ti)
    open(path, "wb").write(zstandard.ZstdCompressor(level=6).compress(buf.getvalue()))


def test_cli_recompress_symlinks(bvm, tmp_path):
    links = {"d/rel": "f0.txt", "abs": "/etc/hostname",
             "longl": "../" * 30 + "deep/target.dat", "dangling": "nope"}   # long -> PAX linkpath
    src = str(tmp_path / "s.tar.zstd")
    _tar_zstd_syms(src, [(f"d/f{i}.txt", b"content %d" % i) for i in range(6)], links)

    for label, extra in (("compat", []), ("nocompat", ["--no-tar-headers"])):
        nk = str(tmp_path / f"{label}.nock")
        out = _q("recompress", nk, *extra, src)
        assert f"{6 + len(links)} members" in out
        un = str(tmp_path / f"un_{label}"); _q("unpack", nk, un)
        for name, tgt in links.items():                        # quiver unpack recreates symlinks
            p = os.path.join(un, name)
            assert os.path.islink(p) and os.readlink(p) == tgt

    # tar-compat parity: `tar x` on the decoded data must also recreate the symlinks
    import zstandard
    from quiver.exec import blocks
    import polars as pl
    idx = blocks.scan_nock(str(tmp_path / "compat.nock")).filter(pl.col("frame") >= 0)
    dctx = zstandard.ZstdDecompressor(); raw = bytearray()
    with open(str(tmp_path / "compat.nock"), "rb") as f:
        for c, l in idx.select("coff", "clen").unique().sort("coff").iter_rows():
            f.seek(c); raw += dctx.decompress(f.read(l))
    found = {}
    with tarfile.open(fileobj=io.BytesIO(bytes(raw))) as tf:
        for m in tf:
            if m.issym():
                found[m.name] = m.linkname
    assert found == links                                      # tar sees every symlink + target


def test_cli_hardlinks(bvm, tmp_path):
    import io as _io
    root = str(tmp_path / "tree"); os.makedirs(root + "/sub")
    open(f"{root}/orig", "wb").write(b"shared" * 10)
    os.link(f"{root}/orig", f"{root}/hl1"); os.link(f"{root}/orig", f"{root}/sub/hl2")
    os.symlink("orig", f"{root}/sy")
    nk = str(tmp_path / "p.nock")
    out = _q("pack", root, nk)
    assert "'hardlinks': 2" in out and "'symlinks': 1" in out
    un = str(tmp_path / "un"); _q("unpack", nk, un)
    o = os.stat(f"{un}/orig")                                  # unpack -> real hardlinks (same inode)
    for hl in ["hl1", "sub/hl2"]:
        assert os.stat(os.path.join(un, hl)).st_ino == o.st_ino
    assert os.readlink(f"{un}/sy") == "orig"
    # tar-compat parity: `zstd -dc` (all frames) yields a tar with type-1/2 headers
    import subprocess
    data = subprocess.run(["zstd", "-dc", nk], capture_output=True).stdout
    hl, sy = {}, {}
    with tarfile.open(fileobj=_io.BytesIO(data)) as tf:
        for m in tf:
            (hl if m.islnk() else sy if m.issym() else {})[m.name] = m.linkname
    assert hl == {"hl1": "orig", "sub/hl2": "orig"} and sy == {"sy": "orig"}


def test_cli_cp_sync_links(bvm, tmp_path):
    src = str(tmp_path / "src"); os.makedirs(src + "/sub")
    open(f"{src}/f", "wb").write(b"data" * 10)
    os.link(f"{src}/f", f"{src}/hl"); os.symlink("f", f"{src}/sy")
    for cmd, dstname in (("cp", "cpd"), ("sync", "syd")):
        dst = str(tmp_path / dstname); _q(cmd, src, dst)
        assert os.stat(f"{dst}/hl").st_ino == os.stat(f"{dst}/f").st_ino   # hardlink preserved
        assert os.readlink(f"{dst}/sy") == "f"                             # symlink preserved


def test_cli_migrate_idempotent(bvm, tmp_path):
    src, nk = str(tmp_path / "s.tar.zstd"), str(tmp_path / "o.nock")
    _tar_zstd(src, _members(20))
    _q("recompress", nk, src)
    assert "already chunked" in _q("migrate", nk)         # blocks writes chunked already

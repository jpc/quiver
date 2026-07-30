"""blocks.py — the PLANNER for the BLOCKS model (docs/BLOCKS.md).

A STAT table + residences + whole-member FRAMES + one COPY verb: no
ALLOC/FREE/gather-triples/frame_base/carry. Every byte is moved by the C executor
(`bvm.c`); this module plans the work, streams it over an Arrow-IPC command pipe,
and assembles the footer from what comes back. Correctness is checked against the
C engine's own round-trips and against upstream tools (tar/zstd) — the pure-Python
reference core that once lived here was removed once nothing executed it.

Model:
  * A STAT batch is a polars frame of {path,size,mode,mtime_ns,uid,gid} + a locator.
    `res` (batch-level) is FS | MEM | NOCK. It is the file list, the in-flight work,
    AND the on-disk footer.
  * A FRAME is the universal unit: one compression frame == one memory block == a
    group of WHOLE members. A member bigger than `frame_cap` is the exception: it
    is cut into pieces and stitched back by an EXTENT row.
  * A block is a refcounted buffer of concatenated member bodies. It frees when the
    last STAT batch pointing into it retires. Bounded memory == a byte budget.
  * COPY(stat, dst[, codec]) is the only mover: MEM->SINK is deflate, NOCK->FS is
    lazy-inflate + scatter, etc.
"""
import os, io, struct, hashlib, threading, queue, time
import polars as pl
import zstandard as zstd

FS, MEM, NOCK = "fs", "mem", "nock"
STAT_COLS = ["path", "size", "mode", "mtime_ns", "uid", "gid",
             "frame", "in_off", "coff", "clen", "digest", "link"]   # frame -1=dir, -2=symlink(->link)


# ------------------------------------------------------------------ blocks
# ------------------------------------------------------------------ SCAN / DECODE
def _stat_rows(members):
    schema = {c: (pl.Utf8 if c in ("path", "link") else pl.Int64) for c in STAT_COLS}
    if not members:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(members, schema=schema).with_columns(pl.col("link").fill_null(""))


def scan_fs(root, chunk_rows=50000, bvm_exe=None, workers=32):
    """SCAN(FS): STREAM STAT batches from the PARALLEL bvm filesystem walk (SCAN_FS: a
    per-directory work queue over the walker pool, fd-relative openat/fstatat — the one
    scanner used everywhere), path,size,mode,mtime_ns,uid,gid,link, NO bodies read. Yields
    polars batches as they stream in so the planner works on data as it arrives."""
    sess = _Bvm(bvm_exe or default_bvm(), workers); sess.scan_fs(0, os.path.abspath(root))
    try:
        while True:
            t, pld = sess.read()
            if t is None or t == 1:
                break
            if t == 0:
                df = _stat_df(pld)[2]
                if not df.height:
                    continue
                files = df.filter(pl.col("is_dir") == 0)     # STAT_COLS view: files/links only
                if files.height:
                    yield files.select(                       # exact STAT_COLS order (concat-safe)
                        path="path",
                        size=pl.col("size").cast(pl.Int64),
                        mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
                        mtime_ns="mtime_ns",
                        uid=pl.col("uid").cast(pl.Int64), gid=pl.col("gid").cast(pl.Int64),
                        frame=pl.lit(0, pl.Int64), in_off=pl.lit(0, pl.Int64),
                        coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
                        digest=pl.lit(-1, pl.Int64), link="link")
    finally:
        sess.finish()


def default_bvm():
    """Path to the bvm executor. This NEVER builds it -- building is an explicit step:

        make -C quiver/exec bvm

    It used to rebuild whenever bvm.c looked newer, which is convenient right up until it
    isn't: any incidental call would relink the executable that running executors were
    mid-flight on. Over a shared filesystem the kernel's ETXTBSY guard does not reach the
    nodes running them, so the write succeeds and they die. That cost a 100-minute backup at
    95%. A stale binary is just as bad in the other direction -- a long benchmark that
    silently measures the code you thought you replaced -- so staleness is an error too.

    QUIVER_BVM points at a prebuilt binary; QUIVER_ALLOW_STALE=1 waives the freshness check.
    """
    d = os.path.dirname(os.path.abspath(__file__))
    exe = os.environ.get("QUIVER_BVM") or os.path.join(d, "bvm")
    src = os.path.join(d, "bvm.c")
    if not os.path.exists(exe):
        raise RuntimeError(f"{exe} not built. Run:  make -C {d} bvm")
    if (os.path.exists(src) and os.path.getmtime(exe) < os.path.getmtime(src)
            and not os.environ.get("QUIVER_ALLOW_STALE")):
        raise RuntimeError(
            f"{exe} is older than {os.path.basename(src)} -- it would measure the wrong code. "
            f"Run:  make -C {d} bvm    (or set QUIVER_ALLOW_STALE=1)")
    return exe


def _member_filter(include=None, exclude=None, min_size=0):
    """The shared PLANNER filter — a predicate over a member dict {path,size,...}, used
    identically by recompress (tar) and pack (fs). `include`/`exclude` are glob lists,
    `min_size` a byte floor. Returns None if nothing filters (the fast, no-filter path)."""
    if not include and not exclude and not min_size:
        return None
    import re

    def rx(globs):
        return re.compile("|".join("(?:^" + "".join(
            ".*" if c == "*" else "." if c == "?" else re.escape(c) for c in g) + "$)"
            for g in globs)) if globs else None
    inc, exc = rx(include), rx(exclude)

    def keep(m):
        p = m["path"]
        return not ((inc and not inc.match(p)) or (exc and exc.match(p))
                    or (min_size and m["size"] < min_size))
    e = pl.lit(True)                                     # the SAME filter as a polars expression —
    if inc:                                              # vectorized planners (pack) use keep.expr,
        e = e & pl.col("path").str.contains(inc.pattern)  # dict planners call keep(m)
    if exc:
        e = e & ~pl.col("path").str.contains(exc.pattern)
    if min_size:
        e = e & (pl.col("size") >= min_size)
    keep.expr = e
    return keep


def _glob_pred(globs):
    """A polars predicate over `path` matching any glob in `globs` (None -> None). `*`
    matches any run (including '/', i.e. whole subtrees), `?` one char. Rust-regex safe."""
    if not globs:
        return None
    import re

    def g2r(g):
        return "^" + "".join(".*" if c == "*" else "." if c == "?" else re.escape(c)
                             for c in g) + "$"
    rx = "|".join(f"(?:{g2r(g)})" for g in globs)
    return pl.col("path").str.contains(rx)


# ------------------------------------------------------------------ fs tools (du/rm/cp)
def du(root, apparent=True):
    """Sum member sizes under `root`. Returns (nbytes, nfiles)."""
    nb = nf = 0
    for b in scan_fs(root):
        nb += int(b["size"].sum()); nf += b.height
    return nb, nf


def _rmdirs(sess, dirs, root_abs):
    """rmdir depth-ordered (children before parents), each level a synchronous UNLINK batch.
    `dirs` is a polars Utf8 Series; the depth split is a vectorized count+group."""
    if not isinstance(dirs, pl.Series):
        dirs = pl.Series("p", sorted(dirs), pl.Utf8)
    df = dirs.to_frame("p").with_columns(d=pl.col("p").str.count_matches("/", literal=True))
    for depth in sorted(df["d"].unique().to_list(), reverse=True):
        sess.unlink(df.filter(pl.col("d") == depth)["p"], removedir=1)
    sess.unlink([root_abs], removedir=1)


def verify_merkle(nock, at=None):
    """Authenticate a snapshot's FOOTER: re-hash every batch payload against the stored
    Merkle leaves and recompute the root. O(footer), touches no data frames. Returns
    dict(root, commit, batches, bad) — bad lists mismatching batch indexes."""
    import hashlib
    from ..nock import nockidx as _nx
    m = _nx.read_directory(nock, at=at, with_prev=True)
    ents, root, leaves = m[0], m[3], m[5]
    if root is None:
        return dict(root=None, commit=None, batches=len(ents or []), bad=None)  # pre-merkle store
    bad = []
    with open(_nx.footer_path(nock), "rb") as f:         # batches live with the directory
        for i, (off, clen, _fr, _nr) in enumerate(ents):
            f.seek(off)
            if hashlib.blake2b(f.read(clen), digest_size=32).digest() != leaves[i]:
                bad.append(i)
    ok = _nx.merkle_root(leaves) == root and not bad
    return dict(root=root.hex(), commit=m[4].hex() if m[4] else None,
                batches=len(ents), bad=bad if not ok else bad)


def verify_complete(nock, root=None, at=None):
    """COMPLETENESS check — the class `verify` structurally cannot see: members that were
    DROPPED at pack time (an errored frame leaves no rows, so re-hashing survivors passes).
    Compares the footer's member set against the live tree at `root` (or, with no root, just
    reports counts + any dangling locators). Returns dict(footer, missing, extra,
    missing_sample, bad_locators)."""
    df = scan_nock(nock, at=at)
    files = df.filter((pl.col("frame") >= 0) | (pl.col("frame") == -4))
    bad = files.filter((pl.col("frame") >= 0) & (pl.col("coff") < 0)).height   # unrecorded frames
    out = dict(footer=files.height, missing=None, extra=None, missing_sample=[], bad_locators=bad)
    if root:
        have = set()
        for dp, _dn, fns in os.walk(root):
            for f in fns:
                p = os.path.join(dp, f)
                if not os.path.islink(p):
                    have.add(os.path.relpath(p, root))
        infoot = set(files["path"].to_list())
        miss = have - infoot
        out.update(missing=len(miss), extra=len(infoot - have), missing_sample=sorted(miss)[:8])
    return out


def store_stats(nock, at=None):
    """Where a store's bytes actually went: per-frame compression ratio, weighted by payload.
    Reads only the footer. The payload-weighted number is the one that matters -- on a real
    home tree the MEDIAN frame sits at 0.95 while the weighted ratio is 0.83, because the
    compressible material is concentrated in relatively few large frames, so tuning on
    per-frame medians misleads. Frames whose declared content size does not match the payload
    the footer assigns them are reported separately: they are not compression, they are loss."""
    import numpy as np
    df = scan_nock(nock, at=at).filter(pl.col("frame") >= 0)
    if "shard" not in df.columns:
        df = df.with_columns(shard=pl.lit(0, pl.Int64))
    fr = df.group_by("shard", "coff", "clen").agg(pl.col("size").sum().alias("pay"))
    p = fr["pay"].to_numpy().astype(float); c = fr["clen"].to_numpy().astype(float)
    good = (p > 0) & (c > 64)                            # tiny clen + huge payload = a lost member
    bogus = int((~good & (p > 0)).sum())
    r = c[good] / p[good]
    EDGES = [0, .05, .1, .2, .3, .4, .5, .6, .7, .8, .9, .95, .99, 1.001, 1e9]
    hist = []
    for i in range(len(EDGES) - 1):
        m = (r >= EDGES[i]) & (r < EDGES[i + 1])
        if m.sum():
            hist.append(dict(lo=EDGES[i], hi=EDGES[i + 1], frames=int(m.sum()),
                             payload=float(p[good][m].sum()), stored=float(c[good][m].sum())))
    return dict(frames=int(good.sum()), bogus_frames=bogus,
                payload=float(p[good].sum()), stored=float(c[good].sum()),
                ratio=float(c[good].sum() / p[good].sum()) if good.sum() else None,
                median_frame_ratio=float(np.median(r)) if r.size else None,
                bogus_payload=float(p[~good].sum()), hist=hist)


def verify(nock, sample=None):
    """Integrity check: re-hash every member body (BLAKE3) against the footer `digest`
    column (-1 rows = packed before digests, skipped). `sample` = check only that many
    frames (random). Returns dict(checked, mismatched, undigested, frames, bad_frames) —
    mismatches also carry the first few offending paths."""
    import random as _r
    import blake3 as _b3
    df = scan_nock(nock).filter(pl.col("frame") >= 0)
    # STRUCTURAL PASS over EVERY frame, before any sampling. A zstd frame header declares its
    # content size, so comparing it against the payload the footer claims lives there costs 18
    # bytes of read per frame and catches a whole class of silent loss that digest checking
    # cannot: a member that failed to buffer (ENOMEM on a giant file) left a 9-byte EMPTY frame
    # with digest -1, so `verify` skipped it as undigested and the run reported lost=0. Two such
    # frames sat in a 4.9 TB home backup claiming 55.7 TB of payload between them.
    short = []
    if "shard" not in df.columns:
        df = df.with_columns(shard=pl.lit(0, pl.Int64))
    frames = df.group_by("shard", "coff", "clen", maintain_order=True).agg(
        pl.col("path"), pl.col("in_off"), pl.col("size"), pl.col("digest"))
    idx = list(range(frames.height))
    if sample is not None and sample < len(idx):
        _r.shuffle(idx); idx = idx[:sample]
    dctx = zstd.ZstdDecompressor()
    checked = mism = undig = bad = 0; badpaths = []
    fhs = [open(p, "rb") for p in store_files(nock)]
    try:
        for k in range(frames.height):                   # ALL frames, not just the sample
            r = frames.row(k, named=True)
            want = int(sum(r["size"]))
            f = fhs[r["shard"]]
            f.seek(r["coff"])
            try:
                got = zstd.frame_content_size(f.read(18))
            except Exception:
                got = -1
            if got >= 0 and got != want:
                short.append((r["path"][0] if r["path"] else "?", want, got, int(r["clen"])))
        for k in idx:
            r = frames.row(k, named=True)
            f = fhs[r["shard"]]
            f.seek(r["coff"]); comp = f.read(r["clen"])
            try:
                body = dctx.decompress(comp)
            except Exception:
                bad += 1; badpaths.append(f"frame@{r['shard']}:{r['coff']}"); continue
            for p, io_, sz, dg in zip(r["path"], r["in_off"], r["size"], r["digest"]):
                if dg == -1:
                    undig += 1; continue
                checked += 1
                h = int.from_bytes(_b3.blake3(body[io_:io_ + sz]).digest(length=8),
                                   "little", signed=True)   # == C blake3_i64 (BLAKE3 XOF[:8] LE)
                if h != dg:
                    mism += 1
                    if len(badpaths) < 8:
                        badpaths.append(p)
    finally:
        for f in fhs:
            f.close()
    return dict(checked=checked, mismatched=mism, undigested=undig,
                frames=len(idx), bad_frames=bad, bad=badpaths,
                truncated=len(short), truncated_sample=short[:5])


def rm(root, workers=32, bvm_exe=None, procs=1):
    """Parallel delete driven entirely by bvm: ONE parallel SCAN_NAMES lists every entry
    (readdir + d_type, NO lstat — rm needs no stat fields, and skipping it is 13x on WEKA:
    ~77k names/s vs ~5.8k lstat/s), THEN bvm io_uring-batch-unlinks all files, then rmdirs
    bottom-up one depth level at a time. The two phases run SEQUENTIALLY on purpose —
    overlapping scan (metadata reads) with unlink (metadata writes) on the same dirs
    destructively contends on WEKA's distributed dir locks (measured: streaming overlap
    2,328 f/s vs 3,124 with the old stat scan; a 2nd concurrent unlinker made it 1,753). With
    the near-free names-only scan the whole-rm is now unlink-bound. The unlink order is
    shuffled so the io_uring in-flight window spans many dirs
    (5.3x on WEKA). `procs>1` splits the unlink phase across that many bvm PROCESSES (separate
    WEKA clients) AFTER the scan completes — no overlap with the scan. Falls back to os.walk +
    threaded unlink if `bvm_exe` is None. Returns entries removed."""
    import random
    root_abs = os.path.abspath(root)
    if not bvm_exe:                                        # bvm-free fallback: os.walk + threaded unlink
        import concurrent.futures as cf
        files = [os.path.join(dp, fn) for dp, _, fns in os.walk(root_abs) for fn in fns]
        with cf.ThreadPoolExecutor(workers) as ex:
            list(ex.map(lambda p: os.unlink(p) if os.path.lexists(p) else None, files))
        for dp, _, _ in os.walk(root, topdown=False):
            try:
                os.rmdir(dp)
            except OSError:
                pass
        return len(files)
    sess = _Bvm(bvm_exe, workers); sess.scan_names(0, root_abs)  # names only: rm needs no stat
    rels, dsers = [], []
    while True:
        t, pld = sess.read()
        if t is None or t == 1:
            break
        if t == 0:
            df = _stat_df(pld)[2]
            rels.append(df.filter(pl.col("is_dir") == 0)["path"])
            dsers.append(df.filter(pl.col("is_dir") == 1)["path"])   # EMITTED dirs: includes EMPTY
    pre = root_abs + "/"
    def _join(sers):                                        # VECTORIZED prefix join (stays a Series)
        s = pl.concat(sers) if sers else pl.Series("p", [], pl.Utf8)
        return s.to_frame("p").select((pl.lit(pre) + pl.col("p")).alias("p"))["p"]
    files = _join(rels).shuffle()                           # spread the io_uring window across dirs
    dirs = _join(dsers)                                     # complete (empty dirs were invisible
    nfiles = files.len()                                    # when derived from file paths -> left behind)
    if procs > 1:                                           # split unlink across separate WEKA clients
        def _unlink_slice(slc):
            u = _Bvm(bvm_exe, workers)
            for i in range(0, slc.len(), 100000):
                u.unlink(slc.slice(i, 100000), removedir=0)
            u.finish()
        parts = [files.gather_every(procs, offset=k) for k in range(procs)]
        ths = [threading.Thread(target=_unlink_slice, args=(pt,)) for pt in parts]
        for th in ths:
            th.start()
        for th in ths:
            th.join()
    else:
        for i in range(0, nfiles, 100000):
            sess.unlink(files.slice(i, 100000), removedir=0)
    _rmdirs(sess, dirs, root_abs)
    sess.finish()
    return nfiles


def cp(src, dst, workers=32, preserve=True):
    """Parallel recursive copy of the `src` tree -> `dst`, faithfully: regular files (mode +
    mtime preserved), symlinks (target + own mtime), and hardlinks (recreated as links, not
    copies). Returns entries copied."""
    import concurrent.futures as cf, shutil, stat as _st
    files, syms, hards, inodes = [], [], [], {}
    for dp, dns, fns in os.walk(src):
        rel_dir = os.path.relpath(dp, src)
        os.makedirs(dst if rel_dir == "." else os.path.join(dst, rel_dir), exist_ok=True)
        symdirs = [d for d in dns if os.path.islink(os.path.join(dp, d))]
        for d in symdirs:
            dns.remove(d)                                    # capture symlinked dirs, don't descend
        for name in fns + symdirs:
            sp = os.path.join(dp, name); rel = os.path.relpath(sp, src)
            try:
                s = os.lstat(sp)
            except OSError:
                continue
            if _st.S_ISLNK(s.st_mode):
                syms.append((rel, os.readlink(sp), s.st_mtime_ns))
            elif _st.S_ISREG(s.st_mode):
                if s.st_nlink > 1:
                    key = (s.st_dev, s.st_ino)
                    if key in inodes:
                        hards.append((rel, inodes[key])); continue
                    inodes[key] = rel
                files.append((rel, s.st_mode, s.st_mtime_ns))

    def _copy(t):
        rel, mode, mt = t
        d = os.path.join(dst, rel); shutil.copyfile(os.path.join(src, rel), d)
        if preserve:
            os.chmod(d, mode & 0o7777); os.utime(d, ns=(mt, mt))
    with cf.ThreadPoolExecutor(workers) as ex:
        list(ex.map(_copy, files))
    for rel, tgt, mt in syms:
        d = os.path.join(dst, rel)
        if os.path.lexists(d):
            os.remove(d)
        os.symlink(tgt, d)
        try:
            os.utime(d, ns=(mt, mt), follow_symlinks=False)
        except (OSError, NotImplementedError):
            pass
    for rel, tgtrel in hards:                                # after files exist
        d = os.path.join(dst, rel)
        if os.path.lexists(d):
            os.remove(d)
        os.link(os.path.join(dst, tgtrel), d)
    return len(files) + len(syms) + len(hards)


def _wal_stat(path):
    """Load the WAL as a STAT table: the prior committed {path,size,mtime_ns,digest,
    frame,in_off,coff,clen}. This IS the manifest of what content exists and where."""
    committed, _, _ = _wal_load(path)
    parts = [s for _, _, s in committed.values()]
    return pl.concat(parts, how="vertical_relaxed") if parts else _stat_rows([])


def _read_msg(f):
    hdr = f.read(4)
    if len(hdr) < 4:
        return None, None
    (mlen,) = struct.unpack("<I", hdr)
    body = f.read(mlen)
    while len(body) < mlen:                              # pipe short read
        more = f.read(mlen - len(body))
        if not more:
            break
        body += more
    return body[0], body[1:]


def _s(s):
    b = s.encode(); return struct.pack("<H", len(b)) + b


FRAME_DT = [("frame", "<u8"), ("coff", "<u8"), ("clen", "<u8"), ("t0_ns", "<i8"),
            ("t1_ns", "<i8"), ("in_bytes", "<i8"), ("jq_depth", "<i4"), ("busy", "<i4"),
            ("worker", "<i4"), ("sink", "<i4")]


def read_frames_live(path):
    """Read a live <store>.frames.bin. Fixed-width records, so this needs no locking and no
    format negotiation: floor to whole records and a half-written tail is simply not there
    yet. This is the dump everything else reads -- the watcher, and the post-hoc analysis."""
    import numpy as np
    rec = np.dtype(FRAME_DT)
    n = os.path.getsize(path) // rec.itemsize
    if not n:
        return pl.DataFrame()
    a = np.fromfile(path, dtype=rec, count=n)
    return pl.DataFrame({k: a[k] for k, _ in FRAME_DT})


def frame_costs(bvms, pdf=None, root_depth=2):
    """One row per frame: when it ran, on which worker and sink, what it cost, and how deep
    the queue was WHEN IT STARTED. With `pdf` (the planner's frame->member table) each row
    also gets the subtree it came from, so a slow stretch can be attributed to a path rather
    than guessed at. This is what the 1 Hz sampler was approximating; the samples blurred a
    metadata-bound small-file phase into a bandwidth-bound big-file one and reported a single
    verdict that fit neither."""
    rows = []
    for i, b in enumerate(bvms):
        if b.frames:
            rows.append(pl.DataFrame(b.frames, schema=list(_Bvm.FRAME_COLS), orient="row")
                        .with_columns(node=pl.lit(i, pl.Int32)))
    if not rows:
        return pl.DataFrame()
    df = pl.concat(rows).with_columns(
        secs=(pl.col("t1_ns") / 1e9), dur_ms=((pl.col("t1_ns") - pl.col("t0_ns")) / 1e6),
        mb_s=(pl.col("in_bytes") / ((pl.col("t1_ns") - pl.col("t0_ns")).clip(1) / 1e9) / 1e6))
    if pdf is not None:
        roots = (pdf.group_by("fid").agg(pl.col("path").first())
                 .with_columns(root=pl.col("path").str.split("/").list.head(root_depth)
                               .list.join("/")).select(frame="fid", root="root"))
        df = df.join(roots, on="frame", how="left")
    return df.sort("secs")


def perf_report(perf):
    """Turn bvm's type-4 counters into WHERE-DID-MY-TIME-GO guidance. Returns
    (summary dict, [human hint lines]). All ns_* are summed across workers."""
    if not perf:
        return {}, ["no perf data (old bvm?)"]
    w = max(perf["nworkers"], 1); wall = max(perf["wall_ns"], 1)
    cap = w * wall                                      # total worker-time available
    busy = {"read": perf["ns_read"], "compress": perf["ns_comp"],
            "decompress": perf["ns_dcomp"], "write": perf["ns_write"],
            "hash": perf["ns_hash"], "open/create": perf["ns_open"],
            "metadata": perf["ns_meta"], "unlink": perf["ns_unlink"]}
    tot_busy = sum(busy.values())
    util = tot_busy / cap
    hints = []
    top = sorted(busy.items(), key=lambda kv: -kv[1])
    parts = "  ".join(f"{k} {v / max(tot_busy, 1) * 100:.0f}%" for k, v in top if v > 0.02 * tot_busy)
    hints.append(f"worker utilization {util * 100:.0f}% of {w} workers; busy split: {parts}")
    gbs = lambda b, ns: b / max(ns, 1) * 1e9 / 1e9
    if perf["ns_comp"] > 0.4 * tot_busy:
        hints.append(f"CPU/COMPRESS-BOUND: zstd at {gbs(perf['comp_in'], perf['ns_comp']):.2f} GB/s in "
                     f"-> consider lower -L or more workers/nodes")
    if perf["ns_read"] + perf["ns_write"] > 0.5 * tot_busy:
        hints.append(f"BANDWIDTH-BOUND: read {gbs(perf['rd_bytes'], max(perf['ns_read'],1)):.2f} GB/s, "
                     f"write {gbs(perf['wr_bytes'], max(perf['ns_write'],1)):.2f} GB/s (per-op avg)")
    if perf["n_open"]:
        avg_open = perf["ns_open"] / perf["n_open"] / 1e3
        slow = perf["slow_open"] / perf["n_open"]
        if slow > 0.3 or avg_open > 400:
            hints.append(f"METADATA-BOUND: {perf['n_open']:,} opens avg {avg_open:.0f}us, "
                         f"{slow*100:.0f}% >500us (backend RPC / dir-lock contention) -> "
                         f"more parallel dirs, or fewer+larger files")
    if perf["n_meta"]:
        avg = perf["ns_meta"] / perf["n_meta"] / 1e3
        if avg > 400 and perf["ns_meta"] > 0.2 * tot_busy:
            hints.append(f"mtime/chown restore costs {avg:.0f}us/file "
                         f"({perf['ns_meta']/1e9:.0f}s total) -> BVM_SCATTER_META=0 if acceptable")
    if perf["scan_entries"]:
        avg = perf["ns_scan_stat"] / perf["scan_entries"] / 1e3
        hints.append(f"scan: {perf['scan_entries']:,} stats avg {avg:.0f}us"
                     + (" (cold metadata)" if avg > 200 else ""))
    if perf["ns_idle"] > 0.5 * cap and util < 0.3:
        hints.append("workers mostly IDLE -> planner/source is the bottleneck "
                     "(or the job is simply small)")
    if perf["ns_emit"] > 0.1 * wall:
        hints.append("stdout emit backpressure: planner slow to drain results")
    if perf["ns_qfull"] > 0.3 * wall:
        hints.append("job queue full 30%+ of the time: workers saturated (this is good scaling)")
    if perf.get("raw_bytes"):
        hints.append(f"incompressible fast-path: {perf['n_raw_frames']:,} raw frames, "
                     f"{perf['raw_bytes']/1e9:.1f} GB stored codec-free (unpack = pure fs-to-fs)")
    if perf.get("cfr_bytes"):
        hints.append(f"zero-copy restore: {perf['cfr_bytes']/1e9:.1f} GB moved via copy_file_range")
    # ---- QUEUE OCCUPANCY: Little's-law bottleneck attribution ----
    qs = perf.get("q_samples") or 0
    if qs:
        jq_occ = perf["jq_sum"] / qs / max(perf["jq_cap"], 1)
        jq_full = perf["jq_full"] / qs
        jq_empty = perf["jq_empty"] / qs
        qbusy = perf["busy_sum"] / qs                    # NB: distinct from `busy` (ns dict)
        dqd = perf["dq_sum"] / qs
        packu = perf["pack_used_sum"] / qs / max(perf["pack_budget"], 1)
        blku = perf["blk_live_sum"] / qs / max(perf["blk_budget"], 1)
        summary_q = (f"queues: jobq {jq_occ*100:.0f}% full ({jq_full*100:.0f}% saturated, "
                     f"{jq_empty*100:.0f}% empty) | workers busy {qbusy:.1f}/{w} "
                     f"({qbusy/w*100:.0f}%) | scanq {dqd:.0f} dirs | packbuf {packu*100:.0f}% "
                     f"| blockbuf {blku*100:.0f}%")
        hints.append(summary_q)
        # attribution
        util_w = qbusy / w
        # "engaged" is NOT "working": a worker blocked in pread counts as busy. Split it, or
        # a job that is 87% blocked on network-filesystem latency and using 3% of the CPU
        # reads as "saturated, add nodes" -- when what it actually needs is more requests in
        # flight, which on this executor means more threads, because one worker runs the whole
        # open->read->hash->compress->write chain inline and can only have ONE read pending.
        wall_ns = perf.get("wall_ns", 0) or 1
        cap_ns = w * wall_ns
        cpu_ns = perf.get("ns_comp", 0) + perf.get("ns_hash", 0)
        io_ns = perf.get("ns_read", 0) + perf.get("ns_write", 0) + perf.get("ns_open", 0)
        cpu_f, io_f = cpu_ns / cap_ns, io_ns / cap_ns
        hints.append(f"worker time: {cpu_f*100:.0f}% CPU (compress+hash), {io_f*100:.0f}% "
                     f"blocked on I/O — of {util_w*100:.0f}% engaged")
        if jq_full > 0.4 and util_w > 0.55 and io_f > 3 * max(cpu_f, 1e-9):
            hints.append(f"BOTTLENECK = READ LATENCY, not this node: workers are "
                         f"{util_w*100:.0f}% engaged but only {cpu_f*100:.0f}% of capacity is "
                         f"CPU — they are waiting, not computing. One worker holds one read at "
                         f"a time, so concurrency == -j. RAISE -j (memory is frame_cap x "
                         f"workers) before adding nodes.")
        elif jq_full > 0.4 and util_w > 0.55:
            hints.append(f"BOTTLENECK = WORKERS ({util_w*100:.0f}% engaged, {cpu_f*100:.0f}% "
                         f"CPU, queue saturated {jq_full*100:.0f}%): this node is the limit — "
                         f"ADD NODES")
        elif jq_empty > 0.5 and util_w < 0.5:
            hints.append(f"BOTTLENECK = PLANNER/SOURCE ({util_w*100:.0f}% engaged, queue empty "
                         f"{jq_empty*100:.0f}%): workers starved — the Python planner or the "
                         f"scan is not feeding them")
        elif jq_full > 0.3 and util_w < 0.4:
            hints.append(f"BOTTLENECK = BLOCKING INSIDE WORKERS ({util_w*100:.0f}% engaged but "
                         f"queue backed up {jq_full*100:.0f}%): check packbuf/blockbuf, or "
                         f"metadata RPC latency")
        elif jq_full > 0.2 or jq_empty > 0.2:
            hints.append(f"queue mixed: workers {util_w*100:.0f}% engaged, saturated "
                         f"{jq_full*100:.0f}% / empty {jq_empty*100:.0f}% — "
                         f"{'downstream (workers) leaning' if jq_full > jq_empty else 'upstream (planner) leaning'}")
        if packu > 0.8:
            hints.append(f"pack buffer budget {packu*100:.0f}% used — giant members are throttling "
                         f"concurrency (raise BVM_PACK_BUDGET_MB or shrink frame_bytes)")
        if blku > 0.8:
            hints.append(f"block budget {blku*100:.0f}% used — decoded blocks throttle recompress")
        if dqd > 1000:
            hints.append(f"scan queue {dqd:.0f} dirs deep — walkers are behind the discovery rate")
    summary = dict(util=round(util, 3), busy_ns=busy, wall_s=round(wall / 1e9, 1),
                   workers=w, rd_gb=round(perf["rd_bytes"] / 1e9, 2),
                   wr_gb=round(perf["wr_bytes"] / 1e9, 2),
                   comp_ratio=round(perf["comp_out"] / max(perf["comp_in"], 1), 3))
    return summary, hints


def _ign_wire(pats):
    """[(flags, pattern)] -> wire bytes ([u16 n] + [u8 flags][u16 len][pat]...)."""
    pats = pats or []
    return struct.pack("<H", len(pats)) + b"".join(
        struct.pack("<B", f) + _s(p) for f, p in pats)


def load_ignore(root, extra=None):
    """Parse ROOT/.quiverignore (gitignore-style) + `extra` pattern strings into the
    (flags, pattern) list the C walker prunes with. Supported: blank/# comments; trailing
    `/` = dirs only; leading `!` = negate (LAST match wins); leading `/` or an inner `/`
    anchors to the root-relative path, otherwise the pattern matches basenames anywhere.
    `**` is folded to `*` (fnmatch's `*` crosses slashes here). NOTE: an excluded dir is
    PRUNED — never descended — so a negation cannot resurrect anything inside it."""
    lines = []
    p = os.path.join(root, ".quiverignore")
    if os.path.exists(p):
        lines += open(p).read().splitlines()
    lines += list(extra or [])
    out = []
    for ln in lines:
        ln = ln.rstrip()
        if not ln or ln.lstrip().startswith("#"):
            continue
        flags = 0
        if ln.startswith("!"):
            flags |= 4; ln = ln[1:]
        if ln.endswith("/"):
            flags |= 2; ln = ln[:-1]
        if ln.startswith("/"):
            flags |= 1; ln = ln[1:]
        elif "/" in ln:
            flags |= 1
        ln = ln.replace("**", "*")
        if ln:
            out.append((flags, ln))
    return out


def _ipc_bytes(df):
    """DataFrame -> one Arrow IPC stream (schema+batch+EOS), compat_level=oldest so the
    C reader sees classic large_utf8/large_binary layouts. The COMMAND wire is Arrow both
    ways: bvm maps the batch's buffers to typed column pointers straight from the payload
    (arrow_batch in bvm.c) — no per-member encode here, no per-member decode there."""
    buf = df.write_ipc_stream(None, compat_level=pl.CompatLevel.oldest())
    return buf.getvalue()


class _SockProc:
    """Makes a TCP-connected executor look exactly like a Popen to the rest of _Bvm: the
    protocol is identical, only the transport differs. Closing stdin must half-close the
    socket rather than tear it down -- the executor still has its whole output to send."""
    def __init__(self, conn, peer=""):
        self.conn, self.peer = conn, peer
        self.stdout = conn.makefile("rb")
        outer = self

        class _W:
            def write(self, b): return outer.conn.sendall(b) or len(b)
            def flush(self): pass
            def close(self):
                try: outer.conn.shutdown(1)          # SHUT_WR: "no more commands"
                except OSError: pass
        self.stdin = _W()

    def wait(self):
        try: self.conn.close()
        except OSError: pass
        return 0

    def kill(self):
        try: self.conn.close()
        except OSError: pass


class _Bvm:
    """A session with the unified bvm command-stream executor: open sources (tars /
    fs subtrees), send COPY/PACK/SCATTER plans, read STAT/SRC_EOF/DONE. One process
    can hold many sources at once (shared pool + block budget)."""
    def __init__(self, bvm_exe, nworkers=16, budget_mb=512, launch=None, sock=None):
        """`launch` puts the executor on ANOTHER HOST: a command prefix such as
        ["srun", "-w", "node7", ...] or ["ssh", "node7"]. bvm speaks its whole protocol
        over stdin/stdout, and srun/ssh forward both, so a remote executor is the same
        object as a local one — the planner does not know the difference. Sinks and source
        trees must be paths both ends can see (a shared filesystem)."""
        import subprocess
        if sock is not None:                             # already-connected executor (--connect)
            self.p = sock
        else:
            argv = list(launch or []) + [bvm_exe, str(nworkers), str(budget_mb)]
            self.p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.errors = []                                 # (path, -errno) per-op failures from bvm
        self._sock_sinks = set()                         # sinks whose receiver reassembles BY NAME
        self.strict = True                               # False: finish() reports, doesn't raise
        self.stats = []                                  # (sid, block, df) STATs seen during finish
                                                         # (pack workers report digests this way)
        self.perf = None                                 # type-4 STATS: the LAST sample (totals)
        self.perf_series = []                            # one per second; counters are cumulative,
                                                         # so difference them for per-interval rates
        self.frames = []                                 # one cost record per frame (FRAME_COLS)
        self.frames_fd = None                            # set to stream them to disk as well
        self.locs = {}                                   # frame -> (coff, clen), ACCUMULATED:
                                                         # they arrive throughout the run now
        # DEDICATED stdout reader: bvm workers emit STAT/ERROR messages at ANY time (pack
        # digests, scan batches); if the planner only reads between sends, the stdout pipe
        # fills, workers block in emit under g_out_mu, the command loop stops reading stdin,
        # and the planner deadlocks in pipe_write (hit on a 6.5 TB home backup — tests fit
        # in the 64 KB pipe buffer). The reader ALWAYS drains; consumers pop the queue.
        self._q = queue.Queue()
        self._rt = threading.Thread(target=self._reader, daemon=True)
        self._rt.start()

    def _reader(self):
        while True:
            t, pld = _read_msg(self.p.stdout)
            self._q.put((t, pld))
            if t is None:
                return

    def _send(self, t, payload):
        self.p.stdin.write(struct.pack("<I", len(payload) + 1) + bytes([t]) + payload)
        self.p.stdin.flush()

    def sink(self, sink_id, spec, kind=0, start=0):     # kind 0=file(spec=path), 1=tcp(host:port)
        if kind:
            self._sock_sinks.add(sink_id)
        self._send(1, struct.pack("<IBq", sink_id, kind, start) + _s(spec))
    def dest(self, path):           self._send(2, _s(path))
    def nock(self, path, nock_id=0):  self._send(3, struct.pack("<I", nock_id) + _s(path))
    def open_tar(self, sid, path, frame_bytes, stat_only=0, tar_compat=0):
        flags = (stat_only & 1) | ((1 if tar_compat else 0) << 1)
        self._send(4, struct.pack("<IqB", sid, frame_bytes, flags) + _s(path))
    def scan_fs(self, sid, root, ignore=None):
        self._send(5, struct.pack("<I", sid) + _s(root) + _ign_wire(ignore))
    def scan_names(self, sid, root, ignore=None):   # readdir-only, no lstat
        self._send(14, struct.pack("<I", sid) + _s(root) + _ign_wire(ignore))
    def manifest(self, sid, root, paths):   # digest+CDC-manifest these files (delta planning)
        s_ = paths if isinstance(paths, pl.Series) else pl.Series(list(paths), dtype=pl.Utf8)
        self._send(15, struct.pack("<I", sid) + _s(root) + _ipc_bytes(s_.rename("path").to_frame()))
    def copy_block(self, block, frame, level, sink_id=0):
        self._send(6, struct.pack("<QQII", block, frame, level, sink_id))
    PACK_SCHEMA = {"fid": pl.Int64, "type": pl.UInt8, "mode": pl.Int32, "size": pl.Int64,
                   "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32,
                   "src_off": pl.Int64, "path": pl.Utf8, "link": pl.Utf8}
    # src_off: pack file bytes [src_off, src_off+size) — delta literal runs; 0 = whole file
    # type: 0 file, 5 dir, 2 symlink, 1 hardlink — dirs+links are BODYLESS tar entries
    # synthesized in bvm (full tar parity through the one typed path)
    def pack_files_df(self, level, root, df, sink_id=0, tar_compat=0):
        """ARROW pack, MANY FRAMES PER MESSAGE: frame id is the `fid` COLUMN; bvm cuts jobs
        at fid changes. NO header blobs on the wire: bvm workers SYNTHESIZE tar headers from
        these STAT columns (ustar 512 B, GNU longname >100-byte paths — the deterministic
        rule `_tar_hlens` budgets, and the same one the old TarFormat polars expressions
        used). size is the LAYOUT truth: bvm lays each member at the planner's cum-sum
        offset and zero-fills/truncates a changed file (reporting it), never shifting
        neighbours."""
        # A SPLIT MEMBER (src_off != 0, i.e. one file packed as several pieces) is legal only
        # where the footer's EXTENT row is what reassembles it. Two consumers reassemble by
        # NAME instead and would silently truncate to the last piece:
        #   - tar_compat: N pieces emit N ustar headers with the same name; `tar -x` keeps the
        #     last one (measured: a 12 MB file extracted as 4 MB). If you ever want tar output
        #     for split members, give each piece its own name (`foo.bin.qpart00001`) or use
        #     GNU multi-volume ('M' typeflag) — but then it is no longer one tar member.
        #   - socket sinks: the receiver's apply path opens each member O_WRONLY|O_CREAT|
        #     O_TRUNC and writes its slice, so piece 2 truncates piece 1.
        if "src_off" in df.columns and df.height:
            split = df.filter((pl.col("type") == 0) & (pl.col("src_off") != 0))
            if split.height and (tar_compat or sink_id in self._sock_sinks):
                who = "tar_compat" if tar_compat else f"socket sink {sink_id}"
                raise ValueError(
                    f"{who} cannot carry split members ({split.height} piece(s), e.g. "
                    f"{split['path'][0]!r} at src_off {split['src_off'][0]}): both reassemble "
                    f"by name and would silently keep only the last piece. Pack whole members "
                    f"there, or give each piece a distinct name.")
        self._send(7, struct.pack("<IIB", level, sink_id, 1 if tar_compat else 0)
                   + _s(root) + _ipc_bytes(df))

    def copy_members_df(self, block, frame, level, df, sink_id=0, tar_compat=0):
        """ARROW copy-members: same columns as PACK_FILES minus fid (block+frame are scalars,
        one filtered block per message); in_off = the member's SOURCE offset in the block.
        bvm reads bodies from the held block and SYNTHESIZES tar headers — COPY_MEMBERS is
        PACK_FILES with a buffer source instead of the filesystem."""
        self._send(12, struct.pack("<QQIIB", block, frame, level, sink_id, 1 if tar_compat else 0)
                   + _ipc_bytes(df))

    SCATTER_SCHEMA = {"nock": pl.Int32, "coff": pl.Int64, "clen": pl.Int64,
                      "in_off": pl.Int64, "size": pl.Int64, "out_off": pl.Int64,
                      "fsize": pl.Int64, "mode": pl.Int32,
                      "mtime_ns": pl.Int64, "uid": pl.Int32, "gid": pl.Int32, "path": pl.Utf8}
    # out_off/fsize: EXTENT pieces (lazy-reference members) — pwrite at out_off into a file
    # ftruncate'd to fsize; whole members are out_off=0, fsize=size
    def scatter_df(self, df):
        """ARROW scatter, MANY FRAMES PER MESSAGE: frame identity (nock,coff,clen) rides as
        COLUMNS; bvm cuts jobs at boundary changes. One message covers ~1M members, so the
        planner's send cost is O(chunks), not O(frames) (polars pays ~0.4 ms per IPC write —
        per-frame messages dominated at small frames). `df` carries SCATTER_SCHEMA."""
        self._send(8, _ipc_bytes(df))

    def free_block(self, block):    self._send(9, struct.pack("<Q", block))   # retire owner-ref
    def key(self, k):               self._send(10, k)   # 32-byte session key, BEFORE tcp sinks
    def listen(self, port, n, apply=0): self._send(11, struct.pack("<IIB", port, n, apply))
    def unlink(self, paths, removedir=0):   # io_uring batch unlink (removedir=1 -> rmdir)
        s_ = paths if isinstance(paths, pl.Series) else pl.Series(list(paths), dtype=pl.Utf8)
        self._send(13, struct.pack("<B", 1 if removedir else 0)
                   + _ipc_bytes(s_.rename("path").to_frame()))
    DONE_SZ = 64                                     # bvm.c: sizeof(Done)
    FRAME_COLS = ("frame", "coff", "clen", "t0_ns", "t1_ns", "in_bytes",
                  "jq_depth", "busy", "worker", "sink")
    PERF_FIELDS = ("rd_bytes", "wr_bytes", "comp_in", "comp_out", "dcomp_out",
                   "ns_read", "ns_comp", "ns_dcomp", "ns_write", "ns_hash",
                   "n_open", "ns_open", "slow_open", "n_meta", "ns_meta",
                   "n_unlink", "ns_unlink", "scan_entries", "ns_scan_stat",
                   "ns_idle", "ns_emit", "ns_qfull", "n_raw_frames", "raw_bytes",
                   "cfr_bytes", "n_probe", "probe_in", "ring_batches", "ring_members",
                   "q_samples", "jq_sum", "jq_full", "jq_empty", "dq_sum", "busy_sum",
                   "pack_used_sum", "blk_live_sum", "jq_cap", "pack_budget", "blk_budget",
                   "wall_ns", "nworkers")
    def read(self):
        while True:                                      # intercept ERROR(3)/STATS(4) transparently
            t, pld = self._q.get()
            if t == 3:
                self._err(pld); continue
            if t == 4:
                self.perf = dict(zip(self.PERF_FIELDS, struct.unpack(f"<{len(self.PERF_FIELDS)}q", pld)))
                self.perf_series.append(self.perf); continue
            return t, pld
    def _err(self, pld):
        err = struct.unpack_from("<i", pld, 0)[0]
        plen = struct.unpack_from("<H", pld, 4)[0]
        self.errors.append((pld[6:6 + plen].decode(errors="replace"), err))

    def _take_done(self, pld):
        """Parse a streamed batch of frame records, append them to the live dump, and return
        their locators. bvm flushes these every DONE_FLUSH frames, so the file on disk is the
        run's state as it happens: a watcher reads it, and after a crash the locators of every
        frame already on disk are still there."""
        nd = struct.unpack_from("<I", pld, 0)[0]
        body = pld[4:4 + nd * self.DONE_SZ]
        for i in range(nd):
            rec = struct.unpack_from("<QQQqqqiiii", body, i * self.DONE_SZ)
            self.locs[rec[0]] = (rec[1], rec[2])         # keep them HERE, not in a return value:
            self.frames.append(rec)                      # poll() discarded what it returned,
                                                         # which lost 2.2M members' locators
        if self.frames_fd is not None:
            os.write(self.frames_fd, body)               # fixed-width; a torn tail is one record

    def ping(self, timeout=120):
        """Round-trip a trivial command so the planner learns whether this executor actually
        STARTED. Under a scheduler a launch can sit queued indefinitely; the planner then
        picks it (zero outstanding wins every least-loaded tie-break), writes to a pipe
        nobody is draining, fills 64 KB, and blocks -- with the executors that DID start
        sitting idle. Observed: 2 of 3 nodes up, 4 minutes, 2.4 GB moved."""
        import tempfile
        d = tempfile.mkdtemp(prefix="quiver-ping-")
        try:
            self.scan_fs(0xFFFF, d)
            end = time.time() + timeout
            while time.time() < end:
                try:
                    t, _pld = self._q.get(timeout=max(0.1, end - time.time()))
                except queue.Empty:
                    return False
                if t is None:
                    return False                         # process died
                if t == 1:
                    return True                          # SRC_EOF: it is alive and draining
            return False
        except (BrokenPipeError, OSError):
            return False
        finally:
            os.rmdir(d)

    def poll(self):
        """Consume whatever this executor has emitted SO FAR (one BSTAT per completed
        frame, plus errors) and return how many frames completed since the last call. Lets
        a planner hand out work against OBSERVED progress instead of a static split — which
        is the whole difference between balanced and long-pole-bound on a real tree.
        finish() still sees everything: a DONE/STATS/EOF message is put straight back."""
        n = 0
        while True:
            try:
                item = self._q.get_nowait()
            except queue.Empty:
                return n
            t, pld = item
            if t == 0:
                self.stats.append(_stat_df(pld)); n += 1
            elif t == 3:
                self._err(pld)
            elif t == 2:                                 # streamed frame records, not the end
                self._take_done(pld)
            elif t == 4:                                 # a periodic sample, not end-of-stream:
                self.perf = dict(zip(self.PERF_FIELDS,   # consume it, or poll() stops draining
                                     struct.unpack(f"<{len(self.PERF_FIELDS)}q", pld)))
                self.perf_series.append(self.perf)
            else:
                self._q.put(item)                        # end-of-stream: finish()'s business
                return n

    def finish(self):
        """Close stdin (no more plans), drain, return {frame_id: (coff, clen)}."""
        self.p.stdin.close()
        locs = self.locs
        while True:
            t, pld = self._q.get()
            if t is None:
                break
            if t == 3:
                self._err(pld); continue
            if t == 4:
                self.perf = dict(zip(self.PERF_FIELDS, struct.unpack(f"<{len(self.PERF_FIELDS)}q", pld)))
                continue
            if t == 0:
                self.stats.append(_stat_df(pld)); continue
            if t == 2:
                self._take_done(pld)
        rc = self.p.wait()
        if self.errors and self.strict:                  # partial failure is an ERROR, never silent
            import errno as _e
            head = ", ".join(f"{p}: {_e.errorcode.get(-c, c)}" for p, c in self.errors[:5])
            raise RuntimeError(f"bvm: {len(self.errors)} op error(s): {head}"
                               + ("..." if len(self.errors) > 5 else ""))
        if rc != 0:
            raise RuntimeError("bvm failed")
        return locs


def _stat_df(payload):
    """STAT payload -> (sid, block_id, DataFrame): the Arrow IPC stream bvm emits, read
    NATIVELY by polars — no per-row Python (the hand-rolled encoding this replaces parsed
    ~17x slower). Full fidelity: the pwalk2/ducl superset schema, incl. directory rows
    (is_dir=1), the inode graph and full st_mode — ducl consumes this directly."""
    sid = struct.unpack_from("<I", payload, 0)[0]
    block = struct.unpack_from("<q", payload, 4)[0]
    return sid, block, pl.read_ipc_stream(io.BytesIO(payload[12:]))


def _parse_stat(payload):
    """Compat view of a STAT payload -> (sid, block_id, [member dicts]) shaped like the old
    wire: FILE/LINK rows only (dir rows dropped) and perm-masked mode. NO planner path uses
    this — every one of them takes the zero-copy Arrow batch through _stat_df. It survives
    only because tests/test_blocks.py needs a block_id out of a STAT message to exercise the
    use-after-free guard; delete it the day that test stops needing one."""
    sid, block, df = _stat_df(payload)
    if df.height:
        df = df.filter(pl.col("is_dir") == 0).with_columns(pl.col("mode") & 0o7777)
    ms = df.select("path", "in_off", "size", "mode", "mtime_ns", "uid", "gid", "link").to_dicts()
    return sid, block, ms


def _tar_hlens(paths, links=None):
    """VECTORIZED tar header-length budget — MUST equal bvm's tar_hlen2() per member:
    base 512 B, + GNU 'L' longname blocks for >100-byte paths, + GNU 'K' longlink blocks
    for >100-byte link targets. The planner's in_off cumsum uses this; bvm writes exactly
    these bytes."""
    import numpy as np
    plen = paths.str.len_bytes().to_numpy().astype(np.int64)
    h = np.where(plen <= 100, 512, 512 + ((plen + 1 + 511) & ~511) + 512)
    if links is not None:
        llen = links.str.len_bytes().fill_null(0).to_numpy().astype(np.int64)
        h = h + np.where(llen <= 100, 0, 512 + ((llen + 1 + 511) & ~511))
    return h


def pack_fs_c(root, out, wal_path, bvm_exe, nworkers=16, level=6, frame_bytes=1 << 20,
              tar_compat=True, predicate=None):
    """INCREMENTAL fs pack in the corrected architecture: bvm (C) WALKS the tree and
    STREAMS STAT up; here (Py) we WAL-diff + frame the delta and stream COPY_FRAME
    plans down; bvm reads + compresses + streams DONE; we assemble the footer + WAL.
    (Change-detection by path+size+mtime; content-dedup is a follow-up.) `tar_compat`
    inlines a planner-formatted tar header before each body (+ a trailing EOF frame) so
    the data section is a valid tar (zstd -dc out | tar x).

    KNOWN EDGE (pre-existing): a `predicate` that excludes a hardlink's CANONICAL path but
    keeps another link to the same inode orphans that link row (unpack skips it) — which
    side is canonical is scan-order-dependent. Filter whole subtrees to avoid it."""
    import numpy as np
    prior = _wal_stat(wal_path).unique(subset="path", keep="last", maintain_order=True)
    if "chunks" not in prior.columns:                    # WALs from before manifests
        prior = prior.with_columns(chunks=pl.lit(None, pl.Binary))
    pf = prior.filter(pl.col("frame") >= 0)
    cursor = int((pf["coff"] + pf["clen"]).max()) if pf.height else 0
    fid = int(pf["frame"].max()) + 1 if pf.height else 0
    b = _Bvm(bvm_exe, nworkers)
    b.sink(0, out, start=cursor)
    b.scan_fs(0, os.path.abspath(root), ignore=load_ignore(os.path.abspath(root)))
    parts, dparts = [], []
    SC_SCHEMA = {"path": pl.Utf8, "size": pl.Int64, "mode": pl.Int64, "mtime_ns": pl.Int64,
                 "uid": pl.Int64, "gid": pl.Int64, "in_off": pl.Int64, "link": pl.Utf8}
    while True:                                          # C streams fs STAT (Arrow batches)
        t, pld = b.read()
        if t is None or t == 1:
            break
        if t == 0:
            df = _stat_df(pld)[2]
            if df.height:
                norm = df.select(
                    path="path", size="size", mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
                    mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64),
                    gid=pl.col("gid").cast(pl.Int64), in_off="in_off", link="link",
                    is_dir="is_dir")
                parts.append(norm.filter(pl.col("is_dir") == 0).drop("is_dir"))
                dparts.append(norm.filter(pl.col("is_dir") == 1).drop("is_dir"))
    sc = pl.concat(parts) if parts else pl.DataFrame(schema=SC_SCHEMA)
    dirs = (pl.concat(dparts) if dparts else pl.DataFrame(schema=SC_SCHEMA)).sort("path")
    if predicate is not None:                            # same PLANNER filter as recompress;
        expr = getattr(predicate, "expr", None)          # vectorized when the filter carries its expr
        sc = sc.filter(expr) if expr is not None else \
            sc.filter(pl.Series([predicate(m) for m in sc.to_dicts()], dtype=pl.Boolean))
    links = sc.filter(pl.col("link") != "")              # sym/hardlinks: footer-only (frame -2/-3)
    files = sc.filter(pl.col("link") == "")
    # WAL-diff as ONE join: unchanged = same path+size+mtime with a live prior locator
    j = files.join(prior.select(path="path", size_p="size", mtime_p="mtime_ns", frame_p="frame",
                                in_off_p="in_off", coff_p="coff", clen_p="clen",
                                mode_p="mode", uid_p="uid", gid_p="gid",
                                digest_p="digest", chunks_p="chunks"),
                   on="path", how="left", maintain_order="left")
    unch = (pl.col("frame_p") >= 0) & (pl.col("size_p") == pl.col("size")) \
        & (pl.col("mtime_p") == pl.col("mtime_ns"))
    reuse = j.filter(unch).select(                        # the PRIOR row, verbatim
        path="path", size="size_p", mode="mode_p", mtime_ns="mtime_p", uid="uid_p", gid="gid_p",
        frame="frame_p", in_off="in_off_p", coff="coff_p", clen="clen_p",
        digest="digest_p", link=pl.lit("", pl.Utf8), chunks="chunks_p")
    topack = j.filter(~unch.fill_null(False)).select(files.columns)
    # ONE TYPED member stream: dirs first (parents lexicographically precede children),
    # then files, then links LAST (a hardlink entry must follow its target in the tar).
    # Dirs+links are bodyless — full tar parity through the same unified path.
    typed = pl.concat([
        dirs.with_columns(type=pl.lit(5, pl.UInt8)),
        topack.with_columns(type=pl.lit(0, pl.UInt8)),
        links.with_columns(type=pl.when(pl.col("in_off") == 1).then(1).otherwise(2).cast(pl.UInt8)),
    ]) if tar_compat else topack.with_columns(type=pl.lit(0, pl.UInt8))
    types = typed["type"].to_numpy()
    sizes = np.where(types == 0, typed["size"].to_numpy(), 0)
    hlens = _tar_hlens(typed["path"], typed["link"]) if tar_compat else np.zeros(sizes.size, np.int64)
    step = hlens + (((sizes + 511) & ~511) if tar_compat else sizes)   # header+body budget
    # frame cut: exact greedy via searchsorted over the step cumsum — O(frames), not O(members)
    pre = np.zeros(step.size + 1, np.int64); np.cumsum(step, out=pre[1:])
    bounds = [0]
    while bounds[-1] < step.size:
        i = bounds[-1]
        e = int(np.searchsorted(pre, pre[i] + frame_bytes, side="right")) - 1
        bounds.append(max(e, i + 1))
    nframes = len(bounds) - 1
    fidx = np.repeat(np.arange(nframes, dtype=np.int64), np.diff(bounds))
    pdf = typed.with_columns(fid=pl.Series(fidx + fid), mode32=pl.col("mode").cast(pl.Int32))
    ba = np.asarray(bounds, dtype=np.int64)
    c = 0
    while c < len(bounds) - 1:                           # chunks CUT ON FRAME BOUNDS: a split frame
        e = int(np.searchsorted(ba, ba[c] + (1 << 20), side="right")) - 1   # would emit its id twice
        e = max(e, c + 1)
        b.pack_files_df(level, os.path.abspath(root),
                        pdf.slice(int(ba[c]), int(ba[e] - ba[c])).select(
                            fid="fid", type="type", mode="mode32", size="size", mtime_ns="mtime_ns",
                            uid=pl.col("uid").cast(pl.Int32), gid=pl.col("gid").cast(pl.Int32),
                            src_off=pl.lit(0, pl.Int64), path="path", link="link"),
                        tar_compat=tar_compat)
        c = e
    locs = b.finish()
    digs = [d.filter(pl.col("digest") != -1).select("path", "digest", "chunks")
            for _s2, _blk, d in b.stats]
    digs = (pl.concat(digs) if digs else
            pl.DataFrame(schema={"path": pl.Utf8, "digest": pl.Int64, "chunks": pl.Binary})
            ).unique(subset="path", keep="last")
    # footer locators, VECTORIZED: within-frame exclusive cumsum of (hlen + laid-out size)
    gcum = pre
    fstart = gcum[np.asarray(bounds[:-1], dtype=np.int64)]
    in_off = gcum[:-1] - np.repeat(fstart, np.diff(bounds)) + hlens
    _missing = [k for k in range(nframes) if fid + k not in locs]
    if _missing:
        raise RuntimeError(f"pack: {len(_missing)} frame(s) unrecorded (first {_missing[:3]}); "
                           f"bvm errors: {b.errors[:3]}")
    coff_a = np.fromiter((locs[fid + k][0] for k in range(nframes)), np.int64, nframes)
    clen_a = np.fromiter((locs[fid + k][1] for k in range(nframes)), np.int64, nframes)
    footer_all = typed.with_columns(
        frame=pl.Series(fidx + fid), in_off_out=pl.Series(in_off),
        coff=pl.Series(np.repeat(coff_a, np.diff(bounds))),
        clen=pl.Series(np.repeat(clen_a, np.diff(bounds))),
        digest=pl.lit(-1, pl.Int64))
    packed = footer_all.filter(pl.col("type") == 0).with_columns(
        in_off=pl.col("in_off_out")).select(STAT_COLS).drop("digest").join(
        digs, on="path", how="left", maintain_order="left").with_columns(   # worker digests
        digest=pl.col("digest").fill_null(-1))
    drows = footer_all.filter(pl.col("type") == 5).select(   # dir rows: frame -1 (metadata only)
        path="path", size=pl.lit(0, pl.Int64), mode="mode", mtime_ns="mtime_ns",
        uid="uid", gid="gid", frame=pl.lit(-1, pl.Int64), in_off=pl.lit(-1, pl.Int64),
        coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link=pl.lit("", pl.Utf8),
        chunks=pl.lit(None, pl.Binary))
    lrows = links.select(                                 # footer link rows (frame -2 sym / -3 hard)
        path="path", size="size", mode="mode", mtime_ns="mtime_ns", uid="uid", gid="gid",
        frame=pl.when(pl.col("in_off") == 1).then(-3).otherwise(-2).cast(pl.Int64),
        in_off=pl.lit(-1, pl.Int64), coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link="link", chunks=pl.lit(None, pl.Binary))
    end = max(cursor, int((coff_a + clen_a).max()) if nframes else cursor)
    wal = open(wal_path, "ab" if prior.height else "wb")
    pk = packed
    fidarr = pk["frame"].to_numpy()                      # file rows, frame-sorted by construction
    lo = np.searchsorted(fidarr, np.arange(nframes) + fid, side="left")
    hi = np.searchsorted(fidarr, np.arange(nframes) + fid, side="right")
    for k in range(nframes):                             # WAL: the frame's FILE rows, O(1) slices
        _wal_append(wal, fid + k, int(coff_a[k] + clen_a[k]), pk.slice(int(lo[k]), int(hi[k] - lo[k])))
    wal.close()
    fd = os.open(out, os.O_RDWR); os.ftruncate(fd, end)
    if tar_compat:
        end = _tar_eof_frame(fd, end, level)             # trailing EOF frame -> a clean tar
    cols12 = STAT_COLS + ["chunks"]
    write_footer(fd, end, [reuse.select(cols12), packed.select(cols12),
                           drows.select(cols12), lrows.select(cols12)]); os.close(fd)
    seen = sc["path"]
    return dict(unchanged=reuse.height, packed=topack.height, dirs=dirs.height,
                symlinks=links.filter(pl.col("in_off") != 1).height,
                hardlinks=links.filter(pl.col("in_off") == 1).height,
                frames=nframes, deleted=prior.filter(~pl.col("path").is_in(seen.to_list())).height)


def _cat_manifests(blobs):
    """Concatenate per-piece CDC manifests into one manifest for the whole member. Valid
    because each piece is chunked content-defined in its own right; the only difference from
    manifesting the file in one go is a forced chunk boundary at each piece cut (one per
    frame_cap, i.e. ~0.0005% of chunks at 256 MB / 64 KB). Returns None if any piece is
    missing a manifest (member smaller than CDC_CUTOFF, or an errored frame)."""
    recs, n = [], 0
    for b_ in blobs:
        if not b_:
            return None
        (k,) = struct.unpack_from("<I", b_, 4)
        recs.append(bytes(b_[8:8 + k * 20])); n += k
    return struct.pack("<BBBBI", 2, 0, 0, 0, n) + b"".join(recs)   # hashid 2 = BLAKE3-128


def _parse_manifest(m):
    """chunks binary -> (hashid, [(len, 16B digest), ...]).
    [u8 hash_id][3 pad][u32 n][n x (u32 len, 16B id)]; hash_id 2 = BLAKE3-128. The count
    lives at offset 4 ONLY because of that header — an older 4-byte-header blob would read
    its first chunk's length as the chunk count and silently mis-delta, so refuse it."""
    if m[0] != 2:
        raise ValueError(f"manifest hash_id {m[0]} is not 2 (BLAKE3-128): refusing to parse")
    (n,) = struct.unpack_from("<I", m, 4)
    out = []
    off = 8
    for _ in range(n):
        (cl,) = struct.unpack_from("<I", m, off)
        out.append((cl, bytes(m[off + 4:off + 20]))); off += 20
    return m[0], out


EXT_V2 = 0x80000000                                       # count high bit == records carry `shard`


def _pack_extents(ex):
    """[(coff, clen, in_off, len, out_off, shard), ...] -> extents binary (v2, 48B records).
    `shard` names WHICH sink file of a sharded store holds those bytes; v1 blobs (40B, no
    shard) still parse and read as shard 0, so old stores keep working unchanged."""
    ex = [tuple(e) + (0,) * (6 - len(e)) for e in ex]
    return struct.pack("<I", EXT_V2 | len(ex)) + b"".join(struct.pack("<6Q", *e) for e in ex)


def _parse_extents(b):
    (n,) = struct.unpack_from("<I", b, 0)
    if n & EXT_V2:
        n &= ~EXT_V2
        return [struct.unpack_from("<6Q", b, 4 + i * 48) for i in range(n)]
    return [struct.unpack_from("<QQQQQ", b, 4 + i * 40) + (0,) for i in range(n)]


def _old_chunk_locator(prow):
    """For a prior member row, return fn(member_off, length) -> (coff, clen, frame_off, shard):
    where those member bytes live. Direct members: one frame; extent members: walk their
    extent list (chunks never straddle extents — extents are built on chunk boundaries)."""
    if prow["frame"] != -4:
        coff, clen, base = prow["coff"], prow["clen"], prow["in_off"]
        sh = prow.get("shard") or 0
        return lambda mo, ln: (coff, clen, base + mo, sh)
    ex = _parse_extents(prow["extents"])            # (coff, clen, in_off, len, out_off, shard)
    # BISECT, not a scan: extents are emitted in increasing out_off, and this is called once
    # per chunk of the new file. A 50 GB member split at 16 MB has 3,200 extents and ~800k
    # chunks — linear lookup would be 2.6e9 comparisons per file.
    import bisect
    starts = [e[4] for e in ex]
    def find(mo, ln):
        i = bisect.bisect_right(starts, mo) - 1
        if i < 0:
            raise KeyError(mo)
        coff, clen, io_, l_, oo, sh = ex[i]
        if not (oo <= mo < oo + l_):
            raise KeyError(mo)
        assert mo + ln <= oo + l_, "chunk straddles extents"
        return (coff, clen, io_ + (mo - oo), sh)
    return find


CDC_CUTOFF = 128 * 1024                                  # == bvm.c: below this, no manifest

# How many bytes of one member a worker materializes at once. Chosen on TWO axes, because the
# first one I measured turned out not to matter:
#   ratio -- flat. bench/framecap on real data: 7.0016x at 16 MB vs 7.0054x at 4 GB (0.06%),
#            and a whole-subtree store differs by 0.02% from 16 MB to 256 MB.
#   write  -- this is the axis that pays. ~/eot, -j128, 16 sinks, flush 64 MB:
#            16 MB 83.7s | 32 MB 87.5s | 64 MB 78.2s | 128 MB 77.7s | 256 MB 75.3s
#            (32 MB above 16 MB is noise -- run-to-run spread here is 5-9%, so read this as
#             ~7-8% for 16 -> 64 MB, not the 15% a single measurement suggested.)
# 64 MB takes most of that for 8 GB across 128 workers; 256 MB buys ~4% more for 32 GB and
# re-creates the memory pressure splitting members exists to avoid. An earlier default derived
# the cap from zstd's window per level -- that was reasoning from the ratio axis, which is flat.
DEFAULT_FRAME_CAP = 64 << 20


def _pieces(off, ln, cap):
    """Split a byte run into <= cap pieces: [(src_off, len), ...]. A worker materializes a
    member's whole slot, so `cap` is what actually bounds pack memory.

    The last piece is FOLDED into its predecessor when the remainder is under CDC_CUTOFF:
    bvm emits no CDC manifest for a member that small, one missing piece manifest makes
    `_cat_manifests` return None, and a member with no `chunks` is silently undeltable
    forever after. That is a `CDC_CUTOFF/cap` chance per split file — 12.5% at a 1 MB cap
    (measured: a 600 MB file re-sent whole, 225 MB, instead of deltaing to 0.17 MB)."""
    out = []
    while ln > 0:
        n = min(cap, ln)
        if 0 < ln - n < CDC_CUTOFF:                      # absorb an undersized tail
            n = ln
        out.append((off, n)); off += n; ln -= n
    return out


def shard_paths(out, shards):
    """A sharded store is `out` (shard 0, which also holds the footer) + out.1 .. out.N-1."""
    return [out] + [f"{out}.{k}" for k in range(1, shards)]


def store_files(out):
    """The physical files of a store: `out` (shard 0, carrying the footer) plus however many
    consecutive `out.1`, `out.2`, ... sinks exist. A single-sink store is just [out], so every
    caller can treat sharded and unsharded stores the same way."""
    paths, k = [out], 1
    while os.path.exists(f"{out}.{k}"):
        paths.append(f"{out}.{k}"); k += 1
    return paths


def _stats_table(bvms):
    """Every executor's BSTAT batches as ONE DataFrame.

    bvm emits one Arrow batch per FRAME, so a whole-tree run leaves ~185,000 small frames per
    executor. Filtering and selecting each one costs 0.85 ms -- trivial alone, 470 s across
    three executors, which was 43% of a footer phase that had become 46% of the run. Concat
    is not the expensive part (0.3 s for 60k); doing per-frame work 185,000 times is. So:
    concatenate first, then filter once, and attach the frame id by repeating each batch's id
    over its own height instead of a with_columns per batch."""
    import numpy as np
    dfs, blks = [], []
    for b in bvms:
        for _sid, blk, d in b.stats:
            if d.height:
                dfs.append(d); blks.append(int(blk))
    if not dfs:
        return None
    heights = np.fromiter((d.height for d in dfs), np.int64, len(dfs))
    fid = np.repeat(np.asarray(blks, dtype=np.int64), heights)
    return pl.concat(dfs, how="vertical_relaxed").with_columns(fid=pl.Series(fid))


def _split_extent_rows(big, fall, fullpaths, chunk_df, frame_cap):
    """EXTENT rows for split members, built set-at-a-time.

    The obvious loop -- for each big member, for each of its pieces, look up the locator and
    struct.pack an extent -- is ~380k Python iterations on a whole-tree backup (9,346 members
    x ~40 pieces), and it dominated a footer phase that cost 885 s, 29% of a 50-minute run.
    Here it is joins and one group_by; the only per-member work left is a numpy .tobytes()
    and a manifest concat. Returns (rows, lost_paths)."""
    import numpy as np
    if not big.height:
        return [], []
    lit = (fall.filter((pl.col("type") == 0) & ~pl.col("path").is_in(list(fullpaths)))
           .select("path", "src_off", "coff", "clen", "in_off_out", "shard", "fid", "size")
           .sort(["path", "src_off"]))
    lit = (lit.join(chunk_df, on=["fid", "path"], how="left")
           if chunk_df is not None and chunk_df.height
           else lit.with_columns(chunks=pl.lit(None, pl.Binary)))
    lit = lit.with_columns(out_off=(pl.col("size").cum_sum() - pl.col("size")).over("path"))
    g = lit.group_by("path", maintain_order=True).agg(
        pl.col("coff"), pl.col("clen"), pl.col("in_off_out"), pl.col("size"),
        pl.col("out_off"), pl.col("shard"), pl.col("chunks"), pl.len().alias("npiece"))
    meta = {r["path"]: r for r in big.iter_rows(named=True)}
    rows, lost = [], []
    seen = set()
    for r in g.iter_rows(named=True):
        m = meta.get(r["path"])
        if m is None:
            continue
        seen.add(r["path"])
        if r["npiece"] != len(_pieces(0, m["size"], frame_cap)) or min(r["coff"]) < 0:
            lost.append(r["path"]); continue         # a piece's frame errored: member is lost
        a = np.empty((r["npiece"], 6), dtype="<u8")  # coff, clen, in_off, len, out_off, shard
        a[:, 0] = r["coff"]; a[:, 1] = r["clen"]; a[:, 2] = r["in_off_out"]
        a[:, 3] = r["size"]; a[:, 4] = r["out_off"]; a[:, 5] = r["shard"]
        rows.append(dict(path=r["path"], size=m["size"], mode=m["mode"], mtime_ns=m["mtime_ns"],
                         uid=m["uid"], gid=m["gid"], frame=-4, in_off=-1, coff=-1, clen=-1,
                         digest=-1, link="", chunks=_cat_manifests(r["chunks"]),
                         extents=struct.pack("<I", EXT_V2 | r["npiece"]) + a.tobytes(),
                         shard=0))
    lost += [p for p in meta if p not in seen]       # no pieces at all: every frame errored
    return rows, lost


def assemble_footer(fpath, pdf, bounds, pre, locs, shard_of, big, small, links,
                    allst, paths, frame_cap, time_ns=None, rows_per_batch=1 << 12):
    """THE footer phase, as one callable: plan + locators -> the footer file on disk.

    Extracted from backup_multi so it can be replayed and profiled without running a backup
    (bench/footerbench.py). It is worth isolating: on a whole-tree run this phase is serial
    after all packing and took 1093 s of 2357 -- 46% -- and every guess about where that time
    went was wrong until it could be run on its own.

    Returns dict(lost_paths, ends, extent_rows, footer_bytes)."""
    import numpy as np
    # STAGE TIMERS. bench/footerbench.py replays this in ~8 s with faithful inputs, writing to
    # the same filesystem, while the real run spends 65 s here. The cost lives in the state of
    # the LIVE process -- allst arrives as ~248k separate Arrow batches, so its string columns
    # are badly fragmented for the joins below -- and replay cannot reproduce it. Measure here
    # rather than infer from the difference, which has been wrong three times.
    _T = {}
    _tk = [time.time()]
    def _lap(name):
        _T[name] = round(time.time() - _tk[0], 2); _tk[0] = time.time()
    nframes = len(bounds) - 1
    gcum = pre
    fstart = gcum[np.asarray(bounds[:-1], dtype=np.int64)]
    in_off = gcum[:-1] - np.repeat(fstart, np.diff(bounds))
    missing = [k for k in range(nframes) if k not in locs]
    coff_a = np.fromiter((locs.get(k, (-1, 0))[0] for k in range(nframes)), np.int64, nframes)
    clen_a = np.fromiter((locs.get(k, (-1, 0))[1] for k in range(nframes)), np.int64, nframes)
    fall = pdf.with_columns(in_off_out=pl.Series(in_off),
                            coff=pl.Series(np.repeat(coff_a, np.diff(bounds))),
                            clen=pl.Series(np.repeat(clen_a, np.diff(bounds))),
                            shard=pl.Series(np.repeat(shard_of, np.diff(bounds))))
    _lap("locators")
    lost_paths = fall.filter((pl.col("type") == 0) & (pl.col("coff") < 0))["path"].to_list()
    fall = fall.filter(pl.col("coff") >= 0)
    dd = (allst.filter(pl.col("digest") != -1) if allst is not None
          else pl.DataFrame(schema={"path": pl.Utf8, "digest": pl.Int64,
                                    "chunks": pl.Binary, "fid": pl.Int64}))
    chunk_df = dd.select("path", "chunks", "fid") if dd.height else None
    digs = dd.select("path", "digest", "chunks").unique(subset="path", keep="last")
    _lap("digests")
    fullpaths = set(small["path"].to_list())
    _lap("fullpaths_set")
    packed = (fall.filter((pl.col("type") == 0) & pl.col("path").is_in(list(fullpaths)))
              .with_columns(frame=pl.col("fid"), in_off=pl.col("in_off_out"),
                            digest=pl.lit(-1, pl.Int64)).select(STAT_COLS + ["shard"])
              .drop("digest").join(digs, on="path", how="left", maintain_order="left")
              .with_columns(digest=pl.col("digest").fill_null(-1),
                            extents=pl.lit(None, pl.Binary)))
    _lap("packed_join")
    drows = []
    _r, _lost = _split_extent_rows(big, fall, fullpaths, chunk_df, frame_cap)
    drows.extend(_r); lost_paths.extend(_lost)
    _lap("extent_rows")
    C13 = STAT_COLS + ["chunks", "extents", "shard"]
    # COLUMNAR, not row-wise: pl.DataFrame(list_of_dicts) walks every row inferring types, and
    # these rows carry `chunks` blobs that are whole concatenated manifests (the largest
    # member's is ~223 MB in one cell). Cheap either way in a fresh process -- this span costs
    # 0.07 s replayed vs 22.4 s live -- so the shape is defensive, not the fix.
    ddf = None
    if drows:
        ddf = pl.DataFrame({k: [r_[k] for r_ in drows] for k in drows[0]}).select(C13)
    _lap("ddf_build")
    dirsr = fall.filter(pl.col("type") == 5).select(
        path="path", size=pl.lit(0, pl.Int64), mode="mode", mtime_ns="mtime_ns", uid="uid",
        gid="gid", frame=pl.lit(-1, pl.Int64), in_off=pl.lit(-1, pl.Int64),
        coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64), digest=pl.lit(-1, pl.Int64),
        link=pl.lit("", pl.Utf8), chunks=pl.lit(None, pl.Binary), extents=pl.lit(None, pl.Binary),
        shard=pl.lit(0, pl.Int64))
    _lap("dirsr")
    linksr = links.select(
        path="path", size="size", mode="mode", mtime_ns="mtime_ns", uid="uid", gid="gid",
        frame=pl.when(pl.col("in_off") == 1).then(-3).otherwise(-2).cast(pl.Int64),
        in_off=pl.lit(-1, pl.Int64), coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link="link", chunks=pl.lit(None, pl.Binary),
        extents=pl.lit(None, pl.Binary), shard=pl.lit(0, pl.Int64))
    # SEPARATE FOOTER. With several shards nothing is gained by hiding the index inside one
    # of them: it forced the footer to be written after shard 0's last frame, and made shard 0
    # the only shard that is both data and index. Its own file has base offset 0 and no
    # ordering relationship to any shard at all. nockidx.footer_path() finds it.
    # Per-shard data end from the LOCATORS, not from stat(): the planner never wrote these
    # files — remote executors did — and a distributed filesystem happily serves the planner
    # a cached size from before those writes (measured: 11 of 12 shards reported ~0 while
    # holding 247 MB). The footer's own locators are authoritative and cost nothing.
    _lap("linksr")
    ends = [0] * len(paths)
    for k in range(nframes):
        if k in locs:
            sh_ = int(shard_of[k])
            ends[sh_] = max(ends[sh_], int(locs[k][0] + locs[k][1]))
    end = 0                                              # the footer file starts at 0
    fd = os.open(fpath, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    ps = [packed.select(C13), dirsr.select(C13), linksr.select(C13)]
    if ddf is not None:
        ps.append(ddf)
    write_footer(fd, end, ps, prev_off=None, rows_per_batch=rows_per_batch, part_cuts=True,
                 snap_time_ns=time_ns if time_ns is not None else time.time_ns())
    os.close(fd)
    _lap("write_footer")
    return dict(lost_paths=lost_paths, ends=ends, extent_rows=len(drows), stages=_T,
                dirs=dirsr.height, links=linksr.height,
                footer_bytes=os.path.getsize(fpath))


def backup(root, out, bvm_exe, nworkers=16, level=6, time_ns=None, excludes=None, strict=True,
           shards=1, frame_cap=None, max_member_bytes=1 << 40):
    """APPEND-ONLY incremental backup of `root` into `out` (a nock snapshot chain).
    Snapshot N appends ONLY new bytes: unchanged files carry their prior footer rows
    verbatim (lazy references into old frames — never recompressed, never read); changed
    big files are CDC-diffed against the prior manifest and store literal runs only,
    becoming EXTENT members (frame -4) that stitch old-frame chunks + new literals;
    everything else (small/new files, dirs, links) goes through the normal typed pack
    path. A member never occupies more than `frame_cap` bytes of any one frame: bigger files
    are cut into pieces of that size, each its own frame, stitched by an EXTENT row — so
    worker memory is bounded by frame_cap, not by the largest file in the tree.

    frame_cap defaults to 8x zstd's match window FOR THIS LEVEL (16 MB at L<=6, 32 at L<=15,
    64 above), floored at 16 MB. A frame restarts the window and the entropy tables, so the
    knee moves with the level, not with the file: `bench/framecap` sweeps (level x cap) on six
    real corpora and finds knees of 1 MB (model weights, audio — nothing to match) up to 64 MB
    (JSONL logs, whose redundancy is long-range). At L6 a 16 MB cap gives up 0.01-0.89% of
    ratio depending on the data; 1 MB would give up 2.4-12.4%, and peak worker memory scales
    linearly with the cap (32 GB -> 2 GB at 128 workers going 256 MB -> 16 MB). Smaller caps
    also mean more extents per member (48 B each) and more forced chunk boundaries.
    Each footer records the previous snapshot's end — every snapshot stays
    restorable (`nockidx.snapshots` + `unpack_c(at=)`). The store is quiver-native
    (extent members have no tar view). Returns a summary dict."""
    import numpy as np
    if frame_cap is None:
        frame_cap = DEFAULT_FRAME_CAP
    root_abs = os.path.abspath(root)
    from ..nock import nockidx as _nx
    exists = os.path.exists(out) and os.path.getsize(out) > 0
    prev_end = os.path.getsize(out) if exists else 0
    prior_ents, prior_batches = [], []
    prior_leaves, prior_commit = None, None
    if exists:
        meta = _nx.read_directory(out, with_prev=True)
        prior_ents = meta[0] or []
        prior_leaves, prior_commit = meta[5], meta[4]
        for pb in _nx.iter_batches(out):
            pb = pb.rename({a: c for a, c in (("frame_coff", "coff"), ("frame_clen", "clen"))
                            if a in pb.columns})
            for c, t in (("chunks", pl.Binary), ("extents", pl.Binary)):
                if c not in pb.columns:
                    pb = pb.with_columns(pl.lit(None, t).alias(c))
            if "digest" not in pb.columns:
                pb = pb.with_columns(digest=pl.lit(-1, pl.Int64))
            if "shard" not in pb.columns:
                pb = pb.with_columns(shard=pl.lit(0, pl.Int64))
            prior_batches.append(pb)
    prior = pl.concat(prior_batches) if prior_batches else _stat_rows([])
    for c, t in (("chunks", pl.Binary), ("extents", pl.Binary)):
        if c not in prior.columns:
            prior = prior.with_columns(pl.lit(None, t).alias(c))
    if "shard" not in prior.columns:                     # stores written before sharding
        prior = prior.with_columns(shard=pl.lit(0, pl.Int64))
    if not exists:
        open(out, "wb").close()
    ign = load_ignore(root_abs, excludes)                # .quiverignore + explicit: pruned in C
    # SHARDED SINKS: parallel filesystems serialize concurrent writes to ONE inode (measured
    # 3.2x on WEKA: 1.81 -> 5.83 GB/s at 32 sinks). Frames round-robin across `shards` files;
    # every STAT row records which shard holds it. Shard 0 also carries the footer, so the
    # store is still addressed by a single path.
    paths = shard_paths(out, shards)
    prev_ends = [os.path.getsize(p) if os.path.exists(p) else 0 for p in paths]
    b = _Bvm(bvm_exe, nworkers)
    b.strict = strict                                    # live trees race: strict=False collects
                                                         # per-file errors into the report instead
    b.frames_fd = os.open(out + ".frames.bin", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    for k, p in enumerate(paths):
        if not os.path.exists(p):
            open(p, "wb").close()
        b.sink(k, p, start=prev_ends[k])                 # APPEND after each shard's tail
    b.scan_fs(0, root_abs, ignore=ign)
    parts, dparts = [], []
    while True:
        t, pld = b.read()
        if t is None or t == 1:
            break
        if t == 0:
            df = _stat_df(pld)[2]
            if df.height:
                nm = df.select(path="path", size="size", mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
                               mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64),
                               gid=pl.col("gid").cast(pl.Int64), in_off="in_off", link="link",
                               is_dir="is_dir")
                parts.append(nm.filter(pl.col("is_dir") == 0).drop("is_dir"))
                dparts.append(nm.filter(pl.col("is_dir") == 1).drop("is_dir"))
    SCH = {"path": pl.Utf8, "size": pl.Int64, "mode": pl.Int64, "mtime_ns": pl.Int64,
           "uid": pl.Int64, "gid": pl.Int64, "in_off": pl.Int64, "link": pl.Utf8}
    sc = pl.concat(parts) if parts else pl.DataFrame(schema=SCH)
    dirs = (pl.concat(dparts) if dparts else pl.DataFrame(schema=SCH)).sort("path")
    links = sc.filter(pl.col("link") != "")
    files = sc.filter(pl.col("link") == "")
    pf = prior.filter((pl.col("frame") >= 0) | (pl.col("frame") == -4)).unique(
        subset="path", keep="last", maintain_order=True)
    j = files.join(pf.select(path="path", size_p="size", mtime_p="mtime_ns",
                             frame_p="frame", in_off_p="in_off", coff_p="coff", clen_p="clen",
                             mode_p="mode", uid_p="uid", gid_p="gid", digest_p="digest",
                             chunks_p="chunks", extents_p="extents", shard_p="shard"),
                   on="path", how="left", maintain_order="left")
    unch = (pl.col("frame_p").is_not_null()) & (pl.col("size_p") == pl.col("size"))         & (pl.col("mtime_p") == pl.col("mtime_ns"))
    carried = j.filter(unch).select(                     # LAZY: prior rows verbatim
        path="path", size="size_p", mode="mode_p", mtime_ns="mtime_p", uid="uid_p", gid="gid_p",
        frame="frame_p", in_off="in_off_p", coff="coff_p", clen="clen_p",
        digest="digest_p", link=pl.lit("", pl.Utf8), chunks="chunks_p", extents="extents_p",
        shard="shard_p")
    todo = j.filter(~unch.fill_null(False))
    deltable = todo.filter(pl.col("chunks_p").is_not_null() & (pl.col("size") >= 128 * 1024))
    full = todo.filter(~(pl.col("chunks_p").is_not_null() & (pl.col("size") >= 128 * 1024)))
    # ---- manifests for delta candidates (parallel C jobs; one message per call) ----
    man = {}
    dp = deltable["path"].to_list()
    CH = 256
    for i in range(0, len(dp), CH):
        b.manifest(0, root_abs, dp[i:i + CH])
    for _ in range(0, len(dp), CH):
        while True:
            t, pld = b.read()
            if t is None:
                raise RuntimeError("bvm died during MANIFEST")
            if t == 0:
                d = _stat_df(pld)[2]
                for r in d.iter_rows(named=True):
                    man[r["path"]] = (r["digest"], r["chunks"], r["size"])
                break
    fid = 0
    pack_rows = []                                       # typed PACK rows (fid assigned later)
    delta_meta = []                                      # (path, row, new_manifest, segments)
    appended_literals = 0
    n_nomanifest = [0]                                   # prior members with no usable manifest
    for r in deltable.iter_rows(named=True):
        if r["path"] not in man or man[r["path"]][1] is None:
            full = pl.concat([full, deltable.filter(pl.col("path") == r["path"])])
            continue
        # The OLD manifest can be absent or empty -- a member the prior run stored without a
        # digest, or a zero-chunk blob. There is nothing to delta against, so re-pack it whole
        # instead of indexing into an empty buffer (this crashed the first real incremental
        # against a 4.9 TB store with IndexError in _parse_manifest). The NEW manifest is
        # already guarded above; this is the same fallback for the other side.
        oldman = r["chunks_p"]
        if oldman is None or len(oldman) < 8:
            full = pl.concat([full, deltable.filter(pl.col("path") == r["path"])])
            n_nomanifest[0] += 1
            continue
        dg, newman, nsz = man[r["path"]]
        _hid, newch = _parse_manifest(newman)
        _hid2, oldch = _parse_manifest(oldman)
        loc = _old_chunk_locator({"frame": r["frame_p"], "coff": r["coff_p"], "clen": r["clen_p"],
                                  "in_off": r["in_off_p"], "extents": r["extents_p"],
                                  "shard": r["shard_p"]})
        oldpos = {}
        mo = 0
        for cl, d16 in oldch:
            oldpos.setdefault(d16, (mo, cl)); mo += cl
        segs = []                             # ('old', coff, clen, foff, len, shard) | ('new', off, len)
        no = 0
        for cl, d16 in newch:
            hit = oldpos.get(d16)
            if hit is not None and hit[1] == cl:
                coff, clen, foff, fsh = loc(hit[0], cl)
                if segs and segs[-1][0] == "old" and segs[-1][1] == coff and segs[-1][2] == clen                         and segs[-1][3] + segs[-1][4] == foff:
                    segs[-1] = ("old", coff, clen, segs[-1][3], segs[-1][4] + cl, fsh)
                else:
                    segs.append(("old", coff, clen, foff, cl, fsh))
            else:
                if segs and segs[-1][0] == "new" and segs[-1][1] + segs[-1][2] == no:
                    segs[-1] = ("new", segs[-1][1], segs[-1][2] + cl)
                else:
                    segs.append(("new", no, cl))
                appended_literals += cl
            no += cl
        delta_meta.append((r, dg, newman, segs))
    # ---- typed pack stream: dirs + full files + links + delta literal runs ----
    # SPLIT oversized members. A member is materialized whole by the worker packing it, so one
    # giant file demanded a buffer its own size — a 50 TiB sparse image ENOMEM'd, and multi-GB
    # checkpoints drove the pack-budget throttle (and an 862 GB OOM). Cutting at frame_cap
    # reuses the delta path verbatim: each piece is a `src_off` run in its own frame, and the
    # member becomes an EXTENT row (frame -4) stitching them — which `unpack` already restores,
    # `gc` already remaps, and the next snapshot can already delta against.
    # SPARSE-FILE GUARD. Splitting a member into frame_cap pieces means an oversized
    # APPARENT size no longer OOMs -- it reads that many bytes of zeros instead. WEKA
    # reports neither SEEK_HOLE/SEEK_DATA (EINVAL) nor FIEMAP (EOPNOTSUPP) nor honest
    # st_blocks, so there is no way to detect the holes: a 50 TiB sparse disk image
    # silently became 3.28M pieces and 86% of a whole-tree backup plan. Refuse it loudly
    # instead; raise max_member_bytes if you really do have a member that big.
    huge = full.filter(pl.col("size") > max_member_bytes)
    if huge.height:
        full = full.filter(pl.col("size") <= max_member_bytes)
    big = full.filter(pl.col("size") > frame_cap)
    small = full.filter(pl.col("size") <= frame_cap)
    typed = pl.concat([
        dirs.with_columns(type=pl.lit(5, pl.UInt8), src_off=pl.lit(0, pl.Int64)),
        small.select(list(SCH)).with_columns(type=pl.lit(0, pl.UInt8), src_off=pl.lit(0, pl.Int64)),
        links.with_columns(type=pl.when(pl.col("in_off") == 1).then(1).otherwise(2).cast(pl.UInt8),
                           src_off=pl.lit(0, pl.Int64)),
    ])
    lit_rows = []
    for r, dg, newman, segs in delta_meta:
        for sg in segs:
            if sg[0] == "new":
                for o2, n2 in _pieces(sg[1], sg[2], frame_cap):
                    lit_rows.append(dict(path=r["path"], size=n2, mode=r["mode"],
                                         mtime_ns=r["mtime_ns"], uid=r["uid"], gid=r["gid"],
                                         in_off=0, link="", type=0, src_off=o2))
    for r in big.iter_rows(named=True):                  # oversized whole files: same shape as
        for o2, n2 in _pieces(0, r["size"], frame_cap):  # a delta literal run, one frame each
            lit_rows.append(dict(path=r["path"], size=n2, mode=r["mode"], mtime_ns=r["mtime_ns"],
                                 uid=r["uid"], gid=r["gid"], in_off=0, link="", type=0,
                                 src_off=o2))
    if lit_rows:
        typed = pl.concat([typed, pl.DataFrame(lit_rows).select(typed.columns).cast(
            {c: typed.schema[c] for c in typed.columns})])
    sizes = np.where(typed["type"].to_numpy() == 0, typed["size"].to_numpy(), 0)
    pre = np.zeros(sizes.size + 1, np.int64); np.cumsum(sizes, out=pre[1:])
    bounds = [0]
    while bounds[-1] < sizes.size:
        i = bounds[-1]
        e = int(np.searchsorted(pre, pre[i] + (1 << 20), side="right")) - 1
        bounds.append(max(e, i + 1))
    nframes = len(bounds) - 1
    fidx = np.repeat(np.arange(nframes, dtype=np.int64), np.diff(bounds))
    pdf = typed.with_columns(fid=pl.Series(fidx))
    ba = np.asarray(bounds, dtype=np.int64)
    c = 0
    while c < len(bounds) - 1:
        e = max(int(np.searchsorted(ba, ba[c] + (1 << 20), side="right")) - 1, c + 1)
        sl = pdf.slice(int(ba[c]), int(ba[e] - ba[c])).select(
            fid="fid", type="type", mode=pl.col("mode").cast(pl.Int32), size="size",
            mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int32), gid=pl.col("gid").cast(pl.Int32),
            src_off="src_off", path="path", link="link")
        for k in range(shards):                          # frame fid -> sink fid % shards:
            part = sl.filter(pl.col("fid") % shards == k) if shards > 1 else sl
            if part.height:                              # frames are ~frame_bytes each, so
                b.pack_files_df(level, root_abs, part,   # round-robin balances bytes too
                                sink_id=k, tar_compat=0)
        c = e
    locs = b.finish()
    # ---- footer assembly ----
    gcum = pre
    fstart = gcum[np.asarray(bounds[:-1], dtype=np.int64)]
    in_off = gcum[:-1] - np.repeat(fstart, np.diff(bounds))
    missing = [k for k in range(nframes) if k not in locs]
    if missing and strict:
        raise RuntimeError(f"{len(missing)} frame(s) unrecorded (first: {missing[:3]}); "
                           f"bvm errors: {b.errors[:3]}")
    coff_a = np.fromiter((locs.get(k, (-1, 0))[0] for k in range(nframes)), np.int64, nframes)
    clen_a = np.fromiter((locs.get(k, (-1, 0))[1] for k in range(nframes)), np.int64, nframes)
    shard_of = np.arange(nframes, dtype=np.int64) % shards
    fall = pdf.with_columns(in_off_out=pl.Series(in_off),
                            coff=pl.Series(np.repeat(coff_a, np.diff(bounds))),
                            clen=pl.Series(np.repeat(clen_a, np.diff(bounds))),
                            shard=pl.Series(np.repeat(shard_of, np.diff(bounds))))
    lost_paths = fall.filter((pl.col("type") == 0) & (pl.col("coff") < 0))["path"].to_list()         if missing else []
    if missing:                                          # errored frames: their bytes are NOT in
        fall = fall.filter(pl.col("coff") >= 0)          # the store — drop members, report lost
    allst = _stats_table([b])                            # ONE table, then filter once
    dd = (allst.filter(pl.col("digest") != -1) if allst is not None
          else pl.DataFrame(schema={"path": pl.Utf8, "digest": pl.Int64,
                                    "chunks": pl.Binary, "fid": pl.Int64}))
    digs = dd.select("path", "digest", "chunks").unique(subset="path", keep="last")
    fullpaths = set(small["path"].to_list())            # big files get EXTENT rows, not direct
    packed = (fall.filter((pl.col("type") == 0) & pl.col("path").is_in(list(fullpaths)))
              .with_columns(frame=pl.col("fid"), in_off=pl.col("in_off_out"),
                            digest=pl.lit(-1, pl.Int64)).select(STAT_COLS + ["shard"])
              .drop("digest").join(digs, on="path", how="left", maintain_order="left")
              .with_columns(digest=pl.col("digest").fill_null(-1),
                            extents=pl.lit(None, pl.Binary)))
    # delta rows: extents mixing old frames + new literal frames (locate literals in fall)
    lit_lookup = {}
    lf = fall.filter((pl.col("type") == 0) & ~pl.col("path").is_in(list(fullpaths)))
    for rr in lf.iter_rows(named=True):
        lit_lookup[(rr["path"], rr["src_off"])] = (rr["coff"], rr["clen"], rr["in_off_out"],
                                                   rr["shard"], rr["fid"])
    # per-PIECE manifests, keyed by (frame, path): a split file's pieces share a path, so the
    # path-keyed `digs` above collapses them. Concatenating the pieces' manifests yields a
    # valid chunking of the whole file (content-defined within each piece, with one forced
    # boundary per frame_cap), which is what keeps a split member deltable next snapshot.
    chunk_df = dd.select("path", "chunks", "fid") if dd.height else None
    drows = []
    for r, dg, newman, segs in delta_meta:
        ex, oo, ok_ = [], 0, True
        for sg in segs:
            if sg[0] == "old":
                ex.append((sg[1], sg[2], sg[3], sg[4], oo, sg[5])); oo += sg[4]
            else:
                for o2, n2 in _pieces(sg[1], sg[2], frame_cap):
                    hit = lit_lookup.get((r["path"], o2))
                    if hit is None:                      # literal run's frame errored: lost
                        ok_ = False; break
                    coff, clen, io_, sh_, _fid = hit
                    ex.append((coff, clen, io_, n2, oo, sh_)); oo += n2
                if not ok_:
                    break
        if not ok_:
            lost_paths.append(r["path"]); continue
        drows.append(dict(path=r["path"], size=r["size"], mode=r["mode"], mtime_ns=r["mtime_ns"],
                          uid=r["uid"], gid=r["gid"], frame=-4, in_off=-1, coff=-1, clen=-1,
                          digest=dg, link="", chunks=newman, extents=_pack_extents(ex), shard=0))
    ndelta = len(drows)                                  # everything past here is a SPLIT, not
    # SPLIT whole files: one extent per piece           # a delta — keep them apart in the report, manifest = the pieces' manifests concatenated.
    # `digest` stays -1 — the whole-file BLAKE3 would need a second full read, and no reader
    # uses it for extent members (`verify` only re-hashes frame>=0 rows).
    _r, _lost = _split_extent_rows(big, fall, fullpaths, chunk_df, frame_cap)
    drows.extend(_r); lost_paths.extend(_lost)
    C13 = STAT_COLS + ["chunks", "extents", "shard"]
    ddf = pl.DataFrame(drows).select(C13) if drows else None
    dirsr = fall.filter(pl.col("type") == 5).select(
        path="path", size=pl.lit(0, pl.Int64), mode="mode", mtime_ns="mtime_ns", uid="uid",
        gid="gid", frame=pl.lit(-1, pl.Int64), in_off=pl.lit(-1, pl.Int64),
        coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64), digest=pl.lit(-1, pl.Int64),
        link=pl.lit("", pl.Utf8), chunks=pl.lit(None, pl.Binary), extents=pl.lit(None, pl.Binary),
        shard=pl.lit(0, pl.Int64))
    linksr = links.select(
        path="path", size="size", mode="mode", mtime_ns="mtime_ns", uid="uid", gid="gid",
        frame=pl.when(pl.col("in_off") == 1).then(-3).otherwise(-2).cast(pl.Int64),
        in_off=pl.lit(-1, pl.Int64), coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link="link", chunks=pl.lit(None, pl.Binary),
        extents=pl.lit(None, pl.Binary), shard=pl.lit(0, pl.Int64))
    # ---- FOOTER-BATCH REUSE (bup tree-sharing analog): a prior batch's directory entry is
    # copied verbatim iff EVERY row in it is still the CURRENT row for its path (same
    # locator, path not changed/deleted/delta'd) and it holds only data rows. Reused rows
    # are then excluded from the fresh `carried` batches — an idle backup rewrites almost
    # nothing; reuse COMPOUNDS down the chain (copied entries may already point at older
    # footers' batches). ----
    dirty = set(todo["path"].to_list()) | {r["path"] for r, *_ in delta_meta}
    latest_keys = carried.select("path", "frame", "coff", "clen", "in_off")
    reuse_ents, reused_paths = [], []
    for kk, pb in enumerate(prior_batches):
        if kk >= len(prior_ents):
            break
        if pb.filter(pl.col("frame") < 0).height and pb.filter(pl.col("frame") != -4).filter(
                pl.col("frame") < 0).height:
            continue                                     # dir/link rows regenerate each snapshot
        if pb.filter(pl.col("path").is_in(list(dirty))).height:
            continue
        m_ = pb.join(latest_keys, on=["path", "frame", "coff", "clen", "in_off"], how="inner")
        if m_.height != pb.height:
            continue                                     # superseded/deleted rows -> rewrite
        leaf = prior_leaves[kk] if prior_leaves else None
        if leaf is None:                                 # pre-merkle store: hash the payload once
            import hashlib
            with open(out, "rb") as _f:
                _f.seek(prior_ents[kk][0]); leaf = hashlib.blake2b(_f.read(prior_ents[kk][1]),
                                                                  digest_size=32).digest()
        reuse_ents.append(tuple(prior_ents[kk]) + (leaf,))
        reused_paths.append(pb["path"])
    if reused_paths:
        rp = pl.concat(reused_paths)
        carried = carried.filter(~pl.col("path").is_in(rp.to_list()))
    # the footer lands on shard 0, after ITS frames only
    end = max([prev_ends[0]] + [int(locs[k][0] + locs[k][1])
                                for k in range(0, nframes, shards) if k in locs])
    fd = os.open(out, os.O_RDWR)
    ps = [carried.select(C13), packed.select(C13), dirsr.select(C13), linksr.select(C13)]
    if ddf is not None:
        ps.append(ddf)
    write_footer(fd, end, ps, prev_off=prev_end if exists else None,
                 reuse_ents=reuse_ents, rows_per_batch=1 << 12, part_cuts=True,
                 snap_time_ns=time_ns if time_ns is not None else time.time_ns(),
                 prev_commit=prior_commit)
    os.close(fd)
    fc = frame_costs([b], pdf)
    if fc.height:
        fc.write_parquet(out + ".frames.parquet")
    return dict(snapshot_bytes=sum(os.path.getsize(p) for p in paths) - sum(prev_ends),
                shards=shards, snapshot_bytes_shard0=end - prev_ends[0], carried=carried.height + sum(p.len() for p in reused_paths),
                reused_batches=len(reuse_ents), rewritten_carried=carried.height,
                full=small.height, split=big.height,
                delta=ndelta, literal_bytes=appended_literals,
                no_manifest=n_nomanifest[0],
                dirs=dirsr.height, links=linksr.height,
                errors=len(b.errors), error_sample=b.errors[:5],
                lost=len(lost_paths), lost_sample=lost_paths[:5],
                skipped_huge=huge.height, skipped_huge_sample=huge["path"].to_list()[:5],
                perf=perf_report(b.perf),
                # the RAW counters too: perf_report() summarises to rates, but attributing a
                # regression needs ns_write vs ns_comp vs raw_bytes side by side.
                perf_raw=dict(b.perf))


def slurm_executors(nodes, bvm_exe, nworkers, budget_mb=512, gpus=8, minutes=240,
                    partition="gpu", timeout=300, verbose=True):
    """Start k executors in ONE all-or-nothing allocation and take their command streams over
    TCP. `srun -N k -n k` is gang-scheduled, so either every node is granted or none is --
    which is the failure mode one-srun-per-node cannot express: a partial allocation leaves
    the planner dispatching into a pipe nobody drains. The k tasks share one stdout, so they
    cannot speak the protocol over pipes; each connects back here instead and gets a private
    full-duplex channel. Returns [_Bvm, ...] and the srun Popen (kill it when done)."""
    import socket, subprocess
    k = len(nodes)
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("0.0.0.0", 0)); ls.listen(k)
    host, port = socket.gethostname(), ls.getsockname()[1]
    argv = ["srun", "-p", partition, "-G", str(gpus * k), "-N", str(k), "-n", str(k),
            "--ntasks-per-node=1", "--mem=0", f"--time={minutes}", "--unbuffered",
            "-w", ",".join(nodes), bvm_exe, str(nworkers), str(budget_mb),
            "--connect", f"{host}:{port}"]
    proc = subprocess.Popen(argv)
    ls.settimeout(timeout)
    bs = []
    try:
        for _ in range(k):
            conn, addr = ls.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            b = _Bvm(bvm_exe, nworkers, budget_mb, sock=_SockProc(conn, addr[0]))
            bs.append(b)
            if verbose:
                print(f"    executor {len(bs)}/{k} connected from {addr[0]}", flush=True)
    except socket.timeout:
        proc.kill()
        raise RuntimeError(f"only {len(bs)}/{k} executors connected within {timeout}s")
    finally:
        ls.close()
    return bs, proc


def slurm_launch(node, gpus=8, minutes=240, partition="gpu"):
    """A `launch` prefix that puts one bvm on `node`. --unbuffered is NOT optional: without
    it srun buffers the task's stdout and the planner's first read blocks forever."""
    return ["srun", "-p", partition, "-G", str(gpus), "-N", "1", "-n", "1", "--mem=0",
            f"--time={minutes}", "-w", node, "--unbuffered"]


def _progress_writer(path, state, stop):
    """Dump live state to disk every couple of seconds so a SEPARATE process can watch. The
    planner cannot report progress itself: after dispatch it is blocked in finish() draining
    tens of thousands of outstanding frames, which is precisely the stretch you want to see.
    Written temp-and-renamed so a reader never catches a half-written file."""
    import json
    while not stop.is_set():
        try:
            snap = state()
            with open(path + ".tmp", "w") as f:
                json.dump(snap, f)
            os.replace(path + ".tmp", path)
        except Exception:
            pass
        stop.wait(2.0)


def backup_multi(root, out, bvm_exe, nodes, nworkers=64, level=6, time_ns=None,
                 excludes=None, strict=False, sinks_per_node=8, frame_cap=None,
                 launch=slurm_launch, chunk_gb=2, verbose=False,
                 max_member_bytes=1 << 40, start_timeout=120, executors=None):
    """ONE planner, N executors, one store. Each node runs a bvm over the shared filesystem
    and owns its own sink files, so nothing is serialized on a single inode or a single host's
    fabric; the planner keeps the footer, which is what makes the result ONE archive rather
    than N archives that have to be globbed back together.

    Work is handed out AS NODES DRAIN, not pre-partitioned. A static split of a real tree is
    long-pole-bound — one subtree here holds 3.86 TB of 5.9 TB, so a size-balanced 3-way split
    still leaves one node doing 65% of the work. The planner instead sends ~`chunk_gb` at a
    time to whichever executor has the fewest frames outstanding, measured from the BSTATs
    coming back (see _Bvm.poll).

    Clean-slate only: this is the full-backup path, no prior snapshot, no delta. Returns a
    summary dict shaped like backup()'s."""
    import numpy as np
    root_abs = os.path.abspath(root)
    if frame_cap is None:
        frame_cap = DEFAULT_FRAME_CAP
    nn = len(nodes)
    ign = load_ignore(root_abs, excludes)
    t_start = time.time()
    phase = ["launch"]
    dispatched = [0]
    outstanding = [0] * len(nodes)
    # `executors` = already-connected executors (slurm_executors: one allocation, TCP command
    # streams). Otherwise one child per node over pipes.
    bs = list(executors) if executors else [
        _Bvm(bvm_exe, nworkers, launch=launch(nd)) for nd in nodes]
    for i, b in enumerate(bs):
        b.strict = strict
        b.frames_fd = os.open(f"{out}.frames.{i}.bin", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    live, dead = [], []
    for i, b in enumerate(bs):                           # never dispatch to an executor that
        (live if (executors or b.ping(start_timeout)) else dead).append((i, b))
    if dead:
        print(f"    {len(dead)} executor(s) never started ({', '.join(nodes[i] for i, _ in dead)})"
              f" — continuing on {len(live)}", flush=True)
        for _i, b in dead:
            try: b.p.kill()
            except Exception: pass
    if not live:
        raise RuntimeError("no executor started")
    nodes = [nodes[i] for i, _ in live]
    bs = [b for _i, b in live]
    nn = len(bs)
    outstanding = [0] * nn
    paths = shard_paths(out, nn * sinks_per_node)         # after the liveness check: a node
    for p in paths:                                       # that never started must not own
        open(p, "wb").close()                             # a slice of the shard space
    # sinks are assigned AFTER the liveness check, so a node that never started does not own
    # a slice of the shard space (its files would stay empty and its frames would be lost)
    for i, b in enumerate(bs):
        for k in range(sinks_per_node):
            b.sink(k, paths[i * sinks_per_node + k], start=0)

    def _snap():
        """Live state for the on-disk dump. Every queue counter bvm emits is CUMULATIVE, so
        dividing by total samples gives the run's average, not the present -- a dashboard then
        reports "scan queue: 812 dirs" long after the walk finished, and a job-queue that was
        full early reads as full forever. Difference the last two 1 Hz samples instead, which
        is what the reader actually wants to see."""
        el = time.time() - t_start
        per = []
        for i, b in enumerate(bs):
            p = b.perf or {}
            ser = b.perf_series
            q = ser[-2] if len(ser) >= 2 else None       # previous sample, for the interval
            def d(k):                                    # per-interval delta
                return (p.get(k, 0) - q.get(k, 0)) if q else p.get(k, 0)
            qs = max(d("q_samples"), 1)
            dt_i = max((d("wall_ns") or 0) / 1e9, 1e-9)
            per.append(dict(node=nodes[i], outstanding=outstanding[i], batches=len(b.stats),
                            rd_gb=round(p.get("rd_bytes", 0) / 1e9, 1),
                            wr_gb=round(p.get("wr_bytes", 0) / 1e9, 1),
                            rd_gbs=round(d("rd_bytes") / 1e9 / dt_i, 3),
                            wr_gbs=round(d("wr_bytes") / 1e9 / dt_i, 3),
                            comp_in=p.get("comp_in", 0), comp_out=p.get("comp_out", 0),
                            raw_frames=p.get("n_raw_frames", 0),
                            raw_gb=round(p.get("raw_bytes", 0) / 1e9, 1),
                            # --- present state, from the last interval only ---
                            busy=round(d("busy_sum") / qs, 1),
                            jq=round(d("jq_sum") / qs, 1),
                            jq_full_pct=round(100 * d("jq_full") / qs, 1),
                            jq_empty_pct=round(100 * d("jq_empty") / qs, 1),
                            scanq=round(d("dq_sum") / qs, 1),
                            packbuf_pct=round(100 * d("pack_used_sum") / qs
                                              / max(p.get("pack_budget", 1), 1), 1),
                            blockbuf_pct=round(100 * d("blk_live_sum") / qs
                                               / max(p.get("blk_budget", 1), 1), 1),
                            jq_cap=p.get("jq_cap", 0),
                            opens=p.get("n_open", 0), slow_opens=p.get("slow_open", 0),
                            # ring_prefetch counts its opens in n_open but never times them, so
                            # slow_opens/opens is DILUTED by ring opens that can never land in
                            # the numerator. Report the ring counters so the sync-open count is
                            # derivable and the two eras stay comparable.
                            ring_batches=p.get("ring_batches", 0),
                            ring_members=p.get("ring_members", 0),
                            workers=p.get("nworkers", nworkers), errors=len(b.errors),
                            # where worker time went IN THIS INTERVAL
                            ns_read=d("ns_read"), ns_comp=d("ns_comp"), ns_write=d("ns_write"),
                            ns_hash=d("ns_hash"), ns_open=d("ns_open"), ns_idle=d("ns_idle")))
        tot_in = sum(x["comp_in"] for x in per); tot_out = sum(x["comp_out"] for x in per)
        return dict(store=out, elapsed_s=round(el, 1), phase=phase[0], nodes=per,
                    frames_total=nframes_box[0], frames_dispatched=dispatched[0],
                    interval=True,                       # values are per-interval, not run means
                    rd_gb=round(sum(x["rd_gb"] for x in per), 1),
                    wr_gb=round(sum(x["wr_gb"] for x in per), 1),
                    ratio=round(tot_out / tot_in, 4) if tot_in else None,
                    raw_gb=round(sum(x["raw_gb"] for x in per), 1),
                    rd_gbs=round(sum(x["rd_gbs"] for x in per), 3),
                    wr_gbs=round(sum(x["wr_gbs"] for x in per), 3))

    nframes_box = [0]
    import threading as _th
    _stop = _th.Event()
    _pw = _th.Thread(target=_progress_writer, args=(out + ".progress.json", _snap, _stop),
                     daemon=True)
    _pw.start()

    # ---- scan (metadata-bound, one node: it is ~2% of a full backup's wall time) ----
    phase[0] = "scan"
    t_scan = time.time()
    bs[0].scan_fs(0, root_abs, ignore=ign)
    parts, dparts = [], []
    while True:
        t, pld = bs[0].read()
        if t is None or t == 1:
            break
        if t == 0:
            df = _stat_df(pld)[2]
            if df.height:
                nm = df.select(path="path", size="size",
                               mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
                               mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64),
                               gid=pl.col("gid").cast(pl.Int64), in_off="in_off", link="link",
                               is_dir="is_dir")
                parts.append(nm.filter(pl.col("is_dir") == 0).drop("is_dir"))
                dparts.append(nm.filter(pl.col("is_dir") == 1).drop("is_dir"))
    SCH = {"path": pl.Utf8, "size": pl.Int64, "mode": pl.Int64, "mtime_ns": pl.Int64,
           "uid": pl.Int64, "gid": pl.Int64, "in_off": pl.Int64, "link": pl.Utf8}
    sc = pl.concat(parts) if parts else pl.DataFrame(schema=SCH)
    dirs = (pl.concat(dparts) if dparts else pl.DataFrame(schema=SCH)).sort("path")
    links, files = sc.filter(pl.col("link") != ""), sc.filter(pl.col("link") == "")
    scan_s = time.time() - t_scan

    # ---- typed member stream, oversized members split (same rule as backup()) ----
    # SPARSE-FILE GUARD. Splitting a member into frame_cap pieces means an oversized
    # APPARENT size no longer OOMs -- it reads that many bytes of zeros instead. WEKA
    # reports neither SEEK_HOLE/SEEK_DATA (EINVAL) nor FIEMAP (EOPNOTSUPP) nor honest
    # st_blocks, so there is no way to detect the holes: a 50 TiB sparse disk image
    # silently became 3.28M pieces and 86% of a whole-tree backup plan. Refuse it loudly
    # instead; raise max_member_bytes if you really do have a member that big.
    huge = files.filter(pl.col("size") > max_member_bytes)
    if huge.height:
        files = files.filter(pl.col("size") <= max_member_bytes)
    big = files.filter(pl.col("size") > frame_cap)
    small = files.filter(pl.col("size") <= frame_cap)
    typed = pl.concat([
        dirs.with_columns(type=pl.lit(5, pl.UInt8), src_off=pl.lit(0, pl.Int64)),
        small.select(list(SCH)).with_columns(type=pl.lit(0, pl.UInt8), src_off=pl.lit(0, pl.Int64)),
        links.with_columns(type=pl.when(pl.col("in_off") == 1).then(1).otherwise(2).cast(pl.UInt8),
                           src_off=pl.lit(0, pl.Int64)),
    ])
    piece_rows = []
    for r in big.iter_rows(named=True):
        for o2, n2 in _pieces(0, r["size"], frame_cap):
            piece_rows.append(dict(path=r["path"], size=n2, mode=r["mode"],
                                   mtime_ns=r["mtime_ns"], uid=r["uid"], gid=r["gid"],
                                   in_off=0, link="", type=0, src_off=o2))
    if piece_rows:
        typed = pl.concat([typed, pl.DataFrame(piece_rows).select(typed.columns).cast(
            {c: typed.schema[c] for c in typed.columns})])
    sizes = np.where(typed["type"].to_numpy() == 0, typed["size"].to_numpy(), 0)
    pre = np.zeros(sizes.size + 1, np.int64); np.cumsum(sizes, out=pre[1:])
    bounds = [0]
    while bounds[-1] < sizes.size:
        i = bounds[-1]
        e = int(np.searchsorted(pre, pre[i] + (1 << 20), side="right")) - 1
        bounds.append(max(e, i + 1))
    nframes = len(bounds) - 1
    fidx = np.repeat(np.arange(nframes, dtype=np.int64), np.diff(bounds))
    pdf = typed.with_columns(fid=pl.Series(fidx))
    ba = np.asarray(bounds, dtype=np.int64)

    # ---- dispatch: ~chunk_gb at a time to the least-loaded executor ----
    phase[0] = "pack"
    nframes_box[0] = nframes
    t_pack = time.time()
    shard_of = np.zeros(nframes, np.int64)
    per_node_frames, sent_bytes = [0] * nn, [0] * nn
    CH = int(chunk_gb * 1e9)
    fcum = pre[ba]                                       # PAYLOAD BYTES at each frame boundary:
    c = 0                                                # `ba` itself holds row indices, and
    while c < len(bounds) - 1:                           # comparing those to a byte budget makes
        e = max(int(np.searchsorted(fcum, fcum[c] + CH,  # the loop emit one giant chunk
                                    side="right")) - 1, c + 1)
        for i, b in enumerate(bs):
            outstanding[i] -= b.poll()
        # least outstanding, ties to whoever has had the least work overall: min() returns
        # the FIRST minimum, so a plain least-outstanding rule sends everything to node 0
        # whenever the executors drain between dispatches.
        i = min(range(nn), key=lambda k: (outstanding[k], sent_bytes[k], k))
        sl = pdf.slice(int(ba[c]), int(ba[e] - ba[c])).select(
            fid="fid", type="type", mode=pl.col("mode").cast(pl.Int32), size="size",
            mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int32), gid=pl.col("gid").cast(pl.Int32),
            src_off="src_off", path="path", link="link")
        fids = sl["fid"].unique().to_list()
        for k in range(sinks_per_node):                  # spread within the node too
            part = sl.filter(pl.col("fid") % sinks_per_node == k)
            if part.height:
                bs[i].pack_files_df(level, root_abs, part, sink_id=k, tar_compat=0)
        for f in fids:
            shard_of[int(f)] = i * sinks_per_node + int(f) % sinks_per_node
        outstanding[i] += len(fids); per_node_frames[i] += len(fids); dispatched[0] = c
        sent_bytes[i] += int(ba[e] - ba[c]) and int(pre[ba[e]] - pre[ba[c]])
        if verbose and c % 200 == 0:
            print(f"    frames {c}/{nframes}  outstanding={outstanding}", flush=True)
        c = e

    globals()["_last_bvms"] = bs                         # so a driver can pull the frame costs
    phase[0] = "drain"
    locs, perfs = {}, []
    for i, b in enumerate(bs):
        locs.update(b.finish())                          # blocks until that executor drains
        perfs.append(b.perf)
    pack_s = time.time() - t_pack
    phase[0] = "footer"
    t_foot = time.time()

    # ---- footer (planner-owned, on shard 0) ----
    fpath = out + ".footer"
    # TIME THESE SEPARATELY. _stats_table() used to be evaluated inside the call's argument
    # list, so it was inside footer_s but invisible to bench/footerbench.py -- the replay
    # measured 9 s against a real 73.7 s and the difference had to be inferred.
    _t = time.time()
    _allst = _stats_table(bs)
    # DROP the per-frame batches once they are one table: ~248k small DataFrames pinned in the
    # planner is real memory pressure going into the most expensive SERIAL phase, and nothing
    # downstream reads b.stats again. (assemble_footer runs ~8x slower in the live process
    # than the same inputs replayed in a fresh one -- bench/footerbench.py -- so heap state is
    # implicated even though the row counts match.)
    for _b in bs:
        _b.stats = []
    stats_s = round(time.time() - _t, 1)
    _t = time.time()
    _fr = assemble_footer(fpath, pdf, bounds, pre, locs, shard_of, big, small, links,
                          _allst, paths, frame_cap, time_ns)
    build_s = round(time.time() - _t, 1)
    footer_stages = _fr.get("stages", {})
    lost_paths, ends = _fr["lost_paths"], _fr["ends"]
    n_dirs, n_links, n_extents = _fr["dirs"], _fr["links"], _fr["extent_rows"]
    phase[0] = "done"; _stop.set()
    footer_s = round(time.time() - t_foot, 1)            # how much is SERIAL after packing?
    fc = frame_costs(bs, pdf)                            # SIDECAR, never inside the archive:
    if fc.height:                                        # telemetry is optional to keep
        fc.write_parquet(out + ".frames.parquet")
    footer_bytes = os.path.getsize(fpath)                    # the planner wrote this one
    return dict(nodes=list(nodes), sinks=len(paths), scan_s=round(scan_s, 1),
                pack_s=round(pack_s, 1), footer_s=footer_s,
                footer_stats_s=stats_s, footer_build_s=build_s,
                footer_stages=footer_stages,
                store_bytes=sum(ends) + footer_bytes, data_per_shard=ends,
                frames=nframes, frames_per_node=per_node_frames,
                bytes_per_node=[int(x) for x in sent_bytes],
                full=small.height, split=big.height, dirs=n_dirs, links=n_links,
                errors=sum(len(b.errors) for b in bs), lost=len(lost_paths),
                lost_sample=lost_paths[:5], skipped_huge=huge.height,
                skipped_huge_sample=huge["path"].to_list()[:5],
                perf=[perf_report(p) for p in perfs if p])


def _obj_store(url):
    """Object-store backend for backup replication: s3://bucket/prefix or file:///dir
    (testing / any shared fs). Just get/put/delete/list — the remote needs NO compute:
    backup logic runs entirely on the sender (the lazy-reference store's receiver was
    already reduced to dumb append-only storage)."""
    if url.startswith("file://"):
        base = url[7:]
        os.makedirs(base, exist_ok=True)
        class F:
            def get(self, name):
                p = os.path.join(base, name)
                return open(p, "rb").read() if os.path.exists(p) else None
            def put(self, name, data):
                tmp = os.path.join(base, "." + name + ".tmp")
                open(tmp, "wb").write(data); os.replace(tmp, os.path.join(base, name))
            def delete(self, name):
                try:
                    os.unlink(os.path.join(base, name))
                except OSError:
                    pass
            def list(self):
                return [f for f in os.listdir(base) if not f.startswith(".")]
        return F()
    if url.startswith("s3://"):
        import boto3
        rest = url[5:]
        bucket, _, prefix = rest.partition("/")
        cli = boto3.client("s3")
        class S:
            def _k(self, name):
                return f"{prefix.rstrip('/')}/{name}" if prefix else name
            def get(self, name):
                try:
                    return cli.get_object(Bucket=bucket, Key=self._k(name))["Body"].read()
                except cli.exceptions.NoSuchKey:
                    return None
                except cli.exceptions.ClientError:
                    return None
            def put(self, name, data):
                cli.put_object(Bucket=bucket, Key=self._k(name), Body=data)
            def delete(self, name):
                cli.delete_object(Bucket=bucket, Key=self._k(name))
            def list(self):
                out, tok = [], None
                while True:
                    kw = dict(Bucket=bucket, Prefix=self._k(""))
                    if tok:
                        kw["ContinuationToken"] = tok
                    r = cli.list_objects_v2(**kw)
                    out += [o["Key"][len(self._k("")):] for o in r.get("Contents", [])]
                    if not r.get("IsTruncated"):
                        return out
                    tok = r.get("NextContinuationToken")
        return S()
    raise ValueError(f"unsupported store url: {url}")


def _tail_hash(path, end):
    import hashlib
    with open(path, "rb") as f:
        f.seek(max(0, end - 32)); return hashlib.blake2b(f.read(min(32, end)), digest_size=8).hexdigest()


def _nockidx_footer(store):
    from ..nock import nockidx as _n
    return _n.footer_path(store)


def push(store, url):
    """Replicate a backup chain to an object store — APPEND-ONLY DELTA: uploads only the
    bytes added since the last push (one segment object per push), verified by a tail
    hash so a GC'd/diverged store triggers a clean full re-upload. `head` (json) is the
    commit point. Returns dict(uploaded, segments, full)."""
    import json
    ob = _obj_store(url)
    head = ob.get("head")
    head = json.loads(head) if head else None
    sfiles = store_files(store)                            # each shard replicates independently
    fp = _nockidx_footer(store)
    if fp != store:
        sfiles = sfiles + [fp]                             # ... and so does a separate footer
    shard_heads, uploaded, full, nseg = [], 0, False, 0
    for k, path in enumerate(sfiles):
        pfx = "" if k == 0 else f"s{k}-"                   # shard 0 keys stay unprefixed
        prior = (head or {}).get("shards", [None] * len(sfiles))[k] if head and             len((head or {}).get("shards", [])) > k else (head if k == 0 else None)
        size = os.path.getsize(path)
        segs, kfull = [], True
        if prior and prior["size"] <= size and _tail_hash(path, prior["size"]) == prior["tail"]:
            segs = prior["segments"]; kfull = False        # clean append since last push
        if kfull:
            for name in list(ob.list()):
                if name.startswith(pfx + "seg-"):          # "seg-" excludes "sN-seg-"
                    ob.delete(name)
            segs = []
        start = segs[-1][1] if segs else 0
        if size > start:
            with open(path, "rb") as f:
                f.seek(start); data = f.read(size - start)
            ob.put(f"{pfx}seg-{start:016x}-{size:016x}", data)
            segs = segs + [[start, size, _tail_hash(path, size)]]  # per-segment tail: pull can
            uploaded += size - start                              # validate any prefix
        shard_heads.append({"size": size, "tail": _tail_hash(path, size), "segments": segs})
        full = full or kfull; nseg += len(segs)
    size, segs = shard_heads[0]["size"], shard_heads[0]["segments"]
    from ..nock import nockidx as _nx
    try:
        _m = _nx.read_directory(store, with_prev=True)
        commit = _m[4].hex() if _m and _m[4] else None
    except Exception:
        commit = None
    ob.put("head", json.dumps({"size": size, "tail": _tail_hash(store, size),
                               "segments": segs, "commit": commit,
                               "shards": shard_heads}).encode())
    return dict(uploaded=uploaded, segments=nseg, shards=len(sfiles), full=full, commit=commit)


def pull(url, store):
    """Fetch a replicated chain: downloads only the segments the local copy lacks (the
    local file must be a clean prefix of the remote — verified by tail hash; otherwise
    refetches everything). Returns dict(downloaded, segments)."""
    import json
    ob = _obj_store(url)
    head = ob.get("head")
    if head is None:
        raise FileNotFoundError(f"no head at {url}")
    head = json.loads(head)
    shards = head.get("shards") or [head]                  # unsharded remote == one shard
    got, nseg = 0, 0
    for k, sh in enumerate(shards):
        path = store if k == 0 else f"{store}.{k}"
        pfx = "" if k == 0 else f"s{k}-"
        lsize = os.path.getsize(path) if os.path.exists(path) else 0
        # prefix valid iff local size equals some segment boundary AND local bytes match;
        # on ANY doubt, refetch all of this shard.
        need = [sg for sg in sh["segments"] if sg[1] > lsize]
        if not any(lsize == sg[0] for sg in need) and lsize != sh["size"]:
            need = sh["segments"]; lsize = 0
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            for s2, e, *_t in need:
                data = ob.get(f"{pfx}seg-{s2:016x}-{e:016x}")
                if data is None or len(data) != e - s2:
                    raise IOError(f"segment {pfx}{s2:x}-{e:x} missing/short at {url}")
                os.pwrite(fd, data, s2); got += len(data)
            os.ftruncate(fd, sh["size"])
        finally:
            os.close(fd)
        if _tail_hash(path, sh["size"]) != sh["tail"]:
            raise IOError(f"pulled shard {k} fails tail check")
        nseg += len(need)
    if head.get("commit"):                                # AUTHENTICATE the replica
        from ..nock import nockidx as _nx
        _m = _nx.read_directory(store, with_prev=True)
        if not _m or not _m[4] or _m[4].hex() != head["commit"]:
            raise IOError("pulled store commit mismatch (replica does not authenticate)")
    return dict(downloaded=got, segments=nseg, shards=len(shards), commit=head.get("commit"))


def retention_select(snaps, last=None, daily=None, weekly=None, monthly=None, yearly=None):
    """borg-style retention over `snaps` (newest-first, from nockidx.snapshots): keep the
    newest snapshot always, the `last` N most recent, and the newest snapshot of each of
    the last `daily` days / `weekly` ISO weeks / `monthly` months / `yearly` years.
    Returns the set of surviving `at` offsets."""
    import datetime
    keep = set()
    if not snaps:
        return keep
    keep.add(snaps[0]["at"])                              # newest always survives
    for sn in snaps[:last or 0]:
        keep.add(sn["at"])
    def buckets(fmt, n):
        seen = set()
        for sn in snaps:                                  # newest-first: first hit per bucket
            if sn["time_ns"] is None:
                continue
            k = datetime.datetime.fromtimestamp(sn["time_ns"] / 1e9).strftime(fmt)
            if k not in seen:
                seen.add(k); keep.add(sn["at"])
                if len(seen) >= n:
                    break
    if daily:
        buckets("%Y-%m-%d", daily)
    if weekly:
        buckets("%G-%V", weekly)                          # ISO year-week
    if monthly:
        buckets("%Y-%m", monthly)
    if yearly:
        buckets("%Y", yearly)
    return keep


def gc(store, last=None, daily=None, weekly=None, monthly=None, yearly=None):
    """Garbage-collect a backup chain under a RETENTION POLICY (no rules = keep latest
    only). Surviving snapshots keep working — their footers are rewritten with remapped
    offsets, SHARED batches are rewritten once and re-shared, and the prev-chain +
    timestamps are preserved. Live frames are copied VERBATIM (never recompressed).
    Atomic replace. Returns dict(before, after, freed, kept, dropped)."""
    import io as _io
    import zstandard as _z
    from ..nock import nockidx as _nx
    from ..nock.nockidx import SKIP_MAGIC
    sfiles = store_files(store)                           # shard 0 carries the footers
    before = sum(os.path.getsize(p) for p in sfiles)
    snaps = _nx.snapshots(store)
    keep_at = retention_select(snaps, last, daily, weekly, monthly, yearly)
    survivors = [sn for sn in snaps if sn["at"] in keep_at]
    survivors.reverse()                                   # oldest -> newest for writing
    # per-survivor directory entries; batches dedup'd by (off, clen)
    sdirs, batches = [], {}
    for sn in survivors:
        ents = _nx.read_directory(store, at=sn["at"])
        sdirs.append((sn, ents))
        for off, clen, first, nrow in ents:
            batches.setdefault((off, clen), (first, nrow))
    def _load(off, clen):
        with open(_nx.footer_path(store), "rb") as f:
            f.seek(off)
            raw = _z.ZstdDecompressor().decompress(f.read(clen))
        df = pl.read_ipc(_io.BytesIO(raw))
        df = df.rename({a: c for a, c in (("frame_coff", "coff"), ("frame_clen", "clen"))
                        if a in df.columns})
        if "shard" not in df.columns:
            df = df.with_columns(shard=pl.lit(0, pl.Int64))
        return df
    bdfs = {k: _load(*k) for k in batches}
    live = {}                                             # (shard, coff, clen) -> new coff
    for df in bdfs.values():
        for sh_, c_, l_ in (df.filter(pl.col("frame") >= 0)
                            .select("shard", "coff", "clen").unique().iter_rows()):
            live[(sh_, c_, l_)] = None
        if "extents" in df.columns:
            for b_ in df.filter(pl.col("frame") == -4)["extents"].to_list():
                if b_ is not None:
                    for c_, l_, _io2, _ln, _oo, sh_ in _parse_extents(b_):
                        live[(sh_, c_, l_)] = None
    tmps = [p + ".gc" for p in sfiles]
    cur = [0] * len(sfiles)
    srcs = [open(p, "rb") for p in sfiles]
    dsts = [open(p, "wb") for p in tmps]
    try:
        for (sh_, c_, l_) in sorted(live):                # live frames, verbatim, per shard
            srcs[sh_].seek(c_); dsts[sh_].write(srcs[sh_].read(l_))
            live[(sh_, c_, l_)] = cur[sh_]; cur[sh_] += l_
        dst = dsts[0]                                     # the footer region lives on shard 0
        # rewrite each unique batch ONCE with remapped locators; re-shared by survivors
        cctx = _z.ZstdCompressor(level=3)
        bmap = {}
        for key, df in bdfs.items():
            first, nrow = batches[key]
            if df.height:
                df = df.with_columns(coff=pl.struct(["shard", "coff", "clen", "frame"]).map_elements(
                    lambda x: live[(x["shard"], x["coff"], x["clen"])] if x["frame"] >= 0
                    else x["coff"], return_dtype=pl.Int64))
                if "extents" in df.columns:
                    exts = [None if b_ is None else _pack_extents(
                        [(live[(sh2, c2, l2)], l2, io2, ln2, oo2, sh2)
                         for c2, l2, io2, ln2, oo2, sh2 in _parse_extents(b_)])
                        for b_ in df["extents"].to_list()]
                    df = df.with_columns(extents=pl.Series(exts, dtype=pl.Binary))
            disk = df.rename({a: b_ for a, b_ in (("coff", "frame_coff"), ("clen", "frame_clen"))
                              if a in df.columns})
            import hashlib as _hl
            buf = _io.BytesIO(); disk.write_ipc(buf)
            comp = cctx.compress(buf.getvalue())
            dst.write(struct.pack("<II", SKIP_MAGIC, len(comp)))
            bmap[key] = (cur[0] + 8, len(comp), first, nrow,
                         _hl.blake2b(comp, digest_size=32).digest())
            dst.write(comp); cur[0] += 8 + len(comp)
        prev, pcommit = None, None
        for sn, ents in sdirs:                            # footers oldest -> newest
            blob = _nx.pack_footer(pl.DataFrame(), cur[0], prev_off=prev,
                                   reuse_ents=[bmap[(o, c2)] for o, c2, _f, _n in ents],
                                   snap_time_ns=sn["time_ns"], prev_commit=pcommit)
            dst.write(blob); cur[0] += len(blob)
            prev = cur[0]                                 # this snapshot's trailer end
            import hashlib as _hl2
            # recompute commit locally (root over this snapshot's leaves + chain)
            leaves2 = [bmap[(o, c2)][4] for o, c2, _f, _n in ents]
            root2 = _nx.merkle_root(leaves2)
            pcommit = _hl2.blake2b(root2 + (pcommit or b"\0" * 32)
                                   + struct.pack("<q", sn["time_ns"] or 0), digest_size=32).digest()
    finally:
        for f in srcs + dsts:
            f.close()
    for t, p in zip(tmps, sfiles):
        os.replace(t, p)
    after = sum(os.path.getsize(p) for p in sfiles)
    return dict(before=before, after=after, freed=before - after,
                kept=len(survivors), dropped=len(snaps) - len(survivors))


def _tar_eof_frame(fd, cursor, level):
    """Append one zstd frame of 1024 zero bytes at `cursor` — the tar end-of-archive
    marker — so the data section ends as a clean single tar. Returns the new cursor."""
    eof = zstd.ZstdCompressor(level=level).compress(b"\x00" * 1024)
    os.pwrite(fd, eof, cursor)
    return cursor + len(eof)


def _plan_block(b, block, df, fid, level, tar_compat, predicate, done_paths, planmap, sink_id=0):
    """The shared recompress PLANNER for one decoded block — the SAME shape as pack_fs_c,
    with the block as body source instead of the fs: apply the member filter (vectorized
    via predicate.expr), honor the WAL, emit the frame — fast verbatim COPY_BLOCK when
    unfiltered, else COPY_MEMBERS (bvm reads bodies at in_off and synthesizes tar headers;
    the footer's output in_off is the same vectorized hlen+pad cumsum pack uses). ALWAYS
    issues exactly one free_block (RETIRE) once decided. Sets planmap[fid] = the frame's
    footer df and returns True if a frame was emitted."""
    import numpy as np
    if predicate is not None:
        expr = getattr(predicate, "expr", None)
        df = df.filter(expr) if expr is not None else \
            df.filter(pl.Series([predicate(m) for m in df.to_dicts()], dtype=pl.Boolean))
        if not df.height:
            b.free_block(block); return False                # nothing kept -> retire (frees now)
    if done_paths and df.height and df["path"][0] in done_paths:
        b.free_block(block); return False                    # already durable -> retire
    core = df.select("path", "size", "mode", "mtime_ns", "uid", "gid", "in_off")
    if predicate is None:
        planmap[fid] = core                                  # verbatim: decode-time in_off holds
        b.copy_block(block, fid, level, sink_id)
    else:
        sizes = core["size"].to_numpy()
        hl = _tar_hlens(core["path"]) if tar_compat else np.zeros(sizes.size, np.int64)
        step = hl + (((sizes + 511) & ~511) if tar_compat else sizes)
        pre = np.zeros(step.size, np.int64); np.cumsum(step[:-1], out=pre[1:])   # exclusive
        b.copy_members_df(block, fid, level, core.select(
            in_off="in_off", type=pl.lit(0, pl.UInt8), mode=pl.col("mode").cast(pl.Int32),
            size="size", mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int32),
            gid=pl.col("gid").cast(pl.Int32), path="path",
            link=pl.lit("", pl.Utf8)), sink_id, tar_compat)
        planmap[fid] = core.with_columns(in_off=pl.Series(pre + hl))   # output body offsets
    b.free_block(block)                                      # retire after the copy is enqueued
    return True


def _frame_footer(df, f_id, coff, clen):
    """One frame's planmap df -> STAT_COLS rows with its locator, vectorized."""
    return df.select(
        path="path", size=pl.col("size").cast(pl.Int64), mode=pl.col("mode").cast(pl.Int64),
        mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64), gid=pl.col("gid").cast(pl.Int64),
        frame=pl.lit(f_id, pl.Int64), in_off=pl.col("in_off").cast(pl.Int64),
        coff=pl.lit(coff, pl.Int64), clen=pl.lit(clen, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link=pl.lit("", pl.Utf8))


def _send_meta_frame(b, symparts, fid, level, sink_id=0, tar_compat=1):
    """FILTERED tar-compat recompress: dirs+links lost their verbatim tar bytes when blocks
    were re-framed — synthesize them as ONE bodyless typed PACK_FILES frame (C headers, no
    fs access; dirs first, links last so hardlinks follow their targets)."""
    d = pl.concat(symparts)
    if not d.height:
        return False
    b.pack_files_df(level, "/", d.with_columns(
        _o=pl.when(pl.col("is_dir") == 1).then(0).otherwise(1)).sort(["_o", "path"]).select(
        fid=pl.lit(fid, pl.Int64),
        type=pl.when(pl.col("is_dir") == 1).then(5)
               .when(pl.col("in_off") == 1).then(1).otherwise(2).cast(pl.UInt8),
        mode=pl.col("mode").cast(pl.Int32), size=pl.lit(0, pl.Int64),
        mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int32), gid=pl.col("gid").cast(pl.Int32),
        src_off=pl.lit(0, pl.Int64), path="path", link="link"),
        sink_id=sink_id, tar_compat=tar_compat)
    return True


def _links_footer(parts):
    """Concat block<0 meta dfs -> footer rows: dir (is_dir=1) -> frame -1; symlink -> -2;
    hardlink (kind in in_off) -> -3. Vectorized."""
    if not parts:
        return _stat_rows([])
    d = pl.concat(parts)
    return d.select(
        path="path", size=pl.col("size").cast(pl.Int64), mode=pl.col("mode").cast(pl.Int64),
        mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64), gid=pl.col("gid").cast(pl.Int64),
        frame=pl.when(pl.col("is_dir") == 1).then(-1)
                .when(pl.col("in_off") == 1).then(-3).otherwise(-2).cast(pl.Int64),
        in_off=pl.lit(-1, pl.Int64), coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link="link")


def recompress_c(src, out, bvm_exe, nworkers=16, level=6, frame_bytes=1 << 20, tar_compat=True,
                 wal_path=None, predicate=None):
    """RECOMPRESS a foreign .tar.zstd -> nock in the corrected architecture: bvm (C)
    stream-DECODEs the archive into whole-member frames and STREAMS member STAT up;
    here we assign frame ids (+ could dedup/skip) and stream COPY(block)/SKIP down;
    bvm compresses in parallel and streams DONE; we write the footer. Single decode
    thread + a block budget bound memory. Returns member count. `tar_compat` keeps the
    raw tar bytes (headers inline) so the data section is a valid tar (zstd -dc | tar x).
    `wal_path`: the WAL is appended AFTER the executor drains (finish()), so a rerun skips
    the frames of a PREVIOUS COMPLETED run — a crash mid-run loses that run's progress
    (no fsync/frame-level commit yet; streaming DONE + fsync barriers are future work,
    and nothing here is durable until the kernel flushes).
    `predicate` (a member-dict -> bool, from `_member_filter`) keeps only matching members:
    filtered blocks are re-framed via COPY_MEMBERS (planner-synthesized headers when
    tar_compat); unfiltered blocks keep the fast verbatim COPY_BLOCK path."""
    committed, cursor, _ = _wal_load(wal_path) if wal_path else ({}, 0, None)
    resume = bool(committed)
    done_paths = set()
    for _f, (_c, _c2, st) in committed.items():
        done_paths |= set(st["path"].to_list())
    if not resume:
        open(out, "wb").close()
    b = _Bvm(bvm_exe, nworkers)
    b.sink(0, out, start=cursor); b.open_tar(0, os.path.abspath(src), frame_bytes, tar_compat=tar_compat)
    fid = (max(committed) + 1) if committed else 0
    planmap, symparts = {}, []
    while True:                                          # C streams member STAT per block
        t, pld = b.read()
        if t is None or t == 1:
            break
        if t != 0:
            continue
        _sid, block, df = _stat_df(pld)
        if block < 0:                                    # sym/hardlinks -> footer-only
            expr = getattr(predicate, "expr", None) if predicate else None
            symparts.append(df.filter(expr) if expr is not None else df); continue
        if _plan_block(b, block, df, fid, level, tar_compat, predicate, done_paths, planmap):
            fid += 1
    if tar_compat and predicate is not None and symparts:   # re-framed: dirs+links need synth headers
        if _send_meta_frame(b, symparts, fid, level):
            fid += 1
    locs = b.finish()
    parts = [st for _f, (_c, _c2, st) in sorted(committed.items())] if resume else []
    wal = _wal_open(wal_path, resume) if wal_path else None
    end = cursor
    for f_id, fdf in planmap.items():
        coff, clen = locs[f_id]; end = max(end, coff + clen)
        st = _frame_footer(fdf, f_id, coff, clen)
        if wal:
            _wal_append(wal, f_id, coff + clen, st)
        parts.append(st)
    if wal:
        wal.close()
    if symparts:
        parts.append(_links_footer(symparts))
    end = max([end] + [c + l for c, l in locs.values()])    # incl. the meta frame
    fd = os.open(out, os.O_RDWR); os.ftruncate(fd, end)
    if tar_compat:
        end = _tar_eof_frame(fd, end, level)
    write_footer(fd, end, parts if parts else [_stat_rows([])]); os.close(fd)
    return sum(int((p["frame"] != -1).sum()) for p in parts)


def recompress_multi(srcs, out, bvm_exe, nworkers=16, level=6, frame_bytes=1 << 20, budget_mb=512,
                     tar_compat=True, predicate=None):
    """Recompress MULTIPLE tars into one nock in a SINGLE bvm session — N decode
    threads run concurrently (shared worker pool + block budget), parallelizing the
    otherwise-serial per-source decode. STAT is tagged by src_id; members from all
    sources land in one footer. Returns member count. `tar_compat` keeps raw tar bytes
    inline; all members + one trailing EOF frame = a single valid tar (zstd -dc | tar x)."""
    open(out, "wb").close()
    b = _Bvm(bvm_exe, nworkers, budget_mb)
    b.sink(0, out)
    for sid, s in enumerate(srcs):
        b.open_tar(sid, os.path.abspath(s), frame_bytes, tar_compat=tar_compat)
    fid, planmap, eofs, symparts = 0, {}, 0, []
    while eofs < len(srcs):
        t, pld = b.read()
        if t is None:
            break
        if t == 1:
            eofs += 1; continue
        if t == 0:
            _sid, block, df = _stat_df(pld)
            if block < 0:                                # sym/hardlinks -> footer-only
                expr = getattr(predicate, "expr", None) if predicate else None
                symparts.append(df.filter(expr) if expr is not None else df); continue
            if _plan_block(b, block, df, fid, level, tar_compat, predicate, None, planmap):
                fid += 1
    if tar_compat and predicate is not None and symparts:   # re-framed: dirs+links need synth headers
        if _send_meta_frame(b, symparts, fid, level):
            fid += 1
    locs = b.finish()
    parts, end = [], 0
    for f_id, fdf in planmap.items():
        coff, clen = locs[f_id]; end = max(end, coff + clen)
        parts.append(_frame_footer(fdf, f_id, coff, clen))
    end = max([end] + [c + l for c, l in locs.values()])    # incl. the meta frame
    parts.append(_links_footer(symparts))
    fd = os.open(out, os.O_RDWR); os.ftruncate(fd, end)
    if tar_compat:
        end = _tar_eof_frame(fd, end, level)
    write_footer(fd, end, parts); os.close(fd)
    return sum(p.height for p in parts)


def _shard_path(tmpl, i):
    if "{shard}" in tmpl:
        return tmpl.format(shard=i)
    if "%d" in tmpl:
        return tmpl % i
    return f"{tmpl}.{i}"


def recompress_shards(srcs, out_tmpl, bvm_exe, shards, nworkers=16, level=6,
                      frame_bytes=1 << 20, tar_compat=True, budget_mb=512, predicate=None):
    """Recompress tar source(s) into N SHARD nocks in ONE pass — whole frames are routed
    round-robin across N sink files (even split; every member lands in exactly one shard,
    each shard a self-contained valid nock). `out_tmpl` uses '{shard}'/'%d', else a '.i'
    suffix. Union-unpack the shards (a glob) to reconstruct the tree. Returns total members.
    (tar_compat: each shard's frames are valid tar members + a trailing EOF; a shard mixes
    members from many source positions, so read a shard with `tar --ignore-zeros`.)"""
    if isinstance(srcs, str):
        srcs = [srcs]
    paths = [_shard_path(out_tmpl, i) for i in range(shards)]
    for p in paths:
        open(p, "wb").close()
    b = _Bvm(bvm_exe, nworkers, budget_mb)
    for i, p in enumerate(paths):
        b.sink(i, p)
    for sid, s in enumerate(srcs):
        b.open_tar(sid, os.path.abspath(s), frame_bytes, tar_compat=tar_compat)
    fid, planmap, shard_of, eofs, blk, symparts = 0, {}, {}, 0, 0, []
    while eofs < len(srcs):
        t, pld = b.read()
        if t is None:
            break
        if t == 1:
            eofs += 1; continue
        if t == 0:
            _sid, block, df = _stat_df(pld)
            if block < 0:                                # sym/hardlinks -> shard 0's footer
                expr = getattr(predicate, "expr", None) if predicate else None
                symparts.append(df.filter(expr) if expr is not None else df); continue
            sh = blk % shards; blk += 1                  # round-robin whole-frame routing
            if _plan_block(b, block, df, fid, level, tar_compat, predicate, None, planmap, sink_id=sh):
                shard_of[fid] = sh; fid += 1
    meta_fid = None
    if tar_compat and predicate is not None and symparts:   # meta frame lives in shard 0
        if _send_meta_frame(b, symparts, fid, level, sink_id=0):
            meta_fid = fid; fid += 1
    locs = b.finish()
    by_shard = {i: [] for i in range(shards)}
    for f_id in planmap:
        by_shard[shard_of[f_id]].append(f_id)
    total = 0
    for i in range(shards):
        rows, end = [], 0
        for dense, f_id in enumerate(sorted(by_shard[i])):   # per-shard dense frame ids
            coff, clen = locs[f_id]; end = max(end, coff + clen)
            rows.append(_frame_footer(planmap[f_id], dense, coff, clen))
        if i == 0:
            rows.append(_links_footer(symparts))          # symlinks live in shard 0
        if i == 0 and meta_fid is not None:
            c0, l0 = locs[meta_fid]; end = max(end, c0 + l0)
        fd = os.open(paths[i], os.O_RDWR); os.ftruncate(fd, end)
        if tar_compat and rows:
            end = _tar_eof_frame(fd, end, level)
        write_footer(fd, end, rows if rows else [_stat_rows([])]); os.close(fd)
        total += sum(p.height for p in rows)
    return total


def diff_trees(a, b, bvm_exe, nworkers=8, digest=False):
    """Two-tree diff — the rsync / tar-vs-fs primitive. Each of a,b is a spec:
    ('fs', root) | ('tar', path) | ('nock', path). The fs/tar sources STREAM their
    STAT from ONE bvm session in parallel (tar in stat-only mode = parse headers,
    skip bodies); a nock reads its footer. The two STAT streams are JOINed by path,
    classifying each as added (in B not A) / removed (in A not B) / changed /
    unchanged by (size, mtime@second-granularity — tar mtime is second-precision,
    so we coarsen fs to match). Returns {added,removed,changed,unchanged} path lists."""
    trees = {0: {}, 1: {}}
    sess = _Bvm(bvm_exe, nworkers); live = 0
    for sid, (kind, path) in enumerate((a, b)):
        if kind == "fs":
            sess.scan_fs(sid, os.path.abspath(path)); live += 1
        elif kind == "tar":
            sess.open_tar(sid, os.path.abspath(path), 1 << 20, stat_only=1); live += 1
        elif kind == "nock":
            idx = scan_nock(path)
            for r in idx.filter(pl.col("frame") >= 0).iter_rows(named=True):
                trees[sid][r["path"]] = (r["size"], r["mtime_ns"] // 10**9)
        else:
            raise ValueError(f"bad spec kind {kind}")
    eofs = 0
    while eofs < live:
        t, pld = sess.read()
        if t is None:
            break
        if t == 1:
            eofs += 1; continue
        if t == 0:
            sid, _blk, df = _stat_df(pld)
            df = df.filter(pl.col("is_dir") == 0)            # compare files/links only
            trees[sid].update(zip(df["path"].to_list(),
                                  zip(df["size"].to_list(), (df["mtime_ns"] // 10**9).to_list())))
    sess.finish()
    A, B = trees[0], trees[1]
    return dict(added=sorted(p for p in B if p not in A),
                removed=sorted(p for p in A if p not in B),
                changed=sorted(p for p in A if p in B and A[p] != B[p]),
                unchanged=sorted(p for p in A if p in B and A[p] == B[p]))


def diff_stream(a_root, b_root, bvm_exe, nworkers=16):
    """STREAMING two-tree diff (fs-vs-fs): partition by top-level subtree, fan the
    SCAN_FS out to bvm's bounded walker pool (parallel), and JOIN + emit + RELEASE
    each partition the moment BOTH its sides finish (SRC_EOF) — so only in-flight
    partitions are ever held, never the whole tree. This is the streaming-rsync
    foundation (each partition's delta could be transferred as it's found). Returns
    {added,removed,changed,unchanged} (paths relative to the roots)."""
    def isdir(root, name):
        return os.path.isdir(os.path.join(root, name))
    a_top = set(os.listdir(a_root)) if os.path.isdir(a_root) else set()
    b_top = set(os.listdir(b_root)) if os.path.isdir(b_root) else set()
    dir_parts = sorted(d for d in a_top | b_top if isdir(a_root, d) or isdir(b_root, d))
    file_tops = sorted(f for f in a_top | b_top if not (isdir(a_root, f) or isdir(b_root, f)))
    sess = _Bvm(bvm_exe, nworkers)
    part, remaining = {}, 0
    for pidx, d in enumerate(dir_parts):                 # issue fine-grained subtree scans
        p = {"dir": d, 0: {}, 1: {}, "eof": set(), "need": set()}
        for tree, root in ((0, a_root), (1, b_root)):
            if isdir(root, d):
                p["need"].add(tree); remaining += 1
                sess.scan_fs(pidx * 2 + tree, os.path.join(root, d))
        part[pidx] = p
    delta = dict(added=[], removed=[], changed=[], unchanged=[])
    def join(p):                                          # merge a completed partition, then drop it
        d, A, B = p["dir"], p[0], p[1]
        for path, kb in B.items():
            full = f"{d}/{path}"
            ka = A.get(path)
            delta["added" if ka is None else "changed" if ka != kb else "unchanged"].append(full)
        for path in A:
            if path not in B:
                delta["removed"].append(f"{d}/{path}")
    while remaining > 0:
        t, pld = sess.read()
        if t is None:
            break
        if t == 0:
            sid, _blk, df = _stat_df(pld); pidx, tree = sid // 2, sid % 2
            df = df.filter(pl.col("is_dir") == 0)            # compare files/links only
            part[pidx][tree].update(zip(df["path"].to_list(),
                                        zip(df["size"].to_list(), (df["mtime_ns"] // 10**9).to_list())))
        elif t == 1:
            sid = struct.unpack("<I", pld)[0]; pidx, tree = sid // 2, sid % 2; remaining -= 1
            p = part[pidx]; p["eof"].add(tree)
            if p["eof"] >= p["need"]:                     # both sides in -> join + release
                join(p); part[pidx] = None
    sess.finish()
    def key(root, f):
        st = os.lstat(os.path.join(root, f)); return (st.st_size, st.st_mtime_ns // 10**9)
    for f in file_tops:                                   # top-level files: direct compare
        a_ex = f in a_top and not isdir(a_root, f); b_ex = f in b_top and not isdir(b_root, f)
        if a_ex and b_ex:
            delta["changed" if key(a_root, f) != key(b_root, f) else "unchanged"].append(f)
        elif b_ex:
            delta["added"].append(f)
        elif a_ex:
            delta["removed"].append(f)
    for k in delta:
        delta[k].sort()
    return delta


def find(root, bvm_exe, name=None, min_size=None, max_size=None, newer_than_ns=None,
         nworkers=16):
    """Parallel `find`: fan SCAN_FS across bvm's bounded walker pool over the top-level
    subtrees (metadata-bound → scales on WEKA), stream STAT, and apply predicates in
    Python. `name` = a glob on the basename; size in bytes; `newer_than_ns` compares
    mtime. Returns matching paths (relative to `root`), sorted. The `find(1)` replacement."""
    import re
    rx = None
    if name:
        rx = re.compile("^" + "".join(".*" if c == "*" else "." if c == "?" else re.escape(c)
                                      for c in name) + "$")

    def ok(path, size, mtime):
        return not ((rx and not rx.match(os.path.basename(path)))
                    or (min_size is not None and size < min_size)
                    or (max_size is not None and size > max_size)
                    or (newer_than_ns is not None and mtime <= newer_than_ns))

    tops = os.listdir(root) if os.path.isdir(root) else []
    dirs = sorted(d for d in tops if os.path.isdir(os.path.join(root, d)))
    out = []
    for f in sorted(tops):                                   # top-level files: direct stat
        fp = os.path.join(root, f)
        if not os.path.isdir(fp):
            st = os.lstat(fp)
            if ok(f, st.st_size, st.st_mtime_ns):
                out.append(f)
    if dirs:
        sess = _Bvm(bvm_exe, nworkers)
        remaining = 0
        for i, d in enumerate(dirs):
            sess.scan_fs(i, os.path.join(root, d)); remaining += 1
        while remaining > 0:
            t, pld = sess.read()
            if t is None:
                break
            if t == 0:
                sid, _blk, df = _stat_df(pld)
                df = df.filter(pl.col("is_dir") == 0)        # predicates NATIVELY, no per-row Python
                if rx is not None:
                    df = df.filter(pl.col("path").str.extract(r"([^/]*)$").str.contains(rx.pattern))
                if min_size is not None:
                    df = df.filter(pl.col("size") >= min_size)
                if max_size is not None:
                    df = df.filter(pl.col("size") <= max_size)
                if newer_than_ns is not None:
                    df = df.filter(pl.col("mtime_ns") > newer_than_ns)
                pre = dirs[sid] + "/"
                out.extend(pre + p for p in df["path"].to_list())
            elif t == 1:
                remaining -= 1
        sess.finish()
    out.sort()
    return out


# ------------------------------------------------------------------ networked rsync
def _sodium():
    import ctypes
    for cand in ("/mnt/weka/jpc/miniconda3/envs/brouhaha/lib/libsodium.so",
                 "libsodium.so", "libsodium.so.23"):
        try:
            so = ctypes.CDLL(cand); so.sodium_init(); return so
        except OSError:
            continue
    raise RuntimeError("libsodium not found")


def _kx_keypair(so):
    import ctypes
    pk = ctypes.create_string_buffer(so.crypto_kx_publickeybytes())
    sk = ctypes.create_string_buffer(so.crypto_kx_secretkeybytes())
    so.crypto_kx_keypair(pk, sk); return pk.raw, sk.raw


def _kx_session(so, client, pk, sk, peer):
    import ctypes
    n = so.crypto_kx_sessionkeybytes()
    rx = ctypes.create_string_buffer(n); tx = ctypes.create_string_buffer(n)
    (so.crypto_kx_client_session_keys if client else so.crypto_kx_server_session_keys)(rx, tx, pk, sk, peer)
    return rx.raw, tx.raw


def _fs_entries(root):
    """Scan `root` -> (files, links, dirs) for rsync. files[rel] = (size, mtime_ns, mode,
    uid, gid); links[rel] = (kind, target, mtime_ns, uid, gid, mode) with kind 0=symlink
    (target=path), 1=hardlink (target=another rel sharing the inode); dirs[rel] = (mode,
    mtime_ns, uid, gid) — EVERY dir, so empty dirs + dir metadata sync too. Captures
    symlinked dirs; the first path at a multi-link inode is the file, later hardlinks."""
    import stat as _st
    files, links, dirs, inodes = {}, {}, {}, {}
    for dp, dns, fns in os.walk(root):
        symdirs = [d for d in dns if os.path.islink(os.path.join(dp, d))]
        for d in symdirs:
            dns.remove(d)
        for d in dns:
            p = os.path.join(dp, d)
            try:
                sd = os.lstat(p)
            except OSError:
                continue
            dirs[os.path.relpath(p, root)] = (sd.st_mode & 0o7777, sd.st_mtime_ns, sd.st_uid, sd.st_gid)
        for name in fns + symdirs:
            p = os.path.join(dp, name)
            try:
                s = os.lstat(p)
            except OSError:
                continue
            rel = os.path.relpath(p, root)
            if _st.S_ISLNK(s.st_mode):
                links[rel] = (0, os.readlink(p), s.st_mtime_ns, s.st_uid, s.st_gid, s.st_mode & 0o7777)
            elif _st.S_ISREG(s.st_mode):
                if s.st_nlink > 1:
                    key = (s.st_dev, s.st_ino)
                    if key in inodes:
                        links[rel] = (1, inodes[key], s.st_mtime_ns, s.st_uid, s.st_gid, s.st_mode & 0o7777)
                        continue
                    inodes[key] = rel
                files[rel] = (s.st_size, s.st_mtime_ns, s.st_mode & 0o7777, s.st_uid, s.st_gid)
    return files, links, dirs


def _recvall(f, n):
    b = f.read(n)
    while len(b) < n:
        m = f.read(n - len(b))
        if not m:
            break
        b += m
    return b


def rsync_serve(dst, bvm_exe, ctrl_port, data_port, n=4, nworkers=8, apply=True, delete=True):
    """RECEIVER side of networked rsync (runs on the target host). Accepts the control
    connection, does the X25519 KX, sends its tree's STAT for the diff, then a bvm
    LISTENs `n` encrypted data connections and applies the delta on the fly. Deletes
    the sender's removed paths. Returns the delete count."""
    import socket, struct, pickle
    os.makedirs(dst, exist_ok=True); so = _sodium()
    cs = socket.socket(); cs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    cs.bind(("0.0.0.0", ctrl_port)); cs.listen(1)
    conn, _ = cs.accept(); rf = conn.makefile("rb")
    pk, sk = _kx_keypair(so); peer = _recvall(rf, so.crypto_kx_publickeybytes()); conn.sendall(pk)
    rx, _tx = _kx_session(so, False, pk, sk, peer)
    blob = pickle.dumps(_fs_entries(dst)); conn.sendall(struct.pack("<Q", len(blob)) + blob)
    (ml,) = struct.unpack("<Q", _recvall(rf, 8))            # delete lists (files, dirs) — FIRST,
    delfiles, deldirs = pickle.loads(_recvall(rf, ml))      # so unlinks can't re-bump dir mtimes
    if delete and apply:                                    # the data plane is about to restore
        for rel in delfiles:
            try:
                os.remove(os.path.join(dst, rel))
            except OSError:
                pass
        for rel in sorted(deldirs, key=lambda r: -r.count("/")):   # children before parents
            try:
                os.rmdir(os.path.join(dst, rel))
            except OSError:
                pass
    recv = _Bvm(bvm_exe, nworkers); recv.dest(dst); recv.key(rx)
    recv.listen(data_port, n, apply=1 if apply else 0)      # sender's sinks retry-connect (bvm)
    recv.finish()                                           # bvm applied files INLINE (mode+mtime+
    conn.close(); cs.close()                                # chown), links + dir metadata at exit
    return len(delfiles) + len(deldirs)


def rsync_push(src, bvm_exe, host, ctrl_port, data_port, n=4, nworkers=8, level=6, frame_bytes=1 << 20):
    """SENDER side of networked rsync (runs on the source host). KX on the control
    plane, receives the target's STAT, diffs src → the delta, and streams only the
    added/changed files over `n` encrypted data connections (sharded). Returns
    {sent, deleted, frames}."""
    import socket, struct, pickle
    so = _sodium(); c = None
    for _ in range(100):
        try:
            c = socket.create_connection((host, ctrl_port)); break
        except OSError:
            time.sleep(0.1)
    rf = c.makefile("rb")
    pk, sk = _kx_keypair(so); c.sendall(pk); peer = _recvall(rf, so.crypto_kx_publickeybytes())
    _rx, tx = _kx_session(so, True, pk, sk, peer)
    (sl,) = struct.unpack("<Q", _recvall(rf, 8)); tgtfiles, tgtlinks, tgtdirs = pickle.loads(_recvall(rf, sl))
    srcfiles, srclinks, srcdirs = _fs_entries(src)
    def fkey(v): return (v[0], v[1])                         # (size, mtime_ns) EXACT: both ends are
    send = [rel for rel in srcfiles if fkey(srcfiles[rel]) != (fkey(tgtfiles[rel]) if rel in tgtfiles else None)]
    send_links = {rel: v for rel, v in srclinks.items()      # our scanner/applier, which restores ns
                  if (v[0], v[1]) != (tgtlinks[rel][:2] if rel in tgtlinks else None)}   # faithfully — @s coarsening
    send_dirs = {rel: v for rel, v in srcdirs.items()        # left same-second drift unconvergeable
                 if rel not in tgtdirs or (v[0], v[1]) != (tgtdirs[rel][0], tgtdirs[rel][1])}
    keep = set(srcfiles) | set(srclinks)
    delete = [rel for rel in (set(tgtfiles) | set(tgtlinks)) if rel not in keep]
    deldirs = [rel for rel in tgtdirs if rel not in srcdirs]
    blob = pickle.dumps((delete, deldirs))               # deletions FIRST (receiver applies them
    c.sendall(struct.pack("<Q", len(blob)) + blob)       # BEFORE the data plane: a post-apply
    frames, cur, cb = [], [], 0
    for rel in sorted(send):
        sz = srcfiles[rel][0]
        if cur and cb + sz > frame_bytes:
            frames.append(cur); cur, cb = [], 0
        cur.append(rel); cb += sz
    if cur:
        frames.append(cur)
    s = _Bvm(bvm_exe, nworkers); s.key(tx)
    for k in range(n):
        s.sink(k, f"{host}:{data_port}", kind=1)
    def _typed(fi, rows):                                    # rows: (type, mode, size, mt, uid, gid, path, link)
        t_, m_, z_, mt_, u_, g_, p_, l_ = zip(*rows)
        return pl.DataFrame({
            "fid": pl.Series([fi] * len(rows), dtype=pl.Int64),
            "type": pl.Series(t_, dtype=pl.UInt8), "mode": pl.Series(m_, dtype=pl.Int32),
            "size": pl.Series(z_, dtype=pl.Int64), "mtime_ns": pl.Series(mt_, dtype=pl.Int64),
            "uid": pl.Series(u_, dtype=pl.Int32), "gid": pl.Series(g_, dtype=pl.Int32),
            "src_off": pl.Series([0] * len(rows), dtype=pl.Int64),
            "path": pl.Series(p_, dtype=pl.Utf8), "link": pl.Series(l_, dtype=pl.Utf8)})
    for fi, rels in enumerate(frames):                       # file frames: bodies + full metadata
        s.pack_files_df(level, os.path.abspath(src), _typed(fi, [
            (0, srcfiles[r][2], srcfiles[r][0], srcfiles[r][1], srcfiles[r][3], srcfiles[r][4], r, "")
            for r in rels]), sink_id=fi % n)
    if send_dirs or send_links:                              # meta frame: dirs + links, bodyless
        rows = [(5, v[0], 0, v[1], v[2], v[3], r, "") for r, v in sorted(send_dirs.items())]              + [(1 if v[0] == 1 else 2, v[5], 0, v[2], v[3], v[4], r, v[1]) for r, v in send_links.items()]
        s.pack_files_df(level, os.path.abspath(src), _typed(len(frames), rows), sink_id=0)
    s.finish()
    c.close()
    return dict(sent=len(send), links=len(send_links), dirs=len(send_dirs),
                deleted=len(delete) + len(deldirs), frames=len(frames))


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def rsync(src, dst, bvm_exe, n=4, nworkers=8, level=6):
    """Sync src → dst over N encrypted connections on localhost (receiver in a thread).
    Convenience wrapper over rsync_serve/rsync_push; returns the sender's summary."""
    import threading
    cport, dport = _free_port(), _free_port()
    res = {}
    t = threading.Thread(target=lambda: res.setdefault(
        "d", rsync_serve(dst, bvm_exe, cport, dport, n, nworkers)))
    t.start(); time.sleep(0.2)
    out = rsync_push(src, bvm_exe, "127.0.0.1", cport, dport, n, nworkers, level)
    t.join(); return out


def scan_nock(path, at=None):
    """SCAN(NOCK): read the chunked footer STAT table (NOCKZC01). Decodes NO data frames
    (they are lazy). Normalizes the on-disk locator names (frame_coff/clen -> coff/clen)
    so the rest of blocks sees one schema. For memory-bounded/streaming reads, callers
    can use nockidx.iter_batches / read_directory directly."""
    from ..nock import nockidx
    df = nockidx.read_footer(path, at=at)
    ren = {a: b for a, b in (("frame_coff", "coff"), ("frame_clen", "clen"))
           if a in df.columns}
    if ren:
        df = df.rename(ren)
    if "digest" not in df.columns:
        df = df.with_columns(digest=pl.lit(-1, pl.Int64))
    if "link" not in df.columns:                             # legacy footer (pre-symlink)
        df = df.with_columns(link=pl.lit("", pl.Utf8))
    return df


# ------------------------------------------------------------------ COPY
# ------------------------------------------------------------------ drivers
def write_footer(sink_fd, cursor, parts, level=3, rows_per_batch=1 << 20, prev_off=None,
                 reuse_ents=None, part_cuts=False, snap_time_ns=None, prev_commit=None):
    """Append the nock footer at `cursor`: STAT -> row-batched, independently
    zstd-compressed skippable frames + directory + trailer (NOCKZC01 via
    nockidx.pack_footer). On disk the locator columns use the canonical
    frame_coff/frame_clen names; the returned in-memory STAT keeps blocks' coff/clen."""
    from ..nock import nockidx
    stat = pl.concat([p for p in parts if p.height], how="vertical_relaxed") \
        if any(p.height for p in parts) else _stat_rows([])
    disk = stat.rename({a: b for a, b in (("coff", "frame_coff"), ("clen", "frame_clen"))
                        if a in stat.columns})
    fc = None
    if part_cuts:                                       # PART boundaries never share a batch —
        acc, fc = 0, []                                 # keeps meta rows out of reusable batches
        for p in parts:
            acc += p.height; fc.append(acc)
    os.pwrite(sink_fd, nockidx.pack_footer(disk, cursor, rows_per_batch, level,
                                           prev_off=prev_off, reuse_ents=reuse_ents,
                                           forced_cuts=fc, snap_time_ns=snap_time_ns,
                                           prev_commit=prev_commit), cursor)
    return stat


def unpack(nocks, dest, bvm_exe, nworkers=16, predicate=None, shuffle=True, at=None,
           stream=None):
    """THE unpack: nock(s) -> filesystem. `nocks` = one path or a list (a set / glob).
    Handles every member kind: files (frame>=0), dirs (-1: created up front, mode+mtime
    applied LAST), symlinks (-2), hardlinks (-3: after all files), EXTENT members (-4:
    lazy-reference snapshot files stitched from pieces of many frames). `at` = snapshot
    end offset(s) to restore an older snapshot from an append-only chain (scalar or
    per-nock list). Two send modes:
      stream=True   one message per footer batch, bounded memory; `shuffle` randomizes
                    frame order WITHIN each batch (single-source default);
      stream=False  collect all frames, shuffle GLOBALLY (workers spread across sources'
                    directory trees — the multi-nock default), send in ~1M-row chunks.
    Returns entries created (files + links + extent members)."""
    import numpy as np, random
    from ..nock import nockidx
    if isinstance(nocks, str):
        nocks = [nocks]
    ats = at if isinstance(at, (list, tuple)) else [at] * len(nocks)
    if stream is None:
        stream = len(nocks) == 1
    os.makedirs(dest, exist_ok=True)
    b = _Bvm(bvm_exe, nworkers); b.dest(dest)
    parts, hardlinks, dirmeta = [], [], []
    n = 0

    def _send(sdf, local_shuffle):
        nonlocal n
        if local_shuffle:
            bnd = (pl.col("nock").diff() != 0) | (pl.col("coff").diff() != 0) | (pl.col("clen").diff() != 0)
            sdf = (sdf.with_columns(_g=bnd.fill_null(True).cum_sum())
                      .with_columns(_k=pl.col("_g").hash(seed=random.randrange(1 << 62)))
                      .sort("_k", maintain_order=True)     # stable: rows stay grouped per frame
                      .drop("_g", "_k"))
        for i in range(0, sdf.height, 1 << 20):
            b.scatter_df(sdf.slice(i, 1 << 20))            # a chunk splitting a frame is harmless

    base = 0                                            # nock_ids are per PHYSICAL file:
    for k, nk in enumerate(nocks):                      # a sharded store contributes several
        sfiles = store_files(nk)
        for si, sp in enumerate(sfiles):
            b.nock(os.path.abspath(sp), nock_id=base + si)
        for batch in nockidx.iter_batches(nk, at=ats[k]):
            if "shard" not in batch.columns:
                batch = batch.with_columns(shard=pl.lit(0, pl.Int64))
            batch = batch.rename({a: c for a, c in (("frame_coff", "coff"), ("frame_clen", "clen"))
                                  if a in batch.columns})
            ext_pieces = []
            neg = batch.filter(pl.col("frame") < 0)
            ext = neg.filter(pl.col("frame") == -4)
            if predicate is not None and ext.height:
                ext = ext.filter(predicate)
            for r in ext.iter_rows(named=True):            # EXTENT members -> scatter pieces
                for coff_, clen_, io_, ln_, oo_, sh_ in _parse_extents(r["extents"]):
                    ext_pieces.append(dict(nock=base + sh_, coff=coff_, clen=clen_,
                                           in_off=io_, size=ln_,
                                           out_off=oo_, fsize=r["size"], mode=r["mode"] & 0o7777,
                                           mtime_ns=r["mtime_ns"], uid=r["uid"], gid=r["gid"],
                                           path=r["path"]))
                n += 1
            for r in neg.filter(pl.col("frame") != -4).iter_rows(named=True):
                p = os.path.join(dest, r["path"])
                if r["frame"] == -3:
                    hardlinks.append(r); continue
                if r["frame"] == -2:
                    dd = os.path.dirname(p)
                    if dd:
                        os.makedirs(dd, exist_ok=True)
                    if os.path.lexists(p):
                        os.remove(p)
                    os.symlink(r["link"], p); n += 1
                    try:                                    # the symlink's own metadata
                        if r["uid"] or r["gid"]:
                            os.chown(p, r["uid"], r["gid"], follow_symlinks=False)
                        os.utime(p, ns=(r["mtime_ns"], r["mtime_ns"]), follow_symlinks=False)
                    except (OSError, NotImplementedError):
                        pass
                else:                                       # -1: dir now, metadata LAST
                    os.makedirs(p, exist_ok=True)
                    dirmeta.append((p, r["mode"], r["mtime_ns"]))
            files = batch.filter(pl.col("frame") >= 0)
            if predicate is not None:
                files = files.filter(predicate)
            if not files.height and not ext_pieces:
                continue
            sdf = files.select(                             # canonical SCATTER_SCHEMA
                nock=(pl.lit(base, pl.Int32) + pl.col("shard").cast(pl.Int32)),
                coff=pl.col("coff"), clen=pl.col("clen"),
                in_off=pl.col("in_off"), size=pl.col("size"),
                out_off=pl.lit(0, pl.Int64), fsize=pl.col("size"),
                mode=pl.col("mode").cast(pl.Int32),
                mtime_ns=pl.col("mtime_ns"), uid=pl.col("uid").cast(pl.Int32),
                gid=pl.col("gid").cast(pl.Int32), path=pl.col("path"))
            if ext_pieces:                                  # pieces join the stream, grouped by frame
                pdf2 = pl.DataFrame(ext_pieces).select(
                    *[pl.col(c).cast(t) for c, t in _Bvm.SCATTER_SCHEMA.items()])
                sdf = pl.concat([sdf, pdf2]).sort(["nock", "coff", "clen"], maintain_order=True)
            n += files.height
            if stream:
                _send(sdf, shuffle)                         # bounded: one message per batch
            else:
                parts.append(sdf)
        base += len(sfiles)
    if not stream and parts:
        _send(pl.concat(parts), shuffle)                    # GLOBAL cross-source shuffle
    b.finish()
    for r in hardlinks:                                     # after all files exist
        p = os.path.join(dest, r["path"]); tgt = os.path.join(dest, r["link"])
        dd = os.path.dirname(p)
        if dd:
            os.makedirs(dd, exist_ok=True)
        if os.path.lexists(p):
            os.remove(p)
        try:
            os.link(tgt, p); n += 1
        except OSError:
            pass
    for p, md, mt in dirmeta:                               # LAST: restrictive dir modes/mtimes
        try:                                                # can't block the writes above
            if md:
                os.chmod(p, md & 0o7777)
            if mt:
                os.utime(p, ns=(mt, mt))
        except OSError:
            pass
    return n


def unpack_c(nock, dest, bvm_exe, nworkers=16, predicate=None, shuffle=False, at=None):
    """Single-nock unpack (streaming, batch-local shuffle) — thin wrapper over unpack()."""
    return unpack(nock, dest, bvm_exe, nworkers=nworkers, predicate=predicate,
                  shuffle=shuffle, at=at, stream=True)


def unpack_set(nocks, dest, bvm_exe, nworkers=16, predicate=None, shuffle=True, at=None):
    """Multi-nock unpack (global cross-source shuffle) — thin wrapper over unpack()."""
    return unpack(nocks, dest, bvm_exe, nworkers=nworkers, predicate=predicate,
                  shuffle=shuffle, at=at, stream=False)


# ------------------------------------------------------------------ WAL (on top)
def _wal_open(path, resume):
    return open(path, "ab" if resume else "wb")


def _wal_append(wal, fid, cursor_after, stat):
    buf = io.BytesIO(); stat.write_ipc(buf); s = buf.getvalue()
    wal.write(struct.pack("<qqI", fid, cursor_after, len(s))); wal.write(s); wal.flush()
    os.fsync(wal.fileno())


def _wal_load(path):
    """Replay the WAL: {frame_id: (coff_after, cursor, stat)} + the resume cursor
    (end of the last durably-committed frame)."""
    committed, cursor = {}, 0
    if not path or not os.path.exists(path):
        return committed, 0, None
    with open(path, "rb") as f:
        data = f.read()
    off = 0
    while off + 20 <= len(data):
        fid, cur_after, n = struct.unpack_from("<qqI", data, off); off += 20
        if off + n > len(data):
            break                                            # torn tail record -> stop
        stat = pl.read_ipc(io.BytesIO(data[off:off + n])); off += n
        committed[fid] = (cur_after, cur_after, stat); cursor = max(cursor, cur_after)
    return committed, cursor, None

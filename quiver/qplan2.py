"""qplan2 — verbs as polars macros over the qvm2 instruction stream (docs/ISA3.md §2:
"the compiler front-end is Polars", taken literally). Each verb turns a data table into
an instruction table; qvm2 executes; footers are written planner-side with the SAME
nockidx machinery bvm's stores use, so everything qplan2 packs is a first-class nock
that blocks.verify/blocks.unpack read unmodified — that cross-engine gate is the
correctness contract (the reference-core method that validated BLOCKS itself).

v1 scope: pack (files+dirs, raw frames — no tar_compat), unpack (files+dirs+meta).
Symlinks/hardlinks/delta ride later; every omission is checked, not silent."""
import os, struct, subprocess, time

import polars as pl
import pyarrow as pa

QVM2 = os.path.join(os.path.dirname(__file__), "exec", "qvm2")

(NEWVAL, MOV, CLOSE, SPAWN, JOIN, SINK, EMIT,
 MKDIR, SYMLINK, LINK, SETMETA, UNLINK, RMDIR, FENCE, READDIR, STATB, SCAN, FREE) = range(18)
E_FS, E_VAL, E_INLINE, E_SINK = range(4)

ISCH = dict(tid=pl.UInt32, op=pl.UInt8, k1=pl.UInt8, k2=pl.UInt8,
            a=pl.Int64, b=pl.Int64, c=pl.Int64, d=pl.Int64,
            path=pl.Utf8, payload=pl.Binary)
_DEFAULTS = dict(k1=0, k2=0, a=0, b=0, c=0, d=0, path="", payload=b"")


def _pad(df):
    """Fill missing instruction columns with defaults, cast to the wire schema."""
    miss = {c: (pl.lit("", pl.Utf8) if c == "path" else
                pl.lit(b"", pl.Binary) if c == "payload" else pl.lit(_DEFAULTS.get(c, 0)))
            for c in ISCH if c not in df.columns}
    return df.with_columns(**miss).select(*[pl.col(c).cast(t) for c, t in ISCH.items()])


def _ipc(df):
    from .exec.blocks import _ipc_bytes
    return _ipc_bytes(_pad(df))


def _run(program_parts, timeout=3600):
    """Send instruction tables to one qvm2 IN ORDER, wait. ORDERING CONTRACT: a
    fiber's rows must be fed before any SPAWN covering it executes — polars splits
    big IPC streams into ~245k-row batches, so root control rows (SPAWN/JOIN) must
    ride in their own FINAL part, never sorted in front of megarow fiber bodies
    (the EVI unpack silently lost whole subtrees to exactly that: later-batch
    fibers spawned empty, completed instantly, made nothing)."""
    blob = b"".join(_ipc(p) for p in program_parts)
    proc = subprocess.Popen([QVM2, "stream"], stdin=subprocess.PIPE)
    proc.stdin.write(blob)
    proc.stdin.close()
    rc = proc.wait(timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"qvm2 exited {rc}")


def _emit_records(path):
    """The 32B EMIT records: tid -> (base, len, digest)."""
    out = {}
    buf = open(path, "rb").read()
    for off in range(0, len(buf), 32):
        tid, _pad_, base, ln, dg = struct.unpack_from("<IIqqq", buf, off)
        out[tid] = (base, ln, dg)
    return out


# ------------------------------------------------------------------ scan (I_SCAN leaf)
def scan(root, walkers=32, emitf=None):
    """The generator-leaf scan -> one polars DataFrame (name = relpath, kind 0/1/2,
    mode, size, mtime_ns, uid, gid)."""
    emitf = emitf or f"/tmp/qplan2.scan.{os.getpid()}"
    open(emitf, "wb").close()
    _run([pl.concat([
        _pad(pl.DataFrame(dict(tid=[0], op=[SINK], a=[0], b=[1], path=[emitf]))),
        _pad(pl.DataFrame(dict(tid=[1], op=[SCAN], a=[0], b=[walkers], path=[os.path.abspath(root)]))),
        _pad(pl.DataFrame(dict(tid=[0], op=[SPAWN], a=[1], b=[1]))),
        _pad(pl.DataFrame(dict(tid=[0], op=[JOIN], a=[1], b=[1])))])])
    buf = open(emitf, "rb").read()
    pos, msgs, schema = 0, [], None
    while pos + 8 <= len(buf):
        mlen, = struct.unpack_from("<I", buf, pos + 4)
        if mlen == 0:
            pos += 8; continue
        m = pos + 8
        root_, = struct.unpack_from("<I", buf, m)
        vt = (m + root_) - struct.unpack_from("<i", buf, m + root_)[0]
        vsz, = struct.unpack_from("<H", buf, vt)
        def fld(idx):
            slot = 4 + 2 * idx
            if slot >= vsz: return None
            vo, = struct.unpack_from("<H", buf, vt + slot)
            return (m + root_ + vo) if vo else None
        htp, blp = fld(1), fld(3)
        blen = struct.unpack_from("<q", buf, blp)[0] if blp else 0
        tot = 8 + mlen + blen
        if htp is not None and buf[htp] == 3:
            msgs.append(buf[pos:pos + tot])
        elif schema is None and mlen:
            schema = buf[pos:pos + tot]
        pos += tot
    blob = schema + b"".join(msgs) + b"\xff\xff\xff\xff\x00\x00\x00\x00"
    df = pl.from_arrow(pa.ipc.open_stream(pa.py_buffer(blob)).read_all())
    os.unlink(emitf)
    return df.filter(pl.col("kind") != 255).drop("tid", "final", "phase")


# ------------------------------------------------------------------ pack
def pack(root, out, level=3, frame_bytes=1 << 20, walkers=32, sdf=None):
    """Tree -> single-file nock (raw frames, inline NOCKZC01 footer). One fiber per
    frame: NEWVAL codec -> per-member MOV fs->val -> CLOSE -> MOV val->sink -> EMIT.
    All authored as column ops; the footer is written planner-side with blocks'
    write_footer, so the result is bvm-readable. Refuses symlinks (v1 scope, checked).
    Returns the summary dict."""
    from .exec import blocks
    root = os.path.abspath(root)
    if sdf is None:
        sdf = scan(root, walkers=walkers)
    if sdf.filter(pl.col("kind") == 2).height:
        raise NotImplementedError("qplan2.pack v1: symlinks/specials present in tree")
    files = (sdf.filter(pl.col("kind") == 0).sort("name")
             .with_columns(fid=(pl.col("size").cum_sum() - pl.col("size"))
                           // frame_bytes)
             .with_columns(fid=pl.col("fid").rank("dense").cast(pl.Int64) - 1,
                           in_off=(pl.col("size").cum_sum() - pl.col("size"))
                                  .over((pl.col("size").cum_sum() - pl.col("size")) // frame_bytes)))
    nframes = int(files["fid"].max()) + 1 if files.height else 0
    dirs = sdf.filter(pl.col("kind") == 1)

    open(out, "wb").close()
    # fibers: frame f -> tid 1+f; vals: vid f. Emit records to sink 1.
    emitf = out + ".emit"
    open(emitf, "wb").close()
    fh = files.with_columns(tid=(pl.col("fid") + 1).cast(pl.UInt32))
    prog = [
        pl.DataFrame(dict(tid=[0, 0], op=[SINK, SINK], a=[0, 1], b=[0, 0],
                          path=[out, emitf])),
        fh.group_by("tid", maintain_order=True)
          .agg(pl.col("fid").first(), pl.col("size").sum())
          .select(tid="tid", op=pl.lit(NEWVAL, pl.UInt8), a="fid",
                  b=pl.lit(1, pl.Int64), c=pl.lit(level, pl.Int64),
                  d="size"),                     # pledged raw size -> frame content size
        fh.select(tid="tid", op=pl.lit(MOV, pl.UInt8),
                  k1=pl.lit(E_FS, pl.UInt8), k2=pl.lit(E_VAL, pl.UInt8),
                  a=pl.lit(0, pl.Int64), b=pl.lit(-1, pl.Int64), d="fid",
                  path=pl.lit(root + "/") + pl.col("name")),
        fh.group_by("tid", maintain_order=True).agg(pl.col("fid").first())
          .select(tid="tid", op=pl.lit(CLOSE, pl.UInt8), a="fid"),
        fh.group_by("tid", maintain_order=True).agg(pl.col("fid").first())
          .select(tid="tid", op=pl.lit(MOV, pl.UInt8),
                  k1=pl.lit(E_VAL, pl.UInt8), k2=pl.lit(E_SINK, pl.UInt8),
                  a="fid", b=pl.lit(0, pl.Int64), c=pl.lit(-1, pl.Int64),
                  d=pl.lit(0, pl.Int64)),
        fh.group_by("tid", maintain_order=True).agg(pl.col("fid").first())
          .select(tid="tid", op=pl.lit(EMIT, pl.UInt8), a=pl.lit(1, pl.Int64)),
        fh.group_by("tid", maintain_order=True).agg(pl.col("fid").first())
          .select(tid="tid", op=pl.lit(FREE, pl.UInt8), a="fid"),
        pl.DataFrame(dict(tid=[0, 0], op=[SPAWN, JOIN], a=[1, 1],
                          b=[nframes, nframes])),
    ]
    # ROW ORDER within a fiber is execution order; fiber BODIES are sorted by
    # (tid, stage) and sent BEFORE root's SPAWN/JOIN (see _run's ordering contract).
    staged = []
    for si, p_ in enumerate(prog[1:-1], start=1):
        staged.append(_pad(p_).with_columns(_s=pl.lit(si)))
    body = pl.concat(staged).sort(["tid", "_s"], maintain_order=True).drop("_s")
    _run([_pad(prog[0]), body, _pad(prog[-1])])

    recs = _emit_records(emitf)
    os.unlink(emitf)
    loc = pl.DataFrame(dict(
        fid=pl.Series([t - 1 for t in sorted(recs)], dtype=pl.Int64),
        coff=pl.Series([recs[t][0] for t in sorted(recs)], dtype=pl.Int64),
        clen=pl.Series([recs[t][1] for t in sorted(recs)], dtype=pl.Int64)))
    stat = (files.join(loc, on="fid", how="left")
            .select(path="name", size=pl.col("size").cast(pl.Int64),
                    mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
                    mtime_ns="mtime_ns",
                    uid=pl.col("uid").cast(pl.Int64), gid=pl.col("gid").cast(pl.Int64),
                    frame="fid", in_off=pl.col("in_off").cast(pl.Int64),
                    coff="coff", clen="clen",
                    digest=pl.lit(-1, pl.Int64), link=pl.lit("", pl.Utf8)))
    dirsr = dirs.select(
        path="name", size=pl.lit(0, pl.Int64), mode=(pl.col("mode") & 0o7777).cast(pl.Int64),
        mtime_ns="mtime_ns", uid=pl.col("uid").cast(pl.Int64), gid=pl.col("gid").cast(pl.Int64),
        frame=pl.lit(-1, pl.Int64), in_off=pl.lit(-1, pl.Int64),
        coff=pl.lit(-1, pl.Int64), clen=pl.lit(-1, pl.Int64),
        digest=pl.lit(-1, pl.Int64), link=pl.lit("", pl.Utf8))
    end = int((stat["coff"] + stat["clen"]).max()) if stat.height else 0
    fd = os.open(out, os.O_RDWR)
    os.ftruncate(fd, end)
    blocks.write_footer(fd, end, [stat, dirsr])
    os.close(fd)
    return dict(files=files.height, dirs=dirs.height, frames=nframes,
                bytes=os.path.getsize(out))


# ------------------------------------------------------------------ unpack
def unpack(nock, dest, walkers=32, phase_times=None):
    """nock -> tree, authored from the footer: MKDIRs, one fiber per frame
    (decode window -> per-member scatter with TRUNC + SETMETA), restrictive dir
    metadata last — ISA3 §4.6 as column ops. Reads bvm-packed nocks too (the
    cross-engine gate). Returns entries created."""
    from .exec import blocks
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)
    foot = blocks.scan_nock(nock)
    if foot.filter(pl.col("frame") < -1).height:
        raise NotImplementedError("qplan2.unpack v1: links/extents in footer")
    files = foot.filter(pl.col("frame") >= 0)
    # DEPTH-SORTED dirs: parents queue before children, so the C-side
    # ENOENT->mkparents fallback (depth-many probing RPCs per miss) becomes a
    # never-taken safety net instead of the common path. Profiled: the unpack is
    # RPC-latency-bound at full worker width — RPCs per op is the lever.
    # (depth, hash) order: parents still lead their children (stratified), but
    # SIBLINGS SCATTER — depth-sort alone parks all 64 workers on the same few
    # parent dirs, and WEKA serializes creates per parent (the ~535/s "ceiling"
    # equalled the SERIAL loop's rate: single-lane-per-parent, not per-client).
    # Same trick as blocks.unpack's shuffle. Stratum-edge races hit the
    # mkparents fallback, which is exactly what it's for.
    dirs = (foot.filter(pl.col("frame") == -1)
            .with_columns(_d=pl.col("path").str.count_matches("/"),
                          _h=pl.col("path").hash(seed=7))
            .sort("_d", "_h").drop("_d", "_h"))
    # frame fibers SHUFFLED for the same reason as dirs: frames group path-sorted
    # siblings, so ordered spawn parks all workers on the same parent dirs (per-
    # parent create serialization — measured 5.7x on mkdirs). blocks.unpack has
    # shuffled frames by default since the multi-node era for exactly this.
    frames = (files.group_by("frame", maintain_order=True)
              .agg(pl.col("coff").first(), pl.col("clen").first())
              .with_columns(_h=pl.col("frame").hash(seed=11)).sort("_h").drop("_h"))
    nfr = frames.height
    nd = dirs.height
    # PRODUCER fiber per frame (decode), CONSUMER fibers per <=512-member chunk
    # (scatter + file meta) sharing the frame's RANDOM val — a byte-capped frame
    # of tiny members previously serialized THOUSANDS of scatters in one fiber
    # (the profiled 7-minute single-worker tail). FREEs ride a cleanup scope.
    K = 512
    fr = frames.with_columns(tid=(pl.int_range(nfr, dtype=pl.UInt32) + 1 + nd),
                             vid=pl.int_range(nfr, dtype=pl.Int64))
    fm = (files.join(fr.select("frame", "vid"), on="frame")
          .with_columns(ci=pl.int_range(pl.len()).over("frame") // K))
    chunks = (fm.group_by("frame", "ci", maintain_order=True).agg(pl.col("vid").first())
              .with_columns(ctid=(pl.int_range(pl.len(), dtype=pl.UInt32) + 1 + nd + nfr)))
    nch = chunks.height
    fmap = fm.join(chunks.select("frame", "ci", "ctid"), on=["frame", "ci"])              .rename({"ctid": "tid"})
    nockp = os.path.abspath(nock)
    prog = [
        dirs.with_columns(tid=(pl.int_range(nd, dtype=pl.UInt32) + 1))
            .select(tid="tid", op=pl.lit(MKDIR, pl.UInt8),
                    path=pl.lit(dest + "/") + pl.col("path")),
        fr.select(tid="tid", op=pl.lit(NEWVAL, pl.UInt8), a="vid",
                  b=pl.lit(2, pl.Int64)),
        fr.select(tid="tid", op=pl.lit(MOV, pl.UInt8),
                  k1=pl.lit(E_FS, pl.UInt8), k2=pl.lit(E_VAL, pl.UInt8),
                  a="coff", b="clen", d="vid", path=pl.lit(nockp)),
        fr.select(tid="tid", op=pl.lit(CLOSE, pl.UInt8), a="vid"),
        fmap.select(tid="tid", op=pl.lit(MOV, pl.UInt8),
                    k1=pl.lit(E_VAL, pl.UInt8), k2=pl.lit(E_FS, pl.UInt8),
                    a="vid", b="in_off", c="size", d="size",
                    path=pl.lit(dest + "/") + pl.col("path")),
        fmap.with_columns(
            c=pl.when(pl.col("uid") == os.getuid()).then(0).otherwise(pl.col("uid")),
            d=pl.when(pl.col("gid") == os.getgid()).then(0).otherwise(pl.col("gid")))
            .select(tid="tid", op=pl.lit(SETMETA, pl.UInt8),
                    a=(pl.col("mode") & 0o7777), b="mtime_ns", c="c", d="d",
                    path=pl.lit(dest + "/") + pl.col("path")),
        pl.DataFrame(dict(tid=[0, 0], op=[SPAWN, JOIN], a=[1, 1], b=[nd, nd])),
        pl.DataFrame(dict(tid=[0, 0], op=[SPAWN, JOIN], a=[nd + 1, nd + 1],
                          b=[nd + nfr + nch, nd + nfr + nch])),
    ]
    base2 = nd + nfr + nch
    # cleanup scope: one fiber batch-FREEing every frame val (local ops, instant)
    prog += [
        fr.select(tid=pl.lit(base2 + 1, pl.UInt32), op=pl.lit(FREE, pl.UInt8), a="vid"),
        pl.DataFrame(dict(tid=[0, 0], op=[SPAWN, JOIN],
                          a=[base2 + 1, base2 + 1], b=[base2 + 1, base2 + 1])),
    ]
    # dir metadata LAST, own scope
    if nd:
        dm = dirs.with_columns(tid=(pl.int_range(nd, dtype=pl.UInt32) + base2 + 2))
        prog += [
            dm.select(tid="tid", op=pl.lit(SETMETA, pl.UInt8),
                      a=(pl.col("mode") & 0o7777), b="mtime_ns",
                      path=pl.lit(dest + "/") + pl.col("path")),
            pl.DataFrame(dict(tid=[0, 0], op=[SPAWN, JOIN],
                              a=[base2 + 2, base2 + 2],
                              b=[base2 + 1 + nd, base2 + 1 + nd])),
        ]
    body_parts, tails = [], []
    for si, p_ in enumerate(prog):
        q_ = _pad(p_).with_columns(_s=pl.lit(si))
        (tails if (p_["tid"].max() is not None and p_["tid"].max() == 0 and p_["tid"].min() == 0)
         else body_parts).append(q_)
    body = pl.concat(body_parts).sort(["tid", "_s"], maintain_order=True).drop("_s")
    # each control part (SPAWN/JOIN pair) must land AFTER the bodies it covers; the
    # dir-meta pair also after the first JOIN — keep tail order as authored
    tail = pl.concat(tails).sort("_s", maintain_order=True).drop("_s")
    if phase_times is None:
        _run([body, tail])
    else:
        # PHASE ATTRIBUTION: run each scope as its own session and time it —
        # mkdir wave / frames+file-meta / dir metadata. Costs 3 process spawns
        # and 3 footer-feeds; only for profiling, results identical.
        dm_lo = nd + nfr + nch + 2                   # dirmeta fibers start here
        scopes = [("mkdir", body.filter(pl.col("op") == MKDIR), tails[0].drop("_s")),
                  ("frames", body.filter((pl.col("op") != MKDIR) &
                                         (pl.col("tid") < dm_lo)),
                   pl.concat([tails[1], tails[2]]).drop("_s")),
                  ("dirmeta", body.filter(pl.col("tid") >= dm_lo),
                   tails[3].drop("_s") if len(tails) > 3 else None)]
        for name, b_, t_ in scopes:
            if t_ is None or not b_.height:
                continue
            t0 = time.time()
            _run([b_, t_])
            phase_times[name] = round(time.time() - t0, 1)
    return files.height + dirs.height

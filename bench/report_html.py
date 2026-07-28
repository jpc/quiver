#!/usr/bin/env python3
"""Render benchmark results.json -> a self-contained HTML report.

  ./report_html.py results/iren-weka [results/local-ext4 ...] [--index results/index.html]

One page per site, plus an optional comparison index. No external assets (works offline
and under a strict CSP): inline CSS, CSS-only bars, no chart library, no webfonts.
"""
import argparse, html, json, os, sys

CSS = """
:root{--ink:#12161b;--paper:#fafbfc;--card:#fff;--muted:#5b6672;--rule:#e3e8ed;
--accent:#0d7490;--accent-soft:#0d74901a;--warn:#a8442a;--warn-soft:#a8442a14}
@media (prefers-color-scheme:dark){:root{--ink:#e7edf3;--paper:#0e1216;--card:#151b21;
--muted:#93a1b0;--rule:#232c35;--accent:#38b6d4;--accent-soft:#38b6d422;--warn:#e08b62;--warn-soft:#e08b6218}}
:root[data-theme=dark]{--ink:#e7edf3;--paper:#0e1216;--card:#151b21;--muted:#93a1b0;
--rule:#232c35;--accent:#38b6d4;--accent-soft:#38b6d422;--warn:#e08b62;--warn-soft:#e08b6218}
:root[data-theme=light]{--ink:#12161b;--paper:#fafbfc;--card:#fff;--muted:#5b6672;
--rule:#e3e8ed;--accent:#0d7490;--accent-soft:#0d74901a;--warn:#a8442a;--warn-soft:#a8442a14}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
font:15px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
-webkit-font-smoothing:antialiased}
.wrap{max-width:1080px;margin:0 auto;padding:44px 22px 80px}
code,.mono,td.n,th.n{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
font-variant-numeric:tabular-nums}
h1{font-size:30px;line-height:1.15;margin:0 0 6px;letter-spacing:-.02em;text-wrap:balance}
h2{font-size:20px;margin:44px 0 4px;letter-spacing:-.01em}
h3{font-size:15px;margin:26px 0 8px;font-weight:600}
.eyebrow{font-size:11px;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);
font-weight:600;margin:0 0 10px}
.sub{color:var(--muted);font-size:13.5px;margin:0 0 2px}
.sub code{font-size:12.5px}
.lede{color:var(--muted);max-width:66ch;margin:6px 0 0}
.card{background:var(--card);border:1px solid var(--rule);border-radius:9px;padding:18px 20px;margin:14px 0}
.verdict{border-left:3px solid var(--accent)}
.verdict ul{margin:8px 0 0;padding-left:20px}
.verdict li{margin:9px 0;max-width:78ch}
.grid{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.tbl{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th{text-align:left;font-weight:600;color:var(--muted);font-size:11px;letter-spacing:.08em;
text-transform:uppercase;padding:0 10px 7px 0;border-bottom:1px solid var(--rule);white-space:nowrap}
td{padding:6px 10px 6px 0;border-bottom:1px solid var(--rule);vertical-align:middle}
tr:last-child td{border-bottom:0}
td.n,th.n{text-align:right;white-space:nowrap}
.bar{position:relative;min-width:120px;width:100%}
.bar i{display:block;height:9px;border-radius:2px;background:var(--accent);opacity:.85}
.bar.w i{background:var(--warn)}
.badge{display:inline-block;font-size:11px;padding:1px 7px;border-radius:99px;
background:var(--accent-soft);color:var(--accent);font-weight:600;white-space:nowrap}
.badge.w{background:var(--warn-soft);color:var(--warn)}
.kv{display:flex;flex-wrap:wrap;gap:6px 26px;margin:10px 0 0;font-size:13px;color:var(--muted)}
.kv b{color:var(--ink);font-weight:600}
.note{font-size:13px;color:var(--muted);margin:10px 0 0;max-width:74ch}
.foot{margin-top:56px;padding-top:18px;border-top:1px solid var(--rule);font-size:12.5px;color:var(--muted)}
a{color:var(--accent)}
"""

def esc(x): return html.escape(str(x))

def bar_rows(d, unit="GB/s", warn_key=None, label="", fmt="{:.2f}"):
    """rows of (label, value) -> table with CSS bars scaled to the row max."""
    items = [(k, v) for k, v in d.items() if isinstance(v, (int, float))]
    if not items: return ""
    mx = max(v for _, v in items) or 1
    out = [f'<div class="tbl"><table><thead><tr><th>{esc(label)}</th>'
           f'<th class="n">{esc(unit)}</th><th style="width:52%"></th></tr></thead><tbody>']
    for k, v in items:
        w = max(1.5, v / mx * 100)
        cls = " w" if warn_key and warn_key(k, v) else ""
        out.append(f'<tr><td class="mono">{esc(k)}</td><td class="n">{fmt.format(v)}</td>'
                   f'<td><div class="bar{cls}"><i style="width:{w:.1f}%"></i></div></td></tr>')
    return "".join(out) + "</tbody></table></div>"

def site_body(res):
    env = res.get("env", {})
    fs = res.get("fsbw") or {}
    if "error" in fs: fs = {}
    P = []
    P.append(f'<p class="eyebrow">quiver benchmark</p><h1>{esc(env.get("fstype","?"))} '
             f'&middot; {esc(env.get("host","?"))}</h1>')
    P.append(f'<p class="sub">target <code>{esc(env.get("dir",""))}</code> &nbsp;·&nbsp; '
             f'{esc(env.get("cpus","?"))} CPUs, {esc(env.get("mem_gb","?"))} GB RAM &nbsp;·&nbsp; '
             f'kernel <code>{esc(env.get("kernel",""))}</code> &nbsp;·&nbsp; {esc(env.get("date",""))}</p>')

    # ---- verdict first: the actionable payload
    recs = recommendations(res)
    if recs:
        P.append('<div class="card verdict"><p class="eyebrow" style="margin-bottom:2px">'
                 'Recommended settings for this filesystem</p><ul>'
                 + "".join(f"<li>{r}</li>" for r in recs) + "</ul></div>")

    if fs:
        P.append("<h2>Write bandwidth</h2>")
        P.append('<p class="lede">All figures <code>O_DIRECT</code> — the page cache is excluded, '
                 'so these are bytes the storage actually ingested.</p>')
        P.append('<div class="grid">')
        P.append('<div class="card"><h3>Threads <span class="badge">separate files</span></h3>'
                 + bar_rows(fs.get("threads", {}), label="threads") + '</div>')
        P.append(f'<div class="card"><h3>Chunk size <span class="badge">at {esc(fs.get("best_threads","?"))} threads</span></h3>'
                 + bar_rows(fs.get("chunk_mb", {}), label="MB") + '</div>')
        P.append('</div>')
        sinks = fs.get("sinks", {})
        if sinks:
            one = sinks.get("1") or 0
            best = max(sinks, key=lambda k: sinks[k] or 0)
            gain = (sinks[best] / one) if one else 0
            P.append('<div class="card"><h3>Sink count '
                     f'<span class="badge{"" if gain>1.5 else " w"}">{gain:.1f}× from 1 → {esc(best)} sinks</span></h3>'
                     '<p class="note" style="margin-top:0">Concurrent writers into <em>one</em> file vs many. '
                     'quiver writes frames into sink files; on a parallel filesystem this is usually the '
                     'largest single win, and it is invisible unless tested explicitly.</p>'
                     + bar_rows(sinks, label="sinks", warn_key=lambda k, v: k == "1") + '</div>')
        mode = fs.get("mode", {})
        if mode:
            P.append('<div class="card"><h3>Mode</h3>'
                     + bar_rows(mode, label="mode", warn_key=lambda k, v: "one_sink" in k) + '</div>')

    rd = res.get("read") or {}
    if rd and "error" not in rd:
        P.append("<h2>Read bandwidth</h2>")
        P.append('<p class="lede">Restore / verify throughput. Page cache dropped per file, so '
                 'these are storage reads.</p><div class="grid">')
        P.append('<div class="card"><h3>Threads</h3>' + bar_rows(rd.get("threads", {}), label="threads") + '</div>')
        P.append('<div class="card"><h3>Mode <span class="badge">one file vs many</span></h3>'
                 + bar_rows(rd.get("mode", {}), label="mode",
                            warn_key=lambda k, v: "one_file" in k) + '</div></div>')

    ds = res.get("dirspread") or {}
    if ds and "error" not in ds:
        P.append("<h2>Directory fan-out</h2>")
        P.append('<p class="lede">The <em>same</em> file work spread over 1..N directories. Parallel '
                 'filesystems lock the parent directory per create/unlink, so fan-out can dominate '
                 'every other tuning knob — this is why quiver shuffles work across directories.</p>'
                 '<div class="grid">')
        for key, title, unit in (("create", "Create", "files/s"), ("unlink", "Unlink", "files/s"),
                                 ("stat", "Stat", "files/s")):
            d = ds.get(key, {})
            if d:
                P.append(f'<div class="card"><h3>{title}</h3>'
                         + bar_rows(d, unit=unit, label="dirs", fmt="{:,.0f}",
                                    warn_key=lambda k, v: k == "1") + '</div>')
        P.append('</div>')

    cdc = res.get("cdc") or {}
    if cdc and "error" not in cdc:
        P.append('<h2>Chunking &amp; delta</h2><div class="card">'
                 f'<div class="kv"><span>chunk <b>{cdc.get("chunk_gbs","?")}</b> GB/s</span>'
                 f'<span>chunk+hash <b>{cdc.get("chunk_hash_gbs","?")}</b> GB/s</span>'
                 f'<span>one 4 KB edit in a {cdc.get("file_mb","?")} MB file sends '
                 f'<b>{cdc.get("delta_send_mb","?")}</b> MB '
                 f'(<b>{cdc.get("delta_pct","?")}%</b>)</span></div></div>')

    mn = res.get("multinode") or {}
    if mn and "error" not in mn:
        P.append('<h2>Multi-node aggregate</h2><div class="card">'
                 '<p class="note" style="margin-top:0">All nodes writing simultaneously to the same '
                 'filesystem. Linear scaling means the fabric is not a shared ceiling.</p>'
                 + bar_rows(mn, label="nodes") + '</div>')

    fc = res.get("framecap") or {}
    if fc and "error" not in fc:
        levs = sorted({k for c in fc.values() for k in c.get("knee", {})}, key=int)
        wins = {}
        for c in fc.values():
            for v, d in c.get("knee", {}).items():
                if d.get("window_mb"):
                    wins[v] = d["window_mb"]
        rows = []
        for name in sorted(fc):
            kn = fc[name].get("knee", {})
            rows.append("<tr><td>" + esc(name) + "</td>" + "".join(
                f'<td class="num">{esc(kn.get(v, {}).get("knee_mb", "-"))}</td>' for v in levs)
                + "</tr>")
        rows.append('<tr><td class="note">zstd window</td>' + "".join(
            f'<td class="num note">{wins.get(v, 0):.1f} MB</td>' for v in levs) + "</tr>")
        P.append('<h2>Frame cap</h2><div class="card">'
                 '<p class="note" style="margin-top:0">quiver splits an oversized member into '
                 '<code>--frame-cap-mb</code> pieces, each compressed as its own frame. Smaller '
                 'caps mean less worker memory and more restore parallelism; bigger caps compress '
                 'better, but the return dies off once the frame outgrows zstd\'s match window — '
                 'which the LEVEL sets. Each cell is the smallest cap within the tolerance of that '
                 'corpus+level\'s asymptotic ratio.</p>'
                 '<table><thead><tr><th>corpus</th>'
                 + "".join(f'<th class="num">L{esc(v)}</th>' for v in levs)
                 + '</tr></thead><tbody>' + "".join(rows) + '</tbody></table>'
                 '<p class="note">Knee, in MB. Highly redundant corpora keep gaining past the '
                 'window (entropy tables amortize); incompressible ones never gain at all.</p>'
                 '</div>')

    cf = res.get("cfr") or {}
    if cf and "error" not in cf:
        rows = []
        for k in sorted(cf, key=int):
            v = cf[k]
            g = (v["wv"] / v["rwz"]) if v.get("wv") and v.get("rwz") else None
            rows.append(f'<tr><td>{esc(k)} MB</td>'
                        + "".join(f'<td class="num">{esc(v.get(m, "-"))}</td>'
                                  for m in ("rw", "rwz", "wv", "cfr", "cfr1"))
                        + f'<td class="num"><b>{g:.2f}x</b></td></tr>' if g else
                        f'<tr><td>{esc(k)} MB</td>'
                        + "".join(f'<td class="num">{esc(v.get(m, "-"))}</td>'
                                  for m in ("rw", "rwz", "wv", "cfr", "cfr1")) + '<td class="num">-</td></tr>')
        P.append('<h2>Stored-frame write path</h2><div class="card">'
                 '<p class="note" style="margin-top:0">Members zstd gives up on are stored '
                 'codec-free, as a zstd frame of Raw_Blocks. <code>rwz</code> is a buffered read '
                 'plus the interleave memcpy that builds the framed output; <code>wv</code> writes '
                 'the same read buffer through <code>writev</code> with the 3-byte block headers as '
                 'separate iovecs, deleting that memcpy. <code>cfr</code> would '
                 '<code>copy_file_range</code> the body — faster still, but unavailable in '
                 'practice: the whole-file digest and the CDC manifest need the bytes in user '
                 'space anyway. <code>cfr1</code> is the unframed upper bound.</p>'
                 '<table><thead><tr><th>member</th><th class="num">rw</th>'
                 '<th class="num">rwz (memcpy)</th><th class="num">wv (writev)</th>'
                 '<th class="num">cfr</th><th class="num">cfr1</th>'
                 '<th class="num">wv / rwz</th></tr></thead><tbody>'
                 + "".join(rows) + '</tbody></table>'
                 '<p class="note">GB/s, higher is better.</p></div>')

    sc = res.get("scan") or {}
    if sc and "error" not in sc:
        n = sc.pop("files", None)
        P.append(f'<h2>Directory scan</h2><div class="card">'
                 + (f'<p class="note" style="margin-top:0">{n:,} files.</p>' if n else "")
                 + bar_rows(sc, unit="entries/s", label="method", fmt="{:,.0f}") + '</div>')
        if n: sc["files"] = n

    nm = res.get("numa") or {}
    if nm and "error" not in nm:
        core = {k: v for k, v in nm.items() if isinstance(v, (int, float)) and not k.endswith("_end_to_end")}
        P.append('<h2>Compression under full load</h2><div class="card">'
                 '<p class="note" style="margin-top:0">One zstd context per allowed CPU (1:1, no '
                 'oversubscription); compress-only throughput with buffer setup excluded.</p>'
                 + bar_rows(core, label="placement", warn_key=lambda k, v: k != "none") + '</div>')

    for key, title, note in (("hashes", "Hashes", "Chunk-identity and digest candidates."),
                             ("crypto", "Crypto", "AEAD / keyed-hash candidates for encryption.")):
        d = res.get(key)
        if d and "error" not in d:
            P.append(f'<h2>{title}</h2><div class="card"><p class="note" style="margin-top:0">{note}</p>'
                     + bar_rows(d, label="algorithm") + '</div>')

    q = res.get("quiver")
    if q and "error" not in q:
        P.append('<h2>End-to-end pack</h2><div class="card">')
        P.append(f'<div class="kv"><span><b>{q.get("files",0):,}</b> files</span>'
                 f'<span><b>{q.get("store_gb","?")}</b> GB store</span>'
                 f'<span><b>{q.get("gb_per_s","?")}</b> GB/s</span>'
                 f'<span><b>{q.get("seconds","?")}</b> s</span>'
                 f'<span>{q.get("workers","?")} workers, level {q.get("level","?")}</span>'
                 f'<span>errors <b>{q.get("errors",0)}</b>, lost <b>{q.get("lost",0)}</b></span></div>')
        if q.get("hints"):
            P.append('<p class="eyebrow" style="margin:16px 0 6px">Self-reported verdict</p><ul class="note" '
                     'style="margin:0;padding-left:20px">'
                     + "".join(f"<li>{esc(h)}</li>" for h in q["hints"]) + "</ul>")
        P.append('</div>')
    return "".join(P)

def recommendations(res):
    fs = res.get("fsbw") or {}
    if "error" in fs: fs = {}
    out = []
    sinks = fs.get("sinks", {})
    if sinks:
        one = sinks.get("1") or 0
        best = max(sinks, key=lambda k: sinks[k] or 0)
        gain = (sinks[best] / one) if one else 0
        if gain >= 1.3:
            out.append(f'<code>--shards {esc(best)}</code> — <b>{sinks[best]:.2f} GB/s</b> vs '
                       f'{one:.2f} on a single sink (<b>{gain:.1f}×</b>). Single-inode write '
                       f'contention is the dominant effect on this filesystem.')
        else:
            out.append(f'<code>--shards 1</code> is fine here — splitting output across '
                       f'{esc(best)} sinks changes throughput by only {gain:.1f}×.')
    if fs.get("best_chunk_mb"):
        out.append(f'<code>--frame-mb {esc(fs["best_chunk_mb"])}</code> — knee of the chunk-size curve.')
    if fs.get("best_threads"):
        out.append(f'<code>-j {esc(fs["best_threads"])}</code> worker threads per node for write-bound work.')
    m = fs.get("mode", {})
    if m.get("one_sink_buffered") and m.get("one_sink_direct"):
        out.append(f'Buffered writes do not rescue single-inode contention '
                   f'({m["one_sink_buffered"]:.2f} vs {m["one_sink_direct"]:.2f} GB/s <code>O_DIRECT</code>).')
    ds = res.get("dirspread") or {}
    if ds and "error" not in ds:
        cr = ds.get("create", {})
        if cr and cr.get("1"):
            best = max(cr, key=lambda k: cr[k] or 0)
            g = (cr[best] / cr["1"]) if cr["1"] else 0
            if g >= 1.3:
                out.append(f'Spread creates across directories — <b>{g:.1f}×</b> from 1 → '
                           f'{esc(best)} dirs ({cr["1"]:,.0f} → {cr[best]:,.0f} files/s). '
                           f'Directory locking, not bandwidth.')
    rd = res.get("read") or {}
    m2 = rd.get("mode", {}) if "error" not in rd else {}
    if m2.get("one_file_direct") and m2.get("separate_direct"):
        r = m2["separate_direct"] / m2["one_file_direct"] if m2["one_file_direct"] else 0
        if r >= 1.3:
            out.append(f'Restores should read from a <b>sharded nockset</b>: many files '
                       f'{m2["separate_direct"]:.2f} GB/s vs one file {m2["one_file_direct"]:.2f} '
                       f'({r:.1f}×).')
    sc = res.get("scan") or {}
    if sc.get("readdir_dtype") and sc.get("readdir_lstat"):
        r = sc["readdir_dtype"] / sc["readdir_lstat"]
        if r >= 1.2:
            out.append(f'Scan: <code>d_type</code> is <b>{r:.1f}×</b> faster than <code>lstat</code> — '
                       f'enumerate-only paths (rm, delete) should never stat.')
        else:
            out.append(f'Scan: <code>d_type</code> and <code>lstat</code> are within noise here '
                       f'({r:.1f}×) — this metric is very sensitive to metadata-cache warmth, so '
                       f're-run cold before concluding anything about the scan path.')
    nm = res.get("numa") or {}
    if nm and "error" not in nm:
        none_v = nm.get("none")
        pinned = [v for k, v in nm.items()
                  if k in ("cpuonly", "local", "interleave", "remote") and isinstance(v, (int, float))]
        if none_v and pinned:
            bp = max(pinned)
            if none_v > bp * 1.15:
                out.append(f'<b>Do not pin worker threads.</b> Unpinned <b>{none_v:.2f} GB/s</b> vs '
                           f'{bp:.2f} for the best pinned placement: a pinned thread sharing a core with '
                           f'anything else (storage spin-pollers, daemons) cannot migrate, and the run '
                           f'waits for its slowest thread. Memory placement showed no consistent signal.')
    h = res.get("hashes") or {}
    if h and "error" not in h:
        f_ = max(h, key=lambda k: h[k])
        # content IDENTITY must be cryptographic: a non-crypto hash that is faster is still
        # the wrong choice, because a forgeable collision means silent dedup corruption.
        crypt = {k: v for k, v in h.items()
                 if any(t in k.upper() for t in ("BLAKE", "SHA")) and "xxh" not in k.lower()}
        if crypt:
            c_ = max(crypt, key=lambda k: crypt[k])
            note = (f'Fastest hash here is <code>{esc(f_)}</code> ({h[f_]:.2f} GB/s), but chunk '
                    f'IDENTITY must be cryptographic — use <code>{esc(c_)}</code> '
                    f'({crypt[c_]:.2f} GB/s). A forgeable collision is silent dedup corruption.'
                    ) if f_ != c_ else (f'Fastest hash here: <code>{esc(f_)}</code> at {h[f_]:.2f} GB/s '
                                        f'(cryptographic — safe for chunk identity).')
            out.append(note)
        else:
            out.append(f'Fastest hash here: <code>{esc(f_)}</code> at {h[f_]:.2f} GB/s.')
    return out

def page(title, body, extra_foot=""):
    return (f"<title>{esc(title)}</title>\n<style>{CSS}</style>\n"
            f'<div class="wrap">{body}'
            f'<div class="foot">Generated by <code>quiver/bench/report_html.py</code> from '
            f'<code>results.json</code>. Numbers are specific to this filesystem, client '
            f'configuration and node — re-run with <code>./run_bench.py --out results/&lt;site&gt; '
            f'--dir &lt;path&gt;</code> before tuning anything.{extra_foot}</div></div>')

def index_page(sites):
    P = ['<p class="eyebrow">quiver benchmark</p><h1>Filesystem comparison</h1>',
         '<p class="lede">The same suite, the same binaries, different storage. Every tuning '
         'recommendation quiver makes is derived per-filesystem — these two sites disagree on '
         'nearly all of them.</p>']
    P.append('<div class="grid">')
    for name, res in sites:
        env = res.get("env", {})
        fs = res.get("fsbw") or {}
        sinks = fs.get("sinks", {}) if "error" not in fs else {}
        one = sinks.get("1") or 0
        best = max(sinks, key=lambda k: sinks[k] or 0) if sinks else None
        gain = (sinks[best] / one) if (best and one) else 0
        peak = max((v for v in fs.get("threads", {}).values() if v), default=0)
        P.append(f'<div class="card"><h3><a href="{esc(name)}/report.html">{esc(env.get("fstype","?"))}'
                 f'</a> <span class="badge">{esc(name)}</span></h3>'
                 f'<div class="kv"><span>peak write <b>{peak:.2f}</b> GB/s</span>'
                 f'<span>sink gain <b>{gain:.1f}×</b></span>'
                 f'<span>{esc(env.get("cpus","?"))} CPUs</span></div>'
                 f'<p class="note">{esc(env.get("host",""))} · {esc(env.get("date",""))}</p></div>')
    P.append('</div>')
    # side-by-side sink curves — the headline disagreement
    P.append('<h2>Sink count: where the filesystems disagree</h2>'
             '<p class="lede">Concurrent writers into one file versus many. This single measurement '
             'changes quiver\'s recommended <code>--shards</code> from 1 to 32.</p><div class="grid">')
    for name, res in sites:
        fs = res.get("fsbw") or {}
        sinks = fs.get("sinks", {}) if "error" not in fs else {}
        if sinks:
            P.append(f'<div class="card"><h3>{esc(res.get("env",{}).get("fstype","?"))}</h3>'
                     + bar_rows(sinks, label="sinks", warn_key=lambda k, v: k == "1") + '</div>')
    P.append('</div>')
    for key, title in (("threads", "Write bandwidth vs threads"),):
        P.append(f'<h2>{title}</h2><div class="grid">')
        for name, res in sites:
            fs = res.get("fsbw") or {}
            d = fs.get(key, {}) if "error" not in fs else {}
            if d:
                P.append(f'<div class="card"><h3>{esc(res.get("env",{}).get("fstype","?"))}</h3>'
                         + bar_rows(d, label="threads") + '</div>')
        P.append('</div>')
    return "".join(P)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--index")
    a = ap.parse_args()
    sites = []
    for d in a.dirs:
        res = json.load(open(os.path.join(d, "results.json")))
        out = os.path.join(d, "report.html")
        open(out, "w").write(page(f"quiver bench · {res.get('env',{}).get('fstype','?')}", site_body(res)))
        print(f"  {out}")
        sites.append((os.path.basename(d.rstrip("/")), res))
    if a.index and len(sites) > 1:
        open(a.index, "w").write(page("quiver bench · comparison", index_page(sites)))
        print(f"  {a.index}")

if __name__ == "__main__":
    main()

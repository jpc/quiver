"""
quiver.nock.zserve — browse a real per-batch-frame (zframe) archive.

A dependency-free HTTP server (stdlib) over a zframe .tar.zstd. It reads
the nock footer once for the member index, and on demand decompresses a
member's frame and slices the member out — so it browses a 2 TB archive
without unpacking it. Run:

    python -m quiver.nock.zserve <archive.tar.zstd> [--port 8756]

then open the printed URL. Frames are LRU-cached, so paging through
members of the same batch is instant.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

import polars as pl
import zstandard as zstd

from . import zframe

_MIME = {"json": "application/json", "jsonl": "application/json",
         "txt": "text/plain", "wav": "audio/wav", "mp3": "audio/mpeg",
         "opus": "audio/ogg", "flac": "audio/flac"}


class Archive:
    def __init__(self, path: str):
        self.path = path
        idx = zframe.read_index(path)
        self.idx = idx.with_row_index("id").with_columns(
            name=pl.col("path").str.split("/").list.last(),
            ext=pl.col("path").str.extract(r"\.([^./]+)$", 1)
                .fill_null("").str.to_lowercase(),
            coll=pl.col("path").str.extract(r"^(.*)/[^/]+$", 1).fill_null(""))
        self._lock = threading.Lock()
        self._dctx = zstd.ZstdDecompressor()
        self._cache: dict[int, bytes] = {}
        self._order: list[int] = []

    def _frame(self, coff: int, clen: int) -> bytes:
        with self._lock:
            hit = self._cache.get(coff)
            if hit is not None:
                return hit
        with open(self.path, "rb") as f:      # concurrent readers, own fd
            f.seek(coff)
            comp = f.read(clen)
        raw = self._dctx.decompress(comp)
        with self._lock:
            self._cache[coff] = raw
            self._order.append(coff)
            while len(self._order) > 12:
                self._cache.pop(self._order.pop(0), None)
        return raw

    def member(self, i: int):
        r = self.idx.row(i, named=True)
        raw = self._frame(r["frame_coff"], r["frame_clen"])
        return raw[r["in_off"]: r["in_off"] + r["size"]], r

    def query(self, q, ext, coll, offset, limit):
        d = self.idx
        if q:
            d = d.filter(pl.col("name").str.contains(q, literal=True))
        if ext:
            d = d.filter(pl.col("ext") == ext)
        if coll:
            d = d.filter(pl.col("coll") == coll)
        total = d.height
        page = d.slice(offset, limit).select(
            "id", "name", "size", "ext", "coll").to_dicts()
        return total, page

    def stats(self):
        ex = (self.idx["ext"].value_counts().sort("count", descending=True)
              .head(16).to_dicts())
        co = (self.idx["coll"].value_counts().sort("count", descending=True)
              .head(60).to_dicts())
        return {"members": self.idx.height,
                "frames": int(self.idx["frame"].n_unique()),
                "bytes": int(self.idx["size"].sum()),
                "collections": int(self.idx["coll"].n_unique()),
                "exts": [{"ext": r["ext"] or "?", "n": r["count"]} for r in ex],
                "colls": [{"coll": r["coll"], "n": r["count"]} for r in co]}


def _handler(arc: Archive):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json"):
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
            elif isinstance(body, str):
                body = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            p = u.path
            try:
                if p == "/":
                    return self._send(200, PAGE, "text/html; charset=utf-8")
                if p == "/api/stats":
                    return self._send(200, arc.stats())
                if p == "/api/members":
                    qs = parse_qs(u.query)
                    total, rows = arc.query(
                        qs.get("q", [""])[0], qs.get("ext", [""])[0],
                        qs.get("coll", [""])[0],
                        int(qs.get("offset", ["0"])[0]),
                        min(int(qs.get("limit", ["200"])[0]), 500))
                    return self._send(200, {"total": total, "rows": rows})
                if p.startswith("/api/member/"):
                    i = int(p.rsplit("/", 1)[1])
                    data, r = arc.member(i)
                    ct = _MIME.get(r["ext"], "application/octet-stream")
                    return self._send(200, data, ct)
                self._send(404, {"error": "not found"})
            except Exception as e:                      # never 500 silently
                self._send(500, {"error": str(e)})
    return H


PAGE = r'''<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>quiver · archive browser</title><style>
:root{--bg:#f4f6f8;--surface:#fff;--surface2:#eef1f4;--border:#dde2e8;
--text:#191c22;--muted:#636b78;--faint:#9aa2af;--accent:#b26a1a;
--accent-soft:#f0e2cf;--text-c:#3a63b8;--audio-c:#7a52c8;--other-c:#7b8494}
@media(prefers-color-scheme:dark){:root{--bg:#111318;--surface:#1a1d24;
--surface2:#21252e;--border:#2b303a;--text:#e7e9ef;--muted:#8b93a3;
--faint:#5c6472;--accent:#e0a040;--accent-soft:#3a2f1c;--text-c:#7092e0;
--audio-c:#a684ec;--other-c:#79828f}}
*{box-sizing:border-box}html,body{margin:0}
body{background:var(--bg);color:var(--text);
font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.mono{font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
font-variant-numeric:tabular-nums}
.wrap{max-width:1240px;margin:0 auto;padding:20px}
header{display:flex;flex-wrap:wrap;align-items:baseline;gap:8px 14px;
padding-bottom:14px;border-bottom:1px solid var(--border);margin-bottom:16px}
.title{font-size:20px;font-weight:650}.title b{color:var(--accent)}
.sub{color:var(--muted);font-size:12.5px}
.path{margin-left:auto;font-size:12px;color:var(--faint)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
gap:11px;margin-bottom:16px}
.stat{background:var(--surface);border:1px solid var(--border);
border-radius:11px;padding:12px 14px}
.stat .n{font-size:22px;font-weight:640;font-variant-numeric:tabular-nums}
.stat .l{font-size:11px;color:var(--muted);text-transform:uppercase;
letter-spacing:.07em;margin-top:2px}
.grid{display:grid;grid-template-columns:minmax(0,430px) minmax(0,1fr);
gap:16px;align-items:start}@media(max-width:820px){.grid{grid-template-columns:1fr}}
.panel{background:var(--surface);border:1px solid var(--border);
border-radius:13px;overflow:hidden}
.controls{padding:12px;display:flex;flex-direction:column;gap:9px;
border-bottom:1px solid var(--border)}
input,select{width:100%;background:var(--bg);color:var(--text);
border:1px solid var(--border);border-radius:8px;padding:8px 10px;font:inherit}
input:focus,select:focus{outline:2px solid var(--accent);outline-offset:1px}
.phead{display:flex;align-items:center;gap:8px;padding:10px 13px;
border-bottom:1px solid var(--border)}
.phead .lbl{font-size:11px;text-transform:uppercase;letter-spacing:.08em;
color:var(--muted);font-weight:600}.phead .c{margin-left:auto;font-size:12px;color:var(--faint)}
.list{max-height:66vh;overflow-y:auto}
.row{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:9px;
align-items:center;padding:8px 13px;border-bottom:1px solid var(--border);cursor:pointer}
.row:hover{background:var(--surface2)}.row.sel{background:var(--accent-soft)}
.row.sel .nm{color:var(--accent)}
.tag{font-size:10px;font-weight:700;text-transform:uppercase;padding:2px 6px;
border-radius:5px;color:#fff}.nm{font-size:12.5px;overflow:hidden;
text-overflow:ellipsis;white-space:nowrap}.sz{font-size:12px;color:var(--muted)}
.more{padding:10px;text-align:center}.more button{width:auto;padding:7px 16px;
cursor:pointer;background:var(--surface2);border:1px solid var(--border);
border-radius:8px;color:var(--text)}
.pv-meta{padding:12px 15px;border-bottom:1px solid var(--border);
display:flex;flex-wrap:wrap;gap:5px 18px}.pv-meta .k{color:var(--muted);
font-size:10px;text-transform:uppercase;letter-spacing:.06em}
.pv-meta .v{font-size:12.5px}.pv-body{padding:15px}
.empty{padding:54px 20px;text-align:center;color:var(--faint)}
pre{margin:0;font-family:ui-monospace,Menlo,monospace;font-size:12.5px;
line-height:1.6;white-space:pre-wrap;word-break:break-word;max-height:60vh;overflow:auto}
.jk{color:var(--text-c)}.jn{color:var(--accent)}.jb{color:var(--audio-c)}.jp{color:var(--faint)}
audio{width:100%}.bin{color:var(--muted)}
::-webkit-scrollbar{width:10px;height:10px}::-webkit-scrollbar-thumb{background:var(--border);border-radius:6px}
</style></head><body><div class="wrap">
<header><div><div class="title"><b>quiver</b> · archive browser</div>
<div class="sub">live over the nock footer — members sliced from their zstd frame on demand</div></div>
<div class="path mono" id="apath"></div></header>
<div class="stats" id="stats"></div>
<div class="grid">
<div class="panel"><div class="controls">
<input id="q" placeholder="Search member names…">
<select id="coll"><option value="">All collections</option></select>
<select id="ext"><option value="">All types</option></select></div>
<div class="phead"><span class="lbl">Members</span><span class="c" id="mc"></span></div>
<div class="list" id="list"></div>
<div class="more" id="more" style="display:none"><button id="moreb">Load more</button></div></div>
<div class="panel"><div class="phead"><span class="lbl">Preview</span><span class="c" id="pvh">select a member</span></div>
<div id="pv"><div class="empty">Pick a member to preview its contents.</div></div></div>
</div></div><script>
const $=s=>document.querySelector(s);
const fmtB=n=>{const u=["B","KB","MB","GB","TB"];let i=0,v=n;while(v>=1024&&i<4){v/=1024;i++}
return (v>=100||!i?v.toFixed(0):v.toFixed(1))+" "+u[i]};
const tc=e=>["json","jsonl","txt"].includes(e)?"var(--text-c)":
["wav","mp3","opus","flac"].includes(e)?"var(--audio-c)":"var(--other-c)";
let offset=0,total=0,sel=null;
async function boot(){
 const s=await(await fetch("/api/stats")).json();
 $("#stats").innerHTML=[["collections",s.collections.toLocaleString()],
  ["members",s.members.toLocaleString()],["frames",s.frames.toLocaleString()],
  ["size (raw)",fmtB(s.bytes)]].map(([l,n])=>
  `<div class="stat"><div class="n">${n}</div><div class="l">${l}</div></div>`).join("");
 $("#ext").innerHTML='<option value="">All types</option>'+
  s.exts.map(e=>`<option value="${e.ext}">${e.ext||"?"} · ${e.n.toLocaleString()}</option>`).join("");
 $("#coll").innerHTML='<option value="">All collections ('+s.collections.toLocaleString()+')</option>'+
  (s.colls||[]).map(c=>{const nm=c.coll.split("/").pop()||c.coll;
   return `<option value="${c.coll}">${nm} · ${c.n.toLocaleString()}</option>`}).join("");
 load(true);
}
async function load(reset){
 if(reset){offset=0;$("#list").innerHTML=""}
 const p=new URLSearchParams({q:$("#q").value,ext:$("#ext").value,
  coll:$("#coll").value,offset,limit:200});
 const r=await(await fetch("/api/members?"+p)).json();
 total=r.total;$("#mc").textContent=total.toLocaleString()+" match";
 $("#list").insertAdjacentHTML("beforeend",r.rows.map(m=>
  `<div class="row" data-id="${m.id}"><span class="tag" style="background:${tc(m.ext)}">${m.ext||"?"}</span>
   <span class="nm mono" title="${m.coll}/${m.name}">${m.name}</span>
   <span class="sz mono">${fmtB(m.size)}</span></div>`).join(""));
 offset+=r.rows.length;
 $("#more").style.display=offset<total?"block":"none";
 $("#list").querySelectorAll(".row").forEach(el=>el.onclick=()=>pick(el));
}
async function pick(el){
 document.querySelectorAll(".row.sel").forEach(x=>x.classList.remove("sel"));
 el.classList.add("sel");const id=+el.dataset.id,nm=el.querySelector(".nm").textContent;
 const ext=el.querySelector(".tag").textContent;$("#pvh").textContent=nm;
 $("#pv").innerHTML='<div class="empty">Decompressing frame…</div>';
 const res=await fetch("/api/member/"+id);const ct=res.headers.get("Content-Type")||"";
 const meta=`<div class="pv-meta"><div><div class="k">Member</div><div class="v mono">${nm}</div></div>
  <div><div class="k">Size</div><div class="v mono">${fmtB(+res.headers.get("Content-Length"))}</div></div>
  <div><div class="k">Type</div><div class="v">${ext}</div></div></div>`;
 let body;
 if(ct.startsWith("audio/")){const b=await res.blob();
  body=`<audio controls src="${URL.createObjectURL(b)}"></audio>
   <div class="bin" style="margin-top:9px">${ext.toUpperCase()} audio — sliced from its zstd frame.</div>`;}
 else{const t=await res.text();
  try{body="<pre>"+hj(JSON.parse(t))+"</pre>"}
  catch(e){body="<pre>"+t.replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]))+"</pre>"}}
 $("#pv").innerHTML=meta+`<div class="pv-body">${body}</div>`;
}
function hj(o){return JSON.stringify(o,null,2).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]))
 .replace(/&quot;/g,'"').replace(/"(\\.|[^"\\])*"(\s*:)?/g,(m,_,c)=>c?`<span class="jk">${m}</span>`:m)
 .replace(/\b(-?\d+\.?\d*)\b/g,'<span class="jn">$1</span>')
 .replace(/\b(true|false)\b/g,'<span class="jb">$1</span>').replace(/\bnull\b/g,'<span class="jp">null</span>');}
let dt;$("#q").oninput=()=>{clearTimeout(dt);dt=setTimeout(()=>load(true),180)};
$("#ext").onchange=()=>load(true);$("#coll").onchange=()=>load(true);
$("#moreb").onclick=()=>load(false);
boot();
</script></body></html>'''


def main(argv=None):
    ap = argparse.ArgumentParser(prog="quiver-serve")
    ap.add_argument("archive")
    ap.add_argument("--port", type=int, default=8756)
    ap.add_argument("--host", default="0.0.0.0")
    a = ap.parse_args(argv)
    print(f"loading footer from {a.archive} …", flush=True)
    arc = Archive(a.archive)
    print(f"  {arc.idx.height:,} members, "
          f"{arc.idx['frame'].n_unique():,} frames", flush=True)
    # collections into the page's dropdown at load time via a tiny inline swap
    srv = ThreadingHTTPServer((a.host, a.port), _handler(arc))
    print(f"serving on http://{a.host}:{a.port}/  (Ctrl-C to stop)", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

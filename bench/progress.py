#!/usr/bin/env python3
"""Live view of a running quiver backup — a SEPARATE process that only reads the planner's
on-disk dump (`<store>.progress.json`, rewritten every 2 s temp-and-renamed). Nothing here
touches the job, so it can be started, stopped, or run from several places at once.

    ./progress.py /path/to/store.nock                 # terminal dashboard
    ./progress.py /path/to/store.nock --html ~/public/quiver-live   # + a page to serve

The page is plain HTML that re-fetches ./progress.json, so serving the directory over HTTP
is the whole deployment.
"""
import argparse, json, os, shutil, sys, time

BAR = " ▁▂▃▄▅▆▇█"


def spark(vals, n=32):
    v = [x for x in vals[-n:] if x is not None]
    if not v:
        return ""
    hi = max(v) or 1
    return "".join(BAR[min(8, int(8 * x / hi))] for x in v)


def fmt(n, unit=""):
    return f"{n:,.1f}{unit}" if isinstance(n, float) else f"{n:,}{unit}"


def render(s, hist, w=100):
    el = s.get("elapsed_s", 0)
    frames, tot = s.get("frames_dispatched", 0), s.get("frames_total", 0) or 1
    pct = 100 * frames / tot
    L = []
    L.append(f"\x1b[1m{os.path.basename(s.get('store',''))}\x1b[0m   "
             f"phase \x1b[36m{s.get('phase','?')}\x1b[0m   elapsed {el/60:.1f} min")
    L.append(f"dispatch {frames:,}/{tot:,} ({pct:5.1f}%)  "
             + "█" * int(pct / 2.5) + "·" * (40 - int(pct / 2.5)))
    ratio = s.get("ratio")
    L.append(f"read {s.get('rd_gb',0):,.0f} GB @ {s.get('rd_gbs',0):.2f} GB/s   "
             f"write {s.get('wr_gb',0):,.0f} GB @ {s.get('wr_gbs',0):.2f} GB/s   "
             f"ratio {ratio if ratio is not None else '-'}   raw {s.get('raw_gb',0):,.0f} GB")
    L.append(f"  read  {spark(hist['rd'])}")
    L.append(f"  write {spark(hist['wr'])}")
    L.append("")
    L.append(f"  {'node':<18}{'busy':>7}{'jq':>9}{'full%':>7}{'outstd':>9}"
             f"{'rd GB':>9}{'wr GB':>9}{'raw GB':>9}{'slow opens':>12}{'err':>5}")
    for nd in s.get("nodes", []):
        L.append(f"  {nd['node'][:18]:<18}{nd['busy']:>7}{nd['jq']:>9}{nd['jq_full_pct']:>7}"
                 f"{nd['outstanding']:>9,}{nd['rd_gb']:>9,.0f}{nd['wr_gb']:>9,.0f}"
                 f"{nd['raw_gb']:>9,.0f}{nd['slow_opens']:>12,}{nd['errors']:>5}")
    return "\n".join(L)


HTML = """<!doctype html><meta charset=utf-8><title>quiver progress</title>
<style>
 body{background:#0f1115;color:#d7dae0;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;
      margin:0;padding:28px 32px}
 h1{font:600 18px/1.3 ui-sans-serif,system-ui;margin:0 0 2px;color:#fff;letter-spacing:-.01em}
 .sub{color:#7b8291;margin-bottom:22px}
 .kv{display:flex;flex-wrap:wrap;gap:26px;margin:18px 0 22px}
 .kv div{min-width:110px} .kv b{display:block;font:600 21px/1.2 ui-sans-serif;color:#fff}
 .kv span{color:#7b8291;font-size:11px;text-transform:uppercase;letter-spacing:.06em}
 .bar{height:7px;background:#1c2029;border-radius:4px;overflow:hidden;margin:6px 0 2px}
 .bar i{display:block;height:100%;background:linear-gradient(90deg,#3b82f6,#22d3ee)}
 table{border-collapse:collapse;width:100%;margin-top:8px}
 th,td{text-align:right;padding:6px 10px;border-bottom:1px solid #1c2029}
 th{color:#7b8291;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
 td:first-child,th:first-child{text-align:left}
 canvas{width:100%;height:70px;display:block;margin:4px 0 18px}
 .ph{display:inline-block;padding:2px 9px;border-radius:99px;background:#1c2029;color:#22d3ee}
 .warn{color:#f59e0b} .err{color:#ef4444}
</style>
<h1>quiver backup <span class=ph id=phase>·</span></h1>
<div class=sub id=store>waiting for the planner's first dump…</div>
<div class=bar><i id=pbar style="width:0"></i></div>
<div class=sub id=pct></div>
<div class=kv id=kv></div>
<canvas id=cv></canvas>
<table><thead><tr><th>node<th>busy<th>job queue<th>full %<th>outstanding<th>read GB
<th>write GB<th>raw GB<th>slow opens<th>err</tr></thead><tbody id=rows></tbody></table>
<script>
const H={rd:[],wr:[]};
const el=id=>document.getElementById(id);
function draw(){const c=el('cv'),d=c.getContext('2d'),w=c.width=c.clientWidth*2,h=c.height=140;
 d.clearRect(0,0,w,h);const m=Math.max(0.001,...H.rd,...H.wr);
 [['rd','#3b82f6'],['wr','#22d3ee']].forEach(([k,col])=>{const a=H[k];if(a.length<2)return;
  d.beginPath();a.forEach((v,i)=>{const x=i/(a.length-1)*w,y=h-v/m*(h-8)-4;
   i?d.lineTo(x,y):d.moveTo(x,y)});d.strokeStyle=col;d.lineWidth=2;d.stroke();});}
async function tick(){try{
 const s=await(await fetch('progress.json?'+Date.now())).json();
 el('phase').textContent=s.phase; el('store').textContent=s.store;
 const p=s.frames_total?100*s.frames_dispatched/s.frames_total:0;
 el('pbar').style.width=p+'%';
 el('pct').textContent=`${s.frames_dispatched.toLocaleString()} / ${(s.frames_total||0).toLocaleString()} frames dispatched · ${(s.elapsed_s/60).toFixed(1)} min elapsed`;
 el('kv').innerHTML=[['read',s.rd_gb.toLocaleString()+' GB'],['read rate',s.rd_gbs+' GB/s'],
   ['written',s.wr_gb.toLocaleString()+' GB'],['write rate',s.wr_gbs+' GB/s'],
   ['ratio',s.ratio??'—'],['stored raw',s.raw_gb.toLocaleString()+' GB']]
   .map(([k,v])=>`<div><b>${v}</b><span>${k}</span></div>`).join('');
 H.rd.push(s.rd_gbs);H.wr.push(s.wr_gbs);if(H.rd.length>240){H.rd.shift();H.wr.shift();}
 el('rows').innerHTML=s.nodes.map(n=>`<tr><td>${n.node}</td><td>${n.busy}</td><td>${n.jq}</td>
  <td class="${n.jq_full_pct>80?'warn':''}">${n.jq_full_pct}</td>
  <td>${n.outstanding.toLocaleString()}</td><td>${n.rd_gb.toLocaleString()}</td>
  <td>${n.wr_gb.toLocaleString()}</td><td>${n.raw_gb.toLocaleString()}</td>
  <td>${n.slow_opens.toLocaleString()}</td>
  <td class="${n.errors?'err':''}">${n.errors}</td></tr>`).join('');
 draw();
}catch(e){}}
tick();setInterval(tick,2000);
</script>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("store", help="the store path; reads <store>.progress.json")
    ap.add_argument("--html", metavar="DIR", help="also publish a page + json here")
    ap.add_argument("--interval", type=float, default=1.0)
    a = ap.parse_args()
    src = a.store + ".progress.json"
    if a.html:
        os.makedirs(a.html, exist_ok=True)
        open(os.path.join(a.html, "index.html"), "w").write(HTML)
    hist = {"rd": [], "wr": []}
    last = None
    while True:
        try:
            with open(src) as f:
                s = json.load(f)
        except Exception:
            sys.stdout.write(f"\x1b[H\x1b[2Jwaiting for {src} …\n"); sys.stdout.flush()
            time.sleep(a.interval); continue
        if s != last:
            hist["rd"].append(s.get("rd_gbs", 0)); hist["wr"].append(s.get("wr_gbs", 0))
            last = s
        sys.stdout.write("\x1b[H\x1b[2J" + render(s, hist) + "\n")
        sys.stdout.flush()
        if a.html:
            tmp = os.path.join(a.html, ".progress.json")
            shutil.copyfile(src, tmp); os.replace(tmp, os.path.join(a.html, "progress.json"))
        if s.get("phase") == "done":
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()

"""
visualisation.py — Wikipedia graph brain visualizer
Reads graph.db, outputs a standalone brain.html.

Usage:
    python visualisation.py                     # 2000 nodes, EN, → brain.html
    python visualisation.py --nodes 5000        # bigger graph (slower layout)
    python visualisation.py --lang de           # German Wikipedia
    python visualisation.py --out my.html       # custom output file
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

DEFAULT_DB    = Path("crawl_output/graph.db")
DEFAULT_OUT   = Path("brain.html")
DEFAULT_LANG  = "en"
DEFAULT_NODES = 2000
DEFAULT_EDGES = 50_000


# ── data extraction ───────────────────────────────────────────────────────────

def build_graph(db: Path, lang: str, max_nodes: int, max_edges: int):
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.execute("PRAGMA cache_size=-65536")

    t = time.time()
    print(f"[vis] querying top {max_nodes} articles by out-degree ({lang}) …", flush=True)

    rows = conn.execute("""
        SELECT from_title, COUNT(*) AS deg
        FROM edges WHERE from_lang = ?
        GROUP BY from_title
        ORDER BY deg DESC
        LIMIT ?
    """, (lang, max_nodes)).fetchall()

    if not rows:
        print("[vis] No data found. Is graph.db populated?")
        sys.exit(1)

    title_idx = {r[0]: i for i, r in enumerate(rows)}
    top_set   = set(title_idx.keys())

    print(f"[vis] {len(rows)} nodes  ({time.time()-t:.1f}s)  —  scanning edges …", flush=True)
    t = time.time()

    # For each top node, fetch its outgoing edges and keep only those
    # that point to another top node. Uses idx_from — no full table scan.
    edges_out: list = []
    in_deg:    dict = {}

    for i, (title, _) in enumerate(rows):
        if i % 500 == 0 and i > 0:
            print(f"[vis]   {i}/{len(rows)} nodes, {len(edges_out)} edges …", flush=True)
        if len(edges_out) >= max_edges:
            break

        neighbors = conn.execute(
            "SELECT to_title FROM edges WHERE from_lang=? AND from_title=? AND to_lang=?",
            (lang, title, lang),
        ).fetchall()

        fi = title_idx[title]
        for (to_title,) in neighbors:
            ti = title_idx.get(to_title)
            if ti is not None:
                edges_out.append({"s": fi, "t": ti})
                in_deg[ti] = in_deg.get(ti, 0) + 1

    conn.close()

    nodes_out = [
        {"id": i, "title": r[0], "out": r[1], "in": in_deg.get(i, 0)}
        for i, r in enumerate(rows)
    ]

    print(f"[vis] {len(edges_out)} edges  ({time.time()-t:.1f}s)", flush=True)
    return nodes_out, edges_out


# ── HTML template ─────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wikipedia Brain</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#03030a;overflow:hidden;font-family:'Courier New',monospace;color:#aac}
canvas{display:block;cursor:crosshair}
#bar{
  position:fixed;top:0;left:0;right:0;padding:10px 18px;
  display:flex;align-items:center;gap:14px;
  background:linear-gradient(180deg,rgba(3,3,10,.96) 60%,transparent);
  z-index:10;pointer-events:none
}
#bar>*{pointer-events:auto}
#ttl{font-size:16px;font-weight:bold;letter-spacing:4px;color:#79c;
     text-shadow:0 0 14px #46a,0 0 30px #236}
#search{
  background:rgba(255,255,255,.06);border:1px solid rgba(70,130,255,.3);
  color:#cdf;padding:4px 14px;border-radius:20px;outline:none;
  font-family:inherit;font-size:13px;width:220px;transition:border .2s
}
#search:focus{border-color:rgba(70,130,255,.7)}
#search::placeholder{color:rgba(110,160,255,.3)}
#stats{color:rgba(100,150,255,.45);font-size:11px;flex:1}
#hint{
  position:fixed;bottom:12px;left:50%;transform:translateX(-50%);
  color:rgba(80,120,180,.35);font-size:11px;pointer-events:none;letter-spacing:1px
}
#tip{
  position:fixed;pointer-events:none;display:none;
  background:rgba(3,6,18,.94);border:1px solid rgba(60,120,255,.3);
  color:#bdf;padding:9px 15px;border-radius:9px;font-size:12px;
  max-width:320px;z-index:20;line-height:1.7;
  box-shadow:0 0 28px rgba(50,100,255,.2)
}
#tip b{color:#9cf;font-size:13px}
</style>
</head>
<body>
<canvas id="c"></canvas>
<div id="bar">
  <span id="ttl">WIKIPEDIA BRAIN</span>
  <input id="search" type="text" placeholder="Search article …" autocomplete="off" spellcheck="false">
  <span id="stats">loading …</span>
</div>
<div id="tip"></div>
<div id="hint">scroll to zoom &nbsp;·&nbsp; drag to pan &nbsp;·&nbsp; hover = connections &nbsp;·&nbsp; click = open Wikipedia</div>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const NODES=__NODES__;
const LINKS=__LINKS__;

// ── canvas ────────────────────────────────────────────────────────────────────
const canvas=document.getElementById('c');
const ctx=canvas.getContext('2d');
let W,H;
const resize=()=>{W=canvas.width=window.innerWidth;H=canvas.height=window.innerHeight};
window.addEventListener('resize',()=>{resize();draw()});
resize();

// ── helpers ────────────────────────────────────────────────────────────────────
const maxDeg=Math.max(1,...NODES.map(n=>(n.out||0)+(n.in||0)));
const nodeR =n=>2.5+Math.sqrt(((n.out||0)+(n.in||0))/maxDeg)*13;
const nodeC =n=>{
  const t=((n.out||0)+(n.in||0))/maxDeg;
  if(t<0.3){const s=t/0.3;return`rgb(${Math.round(20+s*60)},${Math.round(60+s*140)},${Math.round(180+s*75)})`}
  if(t<0.7){const s=(t-0.3)/0.4;return`rgb(${Math.round(80+s*150)},${Math.round(200+s*40)},${Math.round(255-s*60)})`}
  const s=(t-0.7)/0.3;return`rgb(${Math.round(230+s*25)},${Math.round(240-s*30)},${Math.round(195-s*100)})`;
};

// ── simulation ────────────────────────────────────────────────────────────────
const sim=d3.forceSimulation(NODES)
  .force('link',d3.forceLink(LINKS).id(d=>d.id).distance(30).strength(0.2))
  .force('charge',d3.forceManyBody().strength(-100).distanceMax(350))
  .force('center',d3.forceCenter(0,0).strength(0.05))
  .force('collide',d3.forceCollide(d=>nodeR(d)+1.5).strength(0.5))
  .alphaDecay(0.007).velocityDecay(0.42);

// ── view state ────────────────────────────────────────────────────────────────
let tx=0,ty=0,sc=1;
let hlSet=null,hlNode=null;
let panBase=null,dragN=null;

// ── draw ──────────────────────────────────────────────────────────────────────
function draw(){
  ctx.clearRect(0,0,W,H);
  ctx.save();
  ctx.translate(W/2+tx,H/2+ty);
  ctx.scale(sc,sc);

  // edges
  for(const l of LINKS){
    if(!l.source.x)continue;
    const hi=hlSet&&hlSet.has(l.source.id)&&hlSet.has(l.target.id);
    ctx.globalAlpha=hi?0.8:(hlSet?0.015:0.08);
    ctx.strokeStyle=hi?'#4af':'#1a3680';
    ctx.lineWidth=(hi?1:0.3)/Math.max(0.5,sc);
    ctx.beginPath();ctx.moveTo(l.source.x,l.source.y);ctx.lineTo(l.target.x,l.target.y);ctx.stroke();
  }

  // nodes
  for(const n of NODES){
    if(!n.x)continue;
    const r=nodeR(n),col=nodeC(n);
    const hi=hlSet?hlSet.has(n.id):true;
    const hover=hlNode&&hlNode.id===n.id;
    ctx.globalAlpha=hi?1:0.05;
    ctx.shadowBlur=hover?26:(r>8?10:4);
    ctx.shadowColor=col;
    ctx.fillStyle=col;
    ctx.beginPath();ctx.arc(n.x,n.y,hover?r*1.6:r,0,6.283);ctx.fill();
    if(sc>2.2||hover){
      ctx.shadowBlur=0;
      ctx.globalAlpha=(hover?1:Math.min(1,(sc-2.2)*1.5))*(hi?1:0.05);
      ctx.fillStyle=hover?'#fff':'rgba(170,205,255,0.8)';
      ctx.font=`${hover?12:8}px monospace`;
      ctx.textAlign='center';
      ctx.fillText(n.title.substring(0,32),n.x,n.y-r-3);
    }
  }
  ctx.shadowBlur=0;ctx.globalAlpha=1;
  ctx.restore();
}

sim.on('tick',draw);
sim.on('end',()=>{document.getElementById('stats').textContent=
  `${NODES.length.toLocaleString()} articles · ${LINKS.length.toLocaleString()} connections · layout done`;
});
document.getElementById('stats').textContent=
  `${NODES.length.toLocaleString()} articles · ${LINKS.length.toLocaleString()} connections · simulating …`;

// ── interaction ───────────────────────────────────────────────────────────────
const toW=(px,py)=>[(px-W/2-tx)/sc,(py-H/2-ty)/sc];
const findN=(wx,wy)=>{
  let best=null,bd=Infinity;
  for(const n of NODES){
    if(!n.x)continue;
    const r=nodeR(n)*3,dx=n.x-wx,dy=n.y-wy,d=dx*dx+dy*dy;
    if(d<r*r&&d<bd){best=n;bd=d;}
  }
  return best;
};
const buildHL=n=>{
  const s=new Set([n.id]);
  for(const l of LINKS){
    if(l.source.id===n.id)s.add(l.target.id);
    if(l.target.id===n.id)s.add(l.source.id);
  }
  return s;
};

canvas.addEventListener('mousedown',e=>{
  const[wx,wy]=toW(e.clientX,e.clientY);
  const n=findN(wx,wy);
  if(n){dragN=n;n.fx=n.x;n.fy=n.y;sim.alphaTarget(0.05).restart();}
  else panBase={x:e.clientX-tx,y:e.clientY-ty};
});
canvas.addEventListener('mousemove',e=>{
  if(panBase){tx=e.clientX-panBase.x;ty=e.clientY-panBase.y;draw();return;}
  const[wx,wy]=toW(e.clientX,e.clientY);
  if(dragN){dragN.fx=wx;dragN.fy=wy;return;}
  const n=findN(wx,wy);
  const tip=document.getElementById('tip');
  if(n!==hlNode){
    hlNode=n;hlSet=n?buildHL(n):null;
    if(n){
      tip.style.display='block';
      const totalDeg=(n.out||0)+(n.in||0);
      const pct=((totalDeg/maxDeg)*100).toFixed(1);
      tip.innerHTML=`<b>${n.title}</b><br>`+
        `↗ <span style="color:#7df">${(n.out||0).toLocaleString()}</span> outlinks &nbsp;·&nbsp; `+
        `↙ <span style="color:#fa7">${(n.in||0).toLocaleString()}</span> inlinks<br>`+
        `<span style="color:#567;font-size:10px">top ${pct}% by connectivity</span>`;
    }else{tip.style.display='none';}
    draw();
  }
  if(n){tip.style.left=(e.clientX+14)+'px';tip.style.top=(e.clientY-8)+'px';}
});
canvas.addEventListener('mouseup',()=>{
  if(dragN){dragN.fx=null;dragN.fy=null;dragN=null;sim.alphaTarget(0);}
  panBase=null;
});
canvas.addEventListener('click',e=>{
  const[wx,wy]=toW(e.clientX,e.clientY);
  const n=findN(wx,wy);
  if(n)window.open(`https://en.wikipedia.org/wiki/${encodeURIComponent(n.title.replace(/ /g,'_'))}`,'_blank');
});
canvas.addEventListener('wheel',e=>{
  e.preventDefault();
  const f=e.deltaY<0?1.15:1/1.15;
  const cx=e.clientX-W/2,cy=e.clientY-H/2;
  tx=cx-(cx-tx)*f;ty=cy-(cy-ty)*f;
  sc=Math.max(0.04,Math.min(30,sc*f));
  draw();
},{passive:false});

document.getElementById('search').addEventListener('input',function(){
  const q=this.value.toLowerCase().trim();
  if(!q){hlSet=null;hlNode=null;draw();return;}
  const ms=NODES.filter(n=>n.title.toLowerCase().includes(q));
  if(!ms.length){hlSet=null;draw();return;}
  const s=new Set();
  ms.forEach(m=>{buildHL(m).forEach(id=>s.add(id));});
  hlSet=s;
  const f=ms[0];if(f.x){tx=-f.x*sc;ty=-f.y*sc;}
  draw();
});
</script>
</body>
</html>"""


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Wikipedia brain visualizer — reads graph.db")
    ap.add_argument("--db",    default=str(DEFAULT_DB))
    ap.add_argument("--lang",  default=DEFAULT_LANG,     help="en or de")
    ap.add_argument("--nodes", default=DEFAULT_NODES, type=int, help="nodes to show")
    ap.add_argument("--edges", default=DEFAULT_EDGES, type=int, help="max edges")
    ap.add_argument("--out",   default=str(DEFAULT_OUT), help="output HTML file")
    args = ap.parse_args()

    db  = Path(args.db)
    out = Path(args.out)

    if not db.exists():
        print(f"[vis] DB not found: {db}  (run fast_dump.bat first)")
        sys.exit(1)

    nodes, edges = build_graph(db, args.lang, args.nodes, args.edges)

    html = _HTML.replace("__NODES__", json.dumps(nodes, separators=(',', ':')))
    html = html.replace("__LINKS__", json.dumps(edges, separators=(',', ':')))
    out.write_text(html, encoding="utf-8")

    size_kb = out.stat().st_size // 1024
    print(f"[vis] Saved → {out}  ({size_kb} KB,  {len(nodes):,} nodes,  {len(edges):,} edges)")
    print(f"[vis] Open in browser:  {out.resolve()}")


if __name__ == "__main__":
    main()

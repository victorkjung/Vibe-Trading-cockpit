#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kalshi BTC monitoring cockpit — self-contained web service.

Serves a live dashboard (embedded HTML, no build step) plus an SSE stream that
relays the polling monitor to the browser. Monitoring only — there is no order
path anywhere in this service.

Deps:  pip install fastapi uvicorn requests
Run:   python serve.py --series KXBTC15M --host 0.0.0.0 --port 8787
View:  http://<vps-tailscale-ip>:8787   (e.g. http://100.87.250.108:8787)

Endpoints:
    GET /              embedded dashboard
    GET /api/snapshot  one-shot JSON snapshot (with trades)
    GET /api/stream    text/event-stream of monitor ticks (TICK + events)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import monitor  # noqa: E402
import snapshot  # noqa: E402

SERIES = os.environ.get("KALSHI_BTC_SERIES", "KXBTC15M")
INTERVAL = float(os.environ.get("KALSHI_MONITOR_INTERVAL", "2.0"))

app = FastAPI(title="Kalshi BTC Monitor", docs_url=None, redoc_url=None)


@app.get("/api/snapshot")
def api_snapshot() -> JSONResponse:
    return JSONResponse(snapshot.build_snapshot(SERIES, with_trades=True))


@app.get("/api/stream")
def api_stream() -> StreamingResponse:
    """SSE stream: one `data:` frame per poll (sync generator → threadpool)."""
    def gen() -> Iterator[str]:
        state: dict = {}
        while True:
            try:
                snap = snapshot.build_snapshot(SERIES, with_trades=True)
                events = monitor.diff_tick(snap, state)
                frame = {"type": "TICK", "ts": snap["generated_at"],
                         "spot": snap["btc_spot"], "series": SERIES,
                         "markets": snap["markets"], "events": events}
            except Exception as exc:  # noqa: BLE001 - keep the stream alive
                frame = {"type": "ERROR", "error": str(exc)}
            yield f"data: {json.dumps(frame)}\n\n"
            time.sleep(INTERVAL)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _HTML.replace("__SERIES__", SERIES).replace("__INTERVAL__", str(INTERVAL))


_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Kalshi BTC Monitor · __SERIES__</title>
<style>
  :root{--bg:#0b0e14;--panel:#141925;--line:#222a3a;--fg:#d6deeb;--mut:#7a8aa8;
        --grn:#27d796;--red:#ff6b6b;--amb:#ffb454;--blu:#5aa9ff}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);
    font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}
  header{display:flex;gap:18px;align-items:baseline;padding:12px 18px;
    border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--bg)}
  header h1{font-size:15px;margin:0;letter-spacing:.5px}
  header .spot{color:var(--grn);font-size:18px;font-weight:600}
  header .dot{width:8px;height:8px;border-radius:50%;background:var(--red);display:inline-block}
  .wrap{display:grid;grid-template-columns:2fr 1fr;gap:14px;padding:14px 18px}
  .tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;align-content:start}
  .tile{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
  .tile .tk{font-size:12px;color:var(--mut);word-break:break-all}
  .tile .row{display:flex;justify-content:space-between;margin-top:8px}
  .tile .big{font-size:26px;font-weight:700}
  .badge{font-size:10px;padding:2px 7px;border-radius:20px;text-transform:uppercase;letter-spacing:.4px}
  .ok{background:rgba(39,215,150,.15);color:var(--grn)}
  .low{background:rgba(255,180,84,.15);color:var(--amb)}
  .suspect{background:rgba(255,107,107,.15);color:var(--red)}
  .kv{color:var(--mut)} .pos{color:var(--grn)} .neg{color:var(--red)}
  .cd{font-variant-numeric:tabular-nums}
  .feed{background:var(--panel);border:1px solid var(--line);border-radius:10px;
    padding:10px 12px;max-height:80vh;overflow:auto}
  .feed h2{font-size:12px;color:var(--mut);margin:0 0 8px;text-transform:uppercase;letter-spacing:.5px}
  .ev{padding:5px 0;border-bottom:1px solid var(--line);font-size:12px}
  .ev .t{color:var(--mut)} .ev .WINDOW_OPEN{color:var(--blu)}
  .ev .EDGE_SUSPECT{color:var(--red)} .ev .TAPE_IMBALANCE{color:var(--amb)}
  .ev .LARGE_PRINT{color:var(--fg)} .ev .EXPIRING{color:var(--amb)} .ev .IMPLIED_MOVE{color:var(--blu)}
  .warn{color:var(--amb);font-size:11px;margin-top:6px}
  .muted{color:var(--mut)}
</style></head><body>
<header>
  <span class="dot"></span><h1>KALSHI BTC · __SERIES__</h1>
  <span class="spot" id="spot">—</span>
  <span class="kv" id="meta">connecting…</span>
</header>
<div class="wrap">
  <div class="tiles" id="tiles"></div>
  <div class="feed"><h2>Anomaly Feed</h2><div id="feed"></div></div>
</div>
<script>
const fmtPct = x => x==null ? "—" : (x*100).toFixed(0)+"%";
const closeAt = {}; // ticker -> epoch ms of close, for local countdown
function badge(r){return '<span class="badge '+(r||'')+'">'+(r||'')+'</span>';}
function tile(m){
  const edge = m.edge==null?'—':((m.edge>0?'+':'')+(m.edge*100).toFixed(0)+'%');
  const ec = m.edge==null?'':(m.edge>0?'pos':'neg');
  return `<div class="tile" id="t_${m.ticker}">
    <div class="tk">${m.ticker}</div>
    <div class="row"><span class="kv">strike</span><span>$${(m.floor_strike||0).toLocaleString()}</span></div>
    <div class="row"><span class="big">${fmtPct(m.implied_prob)}</span>
      <span class="muted">model ${fmtPct(m.model_prob)}</span></div>
    <div class="row"><span class="kv">edge</span><span class="${ec}">${edge} ${badge(m.edge_reliability)}</span></div>
    <div class="row"><span class="kv">closes in</span><span class="cd" id="cd_${m.ticker}">—</span></div>
    <div class="row"><span class="kv">OI / vol</span><span class="muted">${Math.round(m.open_interest||0).toLocaleString()} / ${Math.round(m.volume||0).toLocaleString()}</span></div>
    ${m.warning?`<div class="warn">⚠ ${m.warning}</div>`:''}
  </div>`;
}
function render(d){
  document.getElementById('spot').textContent = '$'+Math.round(d.spot).toLocaleString();
  document.getElementById('meta').textContent = d.series+' · '+(d.markets.length)+' live · '+new Date(d.ts).toLocaleTimeString();
  const now = Date.now();
  d.markets.forEach(m=>{ if(m.minutes_to_close!=null) closeAt[m.ticker]=now+m.minutes_to_close*60000; });
  document.getElementById('tiles').innerHTML = d.markets.map(tile).join('');
  const feed = document.getElementById('feed');
  (d.events||[]).forEach(e=>{
    const div=document.createElement('div'); div.className='ev';
    const rest=Object.entries(e).filter(([k])=>k!=='type'&&k!=='ticker').map(([k,v])=>k+'='+v).join(' ');
    div.innerHTML=`<span class="t">${new Date(d.ts).toLocaleTimeString()}</span> `
      +`<span class="${e.type}">${e.type}</span> <span class="muted">${e.ticker||''}</span> ${rest}`;
    feed.prepend(div);
  });
  while(feed.childNodes.length>200) feed.removeChild(feed.lastChild);
}
setInterval(()=>{ const now=Date.now();
  Object.entries(closeAt).forEach(([tk,t])=>{ const el=document.getElementById('cd_'+tk);
    if(!el) return; let s=Math.max(0,Math.round((t-now)/1000));
    el.textContent=Math.floor(s/60)+':'+String(s%60).padStart(2,'0');
    el.style.color = s<120 ? 'var(--amb)' : 'var(--fg)'; });
},1000);
function connect(){ const es=new EventSource('/api/stream');
  es.onmessage=ev=>{ try{const d=JSON.parse(ev.data); if(d.type==='TICK') render(d);}catch(e){} };
  es.onerror=()=>{ es.close(); document.getElementById('meta').textContent='reconnecting…'; setTimeout(connect,2000); };
}
connect();
</script></body></html>"""


def main() -> int:
    p = argparse.ArgumentParser(description="Kalshi BTC monitoring cockpit (web)")
    p.add_argument("--series", default=SERIES)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.environ.get("KALSHI_MONITOR_PORT", "8787")))
    args = p.parse_args()
    globals()["SERIES"] = args.series
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

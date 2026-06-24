#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kalshi BTC monitoring cockpit — self-contained web service.

Serves a live dashboard (embedded HTML, no build step) plus an SSE stream that
relays a single shared background poller to all browsers. Monitoring only —
there is no order path anywhere in this service (execution stays on Kalshi).

A background thread polls the snapshot, runs anomaly detection, and maintains a
**window ledger**: when a 15-min window closes it records Kalshi's authoritative
settlement (result) against what the market and our model predicted — the basis
for next-wager analysis. The dashboard auto-rolls to the next live window.

Deps:  pip install fastapi uvicorn requests
Run:   python serve.py --series KXBTC15M --host 0.0.0.0 --port 8787
View:  http://<vps-tailscale-ip>:8787

Endpoints:
    GET /              embedded dashboard
    GET /api/snapshot  one-shot JSON snapshot (with trades)
    GET /api/history   ~60 min of 1m closes to seed the chart
    GET /api/stream    text/event-stream relaying the shared poller
    GET /api/results   recent settled-window ledger (JSON)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import monitor  # noqa: E402
import snapshot  # noqa: E402

SERIES = os.environ.get("KALSHI_BTC_SERIES", "KXBTC15M")
INTERVAL = float(os.environ.get("KALSHI_MONITOR_INTERVAL", "2.0"))
RESULTS_FILE = Path(os.environ.get(
    "KALSHI_RESULTS_FILE", str(Path.home() / ".kalshi_cockpit_results.jsonl")))

app = FastAPI(title="Kalshi BTC Monitor", docs_url=None, redoc_url=None)

# --------------------------------------------------------------------------- #
# Shared state, written only by the single poller thread.
# --------------------------------------------------------------------------- #
_LOCK = threading.Lock()
_LATEST: Optional[dict] = None
_SEQ = 0
_LEDGER: deque = deque(maxlen=200)   # settled windows, newest first


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_ledger() -> None:
    """Restore recent ledger entries from disk so results survive restarts."""
    if not RESULTS_FILE.exists():
        return
    try:
        lines = RESULTS_FILE.read_text(encoding="utf-8").splitlines()[-200:]
        for ln in lines:
            try:
                _LEDGER.appendleft(json.loads(ln))
            except Exception:
                continue
    except Exception:
        pass


def _finalize(ticker: str, result: str, a: dict) -> dict:
    """Build a ledger entry comparing predictions to the settled result."""
    model = a.get("model")
    implied = a.get("implied")
    model_call = ("yes" if model >= 0.5 else "no") if model is not None else None
    market_call = ("yes" if implied >= 0.5 else "no") if implied is not None else None
    entry = {
        "ticker": ticker, "strike": a.get("strike"), "result": result,
        "settle_spot": a.get("last_spot"), "implied": implied, "model": model,
        "edge": a.get("edge"), "reliability": a.get("reliability"),
        "model_call": model_call, "market_call": market_call,
        "model_correct": (model_call == result) if model_call else None,
        "market_correct": (market_call == result) if market_call else None,
        "settled_at": _now(),
    }
    with _LOCK:
        _LEDGER.appendleft(entry)
    try:
        with RESULTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception:
        pass
    return entry


def _poller() -> None:
    """Single background loop: poll, detect anomalies, settle closed windows."""
    global _LATEST, _SEQ
    state: dict = {}            # for monitor.diff_tick
    tracked: dict = {}         # ticker -> last analytics while live
    prev_live: set = set()     # tickers live on the previous tick
    pending: dict = {}         # ticker -> attempts (closed, awaiting result)
    settled_ids: set = set()

    while True:
        try:
            snap = snapshot.build_snapshot(SERIES, with_trades=True)
            events = monitor.diff_tick(snap, state)
            live = set()
            for m in snap["markets"]:
                tk = m["ticker"]
                live.add(tk)
                tracked[tk] = {
                    "strike": m.get("floor_strike"), "implied": m.get("implied_prob"),
                    "model": m.get("model_prob"), "edge": m.get("edge"),
                    "reliability": m.get("edge_reliability"), "last_spot": snap["btc_spot"],
                }
            # Windows that just left the live set → await settlement.
            for tk in prev_live - live:
                if tk not in settled_ids and tk not in pending:
                    pending[tk] = 0
            # Try to resolve pending windows via the authoritative result.
            for tk in list(pending):
                pending[tk] += 1
                mk = snapshot.fetch_market(tk)
                result = (mk.get("result") or "").lower()
                if result in ("yes", "no"):
                    entry = _finalize(tk, result, tracked.get(tk, {}))
                    events.append({"type": "WINDOW_SETTLED", "ticker": tk,
                                   "result": result,
                                   "model_correct": entry["model_correct"],
                                   "market_correct": entry["market_correct"]})
                    settled_ids.add(tk)
                    del pending[tk]
                elif pending[tk] > 45:   # ~90s: give up logging this one
                    del pending[tk]
            prev_live = live

            with _LOCK:
                _SEQ += 1
                _LATEST = {
                    "type": "TICK", "seq": _SEQ, "ts": snap["generated_at"],
                    "spot": snap["btc_spot"], "series": SERIES,
                    "vol": snap.get("realized_vol_annual"),
                    "live_count": snap.get("live_market_count", 0),
                    "markets": snap["markets"], "events": events,
                    "ledger": list(_LEDGER)[:10],
                }
        except Exception as exc:  # noqa: BLE001 - never kill the poller
            with _LOCK:
                _SEQ += 1
                _LATEST = {"type": "ERROR", "seq": _SEQ, "error": str(exc)}
        time.sleep(INTERVAL)


@app.get("/api/snapshot")
def api_snapshot() -> JSONResponse:
    return JSONResponse(snapshot.build_snapshot(SERIES, with_trades=True))


@app.get("/api/history")
def api_history() -> JSONResponse:
    return JSONResponse({"points": snapshot.fetch_price_history(60)})


@app.get("/api/results")
def api_results() -> JSONResponse:
    with _LOCK:
        return JSONResponse({"results": list(_LEDGER)[:50]})


@app.get("/api/stream")
def api_stream() -> StreamingResponse:
    """Relay the shared poller's latest frame to this client (dedup by seq)."""
    def gen() -> Iterator[str]:
        last = -1
        while True:
            with _LOCK:
                frame = dict(_LATEST) if _LATEST else None
            if frame and frame.get("seq") != last:
                last = frame.get("seq")
                yield f"data: {json.dumps(frame)}\n\n"
            time.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _HTML.replace("__SERIES__", SERIES)


_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Kalshi BTC Monitor · __SERIES__</title>
<style>
  :root{--bg:#0a0a0c;--panel:#141519;--line:#23252d;--fg:#ededf2;--mut:#8a8f99;
        --grn:#00d182;--red:#ff5a5f;--amb:#ffb454;--orange:#ff8a3d;--blu:#5aa9ff}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);
    font:14px/1.45 ui-sans-serif,-apple-system,Segoe UI,Roboto,sans-serif}
  .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  .top{display:flex;align-items:center;gap:14px;padding:14px 22px;border-bottom:1px solid var(--line)}
  .btc{width:38px;height:38px;border-radius:9px;background:var(--orange);display:grid;place-items:center;
    font-weight:800;color:#1a1205;font-size:20px}
  .top h1{font-size:18px;margin:0;font-weight:700}
  .top .sub{color:var(--mut);font-size:12px;margin-top:2px}
  .live{display:flex;align-items:center;gap:6px;color:var(--red);font-weight:700;font-size:12px;letter-spacing:.5px}
  .live .dot{width:8px;height:8px;border-radius:50%;background:var(--red);animation:pulse 1.4s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .grow{flex:1}
  .stats{display:flex;gap:0;border-bottom:1px solid var(--line)}
  .stat{padding:16px 22px;border-right:1px solid var(--line);min-width:180px}
  .stat .lbl{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
  .stat .val{font-size:30px;font-weight:700;margin-top:4px;font-variant-numeric:tabular-nums}
  .stat .chg{font-size:13px;margin-top:2px;font-variant-numeric:tabular-nums}
  .pos{color:var(--grn)} .neg{color:var(--red)} .muted{color:var(--mut)}
  .cd{color:var(--amb)}
  .wrap{display:grid;grid-template-columns:1fr 360px;gap:0}
  .left{border-right:1px solid var(--line)}
  .chartbox{padding:14px 18px 4px} #chart{width:100%;height:330px;display:block}
  .settling{padding:8px 22px;color:var(--amb);font-size:13px}
  .chartleg{display:flex;gap:18px;padding:0 22px 10px;color:var(--mut);font-size:12px}
  .ud{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:6px 18px 16px}
  .udcard{border:1px solid var(--line);border-radius:12px;padding:14px 16px;text-align:center}
  .udcard.up{border-color:rgba(0,209,130,.4)} .udcard.down{border-color:rgba(255,90,95,.4)}
  .udcard .t{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px}
  .udcard .p{font-size:30px;font-weight:800;margin-top:6px}
  .up .p{color:var(--grn)} .down .p{color:var(--red)}
  .analytics{display:flex;flex-wrap:wrap;gap:10px;padding:0 18px 18px}
  .chip{border:1px solid var(--line);border-radius:10px;padding:8px 12px;min-width:104px}
  .chip .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  .chip .v{font-size:18px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums}
  .badge{font-size:10px;padding:2px 8px;border-radius:20px;text-transform:uppercase;letter-spacing:.4px;font-weight:700}
  .ok{background:rgba(0,209,130,.15);color:var(--grn)}
  .low{background:rgba(255,180,84,.15);color:var(--amb)}
  .suspect{background:rgba(255,90,95,.15);color:var(--red)}
  .warn{color:var(--amb);font-size:12px;padding:0 18px 16px}
  .rail{display:flex;flex-direction:column;max-height:calc(100vh - 64px - 92px)}
  .results{padding:14px 16px;border-bottom:1px solid var(--line);max-height:34%;overflow:auto}
  .results h2,.feed h2{font-size:11px;color:var(--mut);margin:0 0 10px;text-transform:uppercase;letter-spacing:.7px}
  .res{display:flex;justify-content:space-between;gap:8px;padding:6px 0;border-bottom:1px solid var(--line);font-size:12px}
  .res .yes{color:var(--grn);font-weight:700}.res .no{color:var(--red);font-weight:700}
  .tick{color:var(--grn)}.cross{color:var(--red)}
  .scorebar{display:flex;gap:14px;font-size:12px;color:var(--mut);margin-bottom:8px}
  .feed{padding:14px 16px;overflow:auto;flex:1}
  .ev{padding:7px 0;border-bottom:1px solid var(--line);font-size:12px}
  .ev .t{color:var(--mut)}
  .EDGE_SUSPECT{color:var(--red);font-weight:700}.TAPE_IMBALANCE{color:var(--amb);font-weight:700}
  .LARGE_PRINT{color:var(--fg);font-weight:700}.EXPIRING{color:var(--amb);font-weight:700}
  .IMPLIED_MOVE{color:var(--blu);font-weight:700}.WINDOW_OPEN{color:var(--grn);font-weight:700}
  .WINDOW_SETTLED{color:var(--blu);font-weight:700}
</style></head><body>
<div class="top">
  <div class="btc mono">&#8383;</div>
  <div><h1>BTC 15 min <span class="muted mono" style="font-size:13px">· __SERIES__</span></h1>
    <div class="sub" id="window">—</div></div>
  <div class="grow"></div>
  <div class="live"><span class="dot"></span> LIVE</div>
</div>
<div class="stats">
  <div class="stat"><div class="lbl">To Beat</div><div class="val mono" id="strike">—</div>
    <div class="chg muted">target</div></div>
  <div class="stat"><div class="lbl">Now</div><div class="val mono" id="spot">—</div>
    <div class="chg mono" id="spotChg">—</div></div>
  <div class="stat"><div class="lbl">Closes In</div><div class="val mono cd" id="cd">—</div>
    <div class="chg muted">15-min window</div></div>
  <div class="grow"></div>
  <div class="stat" style="border-right:none;text-align:right">
    <div class="lbl">Realized Vol</div><div class="val mono" id="rv">—</div>
    <div class="chg muted">annualized</div></div>
</div>
<div class="wrap">
  <div class="left">
    <div class="chartbox"><canvas id="chart"></canvas></div>
    <div class="settling" id="settling" style="display:none">⏳ window settling — waiting for the next 15-min window…</div>
    <div class="chartleg">
      <span><span style="color:var(--orange)">━</span> BTC spot</span>
      <span><span style="color:var(--grn)">┄</span> target $<span id="legStrike">—</span></span>
      <span>vol <span class="muted" id="legVol">—</span></span>
    </div>
    <div class="ud">
      <div class="udcard up"><div class="t">Up (Yes)</div><div class="p mono" id="up">—</div></div>
      <div class="udcard down"><div class="t">Down (No)</div><div class="p mono" id="down">—</div></div>
    </div>
    <div class="analytics">
      <div class="chip"><div class="k">Market</div><div class="v" id="implied">—</div></div>
      <div class="chip"><div class="k">Model</div><div class="v" id="model">—</div></div>
      <div class="chip"><div class="k">Edge</div><div class="v" id="edge">—</div></div>
      <div class="chip"><div class="k">Quality</div><div class="v"><span class="badge" id="rel">—</span></div></div>
      <div class="chip"><div class="k">Open Int</div><div class="v" id="oi">—</div></div>
      <div class="chip"><div class="k">Volume</div><div class="v" id="vol">—</div></div>
    </div>
    <div class="warn" id="warn"></div>
  </div>
  <div class="rail">
    <div class="results">
      <h2>Recent Results</h2>
      <div class="scorebar"><span>model <span id="scModel" class="mono">—</span></span>
        <span>market <span id="scMkt" class="mono">—</span></span></div>
      <div id="results"></div>
    </div>
    <div class="feed"><h2>Anomaly Feed</h2><div id="feed"></div></div>
  </div>
</div>
<script>
const dpr = window.devicePixelRatio || 1;
let hist = [], strike = null, closeMs = null;
const f0 = x => x==null ? "—" : Math.round(x).toLocaleString();
const pct = x => x==null ? "—" : (x*100).toFixed(0)+"%";
const cents = x => x==null ? "—" : Math.round(x*100)+"¢";

function primary(markets){
  const live = (markets||[]).filter(m=>!m.expired && m.minutes_to_close!=null);
  if(!live.length) return null;
  return live.sort((a,b)=>a.minutes_to_close-b.minutes_to_close)[0];
}
function drawChart(){
  const c=document.getElementById('chart'); const w=c.clientWidth, h=c.clientHeight;
  c.width=w*dpr; c.height=h*dpr; const ctx=c.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,w,h);
  if(hist.length<2) return;
  const pad={l:8,r:64,t:12,b:20};
  const xs=hist.map(p=>p.t), ys=hist.map(p=>p.spot);
  let lo=Math.min(...ys, strike??Infinity), hi=Math.max(...ys, strike??-Infinity);
  const span=(hi-lo)||1; lo-=span*0.12; hi+=span*0.12;
  const x0=xs[0], x1=xs[xs.length-1]||x0+1;
  const X=t=>pad.l+(t-x0)/((x1-x0)||1)*(w-pad.l-pad.r);
  const Y=v=>pad.t+(hi-v)/(hi-lo)*(h-pad.t-pad.b);
  ctx.font="11px ui-monospace,monospace"; ctx.textBaseline="middle";
  for(let i=0;i<=4;i++){const v=lo+(hi-lo)*i/4, y=Y(v);
    ctx.strokeStyle="#1c1e25"; ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
    ctx.fillStyle="#6b7080"; ctx.fillText("$"+Math.round(v).toLocaleString(), w-pad.r+6, y);}
  if(strike!=null){const y=Y(strike); ctx.setLineDash([5,4]); ctx.strokeStyle="#00d182";
    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();ctx.setLineDash([]);}
  ctx.strokeStyle="#ff8a3d"; ctx.lineWidth=2; ctx.beginPath();
  hist.forEach((p,i)=>{const x=X(p.t),y=Y(p.spot); i?ctx.lineTo(x,y):ctx.moveTo(x,y);}); ctx.stroke();
  const last=hist[hist.length-1]; ctx.fillStyle = strike!=null && last.spot>=strike ? "#00d182":"#ff5a5f";
  ctx.beginPath();ctx.arc(X(last.t),Y(last.spot),4,0,7);ctx.fill();
}
function renderResults(ledger){
  if(!ledger) return;
  const el=document.getElementById('results');
  el.innerHTML = ledger.map(r=>{
    const mc = r.model_correct==null?'' : (r.model_correct?'<span class="tick">✓</span>':'<span class="cross">✗</span>');
    const kc = r.market_correct==null?'' : (r.market_correct?'<span class="tick">✓</span>':'<span class="cross">✗</span>');
    return `<div class="res"><span>$${f0(r.strike)} → <span class="${r.result}">${(r.result||'').toUpperCase()}</span></span>`
      +`<span class="muted">mkt ${pct(r.implied)} ${kc} · mdl ${pct(r.model)} ${mc}</span></div>`;
  }).join('') || '<div class="muted" style="font-size:12px">no settled windows yet</div>';
  const n=ledger.length;
  const mok=ledger.filter(r=>r.model_correct===true).length;
  const kok=ledger.filter(r=>r.market_correct===true).length;
  document.getElementById('scModel').textContent = n? mok+"/"+n+" ("+Math.round(mok/n*100)+"%)" : "—";
  document.getElementById('scMkt').textContent   = n? kok+"/"+n+" ("+Math.round(kok/n*100)+"%)" : "—";
}
function render(d){
  document.getElementById('rv').textContent = d.vol!=null ? (d.vol*100).toFixed(0)+"%" : "—";
  document.getElementById('legVol').textContent = d.vol!=null ? (d.vol*100).toFixed(0)+"%" : "—";
  renderResults(d.ledger);
  (d.events||[]).forEach(e=>{const div=document.createElement('div');div.className='ev';
    const rest=Object.entries(e).filter(([k])=>!['type','ticker','warning'].includes(k)).map(([k,v])=>k+'='+v).join(' ');
    div.innerHTML=`<span class="t">${new Date(d.ts).toLocaleTimeString()}</span> <span class="${e.type}">${e.type}</span> ${rest}`;
    const feed=document.getElementById('feed'); feed.prepend(div);
    while(feed.childNodes.length>150) feed.removeChild(feed.lastChild);});

  const m=primary(d.markets);
  document.getElementById('settling').style.display = m?'none':'block';
  if(!m){ document.getElementById('window').textContent='settling — awaiting next window'; return; }
  strike=m.floor_strike;
  if(m.minutes_to_close!=null) closeMs=Date.now()+m.minutes_to_close*60000;
  hist.push({t:new Date(d.ts).getTime(), spot:d.spot}); if(hist.length>240) hist.shift();
  document.getElementById('window').textContent = m.ticker;
  document.getElementById('strike').textContent = "$"+f0(strike);
  document.getElementById('legStrike').textContent = f0(strike);
  const spotEl=document.getElementById('spot'); spotEl.textContent="$"+f0(d.spot);
  spotEl.className="val mono "+(strike!=null && d.spot>=strike?"pos":"neg");
  if(strike!=null){const diff=d.spot-strike, p=diff/strike*100, c=diff>=0?"pos":"neg";
    document.getElementById('spotChg').innerHTML=`<span class="${c}">${diff>=0?'+':''}${diff.toFixed(2)} (${diff>=0?'+':''}${p.toFixed(3)}%)</span>`;}
  document.getElementById('up').textContent = cents(m.yes_ask);
  document.getElementById('down').textContent = m.yes_bid!=null ? Math.round((1-m.yes_bid)*100)+"¢" : "—";
  document.getElementById('implied').textContent = pct(m.implied_prob);
  document.getElementById('model').textContent = pct(m.model_prob);
  const edgeEl=document.getElementById('edge');
  edgeEl.textContent = m.edge==null?"—":((m.edge>0?'+':'')+(m.edge*100).toFixed(0)+"%");
  edgeEl.className = "v "+(m.edge==null?"":(m.edge>0?"pos":"neg"));
  const rel=document.getElementById('rel'); rel.textContent=m.edge_reliability||"—"; rel.className="badge "+(m.edge_reliability||"");
  document.getElementById('oi').textContent=f0(m.open_interest);
  document.getElementById('vol').textContent=f0(m.volume);
  document.getElementById('warn').textContent = m.warning?("⚠ "+m.warning):"";
  drawChart();
}
setInterval(()=>{const el=document.getElementById('cd');
  if(closeMs==null){return;} let s=Math.max(0,Math.round((closeMs-Date.now())/1000));
  el.textContent=Math.floor(s/60)+":"+String(s%60).padStart(2,'0');
  el.style.color=s<120?"var(--red)":"var(--amb)";},1000);
window.addEventListener('resize',drawChart);
function seedHistory(){fetch('/api/history').then(r=>r.json()).then(d=>{
  if(hist.length===0 && d.points && d.points.length){hist=d.points.slice(); drawChart();}}).catch(()=>{});}
let lastSeq=-1;
function connect(){const es=new EventSource('/api/stream');
  es.onmessage=ev=>{try{const d=JSON.parse(ev.data); if(d.seq===lastSeq) return; lastSeq=d.seq;
    if(d.type==='TICK') render(d);}catch(e){}};
  es.onerror=()=>{es.close();document.getElementById('window').textContent='reconnecting…';setTimeout(connect,2000);};
}
seedHistory(); connect();
</script></body></html>"""


def main() -> int:
    p = argparse.ArgumentParser(description="Kalshi BTC monitoring cockpit (web)")
    p.add_argument("--series", default=SERIES)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.environ.get("KALSHI_MONITOR_PORT", "8787")))
    args = p.parse_args()
    globals()["SERIES"] = args.series
    _load_ledger()
    threading.Thread(target=_poller, daemon=True).start()
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

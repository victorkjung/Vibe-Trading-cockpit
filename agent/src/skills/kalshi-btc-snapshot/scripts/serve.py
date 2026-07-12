#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kalshi BTC monitoring cockpit — self-contained web service.

Serves a live dashboard (embedded HTML, no build step) plus an SSE stream that
relays a single shared background poller to all browsers. Monitoring only —
there is no order path anywhere in this service (execution stays on Kalshi).

The dashboard has two layers:
  * CoinGecko market intelligence (top): price/market stats, multi-range chart,
    exchange markets, public-company treasuries, news, historical data —
    proxied + cached server-side via coingecko.py (free-tier friendly).
  * Kalshi 15-min cockpit (bottom): the original live window monitor — SSE
    stream, model vs market, anomaly feed, settled-window ledger.

A background thread polls the snapshot, runs anomaly detection, and maintains a
**window ledger**: when a 15-min window closes it records Kalshi's authoritative
settlement (result) against what the market and our model predicted — the basis
for next-wager analysis. The dashboard auto-rolls to the next live window.

Deps:  pip install fastapi uvicorn requests
Run:   python serve.py --series KXBTC15M --host 0.0.0.0 --port 8787
View:  http://<vps-tailscale-ip>:8787

Endpoints:
    GET /                  embedded dashboard
    GET /api/snapshot      one-shot JSON snapshot (with trades)
    GET /api/history       ~60 min of 1m closes to seed the chart
    GET /api/stream        text/event-stream relaying the shared poller
    GET /api/results       recent settled-window ledger (JSON)
    GET /api/cg/overview   CoinGecko BTC market overview (cached 60s)
    GET /api/cg/chart      CoinGecko price/mcap/volume series (?days=1|7|30|90|365|max)
    GET /api/cg/tickers    top exchange tickers for BTC (cached 5m)
    GET /api/cg/treasuries public-company BTC treasuries (cached 1h)
    GET /api/cg/news       Bitcoin news (CoinGecko Pro if keyed, else RSS; 15m)
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
import coingecko  # noqa: E402
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


# --------------------------------------------------------------------------- #
# CoinGecko proxy endpoints (server-side cache; see coingecko.py)
# --------------------------------------------------------------------------- #
def _cg(fetch) -> JSONResponse:
    try:
        return JSONResponse(fetch())
    except Exception as exc:  # noqa: BLE001 - degrade to a JSON error, not a 500 page
        return JSONResponse({"error": str(exc)}, status_code=502)


@app.get("/api/cg/overview")
def api_cg_overview() -> JSONResponse:
    return _cg(coingecko.overview)


@app.get("/api/cg/chart")
def api_cg_chart(days: str = "1") -> JSONResponse:
    return _cg(lambda: coingecko.chart(days))


@app.get("/api/cg/tickers")
def api_cg_tickers() -> JSONResponse:
    return _cg(coingecko.tickers)


@app.get("/api/cg/treasuries")
def api_cg_treasuries() -> JSONResponse:
    return _cg(coingecko.treasuries)


@app.get("/api/cg/news")
def api_cg_news() -> JSONResponse:
    return _cg(coingecko.news)


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
<title>Bitcoin · Kalshi × CoinGecko Cockpit · __SERIES__</title>
<style>
  :root{--bg:#0b0f15;--panel:#10151d;--panel2:#0d1219;--line:#1d2531;--fg:#eef2f7;
        --mut:#8b95a6;--gecko:#8dc63f;--grn:#00d181;--red:#ff4d57;--amb:#ffb454;
        --orange:#f7931a;--blu:#5aa9ff}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);
    font:14px/1.45 ui-sans-serif,-apple-system,Segoe UI,Roboto,sans-serif}
  a{color:var(--blu);text-decoration:none} a:hover{text-decoration:underline}
  .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  .pos{color:var(--grn)} .neg{color:var(--red)} .muted{color:var(--mut)}

  /* ── header ─────────────────────────────────────────────────────────── */
  .top{display:flex;align-items:center;gap:14px;padding:13px 22px;
    border-bottom:1px solid var(--line);
    background:linear-gradient(90deg,rgba(141,198,63,.06),rgba(247,147,26,.05) 55%,transparent)}
  .btc{width:40px;height:40px;border-radius:50%;background:var(--orange);display:grid;place-items:center;
    font-weight:800;color:#1a1205;font-size:22px}
  .top h1{font-size:18px;margin:0;font-weight:700;display:flex;align-items:center;gap:8px}
  .rank{font-size:10px;padding:2px 7px;border-radius:6px;background:var(--panel);
    border:1px solid var(--line);color:var(--mut);font-weight:700}
  .top .sub{color:var(--mut);font-size:12px;margin-top:2px}
  .brandx{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--mut)}
  .brandx b{color:var(--grn)} .brandx .gk{color:var(--gecko);font-weight:700}
  .live{display:flex;align-items:center;gap:6px;color:var(--red);font-weight:700;font-size:12px;letter-spacing:.5px}
  .live .dot{width:8px;height:8px;border-radius:50%;background:var(--red);animation:pulse 1.4s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .grow{flex:1}

  /* ── coingecko layer ────────────────────────────────────────────────── */
  .cg{border-bottom:1px solid var(--line)}
  .cghead{display:flex;flex-wrap:wrap;align-items:flex-end;gap:26px;padding:18px 22px 12px}
  .cgprice .p{font-size:38px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1.05}
  .cgprice .l{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:4px;
    display:flex;gap:8px;align-items:center}
  .chip24{font-size:14px;font-weight:700;padding:2px 9px;border-radius:8px}
  .chip24.pos{background:rgba(0,209,129,.13)} .chip24.neg{background:rgba(255,77,87,.13)}
  .rangebar{min-width:250px;flex:0 1 340px;padding-bottom:6px}
  .rangebar .lbls{display:flex;justify-content:space-between;font-size:11px;color:var(--mut);margin-bottom:5px}
  .rangebar .lbls b{color:var(--fg);font-weight:600}
  .rb{height:6px;border-radius:4px;background:linear-gradient(90deg,var(--red),var(--amb),var(--grn));position:relative}
  .rb i{position:absolute;top:-4px;width:3px;height:14px;background:#fff;border-radius:2px;box-shadow:0 0 0 2px var(--bg)}
  .updated{font-size:11px;color:var(--mut);padding-bottom:8px}
  .cgbody{display:grid;grid-template-columns:300px 1fr;gap:0;border-top:1px solid var(--line)}
  .cgstats{border-right:1px solid var(--line);padding:8px 0}
  .srow{display:flex;justify-content:space-between;gap:10px;padding:9px 22px;border-bottom:1px solid var(--panel2);font-size:13px}
  .srow .k{color:var(--mut)} .srow .v{font-weight:600;font-variant-numeric:tabular-nums;text-align:right}
  .srow .v small{color:var(--mut);font-weight:400}
  .cgmain{min-width:0}
  .tabs{display:flex;gap:2px;padding:10px 16px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
  .tab{background:none;border:none;color:var(--mut);font:600 13px inherit;font-family:inherit;
    padding:9px 14px;cursor:pointer;border-bottom:2px solid transparent}
  .tab:hover{color:var(--fg)}
  .tab.on{color:var(--gecko);border-bottom-color:var(--gecko)}
  .tabpane{display:none;padding:14px 18px 18px} .tabpane.on{display:block}
  .chartbar{display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap}
  .pills{display:flex;gap:4px;background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:3px}
  .pill{background:none;border:none;color:var(--mut);font:600 12px inherit;font-family:inherit;
    padding:5px 11px;border-radius:7px;cursor:pointer}
  .pill.on{background:var(--line);color:var(--fg)}
  .pill:hover{color:var(--fg)}
  .cgwrap{position:relative}
  #cgChart{width:100%;height:340px;display:block;cursor:crosshair}
  #cgTip{position:absolute;pointer-events:none;display:none;background:#161d29;border:1px solid var(--line);
    border-radius:8px;padding:7px 10px;font-size:12px;box-shadow:0 6px 18px rgba(0,0,0,.5);z-index:5;white-space:nowrap}
  #cgTip .t{color:var(--mut);font-size:11px}
  .cgnote{font-size:11px;color:var(--mut);margin-top:6px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px}
  .perf{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-top:14px}
  .pcell{border:1px solid var(--line);border-radius:10px;padding:9px 6px;text-align:center}
  .pcell .k{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px}
  .pcell .v{font-size:15px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums}
  table.cgt{width:100%;border-collapse:collapse;font-size:13px}
  .cgt th{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:right;
    padding:7px 10px;border-bottom:1px solid var(--line);font-weight:600}
  .cgt td{padding:8px 10px;border-bottom:1px solid var(--panel2);text-align:right;font-variant-numeric:tabular-nums}
  .cgt th:first-child,.cgt td:first-child{text-align:left}
  .cgt tr:hover td{background:rgba(255,255,255,.02)}
  .trust{display:inline-block;width:9px;height:9px;border-radius:50%}
  .trust.green{background:var(--grn)} .trust.yellow{background:var(--amb)} .trust.red{background:var(--red)}
  .newsit{padding:11px 2px;border-bottom:1px solid var(--panel2)}
  .newsit .h{font-weight:600;font-size:14px}
  .newsit .m{color:var(--mut);font-size:11px;margin-top:3px}
  .newsit .d{color:var(--mut);font-size:12px;margin-top:4px}
  .loading{color:var(--mut);padding:26px 0;text-align:center;font-size:13px}
  .scroller{max-height:430px;overflow:auto}

  /* ── section divider ────────────────────────────────────────────────── */
  .secdiv{display:flex;align-items:center;gap:12px;padding:12px 22px;
    background:linear-gradient(90deg,rgba(0,209,129,.07),transparent 60%);
    border-bottom:1px solid var(--line);font-size:12px;letter-spacing:1.2px;
    text-transform:uppercase;font-weight:700;color:var(--grn)}
  .secdiv .k{color:var(--mut);font-weight:400;letter-spacing:.3px;text-transform:none}

  /* ── kalshi cockpit (original) ──────────────────────────────────────── */
  .cd{color:var(--amb)}
  .stats{display:flex;gap:0;border-bottom:1px solid var(--line)}
  .stat{padding:16px 22px;border-right:1px solid var(--line);min-width:180px}
  .stat .lbl{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
  .stat .val{font-size:30px;font-weight:700;margin-top:4px;font-variant-numeric:tabular-nums}
  .stat .chg{font-size:13px;margin-top:2px;font-variant-numeric:tabular-nums}
  .wrap{display:grid;grid-template-columns:1fr 360px;gap:0}
  .left{border-right:1px solid var(--line)}
  .chartbox{padding:14px 18px 4px} #chart{width:100%;height:330px;display:block}
  .settling{padding:8px 22px;color:var(--amb);font-size:13px}
  .chartleg{display:flex;gap:18px;padding:0 22px 10px;color:var(--mut);font-size:12px}
  .ud{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:6px 18px 16px}
  .udcard{border:1px solid var(--line);border-radius:12px;padding:14px 16px;text-align:center}
  .udcard.up{border-color:rgba(0,209,129,.4)} .udcard.down{border-color:rgba(255,77,87,.4)}
  .udcard .t{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px}
  .udcard .p{font-size:30px;font-weight:800;margin-top:6px}
  .up .p{color:var(--grn)} .down .p{color:var(--red)}
  .analytics{display:flex;flex-wrap:wrap;gap:10px;padding:0 18px 18px}
  .chip{border:1px solid var(--line);border-radius:10px;padding:8px 12px;min-width:104px}
  .chip .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  .chip .v{font-size:18px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums}
  .badge{font-size:10px;padding:2px 8px;border-radius:20px;text-transform:uppercase;letter-spacing:.4px;font-weight:700}
  .ok{background:rgba(0,209,129,.15);color:var(--grn)}
  .low{background:rgba(255,180,84,.15);color:var(--amb)}
  .suspect{background:rgba(255,77,87,.15);color:var(--red)}
  .warn{color:var(--amb);font-size:12px;padding:0 18px 16px}
  .rail{display:flex;flex-direction:column;max-height:860px}
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

  @media (max-width:980px){
    .cgbody{grid-template-columns:1fr}
    .cgstats{border-right:none;border-bottom:1px solid var(--line);
      display:grid;grid-template-columns:1fr 1fr}
    .wrap{grid-template-columns:1fr}
    .left{border-right:none}
    .perf{grid-template-columns:repeat(3,1fr)}
    .stats{flex-wrap:wrap}
  }
</style></head><body>

<div class="top">
  <div class="btc mono">&#8383;</div>
  <div>
    <h1>Bitcoin <span class="muted" style="font-weight:600">BTC</span> <span class="rank" id="rank">#—</span></h1>
    <div class="sub">Kalshi 15-min cockpit · <span class="mono" id="window">—</span></div>
  </div>
  <div class="grow"></div>
  <div class="brandx"><b>KALSHI</b> × <span class="gk">🦎 CoinGecko</span></div>
  <div class="live"><span class="dot"></span> LIVE</div>
</div>

<!-- ═══ CoinGecko market intelligence layer ═══ -->
<section class="cg">
  <div class="cghead">
    <div class="cgprice">
      <div class="l">BTC Price <span class="chip24" id="cgChg24">—</span></div>
      <div class="p mono" id="cgPrice">—</div>
    </div>
    <div class="rangebar">
      <div class="lbls"><span>24h Low <b class="mono" id="cgLow">—</b></span>
        <span>24h High <b class="mono" id="cgHigh">—</b></span></div>
      <div class="rb"><i id="cgMark" style="left:50%"></i></div>
    </div>
    <div class="grow"></div>
    <div class="updated">CoinGecko · updated <span id="cgUpd">—</span></div>
  </div>
  <div class="cgbody">
    <aside class="cgstats">
      <div class="srow"><span class="k">Market Cap</span><span class="v mono" id="cgMcap">—</span></div>
      <div class="srow"><span class="k">Fully Diluted Valuation</span><span class="v mono" id="cgFdv">—</span></div>
      <div class="srow"><span class="k">24h Trading Volume</span><span class="v mono" id="cgVol">—</span></div>
      <div class="srow"><span class="k">Circulating Supply</span><span class="v mono" id="cgCirc">—</span></div>
      <div class="srow"><span class="k">Total / Max Supply</span><span class="v mono" id="cgSupply">—</span></div>
      <div class="srow"><span class="k">All-Time High</span><span class="v mono" id="cgAth">—</span></div>
    </aside>
    <div class="cgmain">
      <nav class="tabs">
        <button class="tab on" data-tab="overview">Overview</button>
        <button class="tab" data-tab="markets">Markets</button>
        <button class="tab" data-tab="treasuries">Treasuries</button>
        <button class="tab" data-tab="news">News</button>
        <button class="tab" data-tab="historical">Historical Data</button>
      </nav>

      <div class="tabpane on" id="tab-overview">
        <div class="chartbar">
          <div class="pills" id="rangePills">
            <button class="pill on" data-days="1">24H</button>
            <button class="pill" data-days="7">7D</button>
            <button class="pill" data-days="30">1M</button>
            <button class="pill" data-days="90">3M</button>
            <button class="pill" data-days="365">1Y</button>
            <button class="pill" data-days="max">MAX</button>
          </div>
          <div class="pills" id="metricPills">
            <button class="pill on" data-metric="prices">Price</button>
            <button class="pill" data-metric="market_caps">Market Cap</button>
            <button class="pill" data-metric="total_volumes">Volume</button>
          </div>
        </div>
        <div class="cgwrap">
          <canvas id="cgChart"></canvas>
          <div id="cgTip"></div>
        </div>
        <div class="cgnote">
          <span id="cgRangeStat">—</span>
          <span>source: CoinGecko API</span>
        </div>
        <div class="perf" id="perf"></div>
      </div>

      <div class="tabpane" id="tab-markets">
        <div class="scroller"><div class="loading" id="marketsLoad">loading exchange markets…</div>
        <table class="cgt" id="marketsTbl" style="display:none">
          <thead><tr><th>Exchange</th><th>Pair</th><th>Price</th><th>Spread</th><th>24h Volume</th><th>Trust</th></tr></thead>
          <tbody></tbody></table></div>
      </div>

      <div class="tabpane" id="tab-treasuries">
        <div class="scroller">
        <div class="analytics" id="treasTotals" style="padding:0 0 12px"></div>
        <div class="loading" id="treasLoad">loading public-company treasuries…</div>
        <table class="cgt" id="treasTbl" style="display:none">
          <thead><tr><th>Company</th><th>Country</th><th>Holdings (BTC)</th><th>Current Value</th><th>% of Supply</th></tr></thead>
          <tbody></tbody></table></div>
      </div>

      <div class="tabpane" id="tab-news">
        <div class="scroller">
        <div class="loading" id="newsLoad">loading Bitcoin news…</div>
        <div id="newsList"></div></div>
      </div>

      <div class="tabpane" id="tab-historical">
        <div class="scroller"><div class="loading" id="histLoad">loading historical data…</div>
        <table class="cgt" id="histTbl" style="display:none">
          <thead><tr><th>Date</th><th>Price</th><th>Market Cap</th><th>Volume</th><th>Δ Day</th></tr></thead>
          <tbody></tbody></table></div>
      </div>
    </div>
  </div>
</section>

<div class="secdiv">◆ Kalshi 15-Min Market Cockpit <span class="k">· __SERIES__ · model vs market, live via SSE</span></div>

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
/* ════════════════════════ shared helpers ════════════════════════ */
const dpr = window.devicePixelRatio || 1;
const $ = id => document.getElementById(id);
const f0 = x => x==null ? "—" : Math.round(x).toLocaleString();
const pct = x => x==null ? "—" : (x*100).toFixed(0)+"%";
const cents = x => x==null ? "—" : Math.round(x*100)+"¢";
const usd = x => x==null ? "—" : "$"+Number(x).toLocaleString(undefined,{maximumFractionDigits: x<10?4:(x<1000?2:0)});
function compact(x){ if(x==null) return "—";
  const a=Math.abs(x);
  if(a>=1e12) return "$"+(x/1e12).toFixed(3)+"T";
  if(a>=1e9)  return "$"+(x/1e9).toFixed(2)+"B";
  if(a>=1e6)  return "$"+(x/1e6).toFixed(2)+"M";
  return usd(x); }
const signPct = x => x==null ? "—" : (x>=0?"▲ ":"▼ ")+Math.abs(x).toFixed(1)+"%";

/* ════════════════════════ CoinGecko layer ════════════════════════ */
const CG = { days:"1", metric:"prices", series:null, loaded:{}, price:null };

async function cgFetch(path){
  const r = await fetch(path);
  if(!r.ok) throw new Error("upstream "+r.status);
  const j = await r.json();
  if(j && j.error) throw new Error(j.error);
  return j;
}

async function loadOverview(){
  try{
    const d = await cgFetch('/api/cg/overview');
    CG.price = d.price;
    $('rank').textContent = d.rank ? "#"+d.rank : "#—";
    $('cgPrice').textContent = usd(d.price);
    const c24 = (d.changes||{})["24h"];
    const chg = $('cgChg24');
    chg.textContent = signPct(c24);
    chg.className = "chip24 "+(c24>=0?"pos":"neg");
    $('cgLow').textContent = usd(d.low_24h); $('cgHigh').textContent = usd(d.high_24h);
    if(d.low_24h!=null && d.high_24h!=null && d.price!=null && d.high_24h>d.low_24h){
      const p = Math.min(1,Math.max(0,(d.price-d.low_24h)/(d.high_24h-d.low_24h)));
      $('cgMark').style.left = (p*100).toFixed(1)+"%";
    }
    $('cgMcap').textContent = compact(d.market_cap);
    $('cgFdv').textContent = compact(d.fdv);
    $('cgVol').textContent = compact(d.volume_24h);
    $('cgCirc').textContent = d.circulating!=null ? (d.circulating/1e6).toFixed(3)+"M BTC" : "—";
    $('cgSupply').textContent = (d.total_supply!=null?(d.total_supply/1e6).toFixed(2)+"M":"—")
      +" / "+(d.max_supply!=null?(d.max_supply/1e6).toFixed(0)+"M":"∞");
    $('cgAth').innerHTML = usd(d.ath)+" <small>("+(d.ath_change_pct!=null?d.ath_change_pct.toFixed(1)+"%":"—")+")</small>";
    $('cgUpd').textContent = d.last_updated ? new Date(d.last_updated).toLocaleTimeString() : "—";
    const P = $('perf'); P.innerHTML = "";
    [["1h","1h"],["24h","24h"],["7d","7d"],["14d","14d"],["30d","30d"],["1y","1y"]].forEach(([k,lbl])=>{
      const v = (d.changes||{})[k];
      P.insertAdjacentHTML('beforeend',
        `<div class="pcell"><div class="k">${lbl}</div>
         <div class="v ${v==null?'muted':(v>=0?'pos':'neg')}">${v==null?'—':(v>=0?'+':'')+v.toFixed(1)+'%'}</div></div>`);
    });
  }catch(e){ $('cgUpd').textContent = "unavailable"; }
}

/* — multi-range chart with crosshair — */
async function loadCgChart(){
  try{
    CG.series = await cgFetch('/api/cg/chart?days='+CG.days);
    drawCgChart();
  }catch(e){
    CG.series = null; drawCgChart();
    $('cgRangeStat').textContent = "chart unavailable ("+e.message+")";
  }
}
function fmtAxisTime(ms){
  const d = new Date(ms);
  if(CG.days==="1") return d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  if(CG.days==="7"||CG.days==="30") return d.toLocaleDateString([],{month:'short',day:'numeric'});
  if(CG.days==="90"||CG.days==="365") return d.toLocaleDateString([],{month:'short',day:'numeric'});
  return d.toLocaleDateString([],{month:'short',year:'2-digit'});
}
function fmtVal(v){ return CG.metric==="prices" ? usd(v) : compact(v); }
let cgPts = [];   // screen-space points for hover
function drawCgChart(){
  const c = $('cgChart'); const w=c.clientWidth, h=c.clientHeight;
  c.width=w*dpr; c.height=h*dpr; const ctx=c.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,w,h);
  cgPts = [];
  const raw = CG.series && CG.series[CG.metric];
  if(!raw || raw.length<2){ ctx.fillStyle="#6b7080"; ctx.font="13px sans-serif";
    ctx.fillText("no data", 20, 30); return; }
  // downsample to ~2 points per px for speed
  const maxPts = Math.max(100, w*2);
  const step = Math.max(1, Math.floor(raw.length/maxPts));
  const data = raw.filter((_,i)=> i%step===0 || i===raw.length-1);
  const pad={l:10,r:76,t:14,b:24};
  const xs=data.map(p=>p[0]), ys=data.map(p=>p[1]);
  let lo=Math.min(...ys), hi=Math.max(...ys);
  const span=(hi-lo)||1; lo-=span*0.06; hi+=span*0.06;
  const x0=xs[0], x1=xs[xs.length-1]||x0+1;
  const X=t=>pad.l+(t-x0)/((x1-x0)||1)*(w-pad.l-pad.r);
  const Y=v=>pad.t+(hi-v)/(hi-lo)*(h-pad.t-pad.b);
  const up = ys[ys.length-1] >= ys[0];
  const col = up ? "#00d181" : "#ff4d57";
  // gridlines + y labels
  ctx.font="11px ui-monospace,monospace"; ctx.textBaseline="middle";
  for(let i=0;i<=4;i++){const v=lo+(hi-lo)*i/4, y=Y(v);
    ctx.strokeStyle="#161d28"; ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
    ctx.fillStyle="#6b7080";
    ctx.fillText(CG.metric==="prices" ? "$"+Math.round(v).toLocaleString() : compact(v), w-pad.r+6, y);}
  // x labels
  ctx.textBaseline="top";
  for(let i=0;i<=5;i++){const t=x0+(x1-x0)*i/5;
    ctx.fillStyle="#5b6272";
    const s=fmtAxisTime(t); const tw=ctx.measureText(s).width;
    ctx.fillText(s, Math.min(Math.max(pad.l, X(t)-tw/2), w-pad.r-tw), h-pad.b+7);}
  // area fill
  const grad = ctx.createLinearGradient(0,pad.t,0,h-pad.b);
  grad.addColorStop(0, up ? "rgba(0,209,129,.22)" : "rgba(255,77,87,.22)");
  grad.addColorStop(1, "rgba(0,0,0,0)");
  ctx.beginPath();
  data.forEach((p,i)=>{const x=X(p[0]),y=Y(p[1]); i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.lineTo(X(x1),h-pad.b); ctx.lineTo(X(x0),h-pad.b); ctx.closePath();
  ctx.fillStyle=grad; ctx.fill();
  // line
  ctx.strokeStyle=col; ctx.lineWidth=2; ctx.beginPath();
  data.forEach((p,i)=>{const x=X(p[0]),y=Y(p[1]); cgPts.push({x,y,t:p[0],v:p[1]}); i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.stroke();
  // end dot
  const lastP=data[data.length-1];
  ctx.fillStyle=col; ctx.beginPath(); ctx.arc(X(lastP[0]),Y(lastP[1]),3.5,0,7); ctx.fill();
  // range stat line
  const chg=(ys[ys.length-1]-ys[0])/ys[0]*100;
  $('cgRangeStat').innerHTML =
    `range <span class="mono">${fmtVal(Math.min(...ys))}</span> – <span class="mono">${fmtVal(Math.max(...ys))}</span>
     · change <span class="${chg>=0?'pos':'neg'} mono">${chg>=0?'+':''}${chg.toFixed(2)}%</span>`;
}
// crosshair tooltip
(function(){
  const c=$('cgChart'), tip=$('cgTip');
  c.addEventListener('mousemove',e=>{
    if(!cgPts.length){tip.style.display='none';return;}
    const r=c.getBoundingClientRect(), mx=e.clientX-r.left;
    let best=cgPts[0], bd=1e9;
    for(const p of cgPts){const d=Math.abs(p.x-mx); if(d<bd){bd=d;best=p;}}
    tip.innerHTML=`<div class="t">${new Date(best.t).toLocaleString()}</div><b class="mono">${fmtVal(best.v)}</b>`;
    tip.style.display='block';
    const tw=tip.offsetWidth;
    tip.style.left=Math.min(Math.max(4,best.x-tw/2), c.clientWidth-tw-4)+"px";
    tip.style.top=Math.max(4,best.y-52)+"px";
  });
  c.addEventListener('mouseleave',()=>tip.style.display='none');
})();

/* — range & metric pills — */
$('rangePills').addEventListener('click',e=>{
  const b=e.target.closest('.pill'); if(!b) return;
  document.querySelectorAll('#rangePills .pill').forEach(p=>p.classList.remove('on'));
  b.classList.add('on'); CG.days=b.dataset.days; loadCgChart();
});
$('metricPills').addEventListener('click',e=>{
  const b=e.target.closest('.pill'); if(!b) return;
  document.querySelectorAll('#metricPills .pill').forEach(p=>p.classList.remove('on'));
  b.classList.add('on'); CG.metric=b.dataset.metric; drawCgChart();
});

/* — tabs (lazy-load each pane once) — */
document.querySelector('.tabs').addEventListener('click',e=>{
  const b=e.target.closest('.tab'); if(!b) return;
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('.tabpane').forEach(p=>p.classList.remove('on'));
  b.classList.add('on'); $('tab-'+b.dataset.tab).classList.add('on');
  const t=b.dataset.tab;
  if(t==='overview'){ drawCgChart(); }
  if(t==='markets' && !CG.loaded.markets){ CG.loaded.markets=1; loadMarkets(); }
  if(t==='treasuries' && !CG.loaded.treas){ CG.loaded.treas=1; loadTreasuries(); }
  if(t==='news' && !CG.loaded.news){ CG.loaded.news=1; loadNews(); }
  if(t==='historical' && !CG.loaded.hist){ CG.loaded.hist=1; loadHistorical(); }
});

async function loadMarkets(){
  try{
    const d = await cgFetch('/api/cg/tickers');
    const tb = document.querySelector('#marketsTbl tbody'); tb.innerHTML="";
    (d.tickers||[]).forEach(t=>{
      const pair = (t.base||"")+"/"+(t.target||"");
      tb.insertAdjacentHTML('beforeend', `<tr>
        <td>${t.url?`<a href="${t.url}" target="_blank" rel="noopener">${t.exchange||"—"}</a>`:(t.exchange||"—")}</td>
        <td class="mono">${pair.length>18?pair.slice(0,17)+"…":pair}</td>
        <td class="mono">${usd(t.price_usd)}</td>
        <td class="mono">${t.spread_pct!=null?t.spread_pct.toFixed(2)+"%":"—"}</td>
        <td class="mono">${compact(t.volume_usd)}</td>
        <td><span class="trust ${t.trust||''}"></span></td></tr>`);
    });
    $('marketsLoad').style.display='none'; $('marketsTbl').style.display='';
  }catch(e){ $('marketsLoad').textContent = "markets unavailable ("+e.message+")"; }
}

async function loadTreasuries(){
  try{
    const d = await cgFetch('/api/cg/treasuries');
    $('treasTotals').innerHTML =
      `<div class="chip"><div class="k">Total Holdings</div><div class="v mono">${f0(d.total_holdings)} BTC</div></div>
       <div class="chip"><div class="k">Total Value</div><div class="v mono">${compact(d.total_value_usd)}</div></div>
       <div class="chip"><div class="k">Supply Dominance</div><div class="v mono">${d.market_cap_dominance!=null?d.market_cap_dominance.toFixed(2)+"%":"—"}</div></div>`;
    const tb = document.querySelector('#treasTbl tbody'); tb.innerHTML="";
    (d.companies||[]).forEach(c=>{
      tb.insertAdjacentHTML('beforeend', `<tr>
        <td>${c.name||"—"} <span class="muted mono" style="font-size:11px">${c.symbol||""}</span></td>
        <td>${c.country||"—"}</td>
        <td class="mono">${f0(c.holdings)}</td>
        <td class="mono">${compact(c.current_value_usd)}</td>
        <td class="mono">${c.pct_of_supply!=null?c.pct_of_supply.toFixed(3)+"%":"—"}</td></tr>`);
    });
    $('treasLoad').style.display='none'; $('treasTbl').style.display='';
  }catch(e){ $('treasLoad').textContent = "treasuries unavailable ("+e.message+")"; }
}

async function loadNews(){
  try{
    const d = await cgFetch('/api/cg/news');
    const el = $('newsList'); el.innerHTML="";
    (d.articles||[]).forEach(a=>{
      const when = a.published_at ? new Date(a.published_at).toLocaleString() : "";
      el.insertAdjacentHTML('beforeend', `<div class="newsit">
        <div class="h"><a href="${a.url}" target="_blank" rel="noopener">${a.title}</a></div>
        <div class="m">${a.source||""} · ${when}</div>
        ${a.description?`<div class="d">${a.description.replace(/<[^>]*>/g,"")}</div>`:""}</div>`);
    });
    if(!(d.articles||[]).length) el.innerHTML = '<div class="loading">no articles right now</div>';
    $('newsLoad').style.display='none';
  }catch(e){ $('newsLoad').textContent = "news unavailable ("+e.message+")"; }
}

async function loadHistorical(){
  try{
    const d = await cgFetch('/api/cg/chart?days=365');   // daily granularity
    const prices=d.prices||[], mcaps=d.market_caps||[], vols=d.total_volumes||[];
    const tb = document.querySelector('#histTbl tbody'); tb.innerHTML="";
    const n = prices.length;
    const rows = [];
    for(let i=Math.max(0,n-31); i<n; i++){
      const prev = i>0 ? prices[i-1][1] : null;
      rows.push({t:prices[i][0], p:prices[i][1],
        m:(mcaps[i]||[])[1], v:(vols[i]||[])[1],
        chg: prev!=null ? (prices[i][1]-prev)/prev*100 : null});
    }
    rows.reverse().forEach(r=>{
      tb.insertAdjacentHTML('beforeend', `<tr>
        <td>${new Date(r.t).toLocaleDateString([],{year:'numeric',month:'short',day:'numeric'})}</td>
        <td class="mono">${usd(r.p)}</td>
        <td class="mono">${compact(r.m)}</td>
        <td class="mono">${compact(r.v)}</td>
        <td class="mono ${r.chg==null?'muted':(r.chg>=0?'pos':'neg')}">${r.chg==null?"—":(r.chg>=0?"+":"")+r.chg.toFixed(2)+"%"}</td></tr>`);
    });
    $('histLoad').style.display='none'; $('histTbl').style.display='';
  }catch(e){ $('histLoad').textContent = "historical data unavailable ("+e.message+")"; }
}

loadOverview(); loadCgChart();
setInterval(loadOverview, 60000);
setInterval(()=>{ if(CG.days==="1" && !document.hidden) loadCgChart(); }, 120000);

/* ════════════════════════ Kalshi cockpit (original) ════════════════════════ */
let hist = [], strike = null, closeMs = null;

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
  if(strike!=null){const y=Y(strike); ctx.setLineDash([5,4]); ctx.strokeStyle="#00d181";
    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();ctx.setLineDash([]);}
  ctx.strokeStyle="#f7931a"; ctx.lineWidth=2; ctx.beginPath();
  hist.forEach((p,i)=>{const x=X(p.t),y=Y(p.spot); i?ctx.lineTo(x,y):ctx.moveTo(x,y);}); ctx.stroke();
  const last=hist[hist.length-1]; ctx.fillStyle = strike!=null && last.spot>=strike ? "#00d181":"#ff4d57";
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
window.addEventListener('resize',()=>{drawChart(); drawCgChart();});
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

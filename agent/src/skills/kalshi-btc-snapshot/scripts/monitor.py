#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kalshi BTC real-time monitor (polling engine).

Drives the snapshot on a fast loop and emits a stream of *changes* and
*anomalies* for the live cockpit — no real money, monitoring only.

Transport: REST polling (default 2s), which needs no credentials and is ample
for 15-minute markets. A WebSocket client (authenticated, tick-level) is the
optional upgrade; this engine's output shape is designed to be fed by either.

Usage:
    python monitor.py --series KXBTC15M                 # pretty console
    python monitor.py --series KXBTC15M --ndjson        # one JSON event/line
    python monitor.py --interval 1.5 --duration 300     # 1.5s ticks, stop after 5m

Each tick emits events: WINDOW_OPEN, EXPIRING, TAPE_IMBALANCE, LARGE_PRINT,
IMPLIED_MOVE, EDGE_SUSPECT, plus a per-market state line. NDJSON mode is what
the FastAPI SSE endpoint will relay to the browser.

Exit: Ctrl+C (clean).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Reuse the validated snapshot logic from the sibling module.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import snapshot  # noqa: E402

# Anomaly thresholds (tunable).
LARGE_PRINT = 100.0      # contracts in a single trade
IMPLIED_MOVE = 0.05      # |Δ implied prob| between ticks
IMBALANCE = 0.70         # |yes-no| / total taker volume
EXPIRING_MIN = 2.0       # minutes-to-close warning


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tape_flow(trades: list[dict[str, Any]], since_ts: Optional[str]) -> dict[str, Any]:
    """Aggregate taker flow from trades newer than since_ts."""
    yes_vol = no_vol = 0.0
    largest = 0.0
    newest_ts = since_ts
    for t in trades:
        ts = t.get("ts")
        if since_ts is not None and ts is not None and ts <= since_ts:
            continue
        cnt = t.get("count") or 0.0
        if t.get("taker_side") == "yes":
            yes_vol += cnt
        elif t.get("taker_side") == "no":
            no_vol += cnt
        largest = max(largest, cnt)
        if newest_ts is None or (ts and ts > newest_ts):
            newest_ts = ts
    total = yes_vol + no_vol
    imbalance = (yes_vol - no_vol) / total if total else 0.0
    return {
        "yes_vol": round(yes_vol, 2), "no_vol": round(no_vol, 2),
        "imbalance": round(imbalance, 3), "largest": round(largest, 2),
        "newest_ts": newest_ts,
    }


def diff_tick(snap: dict[str, Any], state: dict[str, dict]) -> list[dict[str, Any]]:
    """Compare this snapshot to prior state; return a list of events."""
    events: list[dict[str, Any]] = []
    for m in snap["markets"]:
        tkr = m["ticker"]
        prev = state.get(tkr, {})
        trades = m.get("recent_trades", []) or []
        flow = _tape_flow(trades, prev.get("newest_ts"))

        if tkr not in state:
            events.append({"type": "WINDOW_OPEN", "ticker": tkr,
                           "strike": m["floor_strike"], "implied": m["implied_prob"]})

        mins = m.get("minutes_to_close")
        if mins is not None and mins <= EXPIRING_MIN:
            events.append({"type": "EXPIRING", "ticker": tkr, "minutes": mins})

        prev_impl = prev.get("implied")
        if prev_impl is not None and m["implied_prob"] is not None:
            d = m["implied_prob"] - prev_impl
            if abs(d) >= IMPLIED_MOVE:
                events.append({"type": "IMPLIED_MOVE", "ticker": tkr,
                               "delta": round(d, 4), "implied": m["implied_prob"]})

        if flow["largest"] >= LARGE_PRINT:
            events.append({"type": "LARGE_PRINT", "ticker": tkr,
                           "size": flow["largest"], "imbalance": flow["imbalance"]})
        if (flow["yes_vol"] + flow["no_vol"]) > 0 and abs(flow["imbalance"]) >= IMBALANCE:
            events.append({"type": "TAPE_IMBALANCE", "ticker": tkr,
                           "imbalance": flow["imbalance"],
                           "side": "yes" if flow["imbalance"] > 0 else "no"})

        if m.get("edge_reliability") == "suspect":
            events.append({"type": "EDGE_SUSPECT", "ticker": tkr,
                           "edge": m["edge"], "warning": m.get("warning")})

        state[tkr] = {"implied": m["implied_prob"], "newest_ts": flow["newest_ts"]}
    return events


def _print_console(snap: dict[str, Any], events: list[dict[str, Any]]) -> None:
    ts = snap["generated_at"][11:19]
    line = (f"[{ts}] spot ${snap['btc_spot']:,.0f}  "
            f"live={snap.get('live_market_count', '?')}")
    print(line)
    for m in snap["markets"]:
        impl = f"{m['implied_prob']*100:.0f}%" if m["implied_prob"] is not None else "-"
        model = f"{m['model_prob']*100:.0f}%" if m["model_prob"] is not None else "-"
        edge = f"{m['edge']*100:+.0f}%" if m["edge"] is not None else "-"
        mins = f"{m['minutes_to_close']:.1f}m" if m["minutes_to_close"] is not None else "-"
        print(f"    {m['ticker']:<24} {mins:>6}  impl {impl:>4}  model {model:>4}  "
              f"edge {edge:>5} [{m.get('edge_reliability','')}]")
    for e in events:
        print(f"    » {e['type']}: " + ", ".join(f"{k}={v}" for k, v in e.items() if k != "type"))


def run(series: str, interval: float, duration: Optional[float], ndjson: bool) -> int:
    state: dict[str, dict] = {}
    start = time.monotonic()
    while True:
        try:
            snap = snapshot.build_snapshot(series, with_trades=True)
        except Exception as exc:  # noqa: BLE001 - keep the loop alive
            print(json.dumps({"type": "ERROR", "ts": _now(), "error": str(exc)})
                  if ndjson else f"[error] {exc}", file=sys.stderr, flush=True)
            time.sleep(interval)
            continue

        events = diff_tick(snap, state)
        if ndjson:
            print(json.dumps({"type": "TICK", "ts": snap["generated_at"],
                              "spot": snap["btc_spot"], "markets": snap["markets"],
                              "events": events}), flush=True)
        else:
            _print_console(snap, events)

        if duration is not None and (time.monotonic() - start) >= duration:
            return 0
        time.sleep(interval)


def main() -> int:
    p = argparse.ArgumentParser(description="Kalshi BTC real-time monitor (polling)")
    p.add_argument("--series", default="KXBTC15M")
    p.add_argument("--interval", type=float, default=2.0, help="Seconds between polls")
    p.add_argument("--duration", type=float, default=None, help="Stop after N seconds")
    p.add_argument("--ndjson", action="store_true", help="Emit one JSON event per line")
    args = p.parse_args()
    try:
        return run(args.series, args.interval, args.duration, args.ndjson)
    except KeyboardInterrupt:
        print("\n[monitor] stopped.", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

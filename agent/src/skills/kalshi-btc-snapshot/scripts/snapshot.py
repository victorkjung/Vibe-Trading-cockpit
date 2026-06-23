#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kalshi BTC 15-minute market snapshot.

Headless, VPS-friendly snapshot tool for Kalshi's short-horizon Bitcoin
price-bracket markets. For each open bracket in the requested series it joins:

  * Kalshi quote  -> implied probability (from yes bid/ask mid, in cents)
  * BTC spot      -> live price (OKX public ticker)
  * realized vol  -> short-horizon sigma (OKX 1m candles)
  * model         -> driftless log-normal probability BTC settles in the bracket
  * edge          -> model_prob - implied_prob (the tradable signal)

Market-data endpoints used here are PUBLIC, so no Kalshi auth is required for a
snapshot. Trading/portfolio would need a signed API key (see SKILL.md).

Usage:
    python snapshot.py --series KXBTCD --top 8
    python snapshot.py --series KXBTC --json          # machine-readable only
    python snapshot.py --min-edge 0.05               # only show |edge| >= 5%

Exit codes: 0 = ok, 1 = no open markets found, 2 = upstream/data error.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional

import requests

KALSHI_BASE = os.environ.get(
    "KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2"
)
OKX_BASE = os.environ.get("OKX_API_BASE", "https://www.okx.com/api/v5")
HTTP_TIMEOUT = float(os.environ.get("SNAPSHOT_HTTP_TIMEOUT", "10"))

YEAR_MINUTES = 365.0 * 24.0 * 60.0


# --------------------------------------------------------------------------- #
# Math helpers
# --------------------------------------------------------------------------- #
def _norm_cdf(x: float) -> float:
    """Standard normal CDF via the error function (no scipy dependency)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def lognormal_prob_between(
    spot: float, sigma_annual: float, minutes_to_expiry: float,
    floor_strike: Optional[float], cap_strike: Optional[float],
) -> Optional[float]:
    """Driftless log-normal probability that BTC settles inside a bracket.

    Models ln(S_T/S0) ~ N(-0.5*sigma^2*T, sigma^2*T) (martingale, zero rate)
    which is a sensible 15-minute approximation. Returns None if the inputs are
    degenerate (zero time/vol).

    Args:
        spot: Current BTC spot price.
        sigma_annual: Annualized volatility (e.g. 0.6 = 60%).
        minutes_to_expiry: Minutes until the market settles.
        floor_strike: Lower bound of the bracket, or None for "less than cap".
        cap_strike: Upper bound of the bracket, or None for "greater than floor".

    Returns:
        Probability in [0, 1], or None if it cannot be computed.
    """
    t_years = minutes_to_expiry / YEAR_MINUTES
    if spot <= 0 or sigma_annual <= 0 or t_years <= 0:
        return None
    vol = sigma_annual * math.sqrt(t_years)
    mu = -0.5 * vol * vol

    def p_below(strike: Optional[float]) -> float:
        if strike is None:
            return 1.0  # +inf upper bound
        if strike <= 0:
            return 0.0
        d = (math.log(strike / spot) - mu) / vol
        return _norm_cdf(d)

    if floor_strike is None and cap_strike is None:
        return None
    return max(0.0, min(1.0, p_below(cap_strike) - p_below(floor_strike)))


# --------------------------------------------------------------------------- #
# Data fetchers
# --------------------------------------------------------------------------- #
def fetch_btc_spot() -> float:
    """Live BTC-USDT spot price from the OKX public ticker."""
    resp = requests.get(
        f"{OKX_BASE}/market/ticker", params={"instId": "BTC-USDT"}, timeout=HTTP_TIMEOUT
    )
    resp.raise_for_status()
    return float(resp.json()["data"][0]["last"])


def fetch_realized_vol(bars: int = 90) -> float:
    """Annualized realized volatility from recent OKX 1-minute candles.

    Args:
        bars: Number of 1m candles to use for the estimate.

    Returns:
        Annualized volatility (e.g. 0.55 = 55%). Falls back to 0.6 on error.
    """
    try:
        resp = requests.get(
            f"{OKX_BASE}/market/candles",
            params={"instId": "BTC-USDT", "bar": "1m", "limit": str(bars)},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        closes = [float(c[4]) for c in resp.json()["data"]]
        closes.reverse()
        if len(closes) < 5:
            return 0.6
        rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
        n = len(rets)
        mean = sum(rets) / n
        var = sum((r - mean) ** 2 for r in rets) / (n - 1)
        sigma_1m = math.sqrt(var)
        return sigma_1m * math.sqrt(YEAR_MINUTES)
    except Exception:
        return 0.6  # conservative default if the vol feed is unavailable


def fetch_kalshi_markets(series_ticker: str) -> list[dict[str, Any]]:
    """All open markets for a Kalshi series (public market-data endpoint).

    Args:
        series_ticker: e.g. "KXBTCD" / "KXBTC".

    Returns:
        List of Kalshi market dicts with status == "active".
    """
    markets: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    headers = _kalshi_auth_headers()
    while True:
        params: dict[str, str] = {"series_ticker": series_ticker, "status": "open", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(
            f"{KALSHI_BASE}/markets", params=params, headers=headers, timeout=HTTP_TIMEOUT
        )
        resp.raise_for_status()
        payload = resp.json()
        markets.extend(payload.get("markets", []))
        cursor = payload.get("cursor")
        if not cursor:
            break
    return markets


def _kalshi_auth_headers() -> dict[str, str]:
    """Optional bearer header if a static token is supplied via env.

    Market data is public; signed RSA auth (for trading) is intentionally out of
    scope for the snapshot. KALSHI_BEARER_TOKEN, if present, is passed through.
    """
    token = os.environ.get("KALSHI_BEARER_TOKEN")
    return {"Authorization": f"Bearer {token}"} if token else {}


# --------------------------------------------------------------------------- #
# Snapshot assembly
# --------------------------------------------------------------------------- #
def _strikes(m: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Extract (floor, cap) strikes, honoring strike_type semantics."""
    floor = m.get("floor_strike")
    cap = m.get("cap_strike")
    stype = (m.get("strike_type") or "").lower()
    floor = float(floor) if floor is not None else None
    cap = float(cap) if cap is not None else None
    if stype in ("greater", "greater_or_equal"):
        cap = None
    elif stype in ("less", "less_or_equal"):
        floor = None
    return floor, cap


def _minutes_to_close(m: dict[str, Any]) -> Optional[float]:
    """Minutes until the market closes/settles, from close_time/expiration_time."""
    ts = m.get("close_time") or m.get("expiration_time")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    except Exception:
        return None


def build_snapshot(series_ticker: str) -> dict[str, Any]:
    """Assemble the full snapshot payload for a series."""
    spot = fetch_btc_spot()
    sigma = fetch_realized_vol()
    markets = fetch_kalshi_markets(series_ticker)

    rows: list[dict[str, Any]] = []
    for m in markets:
        floor, cap = _strikes(m)
        mins = _minutes_to_close(m)
        yes_bid = m.get("yes_bid")
        yes_ask = m.get("yes_ask")
        # Implied probability from the yes bid/ask mid (cents -> [0,1]).
        implied = None
        if yes_bid is not None and yes_ask is not None and (yes_bid or yes_ask):
            implied = (yes_bid + yes_ask) / 2.0 / 100.0
        elif m.get("last_price"):
            implied = m["last_price"] / 100.0

        model = (
            lognormal_prob_between(spot, sigma, mins, floor, cap)
            if mins is not None else None
        )
        edge = (model - implied) if (model is not None and implied is not None) else None

        rows.append({
            "ticker": m.get("ticker"),
            "subtitle": m.get("yes_sub_title") or m.get("subtitle"),
            "floor_strike": floor,
            "cap_strike": cap,
            "minutes_to_close": round(mins, 1) if mins is not None else None,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "implied_prob": round(implied, 4) if implied is not None else None,
            "model_prob": round(model, 4) if model is not None else None,
            "edge": round(edge, 4) if edge is not None else None,
            "volume": m.get("volume"),
            "open_interest": m.get("open_interest"),
        })

    rows.sort(key=lambda r: (abs(r["edge"]) if r["edge"] is not None else -1), reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "series_ticker": series_ticker,
        "btc_spot": spot,
        "realized_vol_annual": round(sigma, 4),
        "market_count": len(rows),
        "markets": rows,
    }


def _print_table(snap: dict[str, Any], top: int, min_edge: float) -> None:
    """Human-readable snapshot table for terminal / log output."""
    print(f"\n=== Kalshi BTC Snapshot  [{snap['series_ticker']}]  {snap['generated_at']} ===")
    print(f"BTC spot: ${snap['btc_spot']:,.0f}   realized vol (annual): {snap['realized_vol_annual'] * 100:.1f}%   markets: {snap['market_count']}")
    rows = [r for r in snap["markets"] if r["edge"] is None or abs(r["edge"]) >= min_edge][:top]
    if not rows:
        print("(no markets meet the edge threshold)")
        return
    hdr = f"{'ticker':<22}{'bracket':<22}{'min':>5}{'impl':>7}{'model':>7}{'edge':>7}{'OI':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        bracket = f"{r['floor_strike'] or '-'}–{r['cap_strike'] or '+'}"
        impl = f"{r['implied_prob'] * 100:.0f}%" if r["implied_prob"] is not None else "-"
        model = f"{r['model_prob'] * 100:.0f}%" if r["model_prob"] is not None else "-"
        edge = f"{r['edge'] * 100:+.0f}%" if r["edge"] is not None else "-"
        mins = f"{r['minutes_to_close']:.0f}" if r["minutes_to_close"] is not None else "-"
        print(f"{(r['ticker'] or '')[:21]:<22}{bracket[:21]:<22}{mins:>5}{impl:>7}{model:>7}{edge:>7}{str(r['open_interest'] or '-'):>8}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kalshi BTC 15-minute market snapshot")
    parser.add_argument("--series", default=os.environ.get("KALSHI_BTC_SERIES", "KXBTC15M"),
                        help="Kalshi series ticker. KXBTC15M = 15-min up/down (default); "
                             "KXBTCD = hourly above/below strike ladder; KXBTC = longer-dated ranges.")
    parser.add_argument("--top", type=int, default=10, help="Max rows to print in table mode")
    parser.add_argument("--min-edge", type=float, default=0.0, help="Only show |edge| >= this (0-1)")
    parser.add_argument("--json", action="store_true", help="Emit JSON only (machine-readable)")
    args = parser.parse_args()

    try:
        snap = build_snapshot(args.series)
    except requests.HTTPError as exc:
        print(f"[snapshot] HTTP error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - headless tool: report and exit non-zero
        print(f"[snapshot] error: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(snap, indent=2))
    else:
        _print_table(snap, args.top, args.min_edge)

    return 0 if snap["market_count"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

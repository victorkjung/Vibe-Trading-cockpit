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
def _median(xs: list[float]) -> Optional[float]:
    s = sorted(xs)
    n = len(s)
    if not n:
        return None
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def fetch_btc_spot() -> float:
    """Live BTC spot, approximating Kalshi's CF Benchmarks BRTI settlement index.

    Kalshi BTC markets settle on the USD BRTI/BRRNY index (multi-exchange), not a
    single venue. To track the "NOW" Kalshi shows — and to compare against the
    USD strike on the same scale — take the median of USD spot from BRTI
    constituents (Coinbase / Kraken / Bitstamp). Falls back to OKX BTC-USDT only
    if all USD sources are unreachable (note: USDT carries a small basis vs USD).
    """
    t = min(HTTP_TIMEOUT, 4.0)
    prices: list[float] = []
    sources = [
        ("https://api.exchange.coinbase.com/products/BTC-USD/ticker",
         lambda j: float(j["price"])),
        ("https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
         lambda j: float(next(iter(j["result"].values()))["c"][0])),
        ("https://www.bitstamp.net/api/v2/ticker/btcusd/",
         lambda j: float(j["last"])),
    ]
    for url, parse in sources:
        try:
            r = requests.get(url, timeout=t, headers={"User-Agent": "kalshi-snapshot"})
            r.raise_for_status()
            prices.append(parse(r.json()))
        except Exception:
            continue
    med = _median(prices)
    if med is not None:
        return med
    # Fallback: OKX USDT (single venue, small USD basis).
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


def fetch_recent_trades(ticker: str, limit: int = 20) -> list[dict[str, Any]]:
    """Recent executed trades for a market (public /markets/trades endpoint).

    This is the actual trade *flow* (prints), distinct from the current quote.
    For continuous real-time flow use the Kalshi WebSocket `trade` channel; this
    REST call is a point-in-time pull of the most recent fills.

    Args:
        ticker: Full market ticker, e.g. "KXBTC15M-26JUN231530-30".
        limit: Max trades to return (most recent first).

    Returns:
        List of {price, count, taker_side, ts} dicts (newest first).
    """
    try:
        resp = requests.get(
            f"{KALSHI_BASE}/markets/trades",
            params={"ticker": ticker, "limit": str(limit)},
            headers=_kalshi_auth_headers(), timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        out: list[dict[str, Any]] = []
        for t in resp.json().get("trades", []):
            price = t.get("yes_price_dollars")
            price = float(price) if price is not None else (
                t.get("yes_price") / 100.0 if t.get("yes_price") is not None else None
            )
            out.append({
                "price": price,
                "count": _num(t, "count_fp", "count"),
                "taker_side": t.get("taker_side"),
                "ts": t.get("created_time"),
            })
        return out
    except Exception:
        return []


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
    """Minutes until the market settles.

    Uses close_time (the 15-min window close); expiration_time is intentionally
    *not* a fallback for KXBTC15M — it points a week out, not at settlement.
    """
    ts = m.get("close_time") or m.get("expected_expiration_time")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    except Exception:
        return None


def _price(m: dict[str, Any], base: str) -> Optional[float]:
    """Read a price as a probability in [0, 1].

    Kalshi returns dollar-denominated strings (e.g. yes_bid_dollars="0.0020",
    already on a 0-1 scale). Older/cent responses expose integer fields
    (e.g. yes_bid in cents) gated by response_price_units. Prefer dollars.
    """
    d = m.get(f"{base}_dollars")
    if d is not None:
        try:
            return float(d)
        except (TypeError, ValueError):
            return None
    c = m.get(base)
    if c is not None:
        try:
            return float(c) / 100.0
        except (TypeError, ValueError):
            return None
    return None


def _num(m: dict[str, Any], *keys: str) -> Optional[float]:
    """First parseable numeric among keys (handles _fp float-strings)."""
    for k in keys:
        v = m.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _edge_reliability(
    model: Optional[float], implied: Optional[float],
    mins: Optional[float], open_interest: Optional[float],
) -> tuple[str, Optional[str]]:
    """Judge whether an edge is trustworthy or a model artifact.

    The driftless log-normal model is unreliable for short-horizon directional
    markets: near at-the-money it collapses to ~50% and ignores momentum, so it
    routinely disagrees with a deep, liquid book that is the better estimate.
    This gate downgrades those cases instead of advertising phantom edges.

    Returns:
        (reliability, warning) where reliability is "ok" | "low" | "suspect".
    """
    if model is None or implied is None:
        return "low", "no model (expired or zero time/vol)"
    edge = abs(model - implied)
    if mins is not None and mins < 2:
        return "low", "under 2 min to close — settlement-average noise dominates"
    # Big disagreement with a deep book: trust the book, not the model.
    if edge > 0.15 and (open_interest or 0) > 50000:
        return "suspect", "deep liquid book disagrees by >15pts — likely model error, not edge"
    # Near-ATM coin flip: model number is mostly an artifact of the vol input.
    if 0.4 <= model <= 0.6:
        return "low", "model near 50% (at-the-money) — driftless model unreliable here"
    # Near the 0/100 boundary the model under-prices tail/jump risk vs the book.
    if (implied >= 0.85 or implied <= 0.15) and edge >= 0.04:
        return "low", "near-boundary — model under-prices tail/jump risk into close"
    return "ok", None


def build_snapshot(
    series_ticker: str, *, include_expired: bool = False, with_trades: bool = False,
) -> dict[str, Any]:
    """Assemble the full snapshot payload for a series.

    Args:
        series_ticker: Kalshi series (e.g. KXBTC15M).
        include_expired: Keep windows already past close_time. Kalshi leaves a
            just-closed market `active` during settlement, so by default these
            stale windows (minutes_to_close <= 0) are dropped.
        with_trades: Attach recent executed trades (real trade flow) per market.
    """
    spot = fetch_btc_spot()
    sigma = fetch_realized_vol()
    markets = fetch_kalshi_markets(series_ticker)

    rows: list[dict[str, Any]] = []
    for m in markets:
        floor, cap = _strikes(m)
        mins = _minutes_to_close(m)
        yes_bid = _price(m, "yes_bid")
        yes_ask = _price(m, "yes_ask")
        # Implied probability from the yes bid/ask mid (dollar prices are 0-1).
        implied = None
        if yes_bid is not None and yes_ask is not None and (yes_bid or yes_ask):
            implied = (yes_bid + yes_ask) / 2.0
        else:
            implied = _price(m, "last_price")

        model = (
            lognormal_prob_between(spot, sigma, mins, floor, cap)
            if mins is not None else None
        )
        edge = (model - implied) if (model is not None and implied is not None) else None

        expired = mins is not None and mins <= 0
        if expired and not include_expired:
            continue  # Kalshi keeps just-closed windows "active" during settlement

        oi = _num(m, "open_interest_fp", "open_interest")
        reliability, warning = _edge_reliability(model, implied, mins, oi)

        row = {
            "ticker": m.get("ticker"),
            "subtitle": m.get("yes_sub_title") or m.get("subtitle"),
            "floor_strike": floor,
            "cap_strike": cap,
            "minutes_to_close": round(mins, 1) if mins is not None else None,
            "expired": expired,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "implied_prob": round(implied, 4) if implied is not None else None,
            "model_prob": round(model, 4) if model is not None else None,
            "edge": round(edge, 4) if edge is not None else None,
            "edge_reliability": reliability,
            "warning": warning,
            "volume": _num(m, "volume_fp", "volume"),
            "open_interest": oi,
        }
        if with_trades and m.get("ticker"):
            row["recent_trades"] = fetch_recent_trades(m["ticker"])
        rows.append(row)

    rows.sort(key=lambda r: (abs(r["edge"]) if r["edge"] is not None else -1), reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "series_ticker": series_ticker,
        "btc_spot": spot,
        "realized_vol_annual": round(sigma, 4),
        "market_count": len(rows),
        "live_market_count": sum(1 for r in rows if not r["expired"]),
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
    hdr = f"{'ticker':<22}{'bracket':<22}{'min':>5}{'impl':>7}{'model':>7}{'edge':>7}  {'edge?':<8}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        bracket = f"{r['floor_strike'] or '-'}–{r['cap_strike'] or '+'}"
        impl = f"{r['implied_prob'] * 100:.0f}%" if r["implied_prob"] is not None else "-"
        model = f"{r['model_prob'] * 100:.0f}%" if r["model_prob"] is not None else "-"
        edge = f"{r['edge'] * 100:+.0f}%" if r["edge"] is not None else "-"
        mins = f"{r['minutes_to_close']:.0f}" if r["minutes_to_close"] is not None else "-"
        rel = r.get("edge_reliability", "")
        print(f"{(r['ticker'] or '')[:21]:<22}{bracket[:21]:<22}{mins:>5}{impl:>7}{model:>7}{edge:>7}  {rel:<8}")
        if r.get("warning"):
            print(f"    ⚠ {r['warning']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kalshi BTC 15-minute market snapshot")
    parser.add_argument("--series", default=os.environ.get("KALSHI_BTC_SERIES", "KXBTC15M"),
                        help="Kalshi series ticker. KXBTC15M = 15-min up/down (default); "
                             "KXBTCD = hourly above/below strike ladder; KXBTC = longer-dated ranges.")
    parser.add_argument("--top", type=int, default=10, help="Max rows to print in table mode")
    parser.add_argument("--min-edge", type=float, default=0.0, help="Only show |edge| >= this (0-1)")
    parser.add_argument("--json", action="store_true", help="Emit JSON only (machine-readable)")
    parser.add_argument("--trades", action="store_true", help="Attach recent executed trades per market")
    parser.add_argument("--include-expired", action="store_true",
                        help="Keep windows already past close (default: drop stale settling windows)")
    args = parser.parse_args()

    try:
        snap = build_snapshot(args.series, include_expired=args.include_expired, with_trades=args.trades)
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

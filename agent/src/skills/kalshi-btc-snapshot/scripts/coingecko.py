#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CoinGecko data layer for the Kalshi BTC cockpit.

Server-side proxy + cache for the CoinGecko public API (free tier). All
browsers share one cache, so N open dashboards cost the same as one. The free
tier is ~5-15 req/min per IP, so every fetch is:

  * TTL-cached per endpoint (tuned to how fast the data actually moves)
  * globally throttled (min spacing between upstream calls)
  * stale-on-error: an expired cache entry is served if upstream 429s/fails

Optional env:
  COINGECKO_API_KEY   demo or pro key (raises rate limits)
  COINGECKO_API_TIER  "demo" (default when key set) or "pro"

News: CoinGecko's /news endpoint is Pro-only. With a pro key we use it;
otherwise we fall back to free Bitcoin-focused RSS feeds (no extra deps).
"""

from __future__ import annotations

import os
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional

import requests

CG_KEY = os.environ.get("COINGECKO_API_KEY", "").strip()
CG_TIER = os.environ.get("COINGECKO_API_TIER", "demo" if CG_KEY else "").strip().lower()
CG_BASE = (
    "https://pro-api.coingecko.com/api/v3"
    if CG_TIER == "pro"
    else "https://api.coingecko.com/api/v3"
)
HTTP_TIMEOUT = float(os.environ.get("SNAPSHOT_HTTP_TIMEOUT", "10"))
MIN_CALL_SPACING = float(os.environ.get("COINGECKO_MIN_SPACING", "2.5"))  # seconds

_THROTTLE_LOCK = threading.Lock()
_LAST_CALL = 0.0

_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, Any]] = {}  # key -> (fetched_at, value)


def _headers() -> dict[str, str]:
    h = {"User-Agent": "kalshi-btc-cockpit", "Accept": "application/json"}
    if CG_KEY:
        h["x-cg-pro-api-key" if CG_TIER == "pro" else "x-cg-demo-api-key"] = CG_KEY
    return h


def _get(path: str, params: Optional[dict] = None) -> Any:
    """Rate-spaced GET against CoinGecko. Raises on HTTP error."""
    global _LAST_CALL
    with _THROTTLE_LOCK:
        wait = MIN_CALL_SPACING - (time.monotonic() - _LAST_CALL)
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL = time.monotonic()
    r = requests.get(f"{CG_BASE}/{path}", params=params or {},
                     headers=_headers(), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _cached(key: str, ttl: float, fetch: Callable[[], Any]) -> Any:
    """TTL cache with stale-on-error: never drop data just because upstream hiccuped."""
    now = time.monotonic()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        val = fetch()
    except Exception:
        if hit is not None:  # serve stale rather than erroring the dashboard
            return hit[1]
        raise
    with _CACHE_LOCK:
        _CACHE[key] = (now, val)
    return val


# --------------------------------------------------------------------------- #
# Overview (price, market stats, change matrix)
# --------------------------------------------------------------------------- #
def overview() -> dict[str, Any]:
    def fetch() -> dict[str, Any]:
        j = _get("coins/bitcoin", {
            "localization": "false", "tickers": "false", "market_data": "true",
            "community_data": "false", "developer_data": "false", "sparkline": "false",
        })
        md = j.get("market_data", {})
        usd = lambda field: (md.get(field) or {}).get("usd")  # noqa: E731
        return {
            "name": j.get("name"), "symbol": (j.get("symbol") or "").upper(),
            "rank": j.get("market_cap_rank"),
            "price": usd("current_price"),
            "high_24h": usd("high_24h"), "low_24h": usd("low_24h"),
            "market_cap": usd("market_cap"), "fdv": usd("fully_diluted_valuation"),
            "volume_24h": usd("total_volume"),
            "circulating": md.get("circulating_supply"),
            "total_supply": md.get("total_supply"), "max_supply": md.get("max_supply"),
            "ath": usd("ath"), "ath_change_pct": (md.get("ath_change_percentage") or {}).get("usd"),
            "ath_date": (md.get("ath_date") or {}).get("usd"),
            "atl": usd("atl"),
            "sentiment_up_pct": j.get("sentiment_votes_up_percentage"),
            "changes": {
                "1h": (md.get("price_change_percentage_1h_in_currency") or {}).get("usd"),
                "24h": md.get("price_change_percentage_24h"),
                "7d": md.get("price_change_percentage_7d"),
                "14d": md.get("price_change_percentage_14d"),
                "30d": md.get("price_change_percentage_30d"),
                "1y": md.get("price_change_percentage_1y"),
            },
            "last_updated": md.get("last_updated") or j.get("last_updated"),
        }
    return _cached("overview", 60, fetch)


# --------------------------------------------------------------------------- #
# Historical chart (price / market cap / volume series)
# --------------------------------------------------------------------------- #
_CHART_DAYS = {"1": 120.0, "7": 600.0, "30": 900.0, "90": 1800.0, "365": 3600.0, "max": 21600.0}


def chart(days: str) -> dict[str, Any]:
    days = days if days in _CHART_DAYS else "1"

    def fetch() -> dict[str, Any]:
        j = _get("coins/bitcoin/market_chart", {"vs_currency": "usd", "days": days})
        return {
            "days": days,
            "prices": j.get("prices", []),
            "market_caps": j.get("market_caps", []),
            "total_volumes": j.get("total_volumes", []),
        }
    return _cached(f"chart:{days}", _CHART_DAYS[days], fetch)


# --------------------------------------------------------------------------- #
# Markets (top exchange tickers for BTC)
# --------------------------------------------------------------------------- #
def tickers() -> dict[str, Any]:
    def fetch() -> dict[str, Any]:
        j = _get("coins/bitcoin/tickers", {
            "per_page": 50, "order": "volume_desc", "depth": "false",
        })
        rows = []
        for t in j.get("tickers", [])[:50]:
            rows.append({
                "exchange": (t.get("market") or {}).get("name"),
                "base": t.get("base"), "target": t.get("target"),
                "price_usd": (t.get("converted_last") or {}).get("usd"),
                "volume_usd": (t.get("converted_volume") or {}).get("usd"),
                "spread_pct": t.get("bid_ask_spread_percentage"),
                "trust": t.get("trust_score"),
                "url": t.get("trade_url"),
            })
        return {"tickers": rows}
    return _cached("tickers", 300, fetch)


# --------------------------------------------------------------------------- #
# Treasuries (public companies holding BTC)
# --------------------------------------------------------------------------- #
def treasuries() -> dict[str, Any]:
    def fetch() -> dict[str, Any]:
        j = _get("companies/public_treasury/bitcoin")
        return {
            "total_holdings": j.get("total_holdings"),
            "total_value_usd": j.get("total_value_usd"),
            "market_cap_dominance": j.get("market_cap_dominance"),
            "companies": [{
                "name": c.get("name"), "symbol": c.get("symbol"),
                "country": c.get("country"),
                "holdings": c.get("total_holdings"),
                "entry_value_usd": c.get("total_entry_value_usd"),
                "current_value_usd": c.get("total_current_value_usd"),
                "pct_of_supply": c.get("percentage_of_total_supply"),
            } for c in j.get("companies", [])],
        }
    return _cached("treasuries", 3600, fetch)


# --------------------------------------------------------------------------- #
# News (CoinGecko Pro if keyed, else free Bitcoin RSS)
# --------------------------------------------------------------------------- #
_RSS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss/tag/bitcoin"),
    ("Bitcoin Magazine", "https://bitcoinmagazine.com/feed"),
]


def _parse_rss(source: str, xml_text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        ts = None
        if pub:
            try:
                ts = parsedate_to_datetime(pub).astimezone(timezone.utc).isoformat()
            except Exception:
                ts = None
        desc = (item.findtext("description") or "").strip()
        if len(desc) > 240:
            desc = desc[:237] + "…"
        if title and link:
            items.append({"title": title, "url": link, "source": source,
                          "published_at": ts, "description": desc})
    return items


def news() -> dict[str, Any]:
    def fetch() -> dict[str, Any]:
        if CG_TIER == "pro":
            try:
                j = _get("news", {"per_page": 25})
                arts = [{
                    "title": a.get("title"), "url": a.get("url"),
                    "source": a.get("news_site"),
                    "published_at": a.get("updated_at"),
                    "description": (a.get("description") or "")[:240],
                } for a in j.get("data", [])]
                if arts:
                    return {"provider": "coingecko", "articles": arts}
            except Exception:
                pass
        # Free tier: aggregate Bitcoin RSS (plain HTTP, no CoinGecko quota used).
        arts: list[dict[str, Any]] = []
        for source, url in _RSS_FEEDS:
            try:
                r = requests.get(url, timeout=8,
                                 headers={"User-Agent": "kalshi-btc-cockpit"})
                r.raise_for_status()
                arts.extend(_parse_rss(source, r.text)[:10])
            except Exception:
                continue
        arts.sort(key=lambda a: a.get("published_at") or "", reverse=True)
        return {"provider": "rss", "articles": arts[:25]}
    return _cached("news", 900, fetch)

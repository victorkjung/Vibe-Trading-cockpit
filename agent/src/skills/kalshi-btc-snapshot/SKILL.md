---
name: kalshi-btc-snapshot
description: Kalshi BTC short-horizon (15-min / hourly) price-bracket market snapshot. Joins live BTC spot and realized volatility against Kalshi bracket quotes to surface implied probability, model fair value, and tradable edge.
category: crypto
---

# Kalshi BTC 15-Minute Market Snapshot

## Overview

Kalshi lists short-horizon binary markets on the BTC settlement price. Each
market is a YES/NO contract whose YES price in cents is the market's **implied
probability**. The flagship 15-minute market is an **up/down** contract: "will
BTC settle above the strike at the end of this 15-minute window?"

This skill produces a **decision-support snapshot** (research only — it does not
place orders) by joining three feeds:

| Feed | Source | Purpose |
|------|--------|---------|
| BTC spot | OKX public ticker | Current underlying price |
| Realized vol | OKX 1m candles | Short-horizon sigma for the model |
| Kalshi quotes | Kalshi public `/markets` | Implied probability per bracket |

For each open bracket it computes a **model fair value** (driftless log-normal
probability that BTC settles in the bracket) and the **edge = model − implied** —
the signal a desk acts on.

## Deployment note (VPS-primary)

The 15-minute cadence means this is meant to run **headless on the always-on
VPS**, not on a laptop that sleeps. Schedule it (cron / systemd timer) on the VPS
and view results from any browser. Macky / laptops are thin clients only.

## Quick Start

```bash
# On a PEP 668 "externally-managed" host (Debian/Ubuntu), use a venv:
python3 -m venv .venv && . .venv/bin/activate
pip install requests          # only hard dependency

python scripts/snapshot.py --series KXBTC15M --top 8
python scripts/snapshot.py --json            # machine-readable for piping / API
python scripts/snapshot.py --min-edge 0.05   # only markets with |edge| >= 5%
```

> If you hit `error: externally-managed-environment`, that's PEP 668 — use the
> venv above (preferred) or `pip install --break-system-packages requests`.
> Use `python3` if the bare `python` alias isn't on PATH.

Continuous loop on the VPS (every minute):

```bash
watch -n 60 'python scripts/snapshot.py --series KXBTC15M --min-edge 0.04'
```

## Kalshi BTC Market Taxonomy (verified June 2026)

Pick the series that matches your horizon. The snapshot script works for all of
them — only the strike semantics differ.

| Series ticker | Frequency | Structure | Event ticker format | Settlement |
|---------------|-----------|-----------|---------------------|------------|
| **`KXBTC15M`** | **15-min** | **Up/Down** — single strike near the open price; YES = BTC settles *above* strike | `KXBTC15M-{YYMONDDHHMM}` (e.g. `KXBTC15M-26JUN161400`) | Kalshi captured **RTI** reference (avg of 60 prices in the final minute) |
| `KXBTCD` | Hourly | **Above/Below ladder** — ~200 strikes per window, each a "greater than floor" market | `KXBTCD-{YYMONDD}{HH}` (ET hour) | CF Benchmarks **BRRNY** index (1-hour TWAP) |
| `KXBTC` | Daily / longer | **Price ranges** — floor/cap brackets | `KXBTC-{YYMONDD}-{strike}` | BRRNY index |
| `KXBTCY` / `KXBTCMAXY` / `KXBTCMINY` | Yearly | Range / max / min | — | BRRNY index |

Notes that affect the model:
- **15-min markets resolve the winner at 99¢, not 100¢** (1¢ effective fee). The
  snapshot reports edge as a *probability* gap (`model − implied`); for true EV,
  fair YES price ≈ `model_prob × 99¢`.
- `KXBTC15M` strike sits near the prevailing BTC price at open, banded to a round
  number — so the model collapses to `P(S_T > strike)`, which the script computes
  via the "greater" branch (`cap_strike = None`).
- The 15-min reference (RTI) differs slightly from OKX spot used here; treat the
  model as an approximation, sharpest when there's no scheduled catalyst.

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `KALSHI_BTC_SERIES` | `KXBTC15M` | Series ticker (see taxonomy above). **Verify it's live in your account** — query `GET /markets?series_ticker=KXBTC15M` or check the market page URL `kalshi.com/markets/kxbtc15m/...`. |
| `KALSHI_API_BASE` | `https://api.elections.kalshi.com/trade-api/v2` | Kalshi API base |
| `KALSHI_BEARER_TOKEN` | _(unset)_ | Optional pass-through bearer; **not required** for market data |
| `OKX_API_BASE` | `https://www.okx.com/api/v5` | Spot/vol source |
| `SNAPSHOT_HTTP_TIMEOUT` | `10` | Per-request timeout (seconds) |

**Auth:** Kalshi market-data GET endpoints are public, so a snapshot needs **no
credentials**. Live trading / portfolio would require a signed RSA API key — out
of scope for this read-only skill.

## Methodology

1. **Implied probability** — mid of `yes_bid`/`yes_ask` (cents ÷ 100); falls back
   to `last_price` if the book is one-sided.
2. **Model fair value** — `ln(S_T/S₀) ~ N(−½σ²T, σ²T)` (martingale, zero rate; a
   sound 15-minute approximation). Probability BTC lands in `[floor, cap]` is
   `Φ(d_cap) − Φ(d_floor)`, honoring `strike_type` (`between` / `greater` /
   `less`). `T` comes from `close_time`.
3. **σ (annualized)** — realized vol from recent OKX 1m candles, annualized by
   `√(525,600)`. Replace with Kalshi/Deribit IV later for a sharper estimate.
4. **Edge** — `model_prob − implied_prob`. Positive ⇒ market underpricing YES.
   Each row carries `edge_reliability` (`ok` / `low` / `suspect`) + a `warning`.
   The driftless model is unreliable for short directional windows: near ATM it
   collapses to ~50% and ignores momentum, so a large disagreement with a deep,
   liquid book is flagged `suspect` — **trust the book, not the model**. Treat
   `ok` edges in thin/OTM markets as the only candidates, and even then as a
   screen, not a trigger.

## Output Format

```
=== Kalshi BTC Snapshot  [KXBTCD]  2026-06-23T14:31:00+00:00 ===
BTC spot: $104,200   realized vol (annual): 48.3%   markets: 7
ticker                bracket                 min   impl  model   edge      OI
----------------------------------------------------------------------------
KXBTCD-26JUN23-B104   104000–104500            12    41%    52%   +11%    1820
...
```

`--json` emits the full payload (`btc_spot`, `realized_vol_annual`, per-market
`implied_prob` / `model_prob` / `edge` / `open_interest`), ready to feed the
`/kalshi/snapshot` API endpoint (Phase 2) or a cockpit panel.

## Real-Time Monitor

`scripts/monitor.py` polls the snapshot on a fast loop (default 2s — ample for
15-min markets, needs no credentials) and emits **change/anomaly events** for the
live cockpit: `WINDOW_OPEN`, `EXPIRING`, `TAPE_IMBALANCE`, `LARGE_PRINT`,
`IMPLIED_MOVE`, `EDGE_SUSPECT`.

```bash
python scripts/monitor.py --series KXBTC15M            # pretty console
python scripts/monitor.py --series KXBTC15M --ndjson   # one JSON event/line (feeds SSE)
```

`--ndjson` output is the stream the FastAPI SSE endpoint relays to the browser
panel. True tick-level streaming is the optional upgrade: Kalshi's WebSocket
(`wss://.../trade-api/ws/v2`) requires a signed **read-only** API key (no trading
scope), and the engine's event shape is designed to accept it as a drop-in.

## Swarm

For the multi-agent "Palantir desk" view, run the **`kalshi_btc_15m_desk`** swarm
preset (`agent/config/swarm/kalshi_btc_15m_desk.yaml`): a spot/vol analyst, a
Kalshi pricing analyst, and an edge & risk manager that turns the snapshot into a
sized, risk-gated recommendation — streamed live to the SwarmDashboard.

## Notes & Caveats

1. **Research only** — no order execution; consistent with the rest of this repo.
2. **Verify the series ticker** — the single most common failure. List candidates
   with `GET /series?category=Crypto` or check the Kalshi UI.
3. **Realized vol ≠ implied vol** — the model uses backward-looking vol; near
   scheduled catalysts (CPI, FOMC) it will understate true risk. Treat raw edge
   as a screen, not a trade trigger.
4. **Network** — the VPS egress policy must allow `api.elections.kalshi.com` and
   `www.okx.com`.
5. **Thin books** — short-horizon brackets can be illiquid; always sanity-check
   `open_interest` / `volume` before trusting an edge.

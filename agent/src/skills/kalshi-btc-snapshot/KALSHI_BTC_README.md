# Kalshi BTC 15-Minute Monitoring Cockpit

A headless, VPS-friendly **decision-support** toolkit for Kalshi's short-horizon
Bitcoin price-bracket markets. It joins live BTC spot, short-horizon realized
volatility, and Kalshi bracket quotes to surface each market's **implied
probability**, a **model fair value**, and the **edge** between them.

> **Research / monitoring only.** There is no order path anywhere in this
> toolkit — execution stays on the Kalshi app. See [Caveats](#notes--caveats).

## Contents

This application has four parts, layered on a single shared snapshot engine:

| Component | File | What it is |
|-----------|------|------------|
| Snapshot engine + CLI | `scripts/snapshot.py` | Core data join; prints a table or JSON |
| Real-time monitor | `scripts/monitor.py` | Polling loop that emits change/anomaly events |
| Web cockpit | `scripts/serve.py` | Self-contained FastAPI dashboard + SSE stream |
| Swarm desk | `../../../config/swarm/kalshi_btc_15m_desk.yaml` | 3-agent preset that turns a snapshot into a sized, risk-gated call |

The skill manifest (`SKILL.md`) documents how an agent loads and uses these
scripts; this README is the operator's guide to running them directly.

## How it works

For each open bracket in the requested series the engine computes:

1. **Implied probability** — mid of `yes_bid`/`yes_ask` (falls back to
   `last_price` on a one-sided book). Kalshi YES price = the market's implied
   probability.
2. **Model fair value** — a driftless log-normal probability that BTC settles
   in the bracket: `ln(S_T/S₀) ~ N(−½σ²T, σ²T)`, honoring the market's
   `strike_type` (`between` / `greater` / `less`). `T` comes from `close_time`.
3. **Edge** — `model_prob − implied_prob`. Positive ⇒ the market is underpricing
   YES.
4. **Edge reliability** — an `ok` / `low` / `suspect` gate (plus a `warning`)
   that downgrades phantom edges the driftless model produces near at-the-money,
   near the 0/100 boundary, under 2 minutes to close, or when a deep, liquid
   book disagrees sharply. **Trust the book, not the model.**

Data sources:

| Feed | Source | Purpose |
|------|--------|---------|
| BTC spot | Coinbase / Kraken / Bitstamp USD median (OKX USDT fallback) | Tracks Kalshi's CF Benchmarks BRTI settlement index |
| Realized vol | OKX 1m candles, annualized by `√525,600` | Short-horizon σ for the model |
| Kalshi quotes / trades | Kalshi public `/markets` and `/markets/trades` | Implied probability and trade flow per bracket |

Kalshi market-data GET endpoints are **public**, so a snapshot needs **no
credentials**. Live trading would require a signed RSA API key — out of scope.

## Requirements

- Python ≥ 3.11
- `requests` — the only hard dependency for the CLI and monitor
- `fastapi` + `uvicorn` — additionally required for the web cockpit

On a PEP 668 "externally-managed" host (Debian/Ubuntu), use a venv:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install requests              # snapshot + monitor
pip install fastapi uvicorn       # web cockpit
```

> If you hit `error: externally-managed-environment`, that's PEP 668 — use the
> venv above (preferred) or `pip install --break-system-packages requests`.

Run the commands below from this directory
(`agent/src/skills/kalshi-btc-snapshot/`).

## Usage

### 1. Snapshot (one-shot)

```bash
python scripts/snapshot.py --series KXBTC15M --top 8   # human-readable table
python scripts/snapshot.py --json                      # machine-readable payload
python scripts/snapshot.py --min-edge 0.05             # only |edge| >= 5%
python scripts/snapshot.py --trades                    # attach recent trade flow
```

Flags: `--series` (default `KXBTC15M`), `--top` (rows in table mode, default 10),
`--min-edge` (0–1), `--json`, `--trades`, `--include-expired` (keep windows past
close; by default stale settling windows are dropped).

Exit codes: `0` ok · `1` no open markets found · `2` upstream/data error.

Continuous loop on the VPS:

```bash
watch -n 60 'python scripts/snapshot.py --series KXBTC15M --min-edge 0.04'
```

### 2. Real-time monitor

Polls the snapshot on a fast loop (default 2s — ample for 15-min markets, no
credentials needed) and emits change/anomaly events: `WINDOW_OPEN`, `EXPIRING`,
`TAPE_IMBALANCE`, `LARGE_PRINT`, `IMPLIED_MOVE`, `EDGE_SUSPECT`.

```bash
python scripts/monitor.py --series KXBTC15M             # pretty console
python scripts/monitor.py --series KXBTC15M --ndjson    # one JSON event per line
python scripts/monitor.py --interval 1.5 --duration 300 # 1.5s ticks, stop after 5m
```

Flags: `--series`, `--interval` (seconds), `--duration` (stop after N seconds),
`--ndjson`, `--large-print` (single-trade size to flag as `LARGE_PRINT`,
default 500). Exit with `Ctrl+C`.

### 3. Web cockpit

A self-contained dashboard (embedded HTML, no build step) plus an SSE stream of
the monitor — open it from any browser on your mesh.

```bash
python scripts/serve.py --series KXBTC15M --host 0.0.0.0 --port 8787
# then browse to  http://<vps-tailscale-ip>:8787
```

The page shows the live up/down market with an editorial probability chart: the
Kalshi market-implied YES probability in neutral gray versus the VGH model fair
value in a single orange hero accent, with direct endpoint labels, a visible edge
annotation, a declarative live headline, and a muted BTC spot-vs-target context
panel. It also retains reliability, expiry, OI/volume, settled results, and the
scrolling anomaly feed. Every chart includes source/freshness text and has a
375px-first responsive layout.

Endpoints:

| Route | Returns |
|-------|---------|
| `GET /` | Embedded dashboard |
| `GET /api/snapshot` | One-shot JSON snapshot (with trades) |
| `GET /api/history` | Recent ~60 1m BTC closes to seed the chart |
| `GET /api/stream` | `text/event-stream` of monitor ticks (`TICK` + events) |
| `GET /api/results` | Settled-window ledger |
| `GET /api/cg/overview` | CoinGecko BTC market overview (cache 60s) |
| `GET /api/cg/chart?days=` | Price/mcap/volume series (`1\|7\|30\|90\|365\|max`) |
| `GET /api/cg/tickers` | Top BTC exchange tickers (cache 5m) |
| `GET /api/cg/treasuries` | Public-company BTC treasuries (cache 1h) |
| `GET /api/cg/news` | News (Pro `/news` or free RSS; cache 15m) |
| `GET /api/cg/trending` | 24h trending search coins/NFTs/categories (cache 10m; `?force=1` busts) |
| `GET /api/cg/categories` | Category sector rotation (mcap/vol/24h%; `?order=&limit=&force=1`) |
| `GET /api/cg/global` | Global macro: total mcap, volume, BTC/ETH dominance (cache 60s; `?force=1`) |

> Keep it bound to a private (e.g. Tailscale) IP, not a public interface.

### CoinGecko research layer (market intel)

Server-side proxy in `scripts/coingecko.py` so **N open browsers cost the same as one** upstream call:

| Upstream | Cockpit route | Cache TTL | Notes |
|----------|---------------|-----------|--------|
| `GET /search/trending` | `/api/cg/trending` | 10 min | Retail narrative heat detector; CG refreshes ~10m |
| `GET /coins/categories` | `/api/cg/categories` | 5 min | Sector rotation (L1, AI, DePIN, RWA, memes, …) |
| `GET /global` | `/api/cg/global` | 60s | Macro mcap / volume / BTC&ETH dominance |
| `GET /coins/bitcoin` | `/api/cg/overview` | 60s | Price + mcap rail |
| `GET /coins/bitcoin/market_chart` | `/api/cg/chart` | 2–360 min by range | Granularity auto (1d→5m … >90→daily) |
| `GET /coins/bitcoin/tickers` | `/api/cg/tickers` | 5 min | Top 50 by volume |
| `GET /companies/public_treasury/bitcoin` | `/api/cg/treasuries` | 1h | Public cos holdings (free/demo) — **not** tx history |
| Pro `/news` or RSS | `/api/cg/news` | 15 min | `/news` is Pro-only |

**Auth / env (mesh pattern — never commit secrets):**

| Env | Meaning |
|-----|---------|
| `COINGECKO_API_KEY` | Demo or pro key from `~/.vgh.env` (pm2 `--update-env`) |
| `COINGECKO_API_TIER` | `demo` (default when key set) or `pro` |
| `COINGECKO_MIN_SPACING` | Min seconds between upstream GETs (default `2.5`) |

- Demo key → `api.coingecko.com` + header `x-cg-demo-api-key`
- Pro key → `pro-api.coingecko.com` + header `x-cg-pro-api-key`
- Paid-only endpoints deferred: pools megafilter, top holders/traders, **public treasury transaction history** (holdings snapshot is free via Treasuries tab).
- House pattern: **TTL cache + global call spacing + stale-on-error**.

UI tabs: **Global**, **Trending**, **Categories**, Markets, Treasuries, News, Historical. Refresh buttons send `force=1` to bust server cache; open narrative tabs auto-refresh every 5 minutes.

### CoinGecko MCP / agent skill (P1 evaluation)

| Option | Verdict | Notes |
|--------|---------|--------|
| **Cockpit REST proxy (this app)** | **Keep as primary** | Browser-friendly, shared cache, no per-tab API burn, already wired to Kalshi SSE desk |
| **CoinGecko MCP server** | Optional later | Useful for NL agent queries in Hermes/JEFE; **not** a replacement for the dashboard. Adds another process + key surface; free-tier rate limits still apply |
| **CoinGecko agent skill (docs/skills)** | Optional later | Good for one-shot research agents; cockpit should keep deterministic REST endpoints for UI |

Recommendation: ship market intel through `/api/cg/*` first. Revisit MCP only if Hermes needs natural-language pulls beyond the dashboard (can share `COINGECKO_API_KEY` from `.vgh.env`).

### 4. Swarm desk

For the multi-agent desk view, run the **`kalshi_btc_15m_desk`** swarm preset
(`agent/config/swarm/kalshi_btc_15m_desk.yaml`). Three agents run in sequence:

1. **BTC Spot & Volatility Analyst** — establishes spot, σ, expected 15-min
   move, and a regime call.
2. **Kalshi Bracket Pricing Analyst** — runs `snapshot.py --json` and ranks
   brackets by edge.
3. **Edge & Risk Manager** — integrates both into a sized, risk-gated
   recommendation (side, conviction, entry, EV, sizing/liquidity/time/vol gates).

Required template variables: `target` (e.g. `BTC`) and `timeframe` (e.g.
`next 15-minute expiry`).

## Deployment

The 15-minute cadence means this is meant to run **headless on an always-on
VPS**, not a laptop that sleeps. Schedule `snapshot.py` (cron / systemd timer)
or keep `serve.py` running, and treat laptops as thin browser clients. The VPS
egress policy must allow `api.elections.kalshi.com` and `www.okx.com` (plus the
Coinbase/Kraken/Bitstamp spot hosts).

## Configuration

All components read configuration from the environment:

| Env var | Default | Meaning |
|---------|---------|---------|
| `KALSHI_BTC_SERIES` | `KXBTC15M` | Series ticker (see taxonomy in `SKILL.md`). Verify it's live in your account. |
| `KALSHI_API_BASE` | `https://api.elections.kalshi.com/trade-api/v2` | Kalshi API base |
| `KALSHI_BEARER_TOKEN` | _(unset)_ | Optional pass-through bearer; **not required** for market data |
| `OKX_API_BASE` | `https://www.okx.com/api/v5` | Spot/vol fallback source |
| `SNAPSHOT_HTTP_TIMEOUT` | `10` | Per-request timeout (seconds) |
| `KALSHI_MONITOR_INTERVAL` | `2.0` | Web cockpit SSE poll interval (seconds) |
| `KALSHI_MONITOR_PORT` | `8787` | Default web cockpit port |

CLI flags override the corresponding environment defaults.

## Notes & Caveats

1. **Research only** — no order execution, consistent with the rest of this repo.
2. **Verify the series ticker** — the single most common failure. List
   candidates with `GET /series?category=Crypto` or check the Kalshi UI.
3. **Realized vol ≠ implied vol** — the model uses backward-looking vol; near
   scheduled catalysts (CPI, FOMC) it understates true risk. Treat raw edge as a
   screen, not a trigger.
4. **15-min markets resolve the winner at 99¢, not 100¢** (1¢ effective fee);
   the reported edge is a probability gap, so for true EV use
   `fair YES price ≈ model_prob × 99¢`.
5. **Thin books** — short-horizon brackets can be illiquid; always sanity-check
   `open_interest` / `volume` before trusting an edge.

See `SKILL.md` for the full Kalshi BTC market taxonomy, methodology, and the
optional authenticated WebSocket upgrade path.

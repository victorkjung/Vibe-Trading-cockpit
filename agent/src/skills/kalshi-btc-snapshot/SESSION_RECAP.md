# Session Recap — Kalshi BTC 15-Min Cockpit (2026-06-24)

Build log for the Kalshi BTC 15-minute monitoring cockpit, integrated into the
Vibe-Trading repo. Branch: `claude/hermes-kalshi-btc-runbook-rx65m8`.

## Goal
Integrate Kalshi's short-horizon BTC prediction markets into the "Palantir-style"
cockpit. Started as a runbook review (the original local runbook was dropped as
"a bad MVP"); evolved into a live, VPS-hosted, monitoring-only cockpit.

## What was built (commits, oldest → newest)
| Commit | Summary |
|--------|---------|
| `addc548` | Phase 1: `kalshi-btc-snapshot` skill (SKILL.md + snapshot.py) + `kalshi_btc_15m_desk` swarm preset |
| `77f2dc7` | Corrected ticker to **KXBTC15M** (15-min up/down, RTI settlement) — not hourly KXBTCD |
| `624f31d` | Locked parser to live API schema (dollar-string prices, `_fp` vol/OI, `close_time`) |
| `ca82124` | venv / PEP 668 docs |
| `e89652b` | Drop stale post-close windows; add recent-trades feed |
| `03e307b` | **Edge-reliability gate** (ok/low/suspect) after a live +41.6% "edge" proved to be model error |
| `42675d1` | Real-time monitor engine (polling + anomaly events) |
| `682c692` | Self-contained FastAPI web cockpit + tuned anomaly thresholds |
| `430db78` | Redesigned cockpit Kalshi-style (live chart + target line, stat header, Up/Down ¢) |
| `3d095c5` | **BRTI-tracked spot** (Coinbase/Kraken/Bitstamp USD median) so NOW matches Kalshi |
| `624857e` | Chart pre-fill via `/api/history` |
| `9022cbb` | **Auto-roll windows + settlement ledger** (shared poller, `/api/results`, persisted JSONL) |
| `cfb1cc1`, `707069b` | Operator README (committed to git, updated to current state) |

## Architecture / infra
- **VPS-primary** (`srv1393677`, Tailscale `100.87.250.108`): repo at
  `~/projects/Vibe-Trading-cockpit`, venv, running under **pm2** (`kalshi-cockpit`,
  port 8787) with reboot persistence (`pm2 startup` + `pm2 save`).
- **Triggy** (`/home/victor-jung/Vibe-Trading-cockpit`): dev/authoring; Hermes
  agent registered the skill (category `crypto`) + has the swarm preset. Do **not**
  run the live cockpit here (it sleeps).
- **Macky** (`100.126.103.83`): thin browser client only. Earlier was mistakenly
  treated as a file source → Hermes SSH-to-Macky failures; resolved by repointing
  to git.
- **Git = source of truth.** This session (cloud container) can only touch the
  repo; it has no SSH/mesh access to any machine — all live validation was run by
  the operator on the VPS.

## Components (see README.md / SKILL.md)
- `scripts/snapshot.py` — data-join CLI (implied prob, log-normal model, edge, reliability).
- `scripts/monitor.py` — polling loop emitting WINDOW_OPEN/EXPIRING/TAPE_IMBALANCE/LARGE_PRINT/IMPLIED_MOVE/EDGE_SUSPECT/WINDOW_SETTLED.
- `scripts/serve.py` — FastAPI dashboard + SSE + shared poller + window ledger
  (`/`, `/api/snapshot`, `/api/history`, `/api/stream`, `/api/results`).
- `config/swarm/kalshi_btc_15m_desk.yaml` — 3-agent desk (spot/vol → pricing → edge & risk).

## Key learnings
1. **Don't trust model edge over a deep book.** A live ATM window showed model
   48% vs market 6.5% (+41.6% fake edge). The driftless log-normal is unreliable
   near ATM / boundaries / close — hence the reliability gate. The market price
   is the better estimate for short directional windows.
2. **Use the settlement reference.** NOW must track CF Benchmarks BRTI (USD),
   not OKX BTC-USDT (single venue + USDT basis) — fixed the price mismatch and
   the reference-error portion of the edge.
3. **VPS-primary, asymmetric hybrid** beats true split: one authoritative
   runtime (VPS), dev on Triggy, viewers on laptops. Sleeping laptops can't be
   sources or hosts.

## Live status
✅ Cockpit live at `http://100.87.250.108:8787`, validated end-to-end against live
KXBTC15M. Deploy: `git pull && pm2 restart kalshi-cockpit`. Ledger at
`~/.kalshi_cockpit_results.jsonl` / `GET /api/results`.

## Guardrail
**Monitoring only — no execution.** Order placement stays on the Kalshi app.
Live trading would be a separate, gated module (RSA auth, sizing, risk limits,
dry-run, explicit sign-off) — not started.

## Open ideas (not started)
- Browser/sound alerts on high-conviction `ok` edges
- Tick logging to DuckDB for backtesting model + flags
- Authenticated WebSocket (true tick streaming vs 2s poll)
- Multi-window strip; model drift/IV upgrade (low priority — unlikely to beat the book)

"""Editorial summaries for the Kalshi 15-minute cockpit."""

from __future__ import annotations

from typing import Any, Optional


def _pct(value: Optional[float]) -> Optional[int]:
    return None if value is None else round(value * 100)


def editorial_summary(market: dict[str, Any], spot: Optional[float]) -> dict[str, Any]:
    """Turn model-vs-market data into a declarative, human-readable takeaway."""
    implied = market.get("implied_prob")
    model = market.get("model_prob")
    market_pct = _pct(implied)
    model_pct = _pct(model)

    if implied is None or model is None:
        return {
            "headline": "Waiting for a usable 15-minute market quote.",
            "deck": "Market probability and model fair value will appear when both inputs are available.",
            "market_pct": market_pct,
            "model_pct": model_pct,
            "edge_points": None,
        }

    edge_points = round((model - implied) * 100)
    magnitude = abs(edge_points)
    if magnitude <= 2:
        headline = "Market and model are nearly aligned on YES."
    elif edge_points > 0:
        headline = f"Model prices YES {magnitude} points above the market."
    else:
        headline = f"Market prices YES {magnitude} points above the model."

    strike = market.get("floor_strike")
    minutes = market.get("minutes_to_close")
    deck_parts: list[str] = []
    if strike is not None:
        if spot is None:
            position = "spot unavailable"
        else:
            position = "above target" if spot >= strike else "below target"
        deck_parts.append(f"BTC is {position} at a ${strike:,.0f} target")
    if minutes is not None:
        deck_parts.append(f"{max(0, round(minutes))} min remain")
    deck_parts.append(f"market {market_pct}% · model {model_pct}%")

    return {
        "headline": headline,
        "deck": " · ".join(deck_parts) + ".",
        "market_pct": market_pct,
        "model_pct": model_pct,
        "edge_points": edge_points,
    }

#!/usr/bin/env python3
"""
Crude “follow smart money” backtest using only public Polymarket APIs:

- Trade tape (Data API): large fills + directional notional in fixed time windows
  on both YES and NO clob tokens where available.
- Price path (CLOB prices-history): window return and forward return after the window (YES mid).

This is a *proxy* for insider flow — not order-book impact. Outputs mean/median
forward moves for “signal” windows vs the rest so you can sanity-check merit
before you invest in live book logging.

Usage:
  python3 polymarket_smart_money_backtest.py
  python3 polymarket_smart_money_backtest.py --slug rou1-aus-aog-2026-03-23
  python3 polymarket_smart_money_backtest.py --j2-feb28-csv j2_backtest_2026.csv
  python3 polymarket_smart_money_backtest.py --csl-mar-apr-csv out.csv --large-trade-cut histogram
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import os
import math
import ssl
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import certifi as _certifi
except ImportError:  # pragma: no cover
    _certifi = None

_BACKTEST_DIR = os.path.dirname(os.path.abspath(__file__))
if _BACKTEST_DIR not in sys.path:
    sys.path.insert(0, _BACKTEST_DIR)
from sketch_quantile import build_log_spaced_edges, histogram_large_cut_fixed

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

DEFAULT_SLUG = "rou1-aus-aog-2026-03-23"
MATCH_PAD_AFTER = timedelta(hours=2, minutes=30)
WINDOW_SEC = 300
FORWARD_SEC = 900
PAGE_SIZE = 1000
TRADE_PAGE_CAP = 250_000
# Data API ``/trades`` rejects ``offset`` above this (see ``max historical activity offset`` error).
DATA_TRADES_MAX_OFFSET = 3000

# prices-history: API enforces min fidelity for interval=1m (often 10).
PRICE_INTERVAL = "1m"
PRICE_FIDELITY = 10

# Signal thresholds (tune freely).
LARGE_TRADE_Q = 0.90  # vs in-match trade sizes on the YES asset
FLOW_RATIO = 1.5  # window buy_notional >= FLOW_RATIO * median window buy_notional
MIN_WINDOW_NOTIONAL = 25.0
MIN_DELTA_P = 0.005  # window price change (from history) must exceed this

ROU1_SERIES_ID = 10971
CSL_SERIES_ID = 10439  # Gamma ``/series?slug=chinese-super-league``
J2_SERIES_ID = 10443  # Gamma ``/series?slug=japan-j2-league``

# Romanian SuperLiga batch windows (``endDate`` UTC calendar date, inclusive).
def rou1_week1_range(year: int) -> tuple[date, date]:
    return date(year, 1, 24), date(year, 2, 9)


def rou1_week3_range(year: int) -> tuple[date, date]:
    return date(year, 2, 27), date(year, 3, 23)


def csl_march_april_range(year: int) -> tuple[date, date]:
    """Chinese Super League batch: ``endDate`` UTC in [Mar 6, Apr 6] inclusive."""
    return date(year, 3, 6), date(year, 4, 6)


def j2_feb28_through_apr6_range(year: int) -> tuple[date, date]:
    """Japan J2 batch: ``endDate`` UTC from Feb 28 through Apr 6 of ``year`` (inclusive)."""
    return date(year, 2, 28), date(year, 4, 6)


# Cloudflare often blocks ``Python-urllib/*`` on Polymarket; use a normal browser UA.
_HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class PolymarketRequestError(RuntimeError):
    """HTTP/network failure when talking to Polymarket APIs (batch mode catches this)."""


def _request_json(url: str) -> dict | list:
    """GET ``url`` and parse JSON; raise ``PolymarketRequestError`` on failure."""
    req = Request(url, headers=_HTTP_HEADERS)
    ctx = (
        ssl.create_default_context(cafile=_certifi.where())
        if _certifi is not None
        else ssl.create_default_context()
    )
    try:
        with urlopen(req, context=ctx, timeout=90) as resp:
            return json.load(resp)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise PolymarketRequestError(f"HTTP {e.code} for {url}\n{body}") from e
    except URLError as e:
        raise PolymarketRequestError(f"Request failed: {e}") from e


def _get_json(url: str) -> dict | list:
    """GET ``url`` and parse JSON; exit the process on HTTP or network errors."""
    try:
        return _request_json(url)
    except PolymarketRequestError as e:
        raise SystemExit(str(e)) from e


def _fetch_event_slug(slug: str) -> dict:
    """Load a single event from Gamma by human-readable ``slug``; validate shape."""
    data = _request_json(f"{GAMMA_BASE}/events/slug/{slug}")
    if not isinstance(data, dict) or not data.get("id"):
        raise ValueError(f"Bad Gamma payload for slug={slug!r}")
    return data


def _kickoff_utc(event: dict) -> datetime:
    """Return match start as timezone-aware UTC from the event ``endDate`` field."""
    raw = event.get("endDate")
    if not raw or not isinstance(raw, str):
        raise ValueError("Event missing endDate (scheduled start).")
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _clob_token_ids(market: dict) -> list[str]:
    """Normalize Gamma ``clobTokenIds`` (list or JSON string) to token id strings."""
    raw = market.get("clobTokenIds")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def _json_field_list(raw: object) -> list[object]:
    """Parse a Gamma JSON array field that may already be a list or a JSON string."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _pick_binary_assets(event: dict, market_hint: str | None) -> tuple[str, str | None, str, dict]:
    """Choose a market; return (YES clob id, NO clob id or None, question, market dict)."""
    markets = event.get("markets") or []
    if not markets:
        raise ValueError("Event has no markets.")
    chosen = None
    hint = (market_hint or "").strip().lower()
    if hint:
        for m in markets:
            q = (m.get("question") or "").lower()
            if hint in q:
                chosen = m
                break
    if chosen is None:
        chosen = markets[0]
    tokens = _clob_token_ids(chosen)
    if len(tokens) < 1:
        raise ValueError("Market missing clobTokenIds for YES.")
    yes = tokens[0]
    no_tid = str(tokens[1]) if len(tokens) > 1 else None
    q = chosen.get("question") or "(no question)"
    return yes, no_tid, q, chosen


def _resolved_yes_won(market: dict) -> bool | None:
    """
    If the binary market is resolved, return whether the first outcome (YES token / ``clobTokenIds[0]``)
    paid out (~1). Otherwise ``None`` if unresolved or ambiguous.
    """
    if not market.get("closed"):
        return None
    prices_raw = _json_field_list(market.get("outcomePrices"))
    if len(prices_raw) < 2:
        return None
    try:
        p_yes = float(prices_raw[0])
        p_no = float(prices_raw[1])
    except (TypeError, ValueError):
        return None
    if p_yes > 0.9 and p_no < 0.1:
        return True
    if p_yes < 0.1 and p_no > 0.9:
        return False
    return None


def _fetch_trades_event(event_id: int, stop_before_ts: int | None) -> list[dict]:
    """Paginate Data API trades for ``event_id``, optionally stopping when batch is older than ``stop_before_ts``."""
    rows: list[dict] = []
    offset = 0
    while offset < TRADE_PAGE_CAP:
        if offset > DATA_TRADES_MAX_OFFSET:
            break
        q = urlencode(
            {
                "eventId": event_id,
                "limit": PAGE_SIZE,
                "offset": offset,
                "takerOnly": "false",
            }
        )
        batch = _request_json(f"{DATA_BASE}/trades?{q}")
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        oldest = int(batch[-1]["timestamp"])
        offset += len(batch)
        if stop_before_ts is not None and oldest < stop_before_ts:
            break
        if len(batch) < PAGE_SIZE:
            break
    return rows


def fetch_trades_event_since(
    event_id: int,
    since_ts_exclusive: int,
    *,
    min_ts_inclusive: int | None = None,
    max_ts_inclusive: int | None = None,
) -> list[dict]:
    """
    Incremental trade pull: paginate ``/trades`` newest-first (same order as ``_fetch_trades_event``),
    keep rows with ``since_ts_exclusive < timestamp`` and optional ``[min_ts_inclusive, max_ts_inclusive]``.
    Stops when a batch's oldest trade is ``<= since_ts_exclusive`` (no newer unseen rows on later pages).
    """
    rows: list[dict] = []
    offset = 0
    lo = min_ts_inclusive if min_ts_inclusive is not None else -(2**63)
    hi = max_ts_inclusive if max_ts_inclusive is not None else 2**63 - 1
    while offset < TRADE_PAGE_CAP:
        if offset > DATA_TRADES_MAX_OFFSET:
            break
        q = urlencode(
            {
                "eventId": event_id,
                "limit": PAGE_SIZE,
                "offset": offset,
                "takerOnly": "false",
            }
        )
        batch = _request_json(f"{DATA_BASE}/trades?{q}")
        if not isinstance(batch, list) or not batch:
            break
        for row in batch:
            ts = int(row["timestamp"])
            if ts <= since_ts_exclusive:
                continue
            if ts < lo or ts > hi:
                continue
            rows.append(row)
        oldest = int(batch[-1]["timestamp"])
        offset += len(batch)
        if oldest <= since_ts_exclusive:
            break
        if len(batch) < PAGE_SIZE:
            break
    return rows


def _fetch_price_history(asset: str, start_ts: int, end_ts: int) -> list[tuple[int, float]]:
    """Fetch CLOB ``prices-history`` for ``asset`` between UNIX ``start_ts`` and ``end_ts``; return sorted (t, p)."""
    q = urlencode(
        {
            "market": asset,
            "interval": PRICE_INTERVAL,
            "fidelity": PRICE_FIDELITY,
            "startTs": start_ts,
            "endTs": end_ts,
        }
    )
    data = _request_json(f"{CLOB_BASE}/prices-history?{q}")
    if not isinstance(data, dict):
        raise ValueError("Unexpected prices-history payload.")
    if data.get("error"):
        raise ValueError(f"prices-history error: {data['error']}")
    hist = data.get("history") or []
    out: list[tuple[int, float]] = []
    for row in hist:
        if isinstance(row, dict) and "t" in row and "p" in row:
            out.append((int(row["t"]), float(row["p"])))
    out.sort(key=lambda x: x[0])
    return out


def _price_at_or_before(series: list[tuple[int, float]], ts: int) -> float | None:
    """Last price in ``series`` at or before timestamp ``ts``; ``None`` if ``series`` is empty or ``ts`` is before first point."""
    if not series:
        return None
    times = [t for t, _ in series]
    i = bisect.bisect_right(times, ts) - 1
    if i < 0:
        return None
    return series[i][1]


@dataclass
class WindowStat:
    """Aggregated trade stats for one fixed-duration window after kickoff (YES asset only)."""

    idx: int
    t0: int
    t1: int
    buy_notional: float
    sell_notional: float
    buy_size: float
    sell_size: float
    max_trade: float
    n_trades: int


def _build_windows(
    trades: list[dict],
    asset: str,
    kick_ts: int,
    end_ts: int,
) -> tuple[list[WindowStat], list[float]]:
    """Bucket ``trades`` for ``asset`` into ``WINDOW_SEC`` slices from ``kick_ts`` to ``end_ts``; return stats and trade sizes for quantiles."""
    sizes_sample: list[float] = []
    for row in trades:
        if str(row.get("asset")) != asset:
            continue
        ts = int(row["timestamp"])
        if ts < kick_ts or ts > end_ts:
            continue
        sizes_sample.append(float(row["size"]))

    n_win = max(1, (end_ts - kick_ts + WINDOW_SEC) // WINDOW_SEC)
    if not sizes_sample:
        stats = [
            WindowStat(
                idx=i,
                t0=kick_ts + i * WINDOW_SEC,
                t1=kick_ts + (i + 1) * WINDOW_SEC,
                buy_notional=0.0,
                sell_notional=0.0,
                buy_size=0.0,
                sell_size=0.0,
                max_trade=0.0,
                n_trades=0,
            )
            for i in range(n_win)
        ]
        return stats, []
    buckets: list[dict[str, float | int]] = [
        {
            "buy_notional": 0.0,
            "sell_notional": 0.0,
            "buy_size": 0.0,
            "sell_size": 0.0,
            "max_trade": 0.0,
            "n": 0,
        }
        for _ in range(n_win)
    ]

    for row in trades:
        if str(row.get("asset")) != asset:
            continue
        ts = int(row["timestamp"])
        if ts < kick_ts or ts > end_ts:
            continue
        idx = min((ts - kick_ts) // WINDOW_SEC, n_win - 1)
        b = buckets[idx]
        side = row.get("side")
        size = float(row["size"])
        price = float(row["price"])
        notional = size * price
        assert isinstance(b["max_trade"], float)
        b["max_trade"] = max(float(b["max_trade"]), size)
        b["n"] = int(b["n"]) + 1
        if side == "BUY":
            b["buy_notional"] = float(b["buy_notional"]) + notional
            b["buy_size"] = float(b["buy_size"]) + size
        elif side == "SELL":
            b["sell_notional"] = float(b["sell_notional"]) + notional
            b["sell_size"] = float(b["sell_size"]) + size

    stats: list[WindowStat] = []
    for i, b in enumerate(buckets):
        stats.append(
            WindowStat(
                idx=i,
                t0=kick_ts + i * WINDOW_SEC,
                t1=kick_ts + (i + 1) * WINDOW_SEC,
                buy_notional=float(b["buy_notional"]),
                sell_notional=float(b["sell_notional"]),
                buy_size=float(b["buy_size"]),
                sell_size=float(b["sell_size"]),
                max_trade=float(b["max_trade"]),
                n_trades=int(b["n"]),
            )
        )
    return stats, sizes_sample


def _quantile(sorted_vals: list[float], q: float) -> float:
    """Simple index-based quantile on **pre-sorted** ``sorted_vals``; ``q`` in [0, 1]."""
    if not sorted_vals:
        return math.nan
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def _leg_hits(rows: list[dict[str, object]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        for leg in r.get("signal_legs") or []:
            if isinstance(leg, str):
                out[leg] = out.get(leg, 0) + 1
    return out


def fetch_gamma_series_events_in_range(series_id: int, d_start: date, d_end: date) -> list[dict]:
    """
    Paginate Gamma ``events?series_id=…``; keep events whose ``endDate`` UTC calendar date
    lies in ``[d_start, d_end]``; drop slugs containing *more-markets* (case-insensitive).
    """
    out: list[dict] = []
    offset = 0
    while True:
        q = urlencode({"series_id": series_id, "limit": 100, "offset": offset})
        batch = _request_json(f"{GAMMA_BASE}/events?{q}")
        if not isinstance(batch, list) or not batch:
            break
        for e in batch:
            slug_s = e.get("slug") or ""
            if "more-markets" in slug_s.lower():
                continue
            raw = e.get("endDate")
            if not raw or not isinstance(raw, str):
                continue
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            d = dt.date()
            if d_start <= d <= d_end:
                out.append(e)
        offset += len(batch)
        if len(batch) < 100:
            break
    out.sort(key=lambda ev: str(ev.get("endDate") or ""))
    return out


def fetch_rou1_events_in_range(d_start: date, d_end: date) -> list[dict]:
    """ROU1 (series 10971); same filters as ``fetch_gamma_series_events_in_range``."""
    return fetch_gamma_series_events_in_range(ROU1_SERIES_ID, d_start, d_end)


def fetch_rou1_week1_and_week3_events(year: int) -> list[tuple[dict, str]]:
    """
    Events in Week 1 (Jan 24–Feb 9) and Week 3 (Feb 27–Mar 23), ``endDate`` UTC dates inclusive.
    De-duplicated by slug (later band wins if both ever overlapped); sorted by ``endDate``.
    """
    w1_lo, w1_hi = rou1_week1_range(year)
    w3_lo, w3_hi = rou1_week3_range(year)
    by_slug: dict[str, tuple[dict, str]] = {}
    for e in fetch_rou1_events_in_range(w1_lo, w1_hi):
        s = str(e.get("slug") or "").strip()
        if s:
            by_slug[s] = (e, "week1")
    for e in fetch_rou1_events_in_range(w3_lo, w3_hi):
        s = str(e.get("slug") or "").strip()
        if s:
            by_slug[s] = (e, "week3")
    merged = list(by_slug.values())
    merged.sort(key=lambda t: str(t[0].get("endDate") or ""))
    return merged


def fetch_csl_events_march_april(year: int) -> list[tuple[dict, str]]:
    """
    Chinese Super League (series ``CSL_SERIES_ID``): matches with ``endDate`` UTC in
    ``[Mar 6, Apr 6]`` of ``year``, tag ``csl`` for CSV ``rou1_window`` column.
    """
    lo, hi = csl_march_april_range(year)
    events = fetch_gamma_series_events_in_range(CSL_SERIES_ID, lo, hi)
    return [(e, "csl") for e in events]


def fetch_j2_events_feb28_apr6(year: int) -> list[tuple[dict, str]]:
    """
    Japan J2 League (``J2_SERIES_ID``): matches with ``endDate`` UTC in
    ``[Feb 28, Apr 6]`` of ``year``, tag ``j2`` for CSV ``rou1_window`` column.
    """
    lo, hi = j2_feb28_through_apr6_range(year)
    events = fetch_gamma_series_events_in_range(J2_SERIES_ID, lo, hi)
    return [(e, "j2") for e in events]


@dataclass
class MatchSummary:
    """One row of batch CSV + fields needed to print the CLI report."""

    slug: str
    title: str = ""
    event_id: int = 0
    kickoff_utc: str = ""
    yes_market_question: str = ""
    yes_contract_won: str = ""
    n_windows_tape: int = 0
    n_windows_scored: int = 0
    n_flagged: int = 0
    n_buy_yes: int = 0
    n_sell_yes: int = 0
    n_unflagged: int = 0
    leg_yes_buy: int = 0
    leg_no_sell: int = 0
    leg_yes_sell: int = 0
    leg_no_buy: int = 0
    buy_yes_aligned: str = ""
    sell_yes_aligned: str = ""
    n_flagged_correct: str = ""
    n_flagged_wrong: str = ""
    flagged_windows_detail: str = ""
    rou1_window: str = ""
    status: str = "ok"
    error: str = ""
    window_rows: list[dict[str, object]] | None = None
    yes_won: bool | None = None
    yes_asset: str = ""
    no_asset: str | None = None
    trades_fetched: int = 0
    price_points: int = 0
    kick_dt: datetime | None = None
    end_ts: int = 0


def match_summary_to_csv_row(s: MatchSummary) -> dict[str, str]:
    """Flatten ``MatchSummary`` to string values for ``csv.DictWriter``."""
    return {
        "slug": s.slug,
        "title": s.title,
        "event_id": str(s.event_id),
        "kickoff_utc": s.kickoff_utc,
        "yes_market_question": s.yes_market_question,
        "yes_contract_won": s.yes_contract_won,
        "n_windows_tape": str(s.n_windows_tape),
        "n_windows_scored": str(s.n_windows_scored),
        "n_flagged": str(s.n_flagged),
        "n_buy_yes": str(s.n_buy_yes),
        "n_sell_yes": str(s.n_sell_yes),
        "n_unflagged": str(s.n_unflagged),
        "leg_yes_buy": str(s.leg_yes_buy),
        "leg_no_sell": str(s.leg_no_sell),
        "leg_yes_sell": str(s.leg_yes_sell),
        "leg_no_buy": str(s.leg_no_buy),
        "buy_yes_aligned": s.buy_yes_aligned,
        "sell_yes_aligned": s.sell_yes_aligned,
        "n_flagged_correct": s.n_flagged_correct,
        "n_flagged_wrong": s.n_flagged_wrong,
        "flagged_windows_detail": s.flagged_windows_detail,
        "rou1_window": s.rou1_window,
        "status": s.status,
        "error": s.error,
    }


CSV_COLUMNS = list(match_summary_to_csv_row(MatchSummary(slug="")).keys())


def analyze_match(
    slug: str,
    market_hint: str | None = None,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> MatchSummary:
    """
    Run the signal pipeline for one event slug. On failure returns ``MatchSummary`` with
    ``status`` in ``no_tape``, ``insufficient_windows``, ``api_error``, ``data_error`` and ``error`` set.

    ``large_trade_cut_mode``: ``\"exact\"`` uses sorted prefix sizes + ``_quantile`` (default, unchanged).
    ``\"histogram\"`` uses ``sketch_quantile.histogram_large_cut_fixed`` with **fixed** log-spaced edges
    (``build_log_spaced_edges`` over production min/max share notionals), same as a deployed sketch.
    """

    def fail(status: str, msg: str, **fields: object) -> MatchSummary:
        m = MatchSummary(slug=slug, status=status, error=msg)
        for k, v in fields.items():
            if hasattr(m, k):
                setattr(m, k, v)
        return m

    if large_trade_cut_mode not in ("exact", "histogram"):
        return fail("data_error", f"Invalid large_trade_cut_mode={large_trade_cut_mode!r} (use exact|histogram).")

    hist_edges = (
        build_log_spaced_edges(histogram_bins) if large_trade_cut_mode == "histogram" else None
    )

    try:
        event = _fetch_event_slug(slug)
    except PolymarketRequestError as e:
        return fail("api_error", str(e))
    except ValueError as e:
        return fail("data_error", str(e))

    eid = int(event["id"])
    title = str(event.get("title", slug))
    try:
        kick = _kickoff_utc(event)
    except ValueError as e:
        return fail("data_error", str(e), title=title, event_id=eid)

    kick_ts = int(kick.timestamp())
    end_ts_i = int((kick + MATCH_PAD_AFTER).timestamp())
    hist_lo = kick_ts - 3600
    hist_hi = end_ts_i + FORWARD_SEC + 3600
    kickoff_utc = kick.isoformat()

    try:
        yes_asset, no_asset, market_q, focus_market = _pick_binary_assets(event, market_hint)
    except ValueError as e:
        return fail("data_error", str(e), title=title, event_id=eid, kickoff_utc=kickoff_utc)

    yes_won = _resolved_yes_won(focus_market)
    yes_contract = "" if yes_won is None else ("true" if yes_won else "false")

    try:
        trades = _fetch_trades_event(eid, stop_before_ts=kick_ts - 86400)
    except PolymarketRequestError as e:
        return fail(
            "api_error",
            str(e),
            title=title,
            event_id=eid,
            kickoff_utc=kickoff_utc,
            yes_market_question=market_q,
            yes_contract_won=yes_contract,
            yes_won=yes_won,
            yes_asset=yes_asset,
            no_asset=no_asset,
            kick_dt=kick,
            end_ts=end_ts_i,
        )

    w_yes, _ = _build_windows(trades, yes_asset, kick_ts, end_ts_i)
    if no_asset:
        w_no, _ = _build_windows(trades, no_asset, kick_ts, end_ts_i)
    else:
        w_no = []
    if len(w_yes) != len(w_no) and w_no:
        return fail("data_error", "YES/NO window grids misaligned.", title=title, event_id=eid, kickoff_utc=kickoff_utc)
    if not w_no:
        w_no = [
            WindowStat(
                idx=w.idx,
                t0=w.t0,
                t1=w.t1,
                buy_notional=0.0,
                sell_notional=0.0,
                buy_size=0.0,
                sell_size=0.0,
                max_trade=0.0,
                n_trades=0,
            )
            for w in w_yes
        ]

    active_idx = [
        i
        for i, (a, b) in enumerate(zip(w_yes, w_no, strict=True))
        if a.n_trades + b.n_trades > 0
    ]
    n_windows_tape = len(active_idx)
    if not active_idx:
        return fail(
            "no_tape",
            "No trades on YES or NO inside the match window.",
            title=title,
            event_id=eid,
            kickoff_utc=kickoff_utc,
            yes_market_question=market_q,
            yes_contract_won=yes_contract,
            yes_won=yes_won,
            yes_asset=yes_asset,
            no_asset=no_asset,
            trades_fetched=len(trades),
            kick_dt=kick,
            end_ts=end_ts_i,
            n_windows_tape=0,
        )

    try:
        price_series = _fetch_price_history(yes_asset, hist_lo, hist_hi)
    except (PolymarketRequestError, ValueError) as e:
        st = "api_error" if isinstance(e, PolymarketRequestError) else "data_error"
        return fail(
            st,
            str(e),
            title=title,
            event_id=eid,
            kickoff_utc=kickoff_utc,
            yes_market_question=market_q,
            yes_contract_won=yes_contract,
            n_windows_tape=n_windows_tape,
            yes_won=yes_won,
            yes_asset=yes_asset,
            no_asset=no_asset,
            trades_fetched=len(trades),
            kick_dt=kick,
            end_ts=end_ts_i,
        )

    window_rows: list[dict[str, object]] = []
    for i in active_idx:
        wy, wn = w_yes[i], w_no[i]
        # Prefix-causal stats: only windows with grid index <= current, and only trades with ts <= wy.t1.
        prefix_active = [j for j in active_idx if j <= i]
        wy_pre = [w_yes[j] for j in prefix_active]
        wn_pre = [w_no[j] for j in prefix_active]
        med_buy_yes = (
            statistics.median([w.buy_notional for w in wy_pre if w.buy_notional > 0])
            if any(w.buy_notional > 0 for w in wy_pre)
            else 0.0
        )
        med_sell_yes = (
            statistics.median([w.sell_notional for w in wy_pre if w.sell_notional > 0])
            if any(w.sell_notional > 0 for w in wy_pre)
            else 0.0
        )
        med_buy_no = (
            statistics.median([w.buy_notional for w in wn_pre if w.buy_notional > 0])
            if any(w.buy_notional > 0 for w in wn_pre)
            else 0.0
        )
        med_sell_no = (
            statistics.median([w.sell_notional for w in wn_pre if w.sell_notional > 0])
            if any(w.sell_notional > 0 for w in wn_pre)
            else 0.0
        )

        sizes_yes_pre = [
            float(r["size"])
            for r in trades
            if str(r.get("asset")) == yes_asset and kick_ts <= int(r["timestamp"]) <= wy.t1
        ]
        if large_trade_cut_mode == "histogram":
            assert hist_edges is not None
            large_cut_yes = histogram_large_cut_fixed(sizes_yes_pre, LARGE_TRADE_Q, hist_edges)
        else:
            sizes_yes_pre.sort()
            large_cut_yes = _quantile(sizes_yes_pre, LARGE_TRADE_Q)
        if no_asset:
            sizes_no_pre = [
                float(r["size"])
                for r in trades
                if str(r.get("asset")) == no_asset and kick_ts <= int(r["timestamp"]) <= wy.t1
            ]
            if large_trade_cut_mode == "histogram":
                assert hist_edges is not None
                large_cut_no = histogram_large_cut_fixed(sizes_no_pre, LARGE_TRADE_Q, hist_edges)
            else:
                sizes_no_pre.sort()
                large_cut_no = _quantile(sizes_no_pre, LARGE_TRADE_Q)
        else:
            large_cut_no = math.nan

        p_start = _price_at_or_before(price_series, wy.t0)
        p_end = _price_at_or_before(price_series, wy.t1)
        if p_start is None or p_end is None:
            continue
        delta_p = p_end - p_start
        fwd_end = wy.t1 + FORWARD_SEC
        p_fwd = _price_at_or_before(price_series, fwd_end)
        if p_fwd is None:
            continue
        forward = p_fwd - p_end

        net_buy_yes = wy.buy_size - wy.sell_size
        net_buy_no = wn.buy_size - wn.sell_size
        large_yes = (not math.isnan(large_cut_yes)) and wy.max_trade >= large_cut_yes and wy.max_trade > 0
        large_no = (not math.isnan(large_cut_no)) and wn.max_trade >= large_cut_no and wn.max_trade > 0

        fbuy_yes = wy.buy_notional >= max(MIN_WINDOW_NOTIONAL, FLOW_RATIO * med_buy_yes) if med_buy_yes > 0 else wy.buy_notional >= MIN_WINDOW_NOTIONAL
        fsell_yes = wy.sell_notional >= max(MIN_WINDOW_NOTIONAL, FLOW_RATIO * med_sell_yes) if med_sell_yes > 0 else wy.sell_notional >= MIN_WINDOW_NOTIONAL
        fbuy_no = wn.buy_notional >= max(MIN_WINDOW_NOTIONAL, FLOW_RATIO * med_buy_no) if med_buy_no > 0 else wn.buy_notional >= MIN_WINDOW_NOTIONAL
        fsell_no = wn.sell_notional >= max(MIN_WINDOW_NOTIONAL, FLOW_RATIO * med_sell_no) if med_sell_no > 0 else wn.sell_notional >= MIN_WINDOW_NOTIONAL

        buy_via_yes = large_yes and fbuy_yes and delta_p >= MIN_DELTA_P and net_buy_yes > 0
        buy_via_no = (
            bool(no_asset)
            and large_no
            and fsell_no
            and delta_p >= MIN_DELTA_P
            and net_buy_no < 0
        )
        sell_via_yes = large_yes and fsell_yes and delta_p <= -MIN_DELTA_P and net_buy_yes < 0
        sell_via_no = (
            bool(no_asset)
            and large_no
            and fbuy_no
            and delta_p <= -MIN_DELTA_P
            and net_buy_no > 0
        )

        buy_yes = buy_via_yes or buy_via_no
        sell_yes = sell_via_yes or sell_via_no
        flag_kind: str | None = "buy_yes" if buy_yes else ("sell_yes" if sell_yes else None)
        signal_legs: list[str] = []
        if flag_kind == "buy_yes":
            if buy_via_yes:
                signal_legs.append("yes_buy")
            if buy_via_no:
                signal_legs.append("no_sell")
        elif flag_kind == "sell_yes":
            if sell_via_yes:
                signal_legs.append("yes_sell")
            if sell_via_no:
                signal_legs.append("no_buy")

        window_rows.append(
            {
                "window": wy.idx,
                "t0_utc": datetime.fromtimestamp(wy.t0, tz=timezone.utc).isoformat(),
                "n_trades_yes": wy.n_trades,
                "n_trades_no": wn.n_trades,
                "buy_notional_yes": round(wy.buy_notional, 2),
                "sell_notional_yes": round(wy.sell_notional, 2),
                "max_trade_yes": round(wy.max_trade, 4),
                "buy_notional_no": round(wn.buy_notional, 2),
                "sell_notional_no": round(wn.sell_notional, 2),
                "max_trade_no": round(wn.max_trade, 4),
                "p_yes_end": round(p_end, 4),
                "delta_p": round(delta_p, 4),
                "forward_p": round(forward, 4),
                "flag_kind": flag_kind,
                "signal_legs": signal_legs,
            }
        )

    n_windows_scored = len(window_rows)
    if n_windows_scored < 5:
        return fail(
            "insufficient_windows",
            "Too few windows with usable price history — widen time bounds or relax filters.",
            title=title,
            event_id=eid,
            kickoff_utc=kickoff_utc,
            yes_market_question=market_q,
            yes_contract_won=yes_contract,
            n_windows_tape=n_windows_tape,
            n_windows_scored=n_windows_scored,
            yes_won=yes_won,
            yes_asset=yes_asset,
            no_asset=no_asset,
            trades_fetched=len(trades),
            price_points=len(price_series),
            window_rows=window_rows,
            kick_dt=kick,
            end_ts=end_ts_i,
        )

    buy_flags = [r for r in window_rows if r["flag_kind"] == "buy_yes"]
    sell_flags = [r for r in window_rows if r["flag_kind"] == "sell_yes"]
    baseline = [r for r in window_rows if r["flag_kind"] is None]
    all_flags = buy_flags + sell_flags

    leg_all = _leg_hits(all_flags)
    n_buy, n_sell = len(buy_flags), len(sell_flags)
    buy_match_n = sum(1 for _ in buy_flags if yes_won) if yes_won is not None else 0
    sell_match_n = sum(1 for _ in sell_flags if not yes_won) if yes_won is not None else 0
    correct = buy_match_n + sell_match_n if yes_won is not None else 0
    wrong = (n_buy - buy_match_n) + (n_sell - sell_match_n) if yes_won is not None else 0

    detail_parts: list[str] = []
    for r in all_flags:
        fk = r.get("flag_kind")
        legs = "+".join(r.get("signal_legs") or [])  # type: ignore[arg-type]
        t0 = r.get("t0_utc", "")
        p_e = r.get("p_yes_end")
        pe_s = f"{float(p_e):.4f}" if isinstance(p_e, (int, float)) else ""
        detail_parts.append(f"{fk}:{legs}@{t0}|p_end={pe_s}")

    return MatchSummary(
        slug=slug,
        title=title,
        event_id=eid,
        kickoff_utc=kickoff_utc,
        yes_market_question=market_q,
        yes_contract_won=yes_contract,
        n_windows_tape=n_windows_tape,
        n_windows_scored=n_windows_scored,
        n_flagged=len(all_flags),
        n_buy_yes=n_buy,
        n_sell_yes=n_sell,
        n_unflagged=len(baseline),
        leg_yes_buy=leg_all.get("yes_buy", 0),
        leg_no_sell=leg_all.get("no_sell", 0),
        leg_yes_sell=leg_all.get("yes_sell", 0),
        leg_no_buy=leg_all.get("no_buy", 0),
        buy_yes_aligned=str(buy_match_n) if yes_won is not None else "",
        sell_yes_aligned=str(sell_match_n) if yes_won is not None else "",
        n_flagged_correct=str(correct) if yes_won is not None else "",
        n_flagged_wrong=str(wrong) if yes_won is not None else "",
        flagged_windows_detail="; ".join(detail_parts),
        status="ok",
        error="",
        window_rows=window_rows,
        yes_won=yes_won,
        yes_asset=yes_asset,
        no_asset=no_asset,
        trades_fetched=len(trades),
        price_points=len(price_series),
        kick_dt=kick,
        end_ts=end_ts_i,
    )


def _print_backtest_from_summary(s: MatchSummary) -> None:
    """Print the legacy human-readable report (single-match CLI)."""
    if s.status != "ok" or s.window_rows is None or s.kick_dt is None:
        return
    wr = s.window_rows
    buy_flags = [r for r in wr if r["flag_kind"] == "buy_yes"]
    sell_flags = [r for r in wr if r["flag_kind"] == "sell_yes"]
    baseline = [r for r in wr if r["flag_kind"] is None]
    all_flags = buy_flags + sell_flags
    yes_won = s.yes_won

    print(f"Event: {s.title} (id={s.event_id})", flush=True)
    print(f"Focus YES token market: {s.yes_market_question}", flush=True)
    print(f"YES clob token id: {s.yes_asset}", flush=True)
    if s.no_asset:
        print(f"NO  clob token id: {s.no_asset}", flush=True)
    else:
        print("NO  clob token: (missing second outcome — NO-leg signals disabled)", flush=True)
    if yes_won is True:
        print("Resolved outcome: YES (this contract paid out).", flush=True)
    elif yes_won is False:
        print("Resolved outcome: NO (YES contract expired worthless).", flush=True)
    else:
        print("Resolved outcome: unknown or not final (alignment vs result is N/A).", flush=True)
    print(
        f"In-play slice: {s.kick_dt.isoformat()} → "
        f"{datetime.fromtimestamp(s.end_ts, tz=timezone.utc).isoformat()} (UTC)",
        flush=True,
    )
    print(f"Fetched {s.trades_fetched} trade rows (paginated cap {TRADE_PAGE_CAP}).", flush=True)
    print(f"prices-history points: {s.price_points} ({PRICE_INTERVAL}, fidelity={PRICE_FIDELITY})", flush=True)

    def _summ(name: str, rows: list[dict[str, object]]) -> None:
        fw = [float(r["forward_p"]) for r in rows]
        if not fw:
            print(f"{name}: no windows.", flush=True)
            return
        print(
            f"{name}: n={len(fw)}  "
            f"mean_fwdΔp={statistics.mean(fw):.4f}  "
            f"median_fwdΔp={statistics.median(fw):.4f}",
            flush=True,
        )

    print("\n--- Flag distribution (large trade + flow + price move on YES / NO tape) ---", flush=True)
    print(f"  buy_yes:  {len(buy_flags)}", flush=True)
    print(f"  sell_yes: {len(sell_flags)}", flush=True)
    print(f"  none:     {len(baseline)}", flush=True)
    if buy_flags:
        hb = _leg_hits(buy_flags)
        print(
            "  buy_yes legs (window may count twice if both fire): "
            + ", ".join(f"{k}={v}" for k, v in sorted(hb.items())),
            flush=True,
        )
    if sell_flags:
        hs = _leg_hits(sell_flags)
        print(
            "  sell_yes legs: " + ", ".join(f"{k}={v}" for k, v in sorted(hs.items())),
            flush=True,
        )

    if yes_won is not None:
        n_buy, n_sell = len(buy_flags), len(sell_flags)
        buy_match = sum(1 for _ in buy_flags if yes_won)
        sell_match = sum(1 for _ in sell_flags if not yes_won)
        print("\n--- Ex-post vs Polymarket resolution (did flag direction match winner?) ---", flush=True)
        print(
            f"  buy_yes:  {buy_match}/{n_buy} windows align with resolution"
            + ("" if n_buy else "  (no buy_yes flags)"),
            flush=True,
        )
        print(
            f"  sell_yes: {sell_match}/{n_sell} windows align with resolution"
            + ("" if n_sell else "  (no sell_yes flags)"),
            flush=True,
        )
        print(
            "  (buy_yes aligns iff YES paid out; sell_yes aligns iff NO paid out.)",
            flush=True,
        )

    print("\n--- Backtest (forward Δp is from end-of-window to +%ds price) ---" % FORWARD_SEC, flush=True)
    _summ("ALL_FLAGGED", all_flags)
    _summ("BUY_YES", buy_flags)
    _summ("SELL_YES", sell_flags)
    _summ("UNFLAGGED", baseline)

    top = sorted(wr, key=lambda r: (-float(r["forward_p"]), -float(r["delta_p"])))[:8]
    print("\nTop windows by forward Δp (all kinds):", flush=True)
    print(json.dumps(top, indent=2))


def run_backtest(
    slug: str,
    market_hint: str | None,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> None:
    """End-to-end pipeline: fetch event/trades/prices, label windows, print signal vs baseline forward returns."""
    s = analyze_match(
        slug,
        market_hint,
        large_trade_cut_mode=large_trade_cut_mode,
        histogram_bins=histogram_bins,
    )
    if s.status == "insufficient_windows" and s.window_rows is not None:
        print(json.dumps(s.window_rows, indent=2))
    if s.status != "ok":
        print(s.error, file=sys.stderr, flush=True)
        raise SystemExit(1)
    _print_backtest_from_summary(s)


def print_flagged_outcome_vs_resolution_report(summaries: list[MatchSummary]) -> None:
    """Summarize how many flagged windows matched Polymarket resolution vs did not (resolved markets only)."""
    correct = 0
    wrong = 0
    unresolved_flag_windows = 0
    by_band: dict[str, list[int]] = {}

    for s in summaries:
        band = s.rou1_window or "unknown"
        if band not in by_band:
            by_band[band] = [0, 0]
        cc, ww = s.n_flagged_correct, s.n_flagged_wrong
        if cc == "" or ww == "":
            if s.status == "ok" and s.n_flagged > 0:
                unresolved_flag_windows += s.n_flagged
            continue
        ci, wi = int(cc), int(ww)
        correct += ci
        wrong += wi
        by_band[band][0] += ci
        by_band[band][1] += wi

    total = correct + wrong
    print("\n--- Flagged windows vs match outcome (resolved markets only) ---", flush=True)
    print(f"  Aligned with final result: {correct}", flush=True)
    print(f"  Not aligned:               {wrong}", flush=True)
    if total:
        print(f"  Hit rate: {100.0 * correct / total:.1f}% ({correct}/{total})", flush=True)
    if unresolved_flag_windows:
        print(
            f"  Excluded (unresolved market but had {unresolved_flag_windows} flagged window(s))",
            flush=True,
        )
    if any(v[0] + v[1] for v in by_band.values()):
        print("  By rou1_window band:", flush=True)
        for band in sorted(by_band.keys()):
            c, w = by_band[band]
            t = c + w
            if t == 0:
                continue
            print(
                f"    {band}: aligned={c}, not_aligned={w}  (hit_rate={100.0 * c / t:.1f}%)",
                flush=True,
            )


def _binom_pmf(n: int, k: int, p: float) -> float:
    return math.comb(n, k) * (p**k) * ((1.0 - p) ** (n - k))


def _binom_sf(n: int, p: float, k0: int) -> float:
    """P(X >= k0) for X ~ Binomial(n, p)."""
    return sum(_binom_pmf(n, k, p) for k in range(k0, n + 1))


def _binom_cdf(n: int, p: float, k1: int) -> float:
    """P(X <= k1) for X ~ Binomial(n, p)."""
    return sum(_binom_pmf(n, k, p) for k in range(0, k1 + 1))


def _binom_two_sided_vs_half(n: int, k: int) -> tuple[float, float]:
    """
    Exact p-values for Binomial(n, 0.5): k = observed “success” count.
    Returns (one-sided P(X >= k), two-sided p-value, symmetric tails).
    """
    if n <= 0:
        return (float("nan"), float("nan"))
    p0 = 0.5
    one_sided = _binom_sf(n, p0, k)
    mu = n * p0
    if k > mu:
        two_sided = _binom_sf(n, p0, k) + _binom_cdf(n, p0, n - k)
    elif k < mu:
        two_sided = _binom_cdf(n, p0, k) + _binom_sf(n, p0, n - k)
    else:
        two_sided = 1.0
    return (one_sided, two_sided)


def _wilson_ci_95(k: int, n: int) -> tuple[float, float]:
    """Wilson score interval for binomial proportion (95%)."""
    if n <= 0:
        return (float("nan"), float("nan"))
    z = 1.96
    phat = k / n
    denom = 1.0 + z * z / n
    center = (phat + z * z / (2.0 * n)) / denom
    rad = z * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * n)) / n) / denom
    return (max(0.0, center - rad), min(1.0, center + rad))


def print_match_majority_alignment_report(summaries: list[MatchSummary]) -> None:
    """
    Per match with ≥1 resolved flagged window: majority of flags aligned with outcome?
    Reports count of matches (majority correct / majority wrong / tie), Wilson 95% CI, and
    exact binomial p-values vs p=0.5 (match is the unit of replication).
    """
    maj_ok = 0
    maj_bad = 0
    ties = 0
    by_band: dict[str, list[int]] = {}  # band -> [maj_ok, maj_bad, ties]

    for s in summaries:
        if s.status != "ok" or s.n_flagged <= 0:
            continue
        cc, ww = s.n_flagged_correct, s.n_flagged_wrong
        if cc == "" or ww == "":
            continue
        c, w = int(cc), int(ww)
        band = s.rou1_window or "unknown"
        if band not in by_band:
            by_band[band] = [0, 0, 0]
        if c > w:
            maj_ok += 1
            by_band[band][0] += 1
        elif w > c:
            maj_bad += 1
            by_band[band][1] += 1
        else:
            ties += 1
            by_band[band][2] += 1

    n_decisive = maj_ok + maj_bad
    print("\n--- Per-match majority (resolved, ≥1 flag): aligned vs not ---", flush=True)
    print(
        f"  Majority of flagged windows aligned: {maj_ok}  |  majority not aligned: {maj_bad}  |  ties: {ties}",
        flush=True,
    )
    if n_decisive == 0:
        print("  No decisive matches (need at least one flag and c≠w).", flush=True)
        return

    phat = maj_ok / n_decisive
    lo, hi = _wilson_ci_95(maj_ok, n_decisive)
    one_s, two_s = _binom_two_sided_vs_half(n_decisive, maj_ok)
    print(f"  Decisive matches n={n_decisive}  point_est={100.0 * phat:.1f}% ({maj_ok}/{n_decisive})", flush=True)
    print(f"  Wilson 95% CI for P(majority aligns): [{100.0 * lo:.1f}%, {100.0 * hi:.1f}%]", flush=True)
    print(
        f"  Binomial vs p=0.5 (H0: coin flip which side wins majority): "
        f"one-sided P(X>={maj_ok})={one_s:.4g}  two-sided={two_s:.4g}",
        flush=True,
    )

    if any(sum(b[:2]) > 0 for b in by_band.values()):
        print("  By rou1_window (decisive matches only):", flush=True)
        for band in sorted(by_band.keys()):
            ok_b, bad_b, t_b = by_band[band]
            nb = ok_b + bad_b
            if nb == 0:
                continue
            lo_b, hi_b = _wilson_ci_95(ok_b, nb)
            o_b, tw_b = _binom_two_sided_vs_half(nb, ok_b)
            tie_note = f"  ties={t_b}" if t_b else ""
            print(
                f"    {band}: n={nb}  majority_aligned={ok_b}/{nb} ({100.0 * ok_b / nb:.1f}%)"
                f"  Wilson95=[{100.0 * lo_b:.1f}%, {100.0 * hi_b:.1f}%]"
                f"  p_two={tw_b:.4g}{tie_note}",
                flush=True,
            )


def print_flagged_p_yes_end_aligned_report(summaries: list[MatchSummary]) -> None:
    """
    Mean YES mid at window end (``p_yes_end``) for flagged windows that aligned vs did not,
    overall and by ``rou1_window`` (resolved markets with ``status=ok`` only).
    """
    overall_ok: list[float] = []
    overall_bad: list[float] = []
    by_band: dict[str, tuple[list[float], list[float]]] = {}

    for s in summaries:
        if s.status != "ok" or s.window_rows is None or s.yes_won is None:
            continue
        band = s.rou1_window or "unknown"
        if band not in by_band:
            by_band[band] = ([], [])
        yes_won = s.yes_won
        ok_list, bad_list = by_band[band]
        for r in s.window_rows:
            fk = r.get("flag_kind")
            if fk not in ("buy_yes", "sell_yes"):
                continue
            p_raw = r.get("p_yes_end")
            if not isinstance(p_raw, (int, float)):
                continue
            p = float(p_raw)
            aligned = (fk == "buy_yes" and yes_won) or (fk == "sell_yes" and not yes_won)
            if aligned:
                overall_ok.append(p)
                ok_list.append(p)
            else:
                overall_bad.append(p)
                bad_list.append(p)

    def _line(label: str, good: list[float], bad: list[float]) -> None:
        if not good and not bad:
            return
        g_mean = statistics.mean(good) if good else float("nan")
        b_mean = statistics.mean(bad) if bad else float("nan")
        g_s = f"{g_mean:.4f} ({100.0 * g_mean:.1f}%)" if good else "n/a"
        b_s = f"{b_mean:.4f} ({100.0 * b_mean:.1f}%)" if bad else "n/a"
        print(
            f"    {label}: mean p(YES) end — aligned n={len(good)} → {g_s}; "
            f"not_aligned n={len(bad)} → {b_s}",
            flush=True,
        )

    print("\n--- Mean implied YES price at flag (window end), resolved matches only ---", flush=True)
    _line("all", overall_ok, overall_bad)
    for band in sorted(by_band.keys()):
        ok_l, bad_l = by_band[band]
        if not ok_l and not bad_l:
            continue
        _line(band, ok_l, bad_l)


def run_tagged_batch_csv(
    out_path: str,
    tagged: list[tuple[dict, str]],
    market_hint: str | None,
    label_for_log: str,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> None:
    """Analyze each ``(gamma_event, batch_band)``, write CSV, print standard batch reports."""
    summaries: list[MatchSummary] = []
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for ev, band in tagged:
            slug = str(ev.get("slug") or "").strip()
            if not slug:
                continue
            summary = analyze_match(
                slug,
                market_hint,
                large_trade_cut_mode=large_trade_cut_mode,
                histogram_bins=histogram_bins,
            )
            summary.rou1_window = band
            summaries.append(summary)
            if summary.status != "ok":
                print(f"{slug}: {summary.status} — {summary.error}", file=sys.stderr, flush=True)
            writer.writerow(match_summary_to_csv_row(summary))

    print(f"Wrote {len(tagged)} row(s) to {out_path} ({label_for_log})", flush=True)
    print_flagged_outcome_vs_resolution_report(summaries)
    print_flagged_p_yes_end_aligned_report(summaries)
    print_match_majority_alignment_report(summaries)


def run_week3_batch_csv(
    out_path: str,
    year: int,
    market_hint: str | None,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> None:
    """Discover ROU1 Week 1 + Week 3 Gamma events, analyze each, write CSV and print alignment summary."""
    try:
        tagged = fetch_rou1_week1_and_week3_events(year)
    except PolymarketRequestError as e:
        print(str(e), file=sys.stderr, flush=True)
        raise SystemExit(1) from e
    lab = f"ROU1 week1+week3 year={year}"
    if large_trade_cut_mode == "histogram":
        lab += f" large_cut=histogram bins={histogram_bins}"
    run_tagged_batch_csv(
        out_path,
        tagged,
        market_hint,
        lab,
        large_trade_cut_mode=large_trade_cut_mode,
        histogram_bins=histogram_bins,
    )


def run_csl_march_april_batch_csv(
    out_path: str,
    year: int,
    market_hint: str | None,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> None:
    """Chinese Super League: ``endDate`` UTC from Mar 6 through Apr 6 (inclusive) of ``year``."""
    try:
        tagged = fetch_csl_events_march_april(year)
    except PolymarketRequestError as e:
        print(str(e), file=sys.stderr, flush=True)
        raise SystemExit(1) from e
    lab = f"CSL Mar6–Apr6 year={year}"
    if large_trade_cut_mode == "histogram":
        lab += f" large_cut=histogram bins={histogram_bins}"
    run_tagged_batch_csv(
        out_path,
        tagged,
        market_hint,
        lab,
        large_trade_cut_mode=large_trade_cut_mode,
        histogram_bins=histogram_bins,
    )


def run_j2_feb28_batch_csv(
    out_path: str,
    year: int,
    market_hint: str | None,
    *,
    large_trade_cut_mode: str = "exact",
    histogram_bins: int = 64,
) -> None:
    """Japan J2: ``endDate`` UTC from Feb 28 through Apr 6 (inclusive) of ``year``."""
    try:
        tagged = fetch_j2_events_feb28_apr6(year)
    except PolymarketRequestError as e:
        print(str(e), file=sys.stderr, flush=True)
        raise SystemExit(1) from e
    lab = f"J2 Feb28–Apr6 year={year}"
    if large_trade_cut_mode == "histogram":
        lab += f" large_cut=histogram bins={histogram_bins}"
    run_tagged_batch_csv(
        out_path,
        tagged,
        market_hint,
        lab,
        large_trade_cut_mode=large_trade_cut_mode,
        histogram_bins=histogram_bins,
    )


def main() -> None:
    """CLI entry: ``--slug`` or batch ``--rou1-week3-csv`` / ``--csl-mar-apr-csv`` / ``--j2-feb28-csv``."""
    ap = argparse.ArgumentParser(description="Smart-money proxy backtest on Polymarket trade + price history.")
    ap.add_argument(
        "--slug",
        default=DEFAULT_SLUG,
        help="Gamma event slug (ignored when a batch --*-csv flag is set).",
    )
    ap.add_argument(
        "--market-hint",
        default="",
        help="Substring of market question to pick (default: first market).",
    )
    ap.add_argument(
        "--rou1-week3-csv",
        default="",
        metavar="PATH",
        help="Batch: fetch Romanian SuperLiga (series 10971) matches with endDate UTC in "
        "Week 1 (Jan 24–Feb 9) and Week 3 (Feb 27–Mar 23) of --year (default 2026); "
        "analyze each; write CSV to PATH; print alignment counts and mean p(YES) at flag by band.",
    )
    ap.add_argument(
        "--csl-mar-apr-csv",
        default="",
        metavar="PATH",
        help="Batch: Chinese Super League (series 10439), endDate UTC Mar 6–Apr 6 inclusive "
        "of --year (default 2026); write CSV to PATH; same reports as ROU1 batch.",
    )
    ap.add_argument(
        "--j2-feb28-csv",
        default="",
        metavar="PATH",
        help="Batch: Japan J2 League (series 10443), endDate UTC Feb 28–Apr 6 inclusive "
        "of --year (default 2026); write CSV to PATH; same reports as other batches.",
    )
    ap.add_argument(
        "--year",
        type=int,
        default=2026,
        help="Calendar year for batch date bands, default 2026.",
    )
    ap.add_argument(
        "--large-trade-cut",
        choices=("exact", "histogram"),
        default="exact",
        help="Large-trade cutoff: exact prefix quantile (default) or histogram sketch (see sketch_quantile.py).",
    )
    ap.add_argument(
        "--histogram-bins",
        type=int,
        default=64,
        metavar="N",
        help="Number of fixed log-spaced bins (production range) for --large-trade-cut histogram (default 64).",
    )
    args = ap.parse_args()
    hint = args.market_hint or None
    rou1_path = (args.rou1_week3_csv or "").strip()
    csl_path = (args.csl_mar_apr_csv or "").strip()
    j2_path = (args.j2_feb28_csv or "").strip()
    batch_flags = sum(1 for p in (rou1_path, csl_path, j2_path) if p)
    if batch_flags > 1:
        print(
            "Use only one of --rou1-week3-csv, --csl-mar-apr-csv, or --j2-feb28-csv.",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(2)
    lt = args.large_trade_cut
    hb = max(2, args.histogram_bins)
    if rou1_path:
        run_week3_batch_csv(
            rou1_path,
            args.year,
            hint,
            large_trade_cut_mode=lt,
            histogram_bins=hb,
        )
    elif csl_path:
        run_csl_march_april_batch_csv(
            csl_path,
            args.year,
            hint,
            large_trade_cut_mode=lt,
            histogram_bins=hb,
        )
    elif j2_path:
        run_j2_feb28_batch_csv(
            j2_path,
            args.year,
            hint,
            large_trade_cut_mode=lt,
            histogram_bins=hb,
        )
    else:
        run_backtest(
            args.slug,
            hint,
            large_trade_cut_mode=lt,
            histogram_bins=hb,
        )


if __name__ == "__main__":
    main()

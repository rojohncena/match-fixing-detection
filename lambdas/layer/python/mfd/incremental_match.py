"""
Production-style incremental monitor: fetch only **new** trades since a watermark, maintain
fixed-edge histogram counts for YES/NO prefix sizes, per-window directional notionals, and
prefix snapshots at each window end — matching ``analyze_match`` histogram mode without
re-downloading the full tape each tick.

``exact`` large-trade mode is not supported here; use ``analyze_match`` (full replay) instead.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from mfd.sketch_quantile import (
    bin_index_for_edges,
    build_log_spaced_edges,
    quantile_from_histogram_counts,
)

from mfd.pipeline import (
    FORWARD_SEC,
    LARGE_TRADE_Q,
    FLOW_RATIO,
    MATCH_PAD_AFTER,
    MIN_DELTA_P,
    MIN_WINDOW_NOTIONAL,
    WINDOW_SEC,
    PolymarketRequestError,
    WindowStat,
    MatchSummary,
    _fetch_event_slug,
    _fetch_price_history,
    _kickoff_utc,
    _leg_hits,
    _pick_binary_assets,
    _price_at_or_before,
    _resolved_yes_won,
    fetch_trades_event_since,
)


@dataclass
class IncrementalState:
    slug: str
    event_id: int
    kick_ts: int
    end_ts: int
    n_windows: int
    yes_asset: str
    no_asset: str | None
    histogram_bins: int
    watermark_ts: int = 0
    hist_yes: list[int] = field(default_factory=list)
    hist_no: list[int] = field(default_factory=list)
    windows_yes: list[dict[str, float | int]] = field(default_factory=list)
    windows_no: list[dict[str, float | int]] = field(default_factory=list)
    prefix_snap_yes: dict[int, list[int]] = field(default_factory=dict)
    prefix_snap_no: dict[int, list[int]] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "v": 1,
            "slug": self.slug,
            "event_id": self.event_id,
            "kick_ts": self.kick_ts,
            "end_ts": self.end_ts,
            "n_windows": self.n_windows,
            "yes_asset": self.yes_asset,
            "no_asset": self.no_asset,
            "histogram_bins": self.histogram_bins,
            "watermark_ts": self.watermark_ts,
            "hist_yes": self.hist_yes,
            "hist_no": self.hist_no,
            "windows_yes": self.windows_yes,
            "windows_no": self.windows_no,
            "prefix_snap_yes": {str(k): v for k, v in self.prefix_snap_yes.items()},
            "prefix_snap_no": {str(k): v for k, v in self.prefix_snap_no.items()},
        }

    @staticmethod
    def from_json(d: dict[str, Any]) -> IncrementalState:
        def _snap(raw: object) -> dict[int, list[int]]:
            out: dict[int, list[int]] = {}
            if isinstance(raw, dict):
                for k, v in raw.items():
                    out[int(k)] = [int(x) for x in v]  # type: ignore[arg-type]
            return out

        return IncrementalState(
            slug=str(d["slug"]),
            event_id=int(d["event_id"]),
            kick_ts=int(d["kick_ts"]),
            end_ts=int(d["end_ts"]),
            n_windows=int(d["n_windows"]),
            yes_asset=str(d["yes_asset"]),
            no_asset=str(d["no_asset"]) if d.get("no_asset") else None,
            histogram_bins=int(d.get("histogram_bins", 64)),
            watermark_ts=int(d.get("watermark_ts", 0)),
            hist_yes=[int(x) for x in d.get("hist_yes", [])],
            hist_no=[int(x) for x in d.get("hist_no", [])],
            windows_yes=list(d.get("windows_yes") or []),
            windows_no=list(d.get("windows_no") or []),
            prefix_snap_yes=_snap(d.get("prefix_snap_yes")),
            prefix_snap_no=_snap(d.get("prefix_snap_no")),
        )


def _blank_window_row(i: int, kick_ts: int) -> dict[str, float | int]:
    return {
        "idx": i,
        "t0": kick_ts + i * WINDOW_SEC,
        "t1": kick_ts + (i + 1) * WINDOW_SEC,
        "buy_notional": 0.0,
        "sell_notional": 0.0,
        "buy_size": 0.0,
        "sell_size": 0.0,
        "max_trade": 0.0,
        "n_trades": 0,
    }


def _row_to_ws(d: dict[str, float | int], i: int, kick_ts: int) -> WindowStat:
    return WindowStat(
        idx=i,
        t0=int(d.get("t0", kick_ts + i * WINDOW_SEC)),
        t1=int(d.get("t1", kick_ts + (i + 1) * WINDOW_SEC)),
        buy_notional=float(d["buy_notional"]),
        sell_notional=float(d["sell_notional"]),
        buy_size=float(d["buy_size"]),
        sell_size=float(d["sell_size"]),
        max_trade=float(d["max_trade"]),
        n_trades=int(d["n_trades"]),
    )


def _apply_one_trade(
    row: dict[str, object],
    *,
    kick_ts: int,
    end_ts: int,
    n_win: int,
    yes_asset: str,
    no_asset: str | None,
    hist_yes: list[int],
    hist_no: list[int],
    edges: list[float],
    windows_yes: list[dict[str, float | int]],
    windows_no: list[dict[str, float | int]],
) -> None:
    ts = int(row["timestamp"])
    if ts < kick_ts or ts > end_ts:
        return
    aid = str(row.get("asset"))
    if aid not in (yes_asset, no_asset):
        return
    size = float(row["size"])
    price = float(row["price"])
    notional = size * price
    side = row.get("side")
    wix = min((ts - kick_ts) // WINDOW_SEC, n_win - 1)
    if aid == yes_asset:
        bi = bin_index_for_edges(size, edges)
        hist_yes[bi] += 1
        b = windows_yes[wix]
        b["max_trade"] = max(float(b["max_trade"]), size)
        b["n_trades"] = int(b["n_trades"]) + 1
        if side == "BUY":
            b["buy_notional"] = float(b["buy_notional"]) + notional
            b["buy_size"] = float(b["buy_size"]) + size
        elif side == "SELL":
            b["sell_notional"] = float(b["sell_notional"]) + notional
            b["sell_size"] = float(b["sell_size"]) + size
    elif no_asset and aid == no_asset:
        bi = bin_index_for_edges(size, edges)
        hist_no[bi] += 1
        b = windows_no[wix]
        b["max_trade"] = max(float(b["max_trade"]), size)
        b["n_trades"] = int(b["n_trades"]) + 1
        if side == "BUY":
            b["buy_notional"] = float(b["buy_notional"]) + notional
            b["buy_size"] = float(b["buy_size"]) + size
        elif side == "SELL":
            b["sell_notional"] = float(b["sell_notional"]) + notional
            b["sell_size"] = float(b["sell_size"]) + size


def _ingest_trades_into_state(
    state: IncrementalState,
    new_trades: list[dict[str, object]],
    edges: list[float],
) -> int:
    """
    Apply new trades in time order. After each window end boundary, snapshot prefix histograms
    (matches full-tape replay). Trades after the last window end only extend running hists.
    Returns max timestamp applied from ``new_trades`` (or ``state.watermark_ts`` if empty).
    """
    if not new_trades:
        return state.watermark_ts
    new_trades = sorted(new_trades, key=lambda r: int(r["timestamp"]))
    kick_ts = state.kick_ts
    end_ts = state.end_ts
    n_win = state.n_windows
    yes_a = state.yes_asset
    no_a = state.no_asset
    ptr = 0
    max_ts = state.watermark_ts
    for i in range(n_win):
        boundary = kick_ts + (i + 1) * WINDOW_SEC
        while ptr < len(new_trades) and int(new_trades[ptr]["timestamp"]) <= boundary:
            _apply_one_trade(
                new_trades[ptr],
                kick_ts=kick_ts,
                end_ts=end_ts,
                n_win=n_win,
                yes_asset=yes_a,
                no_asset=no_a,
                hist_yes=state.hist_yes,
                hist_no=state.hist_no,
                edges=edges,
                windows_yes=state.windows_yes,
                windows_no=state.windows_no,
            )
            max_ts = max(max_ts, int(new_trades[ptr]["timestamp"]))
            ptr += 1
        state.prefix_snap_yes[i] = list(state.hist_yes)
        state.prefix_snap_no[i] = list(state.hist_no)
    while ptr < len(new_trades):
        _apply_one_trade(
            new_trades[ptr],
            kick_ts=kick_ts,
            end_ts=end_ts,
            n_win=n_win,
            yes_asset=yes_a,
            no_asset=no_a,
            hist_yes=state.hist_yes,
            hist_no=state.hist_no,
            edges=edges,
            windows_yes=state.windows_yes,
            windows_no=state.windows_no,
        )
        max_ts = max(max_ts, int(new_trades[ptr]["timestamp"]))
        ptr += 1
    return max_ts


def _prefix_hist(snap: dict[int, list[int]], running: list[int], i: int) -> list[int]:
    s = snap.get(i)
    return s if s is not None else running


def analyze_match_incremental(
    slug: str,
    state_json: dict[str, Any] | None,
    market_hint: str | None = None,
    *,
    histogram_bins: int = 64,
    clock_now_ts: int | None = None,
) -> tuple[MatchSummary, dict[str, Any]]:
    """
    Histogram-mode incremental pipeline. Pass ``state_json`` from the previous Dynamo tick
    (or ``None`` to cold-start). Returns ``(MatchSummary, state_json_for_next_tick)``.
    """
    clock_now_ts = int(clock_now_ts or datetime.now(timezone.utc).timestamp())

    def fail(status: str, msg: str, **fields: object) -> tuple[MatchSummary, dict[str, Any]]:
        m = MatchSummary(slug=slug, status=status, error=msg)
        for k, v in fields.items():
            if hasattr(m, k):
                setattr(m, k, v)
        return m, {}

    hist_edges = build_log_spaced_edges(histogram_bins)
    n_bins = len(hist_edges) - 1

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

    n_win = max(1, (end_ts_i - kick_ts + WINDOW_SEC) // WINDOW_SEC)

    if state_json and int(state_json.get("event_id", 0)) == eid and str(state_json.get("slug")) == slug:
        state = IncrementalState.from_json(state_json)
    else:
        state = IncrementalState(
            slug=slug,
            event_id=eid,
            kick_ts=kick_ts,
            end_ts=end_ts_i,
            n_windows=n_win,
            yes_asset=yes_asset,
            no_asset=no_asset,
            histogram_bins=histogram_bins,
            watermark_ts=kick_ts - 1,
            hist_yes=[0] * n_bins,
            hist_no=[0] * n_bins,
            windows_yes=[_blank_window_row(i, kick_ts) for i in range(n_win)],
            windows_no=[_blank_window_row(i, kick_ts) for i in range(n_win)],
        )

    if len(state.hist_yes) != n_bins:
        state.hist_yes = [0] * n_bins
        state.hist_no = [0] * n_bins
    if len(state.windows_yes) != n_win:
        state.windows_yes = [_blank_window_row(i, kick_ts) for i in range(n_win)]
        state.windows_no = [_blank_window_row(i, kick_ts) for i in range(n_win)]

    trades_this_tick = 0
    try:
        batch = fetch_trades_event_since(
            eid,
            state.watermark_ts,
            min_ts_inclusive=kick_ts,
            max_ts_inclusive=min(end_ts_i, clock_now_ts),
        )
        trades_this_tick = len(batch)
        max_applied = _ingest_trades_into_state(state, batch, hist_edges)
        state.watermark_ts = max(state.watermark_ts, max_applied)
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

    w_yes = [_row_to_ws(state.windows_yes[i], i, kick_ts) for i in range(n_win)]
    if no_asset:
        w_no = [_row_to_ws(state.windows_no[i], i, kick_ts) for i in range(n_win)]
    else:
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
        out = fail(
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
            trades_fetched=trades_this_tick,
            kick_dt=kick,
            end_ts=end_ts_i,
            n_windows_tape=0,
        )
        return out[0], state.to_json()

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
            trades_fetched=trades_this_tick,
            kick_dt=kick,
            end_ts=end_ts_i,
        )

    window_rows: list[dict[str, object]] = []
    for i in active_idx:
        wy, wn = w_yes[i], w_no[i]
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

        hy = _prefix_hist(state.prefix_snap_yes, state.hist_yes, i)
        large_cut_yes = quantile_from_histogram_counts(hy, hist_edges, LARGE_TRADE_Q)
        if no_asset:
            hn = _prefix_hist(state.prefix_snap_no, state.hist_no, i)
            large_cut_no = quantile_from_histogram_counts(hn, hist_edges, LARGE_TRADE_Q)
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
        m = MatchSummary(
            slug=slug,
            status="insufficient_windows",
            error="Too few windows with usable price history — widen time bounds or relax filters.",
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
            trades_fetched=trades_this_tick,
            price_points=len(price_series),
            window_rows=window_rows,
            kick_dt=kick,
            end_ts=end_ts_i,
        )
        return m, state.to_json()

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

    summary = MatchSummary(
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
        trades_fetched=trades_this_tick,
        price_points=len(price_series),
        kick_dt=kick,
        end_ts=end_ts_i,
    )
    return summary, state.to_json()

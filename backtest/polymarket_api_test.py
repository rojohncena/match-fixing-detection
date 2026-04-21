#!/usr/bin/env python3
"""
Fetch the first 5 Polymarket *trade fills* (matched orders) during the scheduled
match window for AFC Unirea Slobozia vs. ASC Oțelul Galați on 2026-03-23.

Uses the public Data API (no key). Kickoff is read from the Gamma event’s
`endDate` (Polymarket’s scheduled start, 2026-03-23T18:30:00+00:00 for this
event — i.e. 1:30 PM if you use UTC−5).

The `/trades` endpoint returns recent trades first; we page backward until every
trade at or after kickoff has been seen, filter to the live window, then sort by
time and take the first five.
"""

from __future__ import annotations

import json
import ssl
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

# Event slug from Polymarket URL /event/rou1-aus-aog-2026-03-23
EVENT_SLUG = "rou1-aus-aog-2026-03-23"

# Cover 90 min + half-time + stoppage (adjust if you want a tighter window).
MATCH_DURATION = timedelta(hours=2, minutes=15)

FIRST_N = 5
PAGE_SIZE = min(1000, 10_000)


def _get_json(url: str) -> dict | list:
    req = Request(url, headers={"Accept": "application/json"})
    ctx = ssl.create_default_context()
    try:
        with urlopen(req, context=ctx, timeout=60) as resp:
            return json.load(resp)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {e.code} for {url}\n{body}") from e
    except URLError as e:
        raise SystemExit(f"Request failed: {e}") from e


def _fetch_event_by_slug(slug: str) -> dict:
    url = f"{GAMMA_BASE}/events/slug/{slug}"
    data = _get_json(url)
    if not isinstance(data, dict) or not data.get("id"):
        raise SystemExit(f"Unexpected event payload for slug={slug!r}")
    return data


def _kickoff_and_end_utc(event: dict) -> tuple[datetime, datetime]:
    raw = event.get("endDate")
    if not raw or not isinstance(raw, str):
        raise SystemExit("Event missing string endDate (scheduled kickoff).")
    kickoff = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=timezone.utc)
    return kickoff, kickoff + MATCH_DURATION


def _fetch_trades_page(event_id: int, offset: int) -> list[dict]:
    q = urlencode(
        {
            "eventId": event_id,
            "limit": PAGE_SIZE,
            "offset": offset,
            "takerOnly": "false",
        }
    )
    url = f"{DATA_BASE}/trades?{q}"
    data = _get_json(url)
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected /trades response type: {type(data)}")
    return data


def first_trades_during_match() -> list[dict]:
    event = _fetch_event_by_slug(EVENT_SLUG)
    eid = int(event["id"])
    title = event.get("title", EVENT_SLUG)
    start_utc, end_utc = _kickoff_and_end_utc(event)
    start_ts = int(start_utc.timestamp())
    end_ts = int(end_utc.timestamp())

    print(f"Event: {title} (id={eid})", flush=True)
    print(
        f"Match window (UTC): {start_utc.isoformat()} → {end_utc.isoformat()}",
        flush=True,
    )

    in_window: list[dict] = []
    offset = 0
    oldest_ts: int | None = None

    while True:
        batch = _fetch_trades_page(eid, offset)
        if not batch:
            break
        for row in batch:
            ts = int(row["timestamp"])
            if start_ts <= ts <= end_ts:
                in_window.append(row)
        oldest_ts = int(batch[-1]["timestamp"])
        offset += len(batch)
        # Trades are newest-first; stop once this page’s oldest is before kickoff.
        if oldest_ts < start_ts or len(batch) < PAGE_SIZE:
            break
        if offset > 200_000:
            raise SystemExit("Stopped pagination after 200k rows (safety cap).")

    in_window.sort(key=lambda r: int(r["timestamp"]))
    return in_window[:FIRST_N]


def main() -> None:
    rows = first_trades_during_match()
    if not rows:
        print("No trades in that time window (or pagination did not reach them).", flush=True)
        return
    print(f"\nFirst {len(rows)} trade(s) during the match (chronological):\n", flush=True)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()

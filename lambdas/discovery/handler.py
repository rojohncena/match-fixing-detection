"""
Discovery Lambda: paginate Gamma ``events?series_id=…`` for configured leagues,
upsert upcoming matches into DynamoDB so Monitor can evaluate them.

Environment:
  MATCHES_TABLE   — DynamoDB table name (required)
  LOOKAHEAD_DAYS  — UTC calendar window [today, today+N] for event endDate (default 14)
  SERIES_IDS      — comma-separated Gamma series ids (default: ROU1, CSL, J2)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

from mfd.pipeline import (
    CSL_SERIES_ID,
    J2_SERIES_ID,
    ROU1_SERIES_ID,
    PolymarketRequestError,
    fetch_gamma_series_events_in_range,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_DEFAULT_SERIES = f"{ROU1_SERIES_ID},{CSL_SERIES_ID},{J2_SERIES_ID}"


def lambda_handler(event: object, context: object) -> dict[str, object]:
    del context
    table_name = os.environ.get("MATCHES_TABLE")
    if not table_name:
        return {"statusCode": 500, "body": json.dumps({"error": "MATCHES_TABLE is not set"})}

    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", "14"))
    raw_ids = (os.environ.get("SERIES_IDS") or _DEFAULT_SERIES).strip()
    series_ids = [int(x.strip()) for x in raw_ids.split(",") if x.strip()]

    today = datetime.now(timezone.utc).date()
    d_end = today + timedelta(days=lookahead)

    table = boto3.resource("dynamodb").Table(table_name)
    upserts = 0
    errors: list[str] = []

    for sid in series_ids:
        try:
            events = fetch_gamma_series_events_in_range(sid, today, d_end)
        except PolymarketRequestError as e:
            logger.exception("Gamma fetch failed series_id=%s", sid)
            errors.append(f"series {sid}: {e}")
            continue

        for ev in events:
            slug = str(ev.get("slug") or "").strip()
            if not slug:
                continue
            eid = ev.get("id")
            if eid is None:
                continue
            raw_end = ev.get("endDate") or ""
            title = str(ev.get("title") or "")
            existing = table.get_item(Key={"slug": slug}).get("Item") or {}
            item = {
                "slug": slug,
                "event_id": int(eid),
                "title": title,
                "kickoff_iso": str(raw_end),
                "series_id": sid,
                "status": "scheduled",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
            # Preserve Monitor Lambda incremental tape state across discovery refreshes.
            if "monitor_state" in existing and existing["monitor_state"]:
                item["monitor_state"] = existing["monitor_state"]
            table.put_item(Item=item)
            upserts += 1

    body = {
        "date_start": today.isoformat(),
        "date_end": d_end.isoformat(),
        "series_ids": series_ids,
        "upserts": upserts,
        "errors": errors,
    }
    return {"statusCode": 200 if not errors else 207, "body": json.dumps(body)}

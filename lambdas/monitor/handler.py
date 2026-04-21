"""
Monitor Lambda: incremental histogram pipeline (default) or full-replay ``analyze_match``.

**Incremental (``MONITOR_MODE=incremental`` and ``LARGE_TRADE_CUT=histogram``):**
  Only fetches trades with ``timestamp > watermark`` (see ``fetch_trades_event_since``), maintains
  fixed-edge bin counts and per-window notionals in DynamoDB ``monitor_state``.

**Legacy (``MONITOR_MODE=legacy`` or ``LARGE_TRADE_CUT=exact``):**
  Calls ``mfd.pipeline.analyze_match`` — full trade pull each invocation.

Environment:
  MATCHES_TABLE            — DynamoDB (required)
  LARGE_TRADE_CUT          — ``histogram`` (default) or ``exact``
  MONITOR_MODE             — ``incremental`` (default) or ``legacy``
  HISTOGRAM_BINS           — default 64
  ALERT_SNS_TOPIC_ARN      — optional SNS publish when ``n_flagged > 0`` and status ok
  MAX_SLUGS_PER_INVOCATION — default 8

Manual payload: ``{"slugs": ["rou1-…"]}`` bypasses the DynamoDB scan.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import boto3

from mfd.incremental_match import analyze_match_incremental
from mfd.pipeline import MATCH_PAD_AFTER, analyze_match, match_summary_to_csv_row

TABLE_KEY = "slug"
STATE_ATTR = "monitor_state"


def _parse_kickoff(raw: str) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _slug_in_live_window(now: datetime, kickoff_iso: str) -> bool:
    kick = _parse_kickoff(kickoff_iso)
    if kick is None:
        return False
    kick_ts = int(kick.timestamp())
    end_ts = kick_ts + int(MATCH_PAD_AFTER.total_seconds())
    now_ts = int(now.timestamp())
    return kick_ts <= now_ts <= end_ts


def _scan_slugs_live_window(table, *, max_slugs: int) -> list[str]:
    now = datetime.now(timezone.utc)
    out: list[str] = []
    scan_kwargs: dict[str, object] = {}
    while True:
        resp = table.scan(**scan_kwargs)
        for row in resp.get("Items", []):
            raw = row.get("kickoff_iso")
            if not isinstance(raw, str):
                continue
            if _slug_in_live_window(now, raw):
                s = row.get("slug")
                if isinstance(s, str) and s:
                    out.append(s)
                    if len(out) >= max_slugs:
                        return out
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek
    return out


def lambda_handler(event: object, context: object) -> dict[str, object]:
    del context
    table_name = os.environ.get("MATCHES_TABLE")
    if not table_name:
        return {"statusCode": 500, "body": json.dumps({"error": "MATCHES_TABLE is not set"})}

    mode = os.environ.get("LARGE_TRADE_CUT", "histogram")
    if mode not in ("exact", "histogram"):
        mode = "histogram"
    monitor_mode = (os.environ.get("MONITOR_MODE") or "incremental").strip().lower()
    use_incremental = monitor_mode == "incremental" and mode == "histogram"
    bins = max(2, int(os.environ.get("HISTOGRAM_BINS", "64")))
    topic_arn = (os.environ.get("ALERT_SNS_TOPIC_ARN") or "").strip()
    max_slugs = max(1, int(os.environ.get("MAX_SLUGS_PER_INVOCATION", "8")))

    slugs: list[str] = []
    if isinstance(event, dict) and event.get("slugs"):
        raw = event["slugs"]
        if isinstance(raw, list):
            slugs = [str(s).strip() for s in raw if str(s).strip()][:max_slugs]
    if not slugs:
        table = boto3.resource("dynamodb").Table(table_name)
        slugs = _scan_slugs_live_window(table, max_slugs=max_slugs)

    table = boto3.resource("dynamodb").Table(table_name)
    results: list[dict[str, str]] = []
    sns = boto3.client("sns") if topic_arn else None

    for slug in slugs:
        if use_incremental:
            resp = table.get_item(Key={TABLE_KEY: slug})
            item = resp.get("Item") or {}
            raw_state = item.get(STATE_ATTR)
            state_json: dict[str, object] | None = None
            if isinstance(raw_state, str) and raw_state.strip():
                try:
                    state_json = json.loads(raw_state)
                except json.JSONDecodeError:
                    state_json = None
            summary, new_state = analyze_match_incremental(
                slug,
                state_json,
                None,
                histogram_bins=bins,
            )
            row = match_summary_to_csv_row(summary)
            results.append(row)
            if new_state:
                table.update_item(
                    Key={TABLE_KEY: slug},
                    UpdateExpression=f"SET {STATE_ATTR} = :ms",
                    ExpressionAttributeValues={":ms": json.dumps(new_state)},
                )
        else:
            summary = analyze_match(
                slug,
                None,
                large_trade_cut_mode=mode,
                histogram_bins=bins,
            )
            row = match_summary_to_csv_row(summary)
            results.append(row)

        if (
            sns
            and topic_arn
            and summary.status == "ok"
            and summary.n_flagged > 0
        ):
            sns.publish(
                TopicArn=topic_arn,
                Subject=f"mfd: flags on {slug} (n={summary.n_flagged})",
                Message=json.dumps(row, indent=2),
            )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "mode": "incremental" if use_incremental else "legacy",
                "analyzed": len(results),
                "rows": results,
            },
            default=str,
        ),
    }

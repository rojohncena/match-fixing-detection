# AWS Lambda — Discovery + Monitor

Two functions plus a shared layer. The **Monitor** defaults to an **incremental** histogram pipeline (`mfd.incremental_match`) that matches histogram-mode `analyze_match` while only pulling **new** trades after a stored watermark and persisting fixed-edge bin counts + per-window notionals in DynamoDB.

| Function | Role |
|----------|------|
| **Discovery** | Paginates Gamma `events?series_id=…`, upserts rows (`slug` PK). **Preserves** existing `monitor_state` so incremental tape state is not wiped on refresh. |
| **Monitor** | **Incremental** (default): `fetch_trades_event_since` + `monitor_state` JSON on each item. **Legacy**: full `analyze_match` replay when `MONITOR_MODE=legacy` or `LARGE_TRADE_CUT=exact`. Optional SNS when `n_flagged > 0`. |

## Layout

```
lambdas/
  template.yaml              # SAM — table + layer + schedules
  layer/
    requirements.txt         # certifi (HTTPS)
    python/mfd/
      sketch_quantile.py     # synced from backtest (quantile_from_histogram_counts, …)
      pipeline.py            # synced from backtest (fetch_trades_event_since, analyze_match, …)
      incremental_match.py   # synced from backtest/incremental_match.py
  discovery/handler.py
  monitor/handler.py
  sync_from_backtest.sh
```

After you change the backtest or `backtest/incremental_match.py`:

```bash
bash sync_from_backtest.sh
```

## Deploy (SAM CLI)

From this directory:

```bash
sam build
sam deploy --guided
```

## Configure

| Env (Discovery) | Meaning |
|-------------------|---------|
| `MATCHES_TABLE` | Set by SAM. |
| `LOOKAHEAD_DAYS` | Default `14`. |
| `SERIES_IDS` | Default `10971,10439,10443` (ROU1, CSL, J2). |

| Env (Monitor) | Meaning |
|---------------|---------|
| `MONITOR_MODE` | `incremental` (default) or `legacy`. Incremental applies only when `LARGE_TRADE_CUT=histogram`. |
| `LARGE_TRADE_CUT` | `histogram` (default) or `exact` (forces full-replay `analyze_match`). |
| `HISTOGRAM_BINS` | Default `64` (fixed log-spaced edges). |
| `MAX_SLUGS_PER_INVOCATION` | Default `8`. |
| `ALERT_SNS_TOPIC_ARN` | Optional SNS; add `sns:Publish` on the topic. |

**DynamoDB:** Monitor writes JSON to attribute `monitor_state` (prefix histograms, per-window aggregates, watermark). Item size must stay under 400 KB.

## Incremental behavior (summary)

- **Trades:** `fetch_trades_event_since` pages the Data API newest-first and keeps rows with `timestamp > watermark` (and in `[kick, min(end, now)]`).
- **Large-trade cut:** `quantile_from_histogram_counts` on YES/NO bin vectors; prefix snapshots at each window end match full replay.
- **Directional notionals:** updated per trade into per-window buckets (same semantics as `_build_windows`).
- **Prices:** still one CLOB `prices-history` fetch per invocation over the full analysis window (not incremental yet).

## Test invokes

**Discovery** — no payload required.

**Monitor** — optional explicit slugs (skips Dynamo scan):

```bash
aws lambda invoke --function-name YOUR_STACK-monitor \
  --payload '{"slugs":["rou1-aus-aog-2026-03-23"]}' /tmp/out.json && cat /tmp/out.json
```

## Production notes

- **DynamoDB:** Monitor scan + kickoff filter is fine at small scale; add a **GSI** for kickoff/status at volume.
- **SNS / VPC:** Same as before (topic policy, no VPC for outbound HTTPS).

#!/usr/bin/env python3
"""
Build a combined markdown report from ROU1 + CSL + optional Japan J2 batch CSV outputs
(same columns as ``polymarket_smart_money_backtest.py`` batch export).
"""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import sys
from collections import Counter
from pathlib import Path

import os

_REPO = os.path.dirname(os.path.abspath(__file__))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from polymarket_smart_money_backtest import _binom_two_sided_vs_half, _wilson_ci_95

DETAIL_RE = re.compile(
    r"(?P<kind>buy_yes|sell_yes):(?P<legs>[^@]+)@(?P<t0>[^|]+)\|p_end=(?P<p>[\d.]+)"
)

# Embedded in every generated report (keep in sync with COMBINED_ROU1_CSL_REPORT.md intent).
METRICS_DEFINITIONS = r"""
## What each metric means

This report is built from the batch CSV columns produced by `polymarket_smart_money_backtest.py` (one row per Polymarket **event** / match).

### Scope and `rou1_window`

- **`rou1_window` (CSV column name):** batch tag for that row. **`csl`** = Chinese Super League; **`j2`** = Japan J2 League (Feb 28–Apr 6 batch); **`week1`** / **`week3`** = Romanian SuperLiga week bands. The column name is historical (“rou1\_window”); it tags **any** batch slice, not only Romania.

### **Matches in CSV**

- Count of rows in that slice of the export (one row per match the script attempted). Includes failures (`no_tape`, `api_error`, etc.), not only “good” backtests.

### **By `status`**

Pipeline outcome for that match (from the backtest script):

| `status` | Meaning |
|----------|--------|
| **`ok`** | Enough trade + price windows to score; alignment fields are defined when the market is resolved. |
| **`insufficient_windows`** | Not enough 5‑minute windows with usable price history to run the full comparison (threshold in code). |
| **`no_tape`** | No trades on YES/NO inside the modeled match window. |
| **`api_error` / `data_error`** | Bad HTTP/payload or inconsistent data for that match. |

Counts tell you how much of the export is usable for **flag-based** metrics (mostly `ok` with flags).

### **Window-level (resolved flags only)**

- **Unit:** one **flagged time window** (5‑minute bar) where the model produced a `buy_yes` or `sell_yes` signal.
- **Aligned:** flag direction matches the **final** Polymarket resolution for that contract’s **YES** outcome: **`buy_yes`** aligns if YES won; **`sell_yes`** aligns if NO won (YES lost).
- **Not aligned:** flagged but wrong vs that resolution.
- **Hit rate:** `aligned / (aligned + not aligned)`, only for windows where the market is **resolved** and the CSV has non‑empty `n_flagged_correct` / `n_flagged_wrong` (so the script could score alignment).
- **Caveat:** multiple windows in the **same match** are **not independent** (same score, correlated flow).

### **Mean `p_yes_end`**

- For each flagged window the backtest stores **YES midpoint price at the end of that window** (`p_yes_end`), treated as implied **P(YES)** for the **question** on that row.
- **Aligned / not aligned** here splits those windows **after** the fact by whether the flag was correct.
- **Avg for misaligned** uses the same YES price scale; for **`sell_yes`** flags, a low `p_yes_end` still means “market thought YES was unlikely,” not “probability of the side you bet.”

### **Per-match majority (resolved, ≥1 flag, c≠w)**

For each match with `status=ok`, at least one flag, and resolved alignment:

- Count how many flagged windows were **aligned** vs **not aligned** (`n_flagged_correct` vs `n_flagged_wrong`).
- **Majority aligned:** more aligned than not.
- **Majority not aligned:** more not aligned than aligned.
- **Ties:** equal aligned and not (e.g. 2 vs 2). Reported separately; **not** counted as a “win” or “loss” for majority.

### **Decisive matches `n`**

- **`n` = majority aligned + majority not aligned** (ties **excluded**).
- **Percentage** = `majority aligned / n`: “In what share of matches did **most** flagged windows point the right way vs the final result?”
- This is the usual **one row per match** summary for dependence (within-match correlation of windows is collapsed to one binary-ish outcome per game, except ties).

### **Wilson 95% CI (match-level majority)**

- A **confidence interval** for the **true** long‑run fraction of **decisive** matches where the **majority** of flags aligns, if matches were i.i.d. from the same process (informative but approximate; leagues and seasons differ).
- **Wilson** is preferred to a naïve Normal approximation for small **`n`**.

### **Binomial vs 0.5 (decisive matches)**

- Treat each **decisive** match as one Bernoulli trial: “majority aligned” = success.
- **Null hypothesis:** success probability = **0.5** (as if each match were a coin flip between “majority right” and “majority wrong”).
- **One-sided `P(X ≥ k)`:** probability under that null of seeing **at least** as many majority‑aligned matches as observed (**k** = reported majority‑aligned count; **n** = decisive matches).
- **Two-sided p-value:** probability under the same null of an outcome **as extreme or more** in **either** tail (symmetric rule for `p = 0.5`).
- This is a **stylized** benchmark, not proof of tradable edge (no fees, selection, multiple testing across leagues, etc.).

---

## Quick glossary (bullets below)

Each league block repeats the same bullet list: **matches**, **`status` mix**, **window-level** counts, **mean `p_yes_end`**, **majority / ties**, **decisive `n`**, **Wilson CI**, **binomial p** — all defined above. **Pooled** aggregates every row from the input files you pass.

---
"""


def league_from_row(row: dict[str, str]) -> str:
    w = (row.get("rou1_window") or "").strip().lower()
    if w == "csl":
        return "CSL"
    if w == "j2":
        return "J2"
    return "ROU1"


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def window_alignment_totals(rows: list[dict[str, str]]) -> tuple[int, int]:
    c = w = 0
    for row in rows:
        cc, ww = row.get("n_flagged_correct", "").strip(), row.get("n_flagged_wrong", "").strip()
        if not cc or not ww:
            continue
        c += int(cc)
        w += int(ww)
    return c, w


def flag_level_p_stats(rows: list[dict[str, str]]) -> tuple[list[float], list[float]]:
    """Aligned vs misaligned flag ``p_end`` values."""
    ok_p: list[float] = []
    bad_p: list[float] = []
    for row in rows:
        detail = (row.get("flagged_windows_detail") or "").strip()
        if not detail:
            continue
        ycw = (row.get("yes_contract_won") or "").strip().lower()
        yes_won = True if ycw == "true" else (False if ycw == "false" else None)
        if yes_won is None:
            continue
        for part in [x.strip() for x in detail.split(";") if x.strip()]:
            m = DETAIL_RE.match(part)
            if not m:
                continue
            kind = m.group("kind")
            p = float(m.group("p"))
            aligned = (kind == "buy_yes" and yes_won) or (kind == "sell_yes" and not yes_won)
            (ok_p if aligned else bad_p).append(p)
    return ok_p, bad_p


def majority_per_match(rows: list[dict[str, str]]) -> tuple[int, int, int]:
    """Returns (majority_ok, majority_bad, ties)."""
    maj_ok = maj_bad = ties = 0
    for row in rows:
        if row.get("status") != "ok":
            continue
        try:
            nf = int(row.get("n_flagged") or 0)
        except ValueError:
            continue
        if nf <= 0:
            continue
        cc, ww = row.get("n_flagged_correct", "").strip(), row.get("n_flagged_wrong", "").strip()
        if not cc or not ww:
            continue
        ci, wi = int(cc), int(ww)
        if ci > wi:
            maj_ok += 1
        elif wi > ci:
            maj_bad += 1
        else:
            ties += 1
    return maj_ok, maj_bad, ties


def subset(rows: list[dict[str, str]], league: str | None) -> list[dict[str, str]]:
    if league is None:
        return rows
    return [r for r in rows if league_from_row(r) == league]


def section_metrics(label: str, rows: list[dict[str, str]], lines: list[str]) -> None:
    n_matches = len(rows)
    st = Counter((r.get("status") or "").strip() or "unknown" for r in rows)
    wc, ww = window_alignment_totals(rows)
    wtot = wc + ww
    hit = (100.0 * wc / wtot) if wtot else float("nan")

    ok_p, bad_p = flag_level_p_stats(rows)
    mp_ok, mp_bad, ties = majority_per_match(rows)
    n_dec = mp_ok + mp_bad
    maj_rate = (100.0 * mp_ok / n_dec) if n_dec else float("nan")
    lo, hi = _wilson_ci_95(mp_ok, n_dec) if n_dec else (float("nan"), float("nan"))
    one_s, two_s = _binom_two_sided_vs_half(n_dec, mp_ok) if n_dec else (float("nan"), float("nan"))

    lines.append(f"### {label}\n")
    lines.append(f"- **Matches in CSV:** {n_matches}")
    lines.append(
        f"- **By `status`:** "
        + ", ".join(f"{k}={v}" for k, v in sorted(st.items(), key=lambda x: -x[1]))
    )
    lines.append(
        f"- **Window-level (resolved flags only):** aligned **{wc}** / not aligned **{ww}** "
        f"→ **{hit:.1f}%** hit rate (counting each flagged window)."
    )
    if ok_p or bad_p:
        om = statistics.mean(ok_p) if ok_p else float("nan")
        bm = statistics.mean(bad_p) if bad_p else float("nan")
        lines.append(
            f"- **Mean `p_yes_end`:** aligned flags n={len(ok_p)} → **{100.0 * om:.1f}%**; "
            f"not aligned n={len(bad_p)} → **{100.0 * bm:.1f}%**."
        )
    lines.append(
        f"- **Per-match majority (resolved, ≥1 flag, c≠w):** "
        f"majority aligned **{mp_ok}**, majority not **{mp_bad}**, **ties {ties}** "
        f"(ties excluded from binomial `n`)."
    )
    if n_dec:
        lines.append(
            f"- **Decisive matches `n`:** {n_dec} → **{maj_rate:.1f}%** majority correct "
            f"({mp_ok}/{n_dec})."
        )
        lines.append(
            f"- **Wilson 95% CI** (match-level majority): "
            f"**[{100.0 * lo:.1f}%, {100.0 * hi:.1f}%]**."
        )
        lines.append(
            f"- **Binomial vs 0.5** (decisive matches): "
            f"one-sided `P(X≥{mp_ok})` = **{one_s:.4g}**, two-sided **{two_s:.4g}**."
        )
    lines.append("")


def build_report(
    rou1_path: Path,
    csl_path: Path,
    j2_path: Path | None,
    *,
    heading: str | None = None,
    preamble: str | None = None,
) -> str:
    rou1 = load_rows(rou1_path)
    csl = load_rows(csl_path)
    j2 = load_rows(j2_path) if j2_path is not None else []
    all_rows = rou1 + csl + j2

    lines: list[str] = []
    if heading and heading.strip():
        lines.append(heading.strip() + "\n")
    elif j2_path is not None:
        lines.append(
            "# Combined backtest report: Romanian SuperLiga + Chinese Super League + Japan J2\n"
        )
    else:
        lines.append(
            "# Combined backtest report: Romanian SuperLiga + Chinese Super League\n"
        )
    if preamble and preamble.strip():
        lines.append(preamble.strip() + "\n")
    lines.append("## Inputs\n")
    inputs_md = (
        f"| Source | File | Rows |\n|---|---:|---:|\n"
        f"| Romanian SuperLiga (Week 1 + Week 3) | `{rou1_path.name}` | {len(rou1)} |\n"
        f"| Chinese Super League (Mar 6 – Apr 6) | `{csl_path.name}` | {len(csl)} |\n"
    )
    if j2_path is not None:
        inputs_md += (
            f"| Japan J2 League (Feb 28 – Apr 6) | `{j2_path.name}` | {len(j2)} |\n"
        )
    inputs_md += f"| **Pooled** | | **{len(all_rows)}** |\n"
    lines.append(inputs_md)
    lines.append(METRICS_DEFINITIONS.strip() + "\n")
    section_metrics("Romanian SuperLiga (`ROU1`)", subset(all_rows, "ROU1"), lines)
    section_metrics("Chinese Super League (`CSL`)", subset(all_rows, "CSL"), lines)
    if j2_path is not None:
        section_metrics("Japan J2 League (`J2`)", subset(all_rows, "J2"), lines)
    pool_label = "Pooled (all leagues in table)" if j2_path is not None else "Pooled (ROU1 + CSL)"
    section_metrics(pool_label, subset(all_rows, None), lines)

    lines.append("## Interpretation notes\n")
    lines.append(
        "- Pooled **window-level** counts treat every flag as a separate observation; "
        "flags in the **same match** are correlated. **Per-match majority** is usually "
        "the cleaner replication unit.\n"
        "- **Different leagues / calendars** pool heterogeneous settings; use per-league "
        "sections for causal claims.\n"
        "- Binomial **p** vs 0.5 is a stylized null (“coin flip which match has majority-aligned flags”); "
        "it does not include **multiple-testing correction** across leagues.\n"
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Combine ROU1 + CSL (+ optional J2) batch CSVs into one markdown report."
    )
    ap.add_argument(
        "--rou1",
        type=Path,
        default=Path("out.csv"),
        help="ROU1 batch CSV (default: out.csv).",
    )
    ap.add_argument(
        "--csl",
        type=Path,
        default=Path("csl_backtest_mar_apr_2026.csv"),
        help="CSL batch CSV (default: csl_backtest_mar_apr_2026.csv).",
    )
    ap.add_argument(
        "--j2",
        type=Path,
        default=Path("j2_backtest_feb28_2026.csv"),
        help="Japan J2 batch CSV (default: j2_backtest_feb28_2026.csv).",
    )
    ap.add_argument(
        "--omit-j2",
        action="store_true",
        help="Build report from ROU1 + CSL only (ignore --j2).",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("COMBINED_ROU1_CSL_REPORT.md"),
        help="Output markdown path.",
    )
    ap.add_argument(
        "--heading",
        default="",
        help="Override the main # title (default: standard combined title).",
    )
    ap.add_argument(
        "--preamble",
        default="",
        help="Optional markdown block inserted after the title (e.g. method note).",
    )
    args = ap.parse_args()
    if not args.rou1.is_file():
        print(f"Missing ROU1 CSV: {args.rou1}", file=sys.stderr)
        raise SystemExit(1)
    if not args.csl.is_file():
        print(f"Missing CSL CSV: {args.csl}", file=sys.stderr)
        raise SystemExit(1)
    j2_path: Path | None = None if args.omit_j2 else args.j2
    if j2_path is not None and not j2_path.is_file():
        print(f"Missing J2 CSV: {j2_path}", file=sys.stderr)
        raise SystemExit(1)

    text = build_report(
        args.rou1,
        args.csl,
        j2_path,
        heading=(args.heading or None),
        preamble=(args.preamble or None),
    )
    args.output.write_text(text, encoding="utf-8")
    print(f"Wrote {args.output}", flush=True)


if __name__ == "__main__":
    main()

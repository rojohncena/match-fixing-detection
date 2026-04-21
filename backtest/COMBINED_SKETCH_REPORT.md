# Combined backtest report (histogram large-trade cutoff)

> **Variant:** `--large-trade-cut histogram` — prefix **90th-percentile** trade size uses `sketch_quantile.histogram_large_cut_fixed` with **fixed** log-spaced edges (`build_log_spaced_edges`, default 64 bins over production min/max). Flow medians, `MIN_DELTA_P`, and other logic match the **exact** `--large-trade-cut exact` backtest.

## Inputs

| Source | File | Rows |
|---|---:|---:|
| Romanian SuperLiga (Week 1 + Week 3) | `out_sketch_histogram.csv` | 62 |
| Chinese Super League (Mar 6 – Apr 6) | `csl_backtest_sketch_histogram.csv` | 32 |
| Japan J2 League (Feb 28 – Apr 6) | `j2_backtest_sketch_histogram.csv` | 109 |
| **Pooled** | | **203** |

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

### Romanian SuperLiga (`ROU1`)

- **Matches in CSV:** 62
- **By `status`:** ok=50, insufficient_windows=8, no_tape=4
- **Window-level (resolved flags only):** aligned **41** / not aligned **19** → **68.3%** hit rate (counting each flagged window).
- **Mean `p_yes_end`:** aligned flags n=41 → **48.8%**; not aligned n=19 → **50.3%**.
- **Per-match majority (resolved, ≥1 flag, c≠w):** majority aligned **16**, majority not **6**, **ties 4** (ties excluded from binomial `n`).
- **Decisive matches `n`:** 22 → **72.7%** majority correct (16/22).
- **Wilson 95% CI** (match-level majority): **[51.8%, 86.8%]**.
- **Binomial vs 0.5** (decisive matches): one-sided `P(X≥16)` = **0.02624**, two-sided **0.05248**.

### Chinese Super League (`CSL`)

- **Matches in CSV:** 32
- **By `status`:** ok=32
- **Window-level (resolved flags only):** aligned **82** / not aligned **28** → **74.5%** hit rate (counting each flagged window).
- **Mean `p_yes_end`:** aligned flags n=82 → **44.1%**; not aligned n=28 → **44.4%**.
- **Per-match majority (resolved, ≥1 flag, c≠w):** majority aligned **23**, majority not **2**, **ties 5** (ties excluded from binomial `n`).
- **Decisive matches `n`:** 25 → **92.0%** majority correct (23/25).
- **Wilson 95% CI** (match-level majority): **[75.0%, 97.8%]**.
- **Binomial vs 0.5** (decisive matches): one-sided `P(X≥23)` = **9.716e-06**, two-sided **1.943e-05**.

### Japan J2 League (`J2`)

- **Matches in CSV:** 109
- **By `status`:** ok=52, insufficient_windows=31, no_tape=26
- **Window-level (resolved flags only):** aligned **19** / not aligned **10** → **65.5%** hit rate (counting each flagged window).
- **Mean `p_yes_end`:** aligned flags n=19 → **41.7%**; not aligned n=10 → **30.3%**.
- **Per-match majority (resolved, ≥1 flag, c≠w):** majority aligned **13**, majority not **4**, **ties 2** (ties excluded from binomial `n`).
- **Decisive matches `n`:** 17 → **76.5%** majority correct (13/17).
- **Wilson 95% CI** (match-level majority): **[52.7%, 90.4%]**.
- **Binomial vs 0.5** (decisive matches): one-sided `P(X≥13)` = **0.02452**, two-sided **0.04904**.

### Pooled (all leagues in table)

- **Matches in CSV:** 203
- **By `status`:** ok=134, insufficient_windows=39, no_tape=30
- **Window-level (resolved flags only):** aligned **142** / not aligned **57** → **71.4%** hit rate (counting each flagged window).
- **Mean `p_yes_end`:** aligned flags n=142 → **45.1%**; not aligned n=57 → **43.9%**.
- **Per-match majority (resolved, ≥1 flag, c≠w):** majority aligned **52**, majority not **12**, **ties 11** (ties excluded from binomial `n`).
- **Decisive matches `n`:** 64 → **81.2%** majority correct (52/64).
- **Wilson 95% CI** (match-level majority): **[70.0%, 88.9%]**.
- **Binomial vs 0.5** (decisive matches): one-sided `P(X≥52)` = **2.283e-07**, two-sided **4.567e-07**.

## Interpretation notes

- Pooled **window-level** counts treat every flag as a separate observation; flags in the **same match** are correlated. **Per-match majority** is usually the cleaner replication unit.
- **Different leagues / calendars** pool heterogeneous settings; use per-league sections for causal claims.
- Binomial **p** vs 0.5 is a stylized null (“coin flip which match has majority-aligned flags”); it does not include **multiple-testing correction** across leagues.

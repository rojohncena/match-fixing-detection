"""
Approximate prefix quantiles for large-trade detection without storing every raw size
in live systems. Used by ``--large-trade-cut histogram`` backtest mode.

**Production-style** counting uses **fixed bin edges** (shared across all prefixes and
matches), not per-prefix min–max bins. ``histogram_large_cut_fixed`` maps each fill to a
bin, accumulates counts, then applies the same index-based ``q`` as ``_quantile`` in the
main backtest on the multiset of **bin centers** with multiplicities = bin counts.

The legacy ``histogram_large_cut_adaptive`` helper linearly bins between each prefix’s
min..max (good for A/B vs exact, not deployment-faithful).
"""

from __future__ import annotations

import bisect
import math
from typing import Sequence

# Match Lambda / fixed-histogram deployment: log-spaced edges over this notional range (shares).
PRODUCTION_SIZE_MIN = 0.01
PRODUCTION_SIZE_MAX = 500_000.0


def build_log_spaced_edges(n_bins: int, size_min: float = PRODUCTION_SIZE_MIN, size_max: float = PRODUCTION_SIZE_MAX) -> list[float]:
    """Return ``edges`` of length ``n_bins + 1`` with log-spaced boundaries in ``[size_min, size_max]``."""
    if n_bins < 1:
        raise ValueError("n_bins must be >= 1")
    lo, hi = float(size_min), float(size_max)
    if lo <= 0 or hi <= lo:
        raise ValueError("need 0 < size_min < size_max")
    log_lo, log_hi = math.log10(lo), math.log10(hi)
    return [10 ** (log_lo + (log_hi - log_lo) * i / n_bins) for i in range(n_bins + 1)]


def _index_quantile_sorted(sorted_vals: list[float], q: float) -> float:
    """Match ``polymarket_smart_money_backtest._quantile`` on a sorted multiset."""
    if not sorted_vals:
        return math.nan
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def bin_index_for_edges(v: float, edges: Sequence[float]) -> int:
    """Public helper: map a trade size to a fixed-edge bin index (same as the sketch uses)."""
    return _bin_index_fixed(v, edges)


def _bin_index_fixed(v: float, edges: Sequence[float]) -> int:
    n = len(edges) - 1
    if n < 1:
        return 0
    if not math.isfinite(v) or v <= 0:
        return 0
    if v < edges[0]:
        return 0
    if v >= edges[-1]:
        return n - 1
    i = bisect.bisect_right(edges, v) - 1
    return max(0, min(n - 1, i))


def histogram_large_cut_fixed(vals: Sequence[float], q: float, edges: Sequence[float]) -> float:
    """
    Approximate ``q``-quantile from trade sizes using **fixed** bins ``[edges[i], edges[i+1])``,
    surrogate multiset = bin centers with bin counts as multiplicity, then same index-quantile
    as exact path.
    """
    xs = [float(x) for x in vals if math.isfinite(float(x)) and float(x) > 0]
    if not xs:
        return math.nan
    if len(xs) == 1:
        return xs[0]
    n_bins = len(edges) - 1
    if n_bins < 1:
        return xs[0]

    counts = [0] * n_bins
    for v in xs:
        counts[_bin_index_fixed(v, edges)] += 1

    centers = [(edges[i] + edges[i + 1]) / 2.0 for i in range(n_bins)]
    surrogate: list[float] = []
    for c, center in zip(counts, centers):
        surrogate.extend([center] * c)
    surrogate.sort()
    return _index_quantile_sorted(surrogate, q)


def quantile_from_histogram_counts(counts: Sequence[int], edges: Sequence[float], q: float) -> float:
    """
    Same index-based ``q`` as ``histogram_large_cut_fixed`` / ``_quantile``, without expanding
    bin centers into a multiset (O(total count) memory). Use for live incremental monitors.
    """
    n_bins = len(edges) - 1
    if n_bins < 1:
        return math.nan
    total = sum(int(c) for c in counts)
    if total == 0:
        return math.nan
    if total == 1:
        for i, c in enumerate(counts):
            if c:
                return (edges[i] + edges[i + 1]) / 2.0
        return math.nan
    centers = [(edges[i] + edges[i + 1]) / 2.0 for i in range(n_bins)]
    idx = min(total - 1, max(0, int(round(q * (total - 1)))))
    cum = 0
    for i, c in enumerate(counts):
        c = int(c)
        if c <= 0:
            continue
        cum += c
        if cum > idx:
            return centers[i]
    return centers[-1]


def histogram_large_cut_adaptive(vals: Sequence[float], q: float, n_bins: int = 64) -> float:
    """
    Approximate ``q``-quantile by linear binning on ``[min,max]`` of **this** prefix only,
    then quantile on bin centers. Not fixed-edge; useful for comparing sketch error vs exact.
    """
    xs = [float(x) for x in vals]
    if not xs:
        return math.nan
    if len(xs) == 1:
        return xs[0]
    lo, hi = min(xs), max(xs)
    if lo >= hi:
        return lo

    counts = [0] * n_bins
    span = hi - lo
    for v in xs:
        if v <= lo:
            j = 0
        elif v >= hi:
            j = n_bins - 1
        else:
            j = int((v - lo) / span * n_bins)
            if j >= n_bins:
                j = n_bins - 1
        counts[j] += 1

    width = span / n_bins
    centers = [lo + (i + 0.5) * width for i in range(n_bins)]
    surrogate: list[float] = []
    for c, center in zip(counts, centers):
        surrogate.extend([center] * c)
    surrogate.sort()
    return _index_quantile_sorted(surrogate, q)


# Backward-compatible name: older code expected adaptive min–max bins.
histogram_large_cut = histogram_large_cut_adaptive

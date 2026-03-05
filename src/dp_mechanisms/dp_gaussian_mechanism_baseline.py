"""
dp_gaussian_mechanism_baseline.py

Baseline Gaussian DP mechanism — naive (per-query, per-epsilon) approach.

For every query and every epsilon value, the noise scale (sigma) is computed
independently inside the column loop.  This is the simplest correct
implementation and serves as the reference for comparison with the advance
version.

Key difference from advance version:
  BASELINE  → sigma re-computed for every (query, column, epsilon) triple
  ADVANCE   → sigma pre-computed once per unique (sensitivity, epsilon) pair
              and reused across all queries that share those parameters

Pipeline:
1) Load each baseline query CSV from data/mini/ (or data/full/).
2) For each epsilon (seeded rng = RANDOM_SEED + eps_idx):
   a) For each numeric column, compute sigma fresh.
   b) Add N(0, sigma²) noise independently to each numeric column.
   c) Post-process (clamp negatives, re-normalise percentage columns).
   d) Compute the evaluation metric (true vs noisy).
   e) Save noisy CSV to data/dp_gaussian_mini/baseline/eps_<epsilon>/
   f) Append row to running summary DataFrame.
3) Save summary to data/dp_gaussian_mini/baseline/gaussian_metric_summary.csv

Usage:
    cd scripts/dp_mechanisms
    python dp_gaussian_mechanism_baseline.py --database mini
    python dp_gaussian_mechanism_baseline.py --database full
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, os.path.dirname(__file__))
from dp_config import (
    QUERY_META,
    EPSILON_VALUES,
    DEFAULT_DELTA,
    RANDOM_SEED,
    N_DIST_COLS_Q8,
    BASELINE_MINI,
    BASELINE_FULL,
    DP_GAUSSIAN_MINI,
    DP_GAUSSIAN_FULL,
    get_sensitivity,
    gaussian_sigma,
)


# =============================================================================
#  METRIC HELPERS
#  Aligned with the other group's evaluation framework:
#    Agg+Join  → RE        (relative error,          pass if RE ≤ 0.25)
#    Geo/Demo  → RE        (relative error,          pass if RE ≤ 0.25)
#    Top-k     → SPEARMAN  (Spearman ρ,              pass if ρ ≥ 0.5)
#    Histogram → TVD       (total variation distance, pass if TVD ≤ 0.15)
#    Pivot     → TVD       (total variation distance, pass if TVD ≤ 0.15)
# =============================================================================

def _ensure_series(series: pd.Series) -> pd.Series:
    """Drop NaN and reset index — defensive helper used across metrics."""
    return series.dropna().reset_index(drop=True)


def compute_metric_RE(true_df: pd.DataFrame,
                      noisy_df: pd.DataFrame,
                      re_col: str) -> dict:
    """
    Relative Error — Agg+Join and Geo/Demo queries.
    RE(r, s) = |r - s| / |r|  (median across rows).
    Pass threshold: RE ≤ 0.25.
    """
    true_vals  = _ensure_series(true_df[re_col].astype(float))
    noisy_vals = _ensure_series(noisy_df[re_col].astype(float))
    n = min(len(true_vals), len(noisy_vals))
    true_v  = true_vals.values[:n]
    noisy_v = noisy_vals.values[:n]
    nonzero = true_v != 0
    if nonzero.sum() == 0:
        median_re = float("nan")
    else:
        median_re = float(np.median(
            np.abs(true_v[nonzero] - noisy_v[nonzero]) / np.abs(true_v[nonzero])
        ))
    return {
        "metric_type": "RE",
        "median_re"  : round(median_re, 4),
        "pass"       : int(median_re <= 0.25) if not np.isnan(median_re) else 0,
    }


def compute_metric_TVD(true_df: pd.DataFrame,
                       noisy_df: pd.DataFrame,
                       pct_col: str) -> dict:
    """
    Total Variation Distance — Histogram queries.
    TVD = 0.5 * Σ|p_true - p_dp|.  Pass threshold: TVD ≤ 0.15.
    """
    p_true  = _ensure_series(true_df[pct_col].astype(float))
    p_noisy = _ensure_series(noisy_df[pct_col].astype(float))
    p_true  = p_true  / p_true.sum()  if p_true.sum()  > 0 else p_true
    p_noisy = p_noisy / p_noisy.sum() if p_noisy.sum() > 0 else p_noisy
    n   = min(len(p_true), len(p_noisy))
    tvd = float(0.5 * np.sum(np.abs(p_true.values[:n] - p_noisy.values[:n])))
    return {
        "metric_type": "TVD",
        "tvd"        : round(tvd, 4),
        "pass"       : int(tvd <= 0.15),
    }


def compute_metric_TVD_pivot(true_df: pd.DataFrame,
                              noisy_df: pd.DataFrame,
                              dist_cols: list) -> dict:
    """
    TVD for Pivot queries (Q7, Q8) — mean TVD across all distribution columns.
    Pass threshold: mean TVD ≤ 0.15.
    """
    tvd_values = []
    for col in dist_cols:
        if col not in true_df.columns or col not in noisy_df.columns:
            continue
        tvd_values.append(compute_metric_TVD(true_df, noisy_df, col)["tvd"])
    mean_tvd = float(np.mean(tvd_values)) if tvd_values else float("nan")
    max_tvd  = float(np.max(tvd_values))  if tvd_values else float("nan")
    return {
        "metric_type": "TVD",
        "mean_tvd"   : round(mean_tvd, 4),
        "max_tvd"    : round(max_tvd,  4),
        "pass"       : int(mean_tvd <= 0.15) if not np.isnan(mean_tvd) else 0,
    }


def compute_metric_SPEARMAN(true_df: pd.DataFrame,
                             noisy_df: pd.DataFrame,
                             rank_col: str) -> dict:
    """
    Spearman ρ — Top-k queries (Q3, Q6, Q12).
    Pass threshold: ρ ≥ 0.5.
    """
    if not pd.api.types.is_numeric_dtype(true_df[rank_col]):
        true_order    = list(true_df[rank_col].reset_index(drop=True))
        noisy_order   = list(noisy_df[rank_col].reset_index(drop=True))
        category_rank = {v: i for i, v in enumerate(true_order)}
        true_ranks    = np.arange(len(true_order), dtype=float)
        noisy_ranks   = np.array([category_rank.get(v, len(true_order))
                                   for v in noisy_order], dtype=float)
    else:
        true_vals  = _ensure_series(true_df[rank_col].astype(float))
        noisy_vals = _ensure_series(noisy_df[rank_col].astype(float))
        n = min(len(true_vals), len(noisy_vals))
        true_ranks  = pd.Series(true_vals.values[:n]).rank(
            method="average", ascending=False).values
        noisy_ranks = pd.Series(noisy_vals.values[:n]).rank(
            method="average", ascending=False).values

    n = min(len(true_ranks), len(noisy_ranks))
    if n < 2:
        rho, pval = float("nan"), float("nan")
    else:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rho, pval = spearmanr(true_ranks[:n], noisy_ranks[:n])
        rho, pval = float(rho), float(pval)
        if np.isnan(rho):
            rho, pval = 0.0, 1.0

    return {
        "metric_type" : "SPEARMAN",
        "spearman_rho": round(rho,  4) if not np.isnan(rho)  else float("nan"),
        "p_value"     : round(pval, 4) if not np.isnan(pval) else float("nan"),
        "pass"        : int(rho >= 0.5) if not np.isnan(rho) else 0,
    }


# =============================================================================
#  POST-PROCESSING
# =============================================================================

def post_process(noisy_df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    1. Clamp all numeric columns to >= 0.
    2. Re-normalise single percentage column to sum to ~100.
    3. Re-normalise per-row distribution columns to sum to ~100 (Q7, Q8).
    """
    df = noisy_df.copy()

    for col in meta["numeric_cols"]:
        if col in df.columns:
            df[col] = df[col].clip(lower=0.0)

    pct_col = meta.get("pct_col")
    if pct_col and pct_col in df.columns:
        total = df[pct_col].sum()
        if total > 0:
            df[pct_col] = df[pct_col] / total * 100.0

    dist_cols = meta.get("dist_cols", [])
    if dist_cols:
        for i in df.index:
            row_sum = df.loc[i, dist_cols].sum()
            if row_sum > 0:
                df.loc[i, dist_cols] = df.loc[i, dist_cols] / row_sum * 100.0

    return df


# =============================================================================
#  METRIC DISPATCHER
# =============================================================================

def compute_metric(true_df: pd.DataFrame,
                   noisy_df: pd.DataFrame,
                   meta: dict) -> dict:
    """
    Route to the correct metric function based on meta['metric_type'].
        RE       → Agg+Join, Geo/Demo
        TVD      → Histogram, Pivot
        SPEARMAN → Top-k
    """
    mtype = meta["metric_type"]

    if mtype == "RE":
        return compute_metric_RE(true_df, noisy_df, meta["re_col"])

    elif mtype == "TVD":
        dist_cols = meta.get("dist_cols")
        if dist_cols:
            return compute_metric_TVD_pivot(true_df, noisy_df, dist_cols)
        else:
            return compute_metric_TVD(true_df, noisy_df, meta["pct_col"])

    elif mtype == "SPEARMAN":
        return compute_metric_SPEARMAN(true_df, noisy_df, meta["rank_col"])

    else:
        return {"metric_type": mtype, "error": "unknown metric type"}


# =============================================================================
#  CORE: apply Gaussian DP to one query CSV  (BASELINE — naive per-column loop)
# =============================================================================

def apply_gaussian_dp(
    true_df   : pd.DataFrame,
    meta      : dict,
    epsilon   : float,
    delta     : float,
    rng       : np.random.Generator,
    query_num : int,
) -> pd.DataFrame:
    """
    BASELINE version — sigma is computed fresh for every column on every call.

    For eps = inf  →  return an unmodified copy (no noise).
    For eps < inf  →  add N(0, sigma²) noise to each numeric column,
                      then post-process.
    """
    noisy_df     = true_df.copy()
    numeric_cols = meta["numeric_cols"]
    metric_type  = meta["metric_type"]

    # ------------------------------------------------------------------
    # Q6 — Report Noisy Max
    # metric_type == "SPEARMAN" and no numeric_cols → winner-column query.
    # Add Lap(0, 1/ε) to the winner's synthetic count and take argmax.
    # Satisfies ε-DP (sensitivity = 1 for a single count).
    # ------------------------------------------------------------------
    if query_num == 6 and epsilon != float("inf"):
        winner_col = meta.get("winner_col")
        if winner_col and winner_col in noisy_df.columns:
            noisy_count = 1.0 + rng.laplace(
                loc=0.0, scale=1.0 / epsilon, size=len(noisy_df)
            )
            flip_mask = noisy_count < 0.5
            if flip_mask.any():
                noisy_df.loc[flip_mask, winner_col] = "unknown"
                print(f"    [Gaussian Q6] Report Noisy Max flipped {flip_mask.sum()} rows")
            print(f"    [Gaussian Q6] Report Noisy Max  eps={epsilon}  scale={1/epsilon:.4f}")
        return noisy_df

    if not numeric_cols or epsilon == float("inf"):
        return noisy_df

    pct_col   = meta.get("pct_col")
    dist_cols = meta.get("dist_cols", [])

    # Q4 / Q10: normalise true-distribution pct_col to [0,1] before noise
    # so the effective sensitivity matches the [0,1] scale (not [0,100]).
    # Q11 excluded: avg_percentage_used is a per-bin mean, not a distribution.
    is_true_dist = (
        metric_type == "TVD"
        and pct_col is not None
        and query_num in (4, 10)
    )
    if is_true_dist and pct_col in noisy_df.columns:
        pct_sum = noisy_df[pct_col].sum()
        if pct_sum > 0:
            noisy_df[pct_col] = noisy_df[pct_col] / pct_sum  # sums to 1

    # ------------------------------------------------------------------
    # BASELINE: sigma computed fresh for every column (naive loop).
    # ------------------------------------------------------------------
    for col in numeric_cols:
        if col not in QUERY_META[query_num]["sensitivity"]:
            continue

        col_sens = get_sensitivity(query_num, col)
        if col_sens == 0.0:
            continue

        # Q8: split epsilon budget equally across all 28 distribution columns
        col_epsilon = epsilon
        if metric_type == "TVD" and col in dist_cols:
            col_epsilon = epsilon / N_DIST_COLS_Q8

        # Scale sensitivity to [0,1] for true-distribution pct_col
        if is_true_dist and col == pct_col:
            col_sens = col_sens / 100.0

        # ← BASELINE: sigma re-computed every time (no caching)
        sigma = gaussian_sigma(col_sens, col_epsilon, delta)

        print(f"    [Gaussian:{col}] sens={col_sens:.4f}  "
              f"eps_col={col_epsilon:.4f}  sigma={sigma:.4f}")

        noise = rng.normal(loc=0.0, scale=sigma, size=len(noisy_df))
        noisy_df[col] = noisy_df[col].astype(float) + noise

    # Re-scale true-distribution pct_col back to [0,100]
    if is_true_dist and pct_col in noisy_df.columns:
        noisy_df[pct_col] = noisy_df[pct_col] * 100.0

    noisy_df = post_process(noisy_df, meta)

    # Q8: Dirichlet smoothing so near-zero categories don't blow up at low ε
    if metric_type == "TVD" and dist_cols:
        alpha = 0.01
        for i in noisy_df.index:
            row = noisy_df.loc[i, dist_cols].astype(float).values + alpha
            noisy_df.loc[i, dist_cols] = row / row.sum() * 100.0

    return noisy_df


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def run_gaussian_baseline(database: str = "mini") -> None:
    """
    Baseline pipeline — naive per-query, per-epsilon noise calculation.
    Outputs go to data/dp_gaussian_{mini|full}/baseline/
    """
    baseline_dir = BASELINE_MINI if database == "mini" else BASELINE_FULL
    base_out     = DP_GAUSSIAN_MINI if database == "mini" else DP_GAUSSIAN_FULL
    variant_dir  = os.path.join(base_out, "baseline")

    print("=" * 70)
    print("GAUSSIAN DP MECHANISM — BASELINE")
    print("=" * 70)
    print(f"Database      : {database}")
    print(f"Baseline dir  : {baseline_dir}")
    print(f"Output dir    : {variant_dir}")
    print(f"Epsilon values: {EPSILON_VALUES}")
    print(f"Delta         : {DEFAULT_DELTA}")
    print(f"Random seed   : {RANDOM_SEED}")
    print(f"Start time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    all_summary_rows = []

    for query_num, meta in QUERY_META.items():

        filename   = meta["filename"]
        input_path = os.path.join(baseline_dir, filename)

        print(f"\nQuery {query_num:02d} | {filename}")
        print(f"  Metric : {meta['metric_type']}")

        if not os.path.exists(input_path):
            print(f"  ⚠  File not found — skipping: {input_path}")
            continue

        true_df = pd.read_csv(input_path)
        print(f"  Rows: {len(true_df)}, Cols: {list(true_df.columns)}")

        if not meta["numeric_cols"]:
            print("  ℹ  No numeric columns to perturb (Q6 — Report Noisy Max only).")

        for eps_idx, epsilon in enumerate(EPSILON_VALUES):

            eps_str = "inf" if epsilon == float("inf") else str(epsilon)
            print(f"\n  ε = {eps_str}")

            # Seed varies per epsilon so noise draws differ across epsilon values
            seed = RANDOM_SEED + eps_idx
            rng  = np.random.default_rng(seed=seed)

            noisy_df = apply_gaussian_dp(
                true_df   = true_df,
                meta      = meta,
                epsilon   = epsilon,
                delta     = DEFAULT_DELTA,
                rng       = rng,
                query_num = query_num,
            )

            metric_results = compute_metric(true_df, noisy_df, meta)
            for k, v in metric_results.items():
                print(f"    {k}: {v}")

            # Save noisy CSV
            out_dir = os.path.join(variant_dir, f"eps_{eps_str}")
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            noisy_df.to_csv(os.path.join(out_dir, filename), index=False)
            print(f"    Saved → {os.path.join(out_dir, filename)}")

            row = {
                "query_num" : query_num,
                "query_file": filename,
                "mechanism" : "gaussian",
                "variant"   : "baseline",
                "database"  : database,
                "epsilon"   : eps_str,
                "delta"     : DEFAULT_DELTA,
                "seed"      : seed,
                "n_rows"    : len(true_df),
            }
            row.update(metric_results)
            all_summary_rows.append(row)

    summary_df = pd.DataFrame(all_summary_rows)
    Path(variant_dir).mkdir(parents=True, exist_ok=True)
    summary_path = os.path.join(variant_dir, "gaussian_metric_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    print("\n" + "=" * 70)
    print("GAUSSIAN BASELINE COMPLETE")
    print("=" * 70)
    print(f"Summary saved → {summary_path}")
    print(f"Rows in summary: {len(summary_df)}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Baseline Gaussian DP mechanism — naive per-query loop."
    )
    parser.add_argument(
        "--database", choices=["mini", "full"], default="mini",
        help="Which database baseline to use (default: mini).",
    )
    args = parser.parse_args()
    run_gaussian_baseline(database=args.database)
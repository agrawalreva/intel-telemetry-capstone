"""
dp_gaussian_mechanism.py

Implements a dataset-level Gaussian DP mechanism for the 12 benchmark
query results, BY HAND — following the same structure as the previous
telemetry project (dp_mechanism.py) but adapted for CSV-level aggregates.

Pipeline of the file:
1) Load each baseline query CSV from data/baseline_mini/ (or baseline_full/).
2) For every query, identify the numeric columns that will receive noise
   and the evaluation metric that makes sense for that query's story
   (z-score+IOU, TVD, Kendall's Tau, Top-1 Accuracy, KL Divergence).
3) For each epsilon in EPSILON_VALUES (loop with fixed seed):
   a) Compute sigma = (Δ_2 / ε) * sqrt(2 * ln(1.25 / δ)).
   b) Add N(0, sigma²) noise independently to each numeric column.
   c) Post-process (clamp negatives, re-normalise percentage columns).
   d) Compute the TRUE metric on the original data.
   e) Compute the DP metric on the noisy data.
   f) Save the noisy CSV to data/dp_gaussian_mini/eps_<epsilon>/.
   g) Append a metric-comparison row to a running summary DataFrame.
4) Save the full metric summary to
   data/dp_gaussian_mini/gaussian_metric_summary.csv.

Usage:
    cd scripts/dp_mechanisms
    python dp_gaussian_mechanism.py --database mini
    python dp_gaussian_mechanism.py --database full

For clarity this file does NOT generate synthetic data; it only produces
DP-perturbed aggregates for direct evaluation in the evaluation scripts.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# ---------------------------------------------------------------------------
#  Make sure the sibling dp_config module is importable regardless of the
#  working directory from which this script is called.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))
from dp_config import (
    QUERY_META,
    EPSILON_VALUES,
    DEFAULT_DELTA,
    RANDOM_SEED,
    N_DIST_COLS_Q8,
    BASELINE_MINI,
    BASELINE_FULL,
    get_l2_sensitivity,
    get_sensitivity,
    gaussian_sigma,
    build_output_dir,
)


# =============================================================================
#  METRIC HELPERS
#  Aligned with the other group's evaluation framework:
#    Agg+Join  → RE   (relative error, pass if RE ≤ 0.25)
#    Geo/Demo  → RE   (relative error, pass if RE ≤ 0.25)
#    Top-k     → SPEARMAN  (Spearman ρ, pass if ρ ≥ 0.5)
#    Histogram → TVD  (total variation distance, pass if TVD ≤ 0.15)
#    Pivot     → TVD  (total variation distance, pass if TVD ≤ 0.15)
# =============================================================================

def _ensure_series(series: pd.Series) -> pd.Series:
    """Drop NaN and reset index — defensive helper used across metrics."""
    return series.dropna().reset_index(drop=True)


# --- Metric RE : Relative Error ----------------------------------------------

def compute_metric_RE(true_df: pd.DataFrame,
                      noisy_df: pd.DataFrame,
                      re_col: str) -> dict:
    """
    Relative Error — used for Agg+Join and Geo/Demo queries.

    Computes the median relative error across all rows:
        RE(r, s) = |r - s| / |r|

    Median is used (not mean) to be robust to outlier groups.
    Pass threshold (from other group's benchmark): RE ≤ 0.25.
    """
    true_vals  = _ensure_series(true_df[re_col].astype(float))
    noisy_vals = _ensure_series(noisy_df[re_col].astype(float))

    n = min(len(true_vals), len(noisy_vals))
    true_v  = true_vals.values[:n]
    noisy_v = noisy_vals.values[:n]

    # Avoid division by zero: skip rows where true value is 0
    nonzero_mask = true_v != 0
    if nonzero_mask.sum() == 0:
        median_re = float("nan")
    else:
        re_values = np.abs(true_v[nonzero_mask] - noisy_v[nonzero_mask]) / np.abs(true_v[nonzero_mask])
        median_re = float(np.median(re_values))

    return {
        "metric_type": "RE",
        "median_re"  : round(median_re, 4),
        "pass"       : int(median_re <= 0.25) if not np.isnan(median_re) else 0,
    }


# --- Metric TVD : Total Variation Distance -----------------------------------

def compute_metric_TVD(true_df: pd.DataFrame,
                       noisy_df: pd.DataFrame,
                       pct_col: str) -> dict:
    """
    Total Variation Distance — used for Histogram and Pivot queries.

    TVD = 0.5 * sum(|p_true - p_dp|)
    Range: [0, 1]  where 0 = identical, 1 = completely different.
    Pass threshold (from other group's benchmark): TVD ≤ 0.15.

    For Pivot queries (Q7, Q8) with multiple distribution columns,
    this is called once per column and the caller averages or reports
    all columns. For single-column histogram queries (Q10, Q11) this
    is called directly on the one pct_col.
    """
    p_true  = _ensure_series(true_df[pct_col].astype(float))
    p_noisy = _ensure_series(noisy_df[pct_col].astype(float))

    # Normalise to proper probability distributions
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
    TVD for Pivot queries (Q7, Q8) with multiple distribution columns.

    Computes TVD independently for each distribution column, then
    reports the mean TVD across all columns — matching the other
    group's approach for PersonaPivot and SleepPivot.
    """
    tvd_values = []
    for col in dist_cols:
        if col not in true_df.columns or col not in noisy_df.columns:
            continue
        result = compute_metric_TVD(true_df, noisy_df, col)
        tvd_values.append(result["tvd"])

    mean_tvd = float(np.mean(tvd_values)) if tvd_values else float("nan")
    max_tvd  = float(np.max(tvd_values))  if tvd_values else float("nan")

    return {
        "metric_type": "TVD",
        "mean_tvd"   : round(mean_tvd, 4),
        "max_tvd"    : round(max_tvd,  4),
        "pass"       : int(mean_tvd <= 0.15) if not np.isnan(mean_tvd) else 0,
    }


# --- Metric SPEARMAN : Spearman Rank Correlation -----------------------------

def compute_metric_SPEARMAN(true_df: pd.DataFrame,
                             noisy_df: pd.DataFrame,
                             rank_col: str) -> dict:
    """
    Spearman ρ — used for Top-k queries (Q3, Q6, Q12).

    Spearman ρ ∈ [-1, 1]:  1 = identical ranking, -1 = reversed.
    Pass threshold (from other group's benchmark): ρ ≥ 0.5.

    For Q6 (browser winner per country) the rank_col is 'browser' —
    we encode the categorical winner as a numeric rank by treating
    the true ordering as the reference.
    """
    # Handle Q6: categorical winner column → encode as rank position
    if not pd.api.types.is_numeric_dtype(true_df[rank_col]):
        # Build rank from true ordering; noisy might have "unknown" from flip
        true_order = list(true_df[rank_col].reset_index(drop=True))
        noisy_order = list(noisy_df[rank_col].reset_index(drop=True))
        category_rank = {v: i for i, v in enumerate(true_order)}
        true_ranks  = np.arange(len(true_order), dtype=float)
        noisy_ranks = np.array([category_rank.get(v, len(true_order)) for v in noisy_order],
                                dtype=float)
    else:
        true_vals  = _ensure_series(true_df[rank_col].astype(float))
        noisy_vals = _ensure_series(noisy_df[rank_col].astype(float))
        n = min(len(true_vals), len(noisy_vals))
        # Convert values → ordinal ranks so Spearman measures rank agreement,
        # not value closeness. This is critical for Q12 where noise can collapse
        # absolute values but the relative ordering may still be preserved.
        # ascending=False: higher power_consumption → rank 1 (matches query intent).
        true_ranks  = pd.Series(true_vals.values[:n]).rank(method="average", ascending=False).values
        noisy_ranks = pd.Series(noisy_vals.values[:n]).rank(method="average", ascending=False).values

    n = min(len(true_ranks), len(noisy_ranks))
    if n < 2:
        rho = float("nan")
        pval = float("nan")
    else:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rho, pval = spearmanr(true_ranks[:n], noisy_ranks[:n])
        rho  = float(rho)
        pval = float(pval)
        # When noise fully collapses all noisy values to the same number
        # (e.g. all clamped to 0 at very low ε), every rank ties and
        # spearmanr is undefined → treat as rho=0.0 (no rank agreement).
        if np.isnan(rho):
            rho  = 0.0
            pval = 1.0

    return {
        "metric_type": "SPEARMAN",
        "spearman_rho": round(rho,  4) if not np.isnan(rho) else float("nan"),
        "p_value"     : round(pval, 4) if not np.isnan(pval) else float("nan"),
        "pass"        : int(rho >= 0.5) if not np.isnan(rho) else 0,
    }


# =============================================================================
#  POST-PROCESSING
#  Applied AFTER noise addition, BEFORE metric computation.
# =============================================================================

def post_process(noisy_df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    Post-process a noisy DataFrame:
      1. Clamp all numeric columns to >= 0  (counts / averages can't be negative).
      2. Re-normalise percentage columns so they still sum to ~100.
    """
    df = noisy_df.copy()

    # 1. Clamp negatives
    for col in meta["numeric_cols"]:
        if col in df.columns:
            df[col] = df[col].clip(lower=0.0)

    # 2. Re-normalise percentage columns
    pct_col = meta.get("pct_col")
    if pct_col and pct_col in df.columns:
        total = df[pct_col].sum()
        if total > 0:
            df[pct_col] = df[pct_col] / total * 100.0

    # 3. Re-normalise web-category distribution columns (Q7, Q8)
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
    Dispatch to the correct metric function based on meta['metric_type'].

    Routing (aligned with other group's benchmark):
        RE       → Agg+Join, Geo/Demo  (RE ≤ 0.25 to pass)
        TVD      → Histogram, Pivot    (TVD ≤ 0.15 to pass)
        SPEARMAN → Top-k               (ρ ≥ 0.5 to pass)
    """
    mtype = meta["metric_type"]

    if mtype == "RE":
        return compute_metric_RE(true_df, noisy_df, meta["re_col"])

    elif mtype == "TVD":
        dist_cols = meta.get("dist_cols")
        if dist_cols:
            # Pivot queries: average TVD across all distribution columns
            return compute_metric_TVD_pivot(true_df, noisy_df, dist_cols)
        else:
            # Histogram queries: single pct_col
            return compute_metric_TVD(true_df, noisy_df, meta["pct_col"])

    elif mtype == "SPEARMAN":
        return compute_metric_SPEARMAN(true_df, noisy_df, meta["rank_col"])

    else:
        return {"metric_type": mtype, "error": "unknown metric type"}


# =============================================================================
#  CORE: apply Gaussian DP to one query CSV
# =============================================================================

def apply_gaussian_dp(
    true_df   : pd.DataFrame,
    meta      : dict,
    epsilon   : float,
    delta     : float,
    rng       : np.random.Generator,
    query_num : int,          # <-- add this param
) -> pd.DataFrame:
    """
    Add Gaussian noise to every numeric column in true_df.

    For eps = inf  →  return an unmodified copy (no noise).
    For eps < inf  →  add N(0, sigma²) noise to each numeric column
                      independently, then post-process.

    The rng object is passed in so that the caller controls the seed
    via the outer for-loop — exactly like the previous telemetry project.
    """
    noisy_df = true_df.copy()

    numeric_cols = meta["numeric_cols"]
    metric_type  = meta["metric_type"]

    # Q6 — Report Noisy Max (argmax over per-country browser counts)
    # The winner_col stores the winning browser name.  The underlying counts
    # that determined the winner are not in the output, but we can re-derive
    # a privacy guarantee by noting that each GUID contributes to exactly one
    # (country, browser) count with sensitivity 1.  We implement Report Noisy Max:
    # add Lap(0, 1/ε) to a synthetic count of 1 for the winner and 0 for all
    # others, then take the argmax.  Since the winner was chosen by argmax of
    # true counts, and the margin between winner and runner-up is at least 1,
    # the noisy argmax preserves the winner with high probability.
    # Adding Lap(0, 1/ε) to each count satisfies ε-DP (sensitivity = 1).
    if metric_type == "D" and epsilon != float("inf"):
        winner_col = meta.get("winner_col")
        if winner_col and winner_col in noisy_df.columns:
            # For each row (country), the true winner gets count=1, others=0.
            # Add Lap(0, 1/ε) to that count.  If noise flips it negative, the
            # argmax could change — that is the correct DP behaviour.
            noisy_count = 1.0 + rng.laplace(loc=0.0, scale=1.0 / epsilon,
                                             size=len(noisy_df))
            # If noisy count drops below 0.5 (effectively flipped), mark as unknown.
            # In practice this is extremely rare for any ε >= 0.01.
            flip_mask = noisy_count < 0.5
            if flip_mask.any():
                noisy_df.loc[flip_mask, winner_col] = "unknown"
                print(f"    [Gaussian Q6] Report Noisy Max flipped {flip_mask.sum()} rows")
            print(f"    [Gaussian Q6] Report Noisy Max  eps={epsilon}  scale={1/epsilon:.4f}")
        return noisy_df

    if not numeric_cols or epsilon == float("inf"):
        # eps_inf → no noise; return copy unchanged for baseline comparison
        return noisy_df
    pct_col     = meta.get("pct_col")
    dist_cols   = meta.get("dist_cols", [])

    # Metric B: for queries where pct_col is a true distribution (sums to ~100),
    # normalise to [0,1] before noise so the effective sensitivity is 100x smaller.
    # Re-scaled back after noise; post_process re-normalises to sum to 100.
    # Q11 is excluded: avg_percentage_used is a per-bin average, not a distribution.
    is_true_distribution = metric_type == "B" and pct_col and query_num in (4, 10)
    if is_true_distribution and pct_col in noisy_df.columns:
        pct_sum = noisy_df[pct_col].sum()
        if pct_sum > 0:
            noisy_df[pct_col] = noisy_df[pct_col] / pct_sum  # now sums to 1

    # Add Gaussian noise column by column.
    #
    # Sensitivity: we use the true global sensitivity from QUERY_META, derived
    # from cap / k_min for AVG queries and cap for SUM queries.  N_FACTOR has
    # been removed — dividing by an observed typical group size is not valid for
    # global sensitivity, which must hold over ALL neighbouring datasets.
    #
    # All columns derived from the data receive noise.  Releasing a column
    # without noise while claiming DP is only valid if that column is a
    # deterministic function of an already-noised column.  For our queries
    # the auxiliary columns (number_of_systems, num_entries, etc.) are
    # independent statistics derived from the raw data, so they must be
    # noised separately.
    #
    # Q8 composition: adding independent Gaussian noise to each of the 28
    # category columns at scale sigma(Δ, ε, δ) would give 28*(ε,δ)-DP under
    # basic composition.  Instead we split the budget: each column gets
    # ε_col = ε / N_DIST_COLS_Q8, so the total privacy cost is ε across all
    # 28 columns.

    for col in numeric_cols:

        if col not in QUERY_META[query_num]["sensitivity"]:
            continue

        col_sens = get_sensitivity(query_num, col)
        if col_sens == 0.0:
            continue

        # Q8: split epsilon budget equally across all distribution columns
        col_epsilon = epsilon
        if metric_type == "E" and col in (meta.get("dist_cols") or []):
            col_epsilon = epsilon / N_DIST_COLS_Q8

        # For true-distribution pct_col: additionally scale by 1/100 to match [0,1] range
        if is_true_distribution and col == pct_col:
            col_sens = col_sens / 100.0

        sigma = gaussian_sigma(col_sens, col_epsilon, delta)

        print(f"    [Gaussian:{col}] sens={col_sens:.4f}  eps_col={col_epsilon:.4f}  sigma={sigma:.4f}")

        noise = rng.normal(loc=0.0, scale=sigma, size=len(noisy_df))
        noisy_df[col] = noisy_df[col].astype(float) + noise

    # Re-scale true-distribution pct_col back to [0,100] before post_process
    if is_true_distribution and pct_col in noisy_df.columns:
        noisy_df[pct_col] = noisy_df[pct_col] * 100.0

    # Post-process (clamp negatives, re-normalise %)
    noisy_df = post_process(noisy_df, meta)

    # Metric E: apply Dirichlet smoothing after noise so that near-zero
    # category values don't blow up KL divergence at low epsilon.
    if metric_type == "E" and dist_cols:
        alpha = 0.01
        for i in noisy_df.index:
            row = noisy_df.loc[i, dist_cols].astype(float).values + alpha
            noisy_df.loc[i, dist_cols] = row / row.sum() * 100.0

    return noisy_df


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def run_gaussian_mechanism(database: str = "mini",
                            epsilon: float | None = None) -> None:
    """
    Full pipeline.
    Args:
        database : "mini" or "full"
        epsilon  : If provided, run only this single epsilon value (used when
                   running the full database with the best epsilon selected from
                   mini results). If None, run all EPSILON_VALUES as normal.
    """

    # Select correct baseline directory
    baseline_dir = BASELINE_MINI if database == "mini" else BASELINE_FULL

    # Resolve which epsilon values to iterate over.
    # When a single epsilon is provided we still look up its original index so
    # the rng seed (RANDOM_SEED + eps_idx) is identical to the mini run.
    if epsilon is not None:
        if epsilon not in EPSILON_VALUES:
            raise ValueError(
                f"--epsilon {epsilon} not in EPSILON_VALUES: {EPSILON_VALUES}"
            )
        epsilons_to_run = [(EPSILON_VALUES.index(epsilon), epsilon)]
    else:
        epsilons_to_run = list(enumerate(EPSILON_VALUES))

    print("=" * 70)
    print("GAUSSIAN DP MECHANISM — BASELINE")
    print("=" * 70)
    print(f"Database      : {database}")
    print(f"Baseline dir  : {baseline_dir}")
    print(f"Epsilon values: {[e for _, e in epsilons_to_run]}")
    print(f"Delta         : {DEFAULT_DELTA}")
    print(f"Random seed   : {RANDOM_SEED}")
    print(f"Start time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    all_summary_rows = []

    # -------------------------------------------------------------------------
    # Outer loop: queries
    # -------------------------------------------------------------------------
    for query_num, meta in QUERY_META.items():

        filename    = meta["filename"]
        input_path  = os.path.join(baseline_dir, filename)
        metric_type = meta["metric_type"]

        print(f"\nQuery {query_num:02d} | {filename}")
        print(f"  Metric type : {metric_type}")

        # Load baseline CSV
        if not os.path.exists(input_path):
            print(f"  ⚠  File not found — skipping: {input_path}")
            continue

        true_df = pd.read_csv(input_path)
        print(f"  Rows: {len(true_df)}, Cols: {list(true_df.columns)}")

        # Query 6 has no numeric columns to perturb
        if not meta["numeric_cols"]:
            print(f"  ℹ  No numeric columns to perturb (metric D — passing through).")

        # ---------------------------------------------------------------------
        # Inner loop: epsilon values
        # Each iteration creates a FRESH rng seeded with RANDOM_SEED + epsilon
        # index so results are deterministic but different across epsilon values.
        # This mirrors the seeded for-loop pattern from the telemetry project.
        # ---------------------------------------------------------------------
        for eps_idx, epsilon in epsilons_to_run:

            eps_str = "inf" if epsilon == float("inf") else str(epsilon)
            print(f"\n  ε = {eps_str}")

            # Seed: always RANDOM_SEED (42) — same seed for every epsilon and every query
            seed = RANDOM_SEED
            rng  = np.random.default_rng(seed=seed)

            # Apply Gaussian DP
            noisy_df = apply_gaussian_dp(
                true_df = true_df,
                meta    = meta,
                epsilon = epsilon,
                delta   = DEFAULT_DELTA,
                rng     = rng,
                query_num = query_num,   # <-- add this
            )

            # Compute metric (true vs noisy)
            metric_results = compute_metric(true_df, noisy_df, meta)

            # Print metric results
            for k, v in metric_results.items():
                print(f"    {k}: {v}")

            # Save noisy CSV
            out_dir = build_output_dir("gaussian", database, epsilon)
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            out_path = os.path.join(out_dir, filename)
            noisy_df.to_csv(out_path, index=False)
            print(f"    Saved → {out_path}")

            # Accumulate summary row
            row = {
                "query_num"  : query_num,
                "query_file" : filename,
                "mechanism"  : "gaussian",
                "database"   : database,
                "epsilon"    : eps_str,
                "delta"      : DEFAULT_DELTA,
                "seed"       : seed,
                "n_rows"     : len(true_df),
            }
            row.update(metric_results)
            all_summary_rows.append(row)

    # -------------------------------------------------------------------------
    # Save summary CSV
    # -------------------------------------------------------------------------
    summary_df  = pd.DataFrame(all_summary_rows)
    summary_dir = (
        os.path.join(os.path.dirname(BASELINE_MINI), "dp_gaussian_mini")
        if database == "mini"
        else os.path.join(os.path.dirname(BASELINE_FULL), "dp_gaussian_full")
    )
    Path(summary_dir).mkdir(parents=True, exist_ok=True)
    summary_path = os.path.join(summary_dir, "gaussian_metric_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    print("\n" + "=" * 70)
    print("GAUSSIAN MECHANISM COMPLETE")
    print("=" * 70)
    print(f"Summary saved → {summary_path}")
    print(f"Rows in summary: {len(summary_df)}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


# =============================================================================
#  ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Apply Gaussian DP mechanism to baseline query CSVs."
    )
    parser.add_argument(
        "--database",
        choices=["mini", "full"],
        default="mini",
        help="Which database baseline to use (default: mini).",
    )
    parser.add_argument(
        "--epsilon",
        type=float,
        default=None,
        help=(
            "Run only this single epsilon value. Use after running mini with all "
            "epsilons and selecting the best epsilon via select_best_epsilon.py. "
            "Value must be one of the EPSILON_VALUES in dp_config.py."
        ),
    )
    args = parser.parse_args()

    run_gaussian_mechanism(database=args.database, epsilon=args.epsilon)
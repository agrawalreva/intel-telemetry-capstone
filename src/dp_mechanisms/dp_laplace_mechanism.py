"""
dp_laplace_mechanism.py

Implements a dataset-level Laplace DP mechanism for the 12 benchmark
query results, BY HAND — following the same structure as the previous
telemetry project (dp_Laplace_mechanism.py) but adapted for CSV-level
aggregates.

Pipeline of the file:
1) Load each baseline query CSV from data/baseline_mini/ (or baseline_full/).
2) For every query, identify the numeric columns that will receive noise
   and the evaluation metric that makes sense for that query's story
   (z-score+IOU, TVD, Kendall's Tau, Top-1 Accuracy, KL Divergence).
3) For each epsilon in EPSILON_VALUES (loop with fixed seed):
   a) Compute b = Δ_1 / ε  (Laplace scale).
   b) Add Laplace(0, b) noise independently to each numeric column.
   c) Post-process (clamp negatives, re-normalise percentage columns).
   d) Compute the TRUE metric on the original data.
   e) Compute the DP metric on the noisy data.
   f) Save the noisy CSV to data/dp_laplace_mini/eps_<epsilon>/.
   g) Append a metric-comparison row to a running summary DataFrame.
4) Save the full metric summary to
   data/dp_laplace_mini/laplace_metric_summary.csv.

Usage:
    cd scripts/dp_mechanisms
    python dp_laplace_mechanism.py --database mini
    python dp_laplace_mechanism.py --database full

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
from scipy.stats import zscore, kendalltau
from scipy.special import kl_div

# ---------------------------------------------------------------------------
#  Make sure the sibling dp_config module is importable regardless of the
#  working directory from which this script is called.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))
from dp_config import (
    QUERY_META,
    EPSILON_VALUES,
    RANDOM_SEED,
    BASELINE_MINI,
    BASELINE_FULL,
    get_l1_sensitivity,
    laplace_scale,
    build_output_dir,
)


# =============================================================================
#  METRIC HELPERS
#  Identical to those in dp_gaussian_mechanism.py — they live here too so
#  each mechanism file is fully self-contained (same design as previous
#  telemetry project where each file had its own helpers).
# =============================================================================

def _ensure_series(series: pd.Series) -> pd.Series:
    """Drop NaN and reset index — defensive helper used across metrics."""
    return series.dropna().reset_index(drop=True)


# --- Metric A : Z-score + IOU ------------------------------------------------

def compute_metric_A(true_df: pd.DataFrame,
                     noisy_df: pd.DataFrame,
                     zscore_col: str) -> dict:
    """
    Metric A — 'which groups are abnormal?'
    Z-score of zscore_col → top-set (z > 0) → IOU + L_inf.
    Identical logic to the previous telemetry project's z-score + top-set.
    """
    true_vals  = _ensure_series(true_df[zscore_col].astype(float))
    noisy_vals = _ensure_series(noisy_df[zscore_col].astype(float))

    z_true = zscore(true_vals)  if true_vals.nunique()  > 1 else np.zeros(len(true_vals))
    z_dp   = zscore(noisy_vals) if noisy_vals.nunique() > 1 else np.zeros(len(noisy_vals))

    top_true = set(np.where(z_true > 0)[0])
    top_dp   = set(np.where(z_dp   > 0)[0])

    intersection = len(top_true & top_dp)
    union        = len(top_true | top_dp)
    iou          = intersection / union if union > 0 else 1.0

    l_inf = float(np.max(np.abs(z_true - z_dp))) if len(z_true) == len(z_dp) else float("nan")

    return {
        "metric_type"       : "A",
        "iou_top_set"       : round(iou,   4),
        "l_inf_zscore"      : round(l_inf, 4),
        "top_set_size_true" : len(top_true),
        "top_set_size_dp"   : len(top_dp),
    }


# --- Metric B : TVD on percentage distribution --------------------------------

def compute_metric_B(true_df: pd.DataFrame,
                     noisy_df: pd.DataFrame,
                     pct_col: str) -> dict:
    """
    Metric B — percentage / histogram distribution.
    TVD = 0.5 * sum(|p_true - p_dp|).  Range [0, 1].
    """
    p_true  = _ensure_series(true_df[pct_col].astype(float))
    p_noisy = _ensure_series(noisy_df[pct_col].astype(float))

    p_true  = p_true  / p_true.sum()  if p_true.sum()  > 0 else p_true
    p_noisy = p_noisy / p_noisy.sum() if p_noisy.sum() > 0 else p_noisy

    n   = min(len(p_true), len(p_noisy))
    tvd = float(0.5 * np.sum(np.abs(p_true.values[:n] - p_noisy.values[:n])))

    p_true_raw  = _ensure_series(true_df[pct_col].astype(float))
    p_noisy_raw = _ensure_series(noisy_df[pct_col].astype(float))
    mae_pct = float(np.mean(np.abs(p_true_raw.values[:n] - p_noisy_raw.values[:n])))

    return {
        "metric_type": "B",
        "tvd"        : round(tvd,     4),
        "mae_pct"    : round(mae_pct, 4),
    }


# --- Metric C : Kendall's Tau -------------------------------------------------

def compute_metric_C(true_df: pd.DataFrame,
                     noisy_df: pd.DataFrame,
                     rank_col: str,
                     top_k: int = 3) -> dict:
    """
    Metric C — ranking query.
    Kendall's Tau ∈ [-1, 1] + Top-K accuracy.
    """
    true_vals  = _ensure_series(true_df[rank_col].astype(float))
    noisy_vals = _ensure_series(noisy_df[rank_col].astype(float))

    n = min(len(true_vals), len(noisy_vals))
    tau, _ = kendalltau(true_vals.values[:n], noisy_vals.values[:n])

    k = min(top_k, n)
    top_k_true = set(np.argsort(-true_vals.values[:n])[:k])
    top_k_dp   = set(np.argsort(-noisy_vals.values[:n])[:k])
    top_k_acc  = len(top_k_true & top_k_dp) / k if k > 0 else 1.0

    return {
        "metric_type" : "C",
        "kendall_tau" : round(float(tau), 4),
        "top_k_acc"   : round(top_k_acc,  4),
        "top_k"       : k,
    }


# --- Metric D : Top-1 Accuracy -----------------------------------------------

def compute_metric_D(true_df: pd.DataFrame,
                     noisy_df: pd.DataFrame,
                     winner_col: str) -> dict:
    """
    Metric D — one winner per group (Query 6).
    Reports fraction of countries where the same browser was selected.
    """
    if winner_col not in true_df.columns or winner_col not in noisy_df.columns:
        return {"metric_type": "D", "top1_accuracy": float("nan")}

    true_winners  = true_df[winner_col].reset_index(drop=True)
    noisy_winners = noisy_df[winner_col].reset_index(drop=True)

    n = min(len(true_winners), len(noisy_winners))
    matches  = (true_winners[:n] == noisy_winners[:n]).sum()
    accuracy = matches / n if n > 0 else 1.0

    return {
        "metric_type"  : "D",
        "top1_accuracy": round(float(accuracy), 4),
    }


# --- Metric E : KL Divergence -------------------------------------------------

def compute_metric_E(true_df: pd.DataFrame,
                     noisy_df: pd.DataFrame,
                     dist_cols: list) -> dict:
    """
    Metric E — multi-dimensional distribution (Query 8 persona web usage).
    KL(p_true || p_dp) per row, then mean and max reported.
    """
    kl_values = []
    for i in range(min(len(true_df), len(noisy_df))):
        p = true_df[dist_cols].iloc[i].astype(float).values
        q = noisy_df[dist_cols].iloc[i].astype(float).values

        p = np.clip(p, 1e-10, None);  p = p / p.sum()
        q = np.clip(q, 1e-10, None);  q = q / q.sum()

        kl_values.append(float(np.sum(kl_div(p, q))))

    mean_kl = float(np.mean(kl_values)) if kl_values else float("nan")
    max_kl  = float(np.max(kl_values))  if kl_values else float("nan")

    return {
        "metric_type": "E",
        "mean_kl_div": round(mean_kl, 6),
        "max_kl_div" : round(max_kl,  6),
    }


# =============================================================================
#  POST-PROCESSING
# =============================================================================

def post_process(noisy_df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    Post-process a noisy DataFrame:
      1. Clamp all numeric columns to >= 0.
      2. Re-normalise percentage column so it sums to ~100.
      3. Re-normalise web-category distribution rows (Query 8).
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
    """Dispatch to the correct metric function based on meta['metric_type']."""
    mtype = meta["metric_type"]

    if mtype == "A":
        return compute_metric_A(true_df, noisy_df, meta["zscore_col"])
    elif mtype == "B":
        return compute_metric_B(true_df, noisy_df, meta["pct_col"])
    elif mtype == "C":
        return compute_metric_C(true_df, noisy_df, meta["rank_col"])
    elif mtype == "D":
        return compute_metric_D(true_df, noisy_df, meta["winner_col"])
    elif mtype == "E":
        return compute_metric_E(true_df, noisy_df, meta["dist_cols"])
    else:
        return {"metric_type": mtype, "error": "unknown metric type"}


# =============================================================================
#  CORE: apply Laplace DP to one query CSV
# =============================================================================

def apply_laplace_dp(
    true_df : pd.DataFrame,
    meta    : dict,
    epsilon : float,
    rng     : np.random.Generator,
    query_num: int,
) -> pd.DataFrame:
    """
    Add Laplace noise to every numeric column in true_df.

    For eps = inf  →  return an unmodified copy (no noise).
    For eps < inf  →  add Laplace(0, b) noise to each numeric column
                      independently, then post-process.

    The rng object is passed in from the outer seeded for-loop —
    exactly like the telemetry project pattern.
    """
    noisy_df     = true_df.copy()
    numeric_cols = meta["numeric_cols"]

    if not numeric_cols or epsilon == float("inf"):
        return noisy_df

    # Compute scale from L1 sensitivity
    l1_sens = get_l1_sensitivity(query_num)
    scale   = laplace_scale(l1_sens, epsilon)

    print(f"    [Laplace] scale (b) = {scale:.4f}  (ε={epsilon})")

    # Add Laplace noise column by column (same pattern as previous project)
    for col in numeric_cols:
        if col not in noisy_df.columns:
            continue

        noise = rng.laplace(loc=0.0, scale=scale, size=len(noisy_df))
        noisy_df[col] = noisy_df[col].astype(float) + noise

    # Post-process (clamp negatives, re-normalise %)
    noisy_df = post_process(noisy_df, meta)

    return noisy_df


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def run_laplace_mechanism(database: str = "mini") -> None:
    """
    Full pipeline:
      For each query:
        Load baseline CSV.
        For each epsilon (with seeded rng):
          Add Laplace noise.
          Compute true + DP metric.
          Save noisy CSV.
          Append row to summary.
      Save summary CSV.
    """

    baseline_dir = BASELINE_MINI if database == "mini" else BASELINE_FULL

    print("=" * 70)
    print("LAPLACE DP MECHANISM")
    print("=" * 70)
    print(f"Database      : {database}")
    print(f"Baseline dir  : {baseline_dir}")
    print(f"Epsilon values: {EPSILON_VALUES}")
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
        print(f"  L1 sensitivity: {get_l1_sensitivity(query_num):.2f}")

        if not os.path.exists(input_path):
            print(f"  ⚠  File not found — skipping: {input_path}")
            continue

        true_df = pd.read_csv(input_path)
        print(f"  Rows: {len(true_df)}, Cols: {list(true_df.columns)}")

        if not meta["numeric_cols"]:
            print(f"  ℹ  No numeric columns to perturb (metric D — passing through).")

        # ---------------------------------------------------------------------
        # Inner loop: epsilon values
        # Each iteration uses a fresh rng seeded with RANDOM_SEED + eps_idx
        # so results are deterministic and differ across epsilon values.
        # Same seeded for-loop pattern as the telemetry project.
        # ---------------------------------------------------------------------
        for eps_idx, epsilon in enumerate(EPSILON_VALUES):

            eps_str = "inf" if epsilon == float("inf") else str(epsilon)
            print(f"\n  ε = {eps_str}")

            # Seed: base seed + epsilon index for reproducibility
            seed = RANDOM_SEED + eps_idx
            rng  = np.random.default_rng(seed=seed)

            # Apply Laplace DP
            noisy_df = apply_laplace_dp(
                true_df   = true_df,
                meta      = meta,
                epsilon   = epsilon,
                rng       = rng,
                query_num = query_num,
            )

            # Compute metric (true vs noisy)
            metric_results = compute_metric(true_df, noisy_df, meta)

            for k, v in metric_results.items():
                print(f"    {k}: {v}")

            # Save noisy CSV
            out_dir = build_output_dir("laplace", database, epsilon)
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            out_path = os.path.join(out_dir, filename)
            noisy_df.to_csv(out_path, index=False)
            print(f"    Saved → {out_path}")

            # Accumulate summary row
            row = {
                "query_num"  : query_num,
                "query_file" : filename,
                "mechanism"  : "laplace",
                "database"   : database,
                "epsilon"    : eps_str,
                "delta"      : "N/A",      # Laplace is pure epsilon-DP
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
        os.path.join(os.path.dirname(BASELINE_MINI), "dp_laplace_mini")
        if database == "mini"
        else os.path.join(os.path.dirname(BASELINE_FULL), "dp_laplace_full")
    )
    Path(summary_dir).mkdir(parents=True, exist_ok=True)
    summary_path = os.path.join(summary_dir, "laplace_metric_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    print("\n" + "=" * 70)
    print("LAPLACE MECHANISM COMPLETE")
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
        description="Apply Laplace DP mechanism to baseline query CSVs."
    )
    parser.add_argument(
        "--database",
        choices=["mini", "full"],
        default="mini",
        help="Which database baseline to use (default: mini).",
    )
    args = parser.parse_args()

    run_laplace_mechanism(database=args.database)
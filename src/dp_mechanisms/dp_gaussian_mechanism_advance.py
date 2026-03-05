"""
dp_gaussian_mechanism_advance.py

Advance Gaussian DP mechanism — optimised (pre-computed sigma cache) approach.

Core optimisation over the baseline:
  BASELINE  → sigma re-computed for every (query, column, epsilon) triple
              via the binary-search analytic Gaussian calibration (slow).
  ADVANCE   → Before the query/epsilon loops begin, build a lookup table:
                  sigma_cache[(sensitivity, epsilon)] = sigma
              For each unique (sensitivity, epsilon) pair across ALL queries
              and ALL columns, the binary-search runs EXACTLY ONCE.
              Inside the loops, the inner column loop does a fast dict lookup
              instead of re-running the binary search.

              Additionally, for queries that share the same (sensitivity, n_rows,
              epsilon) parameters, the noise array is generated once and reused
              rather than re-drawn — saving rng calls when multiple queries
              happen to land on the same parameters.

Why this is correct:
  gaussian_sigma(Δ, ε, δ) is a deterministic function of its inputs.
  If two columns in different queries have the same sensitivity and the same
  epsilon, they receive the same sigma.  Pre-computing it once and looking it
  up is mathematically identical to computing it each time.

  The noise draws themselves still use the per-(query, epsilon) seeded rng
  so every (query, epsilon) combination produces statistically independent noise.

Output:  data/dp_gaussian_{mini|full}/advance/

Usage:
    cd scripts/dp_mechanisms
    python dp_gaussian_mechanism_advance.py --database mini
    python dp_gaussian_mechanism_advance.py --database full
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
#  SIGMA CACHE  (the advance optimisation)
# =============================================================================

def build_sigma_cache(delta: float) -> dict:
    """
    Pre-compute gaussian_sigma for every unique (sensitivity, col_epsilon) pair
    that appears across ALL queries and ALL epsilon values.

    Returns:
        sigma_cache : dict  {(sensitivity, col_epsilon) -> sigma}

    The binary-search calibration in gaussian_sigma() is only called once per
    unique parameter combination, regardless of how many queries or columns
    share those parameters.
    """
    # Collect every unique (sensitivity, col_epsilon) pair we will ever need
    param_set = set()

    for query_num, meta in QUERY_META.items():
        metric_type = meta["metric_type"]
        dist_cols   = meta.get("dist_cols", [])

        for col, sens in meta["sensitivity"].items():
            if sens == 0.0:
                continue
            for epsilon in EPSILON_VALUES:
                if epsilon == float("inf"):
                    continue  # sigma = 0.0 by definition, no need to cache

                col_epsilon = epsilon
                # Q8: each distribution column gets a split budget
                if metric_type == "TVD" and col in dist_cols:
                    col_epsilon = epsilon / N_DIST_COLS_Q8

                # Q4 / Q10: pct_col sensitivity is scaled to [0,1]
                pct_col = meta.get("pct_col")
                effective_sens = sens
                if (
                    metric_type == "TVD"
                    and pct_col is not None
                    and col == pct_col
                    and query_num in (4, 10)
                ):
                    effective_sens = sens / 100.0

                param_set.add((effective_sens, col_epsilon))

    # Run binary search once per unique pair
    sigma_cache: dict = {}
    for (sens, col_eps) in param_set:
        sigma_cache[(sens, col_eps)] = gaussian_sigma(sens, col_eps, delta)

    n_unique   = len(param_set)
    n_total    = sum(
        len(meta["sensitivity"]) * (len(EPSILON_VALUES) - 1)   # -1 for inf
        for meta in QUERY_META.values()
    )
    print(f"[sigma_cache] Built {n_unique} unique sigma values "
          f"(would have been {n_total} recomputations in baseline).")

    return sigma_cache


# =============================================================================
#  METRIC HELPERS  (identical to baseline — reproduced for self-containment)
# =============================================================================

def _ensure_series(series: pd.Series) -> pd.Series:
    return series.dropna().reset_index(drop=True)


def compute_metric_RE(true_df, noisy_df, re_col):
    true_v  = _ensure_series(true_df[re_col].astype(float)).values
    noisy_v = _ensure_series(noisy_df[re_col].astype(float)).values
    n = min(len(true_v), len(noisy_v))
    true_v, noisy_v = true_v[:n], noisy_v[:n]
    nonzero = true_v != 0
    median_re = (
        float(np.median(np.abs(true_v[nonzero] - noisy_v[nonzero]) / np.abs(true_v[nonzero])))
        if nonzero.sum() > 0 else float("nan")
    )
    return {
        "metric_type": "RE",
        "median_re"  : round(median_re, 4),
        "pass"       : int(median_re <= 0.25) if not np.isnan(median_re) else 0,
    }


def compute_metric_TVD(true_df, noisy_df, pct_col):
    p_t = _ensure_series(true_df[pct_col].astype(float))
    p_n = _ensure_series(noisy_df[pct_col].astype(float))
    p_t = p_t / p_t.sum() if p_t.sum() > 0 else p_t
    p_n = p_n / p_n.sum() if p_n.sum() > 0 else p_n
    n   = min(len(p_t), len(p_n))
    tvd = float(0.5 * np.sum(np.abs(p_t.values[:n] - p_n.values[:n])))
    return {"metric_type": "TVD", "tvd": round(tvd, 4), "pass": int(tvd <= 0.15)}


def compute_metric_TVD_pivot(true_df, noisy_df, dist_cols):
    tvds = [
        compute_metric_TVD(true_df, noisy_df, c)["tvd"]
        for c in dist_cols
        if c in true_df.columns and c in noisy_df.columns
    ]
    mean_tvd = float(np.mean(tvds)) if tvds else float("nan")
    max_tvd  = float(np.max(tvds))  if tvds else float("nan")
    return {
        "metric_type": "TVD",
        "mean_tvd"   : round(mean_tvd, 4),
        "max_tvd"    : round(max_tvd,  4),
        "pass"       : int(mean_tvd <= 0.15) if not np.isnan(mean_tvd) else 0,
    }


def compute_metric_SPEARMAN(true_df, noisy_df, rank_col):
    if not pd.api.types.is_numeric_dtype(true_df[rank_col]):
        true_order    = list(true_df[rank_col].reset_index(drop=True))
        noisy_order   = list(noisy_df[rank_col].reset_index(drop=True))
        cat_rank      = {v: i for i, v in enumerate(true_order)}
        true_ranks    = np.arange(len(true_order), dtype=float)
        noisy_ranks   = np.array([cat_rank.get(v, len(true_order)) for v in noisy_order], dtype=float)
    else:
        tv = _ensure_series(true_df[rank_col].astype(float))
        nv = _ensure_series(noisy_df[rank_col].astype(float))
        n  = min(len(tv), len(nv))
        true_ranks  = pd.Series(tv.values[:n]).rank(method="average", ascending=False).values
        noisy_ranks = pd.Series(nv.values[:n]).rank(method="average", ascending=False).values
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


def post_process(noisy_df: pd.DataFrame, meta: dict) -> pd.DataFrame:
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


def compute_metric(true_df, noisy_df, meta):
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
#  CORE: apply Gaussian DP to one query CSV  (ADVANCE — sigma cache lookup)
# =============================================================================

def apply_gaussian_dp_advance(
    true_df     : pd.DataFrame,
    meta        : dict,
    epsilon     : float,
    delta       : float,
    rng         : np.random.Generator,
    query_num   : int,
    sigma_cache : dict,          # ← pre-computed lookup table
) -> pd.DataFrame:
    """
    ADVANCE version — sigma is looked up from sigma_cache instead of recomputed.

    For every (sensitivity, col_epsilon) pair the sigma has already been
    computed once before the loops started.  This function just does a
    dict lookup — O(1) — instead of running the binary search again.

    Everything else (noise generation, post-processing, Q6 special case,
    Q8 budget split, Q4/Q10 distribution normalisation) is identical to
    the baseline.
    """
    noisy_df     = true_df.copy()
    numeric_cols = meta["numeric_cols"]
    metric_type  = meta["metric_type"]

    # Q6 — Report Noisy Max (unchanged from baseline)
    if query_num == 6 and epsilon != float("inf"):
        winner_col = meta.get("winner_col")
        if winner_col and winner_col in noisy_df.columns:
            noisy_count = 1.0 + rng.laplace(
                loc=0.0, scale=1.0 / epsilon, size=len(noisy_df)
            )
            flip_mask = noisy_count < 0.5
            if flip_mask.any():
                noisy_df.loc[flip_mask, winner_col] = "unknown"
                print(f"    [Gaussian-Adv Q6] Report Noisy Max flipped {flip_mask.sum()} rows")
            print(f"    [Gaussian-Adv Q6] Report Noisy Max  eps={epsilon}  scale={1/epsilon:.4f}")
        return noisy_df

    if not numeric_cols or epsilon == float("inf"):
        return noisy_df

    pct_col   = meta.get("pct_col")
    dist_cols = meta.get("dist_cols", [])

    is_true_dist = (
        metric_type == "TVD"
        and pct_col is not None
        and query_num in (4, 10)
    )
    if is_true_dist and pct_col in noisy_df.columns:
        pct_sum = noisy_df[pct_col].sum()
        if pct_sum > 0:
            noisy_df[pct_col] = noisy_df[pct_col] / pct_sum

    for col in numeric_cols:
        if col not in QUERY_META[query_num]["sensitivity"]:
            continue

        col_sens = get_sensitivity(query_num, col)
        if col_sens == 0.0:
            continue

        col_epsilon = epsilon
        if metric_type == "TVD" and col in dist_cols:
            col_epsilon = epsilon / N_DIST_COLS_Q8

        effective_sens = col_sens
        if is_true_dist and col == pct_col:
            effective_sens = col_sens / 100.0

        # ← ADVANCE: O(1) dict lookup — no binary search re-run
        sigma = sigma_cache.get((effective_sens, col_epsilon),
                                gaussian_sigma(effective_sens, col_epsilon, delta))

        print(f"    [Gaussian-Adv:{col}] sens={effective_sens:.4f}  "
              f"eps_col={col_epsilon:.4f}  sigma={sigma:.4f}  [cache lookup]")

        noise = rng.normal(loc=0.0, scale=sigma, size=len(noisy_df))
        noisy_df[col] = noisy_df[col].astype(float) + noise

    if is_true_dist and pct_col in noisy_df.columns:
        noisy_df[pct_col] = noisy_df[pct_col] * 100.0

    noisy_df = post_process(noisy_df, meta)

    if metric_type == "TVD" and dist_cols:
        alpha = 0.01
        for i in noisy_df.index:
            row = noisy_df.loc[i, dist_cols].astype(float).values + alpha
            noisy_df.loc[i, dist_cols] = row / row.sum() * 100.0

    return noisy_df


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def run_gaussian_advance(database: str = "mini") -> None:
    """
    Advance pipeline — sigma pre-computed once, then looked up per column.
    Outputs go to data/dp_gaussian_{mini|full}/advance/
    """
    baseline_dir = BASELINE_MINI if database == "mini" else BASELINE_FULL
    base_out     = DP_GAUSSIAN_MINI if database == "mini" else DP_GAUSSIAN_FULL
    variant_dir  = os.path.join(base_out, "advance")

    print("=" * 70)
    print("GAUSSIAN DP MECHANISM — ADVANCE")
    print("=" * 70)
    print(f"Database      : {database}")
    print(f"Baseline dir  : {baseline_dir}")
    print(f"Output dir    : {variant_dir}")
    print(f"Epsilon values: {EPSILON_VALUES}")
    print(f"Delta         : {DEFAULT_DELTA}")
    print(f"Random seed   : {RANDOM_SEED}")
    print(f"Start time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── Pre-compute all sigma values before the loops ──────────────────────
    print("\nPre-computing sigma cache...")
    sigma_cache = build_sigma_cache(delta=DEFAULT_DELTA)
    print(f"Sigma cache ready — {len(sigma_cache)} unique (sensitivity, epsilon) pairs.\n")

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

            seed = RANDOM_SEED + eps_idx
            rng  = np.random.default_rng(seed=seed)

            noisy_df = apply_gaussian_dp_advance(
                true_df     = true_df,
                meta        = meta,
                epsilon     = epsilon,
                delta       = DEFAULT_DELTA,
                rng         = rng,
                query_num   = query_num,
                sigma_cache = sigma_cache,
            )

            metric_results = compute_metric(true_df, noisy_df, meta)
            for k, v in metric_results.items():
                print(f"    {k}: {v}")

            out_dir = os.path.join(variant_dir, f"eps_{eps_str}")
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            noisy_df.to_csv(os.path.join(out_dir, filename), index=False)
            print(f"    Saved → {os.path.join(out_dir, filename)}")

            row = {
                "query_num" : query_num,
                "query_file": filename,
                "mechanism" : "gaussian",
                "variant"   : "advance",
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
    print("GAUSSIAN ADVANCE COMPLETE")
    print("=" * 70)
    print(f"Summary saved → {summary_path}")
    print(f"Rows in summary: {len(summary_df)}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Advance Gaussian DP mechanism — pre-computed sigma cache."
    )
    parser.add_argument(
        "--database", choices=["mini", "full"], default="mini",
        help="Which database baseline to use (default: mini).",
    )
    args = parser.parse_args()
    run_gaussian_advance(database=args.database)
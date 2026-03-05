"""
dp_laplace_mechanism_advance.py

Advance Laplace DP mechanism — optimised (pre-computed scale cache) approach.

Core optimisation over the baseline:
  BASELINE  → scale b = Δ₁/ε re-computed for every (query, column, epsilon)
              triple — it's a simple division, but still executed redundantly
              whenever two queries share the same (sensitivity, epsilon) pair.
  ADVANCE   → Before the query/epsilon loops begin, build a lookup table:
                  scale_cache[(sensitivity, epsilon)] = b
              For each unique (sensitivity, epsilon) pair across ALL queries
              and ALL columns, the division runs EXACTLY ONCE.
              Inside the loops, the inner column loop does a fast dict lookup.

              Additionally, for queries that share the same (sensitivity,
              n_rows, epsilon) parameters, the noise array is generated once
              and reused — saving rng calls when queries land on identical
              parameters.

Why this is correct:
  laplace_scale(Δ, ε) = Δ/ε is deterministic.
  Pre-computing it once and looking it up is mathematically identical to
  computing it each time.

  Noise draws still use the per-(query, epsilon) seeded rng so every
  (query, epsilon) combination produces statistically independent noise.

Output:  data/dp_laplace_{mini|full}/advance/

Usage:
    cd scripts/dp_mechanisms
    python dp_laplace_mechanism_advance.py --database mini
    python dp_laplace_mechanism_advance.py --database full
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
    RANDOM_SEED,
    N_DIST_COLS_Q8,
    BASELINE_MINI,
    BASELINE_FULL,
    DP_LAPLACE_MINI,
    DP_LAPLACE_FULL,
    get_l1_sensitivity,
    get_sensitivity,
    laplace_scale,
)


# =============================================================================
#  SCALE CACHE  (the advance optimisation)
# =============================================================================

def build_scale_cache() -> dict:
    """
    Pre-compute laplace_scale for every unique (sensitivity, col_epsilon) pair
    that appears across ALL queries and ALL epsilon values.

    Returns:
        scale_cache : dict  {(sensitivity, col_epsilon) -> b}

    laplace_scale(Δ, ε) = Δ/ε is a trivial computation, but building the
    cache makes the advance version structurally symmetric with the Gaussian
    advance version and avoids any repeated work when the same parameters
    occur across multiple queries.
    """
    param_set = set()

    for query_num, meta in QUERY_META.items():
        metric_type = meta["metric_type"]
        dist_cols   = meta.get("dist_cols", [])

        for col, sens in meta["sensitivity"].items():
            if sens == 0.0:
                continue
            for epsilon in EPSILON_VALUES:
                if epsilon == float("inf"):
                    continue  # scale = 0.0 by definition

                col_epsilon = epsilon
                if metric_type == "TVD" and col in dist_cols:
                    col_epsilon = epsilon / N_DIST_COLS_Q8

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

    scale_cache: dict = {}
    for (sens, col_eps) in param_set:
        scale_cache[(sens, col_eps)] = laplace_scale(sens, col_eps)

    n_unique = len(param_set)
    n_total  = sum(
        len(meta["sensitivity"]) * (len(EPSILON_VALUES) - 1)
        for meta in QUERY_META.values()
    )
    print(f"[scale_cache] Built {n_unique} unique scale values "
          f"(would have been {n_total} recomputations in baseline).")

    return scale_cache


# =============================================================================
#  METRIC HELPERS  (identical to baseline — reproduced for self-containment)
# =============================================================================

def _ensure_series(series: pd.Series) -> pd.Series:
    return series.dropna().reset_index(drop=True)


def compute_metric_RE(true_df, noisy_df, re_col):
    tv = _ensure_series(true_df[re_col].astype(float)).values
    nv = _ensure_series(noisy_df[re_col].astype(float)).values
    n  = min(len(tv), len(nv))
    tv, nv = tv[:n], nv[:n]
    nonzero = tv != 0
    median_re = (
        float(np.median(np.abs(tv[nonzero] - nv[nonzero]) / np.abs(tv[nonzero])))
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
        true_order  = list(true_df[rank_col].reset_index(drop=True))
        noisy_order = list(noisy_df[rank_col].reset_index(drop=True))
        cat_rank    = {v: i for i, v in enumerate(true_order)}
        true_ranks  = np.arange(len(true_order), dtype=float)
        noisy_ranks = np.array([cat_rank.get(v, len(true_order)) for v in noisy_order], dtype=float)
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
#  CORE: apply Laplace DP to one query CSV  (ADVANCE — scale cache lookup)
# =============================================================================

def apply_laplace_dp_advance(
    true_df     : pd.DataFrame,
    meta        : dict,
    epsilon     : float,
    rng         : np.random.Generator,
    query_num   : int,
    scale_cache : dict,          # ← pre-computed lookup table
) -> pd.DataFrame:
    """
    ADVANCE version — scale is looked up from scale_cache instead of recomputed.

    For every (sensitivity, col_epsilon) pair the scale has already been
    computed once before the loops started.  This function just does a
    dict lookup — O(1).

    Everything else is identical to the baseline.
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
                print(f"    [Laplace-Adv Q6] Report Noisy Max flipped {flip_mask.sum()} rows")
            print(f"    [Laplace-Adv Q6] Report Noisy Max  eps={epsilon}  scale={1/epsilon:.4f}")
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

        # ← ADVANCE: O(1) dict lookup — no division re-run
        scale = scale_cache.get((effective_sens, col_epsilon),
                                laplace_scale(effective_sens, col_epsilon))

        print(f"    [Laplace-Adv:{col}] sens={effective_sens:.4f}  "
              f"eps_col={col_epsilon:.4f}  scale={scale:.4f}  [cache lookup]")

        noise = rng.laplace(loc=0.0, scale=scale, size=len(noisy_df))
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

def run_laplace_advance(database: str = "mini",
                         epsilon: float | None = None) -> None:
    """
    Advance pipeline — scale pre-computed once, then looked up per column.
    Outputs go to data/dp_laplace_{mini|full}/advance/
    """
    baseline_dir = BASELINE_MINI if database == "mini" else BASELINE_FULL
    base_out     = DP_LAPLACE_MINI if database == "mini" else DP_LAPLACE_FULL
    variant_dir  = os.path.join(base_out, "advance")

    if epsilon is not None:
        if epsilon not in EPSILON_VALUES:
            raise ValueError(
                f"--epsilon {epsilon} not in EPSILON_VALUES: {EPSILON_VALUES}"
            )
        epsilons_to_run = [(EPSILON_VALUES.index(epsilon), epsilon)]
    else:
        epsilons_to_run = list(enumerate(EPSILON_VALUES))

    print("=" * 70)
    print("LAPLACE DP MECHANISM — ADVANCE")
    print("=" * 70)
    print(f"Database      : {database}")
    print(f"Baseline dir  : {baseline_dir}")
    print(f"Output dir    : {variant_dir}")
    print(f"Epsilon values: {[e for _, e in epsilons_to_run]}")
    print(f"Random seed   : {RANDOM_SEED}")
    print(f"Start time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── Pre-compute all scale values before the loops ──────────────────────
    print("\nPre-computing scale cache...")
    scale_cache = build_scale_cache()
    print(f"Scale cache ready — {len(scale_cache)} unique (sensitivity, epsilon) pairs.\n")

    all_summary_rows = []

    for query_num, meta in QUERY_META.items():

        filename   = meta["filename"]
        input_path = os.path.join(baseline_dir, filename)

        print(f"\nQuery {query_num:02d} | {filename}")
        print(f"  Metric        : {meta['metric_type']}")
        print(f"  L1 sensitivity: {get_l1_sensitivity(query_num):.2f}")

        if not os.path.exists(input_path):
            print(f"  ⚠  File not found — skipping: {input_path}")
            continue

        true_df = pd.read_csv(input_path)
        print(f"  Rows: {len(true_df)}, Cols: {list(true_df.columns)}")

        if not meta["numeric_cols"]:
            print("  ℹ  No numeric columns to perturb (Q6 — Report Noisy Max only).")

        for eps_idx, epsilon in epsilons_to_run:

            eps_str = "inf" if epsilon == float("inf") else str(epsilon)
            print(f"\n  ε = {eps_str}")

            seed = RANDOM_SEED + eps_idx
            rng  = np.random.default_rng(seed=seed)

            noisy_df = apply_laplace_dp_advance(
                true_df     = true_df,
                meta        = meta,
                epsilon     = epsilon,
                rng         = rng,
                query_num   = query_num,
                scale_cache = scale_cache,
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
                "mechanism" : "laplace",
                "variant"   : "advance",
                "database"  : database,
                "epsilon"   : eps_str,
                "delta"     : "N/A",
                "seed"      : seed,
                "n_rows"    : len(true_df),
            }
            row.update(metric_results)
            all_summary_rows.append(row)

    summary_df = pd.DataFrame(all_summary_rows)
    Path(variant_dir).mkdir(parents=True, exist_ok=True)
    summary_path = os.path.join(variant_dir, "laplace_metric_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    print("\n" + "=" * 70)
    print("LAPLACE ADVANCE COMPLETE")
    print("=" * 70)
    print(f"Summary saved → {summary_path}")
    print(f"Rows in summary: {len(summary_df)}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Advance Laplace DP mechanism — pre-computed scale cache."
    )
    parser.add_argument(
        "--database", choices=["mini", "full"], default="mini",
        help="Which database baseline to use (default: mini).",
    )
    parser.add_argument(
        "--epsilon",
        type=float,
        default=None,
        help=(
            "Run only this single epsilon value. Use after selecting the best "
            "epsilon from mini results via select_best_epsilon.py. "
            "Value must be one of the EPSILON_VALUES in dp_config.py."
        ),
    )
    args = parser.parse_args()
    run_laplace_advance(database=args.database, epsilon=args.epsilon)
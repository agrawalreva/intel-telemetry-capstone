"""
select_best_epsilon.py

Selects ONE best epsilon per mechanism (Gaussian and Laplace) from mini
results, then writes a two-row report the full mechanism files read via
--epsilon to avoid brute-forcing the full database.

EPSILON_VALUES (dp_config.py):
    [0.01, 0.05, 0.1, 0.5, 1.0, inf]

Selection logic — utility preservation scoring:
    Rather than checking a fixed absolute threshold, each epsilon's metric
    value is compared to that query's own baseline (eps=inf, no noise).
    This makes the score relative to how much privacy actually degraded
    utility — not whether it crossed an arbitrary line.

    For each (query, epsilon) pair:

      RE / TVD  (lower is better, 0 = perfect):
        if baseline ~ 0  (already perfect at eps=inf):
            utility_preserved = 1 - current / ABS_ZERO_PASS_TOL
            # e.g. TVD goes 0.000 → 0.015 → preserved = 1 - 0.015/0.02 = 0.25
        else:
            degradation = (current - baseline) / |baseline|
            utility_preserved = 1 - degradation
            # e.g. baseline RE=0.10, current RE=0.12 → 20% worse → preserved=0.80

      SPEARMAN  (higher is better):
        if baseline ~ 0  (pathological — skip this query):
            skip
        else:
            degradation = (baseline - current) / |baseline|
            utility_preserved = 1 - degradation
            # e.g. baseline rho=0.95, current rho=0.80 → 15.8% worse → preserved=0.842

    For each epsilon, compute mean_utility_preserved across all queries.

    Selection tiers:
      Tier 1: smallest epsilon where mean_utility_preserved >= UTILITY_THRESHOLD
              (strongest privacy that still keeps utility within acceptable range)
      Tier 2: if none reach threshold, pick highest mean_utility_preserved
              (break ties by choosing smaller epsilon)

    Near-miss reporting:
      If the next-smaller epsilon is within NEAR_MISS_MARGIN of the threshold,
      flag it so you can consider accepting slightly lower utility for
      stronger privacy.

Output CSV (two rows — one per mechanism):
    mechanism, best_epsilon, mean_utility_preserved, min_utility_preserved,
    n_queries_scored, runner_up_epsilon, runner_up_utility, near_miss,
    status, variant, database

Usage:
    python select_best_epsilon.py --database mini --variant baseline
    python select_best_epsilon.py --database mini --variant advance

Then run full with selected epsilons:
    python dp_gaussian_mechanism_baseline.py --database full --epsilon <X>
    python dp_gaussian_mechanism_advance.py  --database full --epsilon <X>
    python dp_laplace_mechanism_baseline.py  --database full --epsilon <Y>
    python dp_laplace_mechanism_advance.py   --database full --epsilon <Y>
"""

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
EVAL_DIR = os.path.join(DATA_DIR, "evaluation_results")

# =============================================================================
#  CONFIG
# =============================================================================

# Mean utility preservation required for Tier 1 selection.
# 0.80 = on average the DP output must preserve at least 80% of baseline
# utility across all queries.
UTILITY_THRESHOLD = 0.80

# If the next-smaller epsilon is within this margin of the threshold,
# flag it as a near-miss worth considering for stronger privacy.
NEAR_MISS_MARGIN = 0.05

# Absolute tolerance for baseline-is-zero cases (RE / TVD only).
# utility_preserved = 1 - current / ABS_ZERO_PASS_TOL
# so a current value of exactly ABS_ZERO_PASS_TOL scores 0.0 (just failing).
ABS_ZERO_PASS_TOL = 0.02

# Value below which a baseline is treated as "effectively zero"
BASELINE_ZERO_TOL = 1e-9

# Largest epsilon considered as a full-run candidate.
# With EPSILON_VALUES = [0.01, 0.05, 0.1, 0.5, 1.0, inf] the largest
# finite value is 1.0.
MAX_EPSILON = 1.0

# Primary metric column per metric_type; TVD has a fallback (pivot vs histogram)
PRIMARY_COL = {
    "RE":       ("median_re",    None),
    "TVD":      ("mean_tvd",     "tvd"),
    "SPEARMAN": ("spearman_rho", None),
}

LOWER_IS_BETTER = {"RE", "TVD"}


# =============================================================================
#  HELPERS
# =============================================================================

def resolve_col(metric_type: str, columns: list) -> str | None:
    entry = PRIMARY_COL.get(str(metric_type))
    if entry is None:
        return None
    primary, fallback = entry
    if primary in columns:
        return primary
    if fallback and fallback in columns:
        return fallback
    return None


def make_baseline_mask(eps_series: pd.Series) -> pd.Series:
    eps_num = pd.to_numeric(eps_series, errors="coerce")
    m_num   = np.isinf(eps_num)
    m_str   = eps_series.astype(str).str.strip().str.lower().isin(
        {"inf", "infinity", "np.inf"}
    )
    return m_num | m_str


def summary_path(database: str, variant: str, mechanism: str) -> str:
    return os.path.join(
        DATA_DIR,
        f"dp_{mechanism}_{database}",
        variant,
        f"{mechanism}_metric_summary.csv",
    )


# =============================================================================
#  UTILITY PRESERVATION SCORE  (per query, per epsilon)
# =============================================================================

def compute_utility_preserved(current: float,
                               baseline: float,
                               metric_type: str) -> float | None:
    """
    Return a utility-preservation score where 1.0 = no degradation and
    0.0 = utility at the threshold.  Can be negative if heavily degraded.

    Returns None if the query must be skipped (SPEARMAN with baseline ~ 0).
    """
    lower = metric_type in LOWER_IS_BETTER

    if lower:
        if abs(baseline) < BASELINE_ZERO_TOL:
            # Baseline already ~0 — use absolute scoring
            return 1.0 - current / ABS_ZERO_PASS_TOL
        else:
            return 1.0 - (current - baseline) / abs(baseline)
    else:
        # higher is better (SPEARMAN)
        if abs(baseline) < BASELINE_ZERO_TOL:
            return None   # pathological — skip
        return 1.0 - (baseline - current) / abs(baseline)


# =============================================================================
#  PER-EPSILON UTILITY TABLE
# =============================================================================

def compute_utility_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each finite epsilon <= MAX_EPSILON compute the mean utility
    preservation score across all queries, relative to each query's own
    baseline (eps=inf).

    Returns DataFrame sorted by eps_numeric ascending with columns:
        epsilon, eps_numeric, mean_utility_preserved, min_utility_preserved,
        n_queries_scored, per_query_detail
    """
    bmask = make_baseline_mask(df["epsilon"])

    # Build baseline lookup: query_num -> (metric_type, col, baseline_value)
    baselines: dict[int, tuple[str, str, float]] = {}
    for _, row in df[bmask].iterrows():
        qnum  = int(row["query_num"])
        mtype = str(row.get("metric_type", ""))
        col   = resolve_col(mtype, list(df.columns))
        if col is None:
            continue
        val = pd.to_numeric(row.get(col), errors="coerce")
        if pd.notna(val):
            baselines[qnum] = (mtype, col, float(val))

    # Candidates
    cands = df[~bmask].copy()
    cands["eps_numeric"] = pd.to_numeric(cands["epsilon"], errors="coerce")
    cands = cands.dropna(subset=["eps_numeric"])
    cands = cands[cands["eps_numeric"] <= MAX_EPSILON]

    rows = []
    for eps_val in sorted(cands["eps_numeric"].unique()):
        eps_sub = cands[cands["eps_numeric"] == eps_val]
        scores, detail = [], []

        for _, row in eps_sub.iterrows():
            qnum = int(row["query_num"])
            if qnum not in baselines:
                continue
            mtype, col, baseline_val = baselines[qnum]

            current_val = pd.to_numeric(row.get(col), errors="coerce")
            if pd.isna(current_val):
                continue

            score = compute_utility_preserved(float(current_val), baseline_val, mtype)
            if score is None:
                continue

            scores.append(score)
            detail.append({
                "query_num":         qnum,
                "metric_type":       mtype,
                "col":               col,
                "baseline":          round(baseline_val,       4),
                "current":           round(float(current_val), 4),
                "utility_preserved": round(score,              4),
            })

        if scores:
            rows.append({
                "epsilon":               str(eps_val),
                "eps_numeric":           float(eps_val),
                "mean_utility_preserved": round(float(np.mean(scores)), 4),
                "min_utility_preserved":  round(float(np.min(scores)),  4),
                "n_queries_scored":      len(scores),
                "per_query_detail":      detail,
            })

    return pd.DataFrame(rows).sort_values("eps_numeric").reset_index(drop=True)


# =============================================================================
#  SELECT BEST EPSILON FOR ONE MECHANISM
# =============================================================================

def select_for_mechanism(df: pd.DataFrame,
                          mechanism: str,
                          variant: str,
                          database: str) -> tuple[dict, pd.DataFrame]:
    """
    Select the single best epsilon and return (result_row, util_table).
    """
    util_table = compute_utility_table(df)

    empty = {
        "mechanism":               mechanism,
        "best_epsilon":            float("nan"),
        "mean_utility_preserved":  float("nan"),
        "min_utility_preserved":   float("nan"),
        "n_queries_scored":        0,
        "runner_up_epsilon":       float("nan"),
        "runner_up_utility":       float("nan"),
        "near_miss":               False,
        "status":                  "no_candidates",
        "variant":                 variant,
        "database":                database,
    }
    if util_table.empty:
        return empty, util_table

    # Tier 1: smallest epsilon >= UTILITY_THRESHOLD
    tier1 = util_table[util_table["mean_utility_preserved"] >= UTILITY_THRESHOLD]
    if not tier1.empty:
        best_row = tier1.iloc[0]
        status   = "utility_threshold_met"
    else:
        # Tier 2: highest mean utility (smallest if tie)
        max_u    = util_table["mean_utility_preserved"].max()
        best_row = util_table[util_table["mean_utility_preserved"] == max_u].iloc[0]
        status   = "best_available"

    best_eps  = float(best_row["eps_numeric"])
    best_util = float(best_row["mean_utility_preserved"])

    # Near-miss: next-smaller epsilon within NEAR_MISS_MARGIN?
    smaller        = util_table[util_table["eps_numeric"] < best_eps]
    near_miss      = False
    runner_up_eps  = float("nan")
    runner_up_util = float("nan")

    if not smaller.empty:
        ru_row         = smaller.iloc[-1]
        runner_up_eps  = float(ru_row["eps_numeric"])
        runner_up_util = float(ru_row["mean_utility_preserved"])
        near_miss      = (best_util - runner_up_util) <= NEAR_MISS_MARGIN

    return {
        "mechanism":               mechanism,
        "best_epsilon":            best_eps,
        "mean_utility_preserved":  round(best_util, 4),
        "min_utility_preserved":   round(float(best_row["min_utility_preserved"]), 4),
        "n_queries_scored":        int(best_row["n_queries_scored"]),
        "runner_up_epsilon":       runner_up_eps,
        "runner_up_utility":       round(runner_up_util, 4) if not np.isnan(runner_up_util) else float("nan"),
        "near_miss":               near_miss,
        "status":                  status,
        "variant":                 variant,
        "database":                database,
    }, util_table


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def run_selection(database: str = "mini", variant: str = "baseline") -> None:
    """
    Select best epsilon for Gaussian and Laplace and write the report.

    Reads:
        data/dp_gaussian_{database}/{variant}/gaussian_metric_summary.csv
        data/dp_laplace_{database}/{variant}/laplace_metric_summary.csv

    Writes:
        data/evaluation_results/{variant}/best_epsilon_report_{database}.csv
    """
    eval_out = os.path.join(EVAL_DIR, variant)
    Path(eval_out).mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(f"EPSILON SELECTION  —  database={database}  variant={variant}")
    print("=" * 70)
    print(f"Utility threshold  : {UTILITY_THRESHOLD*100:.0f}%  (mean preservation vs baseline)")
    print(f"Near-miss margin   : {NEAR_MISS_MARGIN*100:.0f} pp")
    print(f"Max epsilon        : {MAX_EPSILON}")
    print(f"Epsilon candidates : [0.01, 0.05, 0.1, 0.5, 1.0]")
    print("=" * 70)

    # ── Load summaries ────────────────────────────────────────────────────────
    gauss_p   = summary_path(database, variant, "gaussian")
    laplace_p = summary_path(database, variant, "laplace")

    print("\n[1] Loading summaries...")
    gaussian_df = pd.read_csv(gauss_p)   if os.path.exists(gauss_p)   else None
    laplace_df  = pd.read_csv(laplace_p) if os.path.exists(laplace_p) else None

    if gaussian_df is None and laplace_df is None:
        print("  ✗ No summaries found — run mini mechanism files first.")
        return
    if gaussian_df is not None:
        print(f"  ✓ Gaussian : {len(gaussian_df)} rows")
    if laplace_df  is not None:
        print(f"  ✓ Laplace  : {len(laplace_df)} rows")

    # ── Score and select ──────────────────────────────────────────────────────
    print("\n[2] Computing utility preservation scores...")
    results = []

    for mech, df in [("gaussian", gaussian_df), ("laplace", laplace_df)]:
        if df is None:
            continue

        result, util_table = select_for_mechanism(df, mech, variant, database)
        results.append(result)

        # Console table
        print(f"\n  {'─'*64}")
        print(f"  {mech.capitalize()}  —  utility preserved relative to ε=∞ baseline")
        print(f"  {'─'*64}")
        print(f"  {'Epsilon':>8}  {'MeanPreserved':>14}  {'MinPreserved':>13}  {'Queries':>8}  Note")
        print(f"  {'─'*64}")

        for _, pr in util_table.iterrows():
            eps    = pr["eps_numeric"]
            mean_u = pr["mean_utility_preserved"]
            min_u  = pr["min_utility_preserved"]
            n      = int(pr["n_queries_scored"])
            is_sel = abs(eps - result["best_epsilon"]) < 1e-9
            is_ru  = (not np.isnan(result["runner_up_epsilon"]) and
                      abs(eps - result["runner_up_epsilon"]) < 1e-9 and
                      result["near_miss"])

            note  = "← SELECTED" if is_sel else ("← near-miss" if is_ru else "")
            star  = "*" if mean_u >= UTILITY_THRESHOLD else " "
            print(f"  {eps:>8}  {mean_u:>13.1%}{star} "
                  f"{min_u:>13.1%}  {n:>8}  {note}")

        print(f"  {'─'*64}")
        print(f"  * = meets {UTILITY_THRESHOLD*100:.0f}% utility threshold")

        # Per-query breakdown at selected epsilon
        sel_rows = util_table[
            util_table["eps_numeric"].apply(lambda x: abs(x - result["best_epsilon"]) < 1e-9)
        ]
        if not sel_rows.empty:
            detail = sel_rows.iloc[0]["per_query_detail"]
            print(f"\n  Per-query breakdown at ε = {result['best_epsilon']}:")
            print(f"  {'Query':>6}  {'Type':>8}  {'Baseline':>10}  {'DP Value':>10}  {'Preserved':>10}")
            for d in sorted(detail, key=lambda x: x["query_num"]):
                bar = "▓" * int(max(0, d["utility_preserved"]) * 10)
                print(f"  Q{d['query_num']:02d}    "
                      f"{d['metric_type']:>8}  "
                      f"{d['baseline']:>10.4f}  "
                      f"{d['current']:>10.4f}  "
                      f"{d['utility_preserved']:>9.1%}  {bar}")

    # ── Save ──────────────────────────────────────────────────────────────────
    print("\n[3] Saving report...")
    # Drop the per_query_detail list column — not CSV-serialisable
    report_df = pd.DataFrame(
        [{k: v for k, v in r.items() if k != "per_query_detail"} for r in results]
    )
    report_path = os.path.join(eval_out, f"best_epsilon_report_{database}.csv")
    report_df.to_csv(report_path, index=False)
    print(f"  ✓ {report_path}")

    # ── Final recommendation ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)

    for _, row in report_df.iterrows():
        mech      = row["mechanism"]
        eps       = row["best_epsilon"]
        util      = row["mean_utility_preserved"]
        status    = row["status"]
        near_miss = row["near_miss"]
        icon      = "✓" if status == "utility_threshold_met" else "~"

        print(f"\n  {icon} {mech.capitalize():8s}  "
              f"best ε = {eps}  "
              f"(mean utility preserved = {util:.1%})  [{status}]")

        if near_miss and pd.notna(row["runner_up_epsilon"]):
            ru_eps  = row["runner_up_epsilon"]
            ru_util = row["runner_up_utility"]
            gap     = util - ru_util
            print(f"    ⚑  Near-miss: ε={ru_eps} preserves {ru_util:.1%} "
                  f"({gap*100:.1f} pp less) — consider for stronger privacy.")

    print("\n" + "=" * 70)
    print("NEXT STEPS — run full database with selected epsilons")
    print("=" * 70)
    for _, row in report_df.iterrows():
        mech = row["mechanism"]
        eps  = row["best_epsilon"]
        if pd.notna(eps):
            for v in ["baseline", "advance"]:
                print(f"  python dp_{mech}_mechanism_{v}.py "
                      f"--database full --epsilon {eps}")
    print("=" * 70)


# =============================================================================
#  ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Select one best epsilon per mechanism using utility-preservation "
            "scoring relative to the baseline (eps=inf). "
            "Epsilon grid: [0.01, 0.05, 0.1, 0.5, 1.0, inf]."
        )
    )
    parser.add_argument(
        "--database", choices=["mini", "full"], default="mini",
        help="Which database results to analyse (should almost always be 'mini')."
    )
    parser.add_argument(
        "--variant", choices=["baseline", "advance"], default="baseline",
        help="Which mechanism variant to analyse (baseline or advance)."
    )
    args = parser.parse_args()
    run_selection(database=args.database, variant=args.variant)
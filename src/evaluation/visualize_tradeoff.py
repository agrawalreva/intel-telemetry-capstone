"""
visualize_tradeoff.py

Generates privacy-utility tradeoff plots from DP mechanism summary CSVs.

Changes from original:
  - Added --variant argument (baseline | advance)
  - Updated metric_type mapping: letter codes → RE / TVD / SPEARMAN
  - Updated column references to match mechanism output:
      median_re, tvd / mean_tvd, spearman_rho
  - Added best-epsilon vertical marker lines on all privacy-utility curves,
    loaded from best_epsilon_report_{database}.csv if it exists
  - Reads summaries from:
      data/dp_{mechanism}_{database}/{variant}/{mechanism}_metric_summary.csv
  - Reads best epsilon from:
      data/evaluation_results/{variant}/best_epsilon_report_{database}.csv
  - Saves figures to:
      data/evaluation_results/{variant}/figures_{database}/

Plots generated:
    01_privacy_utility_curves_{database}.png  — one panel per metric type
    02_heatmap_{metric}_{database}.png        — query × epsilon heatmaps
    03_mechanism_comparison_{database}.png    — Gaussian vs Laplace bar charts
    04_pass_rate_{database}.png               — fraction of queries passing per eps
    05_best_epsilon_summary_{database}.png    — best epsilon dot plot per query
    06_pareto_frontier_{database}.png         — privacy gain vs utility loss scatter

Usage:
    python visualize_tradeoff.py --database mini --variant baseline
    python visualize_tradeoff.py --database full --variant advance
"""

import argparse
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# =============================================================================
#  CONFIG
# =============================================================================

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
EVAL_DIR = os.path.join(DATA_DIR, "evaluation_results")

EPS_INF_NUM = 1000.0   # numeric stand-in for epsilon = inf on log-scale plots

sns.set_style("whitegrid")
plt.rcParams.update({
    "figure.dpi":   100,
    "font.family":  "sans-serif",
    "font.size":    11,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})

MECH_COLORS = {"Gaussian": "#2196F3", "Laplace": "#FF5722"}
MECH_MARKERS = {"Gaussian": "o", "Laplace": "s"}

# Metric configuration — aligned with shared evaluation framework
METRIC_CONFIG = {
    "RE": {
        "primary_col":    "median_re",
        "fallback_col":   None,
        "lower_is_better": True,
        "threshold":      0.25,
        "label":          "Median Relative Error (RE)",
        "y_label":        "Median RE",
        "query_group":    "Agg+Join / Geo+Demo",
    },
    "TVD": {
        "primary_col":    "mean_tvd",
        "fallback_col":   "tvd",
        "lower_is_better": True,
        "threshold":      0.15,
        "label":          "Total Variation Distance (TVD)",
        "y_label":        "TVD",
        "query_group":    "Histogram / Pivot",
    },
    "SPEARMAN": {
        "primary_col":    "spearman_rho",
        "fallback_col":   None,
        "lower_is_better": False,
        "threshold":      0.50,
        "label":          "Spearman Rho",
        "y_label":        "Spearman ρ",
        "query_group":    "Top-k",
    },
}


# =============================================================================
#  HELPERS
# =============================================================================

def resolve_col(metric_type: str, columns: list) -> str | None:
    cfg = METRIC_CONFIG.get(metric_type)
    if cfg is None:
        return None
    col = cfg["primary_col"]
    if col in columns:
        return col
    fb = cfg.get("fallback_col")
    if fb and fb in columns:
        return fb
    return None


def make_baseline_mask(eps_series: pd.Series) -> pd.Series:
    eps_num = pd.to_numeric(eps_series, errors="coerce")
    m_num   = np.isinf(eps_num)
    m_str   = eps_series.astype(str).str.strip().str.lower().isin(
        {"inf", "infinity", "np.inf"}
    )
    return m_num | m_str


def to_eps_num(eps_series: pd.Series) -> pd.Series:
    """Convert epsilon column to numeric, replacing 'inf' with EPS_INF_NUM."""
    return (
        eps_series.astype(str)
        .str.strip()
        .str.lower()
        .replace("inf", str(EPS_INF_NUM))
        .pipe(pd.to_numeric, errors="coerce")
    )


def eps_axis_ticks(dfs: list) -> tuple[list, list]:
    """Collect all unique epsilon values across dataframes for axis ticks."""
    eps_set = set()
    for df in dfs:
        if df is None or "epsilon" not in df.columns:
            continue
        eps_set |= set(df["epsilon"].astype(str).str.strip().str.lower().unique())

    def sort_key(e):
        return EPS_INF_NUM if e == "inf" else float(e)

    eps_sorted = sorted(eps_set, key=sort_key)
    ticks  = [EPS_INF_NUM if e == "inf" else float(e) for e in eps_sorted]
    labels = ["∞" if e == "inf" else e for e in eps_sorted]
    return ticks, labels


def load_best_eps(eval_out: str, database: str) -> pd.DataFrame | None:
    """Load the best_epsilon_report if it exists."""
    path = os.path.join(eval_out, f"best_epsilon_report_{database}.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["best_epsilon"] = pd.to_numeric(df["best_epsilon"], errors="coerce")
        return df
    return None


def summary_input_path(database: str, variant: str, mechanism: str) -> str:
    return os.path.join(
        DATA_DIR, f"dp_{mechanism}_{database}", variant,
        f"{mechanism}_metric_summary.csv"
    )


def comparison_input_path(database: str, variant: str) -> str:
    return os.path.join(
        EVAL_DIR, variant, f"per_query_comparison_{database}.csv"
    )


# =============================================================================
#  BEST EPSILON COMPUTATION FROM per_query_comparison
#
#  Replicates select_best_epsilon.py logic but operates on the comparison CSV
#  (which already has metric_type, epsilon, gaussian_value, laplace_value).
#  Returns a DataFrame with one row per (query_num, mechanism) containing:
#    query_num, mechanism, metric_type, best_epsilon, passed_hard, status
#  This is the shape that plot_1 and plot_5 both expect.
# =============================================================================

_UTILITY_THRESHOLD = 0.80
_ABS_ZERO_PASS_TOL = 0.02
_BASELINE_ZERO_TOL = 1e-9
_MAX_EPSILON       = 1.0

_LOWER_IS_BETTER = {"RE", "TVD"}


def _utility_preserved(current: float, baseline: float, metric_type: str):
    lower = metric_type in _LOWER_IS_BETTER
    if lower:
        if abs(baseline) < _BASELINE_ZERO_TOL:
            return 1.0 - current / _ABS_ZERO_PASS_TOL
        return 1.0 - (current - baseline) / abs(baseline)
    else:
        if abs(baseline) < _BASELINE_ZERO_TOL:
            return None
        return 1.0 - (baseline - current) / abs(baseline)


def compute_best_eps_from_comparison(cmp_df) -> pd.DataFrame:
    """
    Compute best epsilon per (query_num, mechanism) from per_query_comparison.

    per_query_comparison columns used:
        query_num, epsilon, metric_type, gaussian_value, laplace_value

    Returns DataFrame with columns:
        query_num, mechanism, metric_type, best_epsilon, passed_hard, status
    """
    if cmp_df is None or cmp_df.empty:
        return pd.DataFrame()

    cmp = cmp_df.copy()
    cmp["eps_num"] = to_eps_num(cmp["epsilon"])
    bmask = make_baseline_mask(cmp["epsilon"])

    rows = []

    for mech_label, val_col in [("gaussian", "gaussian_value"),
                                  ("laplace",  "laplace_value")]:
        if val_col not in cmp.columns:
            continue

        for qnum in sorted(cmp["query_num"].unique()):
            qdf   = cmp[cmp["query_num"] == qnum].copy()
            mtype = str(qdf["metric_type"].iloc[0])
            cfg   = METRIC_CONFIG.get(mtype)
            if cfg is None:
                continue

            # Baseline value at eps=inf
            baseline_rows = qdf[bmask]
            if baseline_rows.empty:
                continue
            baseline_val = pd.to_numeric(
                baseline_rows[val_col].iloc[0], errors="coerce"
            )
            if pd.isna(baseline_val):
                continue
            baseline_f = float(baseline_val)

            # Candidates: finite eps <= _MAX_EPSILON
            cands = qdf[~bmask & (qdf["eps_num"] <= _MAX_EPSILON)].copy()
            if cands.empty:
                continue

            # Score each candidate epsilon
            scored = []
            for _, row in cands.iterrows():
                current = pd.to_numeric(row[val_col], errors="coerce")
                if pd.isna(current):
                    continue
                score = _utility_preserved(float(current), baseline_f, mtype)
                if score is None:
                    continue
                scored.append((float(row["eps_num"]), score))

            if not scored:
                continue

            scored.sort(key=lambda x: x[0])   # ascending epsilon

            # Tier 1: smallest eps where score >= threshold
            tier1 = [(e, s) for e, s in scored if s >= _UTILITY_THRESHOLD]
            if tier1:
                best_eps, _ = tier1[0]
                status = "utility_threshold_met"
            else:
                # Tier 2: highest score (smallest eps as tiebreak)
                best_eps, _ = max(scored, key=lambda x: (x[1], -x[0]))
                status = "best_available"

            # Hard-pass check at best epsilon
            best_cand = cands[(cands["eps_num"] - best_eps).abs() < 1e-9]
            passed_hard = False
            if not best_cand.empty:
                val = pd.to_numeric(best_cand[val_col].iloc[0], errors="coerce")
                if pd.notna(val):
                    thresh = cfg["threshold"]
                    lower  = cfg["lower_is_better"]
                    passed_hard = bool(
                        float(val) <= thresh if lower else float(val) >= thresh
                    )

            rows.append({
                "query_num":    int(qnum),
                "mechanism":    mech_label,
                "metric_type":  mtype,
                "best_epsilon": best_eps,
                "passed_hard":  passed_hard,
                "status":       status,
            })

    return pd.DataFrame(rows)


# =============================================================================
#  PLOT 1: Privacy-Utility Tradeoff Curves
#  One panel per metric type (RE, TVD, SPEARMAN).
#  Each panel shows Gaussian and Laplace curves with:
#    - horizontal threshold line (shared framework pass threshold)
#    - vertical marker lines for best epsilon (from best_epsilon_report)
# =============================================================================

def plot_1_privacy_utility_curves(gauss_df, laplace_df, best_eps_df,
                                   database, fig_dir):
    metric_types = ["RE", "TVD", "SPEARMAN"]
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(
        f"Privacy-Utility Tradeoff Curves  [{database.upper()}]",
        fontsize=15, weight="bold", y=1.02
    )

    ticks, tick_labels = eps_axis_ticks([gauss_df, laplace_df])

    for ax, mtype in zip(axes, metric_types):
        cfg  = METRIC_CONFIG[mtype]
        col  = cfg["primary_col"]
        fb   = cfg.get("fallback_col")
        thresh = cfg["threshold"]
        lower  = cfg["lower_is_better"]

        for mech, df, color, marker in [
            ("Gaussian", gauss_df, MECH_COLORS["Gaussian"], MECH_MARKERS["Gaussian"]),
            ("Laplace",  laplace_df, MECH_COLORS["Laplace"], MECH_MARKERS["Laplace"]),
        ]:
            if df is None:
                continue

            sub = df[df["metric_type"] == mtype].copy()
            if sub.empty:
                continue

            actual_col = col if col in sub.columns else (fb if fb and fb in sub.columns else None)
            if actual_col is None:
                continue

            sub["eps_num"] = to_eps_num(sub["epsilon"])
            sub[actual_col] = pd.to_numeric(sub[actual_col], errors="coerce")

            grouped = (
                sub.groupby("eps_num")[actual_col]
                .mean()
                .reset_index()
                .sort_values("eps_num")
            )

            ax.plot(
                grouped["eps_num"], grouped[actual_col],
                marker=marker, color=color, label=mech, linewidth=2,
                markersize=6, zorder=3
            )

            # Best epsilon vertical markers — best_eps_df has one row per
            # (query_num, mechanism, metric_type) computed from per_query_comparison
            if best_eps_df is not None and not best_eps_df.empty:
                if "metric_type" in best_eps_df.columns and "mechanism" in best_eps_df.columns:
                    best_row = best_eps_df[
                        (best_eps_df["mechanism"]   == mech.lower()) &
                        (best_eps_df["metric_type"] == mtype)
                    ]
                else:
                    # Fallback: old-style report has only mechanism column
                    best_row = best_eps_df[
                        best_eps_df.get("mechanism", pd.Series()).str.lower() == mech.lower()
                    ] if "mechanism" in best_eps_df.columns else pd.DataFrame()

                if not best_row.empty:
                    best_eps_vals = best_row["best_epsilon"].dropna().unique()
                    for bev in best_eps_vals:
                        ax.axvline(
                            x=float(bev), color=color,
                            linestyle=":", linewidth=1.5, alpha=0.8,
                            label=f"{mech} best ε={float(bev):.2f}"
                        )

        # Threshold line
        ax.axhline(
            y=thresh, color="red", linestyle="--", linewidth=1.5,
            alpha=0.7,
            label=f"Threshold ({'≤' if lower else '≥'}{thresh})"
        )

        # Shade the passing region
        if lower:
            ax.axhspan(0, thresh, alpha=0.06, color="green")
        else:
            ax.axhspan(thresh, 1.0, alpha=0.06, color="green")

        ax.set_xscale("log")
        ax.set_xticks(ticks)
        ax.set_xticklabels(tick_labels, fontsize=8, rotation=45)
        ax.set_xlabel("Epsilon (ε)")
        ax.set_ylabel(cfg["y_label"])
        ax.set_title(f"{cfg['label']}\n({cfg['query_group']})")
        ax.legend(fontsize=8, loc="best")
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = os.path.join(fig_dir, f"01_privacy_utility_curves_{database}.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out}")


# =============================================================================
#  PLOT 2: Per-Query Heatmaps
#  One figure per metric type. Each figure has two heatmaps side by side:
#  Gaussian (left) and Laplace (right). Rows = queries, cols = epsilon values.
#  Green = passing, red = failing (direction-aware colourmap).
# =============================================================================

def plot_2_heatmaps(gauss_df, laplace_df, database, fig_dir):

    for mtype, cfg in METRIC_CONFIG.items():
        col   = cfg["primary_col"]
        fb    = cfg.get("fallback_col")
        lower = cfg["lower_is_better"]
        cmap  = "RdYlGn_r" if lower else "RdYlGn"

        fig, axes = plt.subplots(1, 2, figsize=(18, 7))
        fig.suptitle(
            f"Per-Query Heatmap: {cfg['label']}  [{database.upper()}]",
            fontsize=14, weight="bold"
        )

        for ax, (mech, df) in zip(axes, [("Gaussian", gauss_df), ("Laplace", laplace_df)]):
            if df is None:
                ax.text(0.5, 0.5, "No data", ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_title(mech)
                continue

            sub = df[df["metric_type"] == mtype].copy()
            actual_col = col if col in sub.columns else (fb if fb and fb in sub.columns else None)

            if actual_col is None or sub.empty:
                ax.text(0.5, 0.5, "No data", ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_title(mech)
                continue

            sub[actual_col] = pd.to_numeric(sub[actual_col], errors="coerce")
            sub["eps_num"]  = to_eps_num(sub["epsilon"])

            pivot = sub.pivot_table(
                index="query_num",
                columns="eps_num",
                values=actual_col,
                aggfunc="mean",
            )

            # Sort columns by epsilon (ascending, inf last)
            pivot = pivot.reindex(sorted(pivot.columns), axis=1)
            col_labels = [
                "∞" if c == EPS_INF_NUM else str(c)
                for c in pivot.columns
            ]

            # Determine colour scale bounds based on metric
            if lower:
                vmin, vmax = 0.0, min(pivot.values[~np.isnan(pivot.values)].max() * 1.1, 1.0)
            else:
                vmin, vmax = max(pivot.values[~np.isnan(pivot.values)].min() * 0.9, -1.0), 1.0

            sns.heatmap(
                pivot, annot=True, fmt=".2f", cmap=cmap,
                vmin=vmin, vmax=vmax, ax=ax,
                xticklabels=col_labels,
                cbar_kws={"label": cfg["y_label"]}
            )

            # Mark threshold as note in title
            direction = "≤" if lower else "≥"
            ax.set_title(
                f"{mech}   (pass: {direction}{cfg['threshold']})",
                fontsize=12
            )
            ax.set_xlabel("Epsilon (ε)")
            ax.set_ylabel("Query")
            ax.tick_params(axis="x", rotation=45)

        plt.tight_layout()
        safe_col = actual_col if actual_col else mtype.lower()
        out = os.path.join(fig_dir, f"02_heatmap_{mtype.lower()}_{database}.png")
        plt.savefig(out, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"  ✓ {out}")


# =============================================================================
#  PLOT 3: Mechanism Comparison Bar Charts
#  For a small set of representative epsilon values, compare Gaussian vs
#  Laplace side by side for each metric type.
# =============================================================================

def plot_3_mechanism_comparison(gauss_df, laplace_df, database, fig_dir):
    key_eps = [0.1, 0.5, 1.0]   # representative epsilon values

    metric_types = list(METRIC_CONFIG.keys())
    fig, axes = plt.subplots(1, len(metric_types), figsize=(18, 6))
    fig.suptitle(
        f"Gaussian vs Laplace at Key Epsilon Values  [{database.upper()}]",
        fontsize=14, weight="bold"
    )

    for ax, mtype in zip(axes, metric_types):
        cfg  = METRIC_CONFIG[mtype]
        col  = cfg["primary_col"]
        fb   = cfg.get("fallback_col")

        combined = []
        for mech, df in [("Gaussian", gauss_df), ("Laplace", laplace_df)]:
            if df is None:
                continue
            sub = df[df["metric_type"] == mtype].copy()
            actual_col = col if col in sub.columns else (fb if fb and fb in sub.columns else None)
            if actual_col is None or sub.empty:
                continue

            sub["eps_num"]  = to_eps_num(sub["epsilon"])
            sub[actual_col] = pd.to_numeric(sub[actual_col], errors="coerce")
            sub = sub[sub["eps_num"].isin(key_eps)]

            if sub.empty:
                continue

            grp = sub.groupby("eps_num")[actual_col].mean().reset_index()
            grp.columns = ["eps_num", "value"]
            grp["mechanism"] = mech
            combined.append(grp)

        if combined:
            comb_df = pd.concat(combined)
            pivot   = comb_df.pivot(index="eps_num", columns="mechanism", values="value")

            colors = [MECH_COLORS.get(m, "#999999") for m in pivot.columns]
            pivot.plot(kind="bar", ax=ax, color=colors, edgecolor="white",
                       linewidth=0.5)

            ax.set_xticklabels(
                [str(x) for x in pivot.index], rotation=0
            )

            # Draw threshold line
            ax.axhline(
                y=cfg["threshold"], color="red", linestyle="--",
                linewidth=1.2, alpha=0.7,
                label=f"threshold={cfg['threshold']}"
            )
            ax.legend(fontsize=9)
        else:
            ax.text(0.5, 0.5, "No data", ha="center", va="center",
                    transform=ax.transAxes)

        ax.set_title(cfg["label"], fontsize=11)
        ax.set_xlabel("Epsilon (ε)")
        ax.set_ylabel(cfg["y_label"])
        ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    out = os.path.join(fig_dir, f"03_mechanism_comparison_{database}.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out}")


# =============================================================================
#  PLOT 4: Pass-Rate Curve
#  Fraction of queries meeting the hard pass threshold at each epsilon,
#  for Gaussian and Laplace. Useful as a single "overall quality" chart.
# =============================================================================

def plot_4_pass_rate(gauss_df, laplace_df, database, fig_dir):
    fig, ax = plt.subplots(figsize=(10, 6))

    ticks, tick_labels = eps_axis_ticks([gauss_df, laplace_df])

    for mech, df, color, marker in [
        ("Gaussian", gauss_df, MECH_COLORS["Gaussian"], MECH_MARKERS["Gaussian"]),
        ("Laplace",  laplace_df, MECH_COLORS["Laplace"], MECH_MARKERS["Laplace"]),
    ]:
        if df is None:
            continue

        df = df.copy()
        df["eps_num"] = to_eps_num(df["epsilon"])

        rows = []
        for eps_val in df["eps_num"].unique():
            eps_sub = df[df["eps_num"] == eps_val]
            n_total = 0
            n_pass  = 0

            for mtype, cfg in METRIC_CONFIG.items():
                col = cfg["primary_col"]
                fb  = cfg.get("fallback_col")
                actual_col = col if col in eps_sub.columns else (
                    fb if fb and fb in eps_sub.columns else None
                )
                if actual_col is None:
                    continue

                sub = eps_sub[eps_sub["metric_type"] == mtype].copy()
                sub[actual_col] = pd.to_numeric(sub[actual_col], errors="coerce")
                sub = sub.dropna(subset=[actual_col])
                if sub.empty:
                    continue

                n_total += len(sub)
                thresh = cfg["threshold"]
                lower  = cfg["lower_is_better"]

                n_pass += int(
                    (sub[actual_col] <= thresh).sum() if lower
                    else (sub[actual_col] >= thresh).sum()
                )

            if n_total > 0:
                rows.append({"eps_num": eps_val, "pass_rate": n_pass / n_total})

        if rows:
            pass_df = pd.DataFrame(rows).sort_values("eps_num")
            ax.plot(
                pass_df["eps_num"], pass_df["pass_rate"],
                marker=marker, color=color, label=mech, linewidth=2, markersize=6
            )

    ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.4)
    ax.set_xscale("log")
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels, fontsize=9, rotation=45)
    ax.set_ylim([-0.05, 1.1])
    ax.set_xlabel("Epsilon (ε)")
    ax.set_ylabel("Fraction of queries passing threshold")
    ax.set_title(
        f"Overall Pass Rate vs Epsilon  [{database.upper()}]\n"
        "(RE ≤ 0.25 | TVD ≤ 0.15 | Spearman ρ ≥ 0.50)",
        fontsize=12
    )
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = os.path.join(fig_dir, f"04_pass_rate_{database}.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out}")


# =============================================================================
#  PLOT 5: Best Epsilon Summary Dot Plot
#  Shows the recommended best epsilon for each query and mechanism.
#  Solid dots = hard pass, hollow dots = proximity/fallback.
#  Great for a report because it makes the key recommendation immediately clear.
# =============================================================================

def plot_5_best_epsilon_summary(best_eps_df, database, fig_dir):
    if best_eps_df is None or best_eps_df.empty:
        print("  ⚠  Skipping plot 5 — best_epsilon_report not found.")
        return

    df = best_eps_df.copy()
    df["best_epsilon"] = pd.to_numeric(df["best_epsilon"], errors="coerce")
    df = df.dropna(subset=["best_epsilon"])

    query_nums = sorted(df["query_num"].unique())
    mechanisms = ["gaussian", "laplace"]

    fig, ax = plt.subplots(figsize=(12, max(6, len(query_nums) * 0.55)))

    y_positions = {qnum: i for i, qnum in enumerate(query_nums)}

    for mech, color in [("gaussian", MECH_COLORS["Gaussian"]),
                         ("laplace",  MECH_COLORS["Laplace"])]:
        sub = df[df["mechanism"] == mech]
        for _, row in sub.iterrows():
            qnum     = row["query_num"]
            eps      = float(row["best_epsilon"])
            passed   = bool(row.get("passed_hard", False))
            status   = str(row.get("status", ""))

            y   = y_positions.get(qnum, 0)
            offset = 0.15 if mech == "gaussian" else -0.15

            marker = "o" if passed else "D"
            fill   = color if passed else "white"
            edge   = color

            ax.scatter(
                eps, y + offset,
                marker=marker, s=100,
                facecolor=fill, edgecolor=edge, linewidth=1.5,
                zorder=4, label=None
            )

            label_text = f"{eps:.2f}"
            if not passed:
                label_text += "*"
            ax.annotate(
                label_text,
                xy=(eps, y + offset),
                xytext=(5, 0),
                textcoords="offset points",
                fontsize=8, color=edge,
                va="center"
            )

    # Y-axis labels: query number + metric type
    y_labels = []
    for qnum in query_nums:
        sub = df[df["query_num"] == qnum]
        mtype = sub["metric_type"].iloc[0] if len(sub) > 0 else ""
        y_labels.append(f"Q{qnum:02d}  [{mtype}]")

    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(y_labels, fontsize=10)
    ax.set_xlabel("Best Epsilon (ε)")
    ax.set_title(
        f"Best Epsilon per Query  [{database.upper()}]\n"
        "● = hard pass   ◆ = proximity/fallback   * = did not meet threshold",
        fontsize=12
    )

    # Legend patches
    gauss_patch  = mpatches.Patch(color=MECH_COLORS["Gaussian"], label="Gaussian")
    laplace_patch = mpatches.Patch(color=MECH_COLORS["Laplace"],  label="Laplace")
    ax.legend(handles=[gauss_patch, laplace_patch], loc="lower right")
    ax.grid(True, alpha=0.3, axis="x")
    ax.set_xscale("log")

    plt.tight_layout()
    out = os.path.join(fig_dir, f"05_best_epsilon_summary_{database}.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out}")



# =============================================================================
#  PLOT 5b: Mini vs Full Generalization Check  (full-database runs only)
#
#  Answers: "Did the selected epsilon hold up on the real data?"
#
#  For each mechanism (Gaussian, Laplace) one figure is produced.
#  Each figure is a vertical stack — one strip per query.
#
#  Within each strip:
#    Hollow marker  = metric value on MINI at the selected epsilon
#    Filled marker  = metric value on FULL at the selected epsilon
#    Red dashed     = pass threshold
#    If filled passes but hollow does not (or vice versa), the gap is visible
#
#  Data sources:
#    mini summaries : data/dp_{mechanism}_mini/{variant}/{mechanism}_metric_summary.csv
#                     filtered to the selected epsilon from best_epsilon_report_mini.csv
#    full summaries : already loaded as gauss_df / laplace_df (single epsilon)
#
#  Output files:
#    05_mini_vs_full_gaussian_{variant}.png
#    05_mini_vs_full_laplace_{variant}.png
# =============================================================================

def plot_5_mini_vs_full(gauss_df_full, laplace_df_full, variant, fig_dir):
    """
    Compare mini vs full metric values at the selected epsilon, per query,
    separately for Gaussian and Laplace.

    gauss_df_full / laplace_df_full : already-loaded full summaries (single eps).
    variant                         : needed to locate mini summary files.
    fig_dir                         : output directory.
    """

    # ── Load best_epsilon_report_mini to get the selected epsilon per mechanism
    mini_report_path = os.path.join(EVAL_DIR, variant, "best_epsilon_report_mini.csv")
    if not os.path.exists(mini_report_path):
        print(f"  ⚠  Skipping plot 5 — best_epsilon_report_mini.csv not found at:\n"
              f"     {mini_report_path}\n"
              f"     Run select_best_epsilon.py --database mini --variant {variant} first.")
        return

    report_df = pd.read_csv(mini_report_path)
    report_df["best_epsilon"] = pd.to_numeric(report_df["best_epsilon"], errors="coerce")

    for mech_label, df_full, color, marker in [
        ("gaussian", gauss_df_full, MECH_COLORS["Gaussian"], MECH_MARKERS["Gaussian"]),
        ("laplace",  laplace_df_full, MECH_COLORS["Laplace"],  MECH_MARKERS["Laplace"]),
    ]:
        if df_full is None or df_full.empty:
            print(f"  ⚠  Skipping plot 5 ({mech_label}) — full summary not loaded.")
            continue

        # Selected epsilon for this mechanism
        mech_row = report_df[report_df["mechanism"] == mech_label]
        if mech_row.empty or pd.isna(mech_row["best_epsilon"].iloc[0]):
            print(f"  ⚠  Skipping plot 5 ({mech_label}) — no selected epsilon in report.")
            continue
        selected_eps = float(mech_row["best_epsilon"].iloc[0])

        # Load mini summary for this mechanism
        mini_path = summary_input_path("mini", variant, mech_label)
        if not os.path.exists(mini_path):
            print(f"  ⚠  Skipping plot 5 ({mech_label}) — mini summary not found:\n"
                  f"     {mini_path}")
            continue
        df_mini = pd.read_csv(mini_path)
        df_mini["eps_num"] = to_eps_num(df_mini["epsilon"])

        # Filter mini to selected epsilon only
        df_mini_eps = df_mini[
            (df_mini["eps_num"] - selected_eps).abs() < 1e-9
        ].copy()
        if df_mini_eps.empty:
            print(f"  ⚠  Skipping plot 5 ({mech_label}) — ε={selected_eps} not found "
                  f"in mini summary.")
            continue

        # Full summary: only one epsilon, but confirm it matches
        df_full = df_full.copy()
        df_full["eps_num"] = to_eps_num(df_full["epsilon"])

        # ── Build one figure per mechanism ────────────────────────────────────
        # Group queries by metric type so strips are logically grouped
        metric_types_present = [
            mt for mt in METRIC_CONFIG
            if not df_full[df_full["metric_type"] == mt].empty
        ]
        if not metric_types_present:
            continue

        # Collect all queries in order
        all_queries = sorted(df_full["query_num"].unique())
        n_queries   = len(all_queries)

        fig, axes = plt.subplots(
            n_queries, 1,
            figsize=(11, max(3 * n_queries, 6)),
            sharex=False,
            squeeze=False,
        )
        mech_title = mech_label.capitalize()
        fig.suptitle(
            f"Mini → Full Generalization Check — {mech_title}  "
            f"[{variant.upper()}  |  ε={selected_eps}]\n"
            f"Hollow = mini value   Filled = full value   "
            f"Red dashed = pass threshold",
            fontsize=13, weight="bold", y=1.01,
        )

        for ax, qnum in zip(axes[:, 0], all_queries):
            # Get metric type for this query
            q_full = df_full[df_full["query_num"] == qnum]
            if q_full.empty:
                ax.set_visible(False)
                continue

            mtype = str(q_full["metric_type"].iloc[0])
            cfg   = METRIC_CONFIG.get(mtype)
            if cfg is None:
                ax.set_visible(False)
                continue

            col    = cfg["primary_col"]
            fb     = cfg.get("fallback_col")
            thresh = cfg["threshold"]
            lower  = cfg["lower_is_better"]

            # Resolve actual column
            actual_col = (col if col in q_full.columns
                          else (fb if fb and fb in q_full.columns else None))
            if actual_col is None:
                ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
                continue

            # Full value
            full_val = pd.to_numeric(q_full[actual_col].iloc[0], errors="coerce")

            # Mini value at selected epsilon
            q_mini = df_mini_eps[df_mini_eps["query_num"] == qnum]
            mini_actual_col = (col if col in q_mini.columns
                               else (fb if fb and fb in q_mini.columns else None))
            mini_val = (pd.to_numeric(q_mini[mini_actual_col].iloc[0], errors="coerce")
                        if mini_actual_col and not q_mini.empty else np.nan)

            # Determine pass/fail
            def passes(v):
                if pd.isna(v):
                    return False
                return float(v) <= thresh if lower else float(v) >= thresh

            # Plot as horizontal dot + line pair so alignment is clear
            x_positions = [0, 1]   # 0 = mini, 1 = full
            values      = [mini_val, full_val]
            fills       = [
                "white" if not passes(mini_val) else color,   # hollow if fail
                color   if passes(full_val)     else "white",
            ]

            for x, val, fill in zip(x_positions, values, fills):
                if pd.isna(val):
                    continue
                ax.scatter(
                    x, float(val),
                    s=120, zorder=5,
                    facecolors=fill, edgecolors=color,
                    marker=marker, linewidths=1.8,
                )

            # Connect mini and full with a thin line to show the gap
            if not pd.isna(mini_val) and not pd.isna(full_val):
                ax.plot([0, 1], [float(mini_val), float(full_val)],
                        color=color, linewidth=1.0, alpha=0.5, zorder=3)

            # Threshold line
            ax.axhline(y=thresh, color="red", linestyle="--",
                       linewidth=1.2, alpha=0.6)

            # Annotate values
            for x, val in zip(x_positions, values):
                if pd.isna(val):
                    continue
                ax.annotate(
                    f"{float(val):.3f}",
                    xy=(x, float(val)),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=8, color=color, va="center",
                )

            ax.set_ylabel(f"Q{qnum:02d} [{mtype}]\n{cfg['y_label']}", fontsize=9)
            ax.set_xticks([0, 1])
            ax.set_xticklabels(["Mini", "Full"], fontsize=10)
            ax.set_xlim(-0.4, 1.8)
            ax.grid(True, alpha=0.25, axis="y")
            ax.tick_params(axis="y", labelsize=8)

            # Shade passing region
            y_min, y_max = ax.get_ylim()
            if lower:
                ax.axhspan(y_min, thresh, alpha=0.04, color="green", zorder=0)
            else:
                ax.axhspan(thresh, y_max, alpha=0.04, color="green", zorder=0)

        # Legend
        legend_elements = [
            plt.Line2D([0], [0], color=color, marker=marker,
                       markerfacecolor="white", markersize=8,
                       label=f"{mech_title} — Mini (hollow=fail, filled=pass)"),
            plt.Line2D([0], [0], color=color, marker=marker,
                       markerfacecolor=color, markersize=8,
                       label=f"{mech_title} — Full (hollow=fail, filled=pass)"),
            plt.Line2D([0], [0], color="red", linestyle="--",
                       label=f"Pass threshold"),
        ]
        axes[0, 0].legend(handles=legend_elements, fontsize=8,
                           loc="upper right", framealpha=0.9)

        plt.tight_layout()
        out = os.path.join(fig_dir, f"05_mini_vs_full_{mech_label}.png")
        plt.savefig(out, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"  ✓ {out}")


# =============================================================================
#  PLOT 6: Pareto Frontier
#  Scatter: privacy gain (1/ε) on x-axis vs utility loss on y-axis.
#  Each dot is one (query, epsilon) point. Shows the fundamental trade-off.
# =============================================================================

def plot_6_pareto(gauss_df, laplace_df, database, fig_dir):
    fig, ax = plt.subplots(figsize=(11, 7))

    for mech, df, color, marker in [
        ("Gaussian", gauss_df, MECH_COLORS["Gaussian"], MECH_MARKERS["Gaussian"]),
        ("Laplace",  laplace_df, MECH_COLORS["Laplace"], MECH_MARKERS["Laplace"]),
    ]:
        if df is None:
            continue

        xs, ys = [], []
        df = df.copy()
        df["eps_num"] = to_eps_num(df["epsilon"])
        bmask = df["eps_num"] >= EPS_INF_NUM

        for _, row in df[~bmask].iterrows():
            eps    = float(row["eps_num"])
            mtype  = str(row["metric_type"])
            cfg    = METRIC_CONFIG.get(mtype)
            if cfg is None:
                continue

            col    = cfg["primary_col"]
            fb     = cfg.get("fallback_col")
            actual_col = col if col in df.columns else (fb if fb and fb in df.columns else None)
            if actual_col is None:
                continue

            qnum      = row["query_num"]
            current   = pd.to_numeric(row[actual_col], errors="coerce")
            baseline_rows = df[bmask & (df["query_num"] == qnum)]

            if baseline_rows.empty or pd.isna(current):
                continue

            baseline = pd.to_numeric(baseline_rows[actual_col].iloc[0], errors="coerce")
            if pd.isna(baseline):
                continue

            baseline_f = float(baseline)
            current_f  = float(current)

            if abs(baseline_f) < 1e-12:
                # Absolute utility loss when baseline ~ 0
                loss = abs(current_f - baseline_f)
            else:
                if cfg["lower_is_better"]:
                    loss = (current_f - baseline_f) / abs(baseline_f)
                else:
                    loss = (baseline_f - current_f) / abs(baseline_f)

            xs.append(1.0 / eps)
            ys.append(loss)

        if xs:
            ax.scatter(xs, ys, alpha=0.55, color=color, marker=marker,
                       label=mech, s=50, zorder=3)

    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.4)
    ax.set_xlabel("Privacy Gain  (1/ε)  →  stronger privacy to the right")
    ax.set_ylabel("Utility Loss  (relative to ε=∞ baseline)")
    ax.set_title(
        f"Privacy-Utility Pareto Frontier  [{database.upper()}]\n"
        "Lower-left is better (low loss, high privacy)",
        fontsize=12
    )
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = os.path.join(fig_dir, f"06_pareto_frontier_{database}.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out}")


# =============================================================================
#  MAIN PIPELINE
# =============================================================================

def _is_single_epsilon_run(df: pd.DataFrame | None) -> bool:
    """Return True if this summary has only one finite epsilon (full-db run)."""
    if df is None:
        return False
    finite_eps = df[~make_baseline_mask(df["epsilon"])]["epsilon"].nunique()
    return finite_eps <= 1


def run_visualization(database: str = "mini", variant: str = "baseline") -> None:
    """
    Generate all figures for one (database, variant) combination.

    For mini runs (all epsilons): full privacy-utility curves + heatmaps.
    For full runs (single best epsilon): comparison bar chart, pass-rate
    summary, and best-epsilon annotated dot plot — the curve plots are
    skipped because a single point is not a curve.

    Reads summaries from  : data/dp_{mechanism}_{database}/{variant}/
    Reads best eps from   : data/evaluation_results/{variant}/best_epsilon_report_{database}.csv
      (for full runs, reads mini report to annotate which epsilon was used)
    Writes figures to     : data/evaluation_results/{variant}/figures_{database}/
    """

    eval_out = os.path.join(EVAL_DIR, variant)
    fig_dir  = os.path.join(eval_out, f"figures_{database}")
    Path(fig_dir).mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(f"VISUALIZATION  —  database={database}  variant={variant}")
    print("=" * 70)
    print(f"Figures dir : {fig_dir}")
    print("=" * 70)

    # Load summaries
    gauss_p   = summary_input_path(database, variant, "gaussian")
    laplace_p = summary_input_path(database, variant, "laplace")

    print("\n[1] Loading summaries...")
    gauss_df   = pd.read_csv(gauss_p)   if os.path.exists(gauss_p)   else None
    laplace_df = pd.read_csv(laplace_p) if os.path.exists(laplace_p) else None

    if gauss_df is not None:
        print(f"  ✓ Gaussian : {len(gauss_df)} rows")
    else:
        print(f"  ⚠  Gaussian not found : {gauss_p}")

    if laplace_df is not None:
        print(f"  ✓ Laplace  : {len(laplace_df)} rows")
    else:
        print(f"  ⚠  Laplace not found  : {laplace_p}")

    if gauss_df is None and laplace_df is None:
        print("  ✗ No data found — run DP mechanism files first.")
        return

    # Detect whether this is a single-epsilon (full) run or all-epsilon (mini) run
    single_eps_run = _is_single_epsilon_run(gauss_df if gauss_df is not None else laplace_df)
    mode_label = "single-epsilon (full)" if single_eps_run else "all-epsilon (mini)"
    print(f"  ℹ  Mode detected: {mode_label}")

    # Load best epsilon report.
    # Primary: compute from per_query_comparison (has metric_type per query).
    # Fallback: load pre-saved best_epsilon_report CSV if comparison not found.
    print("\n[2] Computing / loading best epsilon...")
    cmp_path = comparison_input_path(database, variant)
    if os.path.exists(cmp_path):
        cmp_df      = pd.read_csv(cmp_path)
        best_eps_df = compute_best_eps_from_comparison(cmp_df)
        if not best_eps_df.empty:
            print(f"  ✓ Best epsilon computed from per_query_comparison "
                  f"({len(best_eps_df)} rows — one per query/mechanism)")
        else:
            print("  ⚠  per_query_comparison found but produced no scored rows.")
            best_eps_df = None
    else:
        print(f"  ℹ  per_query_comparison not found at: {cmp_path}")
        print("  ℹ  Falling back to pre-saved best_epsilon_report (if available)...")
        if single_eps_run:
            best_eps_df = load_best_eps(os.path.join(EVAL_DIR, variant), "mini")
        else:
            best_eps_df = load_best_eps(eval_out, database)
        if best_eps_df is not None:
            print(f"  ✓ Loaded best_epsilon_report ({len(best_eps_df)} rows)")
        else:
            print("  ℹ  No best epsilon data — run evaluate_dp_results.py first.")

    # Generate plots — set depends on whether this is mini or full
    print("\n[3] Generating figures...")

    if not single_eps_run:
        print("\n  Plot 1: Privacy-utility tradeoff curves")
        plot_1_privacy_utility_curves(gauss_df, laplace_df, best_eps_df, database, fig_dir)

    if not single_eps_run:
        print("\n  Plot 2: Per-query heatmaps")
        plot_2_heatmaps(gauss_df, laplace_df, database, fig_dir)

    print("\n  Plot 3: Mechanism comparison bar charts")
    plot_3_mechanism_comparison(gauss_df, laplace_df, database, fig_dir)

    if not single_eps_run:
        print("\n  Plot 4: Overall pass-rate curve")
        plot_4_pass_rate(gauss_df, laplace_df, database, fig_dir)

    print("\n  Plot 5: Best epsilon summary / generalization check")
    if single_eps_run:
        # Full run: compare mini vs full at the selected epsilon per query
        plot_5_mini_vs_full(gauss_df, laplace_df, variant, fig_dir)
    else:
        # Mini run: dot plot showing selected best epsilon per query
        plot_5_best_epsilon_summary(best_eps_df, database, fig_dir)

    print("\n  Plot 6: Pareto frontier scatter")
    plot_6_pareto(gauss_df, laplace_df, database, fig_dir)

    print("\n" + "=" * 70)
    print("VISUALIZATION COMPLETE")
    print("=" * 70)
    print(f"All figures saved to: {fig_dir}")
    print("=" * 70)


# =============================================================================
#  ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate privacy-utility tradeoff plots (supports baseline and advance variants)."
    )
    parser.add_argument(
        "--database", choices=["mini", "full"], default="mini",
        help="Which database results to visualize."
    )
    parser.add_argument(
        "--variant", choices=["baseline", "advance"], default="baseline",
        help="Which mechanism variant to visualize (baseline or advance)."
    )
    args = parser.parse_args()
    run_visualization(database=args.database, variant=args.variant)
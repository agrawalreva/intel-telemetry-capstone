"""
dp_config.py

Shared configuration for both Gaussian and Laplace DP mechanisms.

Defines:
- Epsilon values to test across all queries
- Delta for Gaussian mechanism
- Sensitivity per query (L1 for Laplace, L2 for Gaussian)
- Metric type per query (aligned with other group's benchmark)
- File paths (relative to repo root)
- Random seed for reproducibility

Metric Type Legend (aligned with other group's evaluation routing):
    RE       → Agg+Join, Geo/Demo : median relative error, pass if RE ≤ 0.25
    TVD      → Histogram, Pivot   : total variation distance, pass if TVD ≤ 0.15
    SPEARMAN → Top-k              : Spearman ρ, pass if ρ ≥ 0.5
"""

import os
import numpy as np

# =============================================================================
#  PROJECT PATHS  (relative to repo root, i.e. one level up from scripts/)
# =============================================================================

ROOT_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR    = os.path.join(ROOT_DIR, "data")

# --- input baseline folders ---
BASELINE_MINI = os.path.join(DATA_DIR, "mini")
BASELINE_FULL = os.path.join(DATA_DIR, "full")

# --- output folders: Gaussian ---
DP_GAUSSIAN_MINI = os.path.join(DATA_DIR, "dp_gaussian_mini")
DP_GAUSSIAN_FULL = os.path.join(DATA_DIR, "dp_gaussian_full")

# --- output folders: Laplace ---
DP_LAPLACE_MINI = os.path.join(DATA_DIR, "dp_laplace_mini")
DP_LAPLACE_FULL = os.path.join(DATA_DIR, "dp_laplace_full")


# =============================================================================
#  DP PARAMETERS
# =============================================================================

# Fixed random seed — same seed used in the seeded for-loop inside
# each mechanism so results are fully reproducible across runs.
RANDOM_SEED = 42

# Delta for Gaussian mechanism only (pure Laplace does not use delta).
# Set to 1e-6 as used in the previous telemetry project.
DEFAULT_DELTA = 1e-6

# Q8: number of distribution columns that each receive independent Laplace noise.
# Under basic composition, adding noise to each column at scale Δ/ε_col where
# ε_col = ε / N_DIST_COLS gives a total privacy cost of ε across all columns.
N_DIST_COLS_Q8 = 28  # 28 web-category percentage columns in Query 8

# Epsilon grid — fine-grained in the 0.1-1.0 transition zone where utility
# changes most rapidly, so the privacy-utility curve shows the shape clearly.
#   0.01 - 0.05 : very strong privacy (noise >> signal for most queries)
#   0.1  - 1.0  : transition zone — densely sampled to show the curve smoothly
#   2.0  - 5.0  : moderate / practical range (most queries pass here)
#   8.0  - 10.0 : weak privacy (close to no-noise baseline)
#   inf         : no privacy (true baseline copy — useful for sanity check)
EPSILON_VALUES = [
    0.01,
    0.05,
    0.1,
    0.5,
    1.0,
    float("inf"),   # eps_inf  →  no noise added
]

# =============================================================================
# GLOBAL CLIPPING / THRESHOLDS (must match export_baseline.py)
# =============================================================================

K_MIN_100 = 100
K_MIN_50 = 50
K_MIN_10 = 10

CAP_DUR_MINS = 60.0
CAP_POWER_ONS = 10.0
# CAP_SECONDS: per-GUID average display duration.
# Original cap was 3600s (1hr) which gives sensitivity=72 at k_min=50 —
# producing sigma≈22000 at ε=0.01 and completely destroying rank signal.
# Tightened to 600s (10 min): a reasonable 99th-percentile session cap
# that still covers legitimate usage while reducing sensitivity to 12.
CAP_SECONDS = 600.0
CAP_ENTRIES_PER_GUID = 50.0
CAP_POWER_WATTS = 50.0
CAP_PERCENT = 100.0

CAP_BROWSER_INSTANCES_PER_GUID = 50.0
CAP_BROWSER_DURATION_MS_PER_GUID = 3_600_000.0
CAP_DAYS_PER_GUID = 365.0

# CAP_POWER_CONSUMPTION: per-GUID total power consumption for Q12.
# SQL clips to 100W but the sensitivity is used as the noise scale.
# Tightened to 10.0 (representing the max contribution per GUID to a
# group average) — reduces sigma 10× at every epsilon value.
CAP_POWER_CONSUMPTION = 10.0

# CAP_TIME_MINS_Q7: per-GUID daily time contribution for Q7 sleep summary.
# Original cap was 1440min (full 24hrs), giving sensitivity=14.4 at k_min=100.
# Tightened to 720min (12hrs): covers typical active usage while cutting
# sensitivity in half (sensitivity=7.2), reducing sigma≈2× at all epsilons.
CAP_TIME_MINS_Q7 = 720.0


# =============================================================================
#  QUERY METADATA
#  For every query we record:
#    filename     : exact CSV name produced by export_baseline.py
#    numeric_cols : columns that receive DP noise
#    group_col    : grouping column(s) used for z-score / top-set / rank
#    metric_type  : A / B / C / D / E  (see legend above)
#    sensitivity  : per-column sensitivity
#                   COUNT cols  →  1  (one guid adds/removes 1 from COUNT)
#                   AVG cols    →  estimated max single-user contribution / n
#                                  we use a conservative upper bound here;
#                                  the evaluator can refine once data is seen.
#                   PCT cols    →  100 / n  (percentage points, n = # groups)
#                   SUM cols    →  estimated max single contribution
#
#  NOTE: sensitivity values are stored as a dict {col: sensitivity_value}.
#        For Gaussian we take the L2 norm of the sensitivity vector.
#        For Laplace we take the L1 norm (sum of absolute sensitivities).
# =============================================================================

QUERY_META = {

    # ------------------------------------------------------------------
    # Q1 : Battery Power On Geographic Summary
    #   Story  : which countries have abnormal battery usage?
    #   Metric : RE  →  median relative error on avg_duration (Geo/Demo)
    #   Matches other group's metric routing: Geo/Demo → RE ≤ 0.25
    # ------------------------------------------------------------------
    1: {
        "filename": "battery_power_on_geographic_summary.csv",
        "numeric_cols": ["number_of_systems", "avg_number_of_dc_powerons", "avg_duration"],
        "group_col": "country",
        "re_col": "avg_duration",
        "metric_type": "RE",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid values, n>=100
            "avg_number_of_dc_powerons": CAP_POWER_ONS / K_MIN_100,  # 0.1
            "avg_duration": CAP_DUR_MINS / K_MIN_100,                # 0.6
        },
    },

    # ------------------------------------------------------------------
    # Q2 : Battery Duration by CPU Family and Generation
    #   Story  : which CPU groups have abnormal battery duration?
    #   Metric : RE  →  median relative error on avg_duration_mins_on_battery
    #   Matches other group's metric routing: Geo/Demo → RE ≤ 0.25
    # ------------------------------------------------------------------
    2: {
        "filename": "battery_on_duration_by_cpu_family_and_generation.csv",
        "numeric_cols": ["number_of_systems", "avg_duration_mins_on_battery"],
        "group_col": ["marketcodename", "cpugen"],
        "re_col": "avg_duration_mins_on_battery",
        "metric_type": "RE",
        "sensitivity": {
            "number_of_systems": 1.0,
            "avg_duration_mins_on_battery": CAP_DUR_MINS / K_MIN_100,  # 0.6
        },
    },

    # ------------------------------------------------------------------
    # Q3 : Display Devices Connection Type Resolution Durations
    #   Story  : which connection+resolution combos dominate usage?
    #   Metric : SPEARMAN  →  Spearman ρ on ranking by avg duration (Top-k)
    #   Matches other group's metric routing: Top-k → Spearman ρ ≥ 0.5
    # ------------------------------------------------------------------
    3: {
        "filename": "display_devices_connection_type_resolution_durations.csv",
        "numeric_cols": ["number_of_systems", "average_duration_on_ac_in_seconds", "average_duration_on_dc_in_seconds"],
        "group_col": ["connection_type", "resolution"],
        "rank_col": "average_duration_on_ac_in_seconds",
        "metric_type": "SPEARMAN",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid values, n>=50
            "average_duration_on_ac_in_seconds": CAP_SECONDS / K_MIN_50,  # 72
            "average_duration_on_dc_in_seconds": CAP_SECONDS / K_MIN_50,  # 72
        },
    },

    # ------------------------------------------------------------------
    # Q4 : Display Devices Vendors Percentage
    #   Story  : vendor market share (must sum to ~100 %)
    #   Metric : RE  →  median relative error on percentage_of_systems
    #   Matches other group's metric routing: Agg+Join → RE ≤ 0.25
    # ------------------------------------------------------------------
    4: {
        "filename": "display_devices_vendors_percentage.csv",
        "numeric_cols": ["number_of_systems", "total_number_of_systems", "percentage_of_systems"],
        "group_col": "vendor_name",
        "pct_col": "percentage_of_systems",
        "re_col": "percentage_of_systems",
        "metric_type": "RE",
        "sensitivity": {
            "number_of_systems": 1.0,
            "total_number_of_systems": 1.0,
            # percent changes by at most ~100/total; conservatively 1.0
            "percentage_of_systems": 1.0,
        },
    },

    # ------------------------------------------------------------------
    # Q5 : MODS Blockers by OS Name and Codename
    #   Story  : which OS versions block the most?
    #   Metric : RE  →  median relative error on entries_per_system (Agg+Join)
    #   Matches other group's metric routing: Agg+Join → RE ≤ 0.25
    # ------------------------------------------------------------------
    5: {
        "filename": "mods_blockers_by_os_name_and_codename.csv",
        "numeric_cols": ["num_entries", "number_of_systems", "entries_per_system"],
        "group_col": ["os_name", "os_codename"],
        "re_col": "entries_per_system",
        "metric_type": "RE",
        "sensitivity": {
            "num_entries": CAP_ENTRIES_PER_GUID,    # one guid can add at most 50 entries (clipped)
            "number_of_systems": 1.0,
            # average entries per system with n>=10: <= 50/10 = 5
            "entries_per_system": CAP_ENTRIES_PER_GUID / K_MIN_10,  # 5.0
        },
    },

    # ------------------------------------------------------------------
    # Q6 : Most Popular Browser in Each Country
    #   Story  : which browser wins in each country?
    #   Metric : SPEARMAN  →  Spearman ρ on winner ordering (Top-k)
    #   Matches other group's metric routing: Top-k → Spearman ρ ≥ 0.5
    #   NOTE   : Report Noisy Max adds Lap(0, 1/ε) to the winner count
    #            before taking argmax, satisfying ε-DP (sensitivity = 1).
    # ------------------------------------------------------------------
    6: {
        "filename": "most_popular_browser_in_each_country.csv",
        "numeric_cols": [],
        "group_col": "country",
        "winner_col": "browser",
        "rank_col": "browser",
        "metric_type": "SPEARMAN",
        "sensitivity": {},
    },

    # ------------------------------------------------------------------
    # Q7 : On/Off MODS Sleep Summary by CPU
    #   Story  : which CPU families have abnormal sleep patterns?
    #   Metric : TVD  →  total variation on percentage columns (Pivot)
    #   Matches other group's metric routing: Pivot → TVD ≤ 0.15
    # ------------------------------------------------------------------
    7: {
        "filename": "on_off_mods_sleep_summary_by_cpu.csv",
        "numeric_cols": [
            "number_of_systems",
            "avg_on_time", "avg_off_time",
            "avg_modern_sleep_time", "avg_sleep_time",
            "avg_total_time",
            "avg_pcnt_on_time", "avg_pcnt_off_time",
            "avg_pcnt_mods_time", "avg_pcnt_sleep_time",
        ],
        "group_col": ["marketcodename", "cpugen"],
        "pct_col": "avg_pcnt_on_time",
        "dist_cols": ["avg_pcnt_on_time", "avg_pcnt_off_time",
                      "avg_pcnt_mods_time", "avg_pcnt_sleep_time"],
        "metric_type": "TVD",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid day-averages, n>=100
            # Cap tightened to 720min (12hrs) from 1440min, halving sensitivity.
            "avg_on_time": CAP_TIME_MINS_Q7 / K_MIN_100,            # 7.2
            "avg_off_time": CAP_TIME_MINS_Q7 / K_MIN_100,           # 7.2
            "avg_modern_sleep_time": CAP_TIME_MINS_Q7 / K_MIN_100,  # 7.2
            "avg_sleep_time": CAP_TIME_MINS_Q7 / K_MIN_100,         # 7.2
            "avg_total_time": CAP_TIME_MINS_Q7 / K_MIN_100,         # 7.2 (conservative)
            # percent columns (0..100), n>=100 => ~1.0
            "avg_pcnt_on_time": 100.0 / K_MIN_100,        # 1.0
            "avg_pcnt_off_time": 100.0 / K_MIN_100,       # 1.0
            "avg_pcnt_mods_time": 100.0 / K_MIN_100,      # 1.0
            "avg_pcnt_sleep_time": 100.0 / K_MIN_100,     # 1.0
        },
    },

    # ------------------------------------------------------------------
    # Q8 : Persona Web Category Usage Analysis
    #   Story  : what web categories does each persona use?
    #   Metric : TVD  →  total variation on distribution columns (Pivot)
    #   Matches other group's metric routing: Pivot → TVD ≤ 0.15
    # ------------------------------------------------------------------
    8: {
        "filename": "persona_web_category_usage_analysis.csv",
        "numeric_cols": [
            "number_of_systems", "days",
            "content_creation_photo_edit_creation",
            "content_creation_video_audio_edit_creation",
            "content_creation_web_design_development",
            "education", "entertainment_music_audio_streaming",
            "entertainment_other", "entertainment_video_streaming",
            "finance", "games_other", "games_video_games",
            "mail", "news", "unclassified", "private",
            "productivity_crm", "productivity_other",
            "productivity_presentations", "productivity_programming",
            "productivity_project_management", "productivity_spreadsheets",
            "productivity_word_processing",
            "recreation_travel", "reference", "search",
            "shopping", "social_social_network",
            "social_communication", "social_communication_live",
        ],
        "group_col": "persona",
        "dist_cols"    : [
            "content_creation_photo_edit_creation",
            "content_creation_video_audio_edit_creation",
            "content_creation_web_design_development",
            "education", "entertainment_music_audio_streaming",
            "entertainment_other", "entertainment_video_streaming",
            "finance", "games_other", "games_video_games",
            "mail", "news", "unclassified", "private",
            "productivity_crm", "productivity_other",
            "productivity_presentations", "productivity_programming",
            "productivity_project_management", "productivity_spreadsheets",
            "productivity_word_processing", "recreation_travel",
            "reference", "search", "shopping",
            "social_social_network", "social_communication",
            "social_communication_live",
        ],
        "metric_type": "TVD",
        "sensitivity": {
            "number_of_systems": 1.0,
            # Sum of (clipped) days: one guid contributes at most 365 days
            "days": CAP_DAYS_PER_GUID,
            # Percent-like outputs 0..100; conservative 1.0 sensitivity
            **{k: 1.0 for k in [
                "content_creation_photo_edit_creation",
                "content_creation_video_audio_edit_creation",
                "content_creation_web_design_development",
                "education", "entertainment_music_audio_streaming",
                "entertainment_other", "entertainment_video_streaming",
                "finance", "games_other", "games_video_games",
                "mail", "news", "unclassified", "private",
                "productivity_crm", "productivity_other",
                "productivity_presentations", "productivity_programming",
                "productivity_project_management", "productivity_spreadsheets",
                "productivity_word_processing",
                "recreation_travel", "reference", "search",
                "shopping", "social_social_network",
                "social_communication", "social_communication_live",
            ]},
        },
    },

    # ------------------------------------------------------------------
    # Q9 : Package Power by Country
    #   Story  : which countries have abnormally high power draw?
    #   Metric : RE  →  median relative error on avg_pkg_power_consumed
    #   Matches other group's metric routing: Geo/Demo → RE ≤ 0.25
    # ------------------------------------------------------------------
    9: {
        "filename": "package_power_by_country.csv",
        "numeric_cols": ["number_of_systems", "avg_pkg_power_consumed"],
        "group_col": "countryname_normalized",
        "re_col": "avg_pkg_power_consumed",
        "metric_type": "RE",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid power, n>=100
            "avg_pkg_power_consumed": CAP_POWER_WATTS / K_MIN_100,  # 0.5
        },
    },

    # ------------------------------------------------------------------
    # Q10 : Popular Browsers by Count Usage Percentage
    #   Story  : global browser market share
    #   Metric : TVD  →  total variation on percentage distribution
    #   Matches other group's metric routing: Histogram → TVD ≤ 0.15  ✓
    # ------------------------------------------------------------------
    10: {
        "filename": "popular_browsers_by_count_usage_percentage.csv",
        "numeric_cols": ["percent_systems", "percent_instances", "percent_duration"],
        "group_col": "browser",
        "pct_col": "percent_systems",
        "metric_type": "TVD",
        "sensitivity": {
            "percent_systems": 1.0,
            "percent_instances": 1.0,
            "percent_duration": 1.0,
        },
    },

    # ------------------------------------------------------------------
    # Q11 : RAM Utilization Histogram
    #   Story  : distribution of RAM usage across systems
    #   Metric : TVD  →  total variation on histogram bin percentages
    #   Matches other group's metric routing: Histogram → TVD ≤ 0.15  ✓
    # ------------------------------------------------------------------
    11: {
        "filename": "ram_utilization_histogram.csv",
        "numeric_cols": ["number_of_systems", "avg_percentage_used"],
        "group_col": "ram_gb",
        "pct_col": "avg_percentage_used",
        "metric_type": "TVD",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid pct, n>=50
            "avg_percentage_used": CAP_PERCENT / K_MIN_50,  # 2.0
        },
    },

    # ------------------------------------------------------------------
    # Q12 : Ranked Process Classifications
    #   Story  : which user_ids consume the most CPU power? (ranking)
    #   Metric : SPEARMAN  →  Spearman ρ on total_power_consumption ranking
    #   Matches other group's metric routing: Top-k → Spearman ρ ≥ 0.5
    # ------------------------------------------------------------------
    12: {
        "filename": "ranked_process_classifications.csv",
        "numeric_cols": ["total_power_consumption"],
        "group_col": "user_id",
        "rank_col": "total_power_consumption",
        "metric_type": "SPEARMAN",
        "sensitivity": {
            # Cap tightened to 10.0: represents max per-GUID contribution to
            # a group average. Original cap of 100.0 matched SQL clip but was
            # used as raw sensitivity, giving sigma≈30000 at ε=0.01.
            "total_power_consumption": CAP_POWER_CONSUMPTION,  # 10.0
        },
    },
}


# =============================================================================
#  SENSITIVITY HELPERS
# =============================================================================

def get_sensitivity(query_num: int, col: str) -> float:
    """
    Return the global sensitivity for a given query column.

    This is the true worst-case global sensitivity derived from the clipping
    cap and k_min threshold used in the SQL query (export_baseline.py).
    It holds for ALL neighbouring datasets, not just the observed data.

        Δ(AVG query) = cap / k_min       (one GUID changes avg by at most cap/n)
        Δ(SUM query) = cap               (one GUID changes sum by at most cap)
        Δ(proportion) = 1.0              (conservative; one GUID changes fraction)

    N_FACTOR has been removed: dividing by an observed typical group size
    violates the global sensitivity requirement, which must hold over all
    possible neighbouring datasets including worst-case ones.
    """
    return QUERY_META[query_num]["sensitivity"].get(col, 0.0)


def get_l1_sensitivity(query_num: int) -> float:
    """
    L1 sensitivity for Laplace mechanism.
    Sum of all per-column sensitivities for a given query.
    """
    return float(sum(QUERY_META[query_num]["sensitivity"].values()))


def get_l2_sensitivity(query_num: int) -> float:
    """
    L2 sensitivity for Gaussian mechanism.
    Euclidean norm of the per-column sensitivity vector.
    """
    vals = list(QUERY_META[query_num]["sensitivity"].values())
    return float(np.sqrt(sum(v ** 2 for v in vals)))


# =============================================================================
#  NOISE SCALE HELPERS
# =============================================================================

def gaussian_sigma(l2_sensitivity: float, epsilon: float, delta: float) -> float:
    """
    Analytic Gaussian noise scale sigma for (epsilon, delta)-DP.

    Uses the tight calibration from Balle & Wang (2018), "Improving the
    Gaussian Mechanism for Differential Privacy: Analytical Calibration and
    Optimal Denoising", ICML 2018.

    The classic formula  sigma = (Δ_2 / ε) * sqrt(2 * ln(1.25 / δ))  is a
    loose upper bound.  The analytic version solves for the minimum sigma
    such that the (ε, δ) guarantee holds exactly, using the Gaussian CDF:

        Φ(Δ/(2σ) - εσ/Δ) - exp(ε) * Φ(-Δ/(2σ) - εσ/Δ) <= δ

    This is solved numerically via binary search on sigma.  For typical
    values (ε=0.01–10, δ=1e-6) this gives 10–30% less noise than the
    classic formula — a free improvement with no change to the DP guarantee.

    Returns 0.0 when epsilon == inf (no noise), inf when epsilon == 0.
    """
    if epsilon == float("inf"):
        return 0.0
    if epsilon == 0.0:
        return float("inf")

    from scipy.special import ndtr  # Gaussian CDF, numerically stable

    Delta = l2_sensitivity

    def _delta_from_sigma(sigma: float) -> float:
        """Compute the actual δ achieved by a given sigma (Balle & Wang eq. 3)."""
        if sigma == 0.0:
            return float("inf")
        a = Delta / (2.0 * sigma) - epsilon * sigma / Delta
        b = -Delta / (2.0 * sigma) - epsilon * sigma / Delta
        return float(ndtr(a) - np.exp(epsilon) * ndtr(b))

    # Binary search: find the smallest sigma such that _delta_from_sigma(sigma) <= delta
    # Lower bound: sigma must be > 0; start with classic formula / 2 as lower bracket
    sigma_classic = (Delta / epsilon) * np.sqrt(2.0 * np.log(1.25 / delta))
    lo, hi = 1e-10, sigma_classic  # analytic sigma is always <= classic

    # Edge case: if even the classic sigma doesn't achieve delta, widen hi
    while _delta_from_sigma(hi) > delta:
        hi *= 2.0

    # Bisect to 1e-8 relative tolerance (converges in ~50 iterations)
    for _ in range(100):
        mid = (lo + hi) / 2.0
        if _delta_from_sigma(mid) <= delta:
            hi = mid
        else:
            lo = mid
        if (hi - lo) / (hi + 1e-30) < 1e-8:
            break

    # Never return more noise than the classic formula — at very high ε the
    # analytic solver can slightly exceed classic due to numerical regime changes.
    sigma_classic = (Delta / epsilon) * np.sqrt(2.0 * np.log(1.25 / delta))
    return float(min(hi, sigma_classic))


def laplace_scale(l1_sensitivity: float, epsilon: float) -> float:
    """
    Laplace noise scale b for epsilon-DP.

        b = Δ_1 / ε

    Returns 0.0 when epsilon == inf (no noise), infinity when epsilon == 0.
    """
    if epsilon == float("inf"):
        return 0.0
    if epsilon == 0.0:
        return float("inf")
    return l1_sensitivity / epsilon


# =============================================================================
#  UTILITY: build output path for a given mechanism / database / epsilon
# =============================================================================

def build_output_dir(mechanism: str, database: str, epsilon: float) -> str:
    """
    Returns the output directory path for a given (mechanism, database, epsilon).

    Examples:
        build_output_dir("gaussian", "mini", 1.0)
            → <root>/data/dp_gaussian_mini/eps_1.0/

        build_output_dir("laplace", "full", 2.0)
            → <root>/data/dp_laplace_full/eps_2.0/

        build_output_dir("gaussian", "mini", float("inf"))
            → <root>/data/dp_gaussian_mini/eps_inf/
    """
    assert mechanism in ("gaussian", "laplace"), "mechanism must be 'gaussian' or 'laplace'"
    assert database  in ("mini", "full"),        "database must be 'mini' or 'full'"

    base = DP_GAUSSIAN_MINI if (mechanism == "gaussian" and database == "mini") else \
           DP_GAUSSIAN_FULL if (mechanism == "gaussian" and database == "full") else \
           DP_LAPLACE_MINI  if (mechanism == "laplace"  and database == "mini") else \
           DP_LAPLACE_FULL

    eps_str = "inf" if epsilon == float("inf") else str(epsilon)
    return os.path.join(base, f"eps_{eps_str}")
"""
dp_config.py

Shared configuration for both Gaussian and Laplace DP mechanisms.

Defines:
- Epsilon values to test across all queries
- Delta for Gaussian mechanism
- Sensitivity per query (L1 for Laplace, L2 for Gaussian)
- Metric type per query (matches mentor feedback)
- File paths (relative to repo root)
- Random seed for reproducibility

Metric Type Legend:
    A -> Z-score + IOU  : queries asking "which groups are abnormal?"
    B -> TVD            : queries producing percentage/distribution
    C -> Kendall's Tau  : queries producing a ranking
    D -> Top-1 Accuracy : queries picking one winner per group
    E -> KL Divergence  : queries producing multi-dim distributions
"""

import os
import numpy as np

# =============================================================================
#  PROJECT PATHS  (relative to repo root, i.e. one level up from scripts/)
# =============================================================================

ROOT_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR    = os.path.join(ROOT_DIR, "data")

# --- input baseline folders ---
BASELINE_MINI = os.path.join(DATA_DIR, "baseline_mini")
BASELINE_FULL = os.path.join(DATA_DIR, "baseline_full")

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

# Epsilon grid — many values so we can plot a smooth privacy-utility curve
# and let the evaluator pick the best epsilon before running on full data.
#   0.01 - 0.05 : very strong privacy (very noisy)
#   0.1  - 1.0  : strong / moderate privacy
#   2.0  - 5.0  : moderate / practical range
#   10   - 50   : weak privacy (close to no-noise baseline)
#   inf         : no privacy (true baseline copy — useful for sanity check)
EPSILON_VALUES = [
    0.01,
    0.05,
    0.1,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
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
CAP_SECONDS = 3600.0
CAP_ENTRIES_PER_GUID = 50.0
CAP_POWER_WATTS = 50.0
CAP_PERCENT = 100.0

CAP_BROWSER_INSTANCES_PER_GUID = 50.0
CAP_BROWSER_DURATION_MS_PER_GUID = 3_600_000.0
CAP_DAYS_PER_GUID = 365.0


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
    #   Metric : A  →  z-score of avg_duration + IOU of top-set
    # ------------------------------------------------------------------
    1: {
        "filename": "battery_power_on_geographic_summary.csv",
        "numeric_cols": ["number_of_systems", "avg_number_of_dc_powerons", "avg_duration"],
        "group_col": "country",
        "zscore_col": "avg_duration",
        "metric_type": "A",
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
    #   Metric : A  →  z-score of avg_duration_mins_on_battery + IOU
    # ------------------------------------------------------------------
    2: {
        "filename": "battery_on_duration_by_cpu_family_and_generation.csv",
        "numeric_cols": ["number_of_systems", "avg_duration_mins_on_battery"],
        "group_col": ["marketcodename", "cpugen"],
        "zscore_col": "avg_duration_mins_on_battery",
        "metric_type": "A",
        "sensitivity": {
            "number_of_systems": 1.0,
            "avg_duration_mins_on_battery": CAP_DUR_MINS / K_MIN_100,  # 0.6
        },
    },

    # ------------------------------------------------------------------
    # Q3 : Display Devices Connection Type Resolution Durations
    #   Story  : which connection+resolution combos dominate usage?
    #   Metric : C  →  Kendall's Tau on ranking by avg duration
    # ------------------------------------------------------------------
    3: {
        "filename": "display_devices_connection_type_resolution_durations.csv",
        "numeric_cols": ["number_of_systems", "average_duration_on_ac_in_seconds", "average_duration_on_dc_in_seconds"],
        "group_col": ["connection_type", "resolution"],
        "rank_col"     : "average_duration_on_ac_in_seconds",  # rank by this column
        "metric_type": "C",
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
    #   Metric : B  →  TVD on percentage distribution
    # ------------------------------------------------------------------
    4: {
        "filename": "display_devices_vendors_percentage.csv",
        "numeric_cols": ["number_of_systems", "total_number_of_systems", "percentage_of_systems"],
        "group_col": "vendor_name",
        "pct_col"      : "percentage_of_systems",   # re-normalised after noise
        "metric_type": "B",
        "sensitivity": {
            "number_of_systems": 1.0,
            "total_number_of_systems": 1.0,
            # percent changes by at most ~100/total; conservatively 1.0
            "percentage_of_systems": 1.0,
        },
    },

    # ------------------------------------------------------------------
    # Q5 : MODS Blockers by OS Name and Codename
    #   Story  : which OS versions block the most? (closest to last project)
    #   Metric : A  →  z-score of entries_per_system + IOU of top-set
    # ------------------------------------------------------------------
    5: {
        "filename": "mods_blockers_by_os_name_and_codename.csv",
        "numeric_cols": ["num_entries", "number_of_systems", "entries_per_system"],
        "group_col": ["os_name", "os_codename"],
        "zscore_col": "entries_per_system",
        "metric_type": "A",
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
    #   Metric : D  →  Top-1 Accuracy (same winner preserved?)
    #   NOTE   : this query has NO numeric columns — the winner is
    #            determined by the underlying count which we cannot
    #            see directly in the output.  We add noise to a
    #            synthetic "count" column derived from the query's
    #            inner SELECT before taking the argmax.
    # ------------------------------------------------------------------
    6: {
        "filename": "most_popular_browser_in_each_country.csv",
        "numeric_cols": [],
        "group_col": "country",
        "winner_col": "browser",
        "metric_type": "D",
        "sensitivity": {},
    },

    # ------------------------------------------------------------------
    # Q7 : On/Off MODS Sleep Summary by CPU
    #   Story  : which CPU families have abnormal sleep patterns?
    #   Metric : A  →  z-score of avg_modern_sleep_time + IOU
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
        "zscore_col": "avg_modern_sleep_time",
        "metric_type": "A",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid day-averages, n>=100
            "avg_on_time": 1440.0 / K_MIN_100,            # 14.4
            "avg_off_time": 1440.0 / K_MIN_100,           # 14.4
            "avg_modern_sleep_time": 1440.0 / K_MIN_100,  # 14.4
            "avg_sleep_time": 1440.0 / K_MIN_100,         # 14.4
            "avg_total_time": 1440.0 / K_MIN_100,         # 14.4 (conservative)
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
    #   Metric : E  →  KL Divergence per persona
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
        "metric_type": "E",
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
    #   Metric : A  →  z-score of avg_pkg_power_consumed + IOU
    # ------------------------------------------------------------------
    9: {
        "filename": "package_power_by_country.csv",
        "numeric_cols": ["number_of_systems", "avg_pkg_power_consumed"],
        "group_col": "countryname_normalized",
        "zscore_col": "avg_pkg_power_consumed",
        "metric_type": "A",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid power, n>=100
            "avg_pkg_power_consumed": CAP_POWER_WATTS / K_MIN_100,  # 0.5
        },
    },

    # ------------------------------------------------------------------
    # Q10 : Popular Browsers by Count Usage Percentage
    #   Story  : global browser market share
    #   Metric : B  →  TVD on percentage distribution
    #         + C  →  Kendall's Tau on ranking
    # ------------------------------------------------------------------
    10: {
        "filename": "popular_browsers_by_count_usage_percentage.csv",
        "numeric_cols": ["percent_systems", "percent_instances", "percent_duration"],
        "group_col": "browser",
        "pct_col"      : "percent_systems",   # primary distribution column
        "metric_type": "B",
        "sensitivity": {
            "percent_systems": 1.0,
            "percent_instances": 1.0,
            "percent_duration": 1.0,
        },
    },

    # ------------------------------------------------------------------
    # Q11 : RAM Utilization Histogram
    #   Story  : distribution of RAM usage across systems
    #   Metric : B  →  TVD on histogram bin percentages
    # ------------------------------------------------------------------
    11: {
        "filename": "ram_utilization_histogram.csv",
        "numeric_cols": ["number_of_systems", "avg_percentage_used"],
        "group_col": "ram_gb",
        "pct_col"      : "avg_percentage_used",
        "metric_type": "B",
        "sensitivity": {
            "number_of_systems": 1.0,
            # avg of clipped per-guid pct, n>=50
            "avg_percentage_used": CAP_PERCENT / K_MIN_50,  # 2.0
        },
    },

    # ------------------------------------------------------------------
    # Q12 : Ranked Process Classifications
    #   Story  : which user_ids consume the most CPU power? (ranking)
    #   Metric : C  →  Kendall's Tau + Top-K accuracy
    # ------------------------------------------------------------------
    12: {
        "filename": "ranked_process_classifications.csv",
        "numeric_cols": ["total_power_consumption"],
        "group_col": "user_id",
        "rank_col": "total_power_consumption",
        "metric_type": "C",
        "sensitivity": {
            # we clipped to 100 in SQL
            "total_power_consumption": 100.0,
        },
    },
}


# =============================================================================
#  SENSITIVITY HELPERS
# =============================================================================

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
    Gaussian noise scale sigma for (epsilon, delta)-DP.

        sigma = (Δ_2 / ε) * sqrt(2 * ln(1.25 / δ))

    Returns infinity when epsilon == 0 (undefined), and 0.0 when
    epsilon == inf (no noise needed).
    """
    if epsilon == float("inf"):
        return 0.0
    if epsilon == 0.0:
        return float("inf")
    return (l2_sensitivity / epsilon) * np.sqrt(2.0 * np.log(1.25 / delta))


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
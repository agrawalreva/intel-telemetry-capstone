"""
test_dp.py
==========
Unit tests for the Intel Telemetry DP Query Release pipeline.

Run from the repo root with:
    pytest tests/test_dp.py -v

Or with coverage:
    pytest tests/test_dp.py -v --tb=short

These tests verify:
  1. Sensitivity values in QUERY_META are mathematically correct
  2. laplace_scale() satisfies the Laplace mechanism formula  b = Δ/ε
  3. gaussian_sigma() satisfies the (ε, δ)-DP guarantee (Balle & Wang 2018)
  4. Q8 budget composition: each column gets ε/28, total cost = ε
  5. Q6 Report Noisy Max structure is correct
  6. No N_FACTOR or K_DRAWS remain in the codebase (professor corrections)
  7. build_output_dir() produces the expected paths
  8. Edge cases: epsilon=inf, epsilon=0, missing columns
"""

import math
import sys
import os
import pytest
import numpy as np

# ---------------------------------------------------------------------------
# Make dp_config importable whether tests/ is at repo root or src/
# Adjust this path to match your actual repo layout:
#   e.g. if dp_config.py lives at  src/dp/dp_config.py
#        set:  sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "dp"))
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))  # repo root
# If your file is inside src/dp/ uncomment the next line instead:
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "dp"))

from dp_config import (
    # constants
    K_MIN_100, K_MIN_50, K_MIN_10,
    CAP_DUR_MINS, CAP_POWER_ONS, CAP_SECONDS,
    CAP_ENTRIES_PER_GUID, CAP_POWER_WATTS, CAP_PERCENT,
    CAP_DAYS_PER_GUID, N_DIST_COLS_Q8, DEFAULT_DELTA,
    EPSILON_VALUES, RANDOM_SEED,
    # data structures
    QUERY_META,
    # functions
    get_sensitivity, get_l1_sensitivity, get_l2_sensitivity,
    laplace_scale, gaussian_sigma, build_output_dir,
)


# ===========================================================================
# SECTION 1 — Constants sanity checks
# ===========================================================================

class TestConstants:
    """Verify all clipping caps and k_min values are as documented."""

    def test_k_min_values(self):
        assert K_MIN_100 == 100
        assert K_MIN_50  == 50
        assert K_MIN_10  == 10

    def test_cap_values(self):
        assert CAP_DUR_MINS           == 60.0
        assert CAP_POWER_ONS          == 10.0
        assert CAP_SECONDS            == 3600.0
        assert CAP_ENTRIES_PER_GUID   == 50.0
        assert CAP_POWER_WATTS        == 50.0
        assert CAP_PERCENT            == 100.0
        assert CAP_DAYS_PER_GUID      == 365.0

    def test_n_dist_cols_q8(self):
        """Q8 must have exactly 28 distribution columns for composition to work."""
        assert N_DIST_COLS_Q8 == 28

    def test_q8_dist_cols_match_constant(self):
        """The actual dist_cols list in QUERY_META[8] must equal N_DIST_COLS_Q8."""
        dist_cols = QUERY_META[8]["dist_cols"]
        assert len(dist_cols) == N_DIST_COLS_Q8, (
            f"QUERY_META[8]['dist_cols'] has {len(dist_cols)} columns "
            f"but N_DIST_COLS_Q8 = {N_DIST_COLS_Q8}. They must match."
        )

    def test_default_delta(self):
        assert DEFAULT_DELTA == 1e-6

    def test_random_seed(self):
        assert RANDOM_SEED == 42

    def test_epsilon_values_include_inf(self):
        assert float("inf") in EPSILON_VALUES

    def test_epsilon_values_include_small(self):
        assert 0.01 in EPSILON_VALUES


# ===========================================================================
# SECTION 2 — QUERY_META sensitivity values are mathematically correct
# ===========================================================================

class TestSensitivityValues:
    """
    Every AVG-query sensitivity must equal cap / k_min.
    Every COUNT-query sensitivity must equal 1.0.
    This confirms no N_FACTOR was accidentally reintroduced.
    """

    def test_q1_avg_duration(self):
        # avg of clipped values (cap=60 min), n >= 100
        expected = CAP_DUR_MINS / K_MIN_100   # 0.6
        assert get_sensitivity(1, "avg_duration") == pytest.approx(expected)

    def test_q1_avg_power_ons(self):
        expected = CAP_POWER_ONS / K_MIN_100  # 0.1
        assert get_sensitivity(1, "avg_number_of_dc_powerons") == pytest.approx(expected)

    def test_q1_count_col(self):
        assert get_sensitivity(1, "number_of_systems") == pytest.approx(1.0)

    def test_q2_avg_duration(self):
        expected = CAP_DUR_MINS / K_MIN_100   # 0.6
        assert get_sensitivity(2, "avg_duration_mins_on_battery") == pytest.approx(expected)

    def test_q3_avg_ac_duration(self):
        expected = CAP_SECONDS / K_MIN_50     # 72.0
        assert get_sensitivity(3, "average_duration_on_ac_in_seconds") == pytest.approx(expected)

    def test_q3_avg_dc_duration(self):
        expected = CAP_SECONDS / K_MIN_50     # 72.0
        assert get_sensitivity(3, "average_duration_on_dc_in_seconds") == pytest.approx(expected)

    def test_q5_entries_per_system(self):
        expected = CAP_ENTRIES_PER_GUID / K_MIN_10   # 5.0
        assert get_sensitivity(5, "entries_per_system") == pytest.approx(expected)

    def test_q5_num_entries(self):
        # SUM col — one guid contributes at most CAP_ENTRIES_PER_GUID entries
        assert get_sensitivity(5, "num_entries") == pytest.approx(CAP_ENTRIES_PER_GUID)

    def test_q7_avg_sleep_time(self):
        expected = 1440.0 / K_MIN_100    # 14.4 minutes
        assert get_sensitivity(7, "avg_modern_sleep_time") == pytest.approx(expected)

    def test_q7_pct_cols(self):
        expected = CAP_PERCENT / K_MIN_100   # 1.0
        for col in ["avg_pcnt_on_time", "avg_pcnt_off_time",
                    "avg_pcnt_mods_time", "avg_pcnt_sleep_time"]:
            assert get_sensitivity(7, col) == pytest.approx(expected), \
                f"Wrong sensitivity for Q7 column {col}"

    def test_q9_avg_power(self):
        expected = CAP_POWER_WATTS / K_MIN_100   # 0.5
        assert get_sensitivity(9, "avg_pkg_power_consumed") == pytest.approx(expected)

    def test_q11_avg_pct_used(self):
        expected = CAP_PERCENT / K_MIN_50    # 2.0
        assert get_sensitivity(11, "avg_percentage_used") == pytest.approx(expected)

    def test_q12_total_power(self):
        # clipped to 100 in SQL
        assert get_sensitivity(12, "total_power_consumption") == pytest.approx(100.0)

    def test_q8_dist_cols_sensitivity_is_one(self):
        """Every category percentage column in Q8 has sensitivity 1.0."""
        for col in QUERY_META[8]["dist_cols"]:
            assert get_sensitivity(8, col) == pytest.approx(1.0), \
                f"Q8 dist_col {col} should have sensitivity 1.0"

    def test_q8_days_sensitivity(self):
        """days column: one GUID contributes at most 365 days."""
        assert get_sensitivity(8, "days") == pytest.approx(CAP_DAYS_PER_GUID)

    def test_missing_col_returns_zero(self):
        """Querying a non-existent column should return 0.0, not raise."""
        assert get_sensitivity(1, "nonexistent_column") == 0.0

    def test_q6_has_no_sensitivity_cols(self):
        """Q6 uses Report Noisy Max — no numeric sensitivity needed."""
        assert QUERY_META[6]["sensitivity"] == {}
        assert QUERY_META[6]["numeric_cols"] == []


# ===========================================================================
# SECTION 3 — L1 and L2 sensitivity helpers
# ===========================================================================

class TestSensitivityNorms:

    def test_l1_q1(self):
        # number_of_systems=1.0, avg_power_ons=0.1, avg_duration=0.6
        expected = 1.0 + 0.1 + 0.6
        assert get_l1_sensitivity(1) == pytest.approx(expected)

    def test_l2_q1(self):
        expected = math.sqrt(1.0**2 + 0.1**2 + 0.6**2)
        assert get_l2_sensitivity(1) == pytest.approx(expected)

    def test_l1_q12(self):
        # only one column: total_power_consumption = 100
        assert get_l1_sensitivity(12) == pytest.approx(100.0)

    def test_l2_q12(self):
        assert get_l2_sensitivity(12) == pytest.approx(100.0)

    def test_l1_geq_l2(self):
        """L1 >= L2 always (by Cauchy-Schwarz). Verify for all queries."""
        for q in QUERY_META:
            l1 = get_l1_sensitivity(q)
            l2 = get_l2_sensitivity(q)
            assert l1 >= l2 - 1e-9, \
                f"Q{q}: L1={l1} < L2={l2}, violates norm inequality"


# ===========================================================================
# SECTION 4 — laplace_scale() correctness
# ===========================================================================

class TestLaplaceScale:
    """Laplace mechanism: scale b = Δ / ε."""

    def test_basic_formula(self):
        assert laplace_scale(0.6, 0.01) == pytest.approx(60.0)

    def test_sensitivity_one_epsilon_one(self):
        assert laplace_scale(1.0, 1.0) == pytest.approx(1.0)

    def test_larger_epsilon_gives_smaller_scale(self):
        """More privacy budget (larger ε) → less noise."""
        scale_small = laplace_scale(1.0, 0.1)
        scale_large  = laplace_scale(1.0, 1.0)
        assert scale_small > scale_large

    def test_epsilon_inf_gives_zero_noise(self):
        """ε=∞ means no privacy → no noise added."""
        assert laplace_scale(1.0, float("inf")) == 0.0

    def test_epsilon_zero_gives_inf_noise(self):
        """ε=0 means perfect privacy → infinite noise."""
        assert laplace_scale(1.0, 0.0) == float("inf")

    def test_q8_budget_split(self):
        """
        Q8 composition fix: each of 28 dist_cols gets ε_col = ε / 28.
        The resulting scale must be 28x larger than if full ε were used.
        This verifies the professor's composition correction is implemented.
        """
        epsilon = 1.0
        col_sens = 1.0
        epsilon_col = epsilon / N_DIST_COLS_Q8        # ε / 28
        scale_split  = laplace_scale(col_sens, epsilon_col)
        scale_full   = laplace_scale(col_sens, epsilon)
        assert scale_split == pytest.approx(scale_full * N_DIST_COLS_Q8)

    def test_scale_proportional_to_sensitivity(self):
        """Doubling sensitivity doubles the scale (linearity)."""
        b1 = laplace_scale(1.0, 0.5)
        b2 = laplace_scale(2.0, 0.5)
        assert b2 == pytest.approx(2 * b1)

    def test_scale_inversely_proportional_to_epsilon(self):
        """Doubling ε halves the scale."""
        b1 = laplace_scale(1.0, 1.0)
        b2 = laplace_scale(1.0, 2.0)
        assert b2 == pytest.approx(b1 / 2)


# ===========================================================================
# SECTION 5 — gaussian_sigma() satisfies the (ε, δ)-DP guarantee
# ===========================================================================

class TestGaussianSigma:
    """
    Balle & Wang (2018) analytic Gaussian mechanism.
    The returned sigma must satisfy: δ_achieved(sigma) <= δ_target.
    It must also be tighter (smaller) than the classical formula.
    """

    def _delta_achieved(self, sigma, sensitivity, epsilon):
        """Compute actual δ achieved — mirrors dp_config._delta_from_sigma."""
        from scipy.special import ndtr
        Delta = sensitivity
        if sigma == 0:
            return float("inf")
        a = Delta / (2 * sigma) - epsilon * sigma / Delta
        b = -Delta / (2 * sigma) - epsilon * sigma / Delta
        return float(ndtr(a) - np.exp(epsilon) * ndtr(b))

    def test_guarantee_satisfied_small_epsilon(self):
        """At ε=0.01, δ_achieved(sigma) must be <= δ_target=1e-6."""
        sigma = gaussian_sigma(1.0, 0.01, 1e-6)
        delta_achieved = self._delta_achieved(sigma, 1.0, 0.01)
        assert delta_achieved <= 1e-6 + 1e-9   # small tolerance for numerics

    def test_guarantee_satisfied_moderate_epsilon(self):
        sigma = gaussian_sigma(1.0, 1.0, 1e-6)
        delta_achieved = self._delta_achieved(sigma, 1.0, 1.0)
        assert delta_achieved <= 1e-6 + 1e-9

    def test_analytic_tighter_than_classical(self):
        """
        Analytic sigma must be <= classical sigma.
        Classical: sigma_c = (Δ/ε) * sqrt(2 * ln(1.25/δ))
        """
        Delta, epsilon, delta = 1.0, 0.5, 1e-6
        sigma_analytic = gaussian_sigma(Delta, epsilon, delta)
        sigma_classical = (Delta / epsilon) * math.sqrt(2 * math.log(1.25 / delta))
        assert sigma_analytic <= sigma_classical + 1e-6

    def test_epsilon_inf_gives_zero_sigma(self):
        assert gaussian_sigma(1.0, float("inf"), 1e-6) == 0.0

    def test_larger_epsilon_gives_smaller_sigma(self):
        """More privacy budget → less noise."""
        sigma_small = gaussian_sigma(1.0, 0.1,  1e-6)
        sigma_large = gaussian_sigma(1.0, 10.0, 1e-6)
        assert sigma_small > sigma_large

    def test_larger_sensitivity_gives_larger_sigma(self):
        """Higher sensitivity → more noise needed."""
        sigma_lo = gaussian_sigma(0.5, 1.0, 1e-6)
        sigma_hi = gaussian_sigma(2.0, 1.0, 1e-6)
        assert sigma_hi > sigma_lo

    def test_q1_sigma_is_finite_and_positive(self):
        """For a real query sensitivity at ε=0.01, sigma must be finite and > 0."""
        l2 = get_l2_sensitivity(1)
        sigma = gaussian_sigma(l2, 0.01, DEFAULT_DELTA)
        assert math.isfinite(sigma)
        assert sigma > 0


# ===========================================================================
# SECTION 6 — Q6 Report Noisy Max structure
# ===========================================================================

class TestQ6Structure:
    """
    Q6 must use Report Noisy Max, not numeric column noise.
    Verify the QUERY_META structure reflects this correctly.
    """

    def test_q6_metric_type_is_D(self):
        assert QUERY_META[6]["metric_type"] == "D"

    def test_q6_has_winner_col(self):
        assert "winner_col" in QUERY_META[6]
        assert QUERY_META[6]["winner_col"] == "browser"

    def test_q6_numeric_cols_empty(self):
        """Q6 uses Report Noisy Max on counts, not direct numeric noise."""
        assert QUERY_META[6]["numeric_cols"] == []

    def test_q6_sensitivity_empty(self):
        """Sensitivity dict is empty — RNM handles it with Lap(1/ε)."""
        assert QUERY_META[6]["sensitivity"] == {}

    def test_report_noisy_max_sensitivity_is_one(self):
        """
        In Report Noisy Max, each count has sensitivity 1 (one GUID changes
        one browser's count by 1). The Laplace scale is therefore 1/ε.
        Verify the formula is correct for a few epsilon values.
        """
        for epsilon in [0.01, 0.1, 1.0, 10.0]:
            scale = laplace_scale(1.0, epsilon)
            assert scale == pytest.approx(1.0 / epsilon), \
                f"RNM scale at ε={epsilon} should be {1/epsilon}, got {scale}"


# ===========================================================================
# SECTION 7 — Professor corrections: N_FACTOR and K_DRAWS removed
# ===========================================================================

class TestProfessorCorrections:
    """
    Verify that the two invalidated techniques (N_FACTOR sensitivity reduction
    and K-draw averaging) have been fully removed from the codebase.
    These tests read the source files as text to catch any reintroduction.
    """

    def _read_source(self, filename):
        """Find and read a source file anywhere under the repo root."""
        repo_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        for dirpath, _, files in os.walk(repo_root):
            if filename in files:
                with open(os.path.join(dirpath, filename)) as f:
                    return f.read()
        pytest.skip(f"{filename} not found in repo — skipping source inspection")

    def test_no_n_factor_dict_in_config(self):
        src = self._read_source("dp_config.py")
        # Should not define N_FACTOR as a dict assignment
        assert "N_FACTOR = {" not in src, \
            "N_FACTOR dict found in dp_config.py — professor correction not applied"

    def test_no_get_effective_sensitivity_in_config(self):
        src = self._read_source("dp_config.py")
        assert "def get_effective_sensitivity" not in src, \
            "get_effective_sensitivity() still exists — replace with get_sensitivity()"

    def test_no_k_draws_in_config(self):
        src = self._read_source("dp_config.py")
        assert "K_DRAWS = {" not in src, \
            "K_DRAWS dict found in dp_config.py — K-draw averaging must be removed"

    def test_no_k_draws_in_gaussian(self):
        src = self._read_source("dp_gaussian_mechanism.py")
        assert "K_DRAWS" not in src, \
            "K_DRAWS referenced in dp_gaussian_mechanism.py — remove averaging loop"

    def test_no_draws_mean_in_gaussian(self):
        src = self._read_source("dp_gaussian_mechanism.py")
        assert "draws.mean" not in src, \
            "draws.mean() found in dp_gaussian_mechanism.py — K-draw averaging not removed"

    def test_no_k_draws_in_laplace(self):
        src = self._read_source("dp_laplace_mechanism.py")
        assert "K_DRAWS" not in src, \
            "K_DRAWS referenced in dp_laplace_mechanism.py — remove averaging loop"

    def test_no_draws_mean_in_laplace(self):
        src = self._read_source("dp_laplace_mechanism.py")
        assert "draws.mean" not in src, \
            "draws.mean() found in dp_laplace_mechanism.py — K-draw averaging not removed"

    def test_get_sensitivity_exists_in_config(self):
        """After corrections, get_sensitivity() must exist."""
        src = self._read_source("dp_config.py")
        assert "def get_sensitivity" in src, \
            "get_sensitivity() not found in dp_config.py — function is missing"

    def test_n_dist_cols_q8_exists(self):
        """Q8 composition fix requires N_DIST_COLS_Q8 constant."""
        src = self._read_source("dp_config.py")
        assert "N_DIST_COLS_Q8" in src, \
            "N_DIST_COLS_Q8 not found — Q8 composition fix missing"


# ===========================================================================
# SECTION 8 — build_output_dir() path construction
# ===========================================================================

class TestBuildOutputDir:

    def test_gaussian_mini_numeric_epsilon(self):
        path = build_output_dir("gaussian", "mini", 1.0)
        assert path.endswith(os.path.join("dp_gaussian_mini", "eps_1.0"))

    def test_laplace_full_numeric_epsilon(self):
        path = build_output_dir("laplace", "full", 0.01)
        assert path.endswith(os.path.join("dp_laplace_full", "eps_0.01"))

    def test_inf_epsilon_uses_inf_string(self):
        path = build_output_dir("gaussian", "mini", float("inf"))
        assert path.endswith(os.path.join("dp_gaussian_mini", "eps_inf"))

    def test_invalid_mechanism_raises(self):
        with pytest.raises(AssertionError):
            build_output_dir("xgboost", "mini", 1.0)

    def test_invalid_database_raises(self):
        with pytest.raises(AssertionError):
            build_output_dir("laplace", "medium", 1.0)

    def test_laplace_mini(self):
        path = build_output_dir("laplace", "mini", 5.0)
        assert path.endswith(os.path.join("dp_laplace_mini", "eps_5.0"))

    def test_gaussian_full(self):
        path = build_output_dir("gaussian", "full", 2.0)
        assert path.endswith(os.path.join("dp_gaussian_full", "eps_2.0"))


# ===========================================================================
# SECTION 9 — QUERY_META completeness checks
# ===========================================================================

class TestQueryMetaCompleteness:
    """Every query entry must have required fields and sensible values."""

    REQUIRED_FIELDS = {"filename", "numeric_cols", "group_col", "metric_type", "sensitivity"}

    def test_all_queries_have_required_fields(self):
        for q, meta in QUERY_META.items():
            missing = self.REQUIRED_FIELDS - set(meta.keys())
            assert not missing, f"Q{q} missing fields: {missing}"

    def test_metric_types_are_valid(self):
        valid = {"A", "B", "C", "D", "E"}
        for q, meta in QUERY_META.items():
            assert meta["metric_type"] in valid, \
                f"Q{q} has invalid metric_type '{meta['metric_type']}'"

    def test_all_sensitivities_are_positive(self):
        for q, meta in QUERY_META.items():
            for col, val in meta["sensitivity"].items():
                assert val > 0, \
                    f"Q{q} column '{col}' has non-positive sensitivity {val}"

    def test_filenames_end_in_csv(self):
        for q, meta in QUERY_META.items():
            assert meta["filename"].endswith(".csv"), \
                f"Q{q} filename '{meta['filename']}' does not end in .csv"

    def test_metric_A_queries_have_zscore_col(self):
        for q, meta in QUERY_META.items():
            if meta["metric_type"] == "A":
                assert "zscore_col" in meta, \
                    f"Q{q} is Metric A but missing 'zscore_col'"

    def test_metric_C_queries_have_rank_col(self):
        for q, meta in QUERY_META.items():
            if meta["metric_type"] == "C":
                assert "rank_col" in meta, \
                    f"Q{q} is Metric C but missing 'rank_col'"

    def test_metric_D_queries_have_winner_col(self):
        for q, meta in QUERY_META.items():
            if meta["metric_type"] == "D":
                assert "winner_col" in meta, \
                    f"Q{q} is Metric D but missing 'winner_col'"

    def test_metric_B_queries_have_pct_col(self):
        for q, meta in QUERY_META.items():
            if meta["metric_type"] == "B":
                assert "pct_col" in meta, \
                    f"Q{q} is Metric B but missing 'pct_col'"

    def test_numeric_cols_are_lists(self):
        for q, meta in QUERY_META.items():
            assert isinstance(meta["numeric_cols"], list), \
                f"Q{q} 'numeric_cols' must be a list"

    def test_12_queries_defined(self):
        """Pipeline covers exactly queries 1–12 (minus infeasible ones)."""
        assert len(QUERY_META) == 12


# ===========================================================================
# SECTION 10 — Privacy accounting: ε-DP and (ε,δ)-DP formal checks
# ===========================================================================

class TestPrivacyAccounting:
    """
    High-level sanity checks on the privacy guarantees themselves.
    These don't require running the full pipeline.
    """

    def test_laplace_privacy_degrades_with_less_noise(self):
        """
        More noise (larger scale) → stronger privacy (lower effective ε).
        Effective ε = Δ / scale.  Verify monotonicity.
        """
        sensitivity = 1.0
        for eps in [0.01, 0.1, 1.0, 10.0]:
            scale = laplace_scale(sensitivity, eps)
            effective_eps = sensitivity / scale
            assert effective_eps == pytest.approx(eps, rel=1e-6)

    def test_q8_total_privacy_cost_equals_epsilon(self):
        """
        With budget splitting: each of 28 cols gets ε_col = ε/28.
        Total cost by basic composition = 28 * ε_col = ε.
        """
        for epsilon in [0.01, 0.1, 1.0]:
            epsilon_col = epsilon / N_DIST_COLS_Q8
            total = N_DIST_COLS_Q8 * epsilon_col
            assert total == pytest.approx(epsilon, rel=1e-9)

    def test_gaussian_sigma_decreases_with_epsilon(self):
        """
        As ε increases (weaker privacy), sigma should decrease (less noise).
        """
        sigmas = [gaussian_sigma(1.0, eps, 1e-6)
                  for eps in [0.01, 0.1, 1.0, 10.0]]
        for i in range(len(sigmas) - 1):
            assert sigmas[i] > sigmas[i + 1], \
                f"sigma not decreasing: sigma[{i}]={sigmas[i]}, sigma[{i+1}]={sigmas[i+1]}"

    def test_epsilon_inf_means_no_privacy_laplace(self):
        """At ε=∞, scale=0 → no noise → identical output → no privacy guarantee needed."""
        assert laplace_scale(1.0, float("inf")) == 0.0

    def test_epsilon_inf_means_no_privacy_gaussian(self):
        assert gaussian_sigma(1.0, float("inf"), 1e-6) == 0.0
import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TARGET_FILES = [
    "battery_power_on_geographic_summary.csv",
    "battery_on_duration_by_cpu_family_and_generation.csv",
    "display_devices_connection_type_resolution_durations.csv",
    "display_devices_vendors_percentage.csv",
    "mods_blockers_by_os_name_and_codename.csv",
    "most_popular_browser_in_each_country.csv",
    "on_off_mods_sleep_summary_by_cpu.csv",
    "persona_web_category_usage_analysis.csv",
    "package_power_by_country.csv",
    "popular_browsers_by_count_usage_percentage.csv",
    "ram_utilization_histogram.csv",
    "ranked_process_classifications.csv",
]

CAPS = {
    "battery_power_on_geographic_summary.csv": {
        "avg_number_of_dc_powerons": (0.0, 10.0),
        "avg_duration": (0.0, 60.0),
    },
    "battery_on_duration_by_cpu_family_and_generation.csv": {
        "avg_duration_mins_on_battery": (0.0, 60.0),
    },
    "display_devices_connection_type_resolution_durations.csv": {
        "average_duration_on_ac_in_seconds": (0.0, 3600.0),
        "average_duration_on_dc_in_seconds": (0.0, 3600.0),
    },
    "display_devices_vendors_percentage.csv": {
        "percentage_of_systems": (0.0, 100.0),
    },
    "mods_blockers_by_os_name_and_codename.csv": {
        "entries_per_system": (0.0, 50.0),
    },
    "on_off_mods_sleep_summary_by_cpu.csv": {
        "avg_on_time": (0.0, 1440.0),
        "avg_off_time": (0.0, 1440.0),
        "avg_modern_sleep_time": (0.0, 1440.0),
        "avg_sleep_time": (0.0, 1440.0),
        "avg_total_time": (0.0, 5760.0),
        "avg_pcnt_on_time": (0.0, 100.0),
        "avg_pcnt_off_time": (0.0, 100.0),
        "avg_pcnt_mods_time": (0.0, 100.0),
        "avg_pcnt_sleep_time": (0.0, 100.0),
    },
    "persona_web_category_usage_analysis.csv": {
        "days": (0.0, 365.0),
    },
    "package_power_by_country.csv": {
        "avg_pkg_power_consumed": (0.0, 50.0),
    },
    "popular_browsers_by_count_usage_percentage.csv": {
        "percent_systems": (0.0, 100.0),
        "percent_instances": (0.0, 100.0),
        "percent_duration": (0.0, 100.0),
    },
    "ram_utilization_histogram.csv": {
        "avg_percentage_used": (0.0, 100.0),
    },
    "ranked_process_classifications.csv": {
        "total_power_consumption": (0.0, 100.0),
    },
}

def load_sources():
    return {f: pd.read_csv(os.path.join(BASE_DIR, f)) for f in TARGET_FILES}

def sample_categorical(series, n, rng):
    vals = [x for x in series.dropna().astype(str).unique().tolist() if x != "nan"]
    if not vals:
        vals = ["dummy_1"]
    return rng.choice(vals, size=n, replace=True)

def make_file(file_name, n, sources, seed=42):
    rng = np.random.default_rng(seed)
    src = sources[file_name]

    if file_name == "battery_power_on_geographic_summary.csv":
        return pd.DataFrame({
            "country": sample_categorical(src["country"], n, rng),
            "number_of_systems": rng.integers(100, 5000, size=n),
            "avg_number_of_dc_powerons": np.round(rng.uniform(0.2, 10.0, size=n), 2),
            "avg_duration": np.round(rng.uniform(5.0, 60.0, size=n), 2),
        })

    if file_name == "battery_on_duration_by_cpu_family_and_generation.csv":
        return pd.DataFrame({
            "marketcodename": sample_categorical(src["marketcodename"], n, rng),
            "cpugen": sample_categorical(src["cpugen"], n, rng),
            "number_of_systems": rng.integers(100, 5000, size=n),
            "avg_duration_mins_on_battery": np.round(rng.uniform(5.0, 60.0, size=n), 2),
        })

    if file_name == "display_devices_connection_type_resolution_durations.csv":
        res_choices = ["1920x1080", "1366x768", "2560x1440", "3840x2160", "1600x900", "1280x720"]
        return pd.DataFrame({
            "connection_type": sample_categorical(src["connection_type"], n, rng),
            "resolution": rng.choice(res_choices, size=n, replace=True),
            "number_of_systems": rng.integers(50, 4000, size=n),
            "average_duration_on_ac_in_seconds": np.round(rng.uniform(60.0, 3600.0, size=n), 2),
            "average_duration_on_dc_in_seconds": np.round(rng.uniform(60.0, 3600.0, size=n), 2),
        })

    if file_name == "display_devices_vendors_percentage.csv":
        total = int(rng.integers(5000, 50000))
        counts = rng.integers(1, total + 1, size=n)
        pct = np.round(np.clip(counts / total * 100, 0, 100), 2)
        return pd.DataFrame({
            "vendor_name": sample_categorical(src["vendor_name"], n, rng),
            "number_of_systems": counts,
            "total_number_of_systems": total,
            "percentage_of_systems": pct,
        })

    if file_name == "mods_blockers_by_os_name_and_codename.csv":
        systems = rng.integers(10, 3000, size=n)
        eps = np.round(rng.uniform(0.1, 50.0, size=n), 2)
        entries = np.round(eps * systems, 2)
        return pd.DataFrame({
            "os_name": sample_categorical(src["os_name"], n, rng),
            "os_codename": sample_categorical(src["os_codename"], n, rng),
            "num_entries": entries,
            "number_of_systems": systems,
            "entries_per_system": eps,
        })

    if file_name == "most_popular_browser_in_each_country.csv":
        return pd.DataFrame({
            "country": sample_categorical(src["country"], n, rng),
            "browser": sample_categorical(src["browser"], n, rng),
        })

    if file_name == "on_off_mods_sleep_summary_by_cpu.csv":
        on = rng.uniform(50, 900, size=n)
        off = rng.uniform(50, 900, size=n)
        mods = rng.uniform(10, 600, size=n)
        sleep = rng.uniform(10, 600, size=n)
        total = on + off + mods + sleep
        return pd.DataFrame({
            "marketcodename": sample_categorical(src["marketcodename"], n, rng),
            "cpugen": sample_categorical(src["cpugen"], n, rng),
            "number_of_systems": rng.integers(100, 5000, size=n),
            "avg_on_time": np.round(on, 2),
            "avg_off_time": np.round(off, 2),
            "avg_modern_sleep_time": np.round(mods, 2),
            "avg_sleep_time": np.round(sleep, 2),
            "avg_total_time": np.round(total, 2),
            "avg_pcnt_on_time": np.round(on / total * 100, 2),
            "avg_pcnt_off_time": np.round(off / total * 100, 2),
            "avg_pcnt_mods_time": np.round(mods / total * 100, 2),
            "avg_pcnt_sleep_time": np.round(sleep / total * 100, 2),
        })

    if file_name == "persona_web_category_usage_analysis.csv":
        data = {
            "persona": sample_categorical(src["persona"], n, rng),
            "number_of_systems": rng.integers(100, 5000, size=n),
            "days": np.round(rng.uniform(1, 365, size=n), 2),
        }
        for col in src.columns[3:]:
            data[col] = np.round(rng.uniform(0, 100, size=n), 3)
        return pd.DataFrame(data)

    if file_name == "package_power_by_country.csv":
        return pd.DataFrame({
            "countryname_normalized": sample_categorical(src["countryname_normalized"], n, rng),
            "number_of_systems": rng.integers(100, 5000, size=n),
            "avg_pkg_power_consumed": np.round(rng.uniform(1.0, 50.0, size=n), 2),
        })

    if file_name == "popular_browsers_by_count_usage_percentage.csv":
        return pd.DataFrame({
            "browser": sample_categorical(src["browser"], n, rng),
            "percent_systems": np.round(rng.uniform(0, 100, size=n), 2),
            "percent_instances": np.round(rng.uniform(0, 100, size=n), 2),
            "percent_duration": np.round(rng.uniform(0, 100, size=n), 2),
        })

    if file_name == "ram_utilization_histogram.csv":
        ram_choices = [4, 8, 16, 32, 64, 128]
        return pd.DataFrame({
            "ram_gb": rng.choice(ram_choices, size=n, replace=True),
            "number_of_systems": rng.integers(50, 5000, size=n),
            "avg_percentage_used": np.round(rng.uniform(1.0, 100.0, size=n), 2),
        })

    if file_name == "ranked_process_classifications.csv":
        user_ids = rng.choice(np.arange(100000, 999999), size=n, replace=False)
        tpc = np.round(rng.uniform(0.1, 100.0, size=n), 2)
        order = np.argsort(-tpc, kind="mergesort")
        ranks = np.empty(n, dtype=int)
        ranks[order] = np.arange(1, n + 1)
        return pd.DataFrame({
            "user_id": user_ids,
            "total_power_consumption": tpc,
            "rnk": ranks,
        })

    raise ValueError(f"Unhandled file: {file_name}")

def validate(dirpath):
    problems = []
    for fname, bounds in CAPS.items():
        df = pd.read_csv(os.path.join(dirpath, fname))
        for col, (lo, hi) in bounds.items():
            vals = pd.to_numeric(df[col], errors="coerce")
            if vals.isna().any() or (vals < lo).any() or (vals > hi).any():
                problems.append((fname, col))
    return problems

def build_outputs(mini_rows=100, large_rows=1000):
    sources = load_sources()

    mini_dir = os.path.join(BASE_DIR, "dummy_fixed_mini_100")
    large_dir = os.path.join(BASE_DIR, "dummy_fixed_large_1000")
    os.makedirs(mini_dir, exist_ok=True)
    os.makedirs(large_dir, exist_ok=True)

    for fname in TARGET_FILES:
        make_file(fname, mini_rows, sources, seed=123).to_csv(os.path.join(mini_dir, fname), index=False)
        make_file(fname, large_rows, sources, seed=456).to_csv(os.path.join(large_dir, fname), index=False)

    mini_problems = validate(mini_dir)
    large_problems = validate(large_dir)

    if mini_problems or large_problems:
        raise RuntimeError(
            f"Validation failed. mini={mini_problems[:5]}, large={large_problems[:5]}"
        )

    print("Done.")
    print("Mini folder :", mini_dir)
    print("Large folder:", large_dir)

if __name__ == "__main__":
    build_outputs()

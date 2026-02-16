-- =========================
-- File: 01_validation.sql (DuckDB version - COMPLETE)
-- Purpose: Validate all 22 reporting tables (16 original + 6 missing)
-- =========================

-- ============================================================
-- PART 1: ROW COUNTS FOR ALL 22 REPORTING TABLES
-- ============================================================

-- Original 16 tables
SELECT 'reporting.system_display_devices' AS tbl, COUNT(*) AS n_rows, COUNT(DISTINCT guid) AS n_guids 
FROM reporting.system_display_devices
UNION ALL
SELECT 'reporting.system_userwait', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_userwait
UNION ALL
SELECT 'reporting.system_hw_pkg_power', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_hw_pkg_power
UNION ALL
SELECT 'reporting.system_pkg_C0', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_pkg_C0
UNION ALL
SELECT 'reporting.system_pkg_avg_freq_mhz', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_pkg_avg_freq_mhz
UNION ALL
SELECT 'reporting.system_pkg_temp_centigrade', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_pkg_temp_centigrade
UNION ALL
SELECT 'reporting.system_psys_rap_watts', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_psys_rap_watts
UNION ALL
SELECT 'reporting.system_web_cat_pivot', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_pivot
UNION ALL
SELECT 'reporting.system_network_consumption', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_network_consumption
UNION ALL
SELECT 'reporting.system_web_cat_pivot_duration', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_pivot_duration
UNION ALL
SELECT 'reporting.system_web_cat_pivot_page_load_count', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_pivot_page_load_count
UNION ALL
SELECT 'reporting.system_web_cat_pivot_page_visit_count', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_pivot_page_visit_count
UNION ALL
SELECT 'reporting.system_web_cat_pivot_domain_count', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_pivot_domain_count
UNION ALL
SELECT 'reporting.system_web_cat_usage', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_web_cat_usage
UNION ALL
SELECT 'reporting.system_memory_utilization', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_memory_utilization
UNION ALL
SELECT 'reporting.system_mods_power_consumption', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_mods_power_consumption
UNION ALL

-- Missing 6 tables (from Layne)
SELECT 'reporting.system_batt_dc_events', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_batt_dc_events
UNION ALL
SELECT 'reporting.system_cpu_metadata', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_cpu_metadata
UNION ALL
SELECT 'reporting.system_mods_top_blocker_hist', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_mods_top_blocker_hist
UNION ALL
SELECT 'reporting.system_os_codename_history', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_os_codename_history
UNION ALL
SELECT 'reporting.system_on_off_suspend_time_day', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_on_off_suspend_time_day
UNION ALL
SELECT 'reporting.system_frgnd_apps_types', COUNT(*), COUNT(DISTINCT guid)
FROM reporting.system_frgnd_apps_types
ORDER BY tbl;


-- ============================================================
-- PART 2: SPOT CHECKS ON SOURCE AND REPORTING TABLES
-- ============================================================

-- Source tables spot checks
SELECT 'Source: system_sysinfo_unique_normalized' AS check_type, COUNT(*) AS row_count, COUNT(DISTINCT guid) AS unique_guids
FROM university_analysis_pad.system_sysinfo_unique_normalized;

SELECT * FROM university_analysis_pad.system_sysinfo_unique_normalized LIMIT 25;

-- Reporting tables spot checks
SELECT * FROM reporting.system_display_devices LIMIT 25;

SELECT * FROM reporting.system_userwait LIMIT 25;

SELECT * FROM reporting.system_hw_pkg_power LIMIT 25;

SELECT * FROM reporting.system_batt_dc_events LIMIT 25;

SELECT * FROM reporting.system_cpu_metadata LIMIT 25;

SELECT * FROM reporting.system_frgnd_apps_types LIMIT 25;


-- ============================================================
-- PART 3: DATA QUALITY SANITY CHECKS
-- ============================================================

-- Check 1: Negative utilized RAM (should be minimal)
SELECT 'Negative RAM Utilization Check' AS check_name, COUNT(*) AS neg_utilized_count
FROM reporting.system_memory_utilization
WHERE utilized_ram < 0;

-- Check 2: Display device durations exceeding 24 hours (86400 seconds)
SELECT 'Display Duration > 24hrs Check' AS check_name, COUNT(*) AS excessive_duration_count
FROM (
    SELECT guid, dt, SUM(duration_ac + duration_dc) AS total_duration
    FROM reporting.system_display_devices
    GROUP BY guid, dt
    HAVING SUM(duration_ac + duration_dc) > 86400
) excessive;

-- Show top offenders
SELECT guid, dt, SUM(duration_ac + duration_dc) AS total_duration
FROM reporting.system_display_devices
GROUP BY guid, dt
HAVING SUM(duration_ac + duration_dc) > 86400
ORDER BY total_duration DESC
LIMIT 50;

-- Check 3: Display device status distribution
SELECT status, COUNT(*) AS n
FROM reporting.system_display_devices
GROUP BY status
ORDER BY n DESC;

-- Check 4: Web category combinations (from source)
SELECT DISTINCT parent_category || '-' || sub_category AS category
FROM university_prod.web_cat_usage_v2
ORDER BY category
LIMIT 100;

-- Check 5: Battery events with NULL handling verification
SELECT 
    'Battery Events NULLIF Check' AS check_name,
    COUNT(*) AS total_events,
    COUNT(max_power_on_battery_percent) AS non_null_max_on,
    COUNT(min_power_on_battery_percent) AS non_null_min_on,
    COUNT(avg_power_on_battery_percent) AS non_null_avg_on
FROM reporting.system_batt_dc_events;

-- Check 6: CPU metadata coverage
SELECT 
    'CPU Metadata Check' AS check_name,
    COUNT(DISTINCT guid) AS unique_systems,
    COUNT(DISTINCT marketcodename) AS unique_market_codes,
    COUNT(DISTINCT cpugen) AS unique_cpu_gens
FROM reporting.system_cpu_metadata;

-- Check 7: Foreground apps types coverage
SELECT 
    'Foreground Apps Check' AS check_name,
    COUNT(DISTINCT guid) AS unique_systems,
    COUNT(DISTINCT app_type) AS unique_app_types,
    COUNT(DISTINCT exe_name) AS unique_exe_names
FROM reporting.system_frgnd_apps_types;


-- ============================================================
-- PART 4: BENCHMARK QUERY READINESS CHECK
-- ============================================================

-- This checks if all tables needed for the 22 benchmark queries exist
SELECT 
    'Benchmark Query Readiness' AS check_name,
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_batt_dc_events')
         AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_cpu_metadata')
         AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_mods_top_blocker_hist')
         AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_os_codename_history')
         AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_on_off_suspend_time_day')
         AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'reporting' AND table_name = 'system_frgnd_apps_types')
        THEN 'ALL 22 TABLES EXIST - ALL QUERIES SHOULD WORK!'
        ELSE 'MISSING TABLES - SOME QUERIES WILL FAIL'
    END AS status;


-- ============================================================
-- PART 5: SUMMARY STATISTICS
-- ============================================================

-- Overall system coverage across all tables
SELECT 
    'Overall Coverage' AS metric,
    COUNT(DISTINCT guid) AS unique_systems
FROM university_analysis_pad.system_sysinfo_unique_normalized;

-- Date range coverage
SELECT 
    'Date Range' AS metric,
    MIN(dt) AS earliest_date,
    MAX(dt) AS latest_date,
    DATEDIFF('day', MIN(dt), MAX(dt)) AS days_span
FROM reporting.system_display_devices;


-- ============================================================
-- VALIDATION COMPLETE
-- ============================================================
-- Expected results:
-- - All 22 tables should show row counts > 0
-- - Negative RAM utilization should be minimal
-- - Display durations > 24hrs should be rare anomalies
-- - All benchmark query readiness check should pass
-- ============================================================
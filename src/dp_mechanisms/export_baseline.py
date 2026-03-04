"""
export_baseline.py

Purpose: Export benchmark query results to CSV files for DP mechanism testing
Runs first 12 queries from 02_analysis_queries.sql and saves each to a separate CSV

Usage:
    python export_baseline.py

Output:
    CSV files in ../data/baseline_mini/ (or baseline_full/)
    - query_01.csv
    - query_02.csv
    - ... (12 files total)
"""

import duckdb
import pandas as pd
import os
from datetime import datetime
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

# Database path (change to data.duckdb for full database)
DATABASE_PATH = "data_mini.duckdb"

# Output directory (change to baseline_full for full database)
OUTPUT_DIR = "data/baseline_mini"

# Create output directory if it doesn't exist
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Global thresholds / caps (aligned with DP config assumptions)
K_MIN_100 = 100
K_MIN_50 = 50
K_MIN_10 = 10

CAP_DUR_MINS = 60.0
CAP_POWER_ONS = 10.0
CAP_SECONDS = 3600.0
CAP_ENTRIES_PER_GUID = 50.0
CAP_POWER_WATTS = 50.0
CAP_PERCENT = 100.0

# Browser usage caps
CAP_BROWSER_INSTANCES_PER_GUID = 50.0
CAP_BROWSER_DURATION_MS_PER_GUID = 3_600_000.0  # 1 hour cap per guid per browser

# Persona “days” cap (if a GUID shows up in too many rows)
CAP_DAYS_PER_GUID = 365.0


# =============================================================================
# QUERY DEFINITIONS
# =============================================================================

# # Dictionary of queries: {query_number: (query_name, sql_query)}
# QUERIES = {
#     1: ("Battery Power On Geographic Summary", """
#         SELECT
#             countryname_normalized AS country,
#             COUNT(DISTINCT a.guid) AS number_of_systems,
#             AVG(num_power_ons) AS avg_number_of_dc_powerons,
#             AVG(duration_mins) AS avg_duration
#         FROM reporting.system_batt_dc_events AS a
#         INNER JOIN university_analysis_pad.system_sysinfo_unique_normalized AS b
#             ON a.guid = b.guid
#         GROUP BY countryname_normalized
#         HAVING COUNT(DISTINCT a.guid) > 100
#         ORDER BY avg_number_of_dc_powerons DESC
#     """),
    
#     2: ("Battery On Duration by CPU Family and Generation", """
#         SELECT
#             marketcodename,
#             cpugen,
#             COUNT(DISTINCT b.guid) AS number_of_systems,
#             AVG(duration_mins) AS avg_duration_mins_on_battery
#         FROM reporting.system_cpu_metadata AS a
#         INNER JOIN reporting.system_batt_dc_events AS b
#             ON a.guid = b.guid
#         WHERE cpugen <> 'Unknown'
#         GROUP BY marketcodename, cpugen
#         HAVING COUNT(DISTINCT b.guid) > 100
#     """),
    
#     3: ("Display Devices Connection Type Resolution Durations", """
#         SELECT
#             connection_type,
#             CAST(resolution_heigth AS VARCHAR) || 'x' || CAST(resolution_width AS VARCHAR) AS resolution,
#             COUNT(DISTINCT guid) AS number_of_systems,
#             ROUND(AVG(duration_ac), 2) AS average_duration_on_ac_in_seconds,
#             ROUND(AVG(duration_dc), 2) AS average_duration_on_dc_in_seconds
#         FROM reporting.system_display_devices
#         WHERE connection_type IS NOT NULL
#           AND resolution_heigth <> 0
#           AND resolution_width <> 0
#         GROUP BY connection_type, resolution_heigth, resolution_width
#         HAVING COUNT(DISTINCT guid) > 50
#         ORDER BY connection_type, number_of_systems DESC
#     """),
    
#     4: ("Display Devices Vendors Percentage", """
#         SELECT
#             vendor_name,
#             COUNT(DISTINCT guid) AS number_of_systems,
#             total_number_of_systems,
#             ROUND(COUNT(DISTINCT guid) * 100.0 / total_number_of_systems, 2) AS percentage_of_systems
#         FROM reporting.system_display_devices
#         CROSS JOIN (
#             SELECT COUNT(DISTINCT guid) AS total_number_of_systems
#             FROM reporting.system_display_devices
#         ) AS inn
#         GROUP BY vendor_name, total_number_of_systems
#         ORDER BY percentage_of_systems DESC
#     """),
    
#     5: ("MODS Blockers by OS Name and Codename", """
#         SELECT
#             os_name,
#             os_codename,
#             COUNT(*) AS num_entries,
#             COUNT(DISTINCT guid) AS number_of_systems,
#             CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT guid) AS entries_per_system
#         FROM (
#             SELECT a.guid, min_ts, max_ts, os_name, os_codename, dt_utc
#             FROM reporting.system_mods_top_blocker_hist AS a
#             INNER JOIN reporting.system_os_codename_history AS b
#                 ON a.guid = b.guid
#             WHERE a.dt_utc BETWEEN b.min_ts AND b.max_ts
#         ) AS inn
#         GROUP BY os_name, os_codename
#         HAVING COUNT(DISTINCT guid) > 10
#     """),
    
#     6: ("Most Popular Browser in Each Country", """
#         SELECT
#             country,
#             browser
#         FROM (
#             SELECT
#                 countryname_normalized AS country,
#                 browser,
#                 COUNT(DISTINCT b.guid) AS number_of_systems,
#                 RANK() OVER (PARTITION BY countryname_normalized ORDER BY COUNT(DISTINCT b.guid) DESC) AS rnk
#             FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
#             INNER JOIN reporting.system_web_cat_usage AS b
#                 ON a.guid = b.guid
#             GROUP BY countryname_normalized, browser
#         ) AS x
#         WHERE rnk = 1
#     """),
    
#     7: ("On Off MODS Sleep Summary by CPU", """
#         SELECT
#             b.marketcodename,
#             b.cpugen,
#             COUNT(DISTINCT a.guid) AS number_of_systems,
#             ROUND(AVG(on_time), 2) AS avg_on_time,
#             ROUND(AVG(off_time), 2) AS avg_off_time,
#             ROUND(AVG(mods_time), 2) AS avg_modern_sleep_time,
#             ROUND(AVG(sleep_time), 2) AS avg_sleep_time,
#             ROUND(AVG(on_time + off_time + mods_time + sleep_time), 2) AS avg_total_time,
#             ROUND(SUM(on_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_on_time,
#             ROUND(SUM(off_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_off_time,
#             ROUND(SUM(mods_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_mods_time,
#             ROUND(SUM(sleep_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_sleep_time
#         FROM reporting.system_on_off_suspend_time_day AS a
#         INNER JOIN reporting.system_cpu_metadata AS b
#             ON a.guid = b.guid
#         GROUP BY b.marketcodename, b.cpugen
#         HAVING COUNT(DISTINCT a.guid) > 100
#     """),
    
#     8: ("Persona Web Category Usage Analysis", """
#         SELECT
#             persona,
#             COUNT(DISTINCT guid) AS number_of_systems,
#             SUM(days) AS days,
#             ROUND(100 * SUM(days * content_creation_photo_edit_creation / total_duration) / SUM(days), 3) AS content_creation_photo_edit_creation,
#             ROUND(100 * SUM(days * content_creation_video_audio_edit_creation / total_duration) / SUM(days), 3) AS content_creation_video_audio_edit_creation,
#             ROUND(100 * SUM(days * content_creation_web_design_development / total_duration) / SUM(days), 3) AS content_creation_web_design_development,
#             ROUND(100 * SUM(days * education / total_duration) / SUM(days), 3) AS education,
#             ROUND(100 * SUM(days * entertainment_music_audio_streaming / total_duration) / SUM(days), 3) AS entertainment_music_audio_streaming,
#             ROUND(100 * SUM(days * entertainment_other / total_duration) / SUM(days), 3) AS entertainment_other,
#             ROUND(100 * SUM(days * entertainment_video_streaming / total_duration) / SUM(days), 3) AS entertainment_video_streaming,
#             ROUND(100 * SUM(days * finance / total_duration) / SUM(days), 3) AS finance,
#             ROUND(100 * SUM(days * games_other / total_duration) / SUM(days), 3) AS games_other,
#             ROUND(100 * SUM(days * games_video_games / total_duration) / SUM(days), 3) AS games_video_games,
#             ROUND(100 * SUM(days * mail / total_duration) / SUM(days), 3) AS mail,
#             ROUND(100 * SUM(days * news / total_duration) / SUM(days), 3) AS news,
#             ROUND(100 * SUM(days * unclassified / total_duration) / SUM(days), 3) AS unclassified,
#             ROUND(100 * SUM(days * private / total_duration) / SUM(days), 3) AS private,
#             ROUND(100 * SUM(days * productivity_crm / total_duration) / SUM(days), 3) AS productivity_crm,
#             ROUND(100 * SUM(days * productivity_other / total_duration) / SUM(days), 3) AS productivity_other,
#             ROUND(100 * SUM(days * productivity_presentations / total_duration) / SUM(days), 3) AS productivity_presentations,
#             ROUND(100 * SUM(days * productivity_programming / total_duration) / SUM(days), 3) AS productivity_programming,
#             ROUND(100 * SUM(days * productivity_project_management / total_duration) / SUM(days), 3) AS productivity_project_management,
#             ROUND(100 * SUM(days * productivity_spreadsheets / total_duration) / SUM(days), 3) AS productivity_spreadsheets,
#             ROUND(100 * SUM(days * productivity_word_processing / total_duration) / SUM(days), 3) AS productivity_word_processing,
#             ROUND(100 * SUM(days * recreation_travel / total_duration) / SUM(days), 3) AS recreation_travel,
#             ROUND(100 * SUM(days * reference / total_duration) / SUM(days), 3) AS reference,
#             ROUND(100 * SUM(days * search / total_duration) / SUM(days), 3) AS search,
#             ROUND(100 * SUM(days * shopping / total_duration) / SUM(days), 3) AS shopping,
#             ROUND(100 * SUM(days * social_social_network / total_duration) / SUM(days), 3) AS social_social_network,
#             ROUND(100 * SUM(days * social_communication / total_duration) / SUM(days), 3) AS social_communication,
#             ROUND(100 * SUM(days * social_communication_live / total_duration) / SUM(days), 3) AS social_communication_live
#         FROM (
#             SELECT
#                 a.guid,
#                 a.persona,
#                 COUNT(*) AS days,
#                 SUM(b.content_creation_photo_edit_creation) AS content_creation_photo_edit_creation,
#                 SUM(b.content_creation_video_audio_edit_creation) AS content_creation_video_audio_edit_creation,
#                 SUM(b.content_creation_web_design_development) AS content_creation_web_design_development,
#                 SUM(b.education) AS education,
#                 SUM(b.entertainment_music_audio_streaming) AS entertainment_music_audio_streaming,
#                 SUM(b.entertainment_other) AS entertainment_other,
#                 SUM(b.entertainment_video_streaming) AS entertainment_video_streaming,
#                 SUM(b.finance) AS finance,
#                 SUM(b.games_other) AS games_other,
#                 SUM(b.games_video_games) AS games_video_games,
#                 SUM(b.mail) AS mail,
#                 SUM(b.news) AS news,
#                 SUM(b.unclassified) AS unclassified,
#                 SUM(b.private) AS private,
#                 SUM(b.productivity_crm) AS productivity_crm,
#                 SUM(b.productivity_other) AS productivity_other,
#                 SUM(b.productivity_presentations) AS productivity_presentations,
#                 SUM(b.productivity_programming) AS productivity_programming,
#                 SUM(b.productivity_project_management) AS productivity_project_management,
#                 SUM(b.productivity_spreadsheets) AS productivity_spreadsheets,
#                 SUM(b.productivity_word_processing) AS productivity_word_processing,
#                 SUM(b.recreation_travel) AS recreation_travel,
#                 SUM(b.reference) AS reference,
#                 SUM(b.search) AS search,
#                 SUM(b.shopping) AS shopping,
#                 SUM(b.social_social_network) AS social_social_network,
#                 SUM(b.social_communication) AS social_communication,
#                 SUM(b.social_communication_live) AS social_communication_live,
#                 SUM(
#                     b.content_creation_video_audio_edit_creation +
#                     b.content_creation_photo_edit_creation +
#                     b.content_creation_web_design_development +
#                     b.education +
#                     b.entertainment_music_audio_streaming +
#                     b.entertainment_other +
#                     b.entertainment_video_streaming +
#                     b.finance +
#                     b.games_other +
#                     b.games_video_games +
#                     b.mail +
#                     b.news +
#                     b.unclassified +
#                     b.private +
#                     b.productivity_crm +
#                     b.productivity_other +
#                     b.productivity_presentations +
#                     b.productivity_programming +
#                     b.productivity_project_management +
#                     b.productivity_spreadsheets +
#                     b.productivity_word_processing +
#                     b.recreation_travel +
#                     b.reference +
#                     b.search +
#                     b.shopping +
#                     b.social_social_network +
#                     b.social_communication +
#                     b.social_communication_live
#                 ) AS total_duration
#             FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
#             INNER JOIN reporting.system_web_cat_pivot_duration AS b
#                 ON a.guid = b.guid
#             GROUP BY a.guid, a.persona
#         ) AS inn
#         GROUP BY persona
#         ORDER BY number_of_systems DESC
#     """),
    
#     9: ("Package Power by Country", """
#         SELECT
#             a.countryname_normalized,
#             COUNT(DISTINCT b.guid) AS number_of_systems,
#             SUM(nrs * mean) / SUM(nrs) AS avg_pkg_power_consumed
#         FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
#         INNER JOIN reporting.system_hw_pkg_power AS b
#             ON a.guid = b.guid
#         GROUP BY a.countryname_normalized
#         ORDER BY avg_pkg_power_consumed DESC
#     """),
    
#     10: ("Popular Browsers by Count Usage Percentage", """
#         SELECT
#             browser,
#             ROUND(num_systems * 100.0 / total_systems, 2) AS percent_systems,
#             ROUND(num_instances * 100.0 / tot_instances, 2) AS percent_instances,
#             ROUND(sum_duration * 100.0 / tot_duration, 2) AS percent_duration
#         FROM (
#             SELECT browser,
#                    COUNT(DISTINCT guid) AS num_systems,
#                    COUNT(*) AS num_instances,
#                    SUM(duration_ms) AS sum_duration
#             FROM reporting.system_web_cat_usage
#             GROUP BY browser
#         ) AS a
#         CROSS JOIN (
#             SELECT COUNT(DISTINCT guid) AS total_systems,
#                    COUNT(*) AS tot_instances,
#                    SUM(duration_ms) AS tot_duration
#             FROM reporting.system_web_cat_usage
#         ) AS b
#     """),
    
#     11: ("RAM Utilization Histogram", """
#         SELECT
#             (sysinfo_ram / POWER(2, 10)) AS ram_gb,
#             COUNT(DISTINCT guid) AS number_of_systems,
#             ROUND(SUM(nrs * avg_percentage_used) / SUM(nrs)) AS avg_percentage_used
#         FROM reporting.system_memory_utilization
#         WHERE avg_percentage_used > 0
#         GROUP BY sysinfo_ram
#         ORDER BY sysinfo_ram ASC
#     """),
    
#     12: ("Ranked Process Classifications", """
#         SELECT
#             user_id,
#             SUM(total_power_consumption) AS total_power_consumption,
#             RANK() OVER (ORDER BY SUM(total_power_consumption) DESC) AS rnk
#         FROM reporting.system_mods_power_consumption
#         GROUP BY user_id
#     """),
# }

# =============================================================================
# QUERY DEFINITIONS
# =============================================================================

QUERIES = {
    # -------------------------------------------------------------------------
    # Q1: Battery Power On Geographic Summary
    # - per-guid per-country summary, clipped, then averaged
    # -------------------------------------------------------------------------
    1: ("Battery Power On Geographic Summary", f"""
        WITH per_guid AS (
            SELECT
                b.countryname_normalized AS country,
                a.guid,
                LEAST(AVG(a.num_power_ons), {CAP_POWER_ONS}) AS guid_avg_power_ons,
                LEAST(AVG(a.duration_mins), {CAP_DUR_MINS}) AS guid_avg_duration
            FROM reporting.system_batt_dc_events AS a
            INNER JOIN university_analysis_pad.system_sysinfo_unique_normalized AS b
                ON a.guid = b.guid
            GROUP BY b.countryname_normalized, a.guid
        )
        SELECT
            country,
            COUNT(*) AS number_of_systems,
            AVG(guid_avg_power_ons) AS avg_number_of_dc_powerons,
            AVG(guid_avg_duration) AS avg_duration
        FROM per_guid
        GROUP BY country
        HAVING COUNT(*) >= {K_MIN_100}
        ORDER BY avg_number_of_dc_powerons DESC
    """),

    # -------------------------------------------------------------------------
    # Q2: Battery On Duration by CPU Family and Generation
    # -------------------------------------------------------------------------
    2: ("Battery On Duration by CPU Family and Generation", f"""
        WITH per_guid AS (
            SELECT
                a.marketcodename,
                a.cpugen,
                b.guid,
                LEAST(AVG(b.duration_mins), {CAP_DUR_MINS}) AS guid_avg_duration
            FROM reporting.system_cpu_metadata AS a
            INNER JOIN reporting.system_batt_dc_events AS b
                ON a.guid = b.guid
            WHERE a.cpugen <> 'Unknown'
            GROUP BY a.marketcodename, a.cpugen, b.guid
        )
        SELECT
            marketcodename,
            cpugen,
            COUNT(*) AS number_of_systems,
            AVG(guid_avg_duration) AS avg_duration_mins_on_battery
        FROM per_guid
        GROUP BY marketcodename, cpugen
        HAVING COUNT(*) >= {K_MIN_100}
    """),

    # -------------------------------------------------------------------------
    # Q3: Display Devices Connection Type Resolution Durations
    # -------------------------------------------------------------------------
    3: ("Display Devices Connection Type Resolution Durations", f"""
        WITH per_guid AS (
            SELECT
                connection_type,
                resolution_heigth,
                resolution_width,
                guid,
                LEAST(AVG(duration_ac), {CAP_SECONDS}) AS guid_avg_ac,
                LEAST(AVG(duration_dc), {CAP_SECONDS}) AS guid_avg_dc
            FROM reporting.system_display_devices
            WHERE connection_type IS NOT NULL
              AND resolution_heigth <> 0
              AND resolution_width <> 0
            GROUP BY connection_type, resolution_heigth, resolution_width, guid
        )
        SELECT
            connection_type,
            CAST(resolution_heigth AS VARCHAR) || 'x' || CAST(resolution_width AS VARCHAR) AS resolution,
            COUNT(*) AS number_of_systems,
            ROUND(AVG(guid_avg_ac), 2) AS average_duration_on_ac_in_seconds,
            ROUND(AVG(guid_avg_dc), 2) AS average_duration_on_dc_in_seconds
        FROM per_guid
        GROUP BY connection_type, resolution_heigth, resolution_width
        HAVING COUNT(*) >= {K_MIN_50}
        ORDER BY connection_type, number_of_systems DESC
    """),

    # -------------------------------------------------------------------------
    # Q4: Display Devices Vendors Percentage (Structured: percent from counts)
    # -------------------------------------------------------------------------
    4: ("Display Devices Vendors Percentage", f"""
        WITH per_guid AS (
            SELECT DISTINCT vendor_name, guid
            FROM reporting.system_display_devices
            WHERE vendor_name IS NOT NULL
        ),
        counts AS (
            SELECT vendor_name, COUNT(*) AS number_of_systems
            FROM per_guid
            GROUP BY vendor_name
        ),
        total AS (
            SELECT SUM(number_of_systems) AS total_number_of_systems
            FROM counts
        )
        SELECT
            c.vendor_name,
            c.number_of_systems,
            t.total_number_of_systems,
            ROUND(c.number_of_systems * 100.0 / t.total_number_of_systems, 2) AS percentage_of_systems
        FROM counts c
        CROSS JOIN total t
        ORDER BY percentage_of_systems DESC
    """),

    # -------------------------------------------------------------------------
    # Q5: MODS Blockers by OS Name and Codename
    # - clip per-guid entry counts before summing
    # -------------------------------------------------------------------------
    5: ("MODS Blockers by OS Name and Codename", f"""
        WITH joined AS (
            SELECT a.guid, min_ts, max_ts, os_name, os_codename
            FROM reporting.system_mods_top_blocker_hist AS a
            INNER JOIN reporting.system_os_codename_history AS b
                ON a.guid = b.guid
            WHERE a.dt_utc BETWEEN b.min_ts AND b.max_ts
        ),
        per_guid AS (
            SELECT
                os_name,
                os_codename,
                guid,
                LEAST(COUNT(*), {CAP_ENTRIES_PER_GUID}) AS guid_entries
            FROM joined
            GROUP BY os_name, os_codename, guid
        )
        SELECT
            os_name,
            os_codename,
            SUM(guid_entries) AS num_entries,
            COUNT(*) AS number_of_systems,
            CAST(SUM(guid_entries) AS DOUBLE) / COUNT(*) AS entries_per_system
        FROM per_guid
        GROUP BY os_name, os_codename
        HAVING COUNT(*) >= {K_MIN_10}
    """),

    # -------------------------------------------------------------------------
    # Q6: Most Popular Browser in Each Country
    # Keep output schema (country, browser). But make per-guid distinct before rank.
    # -------------------------------------------------------------------------
    6: ("Most Popular Browser in Each Country", f"""
        WITH per_guid AS (
            SELECT DISTINCT
                a.countryname_normalized AS country,
                b.browser,
                b.guid
            FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
            INNER JOIN reporting.system_web_cat_usage AS b
                ON a.guid = b.guid
            WHERE b.browser IS NOT NULL
        ),
        counts AS (
            SELECT
                country,
                browser,
                COUNT(*) AS number_of_systems
            FROM per_guid
            GROUP BY country, browser
        )
        SELECT
            country,
            browser
        FROM (
            SELECT
                country,
                browser,
                number_of_systems,
                RANK() OVER (PARTITION BY country ORDER BY number_of_systems DESC) AS rnk
            FROM counts
        ) x
        WHERE rnk = 1
    """),

    # -------------------------------------------------------------------------
    # Q7: On Off MODS Sleep Summary by CPU
    # - per-guid per cpu averages clipped, then averaged across guids
    # -------------------------------------------------------------------------
    7: ("On Off MODS Sleep Summary by CPU", f"""
        WITH per_guid AS (
            SELECT
                b.marketcodename,
                b.cpugen,
                a.guid,
                LEAST(AVG(a.on_time), {CAP_PERCENT * 14.4})  AS tmp1, -- placeholder to avoid zero division
                LEAST(AVG(a.on_time), {1440.0})  AS guid_on,
                LEAST(AVG(a.off_time), {1440.0}) AS guid_off,
                LEAST(AVG(a.mods_time), {1440.0}) AS guid_mods,
                LEAST(AVG(a.sleep_time), {1440.0}) AS guid_sleep
            FROM reporting.system_on_off_suspend_time_day AS a
            INNER JOIN reporting.system_cpu_metadata AS b
                ON a.guid = b.guid
            GROUP BY b.marketcodename, b.cpugen, a.guid
        ),
        per_guid_total AS (
            SELECT
                marketcodename,
                cpugen,
                guid,
                guid_on,
                guid_off,
                guid_mods,
                guid_sleep,
                (guid_on + guid_off + guid_mods + guid_sleep) AS guid_total
            FROM per_guid
        )
        SELECT
            marketcodename,
            cpugen,
            COUNT(*) AS number_of_systems,
            ROUND(AVG(guid_on), 2) AS avg_on_time,
            ROUND(AVG(guid_off), 2) AS avg_off_time,
            ROUND(AVG(guid_mods), 2) AS avg_modern_sleep_time,
            ROUND(AVG(guid_sleep), 2) AS avg_sleep_time,
            ROUND(AVG(guid_total), 2) AS avg_total_time,
            ROUND(SUM(guid_on)   * 100.0 / NULLIF(SUM(guid_total), 0), 2) AS avg_pcnt_on_time,
            ROUND(SUM(guid_off)  * 100.0 / NULLIF(SUM(guid_total), 0), 2) AS avg_pcnt_off_time,
            ROUND(SUM(guid_mods) * 100.0 / NULLIF(SUM(guid_total), 0), 2) AS avg_pcnt_mods_time,
            ROUND(SUM(guid_sleep)* 100.0 / NULLIF(SUM(guid_total), 0), 2) AS avg_pcnt_sleep_time
        FROM per_guid_total
        GROUP BY marketcodename, cpugen
        HAVING COUNT(*) >= {K_MIN_100}
    """),

    # -------------------------------------------------------------------------
    # Q8: Persona Web Category Usage Analysis
    # Already per-guid in original inner query; we add clipping on totals
    # -------------------------------------------------------------------------
    8: ("Persona Web Category Usage Analysis", f"""
        SELECT
            persona,
            COUNT(DISTINCT guid) AS number_of_systems,
            SUM(days) AS days,
            ROUND(100 * SUM(days * content_creation_photo_edit_creation / total_duration) / SUM(days), 3) AS content_creation_photo_edit_creation,
            ROUND(100 * SUM(days * content_creation_video_audio_edit_creation / total_duration) / SUM(days), 3) AS content_creation_video_audio_edit_creation,
            ROUND(100 * SUM(days * content_creation_web_design_development / total_duration) / SUM(days), 3) AS content_creation_web_design_development,
            ROUND(100 * SUM(days * education / total_duration) / SUM(days), 3) AS education,
            ROUND(100 * SUM(days * entertainment_music_audio_streaming / total_duration) / SUM(days), 3) AS entertainment_music_audio_streaming,
            ROUND(100 * SUM(days * entertainment_other / total_duration) / SUM(days), 3) AS entertainment_other,
            ROUND(100 * SUM(days * entertainment_video_streaming / total_duration) / SUM(days), 3) AS entertainment_video_streaming,
            ROUND(100 * SUM(days * finance / total_duration) / SUM(days), 3) AS finance,
            ROUND(100 * SUM(days * games_other / total_duration) / SUM(days), 3) AS games_other,
            ROUND(100 * SUM(days * games_video_games / total_duration) / SUM(days), 3) AS games_video_games,
            ROUND(100 * SUM(days * mail / total_duration) / SUM(days), 3) AS mail,
            ROUND(100 * SUM(days * news / total_duration) / SUM(days), 3) AS news,
            ROUND(100 * SUM(days * unclassified / total_duration) / SUM(days), 3) AS unclassified,
            ROUND(100 * SUM(days * private / total_duration) / SUM(days), 3) AS private,
            ROUND(100 * SUM(days * productivity_crm / total_duration) / SUM(days), 3) AS productivity_crm,
            ROUND(100 * SUM(days * productivity_other / total_duration) / SUM(days), 3) AS productivity_other,
            ROUND(100 * SUM(days * productivity_presentations / total_duration) / SUM(days), 3) AS productivity_presentations,
            ROUND(100 * SUM(days * productivity_programming / total_duration) / SUM(days), 3) AS productivity_programming,
            ROUND(100 * SUM(days * productivity_project_management / total_duration) / SUM(days), 3) AS productivity_project_management,
            ROUND(100 * SUM(days * productivity_spreadsheets / total_duration) / SUM(days), 3) AS productivity_spreadsheets,
            ROUND(100 * SUM(days * productivity_word_processing / total_duration) / SUM(days), 3) AS productivity_word_processing,
            ROUND(100 * SUM(days * recreation_travel / total_duration) / SUM(days), 3) AS recreation_travel,
            ROUND(100 * SUM(days * reference / total_duration) / SUM(days), 3) AS reference,
            ROUND(100 * SUM(days * search / total_duration) / SUM(days), 3) AS search,
            ROUND(100 * SUM(days * shopping / total_duration) / SUM(days), 3) AS shopping,
            ROUND(100 * SUM(days * social_social_network / total_duration) / SUM(days), 3) AS social_social_network,
            ROUND(100 * SUM(days * social_communication / total_duration) / SUM(days), 3) AS social_communication,
            ROUND(100 * SUM(days * social_communication_live / total_duration) / SUM(days), 3) AS social_communication_live
        FROM (
            SELECT
                a.guid,
                a.persona,
                LEAST(COUNT(*), {CAP_DAYS_PER_GUID}) AS days,
                LEAST(SUM(b.content_creation_photo_edit_creation), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS content_creation_photo_edit_creation,
                LEAST(SUM(b.content_creation_video_audio_edit_creation), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS content_creation_video_audio_edit_creation,
                LEAST(SUM(b.content_creation_web_design_development), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS content_creation_web_design_development,
                LEAST(SUM(b.education), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS education,
                LEAST(SUM(b.entertainment_music_audio_streaming), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS entertainment_music_audio_streaming,
                LEAST(SUM(b.entertainment_other), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS entertainment_other,
                LEAST(SUM(b.entertainment_video_streaming), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS entertainment_video_streaming,
                LEAST(SUM(b.finance), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS finance,
                LEAST(SUM(b.games_other), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS games_other,
                LEAST(SUM(b.games_video_games), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS games_video_games,
                LEAST(SUM(b.mail), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS mail,
                LEAST(SUM(b.news), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS news,
                LEAST(SUM(b.unclassified), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS unclassified,
                LEAST(SUM(b.private), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS private,
                LEAST(SUM(b.productivity_crm), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_crm,
                LEAST(SUM(b.productivity_other), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_other,
                LEAST(SUM(b.productivity_presentations), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_presentations,
                LEAST(SUM(b.productivity_programming), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_programming,
                LEAST(SUM(b.productivity_project_management), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_project_management,
                LEAST(SUM(b.productivity_spreadsheets), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_spreadsheets,
                LEAST(SUM(b.productivity_word_processing), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS productivity_word_processing,
                LEAST(SUM(b.recreation_travel), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS recreation_travel,
                LEAST(SUM(b.reference), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS reference,
                LEAST(SUM(b.search), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS search,
                LEAST(SUM(b.shopping), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS shopping,
                LEAST(SUM(b.social_social_network), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS social_social_network,
                LEAST(SUM(b.social_communication), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS social_communication,
                LEAST(SUM(b.social_communication_live), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS social_communication_live,
                LEAST(
                    SUM(
                        b.content_creation_video_audio_edit_creation +
                        b.content_creation_photo_edit_creation +
                        b.content_creation_web_design_development +
                        b.education +
                        b.entertainment_music_audio_streaming +
                        b.entertainment_other +
                        b.entertainment_video_streaming +
                        b.finance +
                        b.games_other +
                        b.games_video_games +
                        b.mail +
                        b.news +
                        b.unclassified +
                        b.private +
                        b.productivity_crm +
                        b.productivity_other +
                        b.productivity_presentations +
                        b.productivity_programming +
                        b.productivity_project_management +
                        b.productivity_spreadsheets +
                        b.productivity_word_processing +
                        b.recreation_travel +
                        b.reference +
                        b.search +
                        b.shopping +
                        b.social_social_network +
                        b.social_communication +
                        b.social_communication_live
                    ),
                    {CAP_BROWSER_DURATION_MS_PER_GUID}
                ) AS total_duration
            FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
            INNER JOIN reporting.system_web_cat_pivot_duration AS b
                ON a.guid = b.guid
            GROUP BY a.guid, a.persona
        ) AS inn
        WHERE total_duration > 0
        GROUP BY persona
        HAVING COUNT(DISTINCT guid) >= {K_MIN_100}
        ORDER BY number_of_systems DESC
    """),

    # -------------------------------------------------------------------------
    # Q9: Package Power by Country
    # Convert to per-guid average power (clipped) then average across guids
    # -------------------------------------------------------------------------
    9: ("Package Power by Country", f"""
        WITH per_guid AS (
            SELECT
                a.countryname_normalized AS countryname_normalized,
                b.guid,
                LEAST(SUM(b.nrs * b.mean) / NULLIF(SUM(b.nrs), 0), {CAP_POWER_WATTS}) AS guid_avg_pkg_power
            FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
            INNER JOIN reporting.system_hw_pkg_power AS b
                ON a.guid = b.guid
            GROUP BY a.countryname_normalized, b.guid
        )
        SELECT
            countryname_normalized,
            COUNT(*) AS number_of_systems,
            AVG(guid_avg_pkg_power) AS avg_pkg_power_consumed
        FROM per_guid
        GROUP BY countryname_normalized
        HAVING COUNT(*) >= {K_MIN_100}
        ORDER BY avg_pkg_power_consumed DESC
    """),

    # -------------------------------------------------------------------------
    # Q10: Popular Browsers by Count Usage Percentage (Structured)
    # - per-guid per-browser capped instances and duration
    # -------------------------------------------------------------------------
    10: ("Popular Browsers by Count Usage Percentage", f"""
        WITH per_guid AS (
            SELECT
                guid,
                browser,
                LEAST(COUNT(*), {CAP_BROWSER_INSTANCES_PER_GUID}) AS guid_instances,
                LEAST(SUM(duration_ms), {CAP_BROWSER_DURATION_MS_PER_GUID}) AS guid_duration
            FROM reporting.system_web_cat_usage
            WHERE browser IS NOT NULL
            GROUP BY guid, browser
        ),
        by_browser AS (
            SELECT
                browser,
                COUNT(*) AS num_systems,
                SUM(guid_instances) AS num_instances,
                SUM(guid_duration) AS sum_duration
            FROM per_guid
            GROUP BY browser
        ),
        totals AS (
            SELECT
                SUM(num_systems) AS total_systems,
                SUM(num_instances) AS tot_instances,
                SUM(sum_duration) AS tot_duration
            FROM by_browser
        )
        SELECT
            b.browser,
            ROUND(b.num_systems   * 100.0 / NULLIF(t.total_systems, 0), 2) AS percent_systems,
            ROUND(b.num_instances * 100.0 / NULLIF(t.tot_instances, 0), 2) AS percent_instances,
            ROUND(b.sum_duration  * 100.0 / NULLIF(t.tot_duration, 0), 2) AS percent_duration
        FROM by_browser b
        CROSS JOIN totals t
    """),

    # -------------------------------------------------------------------------
    # Q11: RAM Utilization Histogram
    # Convert to per-guid clipped percentage used, then average across guids
    # -------------------------------------------------------------------------
    11: ("RAM Utilization Histogram", f"""
        WITH per_guid AS (
            SELECT
                (sysinfo_ram / POWER(2, 10)) AS ram_gb,
                guid,
                LEAST(AVG(avg_percentage_used), {CAP_PERCENT}) AS guid_avg_pct_used
            FROM reporting.system_memory_utilization
            WHERE avg_percentage_used > 0
            GROUP BY sysinfo_ram, guid
        )
        SELECT
            ram_gb,
            COUNT(*) AS number_of_systems,
            ROUND(AVG(guid_avg_pct_used)) AS avg_percentage_used
        FROM per_guid
        GROUP BY ram_gb
        HAVING COUNT(*) >= {K_MIN_50}
        ORDER BY ram_gb ASC
    """),

    # -------------------------------------------------------------------------
    # Q12: Ranked Process Classifications
    # Clip per-user total power before ranking
    # -------------------------------------------------------------------------
    12: ("Ranked Process Classifications", f"""
        WITH per_user AS (
            SELECT
                user_id,
                LEAST(SUM(total_power_consumption), 100.0) AS total_power_consumption
            FROM reporting.system_mods_power_consumption
            GROUP BY user_id
        )
        SELECT
            user_id,
            total_power_consumption,
            RANK() OVER (ORDER BY total_power_consumption DESC) AS rnk
        FROM per_user
    """),
}


# =============================================================================
# MAIN EXPORT FUNCTION
# =============================================================================

def export_queries():
    """
    Export all queries to CSV files
    """
    print("=" * 70)
    print("BASELINE QUERY EXPORT")
    print("=" * 70)
    print(f"Database: {DATABASE_PATH}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Number of queries: {len(QUERIES)}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # Connect to database
    try:
        conn = duckdb.connect(DATABASE_PATH, read_only=True)
        print("✓ Connected to database\n")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        return
    
    # Track results
    successful = []
    failed = []
    
    # Execute each query
    for query_num in sorted(QUERIES.keys()):
        query_name, sql_query = QUERIES[query_num]
        
        # Convert query name to valid filename
        # Example: "Battery Power On Geographic Summary" -> "battery_power_on_geographic_summary.csv"
        filename = query_name.lower().replace(' ', '_').replace('/', '_') + '.csv'
        output_file = os.path.join(OUTPUT_DIR, filename)
        
        print(f"Query {query_num:02d}: {query_name}")
        print(f"  Output: {output_file}")
        
        try:
            # Execute query
            df = conn.execute(sql_query).fetchdf()
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            
            # Print summary
            print(f"  ✓ Success: {len(df):,} rows, {len(df.columns)} columns")
            successful.append(query_num)
            
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            failed.append((query_num, str(e)))
        
        print()
    
    # Close connection
    conn.close()
    
    # Print summary
    print("=" * 70)
    print("EXPORT SUMMARY")
    print("=" * 70)
    print(f"Total queries: {len(QUERIES)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print()
    
    if successful:
        print("✓ Successfully exported queries:")
        for qnum in successful:
            print(f"  - Query {qnum:02d}: {QUERIES[qnum][0]}")
        print()
    
    if failed:
        print("✗ Failed queries:")
        for qnum, error in failed:
            print(f"  - Query {qnum:02d}: {QUERIES[qnum][0]}")
            print(f"    Error: {error}")
        print()
    
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 70)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    export_queries()

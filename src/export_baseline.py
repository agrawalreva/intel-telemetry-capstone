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


# =============================================================================
# QUERY DEFINITIONS
# =============================================================================

# Dictionary of queries: {query_number: (query_name, sql_query)}
QUERIES = {
    1: ("Battery Power On Geographic Summary", """
        SELECT
            countryname_normalized AS country,
            COUNT(DISTINCT a.guid) AS number_of_systems,
            AVG(num_power_ons) AS avg_number_of_dc_powerons,
            AVG(duration_mins) AS avg_duration
        FROM reporting.system_batt_dc_events AS a
        INNER JOIN university_analysis_pad.system_sysinfo_unique_normalized AS b
            ON a.guid = b.guid
        GROUP BY countryname_normalized
        HAVING COUNT(DISTINCT a.guid) > 100
        ORDER BY avg_number_of_dc_powerons DESC
    """),
    
    2: ("Battery On Duration by CPU Family and Generation", """
        SELECT
            marketcodename,
            cpugen,
            COUNT(DISTINCT b.guid) AS number_of_systems,
            AVG(duration_mins) AS avg_duration_mins_on_battery
        FROM reporting.system_cpu_metadata AS a
        INNER JOIN reporting.system_batt_dc_events AS b
            ON a.guid = b.guid
        WHERE cpugen <> 'Unknown'
        GROUP BY marketcodename, cpugen
        HAVING COUNT(DISTINCT b.guid) > 100
    """),
    
    3: ("Display Devices Connection Type Resolution Durations", """
        SELECT
            connection_type,
            CAST(resolution_heigth AS VARCHAR) || 'x' || CAST(resolution_width AS VARCHAR) AS resolution,
            COUNT(DISTINCT guid) AS number_of_systems,
            ROUND(AVG(duration_ac), 2) AS average_duration_on_ac_in_seconds,
            ROUND(AVG(duration_dc), 2) AS average_duration_on_dc_in_seconds
        FROM reporting.system_display_devices
        WHERE connection_type IS NOT NULL
          AND resolution_heigth <> 0
          AND resolution_width <> 0
        GROUP BY connection_type, resolution_heigth, resolution_width
        HAVING COUNT(DISTINCT guid) > 50
        ORDER BY connection_type, number_of_systems DESC
    """),
    
    4: ("Display Devices Vendors Percentage", """
        SELECT
            vendor_name,
            COUNT(DISTINCT guid) AS number_of_systems,
            total_number_of_systems,
            ROUND(COUNT(DISTINCT guid) * 100.0 / total_number_of_systems, 2) AS percentage_of_systems
        FROM reporting.system_display_devices
        CROSS JOIN (
            SELECT COUNT(DISTINCT guid) AS total_number_of_systems
            FROM reporting.system_display_devices
        ) AS inn
        GROUP BY vendor_name, total_number_of_systems
        ORDER BY percentage_of_systems DESC
    """),
    
    5: ("MODS Blockers by OS Name and Codename", """
        SELECT
            os_name,
            os_codename,
            COUNT(*) AS num_entries,
            COUNT(DISTINCT guid) AS number_of_systems,
            CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT guid) AS entries_per_system
        FROM (
            SELECT a.guid, min_ts, max_ts, os_name, os_codename, dt_utc
            FROM reporting.system_mods_top_blocker_hist AS a
            INNER JOIN reporting.system_os_codename_history AS b
                ON a.guid = b.guid
            WHERE a.dt_utc BETWEEN b.min_ts AND b.max_ts
        ) AS inn
        GROUP BY os_name, os_codename
        HAVING COUNT(DISTINCT guid) > 10
    """),
    
    6: ("Most Popular Browser in Each Country", """
        SELECT
            country,
            browser
        FROM (
            SELECT
                countryname_normalized AS country,
                browser,
                COUNT(DISTINCT b.guid) AS number_of_systems,
                RANK() OVER (PARTITION BY countryname_normalized ORDER BY COUNT(DISTINCT b.guid) DESC) AS rnk
            FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
            INNER JOIN reporting.system_web_cat_usage AS b
                ON a.guid = b.guid
            GROUP BY countryname_normalized, browser
        ) AS x
        WHERE rnk = 1
    """),
    
    7: ("On Off MODS Sleep Summary by CPU", """
        SELECT
            b.marketcodename,
            b.cpugen,
            COUNT(DISTINCT a.guid) AS number_of_systems,
            ROUND(AVG(on_time), 2) AS avg_on_time,
            ROUND(AVG(off_time), 2) AS avg_off_time,
            ROUND(AVG(mods_time), 2) AS avg_modern_sleep_time,
            ROUND(AVG(sleep_time), 2) AS avg_sleep_time,
            ROUND(AVG(on_time + off_time + mods_time + sleep_time), 2) AS avg_total_time,
            ROUND(SUM(on_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_on_time,
            ROUND(SUM(off_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_off_time,
            ROUND(SUM(mods_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_mods_time,
            ROUND(SUM(sleep_time) * 100.0 / SUM(on_time + off_time + mods_time + sleep_time), 2) AS avg_pcnt_sleep_time
        FROM reporting.system_on_off_suspend_time_day AS a
        INNER JOIN reporting.system_cpu_metadata AS b
            ON a.guid = b.guid
        GROUP BY b.marketcodename, b.cpugen
        HAVING COUNT(DISTINCT a.guid) > 100
    """),
    
    8: ("Persona Web Category Usage Analysis", """
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
                COUNT(*) AS days,
                SUM(b.content_creation_photo_edit_creation) AS content_creation_photo_edit_creation,
                SUM(b.content_creation_video_audio_edit_creation) AS content_creation_video_audio_edit_creation,
                SUM(b.content_creation_web_design_development) AS content_creation_web_design_development,
                SUM(b.education) AS education,
                SUM(b.entertainment_music_audio_streaming) AS entertainment_music_audio_streaming,
                SUM(b.entertainment_other) AS entertainment_other,
                SUM(b.entertainment_video_streaming) AS entertainment_video_streaming,
                SUM(b.finance) AS finance,
                SUM(b.games_other) AS games_other,
                SUM(b.games_video_games) AS games_video_games,
                SUM(b.mail) AS mail,
                SUM(b.news) AS news,
                SUM(b.unclassified) AS unclassified,
                SUM(b.private) AS private,
                SUM(b.productivity_crm) AS productivity_crm,
                SUM(b.productivity_other) AS productivity_other,
                SUM(b.productivity_presentations) AS productivity_presentations,
                SUM(b.productivity_programming) AS productivity_programming,
                SUM(b.productivity_project_management) AS productivity_project_management,
                SUM(b.productivity_spreadsheets) AS productivity_spreadsheets,
                SUM(b.productivity_word_processing) AS productivity_word_processing,
                SUM(b.recreation_travel) AS recreation_travel,
                SUM(b.reference) AS reference,
                SUM(b.search) AS search,
                SUM(b.shopping) AS shopping,
                SUM(b.social_social_network) AS social_social_network,
                SUM(b.social_communication) AS social_communication,
                SUM(b.social_communication_live) AS social_communication_live,
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
                ) AS total_duration
            FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
            INNER JOIN reporting.system_web_cat_pivot_duration AS b
                ON a.guid = b.guid
            GROUP BY a.guid, a.persona
        ) AS inn
        GROUP BY persona
        ORDER BY number_of_systems DESC
    """),
    
    9: ("Package Power by Country", """
        SELECT
            a.countryname_normalized,
            COUNT(DISTINCT b.guid) AS number_of_systems,
            SUM(nrs * mean) / SUM(nrs) AS avg_pkg_power_consumed
        FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
        INNER JOIN reporting.system_hw_pkg_power AS b
            ON a.guid = b.guid
        GROUP BY a.countryname_normalized
        ORDER BY avg_pkg_power_consumed DESC
    """),
    
    10: ("Popular Browsers by Count Usage Percentage", """
        SELECT
            browser,
            ROUND(num_systems * 100.0 / total_systems, 2) AS percent_systems,
            ROUND(num_instances * 100.0 / tot_instances, 2) AS percent_instances,
            ROUND(sum_duration * 100.0 / tot_duration, 2) AS percent_duration
        FROM (
            SELECT browser,
                   COUNT(DISTINCT guid) AS num_systems,
                   COUNT(*) AS num_instances,
                   SUM(duration_ms) AS sum_duration
            FROM reporting.system_web_cat_usage
            GROUP BY browser
        ) AS a
        CROSS JOIN (
            SELECT COUNT(DISTINCT guid) AS total_systems,
                   COUNT(*) AS tot_instances,
                   SUM(duration_ms) AS tot_duration
            FROM reporting.system_web_cat_usage
        ) AS b
    """),
    
    11: ("RAM Utilization Histogram", """
        SELECT
            (sysinfo_ram / POWER(2, 10)) AS ram_gb,
            COUNT(DISTINCT guid) AS number_of_systems,
            ROUND(SUM(nrs * avg_percentage_used) / SUM(nrs)) AS avg_percentage_used
        FROM reporting.system_memory_utilization
        WHERE avg_percentage_used > 0
        GROUP BY sysinfo_ram
        ORDER BY sysinfo_ram ASC
    """),
    
    12: ("Ranked Process Classifications", """
        SELECT
            user_id,
            SUM(total_power_consumption) AS total_power_consumption,
            RANK() OVER (ORDER BY SUM(total_power_consumption) DESC) AS rnk
        FROM reporting.system_mods_power_consumption
        GROUP BY user_id
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

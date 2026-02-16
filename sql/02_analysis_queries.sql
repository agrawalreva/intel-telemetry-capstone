-- =========================
-- File: 02_analysis_queries.sql (DuckDB version - UPDATED WITH PROPER AS KEYWORDS)
-- Purpose: 22 benchmark queries for DP mechanism testing
-- All queries use ONLY tables from reporting.* schema
-- Updated: All table aliases now use AS keyword explicitly
-- =========================

-- ------------------------------------------------------------
-- Query 1: Battery Power On Geographic Summary
-- ------------------------------------------------------------
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
ORDER BY avg_number_of_dc_powerons DESC;

-- ------------------------------------------------------------
-- Query 2: Battery On Duration by CPU Family and Generation
-- ------------------------------------------------------------
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
HAVING COUNT(DISTINCT b.guid) > 100;

-- ------------------------------------------------------------
-- Query 3: Display Devices Connection Type Resolution Durations AC/DC
-- ------------------------------------------------------------
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
ORDER BY connection_type, number_of_systems DESC;

-- ------------------------------------------------------------
-- Query 4: Display Devices Vendors Percentage
-- ------------------------------------------------------------
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
ORDER BY percentage_of_systems DESC;

-- ------------------------------------------------------------
-- Query 5: MODS Blockers by OS Name and Codename
-- ------------------------------------------------------------
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
HAVING COUNT(DISTINCT guid) > 10;

-- ------------------------------------------------------------
-- Query 6: Most Popular Browser in Each Country by System Count
-- ------------------------------------------------------------
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
WHERE rnk = 1;

-- ------------------------------------------------------------
-- Query 7: On/Off MODS Sleep Summary by CPU Market Codename and Generation
-- ------------------------------------------------------------
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
HAVING COUNT(DISTINCT a.guid) > 100;

-- ------------------------------------------------------------
-- Query 8: Persona Web Category Usage Analysis
-- ------------------------------------------------------------
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
ORDER BY number_of_systems DESC;

-- ------------------------------------------------------------
-- Query 9: Package Power by Country
-- ------------------------------------------------------------
SELECT
    a.countryname_normalized,
    COUNT(DISTINCT b.guid) AS number_of_systems,
    SUM(nrs * mean) / SUM(nrs) AS avg_pkg_power_consumed
FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
INNER JOIN reporting.system_hw_pkg_power AS b
    ON a.guid = b.guid
GROUP BY a.countryname_normalized
ORDER BY avg_pkg_power_consumed DESC;

-- ------------------------------------------------------------
-- Query 10: Popular Browsers by Count Usage Percentage
-- ------------------------------------------------------------
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
) AS b;

-- ------------------------------------------------------------
-- Query 11: RAM Utilization Histogram
-- ------------------------------------------------------------
SELECT
    (sysinfo_ram / POWER(2, 10)) AS ram_gb,
    COUNT(DISTINCT guid) AS number_of_systems,
    ROUND(SUM(nrs * avg_percentage_used) / SUM(nrs)) AS avg_percentage_used
FROM reporting.system_memory_utilization
WHERE avg_percentage_used > 0
GROUP BY sysinfo_ram
ORDER BY sysinfo_ram ASC;

-- ------------------------------------------------------------
-- Query 12: Ranked Process Classifications
-- ------------------------------------------------------------
SELECT
    user_id,
    SUM(total_power_consumption) AS total_power_consumption,
    RANK() OVER (ORDER BY SUM(total_power_consumption) DESC) AS rnk
FROM reporting.system_mods_power_consumption
GROUP BY user_id;

-- ------------------------------------------------------------
-- Query 13: Server Exploration 1
-- ------------------------------------------------------------
SELECT
    a.guid,
    nrs,
    received_bytes,
    sent_bytes,
    b.chassistype,
    b.modelvendor_normalized AS vendor,
    b.model_normalized AS model,
    b.ram,
    b.os,
    b."#ofcores" AS number_of_cores
FROM (
    SELECT
        guid,
        SUM(nrs) AS nrs,
        SUM(CASE WHEN input_desc = 'OS:NETWORK INTERFACE::BYTES RECEIVED/SEC::' THEN avg_bytes_sec * nrs * 5 ELSE 0 END) AS received_bytes,
        SUM(CASE WHEN input_desc = 'OS:NETWORK INTERFACE::BYTES SENT/SEC::' THEN avg_bytes_sec * nrs * 5 ELSE 0 END) AS sent_bytes
    FROM reporting.system_network_consumption
    GROUP BY guid
    HAVING SUM(nrs) > 720
) AS a
INNER JOIN university_analysis_pad.system_sysinfo_unique_normalized AS b
    ON a.guid = b.guid
WHERE sent_bytes > received_bytes;

-- ------------------------------------------------------------
-- Query 14: Top 10 Applications by App Type Ranked by Focal Time
-- ------------------------------------------------------------
SELECT
    app_type,
    exe_name,
    ROUND(average_focal_sec_per_day) AS average_focal_sec_per_day,
    rank
FROM (
    SELECT
        app_type,
        exe_name,
        AVG(totalsecfocal_day) AS average_focal_sec_per_day,
        RANK() OVER (PARTITION BY app_type ORDER BY AVG(totalsecfocal_day) DESC) AS rank
    FROM reporting.system_frgnd_apps_types
    WHERE exe_name NOT IN ('restricted process', 'desktop')
      AND app_type IS NOT NULL
    GROUP BY app_type, exe_name
) AS inn
WHERE rank <= 10
ORDER BY app_type, rank ASC;

-- ------------------------------------------------------------
-- Query 15: Top 10 Applications by App Type Ranked by System Count
-- ------------------------------------------------------------
SELECT *
FROM (
    SELECT
        app_type,
        exe_name,
        COUNT(DISTINCT guid) AS number_of_systems,
        RANK() OVER (PARTITION BY app_type ORDER BY COUNT(DISTINCT guid) DESC) AS rank
    FROM reporting.system_frgnd_apps_types
    WHERE exe_name NOT IN ('restricted process', 'desktop')
      AND app_type IS NOT NULL
    GROUP BY app_type, exe_name
) AS inn
WHERE rank <= 10
ORDER BY app_type, rank ASC;

-- ------------------------------------------------------------
-- Query 16: Top 10 Applications by App Type Ranked by Total Detections
-- ------------------------------------------------------------
SELECT
    app_type,
    exe_name,
    total_num_detections AS total_number_of_detections,
    rank
FROM (
    SELECT
        app_type,
        exe_name,
        SUM(lines_per_day) AS total_num_detections,
        RANK() OVER (PARTITION BY app_type ORDER BY SUM(lines_per_day) DESC) AS rank
    FROM reporting.system_frgnd_apps_types
    WHERE exe_name NOT IN ('restricted process', 'desktop')
      AND app_type IS NOT NULL
    GROUP BY app_type, exe_name
) AS inn
WHERE rank <= 10
ORDER BY app_type, rank ASC;

-- ------------------------------------------------------------
-- Query 17: Top 10 Processes per User ID Ranked by Total Power Consumption
-- ------------------------------------------------------------
SELECT
    user_id,
    app_id,
    total_power_consumption,
    rnk
FROM (
    SELECT
        user_id,
        app_id,
        SUM(total_power_consumption) AS total_power_consumption,
        RANK() OVER (PARTITION BY user_id ORDER BY SUM(total_power_consumption) DESC) AS rnk
    FROM reporting.system_mods_power_consumption
    GROUP BY user_id, app_id
) AS x
WHERE rnk <= 10;

-- ------------------------------------------------------------
-- Query 18: Top 20 Most Power Consuming Processes by Avg Power Consumed
-- ------------------------------------------------------------
SELECT
    app_id,
    total_power_consumption,
    rnk
FROM (
    SELECT
        app_id,
        AVG(total_power_consumption) AS total_power_consumption,
        RANK() OVER (ORDER BY SUM(total_power_consumption) DESC) AS rnk
    FROM reporting.system_mods_power_consumption
    GROUP BY app_id
) AS x
WHERE rnk <= 20;

-- ------------------------------------------------------------
-- Query 19: Top MODS Blocker Types Durations by OS Name and Codename
-- ------------------------------------------------------------
SELECT
    os_name,
    os_codename,
    blocker_name,
    blocker_type,
    activity_level,
    COUNT(DISTINCT guid) AS number_of_clients,
    AVG(active_time_sec) AS average_active_time_in_seconds,
    COUNT(*) AS number_of_occurences
FROM (
    SELECT
        a.guid,
        blocker_name,
        active_time_ms / 1000.0 AS active_time_sec,
        activity_level,
        blocker_type,
        os_name,
        os_codename
    FROM reporting.system_mods_top_blocker_hist AS a
    INNER JOIN reporting.system_os_codename_history AS b
        ON a.guid = b.guid
       AND a.dt_utc BETWEEN b.min_ts AND b.max_ts
    WHERE active_time_ms > 0
) AS inn
GROUP BY os_name, os_codename, blocker_name, blocker_type, activity_level;

-- ------------------------------------------------------------
-- Query 20: UserWait Top 10 Wait Processes
-- ------------------------------------------------------------
SELECT
    proc_name,
    total_duration_sec_per_instance,
    rank
FROM (
    SELECT
        proc_name,
        SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) AS total_duration_sec_per_instance,
        RANK() OVER (ORDER BY SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) DESC) AS rank
    FROM reporting.system_userwait
    WHERE proc_name NOT IN ('DUMMY_PROCESS', 'DESKTOP', 'explorer.exe', 'RESTRICTED PROCESS', 'UNKNOWN')
      AND event_name NOT IN ('TOTAL_NON_WAIT_EVENTS', 'TOTAL_DISCARDED_WAIT_EVENTS')
    GROUP BY proc_name
) AS inn
WHERE rank <= 10
ORDER BY rank ASC;

-- ------------------------------------------------------------
-- Query 21: UserWait Top 10 Wait Processes by Wait Type AC/DC
-- ------------------------------------------------------------
SELECT
    event_name,
    acdc,
    proc_name,
    ROUND(total_duration_sec_per_instance, 2) AS total_duration_sec_per_instance,
    rank
FROM (
    SELECT
        event_name,
        acdc,
        proc_name,
        SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) AS total_duration_sec_per_instance,
        RANK() OVER (
            PARTITION BY event_name, acdc
            ORDER BY SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) DESC
        ) AS rank
    FROM reporting.system_userwait
    WHERE proc_name NOT IN ('DUMMY_PROCESS', 'DESKTOP', 'explorer.exe', 'RESTRICTED PROCESS', 'UNKNOWN')
      AND event_name NOT IN ('TOTAL_NON_WAIT_EVENTS', 'TOTAL_DISCARDED_WAIT_EVENTS')
    GROUP BY event_name, acdc, proc_name
) AS inn
WHERE rank <= 10
ORDER BY acdc, event_name, rank ASC;

-- ------------------------------------------------------------
-- Query 22: UserWait Top 20 Wait Processes Comparing AC/DC/Unknown Durations
-- ------------------------------------------------------------
SELECT
    proc_name,
    SUM(CASE WHEN acdc = 'AC' THEN ROUND(aggragated_duration_in_seconds / number_of_instances, 2) ELSE 0 END) AS ac_duration,
    SUM(CASE WHEN acdc = 'DC' THEN ROUND(aggragated_duration_in_seconds / number_of_instances, 2) ELSE 0 END) AS dc_duration,
    SUM(CASE WHEN acdc = 'UN' THEN ROUND(aggragated_duration_in_seconds / number_of_instances, 2) ELSE 0 END) AS unknown_duration
FROM (
    SELECT
        procs.proc_name,
        uw.acdc,
        SUM(uw.number_of_instances) AS number_of_instances,
        SUM(uw.total_duration_ms / 1000.0) AS aggragated_duration_in_seconds,
        COUNT(DISTINCT uw.guid) AS number_of_systems
    FROM (
        SELECT
            proc_name,
            ROUND(total_duration_sec_per_instance, 2) AS total_duration_sec_per_instance,
            rank
        FROM (
            SELECT
                proc_name,
                SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) AS total_duration_sec_per_instance,
                RANK() OVER (ORDER BY SUM((total_duration_ms / 1000.0)) / SUM(number_of_instances) DESC) AS rank
            FROM reporting.system_userwait
            WHERE proc_name NOT IN ('DUMMY_PROCESS', 'DESKTOP', 'explorer.exe', 'RESTRICTED PROCESS', 'UNKNOWN')
              AND event_name NOT IN ('TOTAL_NON_WAIT_EVENTS', 'TOTAL_DISCARDED_WAIT_EVENTS')
            GROUP BY proc_name
            HAVING SUM(number_of_instances) > 50
               AND COUNT(DISTINCT guid) > 20
        ) AS inn
    ) AS procs
    INNER JOIN reporting.system_userwait AS uw
        ON procs.proc_name = uw.proc_name
    WHERE procs.rank <= 20
    GROUP BY procs.proc_name, uw.acdc
) AS a
GROUP BY proc_name
ORDER BY proc_name;

-- ------------------------------------------------------------
-- Query 23: Xeon Network Consumption
-- ------------------------------------------------------------
SELECT
    processor_class,
    os,
    COUNT(DISTINCT guid) AS number_of_systems,
    SUM(nrs * avg_bytes_received) / SUM(nrs) AS avg_bytes_received,
    SUM(nrs * avg_bytes_sent) / SUM(nrs) AS avg_bytes_sent
FROM (
    SELECT
        a.guid,
        a.os,
        SUM(b.nrs) AS nrs,
        CASE WHEN a.cpuname = 'Xeon' THEN 'Server Class' ELSE 'Non-Server Class' END AS processor_class,
        SUM(CASE WHEN b.input_desc = 'OS:NETWORK INTERFACE::BYTES RECEIVED/SEC::' THEN b.avg_bytes_sec * b.nrs * 5 ELSE 0 END) AS avg_bytes_received,
        SUM(CASE WHEN b.input_desc = 'OS:NETWORK INTERFACE::BYTES SENT/SEC::' THEN b.avg_bytes_sec * b.nrs * 5 ELSE 0 END) AS avg_bytes_sent
    FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
    INNER JOIN reporting.system_network_consumption AS b
        ON a.guid = b.guid
    GROUP BY a.guid, a.os, a.cpuname
) AS c
GROUP BY processor_class, os
ORDER BY processor_class, os;

-- ------------------------------------------------------------
-- Query 24: Average Platform Power, C0, Frequency, and Temperature by Chassis
-- ------------------------------------------------------------
SELECT
    a.chassistype,
    COUNT(DISTINCT a.guid) AS number_of_systems,
    SUM(b.nrs * b.avg_psys_rap_watts) / SUM(b.nrs) AS avg_psys_rap_watts,
    SUM(c.nrs * c.avg_pkg_c0) / SUM(c.nrs) AS avg_pkg_c0,
    SUM(d.nrs * d.avg_avg_freq_mhz) / SUM(d.nrs) AS avg_freq_mhz,
    SUM(e.nrs * e.avg_temp_centigrade) / SUM(e.nrs) AS avg_temp_centigrade
FROM university_analysis_pad.system_sysinfo_unique_normalized AS a
INNER JOIN reporting.system_psys_rap_watts AS b ON a.guid = b.guid
INNER JOIN reporting.system_pkg_C0 AS c ON a.guid = c.guid
INNER JOIN reporting.system_pkg_avg_freq_mhz AS d ON a.guid = d.guid
INNER JOIN reporting.system_pkg_temp_centigrade AS e ON a.guid = e.guid
GROUP BY a.chassistype;
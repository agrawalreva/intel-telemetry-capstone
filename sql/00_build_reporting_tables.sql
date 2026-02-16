-- =========================
-- File: 00_build_reporting_tables.sql (DuckDB version - COMPLETE)
-- Purpose: Build ALL 22 reporting tables (16 original + 6 missing from Layne)
-- Notes:
--  - Creates reporting schema
--  - Builds 16 original tables from scratchpad SQL
--  - Builds 6 missing tables from Layne's email instructions
--  - All queries converted from PostgreSQL to DuckDB syntax
-- =========================

-- Create the reporting schema
CREATE SCHEMA IF NOT EXISTS reporting;

-- ============================================================
-- PART 1: ORIGINAL 16 REPORTING TABLES (From Scratchpad SQL)
-- ============================================================

-- ============================================================
-- 1) DISPLAY DEVICES  (university_prod.display_devices -> reporting.system_display_devices)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_display_devices;

CREATE TABLE reporting.system_display_devices AS
SELECT
    CAST(load_ts AS TIMESTAMP) AS load_ts,
    guid,
    CAST(dt AS DATE) AS dt,
    CAST(ts AS TIMESTAMP) AS ts,
    display_id,
    adapter_id,
    port,
    sink_index,
    connection_type,
    vendor_name,
    status,
    resolution_width,
    resolution_heigth,
    refresh_rate,
    duration_ac,
    duration_dc
FROM university_prod.display_devices;

CREATE INDEX idx_display_devices_guid ON reporting.system_display_devices (guid);


-- ============================================================
-- 2) USERWAIT (university_prod.userwait_v2 -> reporting.system_userwait)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_userwait;

CREATE TABLE reporting.system_userwait AS
SELECT
    guid,
    dt,
    event_name,
    ac_dc_event_name,
    UPPER(SUBSTRING(ac_dc_event_name, 1, 2)) AS acdc,
    proc_name_current AS proc_name,
    COUNT(*) AS number_of_instances,
    SUM(duration_ms) AS total_duration_ms
FROM university_prod.userwait_v2
GROUP BY guid, dt, event_name, ac_dc_event_name, proc_name_current;

CREATE INDEX idx_userwait_guid_dt ON reporting.system_userwait (guid, dt);


-- ============================================================
-- 3) HW PACKAGE POWER (university_prod.hw_pack_run_avg_pwr -> reporting.system_hw_pkg_power)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_hw_pkg_power;

CREATE TABLE reporting.system_hw_pkg_power AS
SELECT
    load_ts,
    guid,
    dt,
    instance,
    nrs,
    mean,
    rap_22 AS max
FROM university_prod.hw_pack_run_avg_pwr;

CREATE INDEX idx_hw_pkg_power_guid_dt ON reporting.system_hw_pkg_power (guid, dt);


-- ============================================================
-- 4) PKG C0  (university_prod.power_acdc_usage_v4_hist metric -> reporting.system_pkg_C0)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_pkg_C0;

CREATE TABLE reporting.system_pkg_C0 AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    event_name,
    SUM(nrs) AS nrs,
    MAX(CAST(attribute_metric_level1 AS SMALLINT)) AS number_of_cores,
    MAX(duration_ms) AS duration_ms,
    SUM(nrs * min_val) / SUM(nrs) AS min_pkg_c0,
    SUM(nrs * avg_val) / SUM(nrs) AS avg_pkg_c0,
    SUM(nrs * percentile_50th) / SUM(nrs) AS med_pkg_c0,
    SUM(nrs * max_val) / SUM(nrs) AS max_pkg_c0
FROM university_prod.power_acdc_usage_v4_hist
WHERE metric_name = 'HW::CORE:C0:PERCENT:'
GROUP BY guid, dt, event_name;

CREATE INDEX idx_system_pkg_c0_guid_dt ON reporting.system_pkg_C0 (guid, dt);


-- ============================================================
-- 5) PKG AVG FREQ (university_prod.power_acdc_usage_v4_hist metric -> reporting.system_pkg_avg_freq_mhz)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_pkg_avg_freq_mhz;

CREATE TABLE reporting.system_pkg_avg_freq_mhz AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    event_name,
    MAX(nrs) AS nrs,
    MAX(core) AS number_of_cores,
    MAX(duration_ms) AS duration_ms,
    AVG(min_avg_freq_mhz) AS min_avg_freq_mhz,
    AVG(avg_avg_freq_mhz) AS avg_avg_freq_mhz,
    AVG(med_avg_freq_mhz) AS med_avg_freq_mhz,
    AVG(max_avg_freq_mhz) AS max_avg_freq_mhz
FROM (
    SELECT
        guid,
        dt,
        event_name,
        CAST(attribute_metric_level1 AS SMALLINT) AS core,
        SUM(nrs) AS nrs,
        SUM(duration_ms) AS duration_ms,
        SUM(nrs * min_val) / SUM(nrs) AS min_avg_freq_mhz,
        SUM(nrs * avg_val) / SUM(nrs) AS avg_avg_freq_mhz,
        SUM(nrs * percentile_50th) / SUM(nrs) AS med_avg_freq_mhz,
        SUM(nrs * max_val) / SUM(nrs) AS max_avg_freq_mhz
    FROM university_prod.power_acdc_usage_v4_hist
    WHERE metric_name = 'HW::CORE:AVG_FREQ:MHZ:'
    GROUP BY guid, dt, event_name, attribute_metric_level1
) inn
GROUP BY guid, dt, event_name;

CREATE INDEX idx_system_pkg_avg_freq_mhz_guid_dt ON reporting.system_pkg_avg_freq_mhz (guid, dt);


-- ============================================================
-- 6) PKG TEMP  (university_prod.power_acdc_usage_v4_hist metric -> reporting.system_pkg_temp_centigrade)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_pkg_temp_centigrade;

CREATE TABLE reporting.system_pkg_temp_centigrade AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    event_name,
    MAX(nrs) AS nrs,
    MAX(core) AS number_of_cores,
    MAX(duration_ms) AS duration_ms,
    AVG(min_temp_centigrade) AS min_temp_centigrade,
    AVG(avg_temp_centigrade) AS avg_temp_centigrade,
    AVG(med_temp_centigrade) AS med_temp_centigrade,
    AVG(max_temp_centigrade) AS max_temp_centigrade
FROM (
    SELECT
        guid,
        dt,
        event_name,
        CAST(attribute_metric_level1 AS SMALLINT) AS core,
        SUM(nrs) AS nrs,
        SUM(duration_ms) AS duration_ms,
        SUM(nrs * min_val) / SUM(nrs) AS min_temp_centigrade,
        SUM(nrs * avg_val) / SUM(nrs) AS avg_temp_centigrade,
        SUM(nrs * percentile_50th) / SUM(nrs) AS med_temp_centigrade,
        SUM(nrs * max_val) / SUM(nrs) AS max_temp_centigrade
    FROM university_prod.power_acdc_usage_v4_hist
    WHERE metric_name = 'HW::CORE:TEMPERATURE:CENTIGRADE:'
    GROUP BY guid, dt, event_name, attribute_metric_level1
) inn
GROUP BY guid, dt, event_name;

CREATE INDEX idx_system_pkg_temp_centigrade_guid_dt ON reporting.system_pkg_temp_centigrade (guid, dt);


-- ============================================================
-- 7) PSYS RAP WATTS  (university_prod.power_acdc_usage_v4_hist metric -> reporting.system_psys_rap_watts)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_psys_rap_watts;

CREATE TABLE reporting.system_psys_rap_watts AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    event_name,
    SUM(nrs) AS nrs,
    SUM(duration_ms) AS duration_ms,
    SUM(nrs * min_val) / SUM(nrs) AS min_psys_rap_watts,
    SUM(nrs * avg_val) / SUM(nrs) AS avg_psys_rap_watts,
    SUM(nrs * percentile_50th) / SUM(nrs) AS med_psys_rap_watts,
    SUM(nrs * max_val) / SUM(nrs) AS max_psys_rap_watts
FROM university_prod.power_acdc_usage_v4_hist
WHERE metric_name = 'HW:::PSYS_RAP:WATTS:'
GROUP BY guid, dt, event_name;

CREATE INDEX idx_system_psys_rap_watts_guid_dt ON reporting.system_psys_rap_watts (guid, dt);


-- ============================================================
-- 8) WEB CATEGORY PIVOT  (university_prod.web_cat_pivot -> reporting.system_web_cat_pivot)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_pivot;

CREATE TABLE reporting.system_web_cat_pivot AS
SELECT * FROM university_prod.web_cat_pivot;

CREATE INDEX idx_system_web_cat_pivot_guid ON reporting.system_web_cat_pivot (guid);


-- ============================================================
-- 9) NETWORK CONSUMPTION  (university_prod.os_network_consumption_v2 -> reporting.system_network_consumption)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_network_consumption;

CREATE TABLE reporting.system_network_consumption AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    input_description AS input_desc,
    SUM(nr_samples) AS nrs,
    MIN(min_bytes_sec) AS min_bytes_sec,
    SUM(nr_samples * avg_bytes_sec) / SUM(nr_samples) AS avg_bytes_sec,
    MAX(max_bytes_sec) AS max_bytes_sec
FROM university_prod.os_network_consumption_v2
GROUP BY guid, dt, input_description;

CREATE INDEX idx_system_network_consumption_guid ON reporting.system_network_consumption (guid);


-- ============================================================
-- 10) WEB CAT PIVOT DURATION
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_pivot_duration;

CREATE TABLE reporting.system_web_cat_pivot_duration AS
SELECT
    guid,
    dt,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'photo edit/creation' THEN duration_ms ELSE 0 END) AS content_creation_photo_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'video/audio edit/creation' THEN duration_ms ELSE 0 END) AS content_creation_video_audio_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'web design / development' THEN duration_ms ELSE 0 END) AS content_creation_web_design_development,
    SUM(CASE WHEN parent_category = 'education' THEN duration_ms ELSE 0 END) AS education,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'music / audio streaming' THEN duration_ms ELSE 0 END) AS entertainment_music_audio_streaming,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'other' THEN duration_ms ELSE 0 END) AS entertainment_other,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'video streaming' THEN duration_ms ELSE 0 END) AS entertainment_video_streaming,
    SUM(CASE WHEN parent_category = 'finance' THEN duration_ms ELSE 0 END) AS finance,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'other' THEN duration_ms ELSE 0 END) AS games_other,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'video games' THEN duration_ms ELSE 0 END) AS games_video_games,
    SUM(CASE WHEN parent_category = 'mail' THEN duration_ms ELSE 0 END) AS mail,
    SUM(CASE WHEN parent_category = 'news' THEN duration_ms ELSE 0 END) AS news,
    SUM(CASE WHEN parent_category = 'other' THEN duration_ms ELSE 0 END) AS unclassified,
    SUM(CASE WHEN parent_category = 'private' THEN duration_ms ELSE 0 END) AS private,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'crm' THEN duration_ms ELSE 0 END) AS productivity_crm,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'other' THEN duration_ms ELSE 0 END) AS productivity_other,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'presentations' THEN duration_ms ELSE 0 END) AS productivity_presentations,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'programming' THEN duration_ms ELSE 0 END) AS productivity_programming,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'project management' THEN duration_ms ELSE 0 END) AS productivity_project_management,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'spreadsheets' THEN duration_ms ELSE 0 END) AS productivity_spreadsheets,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'word processing' THEN duration_ms ELSE 0 END) AS productivity_word_processing,
    SUM(CASE WHEN parent_category = 'recreation' AND sub_category = 'travel' THEN duration_ms ELSE 0 END) AS recreation_travel,
    SUM(CASE WHEN parent_category = 'reference' THEN duration_ms ELSE 0 END) AS reference,
    SUM(CASE WHEN parent_category = 'search' THEN duration_ms ELSE 0 END) AS search,
    SUM(CASE WHEN parent_category = 'shopping' THEN duration_ms ELSE 0 END) AS shopping,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'social network' THEN duration_ms ELSE 0 END) AS social_social_network,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication' THEN duration_ms ELSE 0 END) AS social_communication,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication - live' THEN duration_ms ELSE 0 END) AS social_communication_live
FROM university_prod.web_cat_usage_v2
GROUP BY guid, dt;

CREATE INDEX idx_system_web_cat_pivot_duration_guid_dt ON reporting.system_web_cat_pivot_duration (guid, dt);


-- ============================================================
-- 11) WEB CAT PIVOT PAGE LOAD COUNT
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_pivot_page_load_count;

CREATE TABLE reporting.system_web_cat_pivot_page_load_count AS
SELECT
    guid, dt,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'photo edit/creation' THEN page_load_count ELSE 0 END) AS content_creation_photo_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'video/audio edit/creation' THEN page_load_count ELSE 0 END) AS content_creation_video_audio_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'web design / development' THEN page_load_count ELSE 0 END) AS content_creation_web_design_development,
    SUM(CASE WHEN parent_category = 'education' THEN page_load_count ELSE 0 END) AS education,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'music / audio streaming' THEN page_load_count ELSE 0 END) AS entertainment_music_audio_streaming,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'other' THEN page_load_count ELSE 0 END) AS entertainment_other,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'video streaming' THEN page_load_count ELSE 0 END) AS entertainment_video_streaming,
    SUM(CASE WHEN parent_category = 'finance' THEN page_load_count ELSE 0 END) AS finance,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'other' THEN page_load_count ELSE 0 END) AS games_other,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'video games' THEN page_load_count ELSE 0 END) AS games_video_games,
    SUM(CASE WHEN parent_category = 'mail' THEN page_load_count ELSE 0 END) AS mail,
    SUM(CASE WHEN parent_category = 'news' THEN page_load_count ELSE 0 END) AS news,
    SUM(CASE WHEN parent_category = 'other' THEN page_load_count ELSE 0 END) AS unclassified,
    SUM(CASE WHEN parent_category = 'private' THEN page_load_count ELSE 0 END) AS private,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'crm' THEN page_load_count ELSE 0 END) AS productivity_crm,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'other' THEN page_load_count ELSE 0 END) AS productivity_other,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'presentations' THEN page_load_count ELSE 0 END) AS productivity_presentations,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'programming' THEN page_load_count ELSE 0 END) AS productivity_programming,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'project management' THEN page_load_count ELSE 0 END) AS productivity_project_management,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'spreadsheets' THEN page_load_count ELSE 0 END) AS productivity_spreadsheets,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'word processing' THEN page_load_count ELSE 0 END) AS productivity_word_processing,
    SUM(CASE WHEN parent_category = 'recreation' AND sub_category = 'travel' THEN page_load_count ELSE 0 END) AS recreation_travel,
    SUM(CASE WHEN parent_category = 'reference' THEN page_load_count ELSE 0 END) AS reference,
    SUM(CASE WHEN parent_category = 'search' THEN page_load_count ELSE 0 END) AS search,
    SUM(CASE WHEN parent_category = 'shopping' THEN page_load_count ELSE 0 END) AS shopping,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'social network' THEN page_load_count ELSE 0 END) AS social_social_network,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication' THEN page_load_count ELSE 0 END) AS social_communication,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication - live' THEN page_load_count ELSE 0 END) AS social_communication_live
FROM university_prod.web_cat_usage_v2
GROUP BY guid, dt;

CREATE INDEX idx_system_web_cat_pivot_page_load_count_guid_dt ON reporting.system_web_cat_pivot_page_load_count (guid, dt);


-- ============================================================
-- 12) WEB CAT PIVOT PAGE VISIT COUNT
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_pivot_page_visit_count;

CREATE TABLE reporting.system_web_cat_pivot_page_visit_count AS
SELECT
    guid, dt,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'photo edit/creation' THEN page_visit_count ELSE 0 END) AS content_creation_photo_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'video/audio edit/creation' THEN page_visit_count ELSE 0 END) AS content_creation_video_audio_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'web design / development' THEN page_visit_count ELSE 0 END) AS content_creation_web_design_development,
    SUM(CASE WHEN parent_category = 'education' THEN page_visit_count ELSE 0 END) AS education,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'music / audio streaming' THEN page_visit_count ELSE 0 END) AS entertainment_music_audio_streaming,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'other' THEN page_visit_count ELSE 0 END) AS entertainment_other,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'video streaming' THEN page_visit_count ELSE 0 END) AS entertainment_video_streaming,
    SUM(CASE WHEN parent_category = 'finance' THEN page_visit_count ELSE 0 END) AS finance,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'other' THEN page_visit_count ELSE 0 END) AS games_other,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'video games' THEN page_visit_count ELSE 0 END) AS games_video_games,
    SUM(CASE WHEN parent_category = 'mail' THEN page_visit_count ELSE 0 END) AS mail,
    SUM(CASE WHEN parent_category = 'news' THEN page_visit_count ELSE 0 END) AS news,
    SUM(CASE WHEN parent_category = 'other' THEN page_visit_count ELSE 0 END) AS unclassified,
    SUM(CASE WHEN parent_category = 'private' THEN page_visit_count ELSE 0 END) AS private,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'crm' THEN page_visit_count ELSE 0 END) AS productivity_crm,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'other' THEN page_visit_count ELSE 0 END) AS productivity_other,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'presentations' THEN page_visit_count ELSE 0 END) AS productivity_presentations,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'programming' THEN page_visit_count ELSE 0 END) AS productivity_programming,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'project management' THEN page_visit_count ELSE 0 END) AS productivity_project_management,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'spreadsheets' THEN page_visit_count ELSE 0 END) AS productivity_spreadsheets,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'word processing' THEN page_visit_count ELSE 0 END) AS productivity_word_processing,
    SUM(CASE WHEN parent_category = 'recreation' AND sub_category = 'travel' THEN page_visit_count ELSE 0 END) AS recreation_travel,
    SUM(CASE WHEN parent_category = 'reference' THEN page_visit_count ELSE 0 END) AS reference,
    SUM(CASE WHEN parent_category = 'search' THEN page_visit_count ELSE 0 END) AS search,
    SUM(CASE WHEN parent_category = 'shopping' THEN page_visit_count ELSE 0 END) AS shopping,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'social network' THEN page_visit_count ELSE 0 END) AS social_social_network,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication' THEN page_visit_count ELSE 0 END) AS social_communication,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication - live' THEN page_visit_count ELSE 0 END) AS social_communication_live
FROM university_prod.web_cat_usage_v2
GROUP BY guid, dt;

CREATE INDEX idx_system_web_cat_pivot_page_visit_count_guid_dt ON reporting.system_web_cat_pivot_page_visit_count (guid, dt);


-- ============================================================
-- 13) WEB CAT PIVOT DOMAIN COUNT
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_pivot_domain_count;

CREATE TABLE reporting.system_web_cat_pivot_domain_count AS
SELECT
    guid, dt,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'photo edit/creation' THEN domain_count ELSE 0 END) AS content_creation_photo_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'video/audio edit/creation' THEN domain_count ELSE 0 END) AS content_creation_video_audio_edit_creation,
    SUM(CASE WHEN parent_category = 'content creation' AND sub_category = 'web design / development' THEN domain_count ELSE 0 END) AS content_creation_web_design_development,
    SUM(CASE WHEN parent_category = 'education' THEN domain_count ELSE 0 END) AS education,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'music / audio streaming' THEN domain_count ELSE 0 END) AS entertainment_music_audio_streaming,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'other' THEN domain_count ELSE 0 END) AS entertainment_other,
    SUM(CASE WHEN parent_category = 'entertainment' AND sub_category = 'video streaming' THEN domain_count ELSE 0 END) AS entertainment_video_streaming,
    SUM(CASE WHEN parent_category = 'finance' THEN domain_count ELSE 0 END) AS finance,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'other' THEN domain_count ELSE 0 END) AS games_other,
    SUM(CASE WHEN parent_category = 'games' AND sub_category = 'video games' THEN domain_count ELSE 0 END) AS games_video_games,
    SUM(CASE WHEN parent_category = 'mail' THEN domain_count ELSE 0 END) AS mail,
    SUM(CASE WHEN parent_category = 'news' THEN domain_count ELSE 0 END) AS news,
    SUM(CASE WHEN parent_category = 'other' THEN domain_count ELSE 0 END) AS unclassified,
    SUM(CASE WHEN parent_category = 'private' THEN domain_count ELSE 0 END) AS private,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'crm' THEN domain_count ELSE 0 END) AS productivity_crm,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'other' THEN domain_count ELSE 0 END) AS productivity_other,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'presentations' THEN domain_count ELSE 0 END) AS productivity_presentations,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'programming' THEN domain_count ELSE 0 END) AS productivity_programming,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'project management' THEN domain_count ELSE 0 END) AS productivity_project_management,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'spreadsheets' THEN domain_count ELSE 0 END) AS productivity_spreadsheets,
    SUM(CASE WHEN parent_category = 'productivity' AND sub_category = 'word processing' THEN domain_count ELSE 0 END) AS productivity_word_processing,
    SUM(CASE WHEN parent_category = 'recreation' AND sub_category = 'travel' THEN domain_count ELSE 0 END) AS recreation_travel,
    SUM(CASE WHEN parent_category = 'reference' THEN domain_count ELSE 0 END) AS reference,
    SUM(CASE WHEN parent_category = 'search' THEN domain_count ELSE 0 END) AS search,
    SUM(CASE WHEN parent_category = 'shopping' THEN domain_count ELSE 0 END) AS shopping,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'social network' THEN domain_count ELSE 0 END) AS social_social_network,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication' THEN domain_count ELSE 0 END) AS social_communication,
    SUM(CASE WHEN parent_category = 'social' AND sub_category = 'communication - live' THEN domain_count ELSE 0 END) AS social_communication_live
FROM university_prod.web_cat_usage_v2
GROUP BY guid, dt;

CREATE INDEX idx_system_web_cat_pivot_domain_count_guid_dt ON reporting.system_web_cat_pivot_domain_count (guid, dt);


-- ============================================================
-- 14) WEB CAT USAGE AGG  (university_prod.web_cat_usage_v2 -> reporting.system_web_cat_usage)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_web_cat_usage;

CREATE TABLE reporting.system_web_cat_usage AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    browser,
    parent_category,
    sub_category,
    SUM(duration_ms) AS duration_ms,
    SUM(page_load_count) AS page_load_count,
    SUM(site_count) AS site_count,
    SUM(domain_count) AS domain_count,
    SUM(page_visit_count) AS page_visit_count
FROM university_prod.web_cat_usage_v2
GROUP BY guid, dt, browser, parent_category, sub_category;

CREATE INDEX idx_system_web_cat_usage_guid_dt ON reporting.system_web_cat_usage (guid, dt);


-- ============================================================
-- 15) MEMORY UTILIZATION  (university_prod.os_memsam_avail_percent + sysinfo -> reporting.system_memory_utilization)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_memory_utilization;

CREATE TABLE reporting.system_memory_utilization AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    nrs,
    avg_free_memory AS avg_free_ram,
    sysinfo_ram,
    sysinfo_ram - avg_free_memory AS utilized_ram,
    ROUND((sysinfo_ram - avg_free_memory) * 100.0 / sysinfo_ram) AS avg_percentage_used
FROM (
    SELECT
        a.guid,
        a.dt,
        SUM(a.sample_count) AS nrs,
        SUM(a.sample_count * a.average) / SUM(a.sample_count) AS avg_free_memory,
        CAST(b.ram * POWER(2, 10) AS INTEGER) AS sysinfo_ram
    FROM university_prod.os_memsam_avail_percent a
    INNER JOIN university_analysis_pad.system_sysinfo_unique_normalized b
        ON a.guid = b.guid
    WHERE b.ram <> 0
    GROUP BY a.guid, a.dt, b.ram
) c;

CREATE INDEX idx_system_memory_utilization ON reporting.system_memory_utilization (guid, dt);


-- ============================================================
-- 16) MODS POWER CONSUMPTION  (analysis_pad.mods_sleepstudy_power_estimation_data_13wks -> reporting.system_mods_power_consumption)
-- ============================================================
DROP TABLE IF EXISTS reporting.system_mods_power_consumption;

CREATE TABLE reporting.system_mods_power_consumption AS
SELECT
    current_timestamp AS load_ts,
    guid,
    dt,
    app_id,
    user_id,
    CAST(SUM(cpu_power_consumption) AS INTEGER) AS cpu_power_consumption,
    CAST(SUM(display_power_consumption) AS INTEGER) AS display_power_consumption,
    CAST(SUM(disk_power_consumption) AS INTEGER) AS disk_power_consumption,
    CAST(SUM(mbb_power_consumption) AS INTEGER) AS mbb_power_consumption,
    CAST(SUM(network_power_consumption) AS INTEGER) AS network_power_consumption,
    CAST(SUM(soc_power_consumption) AS INTEGER) AS soc_power_consumption,
    CAST(SUM(loss_power_consumption) AS INTEGER) AS loss_power_consumption,
    CAST(SUM(other_power_consumption) AS INTEGER) AS other_power_consumption,
    CAST(SUM(total_power_consumption) AS INTEGER) AS total_power_consumption
FROM (
    SELECT
        guid,
        CAST(ts_local AS DATE) AS dt,
        regexp_replace(app_id, '^.*\\', '') AS app_id,
        user_id,
        cpu_power_consumption,
        display_power_consumption,
        disk_power_consumption,
        mbb_power_consumption,
        network_power_consumption,
        soc_power_consumption,
        loss_power_consumption,
        other_power_consumption,
        total_power_consumption
    FROM university_analysis_pad.mods_sleepstudy_power_estimation_data_13wks
) a
GROUP BY guid, dt, app_id, user_id;

CREATE INDEX idx_system_mods_power_consumption_guid_dt_app_id ON reporting.system_mods_power_consumption (guid, dt, app_id);


-- ============================================================
-- PART 2: MISSING 6 REPORTING TABLES (From Layne's Instructions)
-- ============================================================

-- ============================================================
-- 17) BATTERY DC EVENTS (university_analysis_pad.__tmp_batt_dc_events -> reporting.system_batt_dc_events)
-- Source: Aggregated from Swaathi's temp table to daily granularity
-- Uses NULLIF to handle -1 sentinel values
-- ============================================================
DROP TABLE IF EXISTS reporting.system_batt_dc_events;

CREATE TABLE reporting.system_batt_dc_events AS
SELECT
    load_ts,
    guid,
    CAST(power_on_dc_ts AS DATE) AS dt,
    SUM(duration_mins) AS duration_mins,
    MAX(NULLIF(power_on_battery_percent, -1)) AS max_power_on_battery_percent,
    MIN(NULLIF(power_on_battery_percent, -1)) AS min_power_on_battery_percent,
    AVG(NULLIF(power_on_battery_percent, -1)) AS avg_power_on_battery_percent,
    MAX(NULLIF(power_off_battery_percent, -1)) AS max_power_off_battery_percent,
    MIN(NULLIF(power_off_battery_percent, -1)) AS min_power_off_battery_percent,
    AVG(NULLIF(power_off_battery_percent, -1)) AS avg_power_off_battery_percent,
    COUNT(power_on_dc_ts) AS num_power_ons
FROM university_analysis_pad.__tmp_batt_dc_events
GROUP BY load_ts, guid, CAST(power_on_dc_ts AS DATE);

CREATE INDEX idx_batt_dc_events_guid_dt ON reporting.system_batt_dc_events (guid, dt);


-- ============================================================
-- 18) CPU METADATA (university_analysis_pad.system_cpu_metadata -> reporting.system_cpu_metadata)
-- Source: Copied verbatim from analysis_pad
-- ============================================================
DROP TABLE IF EXISTS reporting.system_cpu_metadata;

CREATE TABLE reporting.system_cpu_metadata AS
SELECT * 
FROM university_analysis_pad.system_cpu_metadata;

CREATE INDEX idx_cpu_metadata_guid ON reporting.system_cpu_metadata (guid);


-- ============================================================
-- 19) MODS TOP BLOCKER HIST (university_analysis_pad.mods_sleepstudy_top_blocker_hist -> reporting.system_mods_top_blocker_hist)
-- Source: Copied verbatim from analysis_pad
-- NOTE: Uses dt_utc instead of dt
-- ============================================================
DROP TABLE IF EXISTS reporting.system_mods_top_blocker_hist;

CREATE TABLE reporting.system_mods_top_blocker_hist AS
SELECT * 
FROM university_analysis_pad.mods_sleepstudy_top_blocker_hist;

CREATE INDEX idx_mods_top_blocker_hist_guid_dt ON reporting.system_mods_top_blocker_hist (guid, dt_utc);


-- ============================================================
-- 20) OS CODENAME HISTORY (university_analysis_pad.system_os_codename_history -> reporting.system_os_codename_history)
-- Source: Copied verbatim from analysis_pad
-- ============================================================
DROP TABLE IF EXISTS reporting.system_os_codename_history;

CREATE TABLE reporting.system_os_codename_history AS
SELECT * 
FROM university_analysis_pad.system_os_codename_history;

CREATE INDEX idx_os_codename_history_guid ON reporting.system_os_codename_history (guid);


-- ============================================================
-- 21) ON OFF SUSPEND TIME DAY (university_analysis_pad.guids_on_off_suspend_time_day -> reporting.system_on_off_suspend_time_day)
-- Source: Copied verbatim from analysis_pad
-- ============================================================
DROP TABLE IF EXISTS reporting.system_on_off_suspend_time_day;

CREATE TABLE reporting.system_on_off_suspend_time_day AS
SELECT * 
FROM university_analysis_pad.guids_on_off_suspend_time_day;

CREATE INDEX idx_on_off_suspend_time_day_guid_dt ON reporting.system_on_off_suspend_time_day (guid, dt);


-- ============================================================
-- 22) FOREGROUND APPS TYPES (university_analysis_pad.__tmp_fgnd_apps_date -> reporting.system_frgnd_apps_types)
-- Source: Copied verbatim from analysis_pad temp table
-- NOTE: Source table has known data quality issues (malformed rows) that were
--       handled during database load with ignore_errors=true in Python script
-- ============================================================
DROP TABLE IF EXISTS reporting.system_frgnd_apps_types;

CREATE TABLE reporting.system_frgnd_apps_types AS
SELECT * 
FROM university_analysis_pad.__tmp_fgnd_apps_date;

CREATE INDEX idx_frgnd_apps_types_guid ON reporting.system_frgnd_apps_types (guid);


-- ============================================================
-- BUILD COMPLETE
-- ============================================================
-- Total: 22 reporting tables created
-- - 16 original tables from scratchpad SQL
-- - 6 missing tables from Layne's instructions
-- All 22 benchmark queries should now work!
-- ============================================================
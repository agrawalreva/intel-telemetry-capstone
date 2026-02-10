import duckdb
import os
from pathlib import Path

# ============================================================================
# DATABASE NAME CONFIGURATION
# ============================================================================
# Change this name when sharing with your group
DATABASE_NAME = "data.duckdb"  # <--- CHANGE THIS NAME HERE
# ============================================================================

# Path to your database_tables folder
DATABASE_TABLES_PATH = r"D:\capstone data\database_tables"

# Connect to DuckDB (creates the database file if it doesn't exist)
conn = duckdb.connect(DATABASE_NAME)  # <--- DATABASE_NAME USED HERE

print(f"Creating database: {DATABASE_NAME}")
print("=" * 60)

# Create schemas
print("\n1. Creating schemas...")
conn.execute("CREATE SCHEMA IF NOT EXISTS university_analysis_pad;")
conn.execute("CREATE SCHEMA IF NOT EXISTS university_prod;")
conn.execute("CREATE SCHEMA IF NOT EXISTS reporting;")
print("   ✓ Schemas created: university_analysis_pad, university_prod, reporting")

# Define which tables go in which schema
# NOTE: Added missing tables for Layne's 6 reporting tables
schema_mapping = {
    'university_analysis_pad': [
        'data_dictionary',
        'data_dictionary_collector_il',
        'data_dictionary_collector_inputs',
        'data_dictionary_tables',
        'mods_sleepstudy_power_estimation_data_13wks',
        'mods_sleepstudy_scenario_instance_13wks',
        'mods_sleepstudy_top_blocker_hist',
        'system_sysinfo_unique_normalized',
        'system_cpu_metadata',
        'system_os_codename_history',
        'guids_on_off_suspend_time_day',
        '__tmp_batt_dc_events',
        '__tmp_fgnd_apps_date'
    ],
    'university_prod': [
        'display_devices',
        'hw_pack_run_avg_pwr',
        'os_memsam_avail_percent',
        'os_network_consumption_v2',
        'os_system_data',
        'os_system_gen_data',
        'power_acdc_usage_v4_hist',
        'userwait_v2',
        'web_cat_pivot',
        'web_cat_usage_v2'
    ]
}

# Tables that have .gz files instead of parquet
gz_tables = [
    'display_devices',
    'mods_sleepstudy_power_estimation_data_13wks',
    'mods_sleepstudy_scenario_instance_13wks',
    'mods_sleepstudy_top_blocker_hist',
    '__tmp_batt_dc_events',
    '__tmp_fgnd_apps_date',
    'guids_on_off_suspend_time_day',
    'system_cpu_metadata',
    'system_os_codename_history'
]

print("\n2. Loading tables into database...")
print("-" * 60)

total_tables = 0
successful_tables = 0
failed_tables = []

# Process each schema
for schema_name, table_list in schema_mapping.items():
    print(f"\n   Loading tables into schema: {schema_name}")
    
    for table_name in table_list:
        total_tables += 1
        table_path = os.path.join(DATABASE_TABLES_PATH, table_name)
        
        # Check if folder exists
        if not os.path.exists(table_path):
            print(f"   ✗ {table_name}: Folder not found at {table_path}")
            failed_tables.append((table_name, "Folder not found"))
            continue
        
        try:
            # Determine file type and load accordingly
            if table_name in gz_tables:
                # For .gz files, we need to check what's inside
                # Assuming they are CSV files (most common)
                gz_pattern = os.path.join(table_path, "*.gz")
                
                # Try to load as CSV from gz
                query = f"""
                CREATE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM read_csv_auto('{gz_pattern}', 
                                            compression='gzip',
                                            header=true);
                """
                conn.execute(query)
                
            else:
                # For parquet files
                parquet_pattern = os.path.join(table_path, "*.parquet")
                
                query = f"""
                CREATE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM read_parquet('{parquet_pattern}');
                """
                conn.execute(query)
            
            # Get row count to verify
            result = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}").fetchone()
            row_count = result[0]
            
            print(f"   ✓ {table_name}: Loaded successfully ({row_count:,} rows)")
            successful_tables += 1
            
        except Exception as e:
            print(f"   ✗ {table_name}: Failed to load - {str(e)}")
            failed_tables.append((table_name, str(e)))

# Summary
print("\n" + "=" * 60)
print("LOADING SUMMARY")
print("=" * 60)
print(f"Total tables attempted: {total_tables}")
print(f"Successfully loaded: {successful_tables}")
print(f"Failed: {len(failed_tables)}")

if failed_tables:
    print("\nFailed tables:")
    for table_name, error in failed_tables:
        print(f"  - {table_name}: {error}")

# List all tables in the database
print("\n" + "=" * 60)
print("TABLES IN DATABASE")
print("=" * 60)

for schema_name in ['university_analysis_pad', 'university_prod']:
    tables = conn.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name;
    """).fetchall()
    
    print(f"\n{schema_name}:")
    for table in tables:
        print(f"  - {table[0]}")

# Close connection
conn.close()

print("\n" + "=" * 60)
print(f"Database '{DATABASE_NAME}' created successfully!")
print(f"To use it: duckdb {DATABASE_NAME}")
print("=" * 60)
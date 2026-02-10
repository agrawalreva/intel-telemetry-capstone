import duckdb
import os
from pathlib import Path

# ============================================================================
# DATABASE NAME CONFIGURATION
# ============================================================================
# Change this name when sharing with your group
DATABASE_NAME = "data_mini.duckdb"  # <--- Mini version with max 200MB per table
# ============================================================================

# Path to your database_tables folder
DATABASE_TABLES_PATH = r"D:\capstone data\database_tables"

# Maximum size per table in bytes (200 MB)
MAX_SIZE_MB = 200
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024  # Convert to bytes

# Connect to DuckDB (creates the database file if it doesn't exist)
conn = duckdb.connect(DATABASE_NAME)

print(f"Creating database: {DATABASE_NAME}")
print(f"Max size per table: {MAX_SIZE_MB} MB")
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

def get_folder_size(folder_path):
    """Calculate total size of all files in a folder"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)
    return total_size

def format_size(size_bytes):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

print("\n2. Loading tables into database...")
print("-" * 60)

total_tables = 0
successful_tables = 0
failed_tables = []
sampled_tables = []

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
            # Calculate total size of files in the folder
            total_size = get_folder_size(table_path)
            print(f"   → {table_name}: Total file size = {format_size(total_size)}")
            
            # Determine file type and pattern
            if table_name in gz_tables:
                file_pattern = os.path.join(table_path, "*.gz")
                file_type = "gz"
            else:
                file_pattern = os.path.join(table_path, "*.parquet")
                file_type = "parquet"
            
            # Special handling for problematic tables
            # __tmp_fgnd_apps_date has malformed data - need to ignore errors
            ignore_errors = table_name == '__tmp_fgnd_apps_date'
            
            if ignore_errors:
                print(f"   ⚠ {table_name}: Known data quality issues - using ignore_errors=true")
            
            # Determine if we need to sample based on size
            if total_size > MAX_SIZE_BYTES:
                # Calculate sampling percentage based on size
                sample_percentage = (MAX_SIZE_BYTES / total_size) * 100
                
                print(f"   → {table_name}: Sampling {sample_percentage:.2f}% (target: {MAX_SIZE_MB} MB)")
                
                if file_type == "gz":
                    # Add ignore_errors for problematic CSV files
                    if ignore_errors:
                        query = f"""
                        CREATE TABLE {schema_name}.{table_name} AS 
                        SELECT * FROM read_csv_auto('{file_pattern}', 
                                                    compression='gzip',
                                                    header=true,
                                                    ignore_errors=true,
                                                    sample_size=-1)
                        USING SAMPLE {sample_percentage} PERCENT (bernoulli);
                        """
                    else:
                        query = f"""
                        CREATE TABLE {schema_name}.{table_name} AS 
                        SELECT * FROM read_csv_auto('{file_pattern}', 
                                                    compression='gzip',
                                                    header=true)
                        USING SAMPLE {sample_percentage} PERCENT (bernoulli);
                        """
                else:
                    query = f"""
                    CREATE TABLE {schema_name}.{table_name} AS 
                    SELECT * FROM read_parquet('{file_pattern}')
                    USING SAMPLE {sample_percentage} PERCENT (bernoulli);
                    """
                
                conn.execute(query)
                sampled_tables.append((table_name, format_size(total_size), f"~{MAX_SIZE_MB} MB"))
                
            else:
                # Load all data (less than MAX_SIZE_BYTES)
                print(f"   → {table_name}: Loading all data ({format_size(total_size)} ≤ {MAX_SIZE_MB} MB)")
                
                if file_type == "gz":
                    # Add ignore_errors for problematic CSV files
                    if ignore_errors:
                        query = f"""
                        CREATE TABLE {schema_name}.{table_name} AS 
                        SELECT * FROM read_csv_auto('{file_pattern}', 
                                                    compression='gzip',
                                                    header=true,
                                                    ignore_errors=true,
                                                    sample_size=-1);
                        """
                    else:
                        query = f"""
                        CREATE TABLE {schema_name}.{table_name} AS 
                        SELECT * FROM read_csv_auto('{file_pattern}', 
                                                    compression='gzip',
                                                    header=true);
                        """
                else:
                    query = f"""
                    CREATE TABLE {schema_name}.{table_name} AS 
                    SELECT * FROM read_parquet('{file_pattern}');
                    """
                
                conn.execute(query)
            
            # Get actual row count in the created table
            result = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}").fetchone()
            actual_row_count = result[0]
            
            print(f"   ✓ {table_name}: Loaded successfully ({actual_row_count:,} rows)")
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

if sampled_tables:
    print(f"\nSampled tables (original size → sampled size):")
    for table_name, original_size, sampled_size in sampled_tables:
        print(f"  - {table_name}: {original_size} → {sampled_size}")

if failed_tables:
    print("\nFailed tables:")
    for table_name, error in failed_tables:
        print(f"  - {table_name}: {error}")

# List all tables in the database with row counts
print("\n" + "=" * 60)
print("TABLES IN DATABASE")
print("=" * 60)

for schema_name in ['university_analysis_pad', 'university_prod']:
    print(f"\n{schema_name}:")
    
    tables = conn.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name;
    """).fetchall()
    
    for table in tables:
        table_name = table[0]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}").fetchone()[0]
        print(f"  - {table_name}: {row_count:,} rows")

# Close connection
conn.close()

print("\n" + "=" * 60)
print(f"Database '{DATABASE_NAME}' created successfully!")
print(f"To use it: duckdb {DATABASE_NAME}")
print("=" * 60)
import duckdb
import os
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

DATABASE_PATH = REPO_ROOT / "data.duckdb"

# Point this to wherever your raw source folders live
DATABASE_TABLES_PATH = r""# Due to confidentiality requirements, we are not permitted to share the path. but if you have access to the data, you can set this to the root folder containing the table subfolders.

conn = duckdb.connect(str(DATABASE_PATH))

print(f"Creating database: {DATABASE_PATH}")
print("=" * 60)

# Create schemas
print("\n1. Creating schemas...")
conn.execute("CREATE SCHEMA IF NOT EXISTS university_analysis_pad;")
conn.execute("CREATE SCHEMA IF NOT EXISTS university_prod;")
conn.execute("CREATE SCHEMA IF NOT EXISTS reporting;")
print("   ✓ Schemas created: university_analysis_pad, university_prod, reporting")

schema_mapping = {
    'university_analysis_pad': [
        # Due to confidentiality requirements, we are not permitted to share the tables.
    ],
    'university_prod': [
        # Due to confidentiality requirements, we are not permitted to share the tables.
    ]
}

gz_tables = [
    # Due to confidentiality requirements, we are not permitted to share the tables.
]

print("\n2. Loading tables into database...")
print("-" * 60)

total_tables = 0
successful_tables = 0
failed_tables = []

for schema_name, table_list in schema_mapping.items():
    print(f"\n   Loading tables into schema: {schema_name}")

    for table_name in table_list:
        total_tables += 1
        table_path = os.path.join(DATABASE_TABLES_PATH, table_name)

        if not os.path.exists(table_path):
            print(f"   ✗ {table_name}: Folder not found at {table_path}")
            failed_tables.append((table_name, "Folder not found"))
            continue

        try:
            # Drop if exists (optional but nice if you rerun)
            conn.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

            if table_name in gz_tables:
                gz_pattern = os.path.join(table_path, "*.gz")

                # ✅ FIX: special handling for malformed table
                if table_name == "__tmp_fgnd_apps_date":
                    print(f"   ⚠ {table_name}: using ignore_errors=true and sample_size=-1")
                    query = f"""
                    CREATE TABLE {schema_name}.{table_name} AS
                    SELECT * FROM read_csv_auto(
                        '{gz_pattern}',
                        compression='gzip',
                        header=true,
                        ignore_errors=true,
                        sample_size=-1
                    );
                    """
                else:
                    query = f"""
                    CREATE TABLE {schema_name}.{table_name} AS
                    SELECT * FROM read_csv_auto(
                        '{gz_pattern}',
                        compression='gzip',
                        header=true
                    );
                    """

                conn.execute(query)

            else:
                parquet_pattern = os.path.join(table_path, "*.parquet")
                query = f"""
                CREATE TABLE {schema_name}.{table_name} AS
                SELECT * FROM read_parquet('{parquet_pattern}');
                """
                conn.execute(query)

            row_count = conn.execute(
                f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
            ).fetchone()[0]

            print(f"   ✓ {table_name}: Loaded successfully ({row_count:,} rows)")
            successful_tables += 1

        except Exception as e:
            print(f"   ✗ {table_name}: Failed to load - {str(e)}")
            failed_tables.append((table_name, str(e)))

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
    for (tname,) in tables:
        print(f"  - {tname}")

conn.close()

print("\n" + "=" * 60)
print(f"Database '{DATABASE_PATH.name}' created successfully!")
print(f"Location: {DATABASE_PATH}")
print(f"To use it: duckdb {DATABASE_PATH}")
print("=" * 60)

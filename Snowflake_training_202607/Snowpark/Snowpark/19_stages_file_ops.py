# Demo 19: Stages & File Operations — upload, download, read files

from snowflake.snowpark.context import get_active_session

session = get_active_session()

# =====================================================================
# KEY CONCEPT: STAGES & FILE I/O 
# =====================================================================
# Stages are Snowflake's file storage locations. Three types:
#   - Internal (user/table/named) — stored within Snowflake
#   - External — points to S3, Azure Blob, or GCS
#
# Snowpark can:
#   session.file.put()   -> upload local file to a stage
#   session.file.get()   -> download file from stage to local
#   session.read.*       -> read staged files directly into DataFrames
# =====================================================================

DB = "SNOWFLAKE_TRAINING.TRAIN"

# --- READ CSV FROM STAGE ---
# session.read.option() sets file format options
# .csv() reads CSV files from a stage path
print("1. Reading CSV with schema inference:")
try:
    df_csv = (
        session.read
        .option("INFER_SCHEMA", True)
        .option("SKIP_HEADER", 1)
        .csv(f"@{DB}.SNOWFLAKE_TRAIN_STAGE")
    )
    df_csv.show(5)
except Exception as e:
    print(f"   [Skipped - stage may not have CSV files] {e}")

# --- READ JSON FROM STAGE ---
print("\n2. Reading JSON:")
try:
    df_json = session.read.json(f"@{DB}.SNOWFLAKE_TRAIN_STAGE")
    df_json.show(5)
except Exception as e:
    print(f"   [Skipped] {e}")

# --- LIST FILES IN STAGE ---
print("\n3. List files in a stage (using SQL):")
try:
    session.sql(f"LIST @{DB}.SNOWFLAKE_TRAIN_STAGE").show()
except Exception as e:
    print(f"   [Skipped] {e}")

# --- FILE.PUT / FILE.GET (conceptual) ---
print("""
4. Upload/Download (conceptual — requires local file system):

   # Upload a local file to stage:
   session.file.put(
       "/local/path/data.csv",
       "@MY_STAGE/folder/",
       auto_compress=False
   )

   # Download from stage to local:
   session.file.get(
       "@MY_STAGE/folder/data.csv",
       "/local/download/path/"
   )

   NOTE: put/get work when running Snowpark locally or in
   stored procedures — not directly in Workspaces.
""")

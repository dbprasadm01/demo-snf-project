# Demo 22: Temp vs Permanent objects, Write Modes, and Views

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark import Row

session = get_active_session()

DB = "SNOWFLAKE_TRAINING.TRAIN"

# =====================================================================
# KEY CONCEPT: TEMP vs PERMANENT + WRITE MODES (SnowPro Core)
# =====================================================================
# Snowpark can create different types of objects:
#
#   Permanent table   -> exists until explicitly dropped
#   Temporary table   -> exists only for the session duration
#   Transient table   -> permanent but no Fail-Safe (cheaper)
#   View              -> saved query (no data stored)
#   Temp View         -> session-scoped view
#
# Write modes control what happens if the table already exists.
# =====================================================================

# --- SAMPLE DATA ---
df = session.create_dataframe([
    Row(ID=1, CITY="Mumbai", TEMP_C=35),
    Row(ID=2, CITY="Delhi", TEMP_C=42),
    Row(ID=3, CITY="Bangalore", TEMP_C=28),
])

# --- 1. PERMANENT TABLE ---
print("1. PERMANENT TABLE (persists until dropped):")
df.write.mode("overwrite").save_as_table(f"{DB}.WEATHER_PERM")
session.table(f"{DB}.WEATHER_PERM").show()

# --- 2. TEMPORARY TABLE ---
print("\n2. TEMPORARY TABLE (gone after session ends):")
df.write.mode("overwrite").save_as_table(
    f"{DB}.WEATHER_TEMP",
    table_type="temporary"  # <-- key parameter
)
session.table(f"{DB}.WEATHER_TEMP").show()

# --- 3. WRITE MODES ---
print("""
3. WRITE MODES:

   .write.mode("overwrite")       -> Drop + recreate table
   .write.mode("append")          -> Add rows to existing table
   .write.mode("errorifexists")   -> Fail if table already exists
   .write.mode("ignore")          -> Do nothing if table exists
""")

# Demonstrate APPEND
extra_data = session.create_dataframe([
    Row(ID=4, CITY="Chennai", TEMP_C=38),
])
extra_data.write.mode("append").save_as_table(f"{DB}.WEATHER_PERM")
print("After APPEND (4 rows now):")
session.table(f"{DB}.WEATHER_PERM").show()

# --- 4. TEMPORARY VIEW ---
print("\n4. TEMPORARY VIEW (session-scoped, no data stored):")
hot_cities = df.filter(col("TEMP_C") > 30)
hot_cities.create_or_replace_temp_view("HOT_CITIES_VIEW")

# Query it with SQL
session.sql("SELECT * FROM HOT_CITIES_VIEW").show()

# --- 5. cache_result() ---
print("\n5. cache_result() (temp table for reuse within session):")
print("   - Materializes the DataFrame into a temp table")
print("   - Avoids re-computing expensive transformations")
print("   - Automatically cleaned up when session ends")
df_cached = df.filter(col("TEMP_C") > 30).cache_result()
print(f"   Cached row count: {df_cached.count()}")

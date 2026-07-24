# Demo 15: DataFrame caching and .explain() for query plans

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# =====================================================================
# KEY CONCEPT: CACHING & QUERY PLANS (important for SnowPro Core)
# =====================================================================
# .cache_result() materializes a DataFrame into a temp table so that
# subsequent operations don't re-execute the entire query chain.
#
# Without cache: each action re-runs ALL transformations from scratch.
# With cache:    result is stored once, reused multiple times.
#
# .explain() shows you the execution plan (like SQL EXPLAIN).
# =====================================================================

# --- BUILD A TRANSFORMATION CHAIN ---
df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")
df_transformed = df.filter(col("ID") > 2).select(col("ID"), col("NAME"), col("CATEGORY_ID"))

# --- VIEW THE QUERY PLAN ---
# This shows what SQL Snowpark would generate and how it's optimized.
print("QUERY PLAN (before cache):")
df_transformed.explain()

# --- CACHE THE RESULT ---
# Materializes into a temporary table — subsequent operations are fast.
df_cached = df_transformed.cache_result()

print("\n\nQUERY PLAN (after cache — reads from temp table):")
df_cached.explain()

# --- USE CACHED RESULT MULTIPLE TIMES (no re-computation) ---
print(f"\nCount from cache: {df_cached.count()}")
print("First 3 rows from cache:")
df_cached.limit(3).to_pandas()

# Demo 17: DataFrame operations — sort, limit, distinct, drop, union, sample

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: COMMON DATAFRAME OPERATIONS (important for SnowPro Core)
# =====================================================================
# These are the most frequently tested DataFrame methods:
#   .sort() / .order_by()   -> order rows
#   .limit()                -> take first N rows
#   .distinct()             -> remove duplicates
#   .drop()                 -> remove columns
#   .union() / .union_all() -> combine two DataFrames
#   .sample()               -> random sample of rows
#   .with_column()          -> add or replace a column
# =====================================================================

df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")

# --- SORT (ORDER BY) ---
print("1. SORT by ID descending:")
df.sort(col("ID").desc()).limit(5).show()

# --- DISTINCT ---
print("\n2. DISTINCT CATEGORY_IDs:")
df.select("CATEGORY_ID").distinct().show()

# --- DROP COLUMN ---
print("\n3. DROP columns (remove KEY and 3rd):")
df.drop("KEY", "3rd").limit(3).show()

# --- WITH_COLUMN (add/replace a column) ---
print("\n4. WITH_COLUMN — add a computed column:")
df_with = df.with_column("ID_DOUBLED", col("ID") * 2)
df_with.select("ID", "NAME", "ID_DOUBLED").limit(5).show()

# --- UNION ---
print("\n5. UNION — combine two DataFrames:")
df_a = session.create_dataframe([Row(X=1), Row(X=2)])
df_b = session.create_dataframe([Row(X=3), Row(X=4)])
df_a.union_all(df_b).show()

# --- SAMPLE ---
print("\n6. SAMPLE — random 50% of rows:")
df.sample(frac=0.5).select("ID", "NAME").show()

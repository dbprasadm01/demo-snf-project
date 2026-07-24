# Demo 9: Lazy Evaluation — Snowpark only executes when you trigger an action

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# =====================================================================
# KEY CONCEPT: LAZY EVALUATION (important for SnowPro Core)
# =====================================================================
# Snowpark uses "lazy evaluation" — when you call .filter(), .select(),
# .join(), etc., NO SQL runs yet. Snowpark just builds a query plan.
#
# SQL only executes when you call an ACTION method:
#   .show()          -> prints rows
#   .collect()       -> returns rows as Python list
#   .to_pandas()     -> returns a Pandas DataFrame
#   .count()         -> returns row count
#   .save_as_table() -> writes to Snowflake
#
# WHY? This lets Snowpark optimize the entire chain into a single SQL
# query instead of running multiple queries for each step.
# =====================================================================

# --- TRANSFORMATIONS (no SQL runs yet, just builds the plan) ---
df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")
df_filtered = df.filter(col("ID") > 5)
df_selected = df_filtered.select(col("ID"), col("NAME"))

# --- ACTION (THIS triggers SQL execution) ---
print("Row count (action triggers execution):", df_selected.count())

# --- VIEW THE GENERATED SQL ---
# .queries gives you the SQL that Snowpark will send to Snowflake.
# Great for debugging and understanding what happens behind the scenes.
print("\nGenerated SQL:")
for q in df_selected.queries["queries"]:
    print(q)

# --- DISPLAY RESULT ---
df_selected.to_pandas()

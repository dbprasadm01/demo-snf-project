# Demo 18: Action vs Transformation methods 


from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# =====================================================================
# KEY CONCEPT: ACTIONS vs TRANSFORMATIONS (SnowPro Core exam topic)
# =====================================================================
#
# TRANSFORMATIONS (lazy — build the plan, NO SQL executed):
#   .select()          .filter() / .where()
#   .group_by()        .sort() / .order_by()
#   .join()            .drop()
#   .with_column()     .union() / .union_all()
#   .distinct()        .limit()
#   .agg()             .sample()
#   .rename()          .pivot() / .unpivot()
#
# ACTIONS (eager — TRIGGER SQL execution):
#   .show()            .collect()
#   .to_pandas()       .count()
#   .first()           .save_as_table()
#   .copy_into_table() .cache_result()
#   .explain()         .queries (property)
#
# EXAM TIP: If someone asks "which method triggers execution?"
#           the answer is ACTIONS. Transformations are always lazy.
# =====================================================================

df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")

# --- TRANSFORMATIONS (nothing runs yet) ---
step1 = df.filter(col("CATEGORY_ID") == 5)    # lazy
step2 = step1.select("ID", "NAME")            # lazy
step3 = step2.sort(col("ID").desc())           # lazy

print("No SQL has run yet — just built a plan.")
print(f"Type of step3: {type(step3)}")         # still a DataFrame

# --- ACTION (now SQL runs!) ---
print("\nTriggering .collect() — SQL executes NOW:")
rows = step3.collect()                         # ACTION!
for row in rows:
    print(f"  ID={row['ID']}, NAME={row['NAME']}")

# --- .first() — another action, gets one row ---
print(f"\n.first() returns: {step3.first()}")

# --- .count() — another action ---
print(f".count() returns: {step3.count()}")

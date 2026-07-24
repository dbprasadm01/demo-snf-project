# Demo 6: Filter rows using column expressions


from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# --- LOAD TABLE ---
df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")

# --- FILTER ---
# col("ID") creates a reference to the "ID" column.
# == 1 creates a filter condition: keep only rows where ID equals 1.
# .filter() applies that condition (like SQL's WHERE clause).
df_filtered = df.filter(col("ID") == 1)

print("Filtered to ID == 1:")
df_filtered.show()

# --- MORE FILTER EXAMPLES ---
# You can chain filters or use other operators:
#   col("ID") > 5          -> greater than
#   col("NAME") == "X"     -> string equality
#   (col("ID") > 2) & (col("ID") < 6)  -> AND condition

df_range = df.filter((col("ID") >= 3) & (col("ID") <= 6))
print("\nFiltered to ID between 3 and 6:")
df_range.show()

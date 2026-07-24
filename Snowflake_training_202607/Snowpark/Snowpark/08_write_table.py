# Demo 8: Write a DataFrame to a Snowflake table


from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row

session = get_active_session()

# --- CREATE SAMPLE DATA ---
# First, let's build a small DataFrame from Row objects.
df = session.create_dataframe([
    Row(ID=1, PRODUCT="Widget", PRICE=9.99),
    Row(ID=2, PRODUCT="Gadget", PRICE=19.99),
    Row(ID=3, PRODUCT="Doohickey", PRICE=4.99),
])

print("DataFrame we're about to save:")
df.show()

# --- WRITE TO TABLE ---
# .write.mode("overwrite") means: if the table already exists, replace it.
# Other modes: "append" (add rows), "errorifexists" (fail if table exists).
# .save_as_table() creates (or replaces) a real Snowflake table.
df.write.mode("overwrite").save_as_table("SNOWFLAKE_TRAINING.TRAIN.DEMO_OUTPUT")

print("\nTable saved! Reading it back from Snowflake:")

# --- VERIFY ---
# Read the table back to confirm it was written correctly.
df_verify = session.table("SNOWFLAKE_TRAINING.TRAIN.DEMO_OUTPUT")
df_verify.show()

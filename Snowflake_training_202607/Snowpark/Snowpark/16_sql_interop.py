# Demo 16: SQL interop — run raw SQL from Snowpark and mix with DataFrames

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# =====================================================================
# KEY CONCEPT: SQL + SNOWPARK INTEROP (important for SnowPro Core)
# =====================================================================
# You don't have to choose between SQL and Snowpark — you can mix them.
#
# session.sql("...")  -> runs any SQL and returns a DataFrame
# .create_or_replace_view()  -> saves a DataFrame as a Snowflake VIEW
# .create_or_replace_temp_view() -> temporary view (session-scoped)
#
# Key exam points:
#   - session.sql() supports ANY valid SQL (DDL, DML, queries)
#   - The result is still a lazy DataFrame (no execution until action)
#   - You can chain Snowpark methods on top of session.sql() results
# =====================================================================

# --- RUN RAW SQL ---
# session.sql() can execute any SQL statement and returns a DataFrame.
print("1. Raw SQL query:")
df_sql = session.sql("""
    SELECT ID, NAME, CATEGORY_ID
    FROM SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA
    WHERE CATEGORY_ID > 5
    ORDER BY ID
""")
df_sql.show()

# --- CHAIN SNOWPARK METHODS ON SQL RESULT ---
# You can further filter/transform the SQL result with Snowpark methods.
print("\n2. SQL result + Snowpark .filter():")
df_chained = df_sql.filter(col("ID") > 7)
df_chained.show()

# --- CREATE A TEMPORARY VIEW ---
# Temp views exist only for your session — useful for intermediate results.
df_sql.create_or_replace_temp_view("MY_TEMP_VIEW")

# Now you can reference it in subsequent SQL
print("\n3. Query the temp view with SQL:")
session.sql("SELECT * FROM MY_TEMP_VIEW WHERE ID = 8").to_pandas()

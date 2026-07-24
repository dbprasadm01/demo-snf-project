# Demo 4: Create a DataFrame with an explicit schema (column names + types)

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

session = get_active_session()

# --- DEFINE SCHEMA ---
# StructType is like a blueprint that says:
#   "Column 'a' is an Integer, column 'b' is a String"
# This gives you full control over column names AND data types.
schema = StructType([
    StructField("a", IntegerType()),   # first column: integer
    StructField("b", StringType()),    # second column: string
])

# --- CREATE WITH SCHEMA ---
# Now we pass raw nested lists (no Row objects needed) — the schema
# tells Snowpark which value belongs to which column.
df = session.create_dataframe(
    [[1, "snow"], [3, "flake"], [5, "park"]],
    schema
)

# Useful when you need precise type control, e.g. ensuring a column
# is INTEGER and not accidentally treated as FLOAT.
df.show()

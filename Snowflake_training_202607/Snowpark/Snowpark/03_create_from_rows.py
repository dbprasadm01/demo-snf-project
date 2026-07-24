# Demo 3: Create a DataFrame using Row objects (named columns)


from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row

session = get_active_session()

# --- CREATE FROM ROW OBJECTS ---
# Row() lets you name each column explicitly.
# This is cleaner than passing raw lists when you have multiple columns.
# Each Row(...) becomes one row in the DataFrame.
df = session.create_dataframe([
    Row(a=1, b=2, c=3, d=4),
    Row(a=5, b=6, c=7, d=8),
    Row(a=9, b=10, c=11, d=12),
])

# The column names (a, b, c, d) come directly from the Row keyword arguments.
df.show()

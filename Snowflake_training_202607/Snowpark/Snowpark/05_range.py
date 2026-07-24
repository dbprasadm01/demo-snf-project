# Demo 5: Generate a DataFrame from a numeric range

from snowflake.snowpark.context import get_active_session

session = get_active_session()

# --- RANGE ---
# session.range(start, stop, step) works like Python's range() but
# produces a Snowpark DataFrame instead of a Python list.
#   start=1  -> first value is 1
#   stop=10  -> stop before 10 (exclusive)
#   step=2   -> increment by 2
# Result: 1, 3, 5, 7, 9
df = session.range(1, 10, 2).to_df("a")

# This is handy for generating test data or sequence numbers
# without needing an existing table.
df.show()

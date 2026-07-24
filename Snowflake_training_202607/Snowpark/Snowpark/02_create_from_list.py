# Demo 2: Create a DataFrame from a plain Python list


from snowflake.snowpark.context import get_active_session

session = get_active_session()

# --- CREATE FROM LIST ---
# session.create_dataframe() can turn a simple Python list into a DataFrame.
# Here we pass [1, 2, 3, 4] — a list of integers.
# .to_df("a") renames the auto-generated column to "a".
df = session.create_dataframe([1, 2, 3, 4]).to_df("a")

# This is useful when you have small datasets in Python that you want
# to bring into Snowpark for joining, filtering, or testing.
df.show()

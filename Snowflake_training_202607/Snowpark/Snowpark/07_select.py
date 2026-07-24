# Demo 7: Select specific columns and display as table using to_pandas()

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

# --- LOAD TABLE ---
df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")

# --- SELECT ---
# .select() picks only the columns you want (like SQL's SELECT clause).
# col("COLUMN_NAME") references a specific column.
df_selected = df.select(col("ID"), col("NAME"), col("SERIAL_NUMBER"))

# --- DISPLAY AS TABLE ---
# .to_pandas() converts the Snowpark DataFrame into a Pandas DataFrame.
# When it's the last expression in the file, the Workspace renders it
# as an interactive table (not plain text like .show()).
#
# .show()       -> prints plain text to stdout (good for logs/debugging)
# .to_pandas()  -> renders as a rich table in the output pane (good for demos)
df_selected.to_pandas()

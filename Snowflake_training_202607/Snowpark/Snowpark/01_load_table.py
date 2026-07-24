# Demo 1: Load an existing Snowflake table as a Snowpark DataFrame

# --- IMPORTS ---
# snowpark.context gives us get_active_session to connect to Snowflake
from snowflake.snowpark.context import get_active_session

# --- GET SESSION ---
# This grabs your existing Snowflake connection from the Workspace.
session = get_active_session()

# --- READ TABLE ---
# session.table() creates a DataFrame pointing to a Snowflake table.
# We use the fully qualified name: DATABASE.SCHEMA.TABLE
# because our session doesn't have a default database/schema set.
df = session.table("SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA")

# --- DISPLAY ---
# .show() prints the DataFrame contents in a formatted table.
# By default it shows up to 10 rows.
df.show()
#df.to_pandas()


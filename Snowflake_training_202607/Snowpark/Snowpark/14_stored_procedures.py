# Demo 14: Stored Procedures — reusable server-side logic

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: STORED PROCEDURES (important for SnowPro Core)
# =====================================================================
# Stored Procedures vs UDFs:
#
#   UDF:              Stored Procedure:
#   - Called IN a SQL query     - Called with CALL statement
#   - Returns a value per row   - Returns a single value
#   - Cannot modify data (DDL)  - CAN modify data (INSERT, CREATE, etc.)
#   - Runs with caller rights   - Runs with caller OR owner rights
#
# In Snowpark Python, a stored procedure is just a Python function
# that takes a Session as its first argument.
# =====================================================================

# --- DEFINE THE PROCEDURE LOGIC ---
# This function creates a summary table from source data.
# It demonstrates that stored procedures CAN do DDL/DML (write data).
def create_summary(session, source_table: str, target_table: str) -> str:
    df = session.table(source_table)
    summary = df.group_by("CATEGORY_ID").count()
    summary.write.mode("overwrite").save_as_table(target_table)
    return f"Summary written to {target_table} with {summary.count()} rows"

# --- EXECUTE LOCALLY (without registering) ---
# You can test stored procedure logic directly in a Workspace
# before registering it as a permanent procedure.
result = create_summary(
    session,
    "SNOWFLAKE_TRAINING.TRAIN.SAMPLE_PRODUCT_DATA",
    "SNOWFLAKE_TRAINING.TRAIN.CATEGORY_SUMMARY"
)
print(result)

# --- VERIFY ---
print("\nSummary table contents:")
session.table("SNOWFLAKE_TRAINING.TRAIN.CATEGORY_SUMMARY").to_pandas()

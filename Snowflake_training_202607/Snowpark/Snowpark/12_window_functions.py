# Demo 12: Window Functions — RANK, ROW_NUMBER, running totals

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, row_number, rank, sum as sum_
from snowflake.snowpark.window import Window
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: WINDOW FUNCTIONS (important for SnowPro Core)
# =====================================================================
# Window functions compute values ACROSS a set of rows related to the
# current row — without collapsing rows like GROUP BY does.
#
# Two parts:
#   1. Window.partition_by()  -> like GROUP BY but keeps all rows
#   2. .order_by()            -> defines the ordering within each partition
#
# Common window functions:
#   row_number() -> sequential number within partition
#   rank()       -> rank with gaps for ties
#   dense_rank() -> rank without gaps
#   sum()        -> running or partition total
# =====================================================================

# --- SAMPLE DATA ---
sales = session.create_dataframe([
    Row(REGION="East", REP="Alice", REVENUE=300),
    Row(REGION="East", REP="Bob", REVENUE=450),
    Row(REGION="East", REP="Carol", REVENUE=450),
    Row(REGION="West", REP="Dave", REVENUE=500),
    Row(REGION="West", REP="Eve", REVENUE=200),
    Row(REGION="West", REP="Frank", REVENUE=350),
])

# --- DEFINE WINDOW ---
# Partition by REGION, order by REVENUE descending (highest first)
window_spec = Window.partition_by("REGION").order_by(col("REVENUE").desc())

# --- APPLY WINDOW FUNCTIONS ---
result = sales.select(
    col("REGION"),
    col("REP"),
    col("REVENUE"),
    row_number().over(window_spec).alias("ROW_NUM"),
    rank().over(window_spec).alias("RANK"),
)

print("Ranked sales reps within each region:")
result.to_pandas()

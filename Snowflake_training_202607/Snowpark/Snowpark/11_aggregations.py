# Demo 11: Aggregations — GROUP BY, SUM, AVG, COUNT

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, avg, count, min as min_, max as max_
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: AGGREGATIONS (important for SnowPro Core)
# =====================================================================
# .group_by() + .agg() = SQL's GROUP BY + aggregate functions
# Common aggregate functions:
#   sum(), avg(), count(), min(), max(), count_distinct()
# =====================================================================

# --- SAMPLE DATA ---
sales = session.create_dataframe([
    Row(REGION="East", PRODUCT="Widget", REVENUE=100),
    Row(REGION="East", PRODUCT="Gadget", REVENUE=200),
    Row(REGION="West", PRODUCT="Widget", REVENUE=150),
    Row(REGION="West", PRODUCT="Gadget", REVENUE=300),
    Row(REGION="West", PRODUCT="Widget", REVENUE=50),
    Row(REGION="East", PRODUCT="Gadget", REVENUE=175),
])

# --- GROUP BY with multiple aggregations ---
# This is equivalent to:
#   SELECT REGION, SUM(REVENUE), AVG(REVENUE), COUNT(*)
#   FROM sales GROUP BY REGION
result = sales.group_by("REGION").agg(
    sum_("REVENUE").alias("TOTAL_REVENUE"),
    avg("REVENUE").alias("AVG_REVENUE"),
    count("*").alias("NUM_TRANSACTIONS"),
)

print("Aggregated by REGION:")
result.to_pandas()

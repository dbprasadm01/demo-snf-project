# Demo 13: User-Defined Functions (UDFs) — custom Python logic in SQL

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import IntegerType, StringType
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: UDFs (important for SnowPro Core)
# =====================================================================
# A UDF (User-Defined Function) lets you write custom Python logic
# that runs INSIDE Snowflake's execution engine — on every row.
#
# Two ways to create:
#   1. @udf decorator       -> for named, reusable functions
#   2. Inline/anonymous UDF -> for quick one-off transformations
#
# UDFs run in a secure Python sandbox on Snowflake's compute.
# They're scalar (one input row -> one output value).
# =====================================================================

# --- METHOD 1: INLINE UDF ---
# Define a simple function and register it as a UDF.
# This UDF categorizes revenue into "High", "Medium", or "Low".
categorize = udf(
    lambda revenue: "High" if revenue > 300 else ("Medium" if revenue > 150 else "Low"),
    return_type=StringType(),
    input_types=[IntegerType()],
)

# --- APPLY UDF TO DATAFRAME ---
sales = session.create_dataframe([
    Row(PRODUCT="Widget", REVENUE=100),
    Row(PRODUCT="Gadget", REVENUE=250),
    Row(PRODUCT="Doohickey", REVENUE=500),
    Row(PRODUCT="Thingamajig", REVENUE=50),
])

result = sales.select(
    col("PRODUCT"),
    col("REVENUE"),
    categorize(col("REVENUE")).alias("CATEGORY"),  # apply UDF
)

print("UDF applied — categorized revenue:")
result.to_pandas()

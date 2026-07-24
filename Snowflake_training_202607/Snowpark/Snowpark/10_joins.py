# Demo 10: Joins — combining DataFrames like SQL JOIN

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: JOINS (important for SnowPro Core)
# =====================================================================
# Snowpark supports all standard join types:
#   "inner"      -> only matching rows from both sides
#   "left"       -> all rows from left + matching from right
#   "right"      -> all rows from right + matching from left
#   "full"       -> all rows from both sides
#   "cross"      -> cartesian product (every row x every row)
#   "semi"       -> rows from left that HAVE a match in right
#   "anti"       -> rows from left that DON'T match in right
# =====================================================================

# --- CREATE TWO DATAFRAMES ---
orders = session.create_dataframe([
    Row(ORDER_ID=1, CUSTOMER_ID=101, AMOUNT=250),
    Row(ORDER_ID=2, CUSTOMER_ID=102, AMOUNT=450),
    Row(ORDER_ID=3, CUSTOMER_ID=103, AMOUNT=125),
    Row(ORDER_ID=4, CUSTOMER_ID=104, AMOUNT=800),
])

customers = session.create_dataframe([
    Row(CUSTOMER_ID=101, NAME="Alice"),
    Row(CUSTOMER_ID=102, NAME="Bob"),
    Row(CUSTOMER_ID=105, NAME="Charlie"),  # no matching order
])

# --- INNER JOIN ---
# Only rows where CUSTOMER_ID exists in BOTH DataFrames
print("INNER JOIN (only matching rows):")
inner = orders.join(customers, orders["CUSTOMER_ID"] == customers["CUSTOMER_ID"])
inner.to_pandas()

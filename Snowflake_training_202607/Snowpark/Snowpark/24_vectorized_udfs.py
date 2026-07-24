# Demo 24: Vectorized UDFs — batch processing with Pandas for performance

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, pandas_udf
from snowflake.snowpark.types import IntegerType, StringType, PandasSeriesType
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: VECTORIZED (PANDAS) UDFs (SnowPro Core)
# =====================================================================
# Regular UDF:     processes ONE ROW at a time (Python loop overhead)
# Vectorized UDF:  processes a BATCH of rows as a Pandas Series (fast)
#
# Why faster?
#   - Avoids Python loop overhead (no per-row function call)
#   - Uses Pandas/NumPy vectorized operations (C-level speed)
#   - Snowflake sends data in Arrow batches to the UDF
#
# When to use:
#   - Numeric computations on large datasets
#   - String operations across many rows
#   - Any place where pandas vectorization helps
#
# EXAM TIP: "How to improve UDF performance?" -> Vectorized UDF
# =====================================================================

# --- REGULAR UDF (row-by-row, slower) ---
@session.udf.register(
    return_type=IntegerType(),
    input_types=[IntegerType()],
    name="regular_square",
    replace=True,
)
def regular_square(x: int) -> int:
    return x * x  # called once per row

# --- VECTORIZED UDF (batch, faster) ---
@session.udf.register(
    return_type=PandasSeriesType(IntegerType()),
    input_types=[PandasSeriesType(IntegerType())],
    name="vectorized_square",
    replace=True,
)
def vectorized_square(series):
    import pandas as pd
    # 'series' is a Pandas Series containing a BATCH of values
    # Operations on the entire batch at once — no Python loop
    return series * series

# --- COMPARE THEM ---
df = session.create_dataframe([Row(NUM=i) for i in range(1, 11)])

print("1. Regular UDF result (row-by-row):")
df.select(col("NUM"), regular_square(col("NUM")).alias("SQUARED")).show()

print("\n2. Vectorized UDF result (batch via Pandas):")
df.select(col("NUM"), vectorized_square(col("NUM")).alias("SQUARED")).show()

print("""
3. Performance comparison (conceptual):

   Dataset: 10 million rows
   ┌──────────────────────┬───────────┐
   │ UDF Type             │ Speed     │
   ├──────────────────────┼───────────┤
   │ Regular (row-by-row) │ ~60 sec   │
   │ Vectorized (Pandas)  │ ~5 sec    │
   └──────────────────────┴───────────┘

   The vectorized version is ~10-12x faster because:
   - No Python function call per row
   - NumPy/Pandas uses optimized C code under the hood
   - Data arrives pre-batched in Apache Arrow format
""")

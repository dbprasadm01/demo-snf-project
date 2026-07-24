# Demo 23: UDTFs — Table Functions that return multiple rows

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, table_function, lit
from snowflake.snowpark.types import (
    IntegerType, StringType, StructType, StructField
)
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: UDTFs (User-Defined Table Functions) (SnowPro Core)
# =====================================================================
# UDF:  one row in  -> one value out   (scalar)
# UDTF: one row in  -> MULTIPLE rows out (tabular)
#
# Use cases:
#   - Explode a comma-separated string into rows
#   - Generate date ranges from start/end dates
#   - Parse nested JSON into flat rows
#   - Any 1-to-many transformation
#
# A UDTF class needs three methods:
#   __init__()     -> called once when the function starts
#   process()      -> called once per input row, yields output rows
#   end_partition() -> called at end of each partition (optional)
# =====================================================================

# --- REGISTER A SIMPLE UDTF ---
# This UDTF takes a comma-separated string and explodes it into rows.
from snowflake.snowpark.udtf import UDTFRegistration

# Define output schema: what columns the UDTF returns
output_schema = StructType([
    StructField("WORD", StringType()),
    StructField("POSITION", IntegerType()),
])

# Define the UDTF class
class SplitWords:
    def process(self, text: str):
        # Yield one row for each word in the comma-separated string
        for i, word in enumerate(text.split(","), start=1):
            yield (word.strip(), i)

# Register it
split_words_udtf = session.udtf.register(
    SplitWords,
    output_schema=output_schema,
    input_types=[StringType()],
    name="split_words_udtf",
    is_permanent=False,  # temporary — session-scoped
    replace=True,
)

# --- USE THE UDTF ---
print("1. UDTF — Explode comma-separated values into rows:")
df = session.create_dataframe([
    Row(ID=1, TAGS="python,snowpark,data"),
    Row(ID=2, TAGS="sql,warehouse,cloud"),
])

# Call UDTF using table_function + join
result = df.join_table_function(split_words_udtf(col("TAGS")))
result.show()

# --- EXAM SUMMARY ---
print("""
2. UDF vs UDTF vs Stored Procedure:

   ┌──────────────┬───────────┬──────────────┬──────────────────┐
   │              │ UDF       │ UDTF         │ Stored Procedure │
   ├──────────────┼───────────┼──────────────┼──────────────────┤
   │ Input        │ Row       │ Row          │ Parameters       │
   │ Output       │ 1 value   │ N rows       │ 1 value          │
   │ Called from  │ SELECT    │ TABLE()      │ CALL             │
   │ Can write    │ No        │ No           │ Yes              │
   │ Use case     │ Transform │ Explode/gen  │ Orchestrate      │
   └──────────────┴───────────┴──────────────┴──────────────────┘
""")

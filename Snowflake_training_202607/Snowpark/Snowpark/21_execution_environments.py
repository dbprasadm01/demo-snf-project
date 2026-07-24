# Demo 21: Snowpark Optimized Warehouses & Execution Environments

from snowflake.snowpark.context import get_active_session

session = get_active_session()

# =====================================================================
# KEY CONCEPT: EXECUTION ENVIRONMENTS
# =====================================================================
#
# WHERE does Snowpark code run?
#
#   ┌─────────────────────────────────────────────────────────┐
#   │  YOUR CODE (Python)                                     │
#   │  ↓                                                      │
#   │  Snowpark Library (translates to SQL / executes UDFs)   │
#   │  ↓                                                      │
#   │  Snowflake Warehouse (runs the actual computation)      │
#   └─────────────────────────────────────────────────────────┘
#
# Key point: Your Python runs on Snowflake's compute, NOT your laptop.
# The DataFrame operations translate to SQL executed on the warehouse.
# UDFs run in a secure Python sandbox on worker nodes.
#
# =====================================================================
# SNOWPARK-OPTIMIZED WAREHOUSES
# =====================================================================
# Regular warehouse:
#   - Standard memory per node
#   - Good for SQL queries, small UDFs
#
# Snowpark-optimized warehouse:
#   - 16x MORE memory per node
#   - Required for: large ML models, heavy pandas operations,
#     UDFs that process large data in memory
#   - Created with: CREATE WAREHOUSE ... WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
#
# EXAM TIP: If a question mentions "out of memory" or "large ML model",
#           the answer is likely "Snowpark-optimized warehouse".
# =====================================================================

# --- SHOW CURRENT WAREHOUSE INFO ---
print("1. Current warehouse:")
session.sql("SELECT CURRENT_WAREHOUSE() AS WAREHOUSE").show()

# --- HOW TO CREATE A SNOWPARK-OPTIMIZED WAREHOUSE (SQL) ---
print("""
2. Creating a Snowpark-Optimized Warehouse (SQL syntax):

   CREATE WAREHOUSE my_sp_wh
       WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
       WAREHOUSE_SIZE = 'MEDIUM';

   -- Regular warehouse for comparison:
   CREATE WAREHOUSE my_regular_wh
       WAREHOUSE_SIZE = 'MEDIUM';
       -- (WAREHOUSE_TYPE defaults to 'STANDARD')
""")

# --- EXECUTION ENVIRONMENT SUMMARY ---
print("""
3. Where Snowpark code runs :

   ┌──────────────────────┬─────────────────────────────┐
   │ Environment          │ What runs there             │
   ├──────────────────────┼─────────────────────────────┤
   │ Worksheet/Workspace  │ Interactive development     │
   │ Stored Procedure     │ Server-side, scheduled      │
   │ UDF sandbox          │ Per-row Python execution    │
   │ UDTF sandbox         │ Per-partition execution     │
   │ Container Services   │ Full Docker containers      │
   └──────────────────────┴─────────────────────────────┘

   ALL of these use Snowflake compute (warehouses or
   compute pools) — nothing runs on your local machine.
""")

# --- CALLER vs OWNER RIGHTS ---
print("""
4. Stored Procedure Rights (exam topic):

   CALLER RIGHTS (default):
   - Runs with the CALLING user's permissions
   - Can only access objects the caller can access

   OWNER RIGHTS:
   - Runs with the CREATING user's permissions
   - Can access objects the caller normally can't
   - Used for controlled data access patterns

   CREATE PROCEDURE my_proc()
       RETURNS STRING
       LANGUAGE PYTHON
       RUNTIME_VERSION = '3.11'
       EXECUTE AS OWNER         -- <-- owner rights
       HANDLER = 'run'
       ...
""")

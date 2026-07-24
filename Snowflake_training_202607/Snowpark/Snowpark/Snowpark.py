# Snowpark Demo Index — open and run each file one by one
#
# ===================================================================
# BASICS (01-08)
# ===================================================================
#   01_load_table.py             - Load an existing Snowflake table
#   02_create_from_list.py       - Create a DataFrame from a Python list
#   03_create_from_rows.py       - Create a DataFrame using Row objects
#   04_create_with_schema.py     - Create a DataFrame with an explicit schema
#   05_range.py                  - Generate a DataFrame from a numeric range
#   06_filter.py                 - Filter rows using column expressions
#   07_select.py                 - Select columns + display as table (to_pandas)
#   08_write_table.py            - Write a DataFrame back to Snowflake
#
# ===================================================================
# SNOWPRO CORE — DATAFRAME OPERATIONS (09-18)
# ===================================================================
#   09_lazy_evaluation.py        - Lazy eval, .queries, when SQL actually runs
#   10_joins.py                  - Inner, left, right, full, semi, anti joins
#   11_aggregations.py           - GROUP BY, SUM, AVG, COUNT
#   12_window_functions.py       - RANK, ROW_NUMBER, Window.partition_by
#   13_udfs.py                   - User-Defined Functions (custom Python in SQL)
#   14_stored_procedures.py      - Stored Procedures vs UDFs, data modification
#   15_caching_and_explain.py    - cache_result(), explain(), query plans
#   16_sql_interop.py            - session.sql(), mixing SQL + Snowpark, temp views
#   17_common_operations.py      - sort, limit, distinct, drop, union, sample
#   18_actions_vs_transformations.py - Which methods trigger SQL (exam-critical)
#
# ===================================================================
# SNOWPRO CORE — ADVANCED TOPICS (19-24)
# ===================================================================
#   19_stages_file_ops.py        - Stages, file.put/get, read CSV/JSON/Parquet
#   20_packages_dependencies.py  - Anaconda packages, add_packages, add_import
#   21_execution_environments.py - Where code runs, Snowpark-optimized WH, rights
#   22_temp_vs_permanent.py      - Temp tables, write modes, views, cache_result
#   23_udtfs.py                  - Table functions (one row -> many rows)
#   24_vectorized_udfs.py        - Pandas UDFs for batch performance
#
# ===================================================================
# HOW TO DISPLAY RESULTS
# ===================================================================
#   .show()       -> plain text output (good for quick debugging)
#   .to_pandas()  -> rich table in output pane (use as last line for demos)
#   .collect()    -> Python list of Row objects (for programmatic access)
#
# ===================================================================
# WHY get_active_session()?
# ===================================================================
# Snowpark needs a "session" to talk to Snowflake. The session is your
# live connection. In a Workspace, Snowflake already has a session running.
# get_active_session() grabs that existing connection.
#
#   session = get_active_session()   # "Give me the connection"
#   session.table("MY_TABLE")        # "Use it to read a table"

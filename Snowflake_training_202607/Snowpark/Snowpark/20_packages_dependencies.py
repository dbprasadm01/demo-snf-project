# Demo 20: Packages & Dependencies — adding libraries for UDFs

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import StringType, IntegerType
from snowflake.snowpark import Row

session = get_active_session()

# =====================================================================
# KEY CONCEPT: PACKAGES & DEPENDENCIES (important for SnowPro Core)
# =====================================================================
# When creating UDFs, you may need external Python packages.
# Snowflake provides packages via its Anaconda channel.
#
# Rules:
#   1. ONLY packages from Snowflake's Anaconda channel are allowed
#   2. You CANNOT pip install arbitrary packages from PyPI
#   3. Use session.add_packages() to declare dependencies
#   4. Use session.add_import() to upload your own .py modules
#
# Common available packages:
#   pandas, numpy, scikit-learn, scipy, xgboost,
#   snowflake-snowpark-python, pyyaml, requests (limited)
# =====================================================================

# --- CHECK AVAILABLE PACKAGES ---
# This query shows all packages available in Snowflake's Anaconda channel.
print("1. Sample of available Anaconda packages in Snowflake:")
packages_df = session.sql("""
    SELECT PACKAGE_NAME, VERSION
    FROM INFORMATION_SCHEMA.PACKAGES
    WHERE LANGUAGE = 'python'
    ORDER BY PACKAGE_NAME
    LIMIT 15
""")
packages_df.show()

# --- ADD PACKAGES FOR A UDF ---
# session.add_packages() makes a package available to ALL subsequent UDFs
# in this session.
print("\n2. Adding 'numpy' package for UDF use:")
session.add_packages("numpy")

# --- UDF THAT USES AN EXTERNAL PACKAGE ---
@udf(return_type=StringType(), input_types=[IntegerType()])
def describe_number(n):
    import numpy as np  # available because we added the package
    arr = np.array([n])
    return f"Value={n}, Square root={np.sqrt(arr)[0]:.2f}"

# --- TEST IT ---
df = session.create_dataframe([Row(NUM=4), Row(NUM=16), Row(NUM=25)])
result = df.select(col("NUM"), describe_number(col("NUM")).alias("DESCRIPTION"))

print("\n3. UDF using numpy:")
result.show()

# --- ADD YOUR OWN MODULE (conceptual) ---
print("""
4. Custom module upload (conceptual):

   # If you have a helper file "my_utils.py" with shared logic:
   session.add_import("my_utils.py")

   # Then inside your UDF you can:
   @udf(...)
   def my_func(x):
       from my_utils import helper_function
       return helper_function(x)

   # The file gets uploaded to Snowflake and made available
   # in the UDF's sandbox.
""")

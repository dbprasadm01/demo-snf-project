/*
=============================================================
SNOWPRO CORE EXAM RELEVANCE:
Domain: Snowflake AI Data Cloud Features & Architecture
Key Concepts:
  - Snowflake SQL REST API allows executing SQL via HTTP requests
  - Endpoint: /api/v2/statements
  - Supports synchronous and asynchronous query execution
  - Authentication: OAuth or Key Pair (JWT)
  - Results can be paginated for large datasets
  - Used for application integration without native drivers
  - Separate from Snowpipe REST API (data loading)
=============================================================
*/

-- ============================================
-- SETUP: Create objects to query via REST API
-- ============================================
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS SNOWPRO_DEMO;
CREATE SCHEMA IF NOT EXISTS SNOWPRO_DEMO.REST_API_DEMO;
USE DATABASE SNOWPRO_DEMO;
USE SCHEMA REST_API_DEMO;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE TABLE employees (
    emp_id INT,
    name STRING,
    department STRING,
    salary NUMBER(10,2)
);

INSERT INTO employees VALUES
    (1, 'John Doe', 'Engineering', 95000),
    (2, 'Jane Smith', 'Marketing', 82000),
    (3, 'Mike Johnson', 'Engineering', 105000),
    (4, 'Sarah Williams', 'Sales', 78000),
    (5, 'Tom Brown', 'Marketing', 88000);

-- ============================================
-- PART 1: UNDERSTANDING THE SQL REST API
-- ============================================
/*
EXAM NOTES - SQL REST API Overview:

1. ENDPOINT:
   POST https://<account_identifier>.snowflakecomputing.com/api/v2/statements

2. AUTHENTICATION METHODS:
   - OAuth 2.0 token (Authorization: Bearer <token>)
   - Key Pair authentication (JWT token)
   - NOT username/password (that's deprecated for API access)

3. REQUEST BODY (JSON):
   {
     "statement": "SELECT * FROM employees",
     "timeout": 60,
     "database": "SNOWPRO_DEMO",
     "schema": "REST_API_DEMO",
     "warehouse": "COMPUTE_WH",
     "role": "ACCOUNTADMIN"
   }

4. RESPONSE:
   - Synchronous: Results returned immediately if query completes fast
   - Asynchronous: Returns a statementHandle for polling
   - Status codes: 200 (success), 202 (async/pending), 422 (error)

5. CHECKING STATUS (async queries):
   GET /api/v2/statements/<statementHandle>

6. CANCELLING A QUERY:
   POST /api/v2/statements/<statementHandle>/cancel
*/

-- ============================================
-- PART 2: KEY PAIR AUTHENTICATION SETUP
-- ============================================
-- Key Point: REST API commonly uses key pair auth
-- Step 1: Generate RSA key pair (done outside Snowflake)
-- Step 2: Assign public key to user

-- Example of assigning a public key to a user (for REST API auth)
-- ALTER USER my_api_user SET RSA_PUBLIC_KEY = 'MIIBIjANBg...';

-- Check current user's key fingerprint
DESC USER CURRENT_USER();

-- ============================================
-- PART 3: REST API REQUEST EXAMPLES (Conceptual)
-- ============================================
/*
EXAM NOTES - Example cURL requests:

--- Submit a query ---
curl -X POST \
  https://myaccount.snowflakecomputing.com/api/v2/statements \
  -H "Authorization: Bearer <oauth_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT * FROM SNOWPRO_DEMO.REST_API_DEMO.employees WHERE department = ?",
    "timeout": 60,
    "database": "SNOWPRO_DEMO",
    "schema": "REST_API_DEMO",
    "warehouse": "COMPUTE_WH",
    "bindings": {
      "1": {"type": "TEXT", "value": "Engineering"}
    }
  }'

--- Check query status ---
curl -X GET \
  https://myaccount.snowflakecomputing.com/api/v2/statements/01abc-def-12345 \
  -H "Authorization: Bearer <oauth_token>"

--- Cancel a running query ---
curl -X POST \
  https://myaccount.snowflakecomputing.com/api/v2/statements/01abc-def-12345/cancel \
  -H "Authorization: Bearer <oauth_token>"
*/

-- ============================================
-- PART 4: RESULT PAGINATION
-- ============================================
/*
EXAM NOTES - Pagination:
  - Large result sets are split into partitions
  - First response includes partition info
  - Fetch additional partitions via:
    GET /api/v2/statements/<handle>?partition=<number>
  - Default: results returned inline in JSON
  - Each partition can contain multiple rows
*/

-- ============================================
-- PART 5: API vs OTHER CONNECTIVITY OPTIONS
-- ============================================
/*
EXAM NOTES:
┌───────────────────────┬─────────────────────────────────────────────┐
│ Method                │ Use Case                                    │
├───────────────────────┼─────────────────────────────────────────────┤
│ SQL REST API          │ Lightweight HTTP integration, serverless    │
│ Native Drivers        │ Python, JDBC, ODBC, Node.js, Go, .NET      │
│ Snowpark             │ DataFrame API (Python, Java, Scala)          │
│ SnowSQL CLI          │ Command-line SQL execution                   │
│ Snowpipe REST API    │ Continuous data loading notifications        │
│ Snowsight            │ Web UI for interactive queries               │
└───────────────────────┴─────────────────────────────────────────────┘

Key Differences:
  - SQL REST API: Stateless, HTTP-based, no driver installation needed
  - Drivers: Persistent connections, better for high-throughput
  - Snowpark: Best for data transformations and ML pipelines
*/

-- ============================================
-- PART 6: NETWORK POLICIES & API SECURITY
-- ============================================
-- Key Point: Network policies apply to REST API access too

-- Example: Create a network policy to restrict API access
CREATE OR REPLACE NETWORK POLICY api_access_policy
    ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8')
    COMMENT = 'Restrict REST API access to corporate IPs';

-- View the policy
DESCRIBE NETWORK POLICY api_access_policy;

-- NOTE: Apply to user or account level (not running to avoid lockout)
-- ALTER USER api_service_user SET NETWORK_POLICY = 'api_access_policy';

-- ============================================
-- PART 7: USEFUL QUERIES FOR REST API MONITORING
-- ============================================
-- Check recent queries (could include REST API queries)
SELECT query_id, query_text, user_name, execution_status, 
       start_time, end_time, total_elapsed_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hours', -1, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP()))
WHERE query_text LIKE '%employees%'
ORDER BY start_time DESC
LIMIT 10;

-- ============================================
-- CLEANUP
-- ============================================
-- DROP NETWORK POLICY api_access_policy;
-- DROP TABLE employees;
-- DROP SCHEMA SNOWPRO_DEMO.REST_API_DEMO;

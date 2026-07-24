/*
=============================================================
SNOWPRO CORE EXAM RELEVANCE:
Domain: Snowflake AI Data Cloud Features & Architecture
Key Concepts:
  - Iceberg is an open table format (Apache Iceberg)
  - Snowflake supports Iceberg tables with Snowflake-managed storage
  - Iceberg tables enable interoperability with other engines
  - Two storage modes: Snowflake-managed vs externally-managed
  - Iceberg tables support ACID transactions
  - Iceberg tables use Parquet file format for data
=============================================================
*/

-- ============================================
-- SETUP
-- ============================================
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS SNOWPRO_DEMO;
CREATE SCHEMA IF NOT EXISTS SNOWPRO_DEMO.ICEBERG_DEMO;
USE DATABASE SNOWPRO_DEMO;
USE SCHEMA ICEBERG_DEMO;
USE WAREHOUSE COMPUTE_WH;

-- ============================================
-- 1. CREATE A SNOWFLAKE-MANAGED ICEBERG TABLE
-- ============================================
-- Key Point: CATALOG='SNOWFLAKE' means Snowflake manages the Iceberg metadata
-- Key Point: No external volume needed for Snowflake-managed storage

CREATE OR REPLACE ICEBERG TABLE customer_orders (
    order_id INT,
    customer_name STRING,
    order_date DATE,
    amount DECIMAL(10,2),
    region STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'snowflake'
    BASE_LOCATION = 'customer_orders/';

-- ============================================
-- 2. INSERT DATA INTO ICEBERG TABLE
-- ============================================
INSERT INTO customer_orders VALUES
    (1, 'Alice Johnson', '2024-01-15', 250.00, 'US-EAST'),
    (2, 'Bob Smith', '2024-01-16', 175.50, 'US-WEST'),
    (3, 'Carol White', '2024-01-17', 320.75, 'EU-WEST'),
    (4, 'David Brown', '2024-02-01', 89.99, 'US-EAST'),
    (5, 'Eva Martinez', '2024-02-05', 445.00, 'APAC');

-- ============================================
-- 3. QUERY ICEBERG TABLE (works like standard table)
-- ============================================
SELECT * FROM customer_orders ORDER BY order_id;

-- Aggregation works the same way
SELECT region, COUNT(*) as order_count, SUM(amount) as total_revenue
FROM customer_orders
GROUP BY region
ORDER BY total_revenue DESC;

-- ============================================
-- 4. SHOW TABLE PROPERTIES
-- ============================================
-- Key Point: You can inspect the Iceberg table metadata
SHOW ICEBERG TABLES IN SCHEMA ICEBERG_DEMO;

-- Describe the table to see column details
DESCRIBE TABLE customer_orders;

-- ============================================
-- 5. DML OPERATIONS (Iceberg supports full DML)
-- ============================================
-- UPDATE
UPDATE customer_orders SET amount = 300.00 WHERE order_id = 1;

-- DELETE
DELETE FROM customer_orders WHERE order_id = 5;

-- MERGE (upsert pattern)
MERGE INTO customer_orders AS target
USING (SELECT 6 AS order_id, 'Frank Lee' AS customer_name, 
       '2024-02-10'::DATE AS order_date, 199.99 AS amount, 'US-WEST' AS region) AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_name, order_date, amount, region)
    VALUES (source.order_id, source.customer_name, source.order_date, source.amount, source.region);

SELECT * FROM customer_orders ORDER BY order_id;

-- ============================================
-- 6. KEY DIFFERENCES: ICEBERG vs STANDARD TABLES
-- ============================================
/*
EXAM NOTES:
┌─────────────────────────┬──────────────────────┬─────────────────────────┐
│ Feature                 │ Standard Table       │ Iceberg Table           │
├─────────────────────────┼──────────────────────┼─────────────────────────┤
│ File Format             │ Snowflake proprietary│ Apache Parquet          │
│ Metadata Format         │ Snowflake proprietary│ Apache Iceberg          │
│ Time Travel             │ Up to 90 days        │ Limited                 │
│ Fail-safe               │ 7 days               │ Not supported           │
│ Clustering              │ Supported            │ Supported               │
│ Interoperability        │ Snowflake only       │ Multi-engine (Spark etc)│
│ Storage Cost            │ Standard             │ Lower (Parquet)         │
│ Query Performance       │ Optimized            │ Near-native             │
└─────────────────────────┴──────────────────────┴─────────────────────────┘
*/

-- ============================================
-- 7. CREATE ICEBERG TABLE AS SELECT (CTAS)
-- ============================================
CREATE OR REPLACE ICEBERG TABLE high_value_orders
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'snowflake'
    BASE_LOCATION = 'high_value_orders/'
AS
    SELECT * FROM customer_orders WHERE amount > 200;

SELECT * FROM high_value_orders;

-- ============================================
-- CLEANUP
-- ============================================
-- DROP TABLE customer_orders;
-- DROP TABLE high_value_orders;
-- DROP SCHEMA SNOWPRO_DEMO.ICEBERG_DEMO;

-- Iceberg Tables with AWS S3 Integration - SnowPro Core Demo


/*
=============================================================
PREREQUISITES:
  - Storage Integration: snowflake_aws_integration (already created)
  - S3 Bucket: s3://snowflake-training-202607/
  - IAM Role ARN: arn:aws:iam::448734340866:role/role_for_snf
  - This script creates an external volume pointing to the same
    S3 bucket and then creates Iceberg tables on it.
=============================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_TRAINING;
USE SCHEMA TRAIN;
USE WAREHOUSE COMPUTE_WH;

/* ------------------------------------------------------------
   SECTION 1: CREATE EXTERNAL VOLUME FOR ICEBERG
   An external volume defines where Iceberg table data and
   metadata (Parquet files + Iceberg metadata) are stored.
   ------------------------------------------------------------ */

CREATE OR REPLACE EXTERNAL VOLUME iceberg_s3_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 'snowflake_training_s3'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://snowflake-training-202607/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::448734340866:role/role_for_snf'
        )
    );

-- Describe the volume to get the IAM_USER_ARN and EXTERNAL_ID
-- (Add these to your S3 bucket's IAM trust policy if not already done)
DESC EXTERNAL VOLUME iceberg_s3_volume;


/* ------------------------------------------------------------
   SECTION 2: CREATE ICEBERG TABLES ON S3
   Using CATALOG='SNOWFLAKE' with our external volume so data
   lives in S3 but Snowflake manages the Iceberg catalog.
   ------------------------------------------------------------ */

-- Table 1: Customer Orders
CREATE OR REPLACE ICEBERG TABLE iceberg_customer_orders (
    order_id INT,
    customer_name STRING,
    order_date DATE,
    amount DECIMAL(10,2),
    region STRING,
    status STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'iceberg_s3_volume'
    BASE_LOCATION = 'customer_orders/';

-- Table 2: Products
CREATE OR REPLACE ICEBERG TABLE iceberg_products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock_qty INT
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'iceberg_s3_volume'
    BASE_LOCATION = 'products/';

-- Verify tables were created
SHOW ICEBERG TABLES;


/* ------------------------------------------------------------
   SECTION 3: INSERT DATA
   ------------------------------------------------------------ */

INSERT INTO iceberg_customer_orders VALUES
    (1001, 'Alice Johnson', '2024-06-01', 250.00, 'US-EAST', 'COMPLETED'),
    (1002, 'Bob Smith', '2024-06-02', 175.50, 'US-WEST', 'COMPLETED'),
    (1003, 'Carol White', '2024-06-03', 320.75, 'EU-WEST', 'PENDING'),
    (1004, 'David Brown', '2024-06-05', 89.99, 'US-EAST', 'SHIPPED'),
    (1005, 'Eva Martinez', '2024-06-07', 445.00, 'APAC', 'COMPLETED'),
    (1006, 'Frank Lee', '2024-06-10', 199.99, 'US-WEST', 'PENDING'),
    (1007, 'Grace Kim', '2024-06-12', 530.00, 'APAC', 'COMPLETED'),
    (1008, 'Henry Wilson', '2024-06-15', 67.50, 'EU-WEST', 'CANCELLED');

INSERT INTO iceberg_products VALUES
    (101, 'Laptop Pro 15', 'Electronics', 1299.99, 50),
    (102, 'Wireless Mouse', 'Electronics', 29.99, 500),
    (103, 'Standing Desk', 'Furniture', 499.99, 30),
    (104, 'Ergonomic Chair', 'Furniture', 349.99, 45),
    (105, 'USB-C Hub', 'Electronics', 59.99, 200),
    (106, 'Monitor 27in', 'Electronics', 399.99, 75);

-- Query data
SELECT * FROM iceberg_customer_orders ORDER BY order_id;
SELECT * FROM iceberg_products ORDER BY product_id;


/* ------------------------------------------------------------
   SECTION 4: DML OPERATIONS ON ICEBERG TABLES
   Iceberg tables support full DML just like standard tables.
   ------------------------------------------------------------ */

-- ==================
-- 4A: UPDATE
-- ==================
-- Update order status
UPDATE iceberg_customer_orders
SET status = 'COMPLETED'
WHERE order_id = 1003;

-- Update product stock
UPDATE iceberg_products
SET stock_qty = stock_qty - 5
WHERE product_id = 101;

-- Verify updates
SELECT order_id, customer_name, status FROM iceberg_customer_orders WHERE order_id = 1003;
SELECT product_id, product_name, stock_qty FROM iceberg_products WHERE product_id = 101;

-- ==================
-- 4B: DELETE
-- ==================
-- Delete cancelled orders
DELETE FROM iceberg_customer_orders
WHERE status = 'CANCELLED';

-- Verify deletion
SELECT * FROM iceberg_customer_orders ORDER BY order_id;

-- ==================
-- 4C: MERGE (UPSERT)
-- ==================
-- Merge new orders (insert new, update existing)
MERGE INTO iceberg_customer_orders AS target
USING (
    SELECT column1 AS order_id, column2 AS customer_name, column3::DATE AS order_date,
           column4::DECIMAL(10,2) AS amount, column5 AS region, column6 AS status
    FROM VALUES
        (1004, 'David Brown', '2024-06-05', 89.99, 'US-EAST', 'DELIVERED'),  -- existing: update status
        (1009, 'Iris Chang', '2024-06-18', 275.00, 'APAC', 'PENDING'),       -- new: insert
        (1010, 'Jack Porter', '2024-06-20', 150.00, 'EU-WEST', 'PENDING')    -- new: insert
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET status = source.status
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_name, order_date, amount, region, status)
    VALUES (source.order_id, source.customer_name, source.order_date,
            source.amount, source.region, source.status);

-- Verify merge results
SELECT * FROM iceberg_customer_orders ORDER BY order_id;


/* ------------------------------------------------------------
   SECTION 5: ANALYTICAL QUERIES ON ICEBERG TABLES
   ------------------------------------------------------------ */

-- Revenue by region
SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_revenue,
       AVG(amount) AS avg_order_value
FROM iceberg_customer_orders
GROUP BY region
ORDER BY total_revenue DESC;

-- Orders by status
SELECT status, COUNT(*) AS cnt, SUM(amount) AS revenue
FROM iceberg_customer_orders
GROUP BY status;

-- Product inventory value
SELECT category, COUNT(*) AS products,
       SUM(price * stock_qty) AS inventory_value
FROM iceberg_products
GROUP BY category;


/* ------------------------------------------------------------
   SECTION 6: CREATE ICEBERG TABLE AS SELECT (CTAS)
   Create a derived Iceberg table from a query.
   ------------------------------------------------------------ */

CREATE OR REPLACE ICEBERG TABLE iceberg_completed_orders
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'iceberg_s3_volume'
    BASE_LOCATION = 'completed_orders/'
AS
    SELECT order_id, customer_name, order_date, amount, region
    FROM iceberg_customer_orders
    WHERE status = 'COMPLETED';

SELECT * FROM iceberg_completed_orders;


/* ------------------------------------------------------------
   SECTION 7: INSPECT ICEBERG TABLE METADATA
   ------------------------------------------------------------ */

-- Table properties
SHOW ICEBERG TABLES LIKE 'iceberg_%';

-- Table DDL
SELECT GET_DDL('TABLE', 'iceberg_customer_orders');

-- Describe columns
DESCRIBE TABLE iceberg_customer_orders;
DESCRIBE TABLE iceberg_products;


/* ------------------------------------------------------------
   SECTION 8: KEY EXAM NOTES
   ------------------------------------------------------------ */
/*
SNOWPRO CORE - ICEBERG TABLE KEY POINTS:

1. STORAGE OPTIONS:
   - Snowflake-managed (EXTERNAL_VOLUME = 'snowflake') -> no setup needed
   - Customer-managed (your own S3/Azure/GCS via external volume)

2. CATALOG OPTIONS:
   - CATALOG = 'SNOWFLAKE' -> Snowflake manages Iceberg metadata
   - External catalogs (Glue, Unity, Polaris) -> for multi-engine access

3. FILE FORMAT:
   - Data stored as Apache Parquet (open format)
   - Metadata stored in Iceberg format (manifest files)

4. DML SUPPORT:
   - Full INSERT, UPDATE, DELETE, MERGE supported
   - ACID transactions maintained

5. LIMITATIONS vs STANDARD TABLES:
   - No Fail-safe period (0 days)
   - Limited Time Travel
   - No materialized views on Iceberg tables
   - No dynamic data masking directly (use views)

6. INTEROPERABILITY:
   - Other engines (Spark, Trino, Flink) can read the same data
   - True open table format - no vendor lock-in

7. EXTERNAL VOLUME:
   - Defines the cloud storage location
   - Uses IAM roles (not access keys) for security
   - Must configure trust policy between Snowflake and your cloud
*/


/* ------------------------------------------------------------
   CLEANUP (uncomment to drop objects)
   ------------------------------------------------------------ */
-- DROP TABLE iceberg_completed_orders;
-- DROP TABLE iceberg_customer_orders;
-- DROP TABLE iceberg_products;
-- DROP EXTERNAL VOLUME iceberg_s3_volume;

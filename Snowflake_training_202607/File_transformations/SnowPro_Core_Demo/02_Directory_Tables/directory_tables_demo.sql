-- Directory Tables demo: unload data to AWS S3 and query file metadata
-- Co-authored with CoCo

/*
=============================================================
SNOWPRO CORE EXAM RELEVANCE:
Domain: Data Loading and Unloading
Key Concepts:
  - Directory tables store file-level metadata for staged files
  - They are implicit (attached to a stage, not created separately)
  - Enable querying file metadata: name, size, MD5, last_modified
  - Useful for managing unstructured data (images, PDFs, etc.)
  - Must be enabled on the stage and refreshed to see new files
  - Supports AUTO_REFRESH for automatic metadata updates
=============================================================

PREREQUISITES:
  - Storage Integration: snowflake_aws_integration (already created)
  - S3 Bucket: s3://snowflake-training-202607/
  - IAM Role ARN: arn:aws:iam::448734340866:role/role_for_snf
=============================================================
*/

-- ============================================
-- SETUP
-- ============================================
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_TRAINING;
USE SCHEMA TRAIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================
-- 1. CREATE EXTERNAL STAGE ON S3 WITH DIRECTORY TABLE ENABLED
-- ============================================
-- Key Point: DIRECTORY = (ENABLE = TRUE) activates the directory table
-- Key Point: Using existing snowflake_aws_integration for S3 access
CREATE OR REPLACE STAGE s3_directory_stage
    STORAGE_INTEGRATION = snowflake_aws_integration
    URL = 's3://snowflake-training-202607/directory_tables/'
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = (TYPE = CSV)
    COMMENT = 'External S3 stage with directory table for file metadata tracking';

-- ============================================
-- 2. CREATE SAMPLE DATA TO UNLOAD
-- ============================================
CREATE OR REPLACE TEMPORARY TABLE sales_data (
    sale_id INT, product STRING, quantity INT, revenue DECIMAL(10,2), sale_date DATE
);
INSERT INTO sales_data VALUES
    (1, 'Laptop', 5, 6499.95, '2024-01-15'),
    (2, 'Mouse', 50, 1499.50, '2024-01-16'),
    (3, 'Keyboard', 30, 2099.70, '2024-01-17'),
    (4, 'Monitor', 10, 3999.90, '2024-02-01'),
    (5, 'Headset', 25, 1249.75, '2024-02-05'),
    (6, 'Webcam', 40, 2399.60, '2024-02-10'),
    (7, 'Laptop', 8, 10399.92, '2024-03-01'),
    (8, 'Monitor', 12, 4799.88, '2024-03-15');

CREATE OR REPLACE TEMPORARY TABLE employee_data (
    emp_id INT, name STRING, department STRING, salary DECIMAL(10,2)
);
INSERT INTO employee_data VALUES
    (101, 'Alice Johnson', 'Engineering', 95000.00),
    (102, 'Bob Smith', 'Marketing', 82000.00),
    (103, 'Carol White', 'Engineering', 105000.00),
    (104, 'David Brown', 'Sales', 78000.00),
    (105, 'Eva Martinez', 'Marketing', 88000.00);

-- ============================================
-- 3. UNLOAD DATA TO S3 (into directory_tables/ folder)
-- ============================================
-- Unload sales data as CSV into sales/ subfolder
COPY INTO @s3_directory_stage/sales/sales_jan.csv
FROM (SELECT * FROM sales_data WHERE sale_date < '2024-02-01')
FILE_FORMAT = (TYPE = CSV  COMPRESSION = NONE)
OVERWRITE = TRUE
HEADER = TRUE
SINGLE = TRUE;

COPY INTO @s3_directory_stage/sales/sales_feb.csv
FROM (SELECT * FROM sales_data WHERE sale_date >= '2024-02-01' AND sale_date < '2024-03-01')
FILE_FORMAT = (TYPE = CSV  COMPRESSION = NONE)
OVERWRITE = TRUE
HEADER = TRUE
SINGLE = TRUE;

COPY INTO @s3_directory_stage/sales/sales_mar.csv
FROM (SELECT * FROM sales_data WHERE sale_date >= '2024-03-01')
FILE_FORMAT = (TYPE = CSV  COMPRESSION = NONE)
OVERWRITE = TRUE
HEADER = TRUE
SINGLE = TRUE;

-- Unload employee data into employees/ subfolder
COPY INTO @s3_directory_stage/employees/employee_roster.csv
FROM employee_data
FILE_FORMAT = (TYPE = CSV  COMPRESSION = NONE)
OVERWRITE = TRUE
HEADER = TRUE
SINGLE = TRUE;

-- Unload as Parquet format into reports/ subfolder
COPY INTO @s3_directory_stage/reports/sales_report
FROM (SELECT product, SUM(quantity) AS total_qty, SUM(revenue) AS total_revenue
      FROM sales_data GROUP BY product)
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE;

-- ============================================
-- 4. REFRESH THE DIRECTORY TABLE
-- ============================================
-- Key Point: You must REFRESH to populate/update the directory table metadata
-- For external stages, this syncs metadata from S3
ALTER STAGE s3_directory_stage REFRESH;

-- ============================================
-- 5. QUERY THE DIRECTORY TABLE
-- ============================================
-- Key Point: Use SELECT ... FROM DIRECTORY(@stage_name) syntax
SELECT * FROM DIRECTORY(@s3_directory_stage);

-- ============================================
-- 6. USEFUL DIRECTORY TABLE COLUMNS
-- ============================================
/*
EXAM NOTES - Directory table columns:
  - RELATIVE_PATH  : Path of file relative to stage
  - SIZE           : File size in bytes
  - LAST_MODIFIED  : Timestamp of last modification
  - MD5            : MD5 hash of the file (for integrity checks)
  - ETAG           : Entity tag for the file
  - FILE_URL       : Scoped URL to access the file
*/

-- Query specific metadata
SELECT 
    RELATIVE_PATH,
    SIZE,
    LAST_MODIFIED,
    MD5
FROM DIRECTORY(@s3_directory_stage)
ORDER BY RELATIVE_PATH;

-- ============================================
-- 7. FILTER FILES USING DIRECTORY TABLE
-- ============================================
-- Find all CSV files in the sales folder
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED
FROM DIRECTORY(@s3_directory_stage)
WHERE RELATIVE_PATH LIKE 'sales/%';

-- Find all Parquet files
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED
FROM DIRECTORY(@s3_directory_stage)
WHERE RELATIVE_PATH ILIKE '%.parquet';

-- Find files larger than 100 bytes
SELECT RELATIVE_PATH, SIZE
FROM DIRECTORY(@s3_directory_stage)
WHERE SIZE > 100
ORDER BY SIZE DESC;

-- ============================================
-- 8. AGGREGATE FILE METADATA
-- ============================================
-- Count files per folder
SELECT 
    SPLIT_PART(RELATIVE_PATH, '/', 1) AS folder,
    COUNT(*) AS file_count,
    SUM(SIZE) AS total_size_bytes,
    ROUND(SUM(SIZE) / 1024.0, 2) AS total_size_kb,
    MAX(LAST_MODIFIED) AS latest_file
FROM DIRECTORY(@s3_directory_stage)
GROUP BY folder
ORDER BY folder;

-- ============================================
-- 9. FILE URL FUNCTIONS WITH DIRECTORY TABLE
-- ============================================
-- Generate different types of URLs to access the staged files
SELECT 
    RELATIVE_PATH,
    SIZE,
    BUILD_SCOPED_FILE_URL(@s3_directory_stage, RELATIVE_PATH) AS scoped_url,
    GET_PRESIGNED_URL(@s3_directory_stage, RELATIVE_PATH, 3600) AS presigned_url_1hr
FROM DIRECTORY(@s3_directory_stage)
ORDER BY RELATIVE_PATH;

-- ============================================
-- 10. USING DIRECTORY TABLES WITH PATTERN MATCHING
-- ============================================
-- LIST shows raw stage contents (compare with directory table)
LIST @s3_directory_stage;

-- Directory table provides a SQL-queryable interface vs LIST
SELECT 
    RELATIVE_PATH,
    SIZE,
    LAST_MODIFIED
FROM DIRECTORY(@s3_directory_stage)
WHERE RELATIVE_PATH ILIKE '%.csv'
ORDER BY LAST_MODIFIED DESC;

-- ============================================
-- 11. STREAMS ON DIRECTORY TABLES (Change Tracking)
-- ============================================
-- Key Point: Streams detect new/removed files on the stage
CREATE OR REPLACE STREAM s3_files_stream ON STAGE s3_directory_stage;

-- Check for changes (will be empty until new files are added)
SELECT * FROM s3_files_stream;

-- To test: unload another file, refresh, then query the stream
COPY INTO @s3_directory_stage/sales/sales_apr.csv
FROM (SELECT 9 AS sale_id, 'Tablet' AS product, 15 AS quantity, 
      4499.85 AS revenue, '2024-04-01'::DATE AS sale_date)
FILE_FORMAT = (TYPE = CSV HEADER = TRUE COMPRESSION = NONE)
OVERWRITE = TRUE
SINGLE = TRUE;

ALTER STAGE s3_directory_stage REFRESH;

-- Stream now shows the new file
SELECT * FROM s3_files_stream;

-- ============================================
-- 12. AUTO_REFRESH FOR EXTERNAL STAGES
-- ============================================
/*
EXAM NOTES:
  - AUTO_REFRESH can be enabled for external stages (S3, Azure, GCS)
  - When enabled, new files trigger automatic directory table refresh
  - Uses event notifications (SQS for S3, Event Grid for Azure)
  - Internal stages require MANUAL refresh via ALTER STAGE ... REFRESH

  To enable auto-refresh on this stage:
  
  ALTER STAGE s3_directory_stage SET
      DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);

  Then configure an S3 event notification to send to the SQS queue
  shown in: DESCRIBE STAGE s3_directory_stage;
*/

-- ============================================
-- 13. DIRECTORY TABLE vs LIST COMMAND
-- ============================================
/*
EXAM NOTES:
┌────────────────────────┬──────────────────────┬────────────────────────┐
│ Feature                │ LIST Command         │ Directory Table        │
├────────────────────────┼──────────────────────┼────────────────────────┤
│ Output format          │ Tabular result       │ SQL-queryable table    │
│ JOIN capability        │ No                   │ Yes                    │
│ Filtering             │ Pattern only          │ Full SQL WHERE clause  │
│ Aggregation           │ No                    │ Yes (COUNT, SUM, etc.) │
│ Performance (large)   │ Slower               │ Faster (cached metadata)│
│ Needs refresh         │ No (always live)      │ Yes (manual or auto)   │
│ FILE_URL column       │ No                   │ Yes                    │
│ Stream support        │ No                   │ Yes                    │
└────────────────────────┴──────────────────────┴────────────────────────┘
*/

-- ============================================
-- CLEANUP
-- ============================================
-- DROP STREAM s3_files_stream;
-- DROP STAGE s3_directory_stage;

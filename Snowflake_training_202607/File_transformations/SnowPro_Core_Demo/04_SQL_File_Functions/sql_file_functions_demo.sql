-- SQL File Functions demo for SnowPro Core Certification
-- Co-authored with CoCo

/*
=============================================================
SNOWPRO CORE EXAM RELEVANCE:
Domain: Data Loading and Unloading
Key Concepts:
  - File functions generate URLs to access staged files
  - Three main functions: BUILD_SCOPED_FILE_URL, BUILD_STAGE_FILE_URL,
    GET_PRESIGNED_URL
  - Used to access unstructured data (images, PDFs, etc.)
  - Each has different security and expiration characteristics
  - Critical for building data apps that serve staged files
=============================================================
*/

-- ============================================
-- SETUP
-- ============================================
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS SNOWPRO_DEMO;
CREATE SCHEMA IF NOT EXISTS SNOWPRO_DEMO.FILE_FUNCTIONS_DEMO;
USE DATABASE SNOWPRO_DEMO;
USE SCHEMA FILE_FUNCTIONS_DEMO;
USE WAREHOUSE COMPUTE_WH;

-- Create a stage with directory table for our demo files
CREATE OR REPLACE STAGE demo_files_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for demonstrating SQL file functions';

-- Upload sample files
CREATE OR REPLACE TEMPORARY TABLE sample_report (metric STRING, value FLOAT);
INSERT INTO sample_report VALUES ('revenue', 1000000), ('costs', 750000), ('profit', 250000);

COPY INTO @demo_files_stage/reports/q1_report.csv
FROM sample_report
FILE_FORMAT = (TYPE = CSV)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @demo_files_stage/reports/q2_report.csv
FROM (SELECT * FROM sample_report WHERE value > 500000)
FILE_FORMAT = (TYPE = CSV)
HEADER = TRUE
OVERWRITE = TRUE;

-- Refresh directory table
ALTER STAGE demo_files_stage REFRESH;

-- Verify files exist
SELECT RELATIVE_PATH, SIZE FROM DIRECTORY(@demo_files_stage);

-- ============================================
-- 1. BUILD_SCOPED_FILE_URL
-- ============================================
/*
EXAM NOTES:
  - Returns a scoped URL that is tied to the user's role/session
  - URL is valid only for the user who generated it
  - Expires after 24 hours
  - Most SECURE option (role-based access control applies)
  - Cannot be shared with other users
  - Syntax: BUILD_SCOPED_FILE_URL(@stage, 'relative_path')
*/

SELECT 
    RELATIVE_PATH,
    BUILD_SCOPED_FILE_URL(@demo_files_stage, RELATIVE_PATH) AS scoped_url
FROM DIRECTORY(@demo_files_stage);

-- ============================================
-- 2. BUILD_STAGE_FILE_URL
-- ============================================
/*
EXAM NOTES:
  - Returns a permanent (non-expiring) Snowflake-hosted URL
  - URL format: https://<account>.snowflakecomputing.com/api/files/...
  - Requires authentication to access (user must be logged in)
  - Does NOT expire but requires active Snowflake session
  - Good for embedding in applications where users authenticate
  - Syntax: BUILD_STAGE_FILE_URL(@stage, 'relative_path')
*/

SELECT 
    RELATIVE_PATH,
    BUILD_STAGE_FILE_URL(@demo_files_stage, RELATIVE_PATH) AS stage_file_url
FROM DIRECTORY(@demo_files_stage);

-- ============================================
-- 3. GET_PRESIGNED_URL
-- ============================================
/*
EXAM NOTES:
  - Returns a pre-signed URL that does NOT require authentication
  - Anyone with the URL can access the file (security risk!)
  - Has a configurable expiration time (in seconds)
  - Good for sharing files with external users temporarily
  - Works only with external stages (S3, Azure, GCS) or internal stages
  - Syntax: GET_PRESIGNED_URL(@stage, 'relative_path', expiration_seconds)
*/

-- Generate a presigned URL with 3600 second (1 hour) expiration
SELECT 
    RELATIVE_PATH,
    GET_PRESIGNED_URL(@demo_files_stage, RELATIVE_PATH, 3600) AS presigned_url
FROM DIRECTORY(@demo_files_stage);

-- ============================================
-- 4. COMPARISON OF ALL THREE FUNCTIONS
-- ============================================
/*
EXAM NOTES - Critical Comparison:
┌─────────────────────────┬───────────────────┬───────────────────┬──────────────────┐
│ Feature                 │ SCOPED_FILE_URL   │ STAGE_FILE_URL    │ PRESIGNED_URL    │
├─────────────────────────┼───────────────────┼───────────────────┼──────────────────┤
│ Authentication Required │ Yes (role-based)  │ Yes (session)     │ No               │
│ Expiration              │ 24 hours          │ Never expires     │ Configurable     │
│ Shareable               │ No (user-bound)   │ No (needs login)  │ Yes (anyone)     │
│ Security Level          │ Highest           │ Medium            │ Lowest           │
│ Use Case                │ Internal apps     │ Authenticated apps│ External sharing │
│ Works with external     │ Yes               │ Yes               │ Yes              │
│ Works with internal     │ Yes               │ Yes               │ Yes              │
└─────────────────────────┴───────────────────┴───────────────────┴──────────────────┘
*/

-- Side-by-side comparison query
SELECT 
    RELATIVE_PATH,
    BUILD_SCOPED_FILE_URL(@demo_files_stage, RELATIVE_PATH) AS scoped_url,
    BUILD_STAGE_FILE_URL(@demo_files_stage, RELATIVE_PATH) AS stage_url,
    GET_PRESIGNED_URL(@demo_files_stage, RELATIVE_PATH, 3600) AS presigned_url
FROM DIRECTORY(@demo_files_stage);

-- ============================================
-- 5. PRACTICAL USE CASE: FILE CATALOG TABLE
-- ============================================
-- Combine directory table with file functions to create a file catalog

CREATE OR REPLACE VIEW file_catalog AS
SELECT 
    RELATIVE_PATH AS file_path,
    SIZE AS file_size_bytes,
    ROUND(SIZE / 1024.0, 2) AS file_size_kb,
    LAST_MODIFIED,
    MD5 AS file_hash,
    BUILD_SCOPED_FILE_URL(@demo_files_stage, RELATIVE_PATH) AS secure_access_url,
    GET_PRESIGNED_URL(@demo_files_stage, RELATIVE_PATH, 86400) AS temp_share_url_24h
FROM DIRECTORY(@demo_files_stage);

SELECT * FROM file_catalog;

-- ============================================
-- 6. USING FILE FUNCTIONS WITH STREAMS
-- ============================================
/*
EXAM NOTES:
  - Directory tables support streams for change tracking
  - Detect when new files are added to a stage
  - Combine with file functions in downstream processing
*/

-- Create a stream on the directory table
CREATE OR REPLACE STREAM new_files_stream ON STAGE demo_files_stage;

-- After adding new files and refreshing, query the stream
-- (Stream would show new/modified files)
SELECT * FROM new_files_stream;

-- ============================================
-- 7. FILE FORMAT FUNCTIONS (Related)
-- ============================================
/*
EXAM NOTES - Additional file-related functions:
  - METADATA$FILENAME     : Returns filename during COPY INTO
  - METADATA$FILE_ROW_NUMBER : Returns row number within file
  - These are metadata pseudo-columns, not URL functions
  - Available during data loading (COPY INTO)
*/

-- Example: Track which file each row came from during loading
CREATE OR REPLACE TABLE loaded_data (
    source_file STRING,
    row_num INT,
    metric STRING,
    value FLOAT
);

COPY INTO loaded_data
FROM (
    SELECT 
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        $1,
        $2
    FROM @demo_files_stage/reports/
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
FORCE = TRUE;

SELECT * FROM loaded_data ORDER BY source_file, row_num;

-- ============================================
-- CLEANUP
-- ============================================
-- DROP STREAM new_files_stream;
-- DROP VIEW file_catalog;
-- DROP TABLE loaded_data;
-- DROP STAGE demo_files_stage;
-- DROP SCHEMA SNOWPRO_DEMO.FILE_FUNCTIONS_DEMO;

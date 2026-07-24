/* ------------------------------------------------------------
   SECTION 1: SETUP - Database, Schema, File Formats
   ------------------------------------------------------------ */

-- Create a database to hold all training objects
create or replace database snowflake_training;

-- Create a schema inside the database to organize objects logically
create or replace schema train;

-- File format tells Snowflake how to parse files loaded from a stage
-- JSON format - used for semi-structured JSON files
CREATE OR REPLACE FILE FORMAT snow_json
 TYPE = JSON;

-- Parquet format - used for columnar Parquet files (common for analytics data)
CREATE OR REPLACE FILE FORMAT snow_parquet
 TYPE = PARQUET;


/* ------------------------------------------------------------
   SECTION 2: EXTERNAL STAGE SETUP (AWS S3)
   A storage integration lets Snowflake securely access an S3
   bucket using an IAM role, without hardcoding AWS keys.
   ------------------------------------------------------------ */

CREATE OR REPLACE STORAGE INTEGRATION snowflake_aws_integration
 TYPE = EXTERNAL_STAGE
 STORAGE_PROVIDER = 'S3'
 ENABLED = TRUE
 STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::448734340866:role/role_for_snf'
 STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-training-202607/');

-- View integration details (including the AWS IAM user ARN and external ID
-- that must be added to the S3 bucket's trust policy)
desc integration snowflake_aws_integration;

-- Create an external stage pointing at the S3 bucket, using the
-- integration for auth and the parquet file format by default
CREATE OR REPLACE STAGE snowflake_train_stage
 STORAGE_INTEGRATION = snowflake_aws_integration
 URL = 's3://snowflake-training-202607/'
 FILE_FORMAT = snow_parquet;

-- List all stages in the current schema/database
show stages;

-- List the files currently sitting in the external stage
list @train.snowflake_train_stage;


/* ------------------------------------------------------------
   SECTION 3: CALLING A STORED PROCEDURE
   Calls a procedure (defined later in the DEPLOY section) that
   reads files from a stage and creates a table from them.
   ------------------------------------------------------------ */

call sp_create_table_from_stage(
 {'SCHEMA':'train', 'STAGE':'snowflake_train_stage', 'TABLE_NAME':'house_pricing'}
);


/* ------------------------------------------------------------
   SECTION 4: SAMPLE TABLE FOR SNOWPARK PRACTICE
   A small hierarchical product table (parent_id references id)
   used later to demonstrate Snowpark DataFrame operations.
   ------------------------------------------------------------ */

CREATE OR REPLACE TABLE sample_product_data (
 id INT,
 parent_id INT,
 category_id INT,
 name VARCHAR,
 serial_number VARCHAR,
 key INT,
 "3rd" INT              -- quoted identifier since "3rd" starts with a digit
);

INSERT INTO sample_product_data VALUES
 (1, 0, 5, 'Product 1', 'prod-1', 1, 10),
 (2, 1, 5, 'Product 1A', 'prod-1-A', 1, 20),
 (3, 1, 5, 'Product 1B', 'prod-1-B', 1, 30),
 (4, 0, 10, 'Product 2', 'prod-2', 2, 40),
 (5, 4, 10, 'Product 2A', 'prod-2-A', 2, 50),
 (6, 4, 10, 'Product 2B', 'prod-2-B', 2, 60),
 (7, 0, 20, 'Product 3', 'prod-3', 3, 70),
 (8, 7, 20, 'Product 3A', 'prod-3-A', 3, 80),
 (9, 7, 20, 'Product 3B', 'prod-3-B', 3, 90),
 (10, 0, 50, 'Product 4', 'prod-4', 4, 100),
 (11, 10, 50, 'Product 4A', 'prod-4-A', 4, 100),
 (12, 10, 50, 'Product 4B', 'prod-4-B', 4, 100);

SELECT * FROM sample_product_data;


/* ------------------------------------------------------------
   SECTION 5: GIT INTEGRATION
   Lets Snowflake pull code directly from a GitHub repo as a
   Git-backed stage, so .sql/.py files can be executed in-place.
   ------------------------------------------------------------ */

-- Secret object stores the GitHub username/password (or PAT) securely
CREATE OR REPLACE SECRET snowf_git_secret
 TYPE = password
 USERNAME = 'sganesh'
 PASSWORD = 'TOKEN';   -- replace with a GitHub personal access token

desc secret snowf_git_secret;

-- API integration whitelists which external endpoints Snowflake can call,
-- and which secret(s) are allowed to authenticate to them
CREATE OR REPLACE API INTEGRATION git_api_integration
 API_PROVIDER = git_https_api
 API_ALLOWED_PREFIXES = ('https://github.com/sganesh200594/')
 ALLOWED_AUTHENTICATION_SECRETS = (snowf_git_secret)
 ENABLED = TRUE;

desc integration git_api_integration;

-- Git repository object = a stage-like object backed by a GitHub repo
CREATE OR REPLACE GIT REPOSITORY snowflake_train_repository
 API_INTEGRATION = git_api_integration
 GIT_CREDENTIALS = snowf_git_secret
 ORIGIN = 'https://github.com/sganesh200594/snow_train';

-- List all git repositories registered in the account
show git repositories;

-- List branches available in the repository
show git branches in git repository snowflake_train_repository;

-- Browse files inside specific branches, just like a stage
ls @snowflake_train_repository/branches/main;
ls @snowflake_train_repository/branches/test;

-- Read the raw contents of a file straight from the repo
select $1 from @snowflake_train_repository/branches/test/create_table.py;
select $1 from @snowflake_train_repository/branches/test/create_table.sql;

-- Pull the latest commits from GitHub into the Snowflake git repository object
ALTER GIT REPOSITORY snowflake_train_repository FETCH;

-- Run a .sql script directly from the repo branch
execute immediate from @snowflake_train_repository/branches/test/create_table.sql;

-- Create a stored procedure whose Python code is imported from the git repo
CREATE OR REPLACE PROCEDURE snowflake_train_git()
 RETURNS TABLE()
 LANGUAGE PYTHON
 RUNTIME_VERSION = '3.8'
 PACKAGES = ('snowflake-snowpark-python')
 IMPORTS = ('@snowflake_train_repository/branches/main/create_table.py')
 HANDLER = 'create_table.main';

call snowflake_train_git();


/* ------------------------------------------------------------
   SECTION 6: SECURE VIEWS + ROW-BASED ROLE ACCESS
   Demonstrates a secure view that returns different rows
   depending on which role is querying it (a manual pattern for
   row-level security, before native row access policies).
   ------------------------------------------------------------ */

use role SYSADMIN;

create or replace database analytics_db;
create or replace schema analytics_db.hr;

-- Base employee table
create or replace table analytics_db.hr.employees (
 employee_id number,
 first_name varchar(50),
 last_name varchar(50),
 email varchar(50),
 hire_date date,
 country varchar(50)
);

INSERT INTO analytics_db.hr.employees
 (employee_id, first_name, last_name, email, hire_date, country) VALUES
(100,'Steven','King','SKING@outlook.com','2013-06-17','US'),
(101,'Neena','Kochhar','NKOCHHAR@outlook.com','2015-09-21','US'),
(102,'Lex','De Haan','LDEHAAN@outlook.com','2011-01-13','US'),
(103,'Alexander','Hunold','AHUNOLD@outlook.com','2016-01-03','UK'),
(104,'Bruce','Ernst','BERNST@outlook.com','2017-05-21','UK'),
(105,'David','Austin','DAUSTIN@outlook.com','2015-06-25','UK'),
(106,'Valli','Pataballa','VPATABAL@outlook.com','2016-02-05','CA'),
(107,'Diana','Lorentz','DLORENTZ@outlook.com','2017-02-07','CA'),
(108,'Nancy','Greenberg','NGREENBE@outlook.com','2012-08-17','CA');

-- Mapping table: which role is allowed to see which country's rows
create or replace table analytics_db.hr.role_mapping (
 country varchar(50),
 role_name varchar(50)
);

INSERT INTO analytics_db.hr.role_mapping (country, role_name) VALUES
('US','DATA_ANALYST_US'),
('UK','DATA_ANALYST_UK'),
('CA','DATA_ANALYST_CA');

-- SECURE VIEW hides its underlying SQL definition from non-owners and
-- prevents query-optimization tricks (like injected filters) from
-- being used to bypass its logic - important for governed data.
-- Here it also filters rows so a role only sees its own country's data,
-- by comparing CURRENT_ROLE() against the role_mapping table.
create or replace secure view analytics_db.hr.vw_employees as
select a.* from employees a
join role_mapping b
 on a.country = b.country
 and current_role() = b.role_name;

-- Inspect the view's definition/metadata
desc view hr.vw_employees;

-- Create the three country-specific analyst roles
use role SECURITYADMIN;
create or replace role DATA_ANALYST_US;
create or replace role DATA_ANALYST_UK;
create or replace role DATA_ANALYST_CA;

-- Grant each role access to the database, schema, and views
use role SYSADMIN;
grant usage on database analytics_db to role DATA_ANALYST_US;
grant usage on schema analytics_db.hr to role DATA_ANALYST_US;
grant select on all views in schema analytics_db.hr to role DATA_ANALYST_US;

grant usage on database analytics_db to role DATA_ANALYST_UK;
grant usage on schema analytics_db.hr to role DATA_ANALYST_UK;
grant select on all views in schema analytics_db.hr to role DATA_ANALYST_UK;

grant usage on database analytics_db to role DATA_ANALYST_CA;
grant usage on schema analytics_db.hr to role DATA_ANALYST_CA;
grant select on all views in schema analytics_db.hr to role DATA_ANALYST_CA;

-- Warehouse usage is required to actually run queries
use role ACCOUNTADMIN;
grant usage on warehouse compute_wh to role DATA_ANALYST_US;
grant usage on warehouse compute_wh to role DATA_ANALYST_UK;
grant usage on warehouse compute_wh to role DATA_ANALYST_CA;

-- Assign the roles to the demo user
use role SECURITYADMIN;
grant role DATA_ANALYST_US to user sganesh;
grant role DATA_ANALYST_UK to user sganesh;
grant role DATA_ANALYST_CA to user sganesh;

-- Querying this view returns only rows matching the CURRENT_ROLE() in use
select * from analytics_db.hr.vw_employees;


/* ------------------------------------------------------------
   SECTION 7: DYNAMIC DATA MASKING (COLUMN-LEVEL SECURITY)
   A masking policy hides or obfuscates column data at query
   time based on the querying role - the underlying data is
   never changed.
   ------------------------------------------------------------ */

use role ACCOUNTADMIN;

CREATE OR REPLACE TABLE sensitive_data (
 id INT,
 name STRING,
 email STRING
);

INSERT INTO sensitive_data (id, name, email) VALUES
(1, 'Alice Smith', 'alice.smith@example.com'),
(2, 'Bob Johnson', 'bob.johnson@example.com'),
(3, 'Charlie Brown', 'charlie.brown@example.com');

-- Masking policy: shows real email only to FULL_ACCESS role,
-- everyone else sees a masked placeholder string
CREATE OR REPLACE MASKING POLICY email_masking_policy
AS (email STRING)
RETURNS STRING ->
 CASE
 WHEN CURRENT_ROLE() IN ('FULL_ACCESS') THEN email
 ELSE '****@*****.com'
 END;

-- Attach the policy to the email column
ALTER TABLE sensitive_data MODIFY COLUMN email SET MASKING POLICY email_masking_policy;

create or replace ROLE FULL_ACCESS;
create or replace ROLE LIMITED_ACCESS;

GRANT ROLE FULL_ACCESS TO USER sganesh;
GRANT ROLE LIMITED_ACCESS TO USER sganesh;

GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE FULL_ACCESS;
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE LIMITED_ACCESS;

GRANT USAGE ON SCHEMA HR TO ROLE FULL_ACCESS;
GRANT USAGE ON SCHEMA HR TO ROLE LIMITED_ACCESS;

grant select on table sensitive_data to role FULL_ACCESS;
grant select on table sensitive_data to role LIMITED_ACCESS;

grant usage on warehouse compute_wh to role FULL_ACCESS;
grant usage on warehouse compute_wh to role LIMITED_ACCESS;

-- FULL_ACCESS sees the real email addresses
USE ROLE FULL_ACCESS;
SELECT * FROM sensitive_data;

-- LIMITED_ACCESS sees the masked placeholder instead
USE ROLE LIMITED_ACCESS;
SELECT * FROM sensitive_data;


/* ------------------------------------------------------------
   SECTION 8: MATERIALIZED VIEWS
   Precomputes and stores query results so repeated reads of an
   aggregation are fast; Snowflake automatically refreshes it
   in the background as the base table changes.
   ------------------------------------------------------------ */

create or replace TABLE employees2 (
 id INTEGER,
 name VARCHAR(50),
 department VARCHAR(50),
 salary INTEGER
);

INSERT INTO employees2 (id, name, department, salary)
VALUES (1, 'User1', 'HR', 50000),
 (2, 'User2', 'IT', 75000),
 (3, 'User3', 'Sales', 60000),
 (4, 'User4', 'IT', 80000),
 (5, 'User5', 'Marketing', 55000);

-- Materialized view pre-aggregates total salary per department
create or replace MATERIALIZED VIEW materalized_view_employee_salaries
AS SELECT
 department,
 SUM(salary) AS total_salary
FROM employees2
GROUP BY department;

SELECT * FROM materalized_view_employee_salaries;

-- List all materialized views in the current schema/database
SHOW MATERIALIZED VIEWS;

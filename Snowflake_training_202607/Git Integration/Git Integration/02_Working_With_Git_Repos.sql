-- Git Integration Operations: Fetching, Browsing, Reading, and Executing Files

/* ============================================================================
   SNOWFLAKE GIT INTEGRATION - WORKING WITH REPOSITORIES
   ============================================================================
   SnowPro Core Exam Topics Covered:
   - Stage-like syntax for Git repositories (@repo/branches/...)
   - EXECUTE IMMEDIATE FROM (running SQL from a stage/repo)
   - Importing Python files from Git into stored procedures/UDFs
   - Reading file contents with SELECT $1
   - ALTER GIT REPOSITORY FETCH (syncing changes)
   
   PREREQUISITE: Complete 01_Git_Integration_Setup.sql first.
   ============================================================================ */


-- ============================================================================
-- SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CICD;
USE SCHEMA GIT_REPO;


-- ============================================================================
-- SECTION 1: FETCHING LATEST CHANGES
-- ============================================================================

/*
   EXAM NOTE: A Git repository clone in Snowflake is NOT auto-synced.
   You must explicitly FETCH to pull the latest commits, branches, and tags
   from the remote. FETCH also prunes deleted branches/tags.
   
   Think of it like running "git fetch --all --prune" locally.
   
   In production, you might schedule a TASK to auto-fetch periodically.
*/

-- Pull all latest branches, tags, and commits from the remote.
ALTER GIT REPOSITORY my_project_repo FETCH;


-- ============================================================================
-- SECTION 2: BROWSING REPOSITORY CONTENTS
-- ============================================================================

/*
   EXAM NOTE: A Git repository in Snowflake is referenced with @ prefix,
   just like a named stage. The path structure is:
   
   @<repo_name>/branches/<branch_name>/<file_path>
   @<repo_name>/tags/<tag_name>/<file_path>
   @<repo_name>/commits/<commit_hash>/<file_path>
   
   You can use LS (LIST) to browse, just like listing files on a stage.
*/

-- List all branches available in the repository.
SHOW GIT BRANCHES IN GIT REPOSITORY my_project_repo;


-- Browse the root directory of the 'main' branch.
-- Output columns: name, size, md5, sha1, last_modified
LS @my_project_repo/branches/main;

-- Browse a subfolder within the branch.
LS @my_project_repo/branches/main/dbscripts/;

-- Browse files in a specific tag (e.g., a release version).
-- LS @my_project_repo/tags/v1.0.0;

-- Browse files at a specific commit hash (immutable point-in-time).
-- LS @my_project_repo/commits/abc123def456;


-- ============================================================================
-- SECTION 3: READING FILE CONTENTS
-- ============================================================================

/*
   EXAM NOTE: You can SELECT from a Git repository stage to read raw file
   contents. The column alias $1 returns each line of the file as a row.
   
   This is useful for:
   - Inspecting configuration files
   - Reviewing code before executing
   - Loading data files (CSV, JSON) from the repo
*/

-- Read the raw contents of a SQL file from the main branch.
-- Each line of the file becomes one row in the result.
SELECT $1 AS file_content
FROM @my_project_repo/branches/main/dbscripts/V1__demo.sql;


-- TIP: You can also use PARSE_JSON to process JSON files inline:
-- SELECT PARSE_JSON($1) AS config
-- FROM @my_project_repo/branches/main/config/settings.json;


-- ============================================================================
-- SECTION 4: EXECUTE IMMEDIATE FROM (Run SQL directly from Git)
-- ============================================================================

/*
   EXAM NOTE: EXECUTE IMMEDIATE FROM runs a .sql file directly from a stage
   or Git repository without copying it first. This is the primary mechanism
   for CI/CD pipelines that deploy SQL scripts from version control.
   
   Key Points:
   - The file MUST contain valid SQL statements
   - Multiple statements in the file are executed sequentially
   - If any statement fails, execution stops (no automatic rollback)
   - Works with branches, tags, or commit hashes
   - Required privilege: USAGE on the Git repository stage
   
   EXAM TIP: This is how "GitOps" works in Snowflake — code is versioned
   in Git, and EXECUTE IMMEDIATE FROM deploys it to Snowflake.
*/

-- Execute a SQL script directly from the main branch.
EXECUTE IMMEDIATE FROM @my_project_repo/branches/main/dbscripts/V1__demo.sql;

-- Execute from a specific release tag (ensures reproducibility).
-- EXECUTE IMMEDIATE FROM @my_project_repo/tags/v1.0.0/sql/deploy.sql;

-- Execute from a specific commit (absolute immutability).
-- EXECUTE IMMEDIATE FROM @my_project_repo/commits/a1b2c3d4/sql/hotfix.sql;

-- Execute from a feature branch (useful during development).
-- EXECUTE IMMEDIATE FROM @my_project_repo/branches/feature/new-tables/sql/setup.sql;


-- ============================================================================
-- SECTION 5: IMPORTING PYTHON FROM GIT INTO PROCEDURES/UDFs
-- ============================================================================

/*
   EXAM NOTE: You can import Python (or Java/Scala) files from a Git repo
   into stored procedures and UDFs using the IMPORTS clause. This means
   your handler code lives in Git and Snowflake references it directly.
   
   Key Points:
   - IMPORTS = ('@repo/branches/branch/path/to/file.py')
   - HANDLER = 'module_name.function_name' (file stem + function)
   - The Python file is loaded at procedure creation time
   - To get updated code, you must FETCH + recreate the procedure
   - RUNTIME_VERSION specifies the Python version (3.8, 3.9, 3.10, 3.11)
   - PACKAGES lists pip packages available in Snowflake's Anaconda channel
*/

-- Example: Create a stored procedure whose Python handler lives in Git.
-- The Python file at /branches/main/handlers/data_processor.py must define
-- a function called "run" that accepts a Snowpark session.
CREATE OR REPLACE PROCEDURE process_data_from_git()
  RETURNS TABLE()
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@my_project_repo/branches/main/handlers/data_processor.py')
  HANDLER = 'data_processor.run';

-- Call the procedure (executes the Python code from Git).
-- CALL process_data_from_git();

-- Example: Create a UDF with imported Python logic from Git.
CREATE OR REPLACE FUNCTION transform_value(input_val STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@my_project_repo/branches/main/handlers/transformations.py')
  HANDLER = 'transformations.transform';


-- ============================================================================
-- SECTION 6: AUTOMATING FETCH WITH A TASK (CI/CD pattern)
-- ============================================================================

/*
   EXAM NOTE: You can automate repository syncing by scheduling a TASK
   that calls ALTER GIT REPOSITORY ... FETCH on a cron schedule.
   This ensures your Snowflake clone stays up-to-date with the remote.
   
   Combined with EXECUTE IMMEDIATE FROM, this creates a basic CI/CD pipeline:
   1. Developer pushes code to Git
   2. Scheduled TASK fetches latest changes
   3. Another TASK executes the deployment script
*/

-- Create a warehouse for the task (or use an existing one).
-- CREATE WAREHOUSE IF NOT EXISTS CICD_WH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;

-- Task that fetches from Git every hour.
CREATE OR REPLACE TASK fetch_git_repo_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- Every hour at :00
AS
  ALTER GIT REPOSITORY my_project_repo FETCH;

-- Task that deploys after fetch (chained via AFTER clause).
CREATE OR REPLACE TASK deploy_from_git_task
  WAREHOUSE = COMPUTE_WH
  AFTER fetch_git_repo_task  -- Runs only after the fetch task succeeds
AS
  EXECUTE IMMEDIATE FROM @my_project_repo/branches/main/sql/deploy.sql;

-- NOTE: Tasks are created in SUSPENDED state. Resume to activate.
-- ALTER TASK deploy_from_git_task RESUME;
-- ALTER TASK fetch_git_repo_task RESUME;


-- ============================================================================
-- KNOWLEDGE CHECK (SnowPro Core)
-- ============================================================================

/*
   Q1: What SQL command runs a .sql file from a Git repository?
   A:  EXECUTE IMMEDIATE FROM @repo_name/branches/branch/file.sql

   Q2: How do you sync the latest remote changes to the Snowflake clone?
   A:  ALTER GIT REPOSITORY <name> FETCH;

   Q3: What is the path format to reference a file in a Git branch?
   A:  @<repo_name>/branches/<branch_name>/<path/to/file>

   Q4: Can you reference a specific commit hash when executing code?
   A:  Yes. Use @<repo_name>/commits/<hash>/<path/to/file>

   Q5: How do you import Python from Git into a stored procedure?
   A:  Use IMPORTS = ('@repo/branches/branch/file.py') in CREATE PROCEDURE

   Q6: Are Git repository files automatically updated when remote changes?
   A:  No. You must explicitly FETCH. Auto-sync can be achieved with TASKs.

   Q7: What happens if EXECUTE IMMEDIATE FROM encounters an error mid-file?
   A:  Execution stops at the failing statement. Prior statements are NOT
       rolled back (unless they were inside an explicit transaction).
*/

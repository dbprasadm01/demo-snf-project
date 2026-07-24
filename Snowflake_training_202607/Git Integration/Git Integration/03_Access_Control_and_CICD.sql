-- Git Integration: Access Control, Best Practices, and CI/CD Patterns

/* ============================================================================
   SNOWFLAKE GIT INTEGRATION - ACCESS CONTROL, SECURITY & CI/CD
   ============================================================================
   SnowPro Core Exam Topics Covered:
   - Privilege model for Git objects (SECRET, API INTEGRATION, GIT REPOSITORY)
   - Role hierarchy and least-privilege patterns
   - Multi-environment deployment (DEV/QA/UAT/PROD)
   - OAuth vs PAT authentication
   - Git Integration limitations
   - Workspaces integration (push/pull from Snowsight)
   
   PREREQUISITE: Understand concepts from scripts 01 and 02.
   ============================================================================ */


-- ============================================================================
-- SECTION 1: ACCESS CONTROL & PRIVILEGE MODEL
-- ============================================================================

/*
   EXAM NOTE: Git Integration involves THREE object types, each with its own
   privilege requirements:

   ┌─────────────────────────────────────────────────────────────────────────┐
   │ Object             │ Level     │ Key Privileges                         │
   ├─────────────────────────────────────────────────────────────────────────┤
   │ SECRET             │ Schema    │ CREATE SECRET, USAGE, OWNERSHIP        │
   │ API INTEGRATION    │ Account   │ CREATE INTEGRATION, USAGE              │
   │ GIT REPOSITORY     │ Schema    │ CREATE GIT REPOSITORY, USAGE           │
   └─────────────────────────────────────────────────────────────────────────┘

   IMPORTANT: To CREATE an API Integration, you need CREATE INTEGRATION on the
   ACCOUNT — this is typically restricted to ACCOUNTADMIN or SYSADMIN.

   To USE a Git Repository (LS, SELECT, EXECUTE IMMEDIATE FROM), a role needs:
   - USAGE on the GIT REPOSITORY
   - USAGE on the DATABASE and SCHEMA containing the repository
   
   EXAM TIP: Roles that only need to READ from Git don't need access to the
   secret or API integration directly — just USAGE on the repository.
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE CICD;
USE SCHEMA GIT_REPO;

-- Create a custom role for CI/CD administrators who manage Git objects.
CREATE ROLE IF NOT EXISTS CICD_ADMIN;

-- Grant privileges to create and manage Git-related objects.
GRANT USAGE ON DATABASE CICD TO ROLE CICD_ADMIN;
GRANT USAGE ON SCHEMA CICD.GIT_REPO TO ROLE CICD_ADMIN;
GRANT CREATE SECRET ON SCHEMA CICD.GIT_REPO TO ROLE CICD_ADMIN;
GRANT CREATE GIT REPOSITORY ON SCHEMA CICD.GIT_REPO TO ROLE CICD_ADMIN;

-- Grant USAGE on the API integration (account-level grant).
GRANT USAGE ON INTEGRATION git_api_integration TO ROLE CICD_ADMIN;

-- Create a read-only role for developers who only need to browse/execute.
CREATE ROLE IF NOT EXISTS CICD_DEVELOPER;

GRANT USAGE ON DATABASE CICD TO ROLE CICD_DEVELOPER;
GRANT USAGE ON SCHEMA CICD.GIT_REPO TO ROLE CICD_DEVELOPER;

-- Developers can read/execute from the repo but cannot create or alter it.
-- GRANT USAGE ON GIT REPOSITORY my_project_repo TO ROLE CICD_DEVELOPER;

-- Role hierarchy: CICD_ADMIN inherits from CICD_DEVELOPER.
GRANT ROLE CICD_DEVELOPER TO ROLE CICD_ADMIN;

-- Assign CICD_ADMIN to SYSADMIN so it fits the recommended hierarchy.
GRANT ROLE CICD_ADMIN TO ROLE SYSADMIN;


-- ============================================================================
-- SECTION 2: MULTI-ENVIRONMENT DEPLOYMENT PATTERN
-- ============================================================================

/*
   EXAM NOTE: A common CI/CD pattern is to have separate databases for each
   environment (DEV, QA, UAT, PROD) and use Git branches/tags to deploy
   the appropriate version to each environment.
   
   Pattern:
   - DEV  ← deploys from branches/develop
   - QA   ← deploys from branches/release/*
   - UAT  ← deploys from tags/v*.*.* (release candidates)
   - PROD ← deploys from tags/v*.*.* (approved releases)
*/

-- Create environment databases.
CREATE DATABASE IF NOT EXISTS CICD_DEV;
CREATE DATABASE IF NOT EXISTS CICD_QA;
CREATE DATABASE IF NOT EXISTS CICD_UAT;

-- Deploy to DEV from the develop branch.
-- EXECUTE IMMEDIATE FROM @CICD.GIT_REPO.my_project_repo/branches/develop/sql/deploy.sql;

-- Deploy to QA from the release branch.
-- USE DATABASE CICD_QA;
-- EXECUTE IMMEDIATE FROM @CICD.GIT_REPO.my_project_repo/branches/release/1.0/sql/deploy.sql;

-- Deploy to UAT from a specific release tag.
-- USE DATABASE CICD_UAT;
-- EXECUTE IMMEDIATE FROM @CICD.GIT_REPO.my_project_repo/tags/v1.0.0-rc1/sql/deploy.sql;

-- Deploy to PROD from an approved release tag.
-- USE DATABASE CICD;
-- EXECUTE IMMEDIATE FROM @CICD.GIT_REPO.my_project_repo/tags/v1.0.0/sql/deploy.sql;


-- ============================================================================
-- SECTION 3: AUTHENTICATION METHODS COMPARISON
-- ============================================================================

/*
   EXAM NOTE: Snowflake supports two authentication methods for Git:

   ┌─────────────────────────────────────────────────────────────────────────┐
   │ Method            │ Use Case                │ Secret Type               │
   ├─────────────────────────────────────────────────────────────────────────┤
   │ PAT (Token)       │ Automated pipelines,    │ TYPE = PASSWORD           │
   │                   │ CI/CD, scheduled tasks   │ (username + PAT)          │
   ├─────────────────────────────────────────────────────────────────────────┤
   │ OAuth2            │ Interactive development, │ Configured in API         │
   │                   │ Workspaces (push/pull)   │ INTEGRATION directly      │
   ├─────────────────────────────────────────────────────────────────────────┤
   │ No Auth           │ Public repositories      │ No secret needed          │
   │                   │ (read-only)              │                           │
   └─────────────────────────────────────────────────────────────────────────┘

   PAT (Personal Access Token):
   - Stored in a SECRET object (TYPE = PASSWORD)
   - Token goes in the PASSWORD field
   - Best for automation (tasks, pipelines)
   - Token has an expiry — must be rotated manually

   OAuth2:
   - Configured directly in the API INTEGRATION with OAUTH endpoints
   - User signs in via browser (Snowsight prompts redirect)
   - Best for interactive Workspaces use (push/pull/commit)
   - Supports GitHub App, GitLab, Azure DevOps, Bitbucket
   - No secret rotation needed (tokens refresh automatically)

   No Authentication:
   - For public repos only (e.g., Snowflake Labs examples)
   - Omit GIT_CREDENTIALS from CREATE GIT REPOSITORY
   - Read-only access
*/

-- Example: Create a Git repo for a PUBLIC repository (no credentials needed).
-- Still requires an API integration for the allowed prefix.
CREATE OR REPLACE API INTEGRATION git_public_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
  ENABLED = TRUE;

-- No GIT_CREDENTIALS parameter — public repo, no auth needed.
CREATE OR REPLACE GIT REPOSITORY snowflake_labs_repo
  API_INTEGRATION = git_public_integration
  ORIGIN = 'https://github.com/Snowflake-Labs/sf-samples.git';

-- Fetch and browse the public repo.
ALTER GIT REPOSITORY snowflake_labs_repo FETCH;
LS @snowflake_labs_repo/branches/main;


-- ============================================================================
-- SECTION 4: WORKSPACES GIT INTEGRATION
-- ============================================================================

/*
   EXAM NOTE: Snowflake Workspaces provide a full Git experience inside
   Snowsight (the web UI). With Workspaces + Git, you can:
   
   - Create a Workspace FROM a Git repository (clones the repo)
   - Pull changes from remote into your workspace
   - Push changes from your workspace to the remote
   - Commit changes with messages
   - Switch branches
   - View diff of changes
   
   Workspaces support:
   - .sql files (SQL worksheets)
   - .py files (Python scripts)
   - .ipynb files (Notebooks)
   - dbt projects
   
   Authentication for Workspaces typically uses OAuth2 (interactive sign-in)
   rather than PAT, since it's a user-driven workflow.
   
   EXAM TIP: Workspaces are the PRIMARY way developers interact with Git
   in Snowflake for day-to-day development. The SQL-based approach (CREATE
   GIT REPOSITORY) is for automation and CI/CD pipelines.
*/


-- ============================================================================
-- SECTION 5: SUPPORTED PLATFORMS & LIMITATIONS
-- ============================================================================

/*
   SUPPORTED GIT PLATFORMS:
   - GitHub (github.com or GitHub Enterprise)
   - GitLab (gitlab.com or self-hosted)
   - Bitbucket (Cloud)
   - Azure DevOps
   - AWS CodeCommit
   - Any HTTPS-based Git server at a custom URL

   LIMITATIONS (important for exam):
   1. Only HTTPS is supported (no SSH/git:// protocol)
   2. Repository clone is READ-ONLY via SQL (write only via Workspaces)
   3. No automatic sync — must FETCH manually or via TASK
   4. File size limit applies (same as stage file limits)
   5. Binary files are stored but not directly queryable
   6. EXECUTE IMMEDIATE FROM only works with .sql files
   7. OAuth is NOT supported with outbound Private Link connections
   8. Git LFS (Large File Storage) is not supported
   9. Shallow clones are not supported — full history is fetched
   
   NETWORK OPTIONS:
   - Public network: Standard HTTPS over internet
   - Private network: Via outbound Private Link (token auth only, no OAuth)
   - Egress IP allowlisting: Available for IP-restricted Git servers
*/


-- ============================================================================
-- SECTION 6: CLEANUP (for lab environments)
-- ============================================================================

/*
   Uncomment to clean up all objects created in these demo scripts.
   In production, use with extreme caution.
*/

-- DROP GIT REPOSITORY IF EXISTS my_project_repo;
-- DROP GIT REPOSITORY IF EXISTS snowflake_labs_repo;
-- DROP SECRET IF EXISTS git_pat_secret;
-- DROP API INTEGRATION IF EXISTS git_api_integration;
-- DROP API INTEGRATION IF EXISTS git_public_integration;
-- DROP TASK IF EXISTS deploy_from_git_task;
-- DROP TASK IF EXISTS fetch_git_repo_task;
-- DROP ROLE IF EXISTS CICD_ADMIN;
-- DROP ROLE IF EXISTS CICD_DEVELOPER;
-- DROP DATABASE IF EXISTS CICD_DEV;
-- DROP DATABASE IF EXISTS CICD_QA;
-- DROP DATABASE IF EXISTS CICD_UAT;
-- DROP DATABASE IF EXISTS CICD;


-- ============================================================================
-- KNOWLEDGE CHECK (SnowPro Core)
-- ============================================================================

/*
   Q1: What privilege is needed to create an API Integration?
   A:  CREATE INTEGRATION on the ACCOUNT (typically ACCOUNTADMIN)

   Q2: Can a developer with only USAGE on a Git Repository alter/fetch it?
   A:  No. ALTER GIT REPOSITORY requires OWNERSHIP of the repository OR
       a role with USAGE on both the repository and its API integration.

   Q3: Which authentication method is best for Workspaces (interactive)?
   A:  OAuth2 (user signs in via browser, no PAT needed)

   Q4: Can Snowflake connect to Git via SSH?
   A:  No. Only HTTPS is supported.

   Q5: Is a Git Repository automatically synced when remote changes?
   A:  No. Must run ALTER GIT REPOSITORY ... FETCH (manually or via TASK).

   Q6: What does API_ALLOWED_PREFIXES do?
   A:  Restricts which URLs Snowflake can connect to. Acts as a network
       security boundary — only repos under the specified prefix are allowed.

   Q7: Can you push code to Git from SQL?
   A:  No. Push/pull/commit is only available through Workspaces (Snowsight UI).
       SQL-based Git repositories are read-only.

   Q8: What is the minimum set of objects needed for Git Integration?
   A:  1) API Integration (account-level)
       2) Git Repository (schema-level)
       Optional: Secret (only for private repos)

   Q9: Name three file types you can execute from a Git repository.
   A:  .sql (EXECUTE IMMEDIATE FROM), .py (via IMPORTS in procedures/UDFs),
       .ipynb (via Workspaces/Notebooks)

   Q10: How do you reference a file at a specific Git tag?
   A:   @<repo_name>/tags/<tag_name>/<path/to/file>
*/

-- Git Integration Setup: Secrets, API Integrations, and Git Repository creation
-- Co-authored with CoCo

/* ============================================================================
   SNOWFLAKE GIT INTEGRATION - SETUP & PREREQUISITES
   ============================================================================
   SnowPro Core Exam Topics Covered:
   - Account-level objects (Secrets, API Integrations)
   - Schema-level objects (Git Repository)
   - Access control requirements for Git Integration
   - Network connectivity (public vs private)
   
   KEY CONCEPT: Snowflake Git Integration lets you clone a remote Git repository
   into Snowflake as a special stage-like object called a "repository stage."
   Files sync from the remote repo and can be executed or imported directly.
   
   ARCHITECTURE:
   Remote Git Repo --> [API Integration + Secret] --> Git Repository (Stage) in Snowflake
   
   THREE OBJECTS REQUIRED:
   1. SECRET         - Stores credentials (username + PAT)
   2. API INTEGRATION - Defines allowed endpoints and links to the secret
   3. GIT REPOSITORY  - The actual clone (lives inside a schema)
   ============================================================================ */


-- ============================================================================
-- STEP 1: SET CONTEXT
-- ============================================================================

-- ACCOUNTADMIN is required to create API integrations.
-- In production, delegate to a custom role with CREATE INTEGRATION privilege.
USE ROLE ACCOUNTADMIN;

-- Create a dedicated database and schema to house Git objects.
-- Best practice: isolate CI/CD objects from application data.
CREATE DATABASE IF NOT EXISTS CICD;
CREATE SCHEMA IF NOT EXISTS CICD.GIT_REPO;

-- Set the working context so subsequent CREATE statements land here.
USE DATABASE CICD;
USE SCHEMA GIT_REPO;


-- ============================================================================
-- STEP 2: CREATE A SECRET (stores Git credentials securely)
-- ============================================================================

/*
   EXAM NOTE: A SECRET is a schema-level object that securely stores sensitive
   information (passwords, tokens, OAuth client secrets). Secrets are NOT visible
   in query history or logs — Snowflake redacts them.

   TYPE = PASSWORD means the secret holds a username/password pair.
   For GitHub, use a Personal Access Token (PAT) as the password.

   Other secret types: GENERIC_STRING, OAUTH2 (for OAuth flows).

   Required Privilege: CREATE SECRET on the schema (or OWNERSHIP).
*/

-- Replace '<your_github_username>' and '<your_PAT>' with real values.
-- PATs should have "repo" scope for private repos, or no scope for public repos.
CREATE OR REPLACE SECRET git_pat_secret
  TYPE = PASSWORD
  USERNAME = 'dbprasadm01'       -- GitHub username
  PASSWORD = '<your_PAT_here>';  -- Replace with your GitHub Personal Access Token (never commit real tokens)

-- Verify the secret was created. Note: PASSWORD value is always masked.
DESCRIBE SECRET git_pat_secret;

-- SHOW SECRETS lists all secrets in the current schema.
SHOW SECRETS IN SCHEMA CICD.GIT_REPO;


-- ============================================================================
-- STEP 3: CREATE AN API INTEGRATION (network + auth configuration)
-- ============================================================================

/*
   EXAM NOTE: An API INTEGRATION is an account-level object that defines:
   - Which external endpoints Snowflake is allowed to call
   - Which secrets can be used to authenticate to those endpoints
   - The provider type (git_https_api for Git)

   Key Parameters:
   - API_PROVIDER = git_https_api       --> Tells Snowflake this is for Git over HTTPS
   - API_ALLOWED_PREFIXES               --> URL prefixes Snowflake can access (security boundary)
   - ALLOWED_AUTHENTICATION_SECRETS     --> Which secrets may authenticate to this integration
   - ENABLED = TRUE                     --> Integration is active

   Required Privilege: CREATE INTEGRATION on the ACCOUNT (typically ACCOUNTADMIN).
   
   EXAM TIP: API_ALLOWED_PREFIXES is a security control. It restricts which
   repositories Snowflake can connect to. For example, 'https://github.com/myorg'
   allows any repo under that GitHub organization but blocks other orgs.
*/

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api                              -- Must be git_https_api for Git repos
  API_ALLOWED_PREFIXES = ('https://github.com/dbprasadm01') -- Restrict to your GitHub org/user
  ALLOWED_AUTHENTICATION_SECRETS = (git_pat_secret)         -- Reference the secret created above
  ENABLED = TRUE;                                           -- Activate the integration

-- Inspect the integration's properties.
DESCRIBE INTEGRATION git_api_integration;

-- List all API integrations in the account.
SHOW API INTEGRATIONS;


-- ============================================================================
-- STEP 4: CREATE A GIT REPOSITORY (the actual clone inside Snowflake)
-- ============================================================================

/*
   EXAM NOTE: A GIT REPOSITORY is a schema-level object that acts as a
   read-only stage backed by a remote Git repository. It stores a full clone
   including all branches, tags, and commits.

   Key Parameters:
   - ORIGIN              --> HTTPS URL of the remote repository (must end in .git)
   - API_INTEGRATION     --> Links to the API integration for network/auth config
   - GIT_CREDENTIALS     --> (Optional) Override secret; if omitted, uses integration default

   Required Privileges:
   - CREATE GIT REPOSITORY on the schema
   - USAGE on the API integration
   - USAGE on the secret

   EXAM TIP: The repository is NOT automatically synced. You must call
   ALTER GIT REPOSITORY ... FETCH to pull latest changes from remote.
*/

CREATE OR REPLACE GIT REPOSITORY my_project_repo
  API_INTEGRATION = git_api_integration
  GIT_CREDENTIALS = git_pat_secret
  ORIGIN = 'https://github.com/dbprasadm01/demo-snf-project.git';

-- Verify creation. Shows origin URL, integration, credentials, owner, etc.
DESCRIBE GIT REPOSITORY my_project_repo;

-- List all Git repositories in the current schema.
SHOW GIT REPOSITORIES;


-- ============================================================================
-- STEP 5: INITIAL FETCH (sync remote content to Snowflake)
-- ============================================================================

/*
   EXAM NOTE: After CREATE GIT REPOSITORY, the clone may be empty until
   you explicitly FETCH. FETCH pulls all branches, tags, and commits from
   the remote. It also prunes branches/tags that no longer exist remotely.

   Required Privilege: Ownership of the Git repository OR a role with
   USAGE on the integration that owns it.
*/

-- Pull latest from the remote repository into the Snowflake clone.
ALTER GIT REPOSITORY my_project_repo FETCH;


-- ============================================================================
-- STEP 6: VERIFY THE SETUP
-- ============================================================================

-- List branches that were fetched from the remote.
SHOW GIT BRANCHES IN GIT REPOSITORY my_project_repo;

-- Browse files in the main branch (uses stage-like @ syntax).
-- The path format is: @repo_name/branches/<branch_name>/
LS @my_project_repo/branches/main;


-- ============================================================================
-- KNOWLEDGE CHECK (SnowPro Core)
-- ============================================================================

/*
   Q1: What object type stores GitHub PAT credentials in Snowflake?
   A:  SECRET (schema-level, TYPE = PASSWORD)

   Q2: What is the API_PROVIDER value for Git integrations?
   A:  git_https_api

   Q3: Is a Git Repository an account-level or schema-level object?
   A:  Schema-level (it lives inside DATABASE.SCHEMA)

   Q4: Does creating a Git Repository automatically sync files?
   A:  No. You must run ALTER GIT REPOSITORY ... FETCH explicitly.

   Q5: What does API_ALLOWED_PREFIXES control?
   A:  It restricts which external URL prefixes Snowflake can connect to,
       acting as a security boundary for network egress.

   Q6: Can you use OAuth instead of PAT for Git authentication?
   A:  Yes. Snowflake supports OAuth2 for Git (GitHub App, GitLab, Azure DevOps,
       Bitbucket). Configure API_USER_AUTHENTICATION in the API integration.
*/

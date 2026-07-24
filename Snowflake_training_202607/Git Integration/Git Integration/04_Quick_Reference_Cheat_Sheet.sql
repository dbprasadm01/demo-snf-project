-- Git Integration Quick Reference: Commands Cheat Sheet for SnowPro Core

/* ============================================================================
   SNOWFLAKE GIT INTEGRATION - QUICK REFERENCE & CHEAT SHEET
   ============================================================================
   Use this as a rapid-review document before the SnowPro Core exam.
   All key commands, object relationships, and exam-relevant facts in one place.
   ============================================================================ */


-- ============================================================================
-- OBJECT CREATION COMMANDS (in order of dependency)
-- ============================================================================

-- 1. SECRET (schema-level) — stores credentials
CREATE OR REPLACE SECRET <secret_name>
  TYPE = PASSWORD
  USERNAME = '<git_username>'
  PASSWORD = '<personal_access_token>';

-- 2. API INTEGRATION (account-level) — defines allowed endpoints
CREATE OR REPLACE API INTEGRATION <integration_name>
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('<https_url_prefix>')
  ALLOWED_AUTHENTICATION_SECRETS = (<secret_name>)
  ENABLED = TRUE;

-- 3. GIT REPOSITORY (schema-level) — the actual clone
CREATE OR REPLACE GIT REPOSITORY <repo_name>
  API_INTEGRATION = <integration_name>
  GIT_CREDENTIALS = <secret_name>              -- optional for public repos
  ORIGIN = '<https_repo_url.git>';


-- ============================================================================
-- OPERATIONAL COMMANDS
-- ============================================================================

-- Sync latest from remote (does NOT happen automatically)
ALTER GIT REPOSITORY <repo_name> FETCH;

-- List branches
SHOW GIT BRANCHES IN GIT REPOSITORY <repo_name>;

-- List tags
SHOW GIT TAGS IN GIT REPOSITORY <repo_name>;

-- Browse files (stage-like @ syntax)
LS @<repo_name>/branches/<branch>/;
LS @<repo_name>/tags/<tag>/;
LS @<repo_name>/commits/<hash>/;

-- Read file contents
SELECT $1 FROM @<repo_name>/branches/<branch>/<path/to/file>;

-- Execute SQL file directly from Git
EXECUTE IMMEDIATE FROM @<repo_name>/branches/<branch>/<file.sql>;
EXECUTE IMMEDIATE FROM @<repo_name>/tags/<tag>/<file.sql>;
EXECUTE IMMEDIATE FROM @<repo_name>/commits/<hash>/<file.sql>;


-- ============================================================================
-- IMPORT PYTHON FROM GIT INTO PROCEDURES
-- ============================================================================

CREATE OR REPLACE PROCEDURE <proc_name>()
  RETURNS TABLE()
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@<repo_name>/branches/<branch>/<file.py>')
  HANDLER = '<module_name>.<function_name>';


-- ============================================================================
-- INSPECTION COMMANDS
-- ============================================================================

-- View secret metadata (password is ALWAYS masked)
DESCRIBE SECRET <secret_name>;
SHOW SECRETS;

-- View API integration properties
DESCRIBE INTEGRATION <integration_name>;
SHOW API INTEGRATIONS;

-- View Git repository properties
DESCRIBE GIT REPOSITORY <repo_name>;
SHOW GIT REPOSITORIES;


-- ============================================================================
-- PRIVILEGE REQUIREMENTS (EXAM CRITICAL)
-- ============================================================================

/*
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ Action                        │ Required Privilege                      │
   ├─────────────────────────────────────────────────────────────────────────┤
   │ CREATE SECRET                 │ CREATE SECRET on schema                 │
   │ CREATE API INTEGRATION        │ CREATE INTEGRATION on account           │
   │ CREATE GIT REPOSITORY         │ CREATE GIT REPOSITORY on schema         │
   │                               │ + USAGE on API integration              │
   │                               │ + USAGE on secret (if private repo)     │
   │ ALTER GIT REPOSITORY FETCH    │ OWNERSHIP of repo OR USAGE on both      │
   │                               │   repo and its API integration          │
   │ LS / SELECT / EXECUTE FROM    │ USAGE on the Git repository             │
   │ DROP GIT REPOSITORY           │ OWNERSHIP of the repository             │
   └─────────────────────────────────────────────────────────────────────────┘
*/


-- ============================================================================
-- KEY FACTS FOR THE EXAM
-- ============================================================================

/*
   1. Git Integration uses HTTPS only (no SSH, no git:// protocol)
   2. A Git Repository is a SCHEMA-LEVEL object (like a table or stage)
   3. An API Integration is an ACCOUNT-LEVEL object
   4. A Secret is a SCHEMA-LEVEL object
   5. FETCH is manual — no auto-sync from remote
   6. EXECUTE IMMEDIATE FROM works with .sql files only
   7. Python/Java files are imported via IMPORTS clause in procedures/UDFs
   8. Workspaces allow push/pull/commit (interactive Git workflow)
   9. SQL-based Git repos are READ-ONLY (no push via SQL)
  10. OAuth2 is for interactive use; PAT is for automation
  11. Public repos don't need a secret (omit GIT_CREDENTIALS)
  12. API_ALLOWED_PREFIXES acts as a security allowlist for URLs
  13. Supported platforms: GitHub, GitLab, Bitbucket, Azure DevOps, CodeCommit
  14. File path format: @repo/branches/name/ OR @repo/tags/name/ OR @repo/commits/hash/
  15. Git LFS and shallow clones are NOT supported
*/


-- ============================================================================
-- COMMON EXAM SCENARIOS
-- ============================================================================

/*
   SCENARIO 1: "A developer can't create a Git Repository"
   → Check: Do they have CREATE GIT REPOSITORY on the schema?
   → Check: Do they have USAGE on the API Integration?
   → Check: Do they have USAGE on the Secret?

   SCENARIO 2: "FETCH fails with authentication error"
   → Check: Is the PAT expired? (GitHub PATs have expiry dates)
   → Check: Does the PAT have correct scopes? (need 'repo' for private repos)
   → Check: Is the secret referenced correctly in the API integration?

   SCENARIO 3: "EXECUTE IMMEDIATE FROM returns 'file not found'"
   → Check: Did you FETCH recently? The file might be new.
   → Check: Is the branch/tag name correct? (case-sensitive)
   → Check: Is the file path correct? (use LS to verify)

   SCENARIO 4: "Need to automate Git sync"
   → Solution: Create a TASK with ALTER GIT REPOSITORY ... FETCH on a schedule
   → Chain a second TASK with EXECUTE IMMEDIATE FROM for deployment

   SCENARIO 5: "Developer needs to push code changes"
   → Solution: Use Workspaces (Snowsight UI) — SQL Git repos are read-only
   → Workspaces support OAuth2 for push/pull/commit operations
*/

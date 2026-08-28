# Starlake API Environment Variables

This document describes all environment variables that can be used to configure the Starlake API.

## Session & Cookie Configuration

| Variable | Description | Default                                                                                               |
|----------|-------------|-------------------------------------------------------------------------------------------------------|
| `SL_API_SERVER_SECRET` | Server secret for session encryption | `5678321GHJKDEUGDUYEF3567567E81ghjdzgkez6512528157621ghjgjhkdez145643247547jbhbcdjbckds78HYUHJBSJZ56` |
| `SL_API_DOMAIN` | Cookie domain | `localhost`                                                                                           |
| `SL_API_SECURE` | Enable secure cookies (HTTPS only) | `true`                                                                                                |
| `SL_API_FILE_UPLOAD_MAX_CONTENT_LENGTH` | Maximum file upload content length | `1000 MiB`                                                                                            |

## General Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_APP_TYPE` | Application type: `web`, `snowflake_native_app`, `bigquery_native_app`, `redshift_native_app` | `web` |
| `SL_API_MAX_AGE_MINUTES` | Session maximum age in minutes | `120` |
| `SL_API_MODE` | API mode: `LOCAL` (on-premise auth), `CLOUD` (cloud auth), `ALL` (both), `SAAS` (multitenant) | `LOCAL` |
| `SL_API_UI_FOLDER` | Path where static UI files are located | `/path/to/starlake-api/ui` |
| `SL_API_DAG_FOLDER` | Relative path to project-root where DAGs are generated | `dags` |
| `SL_API_DOCS_USER` | Username for accessing the docs (alias: `SL_API_DOCS_USERS`) | `starlake` |
| `SL_API_DOCS_PASSWORD` | Password for accessing the docs | `s3cret.Paw` |
| `SL_API_CLI_KEY` | Key to use for CLI authentication | `1234` |
| `SL_API_SAAS_DUCKDB_MODE` | Run all queries against DuckDB and transpile if original connector is not DuckDB | `false` |
| `SL_API_PROJECTS_ROOT` | Path where projects are located (alias: `SL_API_PROJECT_ROOT`) | `` |
| `SL_API_USER_MONTHLY_CHARGE` | Monthly charge per user in USD | `120` |
| `SL_API_SESSION_AS_HEADER` | If true, session is passed as header; if false, as cookie | `true` |
| `SL_API_YAML_VERSION` | Version of the YAML file format | `1` |
| `SL_API_MAX_USER_SPACE_MB` | Maximum space allowed for a user in MB | `1` |

## External Projects Share

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_EXTERNAL_PROJECT_SHARE_NAME` | Name of external projects share (alias: `FILESTORE_EXTERNAL_PROJECTS_SHARE_NAME`) | `external_projects` |
| `SL_API_EXTERNAL_PROJECT_SHARE_POLL_INTERVAL_MS` | Poll interval in milliseconds (alias: `FILESTORE_EXTERNAL_PROJECTS_SHARE_POLL_INTERVAL_MS`) | `1000` |

## HTTP Configuration

| Variable | Description | Default                 |
|----------|-------------|-------------------------|
| `SL_API_HTTP_INTERFACE` | Network interface to bind the API to | `0.0.0.0`               |
| `SL_API_HTTP_PORT` | Port to bind the API to | `9900`                  |
| `SL_API_HTTP_FRONT_URL` | Frontend URL used for OAuth redirects | `http://localhost:9900` |

## Orchestrator Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_ORCHESTRATOR_URL` | URL where the orchestrator is located | `http://localhost/airflow/` |
| `SL_API_ORCHESTRATOR_PRIVATE_URL` | Private URL for orchestrator | `` |
| `SL_API_ORCHESTRATOR_EXTERNAL_URL` | External URL for orchestrator | `` |
| `SL_API_ORCHESTRATOR_REDIRECT_URL` | Redirect URL for orchestrator login | `/api/v1/orchestration/login` |
| `SL_API_AIRFLOW_AUTH_CREDENTIALS` | Airflow authentication credentials (Base64 encoded) | `Basic YWlyZmxvdzphaXJmbG93` |

## Starlake Core Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_STARLAKE_CORE_PATH` | Path to starlake-core to launch starlake.sh | *required* |
| `SL_API_STARLAKE_CORE_ENV_VARS` | Environment variables passed to spark-submit command | `SPARK_DRIVER_MEMORY=1g` |
| `SL_API_STARLAKE_CORE_CONF_OPTIONS` | Configuration options (not yet implemented) | - |
| `SL_API_STARLAKE_CORE_DRIVER_OPTIONS` | Driver options (not yet implemented) | - |

## Authentication Providers

### Google OAuth

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_GOOGLE_CLIENT_ID` | Google OAuth client ID | `` |
| `SL_API_GOOGLE_CLIENT_SECRET` | Google OAuth client secret | `` |

### GitHub OAuth

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_GITHUB_CLIENT_ID` | GitHub OAuth client ID | `` |
| `SL_API_GITHUB_CLIENT_SECRET` | GitHub OAuth client secret | `` |

### Azure OAuth

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_AZURE_CLIENT_ID` | Azure OAuth client ID | `` |
| `SL_API_AZURE_CLIENT_SECRET` | Azure OAuth client secret | `` |

### General Auth

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_HOME_PAGE` | API home page URL | `` |

## JDBC Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_JDBC_URL` | URL to user management database (PostgreSQL only) | *required* |
| `SL_API_JDBC_USER` | User to connect to the user management database | *required* |
| `SL_API_JDBC_PASSWORD` | Password to connect to the user management database | *required* |
| `SL_API_JDBC_DRIVER` | JDBC driver to use | `org.postgresql.Driver` |
| `SL_API_AIRFLOW_USER` | Airflow database user | `airflow` |
| `SL_API_AIRFLOW_PASSWORD` | Airflow database password | `airflow` |

## Email Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_MAIL_FROM` | Email address to send emails from | `contact@starlake.ai` |
| `SL_API_MAIL_SUBJECT` | Default email subject | `Email Subject` |
| `SL_API_MAIL_HOST` | SMTP host | `smtp.gmail.com` |
| `SL_API_MAIL_PORT` | SMTP port | `587` |
| `SL_API_MAIL_USER` | SMTP user | `apikey` |
| `SL_API_MAIL_PASSWORD` | SMTP password | `Ebiznext000` |
| `SL_API_MAIL_TLS` | Enable TLS for SMTP | `true` |

## Git Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_GIT_COMMAND_ROOT` | Root directory for git commands | *required* |

## AI Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_AI_APPLICATION_KEY` | AI application key | `unknown` |
| `SL_API_AI_MODEL_NAMES` | Comma-separated list of AI providers to use | `openai,gemini,claude,anthropic` |
| `SL_API_AI_URL` | URL of the AI server | `http://localhost:8000` |
| `SL_API_AI_MODEL` | AI model to use | `llama3:latest` |
| `SL_API_AI_MAX_TOTAL_THREADS` | Maximum total threads for AI requests | `10` |
| `SL_API_AI_MAX_THREADS_PER_ROUTE` | Maximum threads per route for AI requests | `10` |
| `SL_API_AI_MAX_IDLE_TIME_IN_SECONDS` | Maximum idle time in seconds | `60` |

## Snowflake Orchestration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_SNOWFLAKE_DAG_DELETE_COMMAND` | Command to delete a DAG in Snowflake | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} delete'` |
| `SL_API_SNOWFLAKE_DAG_DRY_RUN_COMMAND` | Command to dry-run a DAG in Snowflake | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} dry-run'` |
| `SL_API_SNOWFLAKE_DAG_RUN_COMMAND` | Command to run a DAG in Snowflake | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} run'` |
| `SL_API_SNOWFLAKE_DAG_DEPLOY_COMMAND` | Command to deploy a DAG in Snowflake | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} deploy'` |
| `SL_API_SNOWFLAKE_DAG_BACKFILL_COMMAND` | Command to backfill a DAG in Snowflake | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} --options start_date="{{startDate}}",end_date="{{endDate}}" backfill'` |
| `SL_API_SNOWFLAKE_EMAIL_INTEGRATION` | Snowflake email integration name | `starlake_email_int` |

## Airflow Orchestration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_AIRFLOW_DAG_DELETE_COMMAND` | Command to delete a DAG in Airflow | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} delete'` |
| `SL_API_AIRFLOW_DAG_DRY_RUN_COMMAND` | Command to dry-run a DAG in Airflow | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} dry-run'` |
| `SL_API_AIRFLOW_DAG_RUN_COMMAND` | Command to run a DAG in Airflow | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} run'` |
| `SL_API_AIRFLOW_DAG_DEPLOY_COMMAND` | Command to deploy a DAG in Airflow | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} deploy'` |
| `SL_API_AIRFLOW_DAG_BACKFILL_COMMAND` | Command to backfill a DAG in Airflow | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} --options start_date="{{startDate}}",end_date="{{endDate}}" backfill'` |

## Dagster Orchestration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_API_DAGSTER_DAG_DELETE_COMMAND` | Command to delete a DAG in Dagster | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} delete'` |
| `SL_API_DAGSTER_DAG_DRY_RUN_COMMAND` | Command to dry-run a DAG in Dagster | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} dry-run'` |
| `SL_API_DAGSTER_DAG_RUN_COMMAND` | Command to run a DAG in Dagster | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} run'` |
| `SL_API_DAGSTER_DAG_DEPLOY_COMMAND` | Command to deploy a DAG in Dagster | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} deploy'` |
| `SL_API_DAGSTER_DAG_BACKFILL_COMMAND` | Command to backfill a DAG in Dagster | `bash -c 'python3.12 -m ai.starlake.orchestration --file {{file}} --options start_date="{{startDate}}",end_date="{{endDate}}" backfill'` |

## Legacy/Alias Variables

Some variables have aliases for backward compatibility:

| Primary Variable | Alias |
|-----------------|-------|
| `SL_API_DOCS_USER` | `SL_API_DOCS_USERS` |
| `SL_API_PROJECTS_ROOT` | `SL_API_PROJECT_ROOT` |
| `SL_API_EXTERNAL_PROJECT_SHARE_NAME` | `FILESTORE_EXTERNAL_PROJECTS_SHARE_NAME` |
| `SL_API_EXTERNAL_PROJECT_SHARE_POLL_INTERVAL_MS` | `FILESTORE_EXTERNAL_PROJECTS_SHARE_POLL_INTERVAL_MS` |
| `SL_API_DAG_FOLDER` | `FILESTORE_MNT_DIR` (with `/dags` suffix) |

## Notes

- Variables marked as *required* must be set for the application to start
- Empty default values (``) indicate optional configuration
- Boolean values should be set as `true` or `false`
- The `{{file}}`, `{{startDate}}`, and `{{endDate}}` placeholders in orchestration commands are replaced at runtime


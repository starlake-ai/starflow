# REST API Extraction Samples

Sample configurations for extracting data from popular SaaS APIs using `starlake extract-rest-data`.

## Prerequisites

Set the required environment variables for each API before running.

## Slack

```bash
export SLACK_BOT_TOKEN="xoxb-..."   # Bot User OAuth Token (scopes: channels:read, users:read, channels:history)

# 1. Infer schemas from API responses
starlake extract-rest-schema --config slack

# 2. Extract data to CSV
starlake extract-rest-data --config slack --outputDir /tmp/slack-data

# 3. Load into DuckDB
starlake load --domains slack
```

**Endpoints**: channels, users, messages (per channel with parent-child)

## Jira

```bash
export JIRA_DOMAIN="yourcompany"     # yourcompany.atlassian.net
export JIRA_EMAIL="you@company.com"
export JIRA_API_TOKEN="..."          # https://id.atlassian.com/manage-profile/security/api-tokens

# 1. Infer schemas from API responses
starlake extract-rest-schema --config jira

# 2. Extract data to CSV
starlake extract-rest-data --config jira --outputDir /tmp/jira-data

# 3. Load into DuckDB
starlake load --domains jira
```

**Endpoints**: projects, issues (with JQL), users, worklogs (per issue with parent-child)

## Twitter / X

```bash
export TWITTER_BEARER_TOKEN="..."     # X API v2 Bearer Token
export TWITTER_SEARCH_QUERY="starlake OR dataengineering"
export TWITTER_USERNAME="starlakeai"
export TWITTER_USER_ID="123456789"

# 1. Infer schemas from API responses
starlake extract-rest-schema --config twitter

# 2. Extract data to CSV
starlake extract-rest-data --config twitter --outputDir /tmp/twitter-data

# 3. Load into DuckDB
starlake load --domains twitter
```

**Endpoints**: recent tweet search, user profile, user tweets, followers

## Salesforce

```bash
export SF_INSTANCE="yourorg"          # yourorg.salesforce.com
export SF_CLIENT_ID="..."             # Connected App Consumer Key
export SF_CLIENT_SECRET="..."         # Connected App Consumer Secret

# 1. Infer schemas from API responses
starlake extract-rest-schema --config salesforce

# 2. Extract data to CSV (incremental via LastModifiedDate)
starlake extract-rest-data --config salesforce --outputDir /tmp/sf-data --incremental

# 3. Load into DuckDB
starlake load --domains salesforce
```

**Endpoints**: accounts, contacts, opportunities, leads, tasks, cases (all with SOQL via REST, incremental via LastModifiedDate)

## HubSpot

```bash
export HUBSPOT_ACCESS_TOKEN="..."    # Private app access token (scopes: crm.objects.contacts.read, crm.objects.companies.read, crm.objects.deals.read)

# 1. Infer schemas from API responses
starlake extract-rest-schema --config hubspot

# 2. Extract data to CSV (incremental via hs_lastmodifieddate)
starlake extract-rest-data --config hubspot --outputDir /tmp/hubspot-data --incremental

# 3. Load into DuckDB
starlake load --domains hubspot
```

**Endpoints**: companies, contacts, deals, tickets, owners, deal pipelines (all with cursor pagination, incremental via hs_lastmodifieddate)

## Stripe

```bash
export STRIPE_SECRET_KEY="sk_..."    # Secret key (sk_live_... or sk_test_...)

# 1. Infer schemas from API responses
starlake extract-rest-schema --config stripe

# 2. Extract data to CSV (incremental via created timestamp)
starlake extract-rest-data --config stripe --outputDir /tmp/stripe-data --incremental

# 3. Load into DuckDB
starlake load --domains stripe
```

**Endpoints**: customers, charges, payment intents, subscriptions, invoices, products, prices, refunds, balance transactions (all with auto-pagination via last item id)

## GitHub

```bash
export GITHUB_TOKEN="ghp_..."        # Personal Access Token (scopes: repo, read:org)
export GITHUB_ORG="starlake-ai"

# 1. Infer schemas from API responses
starlake extract-rest-schema --config github

# 2. Extract data to CSV (incremental via updated_at)
starlake extract-rest-data --config github --outputDir /tmp/github-data --incremental

# 3. Load into DuckDB
starlake load --domains github
```

**Endpoints**: repositories, pull requests (per repo), issues (per repo), members, teams (all with Link header pagination, incremental via updated_at)

## Common Options

```bash
# Incremental extraction (only new/updated records)
starlake extract-rest-data --config <api> --outputDir /tmp/data --incremental

# JSON Lines output (preserves nested structures)
starlake extract-rest-data --config <api> --outputDir /tmp/data --outputFormat jsonl

# Resume after failure
starlake extract-rest-data --config <api> --outputDir /tmp/data --resume

# Limit records (for testing)
starlake extract-rest-data --config <api> --outputDir /tmp/data --limit 100

# Load specific tables only
starlake load --domains <api> --tables table1,table2

# Load all domains at once
starlake load
```

## Cloud Storage Output

The `--outputDir` flag accepts any Hadoop-compatible path. Extracted data files and incremental state are written through Starlake's `StorageHandler`, so cloud filesystems work out of the box.

### Google Cloud Storage

```bash
starlake extract-rest-data --config salesforce --outputDir gs://my-bucket/api-data/salesforce --incremental
starlake load --domains salesforce
```

### AWS S3

```bash
starlake extract-rest-data --config stripe --outputDir s3a://my-bucket/api-data/stripe --incremental
starlake load --domains stripe
```

### Azure Blob Storage

```bash
starlake extract-rest-data --config hubspot --outputDir wasbs://container@account.blob.core.windows.net/api-data/hubspot --incremental
starlake load --domains hubspot
```

### HDFS

```bash
starlake extract-rest-data --config jira --outputDir hdfs://namenode:8020/data/api/jira --incremental
starlake load --domains jira
```

Incremental state files (`.state/{domain}/{table}.json`) are stored alongside the data in the same cloud path, so state is preserved across runs regardless of storage backend.
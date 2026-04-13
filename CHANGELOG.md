
# Release notes

# 1.5.15-SNAPSHOT:
__Improvement__:
- **Upgrade self-update**: The `starlake upgrade` command now downloads the latest starlake script from GitHub before proceeding, ensuring the upgrade logic itself is always up to date.
- **CI**: Trigger `snapshot-docker-no-tests` workflow automatically after `snapshot-jar-only` succeeds.

__Bug fix__:
- **DuckDB on Windows**: Fix JDBC URL parsing and path normalization for Windows paths.

# 1.5.14:
__Improvement__:
- **ACL export command**: New `starlake acl --export` command to export ACL/RLS definitions as YAML using Jackson serialization.
- **ProjectCompare enhancements**: Support deep nesting in diff with qualified field paths, show field names in compare report metadata changes, recurse into nested case classes.
- **Upgrade command overhaul**: Install `starlake-api` from zip archive instead of individual JARs (matching Setup.java behavior). Self-update the starlake script on upgrade before proceeding.
- **Download progress bar**: Display curl progress bar during artifact downloads in starlake.sh and starlake.cmd.
- **Windows support**: Add Windows installation instructions to README. Fix CRLF line endings for starlake.cmd. Ensure CRLF normalization in setup.ps1 after downloading starlake.cmd.
- **Bootstrap CLAUDE.md**: Add starlake CLI path guidance to bootstrap CLAUDE.md templates.
- **Default SL_API_STARLAKE_CORE_PATH**: Automatically default to starlake script location.
- **Code quality**: Close resource leaks, add custom exceptions, replace assert with require, add pool shutdown. Refactor LocalStorageHandler and HdfsStorageHandler.

__Bug fix__:
- **BigQuery foreign keys**: Check referenced table exists before adding foreign key constraint.
- **Jinjava templates**: Fix syntax replacing is-not-empty with length filter.
- **Assembly shading**: Resolve MethodTooLargeException during assembly.
- **SchemaHandler caching**: Fix caching issue in ProjectCompare.

__Dependencies__:
- Bump starlake-jdbc from 0.4 to 0.6
- Bump jSqlParser from 5.3.210 to 5.3.218
- Update DuckDB JDBC to 1.5.1.0

# 1.5.13:
__Improvement__:
- **IngestionJob refactoring**: Decomposed the monolithic IngestionJob trait into focused single-responsibility traits: IngestionAudit (audit logging), IngestionExpectations (expectations and metrics), IngestionAcl (Hive/JDBC ACL management), SparkIngestionPipeline (Spark DataFrame pipeline methods).
- **IngestionCounters.aggregate**: Extracted counter aggregation logic into a companion object method for reuse.
- **Ingestion performance**: Use localCheckpoint to ensure source files are read only once during ingestion, truncating lineage for better performance.
- **Data-driven isNativeCandidate**: Replaced repetitive match arms with Map lookup.
- **Code cleanup**: Removed unused imports in Settings.scala.
- **In-place ingestion**: New `--inPlace` flag on the `load` command to ingest files directly from their current location without moving or deleting them.
- **Bidirectional Kafka-BigQuery**: The `kafkaload` command now supports BigQuery as both a source and sink format. Use `--write-format bigquery` to offload Kafka topics to BigQuery tables (batch and streaming), or `--format bigquery` to load BigQuery tables/queries into Kafka topics. Streaming writes use `foreachBatch` for Spark 3.5+ compatibility.

__Bug fix__:
- **Upgrade command**: Derive snapshot version from Maven Central latest release instead of querying stale Sonatype snapshots repository, matching setup.sh logic (starlake.sh and starlake.cmd).
- **Temp table cleanup in native loaders**: Temporary `zztmp_` tables used during two-step native loads (DuckDB, Snowflake, BigQuery) are now cleaned up in a `finally` block, ensuring they are dropped even when the second step fails.

# 1.5.12:
__Improvement__:
- **Standalone site generation**: The `site` command now generates a self-contained responsive HTML website instead of Docusaurus MDX. Features sidebar navigation, tabbed views, and modern CSS.
- **ReactFlow diagrams**: Relations, Lineage, and Access Control tabs render interactive diagrams using ReactFlow with dagre layout (ported from starlake-ui2), replacing static SVG images.
- **SQL/Python syntax highlighting**: Code blocks in task pages display with syntax coloring (Material Oceanic palette) and line numbers.
- **Jinjava template engine**: All templates migrated from SSP (Scalate) and Mustache to Jinja2 (.j2) via Jinjava. Scalate dependency removed.
- **Setup installer**: Snapshot versions removed from the version selection menu in setup.sh/setup.ps1.

# 1.5.11:
- Code cleanup (unused imports)

# 1.5.10:
- Skipped

# 1.5.9:
__Improvement__:
- Setup auto-resolves starlake version from Maven Central when SL_VERSION is not set
- All Setup dependencies (BigQuery, Snowflake, DuckDB, etc.) are now enabled by default
- Setup proceeds immediately to installation after version selection — no interactive dependency menu
- Windows support for Setup installer

__Bug fix__:
- Fix URL query string leaking into downloaded filenames during setup
- Fix local-release.sh failing silently when sourcing bash profile under `set -u`

# 1.5.7:
__Improvement__:

__Bug fix__:
- Expectations on BigQuery should be merged using UNION ALL
- Orchestration uses now the rest instead of the gRPC api to avoid issues on some operating systems  

# 1.5.6:
__Improvement__:
- add support for inferring starlake schema from json schema file
- add agent skills
- generate documentation as json files using the format parameter in the site command. Used but starlake-site-builder to expose documentation in a website.
__Bug fix__:
- Fix foreign key display in GraphViz/ Reactflow diagrams


# 1.5.3:
__Improvement__:
- handle jsonSchema type in openapi schema extraction
- enable concurrency for duckdb connection
- add PollException to bigquery retryable exception 
- add new parameters SL_ON_EXCEPTION_RETRIES to specify the number of attempts of a retryable task on eligible exceptions
- support rename on nested fields using spark engine
- Use port 9900 for starlake serve by default to avoid conflict with minio when running locally on trino
- **BREAKING CHANGE** OPS renamed to OP
- Airflow 3 Support

__Bug fix__:
- drop empty headers or null in native loader
- ingestion failure when detailed load audit is enabled
- infer-schema with complex recursive arrays of struct
- validation on null array with spark engine



# 1.5.1:

__Improvement__:
- look at bigquery job's exceptions recursively to check for a retry
- **BREAKING CHANGE** New column has been added to the AUDIT table to handle state aware orchestration inside Snowflake Tasks

__Bug fix__:
- schema and data extraction runs in parallel again
- bigquery native load didn't merge file's DSV schema with starlake schema before checking for two steps load
- reenable string literal escape during ingestion

# 1.5.0:
__Improvement__:
- add `SL_PRUNE_PARTITION_ON_MERGE` settings to prune partition on merge. Currently for bigquery only. Recommended to be set to true for load scenarios with merge.
- add `partitionPruningKey` and `quotedPartitionPruningKey` variables in write strategy templates. They are only set when target table's partition column is one of the merge keys and feature is enabled.
- chunk log entries for gcp logs in order to avoid `INVALID_ARGUMENT: Request payload size exceeds the limit: 10485760 bytes`
- retry on `TimeoutException` wrapped into `VerifyException`
- It is now possible to run starlake in server mode without installing the API separately.

__Bug fix__:
- fix index out of bound exception when extracting table name from file name
- add rate limit exception retry during table deletion
- fix premature shutdown of parallel executions
- fix incoming data kept for current partition day only when table is partitioned with bq native and requires temporary tables. Now, it follows target expiration. Affected jobs met the criteria below and is recommended to upgrade to this version:
  - target table is partitioned
  - incoming data partitions are older than yesterday 
  - and one of:
    - any load job with scripted fields or ignored fields
    - use merge strategy: scd2, upsert_by_key, delete_then_insert, upsert_by_key_and_timestamp
    - filter incoming data
    - archiveTable
    - detailedLoadAudit is enabled with multiple files to ingest at once (SL_GROUPED set to true)

# 1.4.0

__New Feature__:
- add the ability to have ingestion audit per input file by setting SL_DETAILED_LOAD_AUDIT to true. Useful when there is too many files that generates log entry or sql query higher than the limit.

__Improvement__:
- add rawDomains variable in dag templates in order to have access to the whole domain configuration
- add rawDomain variable to filename variable.
- minimize memory usage inference-schema and adjust attributes types
- inference-schema detects more timestamp pattern
- add confluent setup that can be enabled via ENABLE_KAFKA
- when variant is detected inside a schema for bigquery, intermediate format is forced to AVRO. Indirect mode still needs to be set manually.
- **BREAKING CHANGE** flat and tree row validator have been unified and is optimized by spark
- **BREAKING CHANGE** schema inference consider Numbers starting with 0 as String, such as for company identifier
- **BREAKING CHANGE** schema inference consider Numbers starting with + as String, such as a telephone number
- Move files in parallel during ingestion phase. In order to increase parallelism, set SL_MAX_PAR_COPY. Default to 1.
- add encoding option to schema-inference

__Miscellaneous__:
- **BREAKING CHANGE** default value don't apply on empty string directly. It depends on the definition of emptyIsNull instead. So if emptyIsNull=true then default value is used
- removed RDD usage
- revamped validation phase.

__Bug fix__:
- Fix file pattern inference when last character is not a letter or digit.
- JSON type ingestion in bigquery are now created with JSON type instead of String.
- excluded table during data extraction defined in jdbcSchema are now taken into account
- if column is renamed, check pattern of renamed column instead of original name since it is the target table column's name during schema extraction
- restore time expiration for temporary tables
- **BREAKING CHANGE** when no fields could be inferred from input, inferred schema now fails


# 1.3.0
__New Feature__:
- Extract Starlake schema from OpenAPI definition
- Fallback to variant type when struct's attribute has invalid field names 

# 1.2.2
__Bug Fix__:
- fix usage of technical columns when load table is set to overwrite
- fix gcp log serialization
- fix tree validation
- fix intermediate format to orc rules
- fix conversion from spark schema to starlake schema
- fix template resolution
- fix merge statement when used with quoted column

# 1.2.1

__Bug Fix__:
- propagate final name and variant type for nested struct

# 1.2
__New Feature__:
- Custom DDL mapping for JDBC datawarehouses (Postgres, Snowflake, Redshift, Synapse ...)
- Unit tests for transforms
- Excel export
- Custom write strategies using Jinja2
- The following commands now use HDFS client to interact with files and enables cloud storage support such as S3 or GCS:
  - extract-schema
  - extract-data
  - infer-schema
  - yml2xls
- add stringPartitionFunc attribute to table extraction definition
- Support load unit tests
- add data and schema extraction from sql query for JDBC connection
- migrate command added in order to ease migration between version
- domain template used during schema extraction is now splitted in two files: domain-template and table-template. Table-template are prefixed with `_table_` and domain template may be prefixed with `_domain_` with the config file name. Ex: `_table_config.sl.yml`.
- CURRENT_DATE, CURRENT_TIME and CURRENT_TIMESTAMP are now supported on a per call level during tests
- make comet_input_file_name in bigquery native load as precise as in spark when files are grouped
- Support VIEW materialization
- Testing for load & transform

__Bug Fix__:
- Amazon RedShift write strategy templates
- Add support for Int and Short result in stringHashFunc. Some database don't support implicit cast.
- Fix incremental next query
- Fix restricted renamed column and pk renaming
- Fix comet_input_file_name usage in native bigquery load
- Inherit property from default transform

__Improvement__:
- add log during long extraction around every 30 seconds
- primary and foreign keys extraction failure are not considered as errors anymore
- make data extraction in parallel on schema's level

__Miscellaneous__:
- log level switched to error instead of info. In order to restore previous behavior, set `SL_LOG_LEVEL` to `info`.

__Deprecations removal__:
- `tables` attached to domain description or table description in load stage is not authorized starting from 1.2.0 but a config migration is automatically done for prior configs

# 1.1.1:
__Improvement__:
- add git hash or timestamp if no git info to printed SNAPSHOT version. Requires SBT reload to get new settings.

# 1.1.0:
**BREAKING CHANGE** 
- Data extraction didn't fail on table's extraction failure. In order to keep behaviour, use `--ignoreExtractionFailure`
- Default data extraction output dir is now in 'metadata/extract' instead of 'metadata/load'.
- Defining yaml config file without specifying explicitly one container root attribute is now prohibited
- In load files, can't use `schemas` anymore. Use `tables` instead.
- Default timestamp pattern for data extraction is now the iso format 'yyyy-MM-dd'T'HH:mm:ss.SSSXXX'. To restore previous behavior, set timestamp pattern to 'yyyy-MM-dd HH:mm:ss'

__Bug Fix__:
- concurrent schema extraction close resource prematurely
- fix versions.sh file for linux
- update dockerfile to take environment variable into account
- Throw expected exception when no connection ref found.
- turn missing additional columns optional on native bigquery csv data ingestion
- fix quote on data extraction when no partition is given. Failure occurs when query don't quote with '"'
- table's metadata merge during schema extraction now takes `sink`, `ignore` and `directory` attribute into account.
- Use default load format during native ingestion
- Mysql extraction could fetch wrong table's information
- Data extraction fresh enough was done on any success state, it now only consider successful extractions
- Align infered schema primitive's type with the one declared `types.sl.yml`.
- Fix dockerfile for latest Alpine by adding bash package
- Fix precedence of data extraction mode
- Handle null pointer exception on json schema inference
- Infer schema from complex json with arrays and struct

__Improvement__:
- added `auditConnectionRef` to jdbc extract schemas to be on pair with connectionRef behavior
- warn when starlake version is not installed yet and user want to use it
- added support for mysql extraction
- add the ability to rename column during schema and data extraction
- enhance user error message when defined types is not declared in types
- Schema inference for DSV files improved in type inference

__Feature__:

- generic templating framework for dag generation through the definition of a python library for starlake jobs
- load gzip compressed files (.gz extension) into bigquery
- add adaptive load and supports the following strategy: OVERWRITE, APPEND, UPSERT_BY_KEY, UPSERT_BY_KEY_AND_TIMESTAMP, OVERWRITE_BY_PARTITION

# 1.0.0:
- **BREAKING CHANGE**
  - STAGE no more used in MetricsJob and Expectations. Existing Metrics Database need to be updated.
  - Remove deprecated metadata.partition property. Now part of Sink
  - Remove deprecated metadata.xml property. Now part of metadata.options

__Feature__:
- CLI now support multiple version of starlake at once and use the correct one based on sl_versions.sh/cmd in SL_ROOT
- CLI can now upgrade all components except HADOOP extra elements on windows 
- Support any JDBC compliant database
- Add archive table support for BigQuery
- Configure CSV data extraction output format
- Amazon Redshift support
- Expectations macro support

__Improvement__:
  - Count null partition rows as rejected with dynamic partition overwrite
  - **BREAKING CHANGE** Extract-schema sanitize domain name if sanitizeName is true. Have same value for domain name and its folder. By default sanitizaName is false.
  - 'directory' is not mandatory in extract template
  - load individual domain only in extract-schema

__Bug Fix__:
- Data extraction retrieve last extraction date time but didn't get the right one for partitionned tables.

# 0.8.0:
- ** DEPRECATED **
  - All date time related variables are now deprecated aka; sl_date, sl_year ...

- **BREAKING CHANGE** 
  - extract-schema command line option 'mapping' replaced by 'config' 
  - kafkaload takes now a connection ref parameter
  - application.conf replaced with application.sl.yml or application.yml in metadata folder
  - SL_FS no more used. Set SL_ROOT to an absolute path instead
  - SL_ENGINE no more used. engine is derived from connection
  - format renamed to sparkFormat in connections.
  - COMET_* env vars replaced definitely with SL_* 
  - Sinks "name" attribute renamed to "connectionRef"
  - extensions no more used in file detection. Table patterns are directly applied to detect correct extensions 
  - Default connection ref may be defined in the application.yml file
  - Sink name in XLs files is now translated to a connection ref name
  - "domains" and "jobs" folders renamed to "load" and "transform" respectively
  - "load" and "watch" commands are now merged into one command. They both watch for new files and load them
  - globalJDBCSchema renamed to default
  - SL_DEFAULT_FORMAT renamed to SL_DEFAULT_WRITE_FORMAT
  - SINK_ACCEPTED and SINK_REJECTED  duration are not logged anymore. Only full time LOAD and TRANSFORM are logged
  - configuration files have now the .sl.yml extension
    - On Linux/MacOS, you may have to run the following command to make it work: 
    ```find . -name "*.comet.yml" -exec rename 's/\.comet.yml$/.sl.yml/' '{}' +``` # On MacOS install first with brew install rename
    - On Windows, you may have to run the following command to make it work: 
    ```Get-ChildItem -Path . -Filter "*.comet.yml" -Recurse | Rename-Item -NewName { $_.name -replace '\.comet\.yml$','.sl.yml' }```

__Feature__:
- Databricks on Azure is now fully documented
- Auto merge support added at the task level. MERGE INTO is used to merge data into the target table automatically.
- Use Refs file to configure model references
- Support native loading of data into BigQuery
- Define JDBC connections and audit connections in metadata/connections.sl.yml
- schema extraction and features relying on it benefit from parallel fetching
- use load dataset path as default output dir if not defined for schema inference
- have same file ingestion behavior as spark with big query native loader. Loader follows the same limit as bq load.
  Don't support the following ingestion phases:
  - line ignore filter
  - pre-sql
  - post-sql
  - detailed rejection
  - udf privacy
  - data validation
  - expectations
  - metrics
  - distinct on all lines
  - unique input file name with grouped ingestion

- sink become optional in spark job and can fallback into global connection ref settings
- add dynamicPartitionOverwrite sink options. Available for bigquery sink and file sink. No need to set
  spark.sql.sources.partitionOverwriteMode.


__Bug Fix__:
- **BREAKING CHANGE** the new database and tenant fields should be added to the audit table.
- forceDomainPattern renamed in order to be overridable with environment variable
- audit log was not in UTC when loaded from local
  Please run the following SQL to update your audit table on BigQuery:
```
  ALTER TABLE audit.audit ADD COLUMN IF NOT EXISTS database STRING;
  ALTER TABLE audit.audit ADD COLUMN IF NOT EXISTS tenant STRING;
```

__Feature__:
- extract-schema keep original scripted fields and merge attributes' parameters
- extract-schema quote catalog, schema and table name. 

__Deprecated__:
- **BREAKING CHANGE** Views have their own sections. Views inside jobs are now deprecated.

__Fix__:
- Take into account extensions in domain / metadata attribute
- Deserialization of privacy level was 'null' instead of its default value PrivacyLevel.None 
- log failure in audit during ingestion job when unexpected behavior occurs
- switch audit log and RLS queries as interactive to wait for job output to avoid any async exception and improve job output result accuracy
- escape string parameters while using native query

# 0.7.4:
__Deprecated__:
- **BREAKING CHANGE** Env vars that start with COMET_ are now replaced with SL_ prefix for starlake.cmd

__Feature__:
- Add SL_PROJECT and SL_TENANT env vars to be used in audit table

__Improvements__:
- retry on retryable bigquery exception: rateLimitExceeded and duplicate
- avoid table description update if it didn't change
- avoid table's column description update if it didn't change

__Bug Fix__:
- avoid swallowed exception related to BigquerySparkJob

# 0.7.3:
__Feature__:
- **BREAKING CHANGE**: Rename "cleanOnExtract" to "clean" in ExtractData CLI
- Imply SL_HIVE=true implicitly when running on Databricks
  
__Bug Fix__:
- report correct JSON accepted lines in audit and added unit test
- Resolve env vars in metadata/application.conf

# 0.7.2:
__Feature__:
- allow full export for tables and use partition column only to speed up extraction
- allow to force full export. Useful for re-init cases.
- allow to change date and timestamp format during data extraction
- allow to prevent extraction if last extraction is recent enough
- allow to clean all files of table only when it is extracted
- allow to in/exclude schemas and/or tables from extraction
- allow to filter table data before sinking it.

__Bug Fix__:
- Use audit connection settings while fetching last export and its column quotes
- Division by zero when computing progress bar
- Human Readable throw exception when elapsed time is 0
- **Breaking Change** Make table's fetchSize to have higher precedence than the one defined in jdbcSchema.
- list command for internal ressources doesn't work for yml2dag
- **Breaking Change** Make default configuration of CSV writer in extract-data to match default value of Metadata.
- keep original args in starlake.sh  when they have spaces

# 0.7.1:
__Feature__:
- add support for parallel fetch with String in some databases and give the ability to customize it.
- Add support for running as a standalone docker image on any cloud
 
__Performance__:
- disable table remarks fetch during data extraction
- add parallelism option to data extraction and fetch tables in parallel along with their partitions fetching

__Refactor__:
- rely on a csv library during data extraction

__Bug Fix__:
- use different quote for audit connection

# 0.7.0
__Deprecated__:
- Env vars that start with COMET_ are now deprecated. replace prefix with SL_

__New Feature__:
- Add Project diffs and produce HTML report
- Import existing BigQuery Datasets and Tables
- Generate Dataset and Table statistics
- upsert table description
- Support freshness in command line mode (getting ready for dependency mode)
- Extract BigQuery Tables infos to dataset_info and table_info tables
- Import BigQuery Project into external metadata folder
- Automatically switch to ORC when ingesting complex structures (array of records) into bigquery (https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/251)
- Turn schema extract pattern to a template
- Add global jdbc schema attributes in order to set common attributes once
- Always propagate domain's metadata to tables
- Order column extraction from extract-schema according to database column order
- Set domain description if given on bigquery
- Add the ability to consider empty string as valid value for required String
- Apply trim on all numeric during schema extraction. Have higher precedence than the one defined in the template.
- **BREAKING CHANGE** Add `normalized_domain` variable for schema extraction. `domain` keep the original name.
- keep user changes if set in domain's metadata or table information. Domain-template and other rules that apply to it still have higher precedence.
- Generate domain dags from yml via yml2dag command
- Env vars should start with SL_. Starting with COMET_ is snow deprecated
- Extract database data using multi threading
- Extract Database data in delta mode

__Bug Fix__:
- **BREAKING CHANGE**: Make directory mandatory only when feature require it. If you rely on exception while generating YML files from xls and vice-versa for any missing directory, you'll have to change. 
- Upsert table description for nested fields
- Restore the ability to override intermediate bq format
- Exclude specific BQ partitions when applying Merge with a BQ Table
- Apply spark options defined in the job description (sink) when saving into file
- archived Spark versions may be now referenced in starlake CLI (@sabino).
- Use gs if full uri is given even if default fs is file
- Remove lowercase on various name in extract-script in order to make it coherent with extract-schema
- remove strip comments
- **BREAKING CHANGE**: support other projectId for bqNativeJob:
- close resources for schema extraction
- escape replacement value
- **BREAKING CHANGE** domain template in extract-schema is no more interpolated since we plan to use domains accross multiple environment
- **BREAKING CHANGE** use original domain and table's name. Applies to starlake folder and audit logs. 

# 0.6.3
__New Feature__:
- Support task refs in job definition files
- Support multiple buckets for between domain files and between domain files and metadata

# 0.6.2
__Bug Fix__:
- allow bigquery job to work on a dataset of another project based on dataset name combined with projectId

__New Feature__:
- Support BigQuery IAM Policy Tags
- Support task refs in autojob file
- Support materialized views in autojob

- __ Breaking Changes__
  XLS and YML readers renamed. Breaking change if you are calling them outside the starlake command line

__Bug Fix__:
- beauty fail when no SQL is defined for a transform task
- make it build on windows
- fix quickstart bootstrap

__Build__:
- add default sbt options and force test file encoding to be UTF-8

__Doc__:
- enhance quickstart guide
- fix some typos

# 0.6.1
__ Breaking Changes__
- Extract has been refactored to 3 different scripts: extract-schema, extract-data and extract-script

# 0.6.0
__New Feature__:
- Support for Jinja templating everywhere
- area property is now ignored in YAML files
- Support for Amazon Redshift and Snowflake
- Quickstart documentation upgraded
- single command setup and run using starlake.sh / starlake.cmd
- Updated quickstart with docker use
- Infer schema now recognize date as date not timestamp

# 0.5.2
__New Feature__:
- Domain & Jobs delivery in rest api

# 0.5.1
__Bug Fix__:
- Support dynamic value for comet metadata through rest api.


# 0.5.0
__New Feature__:
- Add Server mode
__Bug Fix__:
- Extensions may be defined at the domain level

# 0.4.2
__Bug Fix__:
- Use Spark Project Jetty shaded class to remove extra jetty dependency in Starlake server

# 0.4.1
__New feature__:
- Added "serve --port 7070" to start starlake in server mode and wait for requests

# 0.4.0
__New feature__:
- Support any source to any sink using kafkaload including sink and source that are not kafka. This has been possible at the cost of a breaking change
- Support table and column remarks extraction on DB2 iSeries databases

__CI__:
 - remove support of github registry
 - Remove scala 2.11 support

# 0.3.26
__New feature__:
- Support JINJA in autojob
- Support external views defined using JINJA
- File Splitter allow to split file based on first column or position in line.

# 0.3.25
__New feature__:
- Add ACL Graph generation

# 0.3.24
__Bug Fix__:
- Improve GraphViz Generation

# 0.3.23
__Bug Fix__:
- Generate final name in Graphiz diagram

# 0.3.22
__New feature__:
- Improve cli doc generation. Extra doc can be added in docs/merge/cli folder
- prepare to deprecate xml tag in metadata section.

__Bug Fix__:
- Code improvement: JDBC is handled as a generic sink
- add extra parenthesis in BQ queries only for SELECT and WITH requests


# 0.3.21
__New feature__:
- Reduce assembly size
- Update to sbt 1.7.1
- Add interactive mode for transform with csv, json and table output formats
- Improve FS Sink handling

__Bug Fix__:
- Support empty env files

# 0.3.20
__Bug Fix__:
- Keep retrocompatibility with scala 2.11

# 0.3.19
__New feature__:
- Handle Mixed XSD / YML ingestion & validation
- Support JSON / XML descriptions in XLS files
- Support arrays in XLS files

__Bug Fix__:
- Support file system sink options in autojob

# 0.3.18
__New feature__:
- Enhance XLS support for escaping char
- Support HTTP Stream Source
- Support XSD Validation
- Transform  jobs now report on the number of affected rows.

__Bug Fix__:
- Regression return value of an autojob

# 0.3.17
__New feature__:
- Support extra dsv options in conf file
- support any option stored in metadata.options as an option for the reader.
- Support VSCode  Development


# 0.3.16
__New feature__:
- Upgrade Kafka libraries
- Simplify removal of comments in autojobs SQL statements.

# 0.3.15

__New feature__:
- deprecate usage of schema, schemaRefs in domains and dataset in autojobs. Prefer the use of table and tableRefs

__Bug Fix__:
- fix regression on Merge mode without Timestamp option

# 0.3.14
__Bug Fix__:
- Xls2Yml - Get a correct sheet name based on the schema name field

# 0.3.13
__New feature__:
- Improve XLS support for long name
- Handle rate limit exceeded by setting COMET_GROUPED_MAX to avoid HTTP 429 on some cloud providers. 

# 0.3.12
__Bug Fix__: reorder transformation on attributes as follows:
- rename columns
  - run script fields
  - apply transformations (privacy: "sql: ...")
  - remove ignore fields
  - remove input filename column

# 0.3.11 
__Bug Fix__:
- Handle field relaxation when in Append Mode and table does not exist.

# 0.3.9 / 0.3.10 / 0.3.11
__Bug Fix__:
- Make fields in rejected table optional

# 0.3.8
__New feature__:
- Rollback on support for kafka.properties files. It is useless since we already have a server-options properties.

# 0.3.7
__New feature__:
- Improve XLS support for metadata

# 0.3.6
__New feature__:
- Autoload kafka.properties file from metadata directory.

# 0.3.5
__New feature__:
- Parallel copy of files when loading and archiving
- Support renaming of domains and schemas in XLS

# 0.3.3 / 0.3.4
- Fixing release process

# 0.3.2
__New feature__:
- import step can be limited to one or more domains

# 0.3.1
__New feature__:
- Update Kafka / BigQuery libraries
- Add new  [preset env vars](https://docs.starlake.ai/configuration/environment#predefined-variables)
- Allow renaming of domains and schemas

# 0.3.0
__New feature__:
- Vars in assertions are now substituted at load time
- Support SQL statement in privacy phase
- Support parameterized semantic types
- Add support for generic sink
- Allow use of custom deserializer on Kafka source

# 0.2.10
__New feature__:
- Drop Java 1.8 prerequisite for compilation  
- Support custom database name for Hive compatible metastore
- Support custom dataset name in BQ

# 0.2.9
__New feature__:
- Drop support for Spark 2.3.X
- Allow table renaming on write
- Any Spark supported input is now allowed
- Env vars in env.yml files

# 0.2.8
__New feature__:
- Generate DDL from YML files with support for BigQuery, Snowflake, Synapse and Postgres #51 / #56
- Improve XLS handling: Add support for presql / postsql, tags, primary and foreign keys #59
- Add optional application of row & column level security
- Databricks Support
- Signification reduction of memory consumption
- Support application.conf file in metadata folder (COMET_METADATA_FS and COMET_ROOT must still be passed as env variables)

__Bug Fix__:
- Include env var and option when running presql in ingestion mode #58

# 0.2.7
__New feature__:
- Support merging dataset with updated schema
- Support publishing to github packages
- Reduce number of dependencies
- Allow Audit sink name configuration from environment variable
- Dropped support for elasticsearch 6

__Bug Fix__:
- Support timestamps as long in XML & JSOn FIles

# 0.2.6
__New feature__:
- Support XML Schema inference
- Support the ability to reject the whole file on error
- Improve error reporting
- Support engine on task SQL (query pushdown to BigQuery)
- Support last(n) partition on merge
- Added new env var to control parititioning COMET_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE
- Added env var to control BigQuery materialization on pushdown queries COMET_SPARK_BIGQUERY_MATERIALIZATION_PROJECT, COMET_SPARK_BIGQUERY_MATERIALIZATION_DATASET (default to materalization)
- Added env var to control BigQuery read data format COMET_SPARK_BIGQUERY_READ_DATA_FORMAT (default to AVRO)
- When COMET_MERGE_OPTIMIZE_PARTITION_WRITE is set and dynamic partition is active, only write partition containing new records or records to be deleted or updated for BQ (handled by Spark by default for FS).
- Add VALIDATE_ON_LOAD (comet-validate-on-load) property to raise an exception if one of the domain/job YML file is invalid. default to false
- Add custom file extensions property in Domain import ```default-file-extensions``` and env var ```COMET_DEFAULT_FILE_EXTENSIONS```
__Bug Fix__:
- Loading empty files when the schema contains script fields
- Applying default value for an attribute when value in the input data is null
- Transformation job with BQ engine fails when no views block is defined
- XLS2YML: remove non-breaking spaces from Excel file cells to avoid parsing errors
- Fix merge using timestamp option
- Json ingestion fails with complex array of objects
- Remove duplicates on incoming when existingDF does not exist or is empty
- Parse Sink options correctly 
- Handle extreme cases where audit lock raise an exception on creation
- Handle files without extension in the landing zone
- Store audit log with batch priority on BigQuery

# 0.2.4 / 0.2.5
__Bug Fix__:
- Handle [Jackson bug](https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjo9qr3v4PxAhWNohQKHfh1CqoQFjAAegQIAhAD&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F218&usg=AOvVaw02niMBgrqd-BWw7-e1YQfc)

# 0.2.3
__New feature__:
- Add ability to ignore some fields (only top level fields supported)
- **BREAKING CHANGE**: Handle multiple schemas during extraction. Update your `extract` configurations before migrating to this version.
- Improve InferSchemaJob
- Include primary keys & foreign keys in JDBC2Yml

__Bug Fix__:
- Handle rename in JSON / XML files
- Handle timestamp fields in JSON / XML files
- Do not partition rejected files
- Add COMET_CSV_OUTPUT_EXT env var to customize filename extension after ingestion when CSV is active.  

## 0.2.2
__New feature__:
- Use the same variable for Lock timeout
- Improve logging when locking file fails
- File sink while still the default is now controlled by the sink tag in the YAML file. The option sink-to-file is removed and used for testing purpose only.
- Allow custom topic name for comet_offsets
- Add ability to coalesce(int) to kafka offloading feature
- Attributes may now be declared as primary and or foreign keys even though no check is made.  
- Export schema and  relations(PK / FK) as dot (graphviz) files.
- Support saving comet offsets to filesystem instead of kafka using the new setting comet-offsets-mode = "STREAM"

__Bug Fix__:
- Invalid YAML files produce now an error at startup instead of displaying a warning.

## 0.2.1
- Version skipped

## 0.2.0
__New feature__:
- Export all tables in JDBC2Yml generation
- Include table & column names when meeting unknown column type in JDBC source schema
- Better logging on forced conversion in JDBC2Yml
- Compute Hive Statistics on Table & Partitions
- DataGrip support with implementation of substitution for ${} in addition to {{}}
- Improve logging
- Add column type during for database extraction
- The name attribute inside a job file should reflect the filename. This attribute will soon be deprecated
- Allow Templating on jobs. Useful to generate Airflow / Oozie Dags from job.sl.yml/job.sql code
- Switch from readthedocs to docusaurus
- Add local and bigquery samples
- Custom var pattern through sql-pattern-parameter in reference.conf

__Bug Fix__:
- Avoid computing statistics on struct fields
- Make database-extractor optional in application.conf


## 0.1.36
__New feature__:
- Parameterize with Domain & Schema metadata in JDBC2Yml generation 
__Bug Fix__:

## 0.1.35
__New feature__:
- Auto compile with scala 2.11 for Spark 2 and with scala 2.12 for Spark 3. [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- Performance optimization when using Privacy Rules. [[459]](https://github.com/ebiznext/comet-data-pipeline/pull/459)
- Rejected area and audit logs support can have their own write format (default-rejected-write-format and default-audit-write-format properties)
- Deep JSON & XML files are now validated against the schema
- Privacy is applied on deep JSON & XML inputs [[461]](https://github.com/ebiznext/comet-data-pipeline/pull/461)
- Domains & Jobs may be defined in subdirectories allowing better metatdata files organization [[462]](https://github.com/ebiznext/comet-data-pipeline/pull/462)
- Substitute variables through CLI & env files in views, assertions, presql, main sql and post sql requests [[462]](https://github.com/ebiznext/comet-data-pipeline/pull/462)
- Semantic type Date supports dates with _MMM_ month representation [[463]](https://github.com/ebiznext/comet-data-pipeline/pull/463)
- Split reference.conf into multiple files. [[460]](https://github.com/ebiznext/comet-data-pipeline/pull/460)
- Support kafka Source & Sink through Spark Streaming [[460]](https://github.com/ebiznext/comet-data-pipeline/pull/460)
- Add an alternative way for applying privacy on XML files.[[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Generate Excel files from YML files
- Generate YML file from Database Schema

__Bug Fix__:
- Make Jackson lib provided. [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- Support Spark 2.3. by not using Dataframe.isEmpty [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- _comet_input_file_name_ missing when ingesting Position files [[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Apply postsql queries on the accepted DataFrame [[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Check that scripted fields are defined at the end of the schema in the YML file [#384]

## 0.1.34
__New feature__:
- Allow sink options to be defined in YML instead of Spark Submit. [#450] [#454]

__Bug Fix__:
- Parse dates with yyyyMM format correctly [#451]
- Fix error when saving a csv with an empty DataFrame [#451]
- Keep column description in BQ tables when using Overwrite mode [#453]

## 0.1.29
__Bug Fix__:
- Support correctly merge mode in BQ [#449]
- Fix for sinking XML to BQ [#448]

## 0.1.27
__New feature__:
- Kafka Support improved

## 0.1.26
__New feature__:
- Optionally sink to file using property sink-to-file = ${?COMET_SINK_TO_FILE}

__Bug Fix__:
- Sink name was ignored and always considered as None

## 0.1.23
__New feature__:
- YML files are now renamed with the suffix .sl.yml
- Comet Schema is now published on SchemaStore. This allows Intellisense in VSCode & Intellij
- Assertions may now be executed as part of the Load and transform processes
- Shared Assertions UDF may be defined and stored in COMET_ROOT/metadata/assertions
- Views mays also be defined and shared in COMET_ROOT/metadata/views.
- Views are accessible in the load and transform processes.
- Domain may be now prefixed by the "load" tag. Defining a domain without the "load" tag is now deprecated
- AutoJob may be now prefixed by the "transform" tag. Defining a autojob without the "transform" tag is now deprecated

__Breaking Changes__:
- N.A.

__Bug Fix__:
- Use Spark Application Id for JobID information to make auditing easier

## 0.1.22
__New feature__:
- Expose a REST API to generate a Yaml Schema from an Excel file. [#387]
- Support ingesting multiline complex JSON. [#391]
- Support nested fields when generating schema for BigQuery tables. [#391]
- Enhancements on Spark to BigQuery schema. [#395]
- Support merging a part of a BigQuery Table, rather than all the Table. [#397]
- Enable setting BigQuery intermediate format when sinking using ${?COMET_INTERMEDIATE_BQ_FORMAT}. [#398] [#400]
- Enhancement on Merging mode: do not depend on parquet files when using BigQuery tables.

__Dependencies__:
- Update sbt to 1.4.4 [#385]
- Update scopt to 4.0.0 [#390]

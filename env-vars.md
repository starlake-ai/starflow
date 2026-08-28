# Starlake Starflow Environment Variables

This document lists all environment variables that can be used to configure Starflow.

## General Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_ROOT` | Root path for all Starflow relative paths | `/tmp` |
| `SL_APPLICATION_VERSION` | Version of the application | `0.0.1` |
| `SL_DATABASE` | Database name | `` |
| `SL_CONNECTION_REF` | Default connection name | `` |
| `SL_LOAD_CONNECTION_REF` | Load connection reference (falls back to SL_CONNECTION_REF) | `` |
| `SL_TRANSFORM_CONNECTION_REF` | Transform connection reference (falls back to SL_CONNECTION_REF) | `` |
| `SL_TENANT` | Tenant name (optional) | `` |
| `SL_HIVE_IN_TEST` | Enable Hive in test mode | `false` |
| `SL_VALIDATE_ON_LOAD` | Validate all sl.yml files on load and stop if at least one is invalid | `false` |
| `SL_REJECT_WITH_VALUE` | Add value along with the rejection error (disabled by default for security) | `false` |
| `SL_AUTO_EXPORT_SCHEMA` | Export schema to metadata folder when a table is created through a transform | `false` |
| `SL_LONG_JOB_TIMEOUT_MS` | Timeout in milliseconds for long jobs (ingestion, transform, load) | `1800000` (30 min) |
| `SL_SHORT_JOB_TIMEOUT_MS` | Timeout in milliseconds for short jobs (apply RLS, check table exists, validation, audit, expectations) | `180000` (3 min) |
| `SL_SESSION_DURATION_SERVE` | Session duration in minutes for starlake server | `10` |
| `SL_DATASETS` | Datasets path, may be relative or absolute | `${root}/datasets` |
| `SL_METADATA` | Metadata path, may be relative or absolute | `${root}/metadata` |
| `SL_DAGS` | Dags path, may be relative or absolute | `${metadata}/dags` |
| `SL_TESTS` | Tests path, may be relative or absolute | `tests` |
| `SL_TYPES` | Types path | `${metadata}/types` |
| `SL_MACROS` | Macros path | `${metadata}/macros` |
| `SL_TEST_CSV_NULL_STRING` | String to represent null in test CSV files | `null` |
| `SL_PRUNE_PARTITION_ON_MERGE` | Pre-compute incoming partitions to prune partitions on merge statement | `false` |
| `SL_WRITE_STRATEGIES` | Write strategies path | `${metadata}/write-strategies` |
| `SL_LOAD_STRATEGIES` | Load strategies path | `${metadata}/load-strategies` |
| `SL_USE_LOCAL_FILE_SYSTEM` | Do not use Hadoop HDFS path abstraction, use java file API instead | `false` |
| `SL_CREATE_SCHEMA_IF_NOT_EXISTS` | Create schema if it does not exist | `true` |
| `SL_DUCKDB_ENABLE_EXTERNAL_ACCESS` | Enable external access in DuckDB | `true` |
| `SL_DUCKDB_MODE` / `SL_DUAL_MODE` | Enable DuckDB mode | `false` |
| `SL_DUCKDB_PATH` | Path to DuckDB database file | - |
| `SL_DUCKDB_EXTENSIONS` | DuckDB extensions to load | `json,httpfs,spatial` |

## Area Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_AREA_INCOMING` / `SL_INCOMING` | Incoming area name | `incoming` |
| `SL_AREA_STAGE` | Stage area name | `stage` |
| `SL_AREA_UNRESOLVED` | Unresolved area name | `unresolved` |
| `SL_AREA_ARCHIVE` | Archive area name | `archive` |
| `SL_AREA_INGESTING` | Ingesting area name | `ingesting` |
| `SL_AREA_REPLAY` | Replay area name | `replay` |
| `SL_AREA_HIVE_DATABASE` | Hive database pattern | `${domain}_${area}` |

## Archive Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_ARCHIVE` | Archive file after load | `true` |
| `SL_ARCHIVE_TABLE` | Archive table name after load | `false` |

## Write Format Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_DEFAULT_WRITE_FORMAT` | Default format for write | `parquet` |
| `SL_DEFAULT_REJECTED_WRITE_FORMAT` | Default format for rejected write | `parquet` |
| `SL_DEFAULT_AUDIT_WRITE_FORMAT` | Default format for audit write | `parquet` |

## Error Handling Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_REJECT_ALL_ON_ERROR` | Reject all records on error | `false` |
| `SL_REJECT_MAX_RECORDS` | Max records to reject | `2147483647` |
| `SL_ON_EXCEPTION_RETRIES` | Max attempts of a retryable task on eligible exceptions | `3` |

## Lock Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_LOCK_PATH` | Lock path | `${datasets}/locks` |
| `SL_LOCK_TIMEOUT` | Lock timeout | `-1` |

## Load Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_GROUPED` | Grouped load | `false` |
| `SL_GROUPED_MAX` | Max files per grouped load | `1000000` |
| `SL_LOAD_STRATEGY` | Load strategy class | `ai.starlake.job.load.IngestionTimeStrategy` |
| `SL_SINK_REPLAY_TO_FILE` | Sink replay to file | `false` |
| `SL_CSV_OUTPUT` | Save Format in CSV with coalesce(1) | `false` |
| `SL_CSV_OUTPUT_EXT` | Save file extension when saving in CSV with coalesce(1) | `` |
| `SL_MAX_PAR_COPY` | Max parallelism for a copy job (1 means no parallelism) | `1` |
| `SL_MAX_PAR_TASK` | Max parallelism for a dag of task (1 means no parallelism) | `1` |
| `SL_FORCE_HALT` | Force JVM halt at the end of the process | `false` |
| `SL_JOB_ID_ENV_NAME` | Name of environment variable to retrieve job id from audit logs | - |

## Privacy Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_PRIVACY_ONLY` | Privacy only mode | `false` |

## SCD2 Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_SCD2_START_TIMESTAMP` | SCD2 start timestamp column name | `sl_start_ts` |
| `SL_SCD2_END_TIMESTAMP` | SCD2 end timestamp column name | `sl_end_ts` |

## Other General Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_UDFS` | User-defined functions | - |
| `SL_EMPTY_IS_NULL` | Treat empty strings as null | `true` |
| `SL_LOADER` | Loader type ("spark" or "native") | `spark` |
| `SL_ENV` | Environment name | `` |
| `SL_TIMEZONE` | Timezone | `UTC` |
| `SL_MAX_INTERACTIVE_RECORDS` | Max interactive records | `1000` |
| `SL_SYNC_YAML_WITH_DB` | Sync YAML with database | `true` |

## Access Policies Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_ACCESS_POLICIES_APPLY` | Apply access policies | `true` |
| `SL_ACCESS_POLICIES_LOCATION` | Access policies location (e.g., europe-west1, eu, us) | `invalid_location` |
| `SL_ACCESS_POLICIES_PROJECT_ID` | Access policies project ID (falls back to GCLOUD_PROJECT, SL_DATABASE) | `invalid_project` |
| `SL_ACCESS_POLICIES_TAXONOMY` | Access policies taxonomy | `invalid_taxonomy` |

## Pattern Validation Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_FORCE_VIEW_PATTERN` | Pattern for view names | `[a-zA-Z][a-zA-Z0-9_]{1,256}` |
| `SL_FORCE_DOMAIN_PATTERN` | Pattern for domain names | `[_a-zA-Z][$£a-zA-Z0-9_]{1,100}` |
| `SL_FORCE_TABLE_PATTERN` | Pattern for table names | `[_a-zA-Z][$£a-zA-Z0-9_]{1,256}` |
| `SL_FORCE_JOB_PATTERN` | Pattern for job names | `[a-zA-Z][a-zA-Z0-9_]{1,100}` |
| `SL_FORCE_TASK_PATTERN` | Pattern for task names | `[a-zA-Z][a-zA-Z0-9_]{1,100}` |

## Audit Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_AUDIT_ACTIVE` | Enable audit | `true` |
| `SL_LOCK_AUDIT_TIMEOUT` | Audit timeout | `-1` |
| `SL_DETAILED_LOAD_AUDIT` | Create individual entry for each ingested file instead of a global one | `false` |
| `SL_AUDIT_DATABASE` | Audit database | - |
| `SL_AUDIT_DOMAIN` | Audit domain | `audit` |
| `SL_AUDIT_EXPECTATIONS_DOMAIN` | Expectations audit domain (falls back to SL_AUDIT_DOMAIN) | `expectations` |
| `SL_AUDIT_REJECTED_DOMAIN` | Rejected audit domain (falls back to SL_AUDIT_DOMAIN) | `rejected` |
| `SL_AUDIT_PATH` | Audit path (FS options) | `${datasets}/audit` |
| `SL_AUDIT_SINK_CONNECTION_REF` | Audit sink connection reference (falls back to SL_CONNECTION_REF) | - |
| `SL_AUDIT_ALLOW_FIELD_ADDITION` | Allow field addition in audit | `true` |
| `SL_AUDIT_ALLOW_FIELD_RELAXATION` | Allow field relaxation in audit | `true` |
| `SL_AUDIT_SINK_PARTITIONS` | Audit sink partitions (JDBC) | `1` |
| `SL_AUDIT_SINK_BATCH_SIZE` | Audit sink batch size (JDBC) | `1000` |

## Metrics Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_METRICS_ACTIVE` | Enable metrics | `false` |
| `SL_METRICS_PATH` | Metrics path | `${datasets}/audit/metrics/{{schema}}` |
| `SL_METRICS_DISCRETE_MAX_CARDINALITY` | Discrete max cardinality | `10` |

## Expectations Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_EXPECTATIONS_ACTIVE` | Enable expectations | `true` |
| `SL_EXPECTATIONS_PATH` | Expectations path | `${datasets}/expectations/{{domain}}` |
| `SL_EXPECTATIONS_FAIL_ON_ERROR` | Fail on expectation error | `false` |

## Spark Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_SPARK_CATALOGS` | Spark catalog keys | `hive.metastore.uris,spark.sql.catalog.iceberg,...` |
| `SL_SPARK_SERIALIZER` | Spark serializer | `org.apache.spark.serializer.KryoSerializer` |
| `SL_SPARK_KRYO_REGISTRATOR` | Spark Kryo registrator | `ai.starlake.config.KryoSerialization` |
| `SL_SPARK_UI_ENABLED` | Enable Spark UI | `false` |
| `SL_SPARK_DEBUG_MAX_TO_STRING_FIELDS` | Spark debug max to string fields | `100` |
| `SL_SPARK_LOCAL_CATALOG` | Local catalog type (hive, delta, iceberg, none) | `delta` |
| `SL_SPARK_SQL_PARQUET_INT96_REBASE_MODE_IN_WRITE` | Parquet int96 rebase mode in write | `CORRECTED` |
| `SL_SPARK_SQL_PARQUET_DATETIME_REBASE_MODE_IN_WRITE` | Parquet datetime rebase mode in write | `CORRECTED` |
| `SL_SPARK_SQL_STREAMING_CHECKPOINT_LOCATION` | Streaming checkpoint location | `/tmp` |
| `SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE` | Partition overwrite mode | `DYNAMIC` |
| `SL_SPARK_BIGQUERY_VIEWS_ENABLED` | Enable BigQuery views | `true` |
| `SL_SPARK_BIGQUERY_MATERIALIZATION_PROJECT` | BigQuery materialization project | - |
| `SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET` | BigQuery materialization dataset | - |
| `SL_SPARK_BIGQUERY_READ_DATA_FORMAT` | BigQuery read data format | `avro` |
| `SL_SPARK_PY_FILES` | Spark Python files | `` |
| `SL_SPARK_DELTA_LOGSTORE_CLASS` | Delta log store class | `org.apache.spark.sql.delta.storage.LocalLogStore` |

## Spark Scheduling Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_SCHEDULING_MAX_JOBS` | Max scheduling jobs | `1` |
| `SL_SCHEDULING_MODE` | Scheduling mode | `FIFO` |
| `SL_SCHEDULING_FILE` | Scheduling file | `` |
| `SL_SCHEDULING_POOL_NAME` | Scheduling pool name | `default` |

## HTTP Service Configuration

| Variable | Description | Default     |
|----------|-------------|-------------|
| `SL_HTTP_HOST` | HTTP interface/host | `127.0.0.1` |
| `SL_HTTP_PORT` | HTTP port | `9900`      |

## Internal Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_INTERNAL_CACHE_STORAGE_LEVEL` | Cache storage level | `MEMORY_AND_DISK` |
| `SL_INTERMEDIATE_BQ_FORMAT` | Intermediate BigQuery format (parquet, orc, avro) | `parquet` |
| `SL_INTERNAL_SUBSTITUTE_VARS` | Substitute variables | `true` |
| `SL_TEMPORARY_GCS_BUCKET` / `TEMPORARY_GCS_BUCKET` | Temporary GCS bucket | - |
| `SL_BQ_AUDIT_SAVE_IN_BATCH_MODE` | Save BigQuery audit in batch mode | `true` |

## DAG Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SL_DAG_REF_LOAD` | DAG reference for load | - |
| `SL_DAG_REF_TRANSFORM` | DAG reference for transform | - |
| `SL_DAG_ACCESS_VIEWER` | DAG access for viewer role | `{"DAGs": { "can_read"}, ...}` |
| `SL_DAG_ACCESS_USER` | DAG access for user role | `{"DAGs": { "can_read", "can_edit"}, ...}` |
| `SL_DAG_ACCESS_OPS` | DAG access for ops role | `{"DAGs": { "can_read", "can_edit", "can_delete"}, ...}` |


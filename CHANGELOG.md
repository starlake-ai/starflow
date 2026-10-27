
# Release notes

# 1.7.7:
__Bug fix__:
- **Privacy is applied again when loading files**: the privacy levels that are not SQL (`HIDE`, `HIDE10X`, `APPROXLONG20`, `MD5`, `SHA1`, `SHA256`, `SHA512`, `INITIALS` and those declared in `privacy.options`) were checked against the attribute type but never applied, so these columns were loaded in clear text by every Spark load, whatever the format, since 1.5.16. The privacy is now applied to the input value of each attribute, after `trim` and `default` (including the default of a missing column), for nested fields, array elements and `variant` attributes too (stored as a JSON string), and its output is converted to the attribute type. SQL privacy (`privacy: "SQL: ..."`) is unchanged.
  - A row is rejected when the privacy engine fails on its value, when its output cannot be converted to the attribute type (typed JSON or Parquet input included), or when the output is null for a required attribute; `AcceptAllValidator` still accepts every row. An empty output, such as `HIDE`, is null for a non-string attribute. A failing engine used to make the whole load fail with a `NullPointerException`. `INITIALS` no longer fails on a value starting with spaces.
  - The native loaders (`loader: native`) only apply SQL privacy. A table with a non-SQL privacy is now loaded with Spark, with a `WARN` naming the attributes, instead of natively with clear values. Its scripts, SQL privacy and filter then run as Spark SQL, so expressions written in the dialect of the datawarehouse may have to be rewritten, and the load needs a Spark runtime that can write to the sink. The Snowflake orchestrator templates of `dag-generate`, which run the native SQL, refuse such a table with an explicit error: use a SQL privacy there.
  - The schema `filter` (except the filter of POSITION files, applied to their raw lines), scripted attributes, SQL privacy and expectations now see the transformed values.
  - **Upgrade note**: the tables loaded since 1.5.16 with a non-SQL privacy contain clear values. Reload them or transform the existing rows, then remove the clear values that remain elsewhere: time travel and table history (Delta, Iceberg, BigQuery), rejected and audit tables, downstream tables and exports. Until then, new rows are masked and old rows are not, so a merge key (`UPSERT_BY_KEY`, `SCD2`) or a join on such a column no longer matches the old rows.
  - Known limitations: the privacy of a scripted attribute is not applied (write it in the script); rejected records keep their input values; custom privacy engines do not receive the other columns of the row; typed input is transformed in its Spark string form (for example a timestamp in the session time zone).

__Improvement__:
- **`extract-bq-schema` extracts views, materialized views, external tables and snapshots on demand** (#1832): only standard tables were extracted, and every other BigQuery object was dropped without a log line, so a dataset made only of views (a BigQuery sharing linked dataset, for example) produced an empty domain. The object types to extract are now set with the `tableTypes` attribute of a `jdbcSchemas` entry of the extract config (`extract-schema` on a BigQuery connection), or with the new `--table-types` option of `extract-bq-schema`: `TABLE`, `VIEW`, `MATERIALIZED_VIEW` (or `MATERIALIZED VIEW`), `EXTERNAL` and `SNAPSHOT`, case insensitive. The schema, column descriptions and modes are read the same way whatever the object type.
  - BigQuery does not recompute the stored schema of a view when its underlying tables change, so its column types can be stale. The types and modes of a standard SQL view are therefore read from a free dry run of `SELECT *` on it, run in the dataset location and billed to the connection project, and its descriptions from its stored schema. A legacy SQL view (the dialect of the views that do not set it) keeps its stored schema, with a `WARN`: BigQuery returns no schema for a legacy SQL dry run. A `WARN` lists the columns whose stored type, parameters or mode were stale. When the dry run fails or returns no schema (no `bigquery.jobs.create` permission, a view over a table that requires a partition filter, for example), the stored schema is used, with a `WARN`. When the effective schema contains a type that cannot be converted, the view is not extracted rather than extracted with stale types.
  - The default is unchanged (`TABLE` only), but every skipped object is now logged as a `WARN` with its type and how to include it. An extract config that already sets `tableTypes`, which BigQuery used to ignore, is now honoured: `tableTypes: [TABLE, VIEW]` starts extracting views, with one dry run per view. The JDBC default table types, which a `default:` section of the extract config fills in when `tableTypes` is not set, still mean `TABLE` only on BigQuery. Types unknown to BigQuery (`SYSTEM TABLE`, ...) are ignored with a warning, and a list with no BigQuery type fails.
  - An object that was deleted since it was listed is now skipped with an error log, instead of failing the whole extraction. A final `ERROR` per dataset names the selected objects that could not be extracted; the command still succeeds.
  - `extract-schema` on BigQuery now extracts with the connection and access token it was given (extract config `connectionRef`, then `--connectionRef`, and `--accessToken`) instead of the default connection. The generated domain's BigQuery sink now carries that `connectionRef`, as `extract-bq-schema --connection` already did: change it before using the domain to load into another connection.
- **Native BigQuery loads use the BigQuery type of the attribute types** (#1836, part (a)): the tables and columns that the native BigQuery loader creates were typed from the Spark type of the primitive type, so the `ddlMapping.bigquery` of a custom type had no effect (a type mapped to `DATETIME` gave a `TIMESTAMP` column). The columns now take the BigQuery type of their attribute type mapping when it differs from the derived one, for example `DATETIME`, `TIME`, `JSON`, `GEOGRAPHY` or `STRING(n)`; `BYTES`, `INTERVAL`, `RANGE<...>` and `BYTES(n)` are accepted too. This applies to the temporary table of a two-step load, the target table it creates and the columns it adds.
  - The default types are unchanged: their mapping has the same BigQuery type as the derived one. `decimal` attributes keep the precision and scale of their zone (a `WARN` says when a decimal mapping is not applied), and the `variant` type stays `JSON` (a custom type of primitive `variant` takes its mapping, `JSON` for example).
  - A column that already exists in the target table keeps its type when it is the derived or the mapped one, with a `WARN`, since BigQuery cannot change the type of a column: recreate the column to use the mapping. A load that replaces the table (`OVERWRITE` in one step) uses the new type. Any other type difference is a type change of the attribute, rejected by BigQuery as before.
  - In the temporary table of a two-step load, the ignored, transformed and scripted attributes keep their derived type: their raw values are not loaded as such into the target. POSITION loads cast their values to the type of the target column. An archive table (`archiveTable`) keeps the types it was created with: recreate it with the target columns.
  - A mapping that is not a BigQuery column type (`STRUCT<...>`, `varchar(8000)`, `RANGE` without element type, parameters out of the BigQuery bounds) is ignored with a `WARN`.
  - Set `bigqueryNativeSchemaFromDdlMapping` (`SL_BIGQUERY_NATIVE_SCHEMA_FROM_DDL_MAPPING`) to `false` to create new columns with the derived types; the columns already created with the mapped types keep them, except in a load that replaces the table (`OVERWRITE` in one step).
  - **Behaviour change** for projects whose custom types already had such a mapping: their new tables and columns get the stricter type, and so do the existing tables of a one-step `OVERWRITE` load at their next load, while the other existing columns keep theirs: a new environment can have `DATETIME` where an older one has `TIMESTAMP`. A value that does not fit the column (a string longer than `STRING(n)`, an invalid `TIME` or `GEOGRAPHY`) is a bad record of the load, counted as rejected with the default `rejectMaxRecords`; POSITION loads store null instead (`SAFE_CAST`), and the value of a transformed or scripted attribute makes the second step fail. The scripts, SQL privacy and merge or partition SQL of a two-step load must work with the mapped type.
  - Updating the schema of an existing BigQuery table, for native loads and for the Spark loads and transforms that update their target table, no longer converts it to Spark, which failed on an unparameterized `BIGNUMERIC` or a `RANGE` column. The rule is unchanged: a nullable column stays nullable.
- **New `datetime`, `time` and `binary` types** (#1836, part (b)): a date and time without time zone, a time of day and a sequence of bytes, such as the BigQuery `DATETIME`, `TIME` and `BYTES` types, had no Starlake type. They are new primitive types, with a built-in type of the same name mapped to `DATETIME`, `TIME` and `BYTES` on BigQuery, `TIMESTAMP_NTZ`, `TIME` and `BINARY` on Snowflake, `TIMESTAMP`, `TIME` and `BYTEA` on Postgres, `datetime2`, `time` and `varbinary(max)` on Synapse, and `TIMESTAMP`, `TIME` and `BLOB` on DuckDB.
  - `datetime` is a Spark `TIMESTAMP_NTZ`. Its pattern is a date time pattern or one of `SQL_DATE_TIME` (the built-in type: a date, a space or a `T`, and a time whose seconds and fraction are optional, `2019-12-31 23:59:02.123456` for example), `BASIC_ISO_DATE`, `ISO_LOCAL_DATE`, `ISO_DATE`, `ISO_LOCAL_DATE_TIME`, `ISO_DATE_TIME`, `ISO_ORDINAL_DATE` and `ISO_WEEK_DATE`. A date without time is at the start of the day. A value with an offset or a time zone, or out of the years 1 to 9999, is rejected, and a pattern that cannot read the date and time it writes (an epoch, an offset or a time zone, no date) is not valid. A typed `timestamp` or `date` input, from a Parquet file for example, is converted in the Spark session time zone.
  - `time` has no Spark type: on Spark loads, its value is the text `HH:mm:ss`, followed by the microseconds when they are not 0. Its pattern is a time pattern or `ISO_LOCAL_TIME` (the built-in type) or `ISO_TIME`. Digits beyond the microsecond are dropped, a value with an offset is rejected, and a pattern that cannot read the time it writes is not valid. Spark cannot read the `TIME` columns of a Parquet file.
  - `binary` is a Spark `BINARY`. Its pattern is the text encoding of the values: `base64` (the built-in type, as BigQuery exports them, line breaks ignored) or `hex`.
  - The default value of a `datetime` or `binary` attribute is read with the pattern of its type, like a value.
  - Native loads read the values in the format of their engine, the pattern of the type is not applied. A table with a loaded `binary` attribute whose encoding the native engine does not decode is loaded with Spark, with a `WARN`, instead of storing other bytes: BigQuery decodes `base64` in DSV and JSON loads (not in POSITION loads), Snowflake decodes `hex`, DuckDB decodes neither, and a Parquet file holds the bytes themselves. A non-SQL privacy of a `binary` attribute is applied to the text of its value, and its output is decoded with the encoding of the type: use a SQL privacy. A Spark load into BigQuery or a JDBC database writes a `time` as a string, which a `TIME` column created by a native load refuses. Adding a `datetime` column to an existing Delta table requires its `timestampNtz` table feature.
  - The built-in types come after the types of the project, so that a project with a copy of `default.sl.yml` finds its own types first. A project type of the same name replaces the built-in one, with a `WARN` and a `validate` warning when its primitive type differs: older project templates define a `datetime` timestamp type and a `time` string type, which keep working as before. The type found for a Snowflake column from its SQL type is unchanged (`TIMESTAMP_NTZ` is still a `timestamp`, `TIME` and `BINARY` still a `string`), even in a project holding a copy of the built-in types. In `yml2ddl`, a custom type of these primitive types without a mapping for an engine takes the mapping of the first type of its primitive type that has one, the built-in type for example.
  - **Behaviour change**: a Spark load of a Parquet file with a `TIMESTAMP_NTZ` column, which failed, gives a `datetime`; infer-schema gives the `datetime` and `binary` types to the `TIMESTAMP_NTZ` and binary columns of a typed file (binary columns were `string`), or the first project type of that primitive type when the project replaces the built-in type, and keeps the name with a `WARN` when the project has no type of that primitive type; the attributes derived from the output of a Spark transform get these type names too, instead of `string` or a failure, and a Spark load or transform into BigQuery of a binary or `TIMESTAMP_NTZ` column without a Starlake schema no longer fails; `extract-bq-schema` extracts the `BYTES` columns as `binary` instead of skipping the table or view, and so do the schema sync and diff of BigQuery transforms; a SQL sync type `binary` is now a valid type. `DATETIME` and `TIME` columns: see part (c).
  - A typed value that Spark cannot cast to a `datetime`, `time`, `binary`, `date` or `timestamp` attribute, a JSON number for a `datetime` for example, is now read from its text with the pattern of the type, like a text value, instead of failing the whole load.
- **`BIGNUMERIC`, `GEOGRAPHY`, `INTERVAL` and `RANGE` columns, and an `extract-bq-schema` that never fails on a column type** (#1836, part (c)):
  - New built-in types, appended after the project types like those of part (b): `bignumeric`, a decimal of zone `76,38` mapped to BigQuery `BIGNUMERIC` and Postgres `NUMERIC`, and `geography`, `interval`, `range_date`, `range_datetime` and `range_timestamp`, strings mapped to BigQuery `GEOGRAPHY`, `INTERVAL` and `RANGE<DATE|DATETIME|TIMESTAMP>`, and to text on the other engines. A Spark load into BigQuery writes them as strings.
  - `validate` reports a decimal zone that is not `precision,scale` without spaces, which Spark ignores, whose scale is not between 0 and the precision, or whose precision is above 38 and out of the bounds of a BigQuery `BIGNUMERIC`: a scale of at most 38, a precision of at most the scale + 38. The scale of a zone was not checked.
  - For a decimal above the precision 38 of Spark, the native BigQuery loader creates a `BIGNUMERIC(p,s)` column from the zone, or an unparameterized `BIGNUMERIC` when its mapping is one, as for the built-in `bignumeric`, even when `bigqueryNativeSchemaFromDdlMapping` is `false`: it created a `NUMERIC(38,9)`, which an existing column keeps, with a `WARN`. Another mapping is not applied, with a `WARN` when the mappings are enabled. The temporary table of a two-step load holds such a column even for an ignored, transformed or scripted attribute, and POSITION loads cast the values to the column type.
  - Spark, DuckDB and Snowflake decimals have a precision of 38 at most: a Spark load, a native DuckDB or Snowflake load and the Snowflake orchestrator now refuse a table with a decimal attribute above it, scripted and nested attributes included, with an explicit message. `yml2ddl` and `starlake test` still give it the mapping of the first decimal type for an engine without one, now with a `WARN`.
  - POSITION loads read their `GEOGRAPHY` and `JSON` values, `variant` attributes included, with `ST_GEOGFROM` and `PARSE_JSON`: BigQuery cannot cast a string to these types. They build their `RANGE` values from the bounds of their text, `[lower, upper)`, a malformed text giving null: the SQL parser of the second step cannot parse a cast to `RANGE<T>`. An attribute without a BigQuery mapping no longer prevents the cast of the others.
  - `extract-bq-schema` reads the column types from the BigQuery schema instead of converting it to Spark, and no type makes it fail: `DATETIME` gives `datetime` (was `string`), `TIME` gives `time` (was `long`), `BIGNUMERIC` gives `bignumeric` (was `decimal`, and the extraction failed above the precision 38), `GEOGRAPHY` and `INTERVAL` give `geography` and `interval` (were `string`), `RANGE<...>` gives `range_date`, `range_datetime` or `range_timestamp` (failed), a repeated record of `key` and `value` fields is an array of structs (failed), and a type unknown to Starlake is a `string`. One `WARN` per table lists the parameters that the types do not keep (`STRING(n)`, `BYTES(n)`, `NUMERIC(p,s)`, `BIGNUMERIC(p,s)`) and the unknown types. When the project replaces a built-in type with a type of another primitive type, its columns get the first project type of the same primitive type, with the same BigQuery mapping if any, or keep the name when the project has none, with a `WARN`.
  - **Behaviour changes**:
    - a Spark, DuckDB or Snowflake load of a decimal above the precision 38, which stored its values with a precision of 38 and a scale of 9, now fails;
    - `validate` reports the zones it accepted although Spark could not use them, such as `10,12`, `50,5` or `10, 2`;
    - a project type named like a new built-in type replaces it, with a `WARN` when its primitive type differs;
    - the types above change in the output of `extract-bq-schema`, and in the schema sync and diff of BigQuery transforms, which use the same extraction, and the tables and views that failed are extracted. A `BIGNUMERIC(p,s)` column gives `bignumeric`, which only the native BigQuery loader loads, and an extracted `NUMERIC` column loads into a `NUMERIC(38,9)`: the same type, with its parameters written.
- **Native BigQuery load of Parquet files** (#1833): with `loader: native`, a `PARQUET` table was loaded by Spark (`Using spark as ingestion engine`), so a Spark runtime read every byte that a BigQuery load job reads by itself. It is now loaded by BigQuery load jobs, like `DSV`, `JSON`, `JSON_FLAT` and `POSITION`, with no Spark runtime. A table exported by `EXPORT DATA (format = 'PARQUET')`, with its `JSON`, `INTERVAL` and `RANGE` columns exported as strings, loads with its schema and values into the domain that `extract-bq-schema` extracts from it.
  - The load always gives BigQuery the schema of the table YAML, so the columns get the types of the contract instead of types inferred from the files: a schema-less load reads a `DATETIME` as a `TIMESTAMP` and narrows a `BIGNUMERIC` to a `NUMERIC`. Parquet columns are matched by name, ignoring case, with the source names of the attributes, and the columns that no attribute declares are ignored. From Cloud Storage, Parquet `LIST` columns, as `EXPORT DATA` and Spark write them, load as repeated fields (list inference), and `ENUM` columns as strings.
  - Top level `JSON`, `INTERVAL` and `RANGE` columns are read from the files as strings, then converted by the second step (`PARSE_JSON`, `CAST`, or the bounds of a `RANGE`): `EXPORT DATA` does not export `JSON` and `INTERVAL` columns to Parquet (`Type ... is not currently supported for parquet exports`), and Parquet loads document no conversion to these types. Export them as strings, `TO_JSON_STRING(column)` for `JSON` and `CAST(column AS STRING)` for `INTERVAL` and `RANGE`, whose texts the load reads. An invalid value fails the load.
  - Besides the usual cases, the load runs in two steps (a temporary table, then the SQL of the write strategy) when the table has a required attribute, at any level, a top level attribute of type `variant` or mapped to `JSON`, `INTERVAL` or `RANGE`, a `presql` or a `postsql`. The first step reads every column as nullable, and the second step rejects the rows without a value for a required attribute, at any level (a required field of a null record has no value to check): they are counted as rejected, as the bad records of the other native formats, and are not stored in the rejected table. The load fails instead when there are more than `rejectMaxRecords` of them over its files, or on the first one with `rejectAllOnError`; a load whose rows are all rejected succeeds, and an `OVERWRITE` then empties the table, as with Spark. Counting them takes one more query on the temporary tables. In two steps, each file or URI is loaded into its own temporary table, which records its name: a wildcard URI loads the files of an export with a single job.
  - A Parquet load job cannot add or relax columns: BigQuery refuses `ALLOW_FIELD_ADDITION` with `ignoreUnknownValues` for this format. Before a one-step `APPEND` into an existing table, the loader adds the new columns of the YAML to the table, at any level, and makes nullable the required columns that the YAML makes nullable.
  - Local files are uploaded through a single stream per load job, without the Parquet options: several local Parquet files are loaded by one job each, in two steps.
  - BigQuery cannot skip the other bad rows of a Parquet file: a value that does not fit its column fails the load, whatever `rejectMaxRecords`. Like the other native loads, it does not apply `emptyIsNull` or the patterns of the types, and the Parquet columns must have the type of their attribute: BigQuery does not parse a date from a string column, for example.
  - **Behaviour change** for projects with `loader: native` and a BigQuery sink: their Parquet tables were loaded by Spark and are now loaded natively. A row with a value that does not fit its column now fails the load instead of being rejected (a row without a value for a required attribute is still rejected, but unlike Spark, a null record with a required field or a null required array is accepted, as BigQuery does, and the rejected rows are not stored in the rejected table), and their scripts, SQL privacy and filter now run as BigQuery SQL, as for the other native formats, so expressions written in Spark SQL may have to be rewritten. Set `loader: spark` on a table to keep loading it with Spark. A Parquet table that uses a feature that a BigQuery load job would not apply as Spark does keeps being loaded by Spark, with a `WARN` naming its attributes: Spark read `options` (`format: orc`, `mergeSchema`...), array attributes in local files, a rename below the top level, a `default`, a `trim`, or nested or repeated `JSON`, `INTERVAL` or `RANGE` fields.
- **BigQuery transforms write their files with `EXPORT DATA`** (#1834): a transform running on BigQuery whose sink is a file sink was run by Spark, which read its rows through the Spark BigQuery connector, with a materialization dataset in the source location, and wrote the files from its executors. When the sink `path` is on Cloud Storage (`gs://`, after Jinja) and its `format` is `parquet`, `avro` or `json`, the transform now runs as an `EXPORT DATA` statement in the location of its connection, with no Spark runtime. From a dataset of views, it is the export of a relocation to another location, before a native Parquet load there (#1833).
  - The files of a table are written to the folder of its domain under the `path`, named after the table, the run and their number: `<path>/<domain>/<table>-<run>-000000000000.parquet`, which a table pattern such as `<table>-.*\.parquet` stages and loads. A `path` with a single `*` in the file name is the URI of the files of the task, the run being inserted before the number (`gs://bucket/folder/orders_*.parquet`): do not set such a path in a default shared by several tasks. The extension is the sink `extension`, or the one of the format.
  - The run is the scheduled date of the transform (`--scheduledDate`), or its start time, followed by a random suffix, so that a run overwrites no file of another. Once the export succeeded, `OVERWRITE` deletes the files of the previous runs, and `APPEND` the files of a previous run of the same scheduled date, which it replaces; the other files of the folder are kept. A failed run deletes the files it wrote, so that the folder keeps the files of the last successful runs. This needs the permission to list and delete the objects of the folder, with the credentials of the run connection or the access token of the run. `EXPORT DATA` overwrites the files of the same names and deletes none.
  - The other write strategies, and a sink `partition`, are refused by `validate` and fail the task. The sink `clustering` and `coalesce`, the ACL, the RLS and the expectations do not apply to files: they are ignored, with a `WARN`. Python tasks keep running with Spark.
  - The sink options `compression` and, for Avro files, `use_avro_logical_types` are options of the `EXPORT DATA` statement; the other options of the sink are ignored, with a `WARN`. Parquet and Avro files are compressed with Snappy by default, as Spark writes them: set `compression: none` for uncompressed files. The files are written by BigQuery in its own way: Avro files use logical types for dates and times, and JSON files write `INT64` values as strings.
  - `EXPORT DATA` does not export `JSON`, `INTERVAL` and `RANGE` columns to Parquet, nor `RANGE` columns to Avro and JSON. The top level columns of these types that are not repeated, read from a dry run of the SELECT, are exported as text (`TO_JSON_STRING`, `CAST(... AS STRING)`), which the native Parquet load converts back; a SQL `NULL` stays `NULL`. The other ones still fail the export, and so do these columns when the SELECT needs the presql, with a `WARN`.
  - The presql and postsql run in the same script as the export. The audit log counts the exported rows, and its message gives the number of files and the URI. A dry run (`--dry-run`) checks the statement and prints the bytes it would process, without deleting any file, and `transform --compile` prints the `EXPORT DATA` statement.
  - `csv` and `xls` exports, and the file sinks without a `path` on Cloud Storage, keep running with Spark. The sink `path`, documented in the JSON schema but dropped when the YAML was read, is now read: it only sets where BigQuery exports the files, the `csv` and `xls` exports keep the file of their `csv:path` and `xls:path` options.
  - **Behaviour change**: a BigQuery transform that has a Cloud Storage `path` with a Parquet, Avro or JSON format, which Spark wrote to a table of the warehouse, now writes its files to that path.
- **Native BigQuery loads keep their rejected rows, as the Spark loads do** (#1856): they only counted them, so the audit `rejected` table stayed empty and the rejected rows were lost with the temporary tables.
  - The rows of a Parquet load without a value for a required attribute (#1833) are written to the audit `rejected` table, at most `audit.maxErrors` of them, with the columns and the error text of the Spark loads: `ERR -> `, one line per attribute without a value (`` `record.code`  can't be null ``, without the index of the element of a repeated record), then `FILE -> ` and the file or URI of the load. With `rejectWithValue`, the row follows as JSON (`ROW -> {...}`), without the values of the ignored attributes and of the attributes with a privacy, still in clear before the second step.
  - With `sinkReplayToFile`, all of them are exported by BigQuery to Parquet replay files of the replay area of the domain, `<domain>.<table>.<timestamp>.replay-<id>-000000000000.parquet`, ...: they have the columns of the input files, so that the same table loads them again once corrected. BigQuery exports to Cloud Storage only: when the replay area is elsewhere, the files are exported to the `temporaryGcsBucket` or the `gcsBucket` of the connection, or to the `TEMPORARY_GCS_BUCKET` of the application, then moved there; without such a bucket, a `WARN` says that they are not written.
  - The bad records that a POSITION load job skipped, or that made a CSV, JSON or POSITION load job fail, are written with the message of BigQuery and their file. BigQuery reports a few of them only, and not their rows: those that a CSV or JSON load job skipped are found as rows (#1857, below). Its messages may quote the bad values, whatever `rejectWithValue`.
  - As with Spark, the rejected rows are kept before the load fails because of `rejectAllOnError` or `rejectMaxRecords`, and a failure to write them fails the load, before anything is loaded. The bad records of a one-step load are only known once its rows are written: a failure to write them is logged as an `ERROR` instead. The counts of the audit log are unchanged.
  - A BigQuery or JDBC audit sink is written with a parameterized `INSERT`, the audit table being created when missing, and a Cloud Logging one directly: none of them needs Spark. A file audit sink is written with Spark, as for the Spark loads. These writes are interactive whatever `bqAuditSaveInBatchMode`, so that their failure is known.
- **Native BigQuery loads of `DSV`, `JSON` and `JSON_FLAT` files find their rejected rows** (#1857): a BigQuery load job skips the bad records of these files and only counts them, reporting at most five of them and never their rows, so the audit `rejected` table only had BigQuery's messages and no replay file could be written.
  - When a load job of these formats succeeds with bad records, its files are loaded again into temporary tables of the dataset of the target, each CSV value or JSON line as a string, with the same CSV options. A query then finds the rows that the load job rejected, by applying to them the rules that BigQuery documents for its load jobs, for the type of each column of the load: `BOOL` as 1/0, true/false, t/f, yes/no or y/n in any case, `INT64` but in hexadecimal, `BYTES` in base64, a `TIMESTAMP` with a date and a time, a slash date or a Unix epoch, `GEOGRAPHY` in WKT, WKB or GeoJSON (a string in JSON files), `INTERVAL` in its canonical text in CSV files and in ISO 8601 in JSON files, the canonical text of `RANGE`, the length of `STRING(L)` and `BYTES(L)`, at most S decimals and P - S integer digits for `NUMERIC(P, S)` and `BIGNUMERIC(P, S)` (9 and 29 for `NUMERIC`: load jobs do not round), the other types as `SAFE_CAST` reads them. Without a null marker, an empty CSV value, quoted or not, is null in the columns of other types than `STRING` and `BYTES`. A JSON line must be a JSON object, whose fields have a value when they are required, an object when they are records and an array without null element when they are repeated, at every level. The rows that the temporary load rejects too have structural errors, such as an unterminated quote: they are only counted, with the message of BigQuery.
  - The rows found are written to the audit `rejected` table instead of the messages of BigQuery, at most `audit.maxErrors` of them, before the messages of the structural errors: `ERR -> `, one line per invalid attribute (`` `amount` is not a valid FLOAT64 ``, `` `id`  can't be null ``, `` `tags` holds a null element ``, `The line is not valid JSON`...), then `FILE -> ` and the file of the row. With `rejectWithValue`, the row follows as JSON (`ROW -> {...}`): the line of a JSON file, the values of a CSV row as strings. Without the values of the ignored attributes and of the attributes with a privacy: a JSON line is then rebuilt from the fields that the attributes declare, in their case, its wide numbers rounded.
  - With `sinkReplayToFile`, all of them are written to a replay file of the replay area of the domain, `<domain>.<table>.<timestamp>.replay-<id>.csv` or `.json`, so that the same table loads them again once corrected: the lines of a JSON file as they are, the rows of a CSV file with its header, separator, quote and null value, in its encoding, quoted as BigQuery reads them (a quote within a quoted value is doubled, BigQuery has no escape character), without the columns of the scripted attributes. A failure to write the replay file deletes it, with a `WARN`; a failure to write the audit deletes it too.
  - The rows found are exact when there are as many as the bad records that the load job counted, less the structural errors. Otherwise, a rule differs from BigQuery for some value: a `WARN` says so, the bad records that BigQuery reports for this load job are written to the audit too, and no replay file is written for the load, so that no loaded row is loaded twice.
  - The accepted rows and the counts of the audit log are unchanged. Nothing more runs when no record is rejected; otherwise, one more load job per file (BigQuery does not bill load jobs) and one billed query on the temporary tables, which are dropped. The temporary tables get the column-level security of the attributes, as the tables of the first step of a load: reading them needs the Fine-Grained Reader role on their policy tags. When finding the rows fails, a `WARN` says so and the messages of BigQuery are kept, as before. A load job that fails on its bad records (`rejectAllOnError`, `rejectMaxRecords`) keeps the messages of BigQuery, as before.
  - Limits: the count cannot tell when a rule rejects a loaded row and accepts a rejected one of the same load job. The field names of a JSON line are matched in their case, while BigQuery reads them in any case. Several local files loaded in one step are uploaded as one stream, whose header lines after the first one are bad records that the per-file temporary loads do not see: their rows are not exact. The rejected rows of `POSITION` files, whose invalid values become null, are not covered.
- **`extract-bq-schema` keeps the parameters of `NUMERIC(P, S)`, `BIGNUMERIC(P, S)`, `STRING(L)` and `BYTES(L)` columns** (#1853): Starlake types carry no parameters, so these columns were extracted with the built-in `decimal`, `bignumeric`, `string` and `binary` types, with a `WARN`, and the native BigQuery loader created `NUMERIC(38, 9)`, `BIGNUMERIC`, `STRING` and `BYTES` columns. A dataset relocated through Starlake lost the bounds and lengths that BigQuery enforces.
  - `extract-bq-schema`, and `extract-schema` on a BigQuery connection, now give each parameter set a type of its own, named after it (`numeric_10_2`, `bignumeric_50_20`, `string_10`, `binary_16`), and append it to the `types.sl.yml` of the project, after its own types, whose order is kept. The file keeps its text and comments when the types can be appended to it; otherwise it is written again, without its comments, with a `WARN`. A generated type is the type of the project that the built-in type of the column names, usually the built-in type itself, with the BigQuery mapping of the parameters (`NUMERIC(10, 2)`) and, for a decimal, the zone of its precision and scale (`10,2`), which the Spark loads use. The other engines keep the mapping of that type. A type of the project with the same name is reused when it has the same primitive type, pattern, zone and BigQuery mapping; otherwise the name gets a suffix (`numeric_10_2_extracted`). The other readers of BigQuery schemas (column completion, schema diffs, `autoExportSchema`) write no type: their columns keep the built-in types.
  - An unparameterized `NUMERIC` column is extracted with the new built-in `numeric` type (zone `38,9`, mapping `NUMERIC`), which creates an unparameterized `NUMERIC` column, while `decimal` keeps creating a `NUMERIC(38, 9)`. A declared `NUMERIC(38, 9)` is extracted as a `decimal`. A type `numeric` of the project replaces the built-in one, as for the other built-in types; the extraction uses it only when it is defined as the built-in one, and `decimal` otherwise, with a `WARN`. A type of the parameters of a column never replaces a built-in type of the project.
  - **Behaviour change**: the unparameterized `NUMERIC` columns, extracted as `decimal` so far, are extracted as `numeric`. The native BigQuery loader takes the BigQuery mapping of a decimal type when it has the precision and scale of its zone: a `BIGNUMERIC(P, S)`, which the zone gave as a `NUMERIC(P, S)` when S <= 9 and P - S <= 29, a `NUMERIC(P)` without scale, and an unparameterized `NUMERIC` for the zone `38,9`. New columns of the custom decimal types with such a zone and mapping change accordingly; the columns that already exist keep their type, except in a load that replaces the table (`OVERWRITE` in one step), as for any type change.
  - `POSITION` loads cast their values to a parameterized type without its parameters, which BigQuery refuses in `CAST`, and give NULL to a value beyond them, as to a malformed one. The generated types are not used to find the type of a database column from its SQL type, nor as the default type of a primitive type in `yml2ddl`.
  - Limit: two processes extracting into the same project at the same time can lose the types of one of them.

# 1.7.6:
__Bug fix__:
- **BigQuery transforms whose presql creates their target run on a new environment** (#1803): on the native BigQuery path, Starlake decided between `CREATE TABLE ... AS SELECT` and `INSERT`/`MERGE` by checking whether the table existed *before* the script ran, while the presql runs *inside* that script. A presql creating the target (`CREATE TABLE IF NOT EXISTS my_domain.my_table (...)`, typically followed by a `DELETE` of the re-computed window) therefore collided with the `CREATE TABLE ... AS SELECT` on the first run (`Already Exists ... at [3:1]`), and the task only succeeded from its second run on. When a top-level presql statement is a `CREATE [OR REPLACE] TABLE [IF NOT EXISTS]` of the task's own target (dataset and table names as in the sink, case-sensitive, project optional), the main query is now always an `INSERT`/`MERGE` into it: the first run behaves like every later one, and the table has the columns, partitioning and options the presql declares. The `INSERT`/`MERGE` still writes the columns of the SELECT, so a column the presql does not declare fails at the first run instead of being created.
  - A presql that only `DELETE`s from a table that does not exist yet still fails on the first run: add a `CREATE TABLE IF NOT EXISTS` for the target at the top of the presql. A `CREATE` nested in a scripting block (`IF ... THEN`, `BEGIN ... END`) is not taken into account, since it may not run.
  - `SCD2` tasks: the presql `CREATE` must declare the start/end timestamp columns.
  - Unchanged: tasks whose presql does not create their target, views, materialized views, audit tables, and runs on a data branch.

# 1.7.5:
__Bug fix__:
- **Installing 1.7.x no longer provisions master's connector set**: `starlake install` fetched `setup.jar` from `master` no matter which version was being installed, and `setup.jar` is what pins Spark, Hadoop and every connector version written into `versions.sh` and downloaded into `bin/`. A 1.7 install (built for Spark 3.5) was therefore paired with master's Spark 4 pins. Most visibly it broke the 1.7 Docker image, whose build asked Maven Central for `delta-spark_3.5_2.13/4.3.1`, an artifact that does not exist, so the image never published for 1.7.3 or 1.7.4. `setup.jar` is now fetched from the tag of the release being installed, so it and the core jar always come from the same release. SNAPSHOTs and local builds, which have no release tag to fetch from, still use `master`.

# 1.7.4:
__Bug fix__:
- **BigQuery transforms no longer break when the task declares a presql** (#1792): on the native BigQuery path the presql statements and the main query were joined with `;` as a *separator*, so the last presql statement was glued to the `CREATE`/`INSERT` that followed it and BigQuery rejected the whole script with `Syntax error: Expected end of input but got keyword CREATE`. Adding a trailing `;` in the YAML could not work around it, since every presql is stripped of its terminator when the project is loaded. Each statement now carries its own terminator, and a task with no presql is unaffected.

# 1.7.3:
__Bug fix__:
- **DuckDB extraction finds its tables again** (#1737): the 1.8.3 fix backported to the 1.7 line. duckdb_jdbc 1.5.3+ reports base tables with the JDBC standard type `TABLE` instead of `BASE TABLE`, which made `starlake extract-data` against DuckDB abort with `SL_LAST_EXPORT table not found` after finding 0 tables. Both spellings are now passed, which works on old and new drivers alike.

__Improvement__:
- **Airflow orchestration templates move to starlake-airflow 0.6.17** (from 0.6.14): the DAG REST client now authenticates on Google-managed Airflow (Cloud Composer), its knobs (endpoint, auth, timeouts) are exposed as DAG options declared in the dag templates, and Cloud Run jobs honour `cloud_run_async_poke_interval` on the operator path, not just the sensor path. The bundled dagster (0.5.9) and orchestration (0.5.6.1) wheels are resynced with the versions the build pins.
- **Bootstrapped projects brief every assistant**: `starlake bootstrap` now ships AGENTS.md, GEMINI.md and copilot-instructions.md alongside CLAUDE.md, so Copilot, Gemini and any AGENTS.md-aware assistant get the same project context Claude does, and the sample project's README tells the StarBake story.

# 1.7.2:
__Bug fix__:
- **OVERWRITE transforms insert by name, never by position** (#1722): overwriting an existing table used a positional `INSERT` on most engines, so a SELECT whose column order diverged from the table's could silently write every value into the wrong column, or fail with a confusing count/cast error. All engines now pass the explicit column list resolved from the SELECT: values land in the columns of the same name regardless of SELECT order, and a column the table does not have fails loudly with a clear message.

__Improvement__:
- **Actionable schema-mismatch errors**: when a transform fails because the SELECT returns columns the target table lacks, the error now names the missing columns and prints the exact commands that fix it (`starlake transform --name <task> --sync-apply`, then re-run).
- **Automatic SQL-to-YAML sync (opt-in)**: with `SL_SYNC_SQL_WITH_YAML=true`, a transform aligns the task's YAML attributes with its SQL before running, so adding a column anywhere in the SELECT evolves the target table with no manual step. Off by default because it rewrites `.sl.yml` files in your project.

# 1.7.1:
__Improvement__:
- **All artifacts on GitHub Releases**: SNAPSHOT builds of starlake-core (assembly jar) and starlake-api (zip) are now published to a rolling pre-release tagged `v<version>-SNAPSHOT` on starlake-ai/starlake, with sha256 companions, replacing Sonatype snapshot publishing entirely. Setup and CI download scripts use one URL scheme for snapshots and releases. The release flow deletes the stale snapshot pre-release when the version ships.

# 1.7.0:
__Improvement__:
- **Quack server CLI**: New `starlake quack` command — host a Quack DuckDB query server (exposing a DuckLake without sharing object-storage credentials with clients) directly from the Starlake JVM. Actions: `serve` (foreground), `start` (detached daemon, state under `$SL_ROOT/.quack/`), `stop`, `list`, `stop-all`. Companion client-side recognition: connections whose `preActions` contain a `'quack:` ATTACH are pooled distinctly so multiple Quack clients don't collide. Token + bind + port can be set via connection options (`quackServerToken`, `quackBind`, `quackPort`) or per invocation (`--token`, `--bind`, `--port`). Default bind is `127.0.0.1` — use a reverse proxy with TLS to expose on the network.
- **Upgrade DuckDB**: Update DuckDB JDBC to 1.5.3 so the `quack` extension is available as a core extension (it lives in `core_nightly` on 1.5.2).
- **Arrow Flight SQL client**: Any Flight SQL endpoint (quack-on-demand gateway, GizmoSQL, Dremio, ...) is now a regular Starlake JDBC connection: `url: "jdbc:arrow-flight-sql://host:port?useEncryption=true&tenant=acme&pool=bi"`. The query string is passed to the Arrow driver untouched (TLS flags consumed by the driver, gateway routing params forwarded to the server). The driver class defaults to the official Arrow Flight SQL JDBC driver, downloaded by setup when `ENABLE_FLIGHTSQL` is on (version via `FLIGHT_SQL_JDBC_VERSION`). Flight SQL is a transport: the SQL dialect of the backing engine is chosen with the `dialect` connection option (default `duckdb`). The client stays fully remote: client-side ducklake/quack ATTACH rewriting and local DuckDB session setup (S3 secrets, home_directory) are skipped for Flight connections.

__Breaking change__:
- **`gizmosql` command removed**: The GizmoSQL process-manager integration (`starlake gizmosql start|stop|list|stop-all`, the `gizmosql.url` / `gizmosql.apiKey` settings and `SL_GIZMO_URL` / `SL_GIZMO_API_KEY` env vars) is gone. Expose DuckDB/DuckLake through any Arrow Flight SQL server and connect with a `jdbc:arrow-flight-sql://` connection instead.

__Bug fix__:
- **Domain lookup with shared physical schema**: Fix nine sites that picked the first logical domain matching a `finalName` and scoped table search inside it, silently dropping tables owned by sibling logical domains renamed to the same physical schema. Affected paths: `AutoTask.attDdl` (missing DDL mappings in temp tables), `SchemaHandler.findTableNames`, `AutoTaskDependencies.enrichItemWithColumns`, three sites in `TaskViewDependency` (schedule, parent-table resolution, writeStrategy/freshness), `ExtractBigQuerySchema` (incomplete exclusion list), and `FreshnessJob` (skipped freshness probes). In `AclDependencies`, `.toMap` on a key keyed by `finalName` collapsed duplicates — replaced with `groupBy` merge so RLS/ACL data from all matching domains is preserved.

# 1.5.15-SNAPSHOT:
__Improvement__:
- **REST API extraction**: New generic REST API extractor for SaaS/API data ingestion. Two new CLI commands: `extract-rest-schema` (infer table schemas from API responses) and `extract-rest-data` (fetch data to CSV or JSON Lines files). Features:
  - **Authentication**: Bearer token, API key, HTTP Basic, OAuth2 client credentials with automatic token refresh.
  - **Pagination**: Offset, cursor, Link header (RFC 5988), and page number strategies.
  - **Rate limiting**: Configurable requests-per-second throttling.
  - **Retry**: Configurable max retries with exponential backoff on 429/5xx/connection failures.
  - **Timeouts**: Configurable connect and read timeouts.
  - **Proxy**: HTTP proxy support with optional authentication.
  - **TLS/mTLS**: Custom CA certificates, client certificates, and insecure mode for development.
  - **Incremental extraction**: Track last extracted value via `incrementalField`, state persisted between runs.
  - **Resume on failure**: `--resume` flag skips already-extracted pages and continues from where extraction failed.
  - **Parent-child endpoints**: `{parent.fieldName}` path placeholders for dependent sub-resources.
  - **JSON Lines output**: `--outputFormat jsonl` preserves nested JSON structure without flattening.
  - **Response validation**: `errorPath` detects error indicators in HTTP 200 responses.
  - **XML support**: Automatic XML-to-JSON conversion for APIs returning `application/xml`.
  - **Schema evolution detection**: Warns when API response structure changes between extractions.
  - **Conditional requests**: ETag/If-Modified-Since support to skip unchanged data.
  - New `REST` connection type (aliases: `HTTP`, `API`).
  - JSON schema validation for `restAPI` extract configurations.
  - Sample extraction configs for 7 SaaS APIs: Slack, Jira, Twitter/X, Salesforce, HubSpot, Stripe, and GitHub (`samples/api/`).
- **Preload sentinel path**: The `preload` command now supports `--notReadySentinel <path>` to write a zero-byte marker file when no files match the expected pattern. Orchestrators can check for the sentinel to distinguish "not ready, retry later" (sentinel present, exit 0) from actual failures (no sentinel, non-zero exit). Supports local filesystem, GCS, S3, and HDFS paths.
- **BigQuery data branching**: New `SL_DATA_BRANCH` support for BigQuery native API. When set (via env var or connection options), writes are redirected to a branch dataset using BigQuery zero-copy `CLONE`, mirroring starlakeJDBC's branching mechanism. Works for both Spark and native execution paths on transforms and loads.
- **BigQuery JDBC branching**: Extend starlakeJDBC driver wrapping to BigQuery JDBC connections (alongside existing Snowflake support).
- **Upgrade DuckDB**: Update DuckDB to 1.5.2.0.
- **Upgrade starlakeJDBC**: Update starlakeJDBC to 0.7.
- **CI PySpark support**: Install Python 3.12 and PySpark 3.5.8 in CI workflows for PySpark-based tests.
- **Upgrade self-update**: The `starlake upgrade` command now downloads the latest starlake script from GitHub before proceeding, ensuring the upgrade logic itself is always up to date.
- **CI**: Trigger `snapshot-docker-no-tests` workflow automatically after `snapshot-jar-only` succeeds.
- **Bootstrap DuckDB extensions**: Install configured DuckDB extensions in `datasets/duckdb.db` during project bootstrap.
- **Site ReactFlow toggle attributes**: Add show/hide attributes button on lineage and relation diagrams. When hidden, edges fall back to table-level connections.
- **Site transform relations attributes**: Populate columns in transform relation diagrams from load table schemas and task attribute definitions.

__Bug fix__:
- **Spark BigQuery location**: Pass all connection options (including `location`) to the Spark BigQuery reader, ensuring queries run in the correct region instead of defaulting to US.
- **Spark BigQuery materializationDataset**: Fix `application.sl.yml` to respect `SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET` env var via Jinja, preventing region mismatch when the hardcoded dataset is in a different location.
- **Spark BigQuery validation**: Fail early with a clear error if `materializationDataset` is not configured before running a BigQuery SQL query via Spark.
- **DuckDB JDBC sink**: Use parquet intermediate format for all DuckDB write paths.
- **DuckDB secret_directory**: Fix setting order to prevent Secret Manager error.
- **PySpark to BigQuery sink**: Fix `SparkAutoTask.buildDataFrameToSink()` to support PySpark tasks sinking to non-FS targets (BigQuery, JDBC). Previously PySpark tasks only worked with filesystem sinks.
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

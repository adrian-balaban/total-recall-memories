---
title: 'Apache Flink 2.3 docs — SQL Reference: DDL (CREATE/DROP/ALTER)'
tags: [org, flink, flink-2.3, docs, sql, sql-reference, ddl, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:29:33.983Z'
updated: '2026-07-07T19:29:33.983Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 docs — SQL Reference DDL: CREATE / DROP / ALTER statements. Captured 2026-07-07 from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/ddl/ (English). Prose/headings verbatim; code condensed to `[code: ... (N chars)]`. Run-a-statement boilerplate (Java executeSql()/Scala executeSql()/Python execute_sql()/SQL CLI) is identical across all DDL statements: returns 'OK' on success, throws on failure.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/ddl/create/
# CREATE Statements
CREATE statements register a table/view/function into current or specified Catalog. Supported: CREATE TABLE, [CREATE OR] REPLACE TABLE, CREATE [OR ALTER] MATERIALIZED TABLE, CREATE CATALOG, CREATE DATABASE, CREATE VIEW, CREATE FUNCTION, CREATE MODEL.

## CREATE TABLE
Grammar: `[code: CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name ... (1444 chars)]`. Creates a table; throws if a table with same name exists.

### Columns
- Physical/Regular columns: define names, types, order of fields in physical data; represent payload read/written to external system. Connectors/formats use these columns in defined order. Other columns can be declared between but don't influence final physical schema.
- Metadata columns (extension to SQL standard): indicated by METADATA keyword; access connector/format-specific fields per row. Identified by string key + documented data type. Optional. Example: Kafka exposes `timestamp` of type TIMESTAMP_LTZ(3) for read/write. FROM clause can be omitted if column name == metadata key. Runtime does explicit cast if column type differs from metadata field type (must be compatible). By default usable for both reading and writing; VIRTUAL keyword excludes from persisting (read-only). Thus source-to-query schema (SELECT) and query-to-sink (INSERT INTO) differ.
- Computed columns: virtual, syntax `column_name AS computed_column_expression`. Evaluates expression referencing other columns (physical + metadata). Not physically stored. Type derived from expression. Transformed by planner into regular projection after source. Expression can contain columns/constants/functions, cannot contain subquery. Commonly used for time attributes: `proc AS PROCTIME()` (processing time), or pre-process event-time field before WATERMARK. Like virtual metadata, excluded from persisting; cannot be INSERT INTO target.

### WATERMARK
`WATERMARK FOR rowtime_column_name AS watermark_strategy_expression`. rowtime_column_name must be existing column of type TIMESTAMP(3), top-level, may be computed. watermark_strategy_expression: arbitrary non-query expression, return type TIMESTAMP(3) (timestamp since Epoch). Returned watermark emitted only if non-null and larger than previously emitted local watermark. Framework evaluates per record, periodically emits largest generated watermark (interval = pipeline.auto-watermark-interval). If interval 0ms, emitted per-record.
Common strategies:
- Strictly ascending: `WATERMARK FOR rowtime AS rowtime` (max observed timestamp; rows bigger than max are not late).
- Ascending: `WATERMARK FOR rowtime AS rowtime - INTERVAL '0.001' SECOND` (max minus 1; rows >= max not late).
- Bounded out of orderness: `WATERMARK FOR rowtime AS rowtime - INTERVAL 'string' timeUnit` (e.g. 5 SECOND delay).

### PRIMARY KEY
Hint for optimizations: column(s) unique and NOT NULL. Declared as column constraint or table constraint, singleton only. Only NOT ENFORCED mode supported (Flink doesn't own data; user enforces). Creating PK alters column nullability to NOT NULL.

### PARTITIONED BY
Partition created table by specified columns. Directory created per partition if used as filesystem sink.

### DISTRIBUTED
Buckets enable load balancing by splitting data into disjoint subsets. Bucketing depends on connector semantics; user can influence via number of buckets, algorithm, and (if algorithm allows) bucket key columns. All components optional from SQL perspective. Example 1: hash on fixed 4 buckets (HASH(uid)%4). Example 2: algorithm up to connector. Example 3: number of buckets up to connector. Example 4: only number of buckets defined.

### WITH Options
Table properties to create source/sink; key/value both string literals. Table name formats: catalog_name.db_name.table_name / db_name.table_name / table_name. Table registered with CREATE TABLE can be used as both source and sink (decided when referenced in DML).

### LIKE
Variant of SQL features T171 + T173. Creates table based on existing table definition; extend or exclude parts. Must be top-level of CREATE statement. Controls merging of: CONSTRAINTS, GENERATED, METADATA, OPTIONS, DISTRIBUTION, PARTITIONS, WATERMARKS. Strategies: INCLUDING (include, fail on duplicate), EXCLUDING (don't include), OVERWRITING (include, overwrite duplicates). INCLUDING/EXCLUDING ALL sets default for unspecified. Default (no options): INCLUDING ALL OVERWRITING OPTIONS. Physical columns always merged as INCLUDING. source_table can be compound identifier (different catalog/db).

### AS select_statement (CTAS)
Create-and-populate in one CTAS statement. SELECT part any Flink query; CREATE part takes resulting schema, requires WITH options. Creating-table operation depends on target Catalog (Hive Catalog creates physical table; in-memory catalog registers metadata in client memory). CREATE part can specify explicit columns — resulting schema = CREATE-part columns first, then SELECT-part columns; columns in both keep SELECT position; SELECT data types can be overridden. CREATE part can specify primary keys (only on NOT NULL columns from SELECT part; CREATE part doesn't allow NOT NULL definitions) and distribution. CTAS allows reordering SELECT columns by specifying all column names without data types (must match names/count; cannot combine with new columns requiring data types).
Restrictions: no temporary table, no partitioned table. Default non-atomic (table not dropped on insert error).
Atomicity: requires sink implements atomicity (SupportsStaging) AND `table.rtas-ctas.atomicity-enabled=true`.

## [CREATE OR] REPLACE TABLE (RTAS)
`[code: [CREATE OR] REPLACE TABLE [catalog_name.][db_name.]table_name ... (338 chars)]`. Semantics: REPLACE TABLE AS SELECT requires target exists (else exception); CREATE OR REPLACE TABLE AS SELECT creates if not exists, replaces if exists. RTAS = drop + create + insert. Allows explicit columns, watermarks, primary keys, distribution. Restrictions: no temporary, no partitioned. Default non-atomic. In-memory catalog: dropping only removes from catalog without removing physical data, so pre-RTAS data persists.
Atomicity: same as CTAS (SupportsStaging + table.rtas-ctas.atomicity-enabled=true).

## CREATE [OR ALTER] MATERIALIZED TABLE
See dedicated Materialized tables page.

## CREATE CATALOG
`[code: CREATE CATALOG [IF NOT EXISTS] catalog_name ... (106 chars)]`. IF NOT EXISTS: no-op if exists. WITH OPTIONS: catalog properties (key/value string literals). See Catalogs.

## CREATE DATABASE
`[code: CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name ... (120 chars)]`. IF NOT EXISTS no-op. WITH OPTIONS: database properties (string literals).

## CREATE VIEW
`[code: CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_ ... (155 chars)]`. TEMPORARY: has catalog/db namespaces, overrides views. IF NOT EXISTS no-op.

## CREATE FUNCTION
`[code: CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION ... (268 chars)]`. Creates catalog function with identifier + optional language tag. JAVA/SCALA: identifier = full classpath of UDF. PYTHON: identifier = fully qualified name (e.g. pyflink.table.tests.test_udf.add); requires configuring Python deps if program is Java/Scala/SQL. TEMPORARY: catalog/db namespaces, overrides catalog functions. TEMPORARY SYSTEM: no namespace, overrides built-ins. IF NOT EXISTS no-op. LANGUAGE JAVA|SCALA|PYTHON (default JAVA). USING: list of jar resources containing implementation + deps (local/remote fs hdfs/s3/oss); only JAVA/SCALA support USING.

## CREATE MODEL
`[code: CREATE [TEMPORARY] MODEL [IF NOT EXISTS] [catalog_name.][db_name.]mode ... (393 chars)]`. Creates model with optional input/output column definitions. TEMPORARY: catalog/db namespaces, overrides models. IF NOT EXISTS no-op. Input columns = features for inference; output columns = predictions; each needs name + data type. WITH OPTIONS: model properties (string literals) to find/create model provider. Properties/supported types vary by provider. Examples: `CREATE MODEL sentiment_analysis_model ...` and Triton text classifier.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/ddl/drop/
# DROP Statements
Remove a catalog or registered table/view/function. Supported: DROP CATALOG, DROP TABLE, DROP MATERIALIZED TABLE, DROP DATABASE, DROP VIEW, DROP FUNCTION, DROP MODEL.

## DROP CATALOG
`DROP CATALOG [IF EXISTS] catalog_name`. IF EXISTS no-op.

## DROP TABLE
`DROP [TEMPORARY] TABLE [IF EXISTS] [catalog_name.][db_name.]table_name`. Throws if not exists. TEMPORARY: catalog/db namespaces. IF EXISTS no-op.

## DROP MATERIALIZED TABLE
See Materialized tables page.

## DROP DATABASE
`DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]`. RESTRICT (default): throws on non-empty. CASCADE: drops all associated tables and functions.

## DROP VIEW
`DROP [TEMPORARY] VIEW [IF EXISTS] [catalog_name.][db_name.]view_name`. TEMPORARY: catalog/db namespaces. Flink does not maintain view dependencies via CASCADE/RESTRICT; produces postpone error when view used after underlying table dropped.

## DROP FUNCTION
`DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name`. TEMPORARY: catalog/db namespaces. TEMPORARY SYSTEM: no namespace. IF EXISTS no-op.

## DROP MODEL
`DROP [TEMPORARY] MODEL [IF EXISTS] [catalog_name.][db_name.]model_name`. TEMPORARY: catalog/db namespaces. IF EXISTS no-op.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/ddl/alter/
# ALTER Statements
Modify definition of registered table/view/function or catalog. Supported: ALTER TABLE, ALTER MATERIALIZED TABLE, ALTER VIEW, ALTER DATABASE, ALTER FUNCTION, ALTER CATALOG, ALTER MODEL.

## ALTER TABLE
`[code: ALTER TABLE [IF EXISTS] table_name { ... (1537 chars)]`. IF EXISTS no-op.
- ADD: add columns, constraints, watermark, partitions, distribution. FIRST or AFTER col_name positions; default append last. Adding a PK column changes nullability to false implicitly.
- MODIFY: change column position/type/comment/nullability, change PK columns, watermark strategy. FIRST or AFTER col_name; default position unchanged. Modify-to-PK changes nullability to false implicitly.
- DROP: drop columns, primary key, partitions, watermark strategy.
- RENAME: rename column.
- SET: set/override one or more table properties.
- RESET: reset one or more properties to default.

## ALTER MATERIALIZED TABLE
See Materialized tables page.

## ALTER VIEW
`ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name` (rename within same catalog/db) or `ALTER VIEW [...]view_name AS new_query_expression` (change underlying query).

## ALTER DATABASE
`ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)`. Set/override properties.

## ALTER FUNCTION
`ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON]`. Alter catalog function with new identifier + language tag. JAVA/SCALA: full classpath. PYTHON: fully qualified name. TEMPORARY: catalog/db namespaces. TEMPORARY SYSTEM: no namespace. IF EXISTS no-op. Default language JAVA.

## ALTER CATALOG
`ALTER CATALOG catalog_name SET (...)` (set/override properties), `RESET (...)` (reset to default), `COMMENT 'comment'` (set/override comment).

## ALTER MODEL
`ALTER MODEL [IF EXISTS] [catalog_name.][db_name.]model_name SET (...)` / `RESET (...)` (properties), `RENAME TO new_name` (rename within same catalog/db). IF EXISTS no-op.
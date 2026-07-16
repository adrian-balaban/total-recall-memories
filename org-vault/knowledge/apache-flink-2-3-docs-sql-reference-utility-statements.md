---
title: 'Apache Flink 2.3 docs — SQL Reference: Utility Statements'
tags: [org, flink, flink-2.3, docs, sql, sql-reference, utility, ddl, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:40:48.174Z'
updated: '2026-07-07T19:40:48.174Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL Reference — Utility Statements. Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`. Covers: analyze, describe, explain, use, show, load, unload, set, reset, jar, job, call.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/analyze/
# ANALYZE Statements
Collect statistics for existing tables, store result to catalog. Only ANALYZE TABLE supported, triggered manually.
Attention: ANALYZE TABLE only batch mode. Existing table only (view or nonexistent → exception).
Executed via executeSql()/execute_sql() (Java/Scala/Python), or SQL CLI.
## Syntax
`[code: ANALYZE TABLE [catalog_name.][db_name.]table_name PARTITION(partcol1[= ... (171 chars)]`
- PARTITION(partcol1[=val1] [, partcol2[=val2], …]): required for partition table. No partition → all partitions. Specific partition → only that partition. Non-partition table with partition specified → exception. Nonexistent partition → exception.
- FOR COLUMNS col1[,...] or FOR ALL COLUMNS: optional. No column → only table-level stats. Nonexistent/non-physical column → exception. Column specified → column-level stats.
Column-level stats: ndv (distinct values), nullCount, avgLen, maxLen, minValue, maxValue, valueCount (boolean only).
Supported types and stats matrix ("Y" support, "N" unsupported):
[table: 16 rows]
NOTE: fixed-length types (BOOLEAN, INTEGER, DOUBLE, etc.) don't collect avgLen/maxLen from records.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/describe/
# DESCRIBE Statements
Describe schema of table/view, metadata of catalog/function, or specified job.
Executed via executeSql()/execute_sql(), or SQL CLI.
## Syntax
### DESCRIBE TABLE
`[code: { DESCRIBE | DESC } [catalog_name.][db_name.]table_name ... (55 chars)]`
### DESCRIBE CATALOG
`[code: { DESCRIBE | DESC } CATALOG [EXTENDED] catalog_name ... (51 chars)]`
### DESCRIBE FUNCTION
`[code: { DESCRIBE | DESC } FUNCTION [EXTENDED] [catalog_name.][db_name.]funct ... (78 chars)]`
### DESCRIBE JOB
`[code: { DESCRIBE | DESC } JOB '<job_id>' ... (34 chars)]`
Attention: DESCRIBE JOB only in SQL CLI or SQL Gateway.
### DESCRIBE MODEL
`[code: { DESCRIBE | DESC } MODEL [EXTENDED] [catalog_name.][db_name.]model_na ... (72 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/explain/
# EXPLAIN Statements
Explain logical and optimized query plans of a query or INSERT statement.
Executed via executeSql()/execute_sql(), or SQL CLI.
Outputs: EXPLAIN PLAN (AST + optimized plans), EXPLAIN PLAN WITH DETAILS.
## ExplainDetails
ESTIMATED_COST: attach estimated optimal cost of each physical rel node.
`[code: == Optimized Physical Plan == ... (127 chars)]`
CHANGELOG_MODE: attach changelog mode (see Dynamic Tables) of each physical rel node.
`[code: == Optimized Physical Plan == ... (73 chars)]`
PLAN_ADVICE (since Flink 1.17): analyze optimized physical plan for risk warnings / optimization advice. Changes title to "Optimized Physical Plan with Advice". Categorized by Kind and Scope.
[table: 3 rows]
[table: 3 rows]
Targets:
- Data Skewness from Group Aggregation (see Group Aggregation, Performance Tuning).
- Non-deterministic Updates (NDU, see Determinism In Continuous Queries).
If GroupAggregate optimizable to local-global, optimizer tags advice id, suggests config updates.
`[code: SET 'table.exec.mini-batch.enabled' = 'true'; ... (489 chars)]`
NODE_LEVEL ADVICE:
`[code: == Optimized Physical Plan With Advice == ... (657 chars)]`
If NDU detected, warning appended at end of physical plan.
`[code: CREATE TABLE MyTable ( ... (461 chars)]`
QUERY_LEVEL WARNING:
`[code: == Optimized Physical Plan With Advice == ... (1148 chars)]`
No warning/advice → "No available advice" notice.
`[code: CREATE TABLE MyTable ( ... (186 chars)]`
JSON_EXECUTION_PLAN: attach json-format execution plan.
## Syntax
`[code: EXPLAIN [([ExplainDetail[, ExplainDetail]*]) | PLAN FOR] <query_statem ... (192 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/use/
# USE Statements
Set current database or catalog, or change resolution order and enabled status of module.
Executed via executeSql()/execute_sql(), or SQL CLI. Returns 'OK' on success.
## USE CATALOG
`[code: USE CATALOG catalog_name ... (24 chars)]`
Set current catalog. Subsequent commands without explicit catalog use this one. Nonexistent → exception. Default: default_catalog.
## USE MODULES
`[code: USE MODULES module_name1[, module_name2, ...] ... (45 chars)]`
Set enabled modules with declared order. Subsequent commands resolve metadata (functions/UDTs/rules) within enabled modules following resolution order. Module used by default when loaded. Loaded modules disabled if not in USE MODULES. Default loaded+enabled: core.
## USE
`[code: USE [catalog_name.]database_name ... (32 chars)]`
Set current database. Nonexistent → exception. Default: default_database.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/show/
# SHOW Statements
List objects within parent (catalogs, databases, tables/views, columns, functions, modules).
SHOW CREATE: print DDL for given object (currently only table and view, plus catalog/materialized table/model).
Supported SHOW statements:
- SHOW CATALOGS, SHOW CURRENT CATALOG, SHOW CREATE CATALOG
- SHOW DATABASES, SHOW CURRENT DATABASE
- SHOW TABLES, SHOW CREATE TABLE, SHOW COLUMNS, SHOW PARTITIONS, SHOW PROCEDURES
- SHOW VIEWS, SHOW CREATE VIEW
- SHOW MATERIALIZED TABLES, SHOW CREATE [OR ALTER] MATERIALIZED TABLE
- SHOW FUNCTIONS, SHOW MODULES, SHOW JARS, SHOW JOBS
- SHOW MODELS, SHOW CREATE MODEL
LIKE/ILIKE filter syntax (MySQL-dialect pattern): % = any chars (incl. zero), \% = literal %, _ = exactly one char, \_ = literal _. ILIKE = case-insensitive.
## SHOW CATALOGS
`[code: SHOW CATALOGS [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ] ... (57 chars)]`
## SHOW CURRENT CATALOG
`[code: SHOW CURRENT CATALOG ... (20 chars)]`
## SHOW CREATE CATALOG
`[code: SHOW CREATE CATALOG catalog_name ... (32 chars)]`
Includes name and properties. Example: `show create catalog cat2;` (518 chars output).
## SHOW DATABASES
`[code: SHOW DATABASES [ ( FROM | IN ) catalog_name] [ [NOT] (LIKE | ILIKE) <s ... (88 chars)]`
## SHOW CURRENT DATABASE
`[code: SHOW CURRENT DATABASE ... (21 chars)]`
## SHOW TABLES
`[code: SHOW TABLES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIK ... (92 chars)]`
Examples: `show tables from db1;`, `show tables from db1 like '%n';`, `show tables from db1 not like '%n';`, `show tables;`.
## SHOW CREATE TABLE
`[code: SHOW CREATE TABLE [[catalog_name.]db_name.]table_name ... (53 chars)]`
Includes table name, columns, types, constraints, comments, config. Attention: only tables created by Flink SQL DDL.
## SHOW COLUMNS
`[code: SHOW COLUMNS ( FROM | IN ) [[catalog_name.]database.]<table_name> [ [N ... (98 chars)]`
## SHOW PARTITIONS
`[code: SHOW PARTITIONS [[catalog_name.]database.]<table_name> [ PARTITION <pa ... (133 chars)]`
PARTITION shows partitions under <partition_spec>. Examples: `show partitions table1;`, `show partitions table1 partition (id=1002);`.
## SHOW PROCEDURES
`[code: SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] ... (106 chars)]`
## SHOW VIEWS
`[code: SHOW VIEWS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE ... (91 chars)]`
## SHOW CREATE VIEW
`[code: SHOW CREATE VIEW [catalog_name.][db_name.]view_name ... (51 chars)]`
## SHOW MATERIALIZED TABLES
`[code: SHOW MATERIALIZED TABLES [ ( FROM | IN ) [catalog_name.]database_name  ... (105 chars)]`
## SHOW CREATE [OR ALTER] MATERIALIZED TABLE
`[code: SHOW CREATE MATERIALIZED TABLE [catalog_name.][db_name.]materialized_t ... (79 chars)]`
`[code: SHOW CREATE OR ALTER MATERIALIZED TABLE [catalog_name.][db_name.]mater ... (88 chars)]`
## SHOW FUNCTIONS
`[code: SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ ... (112 chars)]`
USER shows only UDFs.
## SHOW MODULES
`[code: SHOW [FULL] MODULES ... (19 chars)]`
FULL shows loaded modules and enabled status with resolution order.
## SHOW JARS
`[code: SHOW JARS ... (9 chars)]`
All added jars (via ADD JAR) in session classloader. Attention: only SQL CLI or SQL Gateway.
## SHOW JOBS
`[code: SHOW JOBS ... (9 chars)]`
Jobs in Flink cluster. Attention: only SQL CLI or SQL Gateway.
## SHOW MODELS
`[code: SHOW MODELS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LI ... (102 chars)]`
## SHOW CREATE MODEL
`[code: SHOW CREATE MODEL [catalog_name.][db_name.]model_name ... (53 chars)]`
Includes model name, input/output schema, options, config. Example: `show create model my_model;` (579 chars).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/load/
# LOAD Statements
Load a built-in or user-defined module. Returns 'OK'.
## LOAD MODULE
`[code: LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)] ... (70 chars)]`
module_name simple identifier, case-sensitive, identical to module type in module factory (used for discovery). Properties (except 'type') passed to discovery service to instantiate module.
Example: `Flink SQL> LOAD MODULE hive WITH ('hive-version' = '3.1.3');` (212 chars).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/unload/
# UNLOAD Statements
Unload a built-in or user-defined module. Returns 'OK'.
## UNLOAD MODULE
`[code: UNLOAD MODULE module_name ... (25 chars)]`
Example: `Flink SQL> UNLOAD MODULE core;` (98 chars).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/set/
# SET Statements
Modify or list configuration. SQL CLI only.
## Syntax
`[code: SET ('key' = 'value')? ... (22 chars)]`
No key/value → prints all properties. Otherwise sets key to value.
Example: `Flink SQL> SET 'table.local-time-zone' = 'Europe/Berlin';` (154 chars).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/reset/
# RESET Statements
Reset configuration to default. SQL CLI only.
## Syntax
`[code: RESET ('key')? ... (14 chars)]`
No key → reset all to default. Otherwise reset specified key.
Example: `Flink SQL> RESET 'table.planner';` (161 chars).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/jar/
# JAR Statements
Add/remove user jars from classpath, or show added jars.
Supported: ADD JAR, SHOW JARS, REMOVE JAR. SQL CLI.
## ADD JAR
`[code: ADD JAR '<path_to_filename>.jar' ... (32 chars)]`
Add JAR (local or remote filesystem) to resources. Listed via SHOW JARS.
Limitation: Don't use ADD JAR to load Hive source/sink/function/catalog (known Hive connector limitation; future fix). Use Hive integration setup instruction.
## SHOW JARS
`[code: SHOW JARS ... (9 chars)]`
## REMOVE JAR
`[code: REMOVE JAR '<path_to_filename>.jar' ... (35 chars)]`
Attention: REMOVE JAR only in SQL CLI.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/job/
# JOB Statements
Flink job management. Supported: SHOW JOBS, DESCRIBE JOB, STOP JOB. SQL CLI.
## SHOW JOBS
`[code: SHOW JOBS ... (9 chars)]`
Attention: only SQL CLI or SQL Gateway.
## DESCRIBE JOB
`[code: { DESCRIBE | DESC } JOB '<job_id>' ... (34 chars)]`
Show specified job. Attention: only SQL CLI or SQL Gateway.
## STOP JOB
`[code: STOP JOB '<job_id>' [WITH SAVEPOINT] [WITH DRAIN] ... (49 chars)]`
WITH SAVEPOINT: perform savepoint before stopping. Path via execution.checkpointing.savepoint-dir (cluster config) or SET (SET takes precedence).
WITH DRAIN: increase watermark to maximum before last checkpoint barrier. Use when terminating job permanently.
Attention: only SQL CLI or SQL Gateway.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/utility/call/
# Call Statements
Call a stored procedure (data manipulation or administrative tasks).
Attention: requires procedure to exist in catalog; else exception. Refer to catalog doc for available procedures. To implement: see Procedure.
Executed via executeSql()/execute_sql() (immediately calls procedure, returns TableResult), or SQL CLI.
## Syntax
`[code: CALL [catalog_name.][database_name.]procedure_name ([ expression [, ex ... (84 chars)]`
Example: `// assuming the procedure `generate_n` has existed in `system` database ... (244 chars)`.
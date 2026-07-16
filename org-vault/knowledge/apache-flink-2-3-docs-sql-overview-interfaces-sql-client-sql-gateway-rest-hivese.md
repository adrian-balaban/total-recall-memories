---
title: 'Apache Flink 2.3 docs — SQL Overview & Interfaces (SQL Client, SQL Gateway REST/HiveServer2, JDBC Driver)'
tags: [org, flink, flink-2.3, docs, sql, sql-client, sql-gateway, jdbc, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:20:36.880Z'
updated: '2026-07-07T19:20:36.880Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 docs — SQL Overview & Interfaces. Captured 2026-07-07 from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/overview/ and /docs/sql/interfaces/*. Prose/headings kept verbatim; code blocks condensed to `[code: <first line> ... (N chars)]`; tables condensed to `[table: N rows]`.

---

# Flink SQL (sql/overview)
Flink SQL enables developing streaming and batch applications using standard SQL, based on Apache Calcite (implements the SQL standard). Queries have the same semantics and produce the same results regardless of whether input is continuous (streaming) or bounded (batch). Flink SQL integrates seamlessly with the Table API and DataStream API; you can detect patterns with MATCH_RECOGNIZE then build alerting with DataStream API.

## Ways to Use Flink SQL
Flink SQL can be used through several interfaces depending on use case: [table: 5 rows — SQL Client, SQL Gateway (REST/HiveServer2), JDBC Driver, Table API, OLAP].

## Key Concepts
Built on dynamic tables (bounded batch and unbounded streaming). SQL queries on dynamic tables produce continuously updating results. See Dynamic Tables, Time Attributes, Streaming Concepts.

## Where to Go Next
SQL Client (interactive CLI), SQL Reference (DDL/DML/queries syntax), Built-in Functions, SQL Gateway (REST + HiveServer2 for remote SQL access), JDBC Driver (connect from JDBC-compatible tools), Catalogs (metadata management), Hive Compatibility (metastore + syntax), Materialized Tables (incrementally maintained query results), Connectors/formats, Data Types, Time Zone, Table API, OLAP Quickstart.

---

# SQL Client (sql/interfaces/sql-client)
The Table & SQL API requires queries embedded in Java/Scala table programs packaged with build tools — limiting Flink to Java/Scala programmers. The SQL Client provides an easy way to write, debug, and submit table programs without a single line of Java/Scala code, and visualize real-time results from the running distributed application on the command line.

## Getting Started
Bundled in the regular Flink distribution; requires a running Flink cluster. Start a local cluster: `./bin/start-cluster.sh`.

### Starting the SQL Client CLI
Two options: embedded standalone process, or connecting to a remote SQLGateway. Default mode is embedded.
- Embedded: `./bin/sql-client.sh` (or `./bin/sql-client.sh embedded`).
- Gateway: `./bin/sql-client.sh gateway --endpoint <gateway address>` — submits SQL to a remote gateway to execute. `<gateway address>` as host:port or full URL. Override default job manager: `./bin/sql-client.sh gateway --endpoint <gateway address> -Drest.address=...`.
- Custom HTTP headers via `FLINK_REST_CLIENT_HEADERS` env var (e.g. `export FLINK_REST_CLIENT_HEADERS="Cookie:myauthcookie=foobar;..."`); multiple headers separated by newline.
- Default truststore from `security.ssl.rest.truststore`/`-password`; otherwise JDK default certificate stores.
- Note: SQL Client only supports connecting to the REST Endpoint since v2.

### Running SQL Queries
Validate: `SET 'sql-client.execution.result-mode' = 'tableau';` then a query; press Enter; close result view with Q. SET tunes job execution and sql client behaviour. After a query is defined, it can be submitted as a long-running detached Flink job.

### Key-strokes
[table: 28 rows — navigation/editing shortcuts in SQL Client]

### Getting help
Type HELP. See general SQL documentation.

## Configuration
### SQL Client startup options
`./sql-client [MODE] [OPTIONS]` [code: full options table (10718 chars)].

### SQL Client Configuration
Configure by setting options or any valid Flink config entry: `SET 'key' = 'value';`. [table: 7 rows — result-mode, max-table-result.rows, verbose, etc.]

### SQL Client result modes
Three modes for maintaining/visualizing results:
- table mode: materializes results in memory, paginated table. `SET 'sql-client.execution.result-mode' = 'table';`.
- changelog mode: does not materialize; visualizes insertions (+) and retractions (-). `SET '...result-mode' = 'changelog';`.
- tableau mode: traditional, displays directly with tableau format; influenced by execution.type. `SET '...result-mode' = 'tableau';`. With streaming query, results continuously print; for bounded input the job terminates after processing; CTRL-C to terminate.
All modes store results in the SQL Client's Java heap. Changelog shows latest 1000 changes; table mode limited by memory and `sql-client.execution.max-table-result.rows`. Batch environment queries only retrievable with table or tableau mode.

### Initialize Session Using SQL Files
`-i` startup option executes an initialization SQL file to setup environment. Defines catalogs, table sources/sinks, UDFs, properties. Example [code: defines Hive catalog, MyTable CSV source, MyCustomView, myUDF, streaming mode parallelism 1, table result mode, planner adjustments].
Allowed in init file: DDL (CREATE/DROP/ALTER), USE CATALOG/DATABASE, LOAD/UNLOAD MODULE, SET, RESET. For queries/inserts use interactive mode or `-f`. Errors during init cause SQL Client to exit.

### Dependencies
No Java project needed; pass dependencies as JAR files via `--jar` or library directories via `--library`. Ready-to-use JAR bundles for connectors/formats downloadable from Maven central. See connection-to-external-systems page.

## Usage
Submit jobs in interactive command line or with `-f` for SQL files. Parses and executes all Flink-supported SQL statements.

### Interactive Command Line
Reads inputs, executes statements terminated by `;`. Prints success/error messages; by default error shows only cause — set `sql-client.verbose` = true for full stack.

### Execute SQL Files in a Session Cluster
`-f` executes statements one by one, printing messages; on first failure SQL Client exits and remaining statements are not executed. Example [code: CSV source users, sets job name, savepoint path, submits job loading the savepoint]. Compared to interactive mode, stops and exits on errors.

### Deploy SQL Files to an Application Cluster
`-f` deploys a script to an Application Cluster if deployment target specified in config.yaml or startup options. Example: `./bin/sql-client.sh -f oss://path/to/script.sql \ ...` [code (205 chars)]. Prints cluster id. Application cluster only supports one job. When deploying a script, only `--jars` startup option supported (not `--init`).

### Execute a set of SQL statements
SQL Client executes each INSERT INTO as a single job. STATEMENT SET syntax executes a set of INSERT INTO statements, holistically optimized and executed as a single Flink job (reusing common intermediate results). Syntax: `EXECUTE STATEMENT SET BEGIN INSERT INTO ...; INSERT INTO ...; END;`. Old `BEGIN STATEMENT SET; ... END;` deprecated. Statements in a STATEMENT SET must be separated by `;`.

### Execute DML statements sync/async
By default DML executed asynchronously (submits job, doesn't wait; can submit multiple jobs). CLI shows job info after submission. SQL Client does not track running job status after submission; CLI can be shut down without affecting the detached query; use job statements to monitor/stop. For batch, set `table.dml-sync` = true to execute synchronously. CTRL-C to cancel.

### Start a SQL Job from a savepoint
`SET 'execution.state-recovery.path' = '/tmp/flink-savepoint...'` — Flink restores state from the savepoint for all following DML statements. RESET to disable. See Job Lifecycle Management.

### Define a Custom Job Name
`SET 'pipeline.name' = 'kafka-to-hive'` — affects all following queries/DML. RESET to revert. Default name e.g. `insert-into_<sink_table_name>`.

### Monitoring Job Status
`SHOW JOBS;` lists jobs status in the cluster.

### Terminating a Job
`STOP JOB '<job_id>' WITH SAVEPOINT;` — savepoint dir configurable via `execution.checkpointing.savepoint-dir`. See Job Statements.

### SQL Syntax highlighting
`sql-client.display.color-schema` sets a color scheme: chester, dracula, solarized, vs2010, obsidian, geshi, dark, light, default (no highlighting). Fallback to default on wrong name. [table: 10 rows]

---

# SQL Gateway — Overview (sql/interfaces/sql-gateway/overview)
## Introduction
The SQL Gateway enables multiple remote clients to execute SQL concurrently — easy way to submit Flink Jobs, look up metadata, analyze data online. Composed of pluggable endpoints and the SqlGatewayService (a processor reused by endpoints). Endpoints are entry points allowing users to connect.

## Getting Started
Bundled in the regular distribution; requires a running Flink cluster. Start a local cluster: `./bin/start-cluster.sh`.

### Starting the SQL Gateway
`./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost ...` — starts with REST Endpoint listening on localhost:8083. Verify: `curl http://localhost:8083/v1/info`.

### Running SQL Queries
Step 1 — Open a session: `curl --request POST http://localhost:8083/v1/sessions` — returns sessionHandle (uniquely identifies every active user).
Step 2 — Execute a query: `curl --request POST http://localhost:8083/v1/sessions/${sessionHandle}/...` — returns operationHandle (uniquely identifies the submitted SQL). Enrich the POST body with rest.address and rest.port inside executionConfig to set the Flink cluster address (remote execution).
Step 3 — Fetch results: `curl --request GET http://localhost:8083/v1/sessions/${sessionHandle}/...` — returns a batch with schema and a nextResultUri (fetch next batch if not null).

### Deploying a Script
SQL Gateway supports deploying a script in Application Mode (JobManager compiles the script). Use ADD JAR to download custom resources (e.g. Kafka Source). Example for native K8S cluster with cluster id CLUSTER_ID. For PyFlink, use an image with PyFlink installed.

## Configuration
### SQL Gateway startup options
`./bin/sql-gateway.sh --help` [code (397 chars)]. For "start"/"start-foreground", `./bin/sql-gateway.sh start --help` [code (290 chars)].
### SQL Gateway Configuration
`./sql-gateway -Dkey=value` [table: 10 rows].

## Supported Endpoints
Flink natively supports REST Endpoint and HiveServer2 Endpoint. REST bundled by default. Switch with `./bin/sql-gateway.sh start -Dsql-gateway.endpoint.type=hiveserver2` or `sql-gateway.endpoint.type: hiveserver2` in config. CLI command has higher priority than config file.

---

# REST Endpoint (sql/interfaces/sql-gateway/rest)
The REST endpoint allows connecting to SQL Gateway with REST API.

## Overview of SQL Processing
### Open Session
On connect, the SQL Gateway creates a Session storing user-specified info; returns a SessionHandle for later interactions.
### Submit SQL
Client submits SQL; translated to an Operation; an OperationHandle returned for fetching results. Operation has lifecycle — client can cancel or close to release resources.
### Fetch Results
With OperationHandle, client fetches results. If ready, returns a batch with schema and a URI for the next batch. When all fetched, resultType = EOS and next-batch URI is null.

## Endpoint Options
[table: 5 rows]

## REST API
Available OpenAPI specification; default version is v3. [table: 5 rows] — experimental.
### API reference
v4, v3, v2, v1 — each with many endpoints (open/close session, execute statement, fetch results, cancel operation, configure session, complete statement, etc.) [multiple tables of 5–9 rows each].
## Data Type Mapping
REST endpoint supports serializing RowData with query parameter rowFormat. Uses JSON format to serialize Table Objects (see JSON format mappings). Also supports PLAIN_TEXT format (casts all columns to String).

---

# HiveServer2 Endpoint (sql/interfaces/sql-gateway/hiveserver2)
The Flink SQL Gateway can deploy as a HiveServer2 Endpoint compatible with the HiveServer2 wire protocol — submit Hive-dialect SQL through Flink SQL Gateway with existing Hive clients (Thrift or Hive JDBC driver): Beeline, DBeaver, Apache Superset. Recommended to use with a Hive Catalog and Hive dialect.

## Setting Up
### Configure HiveServer2 Endpoint
Not the default endpoint. Configure: `./bin/sql-gateway.sh start -Dsql-gateway.endpoint.type=hiveserver2 ...` or in config: `sql-gateway.endpoint.type: hiveserver2` (+ hive conf path).
### Connecting to HiveServer2
After starting, submit SQL with Apache Hive Beeline: `./beeline ...` [code (1792 chars)].

## Endpoint Options
[table: 14 rows — options for creating a HiveServer2 Endpoint instance with YAML or DDL].

## HiveServer2 Protocol Compatibility
Aims to provide the same experience as HiveServer2. Automatically initializes the environment: create the Hive Catalog as default catalog; load Hive function module (first in function module list); switch to Hive dialect (table.sql-dialect = hive); switch to batch execution mode (execution.runtime-mode = BATCH); execute DML statements blocking and one by one (table.dml-sync = true). Submit Hive SQL in Hive style but execute in Flink environment.

## Clients & Tools
Compatible with HiveServer2 wire protocol. Tested: Hive JDBC, Hive Beeline, DBeaver, Apache Superset.
### Hive JDBC
Add dependencies in pom.xml; connect and list tables in the Hive Catalog [code sample].
### DBeaver
Uses Hive JDBC. Connect like HiveServer2. Currently no authentication — use `jdbc:hive2://{host}:{port}/{database};auth=noSasl`.
### Apache Superset
Connect like Hive. No auth — use `hive://hive@{host}:{port}/{database}?auth=NOSASL`.

## Streaming SQL
Flink is a batch-streaming unified engine. Switch: `SET table.sql-dialect=default;` — environment ready to parse Flink SQL, optimize with the streaming planner, submit async. Notice: RowKind in the HiveServer2 API is always INSERT, so HiveServer2 Endpoint doesn't support presenting CDC data.

## Supported Types
Built on Hive2; supports all Hive2 available types. For Hive-compatible tables, obeys the same rule as HiveCatalog to convert Flink types to Hive Types and serialize to thrift object. See HiveCatalog for type mappings.

---

# Flink JDBC Driver (sql/interfaces/jdbc-driver)
The Flink JDBC Driver is a Java library enabling clients to send Flink SQL to a Flink cluster via the SQL Gateway. You can also use the Hive JDBC Driver with Flink (run SQL Gateway with HiveServer2 endpoint for Hive dialect SQL and Hive Catalog).

## Usage
Before using, start a SQL Gateway with REST endpoint — acts as the JDBC server bound to your Flink cluster.

## Dependency
All dependencies packaged in `flink-sql-jdbc-driver-bundle`; download and add the jar. [table: 2 rows] Or add maven/gradle dependency: `<dependency> ...` [code (171 chars)].

## JDBC Clients
The Flink JDBC driver is not included with the Flink distribution; download from Maven. May also need SLF4J jar.
### Beeline
Hive's CLI, supports general JDBC drivers. Download flink-jdbc-driver-bundle-{VERSION}.jar to $HIVE_HOME/lib; run beeline and connect: `beeline> !connect jdbc:flink://localhost:8083` (leave user/password empty); execute statements. [code: sample session (1291 chars)].
### SQLLine
Lightweight JDBC CLI; supports general JDBC drivers. Clone GitHub, compile (`./mvnw package -DskipTests`); add Flink JDBC Driver + SLF4J jars; run `./bin/sqlline`; connect with `!connect jdbc:flink://...`. [code: sample (932 chars)].
### Tableau
Supports Other Database (JDBC) connection from version 2018.3 (need >= 2018.3). Download jar to Tableau driver path (Windows: C:\Program Files\Tableau\Drivers; Mac: ~/Library/Tableau/Drivers; Linux: /opt/tableau/tableau_driver/jdbc). Select Other Database (JDBC), fill Flink SQL gateway url, select SQL92 dialect, leave user/password empty.
### Use with other JDBC Tools
Any tool supporting JDBC API works with Flink JDBC driver and SQL gateway.

## Use with Application
### Java
Library for accessing Flink clusters through JDBC API. Add dependency or jar to classpath; connect with specific url; execute statements. Supports DriverManager and DataSource. [code: Sample.java using DriverManager (735 chars); DataSource.java (799 chars)].
### Other languages
Any JVM language (Scala, Kotlin). Configure frameworks like JOOQ, MyBatis, Spring Data to use the Flink JDBC driver to perform SQL queries on a Flink cluster.
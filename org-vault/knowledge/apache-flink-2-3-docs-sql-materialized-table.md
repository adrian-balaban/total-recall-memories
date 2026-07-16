---
title: Apache Flink 2.3 docs — SQL Materialized Table
tags: [org, flink, flink-2.3, docs, sql, materialized-table, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:47:59.559Z'
updated: '2026-07-07T19:47:59.559Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL — Materialized Table. Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`. Covers: overview (data freshness, refresh mode, query definition, schema), statements (CREATE [OR ALTER], ALTER — ADD/MODIFY/DROP/SUSPEND/RESUME/REFRESH/AS, DROP), deployment (architecture, gateway config, operation guide), quickstart (continuous + full mode walkthrough).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/materialized-table/overview/
# Introduction
Materialized Table is a new table type introduced in Flink SQL, aimed at simplifying both batch and stream data pipelines, providing a consistent development experience. By specifying data freshness and query when creating Materialized Table, the engine automatically derives the schema for the materialized table and creates corresponding data refresh pipeline to achieve the specified freshness.
# Core Concepts
Materialized Table encompass the following core concepts: Data Freshness, Refresh Mode, Query Definition and Schema.
## Data Freshness
Data freshness defines the maximum amount of time that the materialized table's content should lag behind updates to the base tables. Freshness is not a guarantee. Instead, it is a target that Flink attempts to meet. The data in materialized table is refreshed as closely as possible within the freshness target.
Data freshness is optional when creating a materialized table. If not specified, the system uses the default freshness based on the refresh mode: materialized-table.default-freshness.continuous (default: 3 minutes) for CONTINUOUS mode, or materialized-table.default-freshness.full (default: 1 hour) for FULL mode.
Data freshness is a crucial attribute of a materialized table, serving two main purposes:
- Determining the Refresh Mode. Currently, there are CONTINUOUS and FULL modes. For details on how to determine the refresh mode, refer to the materialized-table.refresh-mode.freshness-threshold configuration item.
  - CONTINUOUS mode: Launches a Flink streaming job that continuously refreshes the materialized table data.
  - FULL mode: The workflow scheduler periodically triggers a Flink batch job to refresh the materialized table data.
- Determining the Refresh Frequency.
  - In CONTINUOUS mode, data freshness is converted into the checkpoint interval of the Flink streaming job.
  - In FULL mode, data freshness is converted into the scheduling cycle of the workflow, e.g., a cron expression.
## Refresh Mode
There are two refresh modes: FULL and CONTINUOUS. By default, the refresh mode is inferred based on data freshness. Users can explicitly specify the refresh mode for specific business scenarios, which will take precedence over the data freshness inference.
- CONTINUOUS Mode: The Flink streaming job incrementally updates the materialized table data. The visibility of this data depends on the behavior of the corresponding Connector, either being immediate or after checkpoint completion.
- FULL Mode: The scheduler periodically triggers a full overwrite of the materialized table data, with the data refresh cycle matching the workflow's scheduling cycle.
- The default overwrite behavior is table-level. If there are partition fields and the time partition field format is specified via partition.fields.#.date-formatter, the overwrite is by partition. Only the latest partition is refreshed each time.
## Query Definition
The query definition of a materialized table supports all Flink SQL Queries. The query results are used to populate the materialized table. In CONTINUOUS mode, the query results are updated to the materialized table continuously, while in FULL mode, the query results overwrite the materialized table each time.
## Schema
The schema definition of a materialized table is the same as the regular table. It can declare primary keys and partition keys. The column names and types of materialized table are automatically inferred from the corresponding query, and users cannot specify them.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/materialized-table/statements/
# Materialized Table Statements
Flink SQL supports the following Materialized Table statements for now:
- CREATE [OR ALTER] MATERIALIZED TABLE
- ALTER MATERIALIZED TABLE
- DROP MATERIALIZED TABLE
# CREATE [OR ALTER] MATERIALIZED TABLE
`[code: CREATE [OR ALTER] MATERIALIZED TABLE [catalog_name.][db_name.]table_na ... (1282 chars)]`
## PRIMARY KEY
PRIMARY KEY defines an optional list of columns that uniquely identifies each row within the table. The column as the primary key must be non-null.
## PARTITIONED BY
PARTITIONED BY defines an optional list of columns to partition the materialized table. A directory is created for each partition if this materialized table is used as a filesystem sink.
Example: `[code: -- Create a materialized table and specify the partition field as 'ds' ... (223 chars)]`
Note
- The partition column must be included in the query statement of the materialized table.
## WITH Options
WITH Options are used to specify the materialized table properties, including connector options and time format option for partition fields.
`[code: -- Create a materialized table, specify the partition field as 'ds', a ... (302 chars)]`
As shown in the above example, we specified the date-formatter option for the ds partition column. During each scheduling, the scheduling time will be converted to the ds partition value. For example, for a scheduling time of 2024-01-01 00:00:00, only the partition ds = '2024-01-01' will be refreshed.
Note
- The partition.fields.#.date-formatter option only works in full mode.
- The field in the partition.fields.#.date-formatter must be a valid string type partition field.
## FRESHNESS
FRESHNESS defines the data freshness of a materialized table.
FRESHNESS is optional. When omitted, the system uses the default freshness based on the refresh mode: materialized-table.default-freshness.continuous (default: 3 minutes) for CONTINUOUS mode, or materialized-table.default-freshness.full (default: 1 hour) for FULL mode.
FRESHNESS and Refresh Mode Relationship
FRESHNESS defines the maximum amount of time that the materialized table's content should lag behind updates to the base tables. When not specified, it uses the default value from configuration based on the refresh mode. It does two things: firstly it determines the refresh mode of the materialized table through configuration, followed by determining the data refresh frequency to meet the actual data freshness requirements.
Explanation of FRESHNESS Parameter
The FRESHNESS parameter range is INTERVAL '<num>' { SECOND | MINUTE | HOUR | DAY }. '<num>' must be a positive integer, and in FULL mode, '<num>' should be a common divisor of the respective time interval unit.
Examples: (Assuming materialized-table.refresh-mode.freshness-threshold is 30 minutes)
`[code: -- The corresponding refresh pipeline is a streaming job with a checkp ... (504 chars)]`
Default FRESHNESS Example: (Assuming materialized-table.default-freshness.continuous is 3 minutes, materialized-table.default-freshness.full is 1 hour, and materialized-table.refresh-mode.freshness-threshold is 30 minutes)
`[code: -- FRESHNESS is omitted, uses the configured default of 3 minutes for  ... (573 chars)]`
Invalid FRESHNESS Examples:
`[code: -- Interval is a negative number ... (358 chars)]`
Note
- If FRESHNESS is not specified, the table will use the default freshness interval based on the refresh mode: materialized-table.default-freshness.continuous (default: 3 minutes) for CONTINUOUS mode, or materialized-table.default-freshness.full (default: 1 hour) for FULL mode.
- The materialized table data will be refreshed as closely as possible within the defined freshness but cannot guarantee complete satisfaction.
- In CONTINUOUS mode, setting a data freshness interval that is too short can impact job performance as it aligns with the checkpoint interval. To optimize checkpoint performance, consider enabling-changelog.
- In FULL mode, data freshness must be translated into a cron expression, consequently, only freshness intervals within predefined time spans are presently accommodated, this design ensures alignment with cron's capabilities. Specifically, support for the following freshness:
  - Second: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30.
  - Minute: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30.
  - Hour: 1, 2, 3, 4, 6, 8, 12.
  - Day: 1.
## REFRESH_MODE
REFRESH_MODE is used to explicitly specify the refresh mode of the materialized table. The specified mode takes precedence over the framework's automatic inference to meet specific scenarios' needs.
Examples: (Assuming materialized-table.refresh-mode.freshness-threshold is 30 minutes)
`[code: -- The refresh mode of the created materialized table is CONTINUOUS, a ... (492 chars)]`
## AS <select_statement>
This clause is used to define the query for populating materialized table data. The upstream table can be a materialized table, table, or view. The select statement supports all Flink SQL Queries.
Example: `[code: CREATE MATERIALIZED TABLE my_materialized_table ... (136 chars)]`
## OR ALTER
The OR ALTER clause provides create-or-update semantics:
- If the materialized table does not exist: Creates a new materialized table with the specified options
- If the materialized table exists: Modifies the query definition (behaves like ALTER MATERIALIZED TABLE AS)
This is particularly useful in declarative deployment scenarios where you want to define the desired state without checking if the materialized table already exists.
Behavior when materialized table exists: The operation updates the materialized table similarly to ALTER MATERIALIZED TABLE AS:
Full mode: Updates the schema and query definition; The materialized table is refreshed using the new query when the next refresh job is triggered.
Continuous mode: Pauses the current running refresh job; Updates the schema and query definition; Starts a new refresh job from the beginning.
See ALTER MATERIALIZED TABLE AS for more details.
## Examples
Assuming materialized-table.refresh-mode.freshness-threshold is 30 minutes.
Create a materialized table with a data freshness of 10 seconds and the derived refresh mode is CONTINUOUS:
`[code: CREATE MATERIALIZED TABLE my_materialized_table_continuous ... (595 chars)]`
Create a materialized table with a data freshness of 1 hour and the derived refresh mode is FULL:
`[code: CREATE MATERIALIZED TABLE my_materialized_table_full ... (626 chars)]`
And same materialized table with explicitly specified columns
`[code: CREATE MATERIALIZED TABLE my_materialized_table_full ( ... (128 chars)]`
The order of the columns doesn't need to be the same as in the query, Flink will do reordering if required i.e. this will be also valid
`[code: CREATE MATERIALIZED TABLE my_materialized_table_full ( ... (128 chars)]`
Another way of doing this is putting name and data type
`[code: CREATE MATERIALIZED TABLE my_materialized_table_full ( ... (163 chars)]`
It might happen that types of columns are not the same, in that case implicit casts will be applied. If for some of the combinations implicit cast is not supported then there will be validation error thrown. Also, it is worth to note that reordering can also be done here.
Create or alter a materialized table executed twice:
`[code: -- First execution: creates the materialized table ... (825 chars)]`
Note
- When altering an existing materialized table, schema evolution currently only supports adding nullable columns to the end of the original materialized table's schema.
- In continuous mode, the new refresh job will not restore from the state of the original refresh job when altering.
- All limitations from both CREATE and ALTER operations apply.
## Limitations
- Does not support explicitly specifying physical columns which are not used in the query
- Does not support referring to temporary tables, temporary views, or temporary functions in the select query
# ALTER MATERIALIZED TABLE
`[code: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name ... (1351 chars)]`
ALTER MATERIALIZED TABLE is used to manage materialized tables. This command allows users to suspend and resume refresh pipeline of materialized tables and manually trigger data refreshes, and modify the query definition of materialized tables.
## ADD
Use ADD clause to add columns (only non persisted like computed and metadata virtual), constraints, a watermark, and a distribution to an existing materialized table.
To add a column at the specified position, use FIRST or AFTER col_name. By default, the column is appended at last.
The following examples illustrate the usage of the ADD statements.
`[code: -- add a new column  ... (882 chars)]`
Note Add a column to be primary key will change the column's nullability to false implicitly.
## MODIFY
Use MODIFY clause to change column's comment, position, type (only non persisted like computed and metadata virtual also see columns), change primary key columns and watermark strategy to an existing table.
To modify an existent column to a new position, use FIRST or AFTER col_name. By default, the position remains unchanged.
The following examples illustrate the usage of the MODIFY statements.
`[code: -- modify a column type, comment and position ... (558 chars)]`
Note Modify a column to be primary key will change the column's nullability to false implicitly.
## DROP
Use the DROP clause to drop columns (only non persisted like computed and metadata virtual, also see columns), primary key, partitions, and watermark strategy to an existing table.
The following examples illustrate the usage of the DROP statements.
`[code: -- drop a column ... (418 chars)]`
## SUSPEND
`[code: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND ... (68 chars)]`
SUSPEND is used to pause the background refresh pipeline of the materialized table.
Example: `[code: -- Specify SAVEPOINT path before pausing ... (212 chars)]`
Note: When suspending a table in CONTINUOUS mode, the job will be paused using STOP WITH SAVEPOINT by default. You need to set the SAVEPOINT save path using parameters.
## RESUME
`[code: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESUME [W ... (102 chars)]`
RESUME is used to resume the refresh pipeline of a materialized table. Materialized table dynamic options can be specified through WITH options clause, which only take effect on the current refreshed pipeline and are not persistent.
Example: `[code: -- Resume the specified materialized table ... (256 chars)]`
## REFRESH
`[code: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name REFRESH [ ... (95 chars)]`
REFRESH is used to proactively trigger the refresh of the materialized table.
Example: `[code: -- Refresh the entire table data ... (209 chars)]`
Note: The REFRESH operation will start a Flink batch job to refresh the materialized table data.
## AS <select_statement>
`[code: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name AS <selec ... (82 chars)]`
The AS <select_statement> clause allows you to modify the query definition for refreshing materialized table. It will first evolve the table's schema using the schema derived from the new query and then use the new query to refresh the table data. It is important to emphasize that, by default, this does not impact historical data.
The modification process depends on the refresh mode of the materialized table:
Full mode: Update the schema and query definition of the materialized table. The table is refreshed using the new query definition when the next refresh job is triggered: If it is a partitioned table and partition.fields.#.date-formatter is correctly set, only the latest partition will be refreshed. Otherwise, the table will be overwritten entirely.
Continuous mode: Pause the current running refresh job. Update the schema and query definition of the materialized table. Start a new refresh job to refresh the materialized table: The new refresh job starts from the beginning and does not restore from the previous state. The starting offset of the data source is determined by the connector's default implementation or the dynamic hint specified in the query.
Example: `[code: -- Definition of origin materialized table ... (740 chars)]`
Note: Schema evolution currently only supports adding nullable columns to the end of the original table's schema. In continuous mode, the new refresh job will not restore from the state of the original refresh job. This may result in temporary data duplication or loss.
# DROP MATERIALIZED TABLE
`[code: DROP MATERIALIZED TABLE [IF EXISTS] [catalog_name.][database_name.]tab ... (77 chars)]`
When dropping a materialized table, the background refresh pipeline will be deleted first, and then the metadata corresponding to the materialized table will be removed from the Catalog.
Example: `[code: -- Delete the specified materialized table ... (99 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/materialized-table/deployment/
# Introduction
Creating and operating materialized tables involves multiple components' collaborative work. This document will systematically explain the complete deployment solution for Materialized Tables, covering architectural overview, environment preparation, deployment procedures, and operational practices.
# Architecture Introduction
- Client: Could be any client that can interact with Flink SQL Gateway, such as SQL Client, Flink JDBC Driver and so on.
- Flink SQL Gateway: Supports creating, altering, and dropping Materialized table. It also serves as an embedded workflow scheduler to periodically refresh full mode Materialized Table.
- Flink Cluster: The pipeline for refreshing Materialized Table will run on the Flink cluster.
- Catalog: Manages the creation, retrieval, modification, and deletion of the metadata of Materialized Table.
- Catalog Store: Supports catalog property persistence to automatically initialize catalogs for retrieving metadata in Materialized Table related operations.
# Deployment Preparation
## Flink Cluster Setup
Materialized Table refresh jobs currently support execution in these cluster environments: Standalone clusters, YARN clusters, Kubernetes clusters.
## Flink SQL Gateway Deployment
Materialized Tables must be created through SQL Gateway, which requires specific configurations for metadata persistence and job scheduling.
### Configure Catalog Store
Add catalog store configurations in config.yaml to persist catalog properties.
`[code: table: ... (115 chars)]`
Refer to Catalog Store for details.
### Configure Workflow Scheduler Plugin
Add workflow scheduler configurations in config.yaml for periodic refresh job scheduling. Currently, only the embedded scheduler is supported:
`[code: workflow-scheduler: ... (36 chars)]`
### Start SQL Gateway
Start the SQL Gateway using: `[code: ./sql-gateway.sh start ... (22 chars)]`
Note: The Catalog must support creating materialized tables, which is currently only supported by Paimon Catalog.
# Operation Guide
## Connecting to SQL Gateway
Example using SQL Client: `[code: ./sql-client.sh gateway --endpoint {gateway_endpoint}:{gateway_port} ... (68 chars)]`
## Creating Materialized Tables
### Refresh Jobs Running on Standalone Cluster
`[code: Flink SQL> SET 'execution.mode' = 'remote'; ... (183 chars)]`
### Refresh Jobs Running in Session Mode
For session modes, pre-create session cluster as documented in yarn-session or kubernetes-session.
Kubernetes session mode: `[code: Flink SQL> SET 'execution.mode' = 'kubernetes-session'; ... (303 chars)]` — Set execution.mode to kubernetes-session and specify a valid kubernetes.cluster-id corresponding to an existing Kubernetes session cluster.
YARN session mode: `[code: Flink SQL> SET 'execution.mode' = 'yarn-session'; ... (285 chars)]` — Set execution.mode to yarn-session and specify a valid yarn.application.id corresponding to an existing YARN session cluster.
### Refresh Jobs Running in Application Mode
Kubernetes application mode: `[code: Flink SQL> SET 'execution.mode' = 'kubernetes-application'; ... (311 chars)]` — Set execution.mode to kubernetes-application. The kubernetes.cluster-id is optional; if not set, it will be automatically generated.
YARN application mode: `[code: Flink SQL> SET 'execution.mode' = 'yarn-application'; ... (193 chars)]` — Only set execution.mode to yarn-application. The yarn.application.id doesn't need to be set; it will be automatically generated during submission.
## Maintenance Operations
Cluster information (e.g., execution.mode or kubernetes.cluster-id) is already persisted in the catalog and does not need to be set when suspend or resume the refresh jobs of Materialized Table.
### Suspend Refresh Job: `[code: -- Suspend the MATERIALIZED TABLE refresh job ... (148 chars)]`
### Resume Refresh Job: `[code: -- Resume the MATERIALIZED TABLE refresh job ... (146 chars)]`
### Modify Query Definition: `[code: -- Modify the MATERIALIZED TABLE query definition ... (163 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/materialized-table/quickstart/
# Quickstart Guide
This guide will help you quickly understand and get started with materialized tables. It includes setting up the environment and creating, altering, and dropping materialized tables in CONTINUOUS and FULL mode.
# Environment Setup
## Directory Preparation
Replace the example paths below with real paths on your machine.
- Create directories for Catalog Store and test-filesystem Catalog: `[code: # Directory for File Catalog Store to save catalog information ... (289 chars)]`
- Create directories for Checkpoints and Savepoints: `[code: mkdir -p {checkpoints_path} ... (55 chars)]`
## Resource Preparation
The method here is similar to the steps recorded in local installation. Flink can run on any UNIX-like operating system, such as Linux, Mac OS X, and Cygwin (for Windows).
Download the latest Flink binary package and extract it: `[code: tar -xzf flink-*.tgz ... (20 chars)]`
Download the test-filesystem connector and place it in the lib directory: `[code: cp flink-table-filesystem-test-utils-{VERSION}.jar flink-*/lib/ ... (63 chars)]`
## Configuration Preparation
Edit the config.yaml file and add the following configurations: `[code: execution: ... (362 chars)]`
## Start Flink Cluster
`[code: ./bin/start-cluster.sh ... (22 chars)]`
## Start SQL Gateway
`[code: ./bin/sql-gateway.sh start ... (26 chars)]`
## Start SQL Client
`[code: ./bin/sql-client.sh gateway --endpoint http://127.0.0.1:8083 ... (60 chars)]`
## Create Catalog and Source Table
- Create the test-filesystem catalog: `[code: CREATE CATALOG mt_cat WITH ( ... (141 chars)]`
- Create the Source table: `[code: -- 1. Create Source table and specify the data format is json ... (652 chars)]`
# Create Continuous Mode Materialized Table
## Create Materialized Table
Create a materialized table in CONTINUOUS mode with a data freshness of 30 seconds. You can find the Flink streaming job for continuous refresh the materialized table is running on the page http://localhost:8081. And it's checkpoint interval is 30 seconds.
`[code: CREATE MATERIALIZED TABLE continuous_users_shops ... (456 chars)]`
## Suspend Materialized Table
Suspend the refresh pipeline of the materialized table. Your will find that the Flink streaming job for continuous refresh the materialized table transitions to FINISHED state on http://localhost:8081. Before executing the suspend operation, you need to set the savepoint path.
`[code: -- Set savepoint path before suspending ... (171 chars)]`
## Query Materialized Table
Query the materialized table data and confirm that data has already been written. `[code: SELECT * FROM continuous_users_shops; ... (37 chars)]`
## Resume Materialized Table
Resume the refresh pipeline of the materialized table. You will find that a new Flink streaming job for continuous refresh the materialized table is started and restored state from the specified savepoint path on http://localhost:8081 page.
`[code: ALTER MATERIALIZED TABLE continuous_users_shops RESUME; ... (55 chars)]`
## Drop Materialized Table
Drop the materialized table, and you will find that the Flink streaming job for continuous refresh the materialized table transitions to the CANCELED state on http://localhost:8081 page.
`[code: DROP MATERIALIZED TABLE continuous_users_shops; ... (47 chars)]`
# Create Full Mode Materialized Table
## Create Materialized Table
Create a materialized table in FULL mode with a data freshness of 1 minute. (Here we set freshness to 1 minute just for convenience of testing) You will find that the Flink Batch job for periodic refreshing the materialized table is scheduled every 1 minute on the http://localhost:8081.
`[code: CREATE MATERIALIZED TABLE full_users_shops ... (412 chars)]`
## Query Materialized Table
Insert some data into today's partition. Wait at least 1 minute and query the materialized table results to find that only today's partition data is refreshed.
`[code: INSERT INTO json_source VALUES  ... (255 chars)]`
`[code: SELECT * FROM full_users_shops; ... (31 chars)]`
## Manually Refresh Historical Partition
Manually refresh the partition ds='2024-06-20' and verify the data in the materialized table. You can find the Flink batch job for the current refresh operation on the http://localhost:8081 page.
`[code: -- Manually refresh historical partition ... (184 chars)]`
## Suspend and Resume Materialized Table
By suspending and resuming operations, you can control the refresh jobs corresponding to the materialized table. After suspending, the Flink batch job for periodic refreshing the materialized table will not be scheduled. After resuming, the Flink batch job for periodic refreshing the materialized table will be rescheduled again. You can find the Flink job scheduling status on the http://localhost:8081 page.
`[code: -- Suspend background refresh pipeline ... (178 chars)]`
## Drop Materialized Table
After dropping the materialized table, the Flink batch job for periodic refreshing the materialized table will not be scheduled again. You can confirm this on the http://localhost:8081 page.
`[code: DROP MATERIALIZED TABLE full_users_shops; ... (41 chars)]`
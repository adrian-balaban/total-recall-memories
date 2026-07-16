---
title: Apache Flink 2.3 docs — Dev Table (overview + intro + functions overview)
tags: [org, flink, flink-2.3, docs, dev, table-api, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:10:40.996Z'
updated: '2026-07-08T04:10:40.996Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/overview/
# Flink APIs

Apache Flink provides two main APIs for building streaming and batch applications: the Table API and the DataStream API. Both APIs can be used with Java, Scala, and Python.

## Choosing the Right Approach

Flink offers a spectrum from fully declarative to fully imperative programming:

[table: 5 rows]

### Start Declarative

For most use cases, start with SQL or the Table API:

- Flink optimizes your queries automatically

- Built-in operators cover common patterns (joins, aggregations, windows)

- Pattern matching is available via MATCH_RECOGNIZE

### Extend When Needed

When built-in capabilities aren't enough:

- User-Defined Functions (UDFs): Add custom scalar, table, or aggregate functions

- ProcessTableFunction: Access state and timers for specific parts of your pipeline while staying in the Table API

### Go Imperative for Full Control

The DataStream API gives you fine-grained control over Flink's execution model, but requires understanding state management, operator chaining, and serialization. It's best suited for experienced users who have specific needs that the Table API cannot address.

Use the DataStream API when:

- You want to build the entire application imperatively

- Your use case doesn't fit the relational/table abstraction

- You need control over aspects that Table API doesn't expose

## Comparing the APIs

[table: 6 rows]

## Mixing APIs

The Table API and DataStream API can be used together. You can:

- Convert a DataStream to a Table for relational operations

- Convert a Table back to a DataStream for low-level processing

- Use SQL within a DataStream application

See DataStream API Integration for details.

## Where to Go Next

- Table API: Declarative API for relational operations.

- DataStream API: Imperative API for stream processing.

- Configuration: Project setup and dependencies.

### Getting Started Tutorials

- Flink SQL Tutorial: Get started with SQL (no programming required).

- Table API Tutorial: Build a streaming pipeline with the Table API.

- DataStream API Tutorial: Build an event-driven application with the DataStream API.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/overview/
# Table API

The Table API is a language-integrated query API for Java, Scala, and Python that allows the composition of queries from relational operators such as selection, filter, and join in a very intuitive way. It shares the same underlying engine as Flink SQL, meaning queries specified in either interface have the same semantics and specify the same result regardless of whether the input is continuous (streaming) or bounded (batch).

The Table API and SQL integrate seamlessly with each other and Flink's DataStream API. You can easily switch between all APIs and libraries which build upon them. For instance, you can detect patterns from a table using the MATCH_RECOGNIZE clause and later use the DataStream API to build alerting based on the matched patterns.

## When to Use the Table API

Use the Table API when you want to:

- Write relational queries in Java, Scala, or Python code

- Compose queries programmatically with type-safe operators

- Combine SQL statements with programmatic table operations

- Integrate with the DataStream API for mixed use cases

For pure SQL usage without programming, see Flink SQL.

## Table Program Dependencies

You will need to add the Table API as a dependency to a project in order to use the Table API for defining data pipelines.

For more information on how to configure these dependencies for Java and Scala, please refer to the project configuration section.

If you are using Python, please refer to the PyFlink documentation.

## Where to Go Next

- Concepts & Common API: Shared concepts and APIs of the Table API and SQL.

- Data Types: Lists pre-defined data types and their properties.

- Streaming Concepts: Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.

- Connect to External Systems: Available connectors and formats for reading and writing data to external systems.

- Table API Operations: Supported operations and API for the Table API.

- SQL Reference: Supported operations and syntax for SQL.

- Built-in Functions: Supported functions in Table API and SQL.

- PyFlink: Python-specific Flink documentation.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/common/
# Concepts & Common API

The Table API and SQL are integrated in a joint API.
The central concept of this API is a Table which serves as input and output of queries.
This document shows the common structure of programs with Table API and SQL queries, how to register a Table, how to query a Table, and how to emit a Table.

## Structure of Table API and SQL Programs

The following code example shows the common structure of Table API and SQL programs.

  
All Flink Scala APIs are deprecated and will be removed in a future Flink version. You can still build your application in Scala, but you should move to the Java version of either the DataStream and/or Table API.

See FLIP-265 Deprecate and remove Scala API support

  Java
  
`[code: import org.apache.flink.table.api.*; ... (1072 chars)]`

  Scala
  
`[code: import org.apache.flink.table.api._ ... (1029 chars)]`

  Python
  
`[code: from pyflink.table import * ... (782 chars)]`

  Table API and SQL queries can be easily integrated with and embedded into DataStream programs.
Have a look at the DataStream API Integration page
to learn how DataStreams can be converted into Tables and vice versa.

## Create a TableEnvironment

The TableEnvironment is the entrypoint for Table API and SQL integration and is responsible for:

- Registering a Table in the internal catalog

- Registering catalogs

- Loading pluggable modules

- Executing SQL queries

- Registering a user-defined (scalar, table, or aggregation) function

- Converting between DataStream and Table (in case of StreamTableEnvironment)

For a complete reference of all TableEnvironment methods, see the TableEnvironment API page.

A Table is always bound to a specific TableEnvironment.
It is not possible to combine tables of different TableEnvironments in the same query, e.g., to join or union them.
A TableEnvironment is created by calling the static TableEnvironment.create() method.

  Java
  
`[code: import org.apache.flink.table.api.EnvironmentSettings; ... (295 chars)]`

  Scala
  
`[code: import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironme ... (231 chars)]`

  Python
  
`[code: from pyflink.table import EnvironmentSettings, TableEnvironment ... (343 chars)]`

Alternatively, users can create a StreamTableEnvironment from an existing StreamExecutionEnvironment
to interoperate with the DataStream API.

  Java
  
`[code: import org.apache.flink.streaming.api.environment.StreamExecutionEnvir ... (356 chars)]`

  Scala
  
`[code: import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment ... (302 chars)]`

  Python
  
`[code: from pyflink.datastream import StreamExecutionEnvironment ... (215 chars)]`

## Create Tables in the Catalog

A TableEnvironment maintains a map of catalogs of tables which are created with an identifier. Each
identifier consists of 3 parts: catalog name, database name and object name. If a catalog or database is not
specified, the current default value will be used (see examples in the Table identifier expanding section).

Tables can be either virtual (VIEWS) or regular (TABLES). VIEWS can be created from an
existing Table object, usually the result of a Table API or SQL query. TABLES describe
external data, such as a file, database table, or message queue.

### Temporary vs Permanent tables.

Tables may either be temporary, and tied to the lifecycle of a single Flink session, or permanent,
and visible across multiple Flink sessions and clusters.

Permanent tables require a catalog (such as Hive Metastore)
to maintain metadata about the table. Once a permanent table is created, it is visible to any Flink
session that is connected to the catalog and will continue to exist until the table is explicitly
dropped.

On the other hand, temporary tables are always stored in memory and only exist for the duration of
the Flink session they are created within. These tables are not visible to other sessions. They are
not bound to any catalog or database but can be created in the namespace of one. Temporary tables
are not dropped if their corresponding database is removed.

#### Shadowing

It is possible to register a temporary table with the same identifier as an existing permanent
table. The temporary table shadows the permanent one and makes the permanent table inaccessible as
long as the temporary one exists. All queries with that identifier will be executed against the
temporary table.

This might be useful for experimentation. It allows running exactly the same query first against a
temporary table that e.g. has just a subset of data, or the data is obfuscated. Once verified that
the query is correct it can be run against the real production table.

### Create a Table

#### Virtual Tables

A Table API object corresponds to a VIEW (virtual table) in SQL terms. It encapsulates a logical
query plan. It can be created in a catalog as follows:

  Java
  
`[code: // get a TableEnvironment ... (323 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (312 chars)]`

  Python
  
`[code: # get a TableEnvironment ... (298 chars)]`

Note: Table objects are similar to VIEW's from relational database
systems, i.e., the query that defines the Table is not optimized but will be inlined when another
query references the registered Table. If multiple queries reference the same registered Table,
it will be inlined for each referencing query and executed multiple times, i.e., the result of the
registered Table will not be shared.

#### Connector Tables

It is also possible to create a TABLE as known from relational databases from a connector declaration.
The connector describes the external system that stores the data of a table. Storage systems such as Apache Kafka or a regular file system can be declared here.

Such tables can either be created using the Table API directly, or by switching to SQL DDL.

  Java
  
`[code: // Using table descriptors ... (478 chars)]`

  Python
  
`[code: # Using table descriptors ... (451 chars)]`

### Expanding Table identifiers

Tables are always registered with a 3-part identifier consisting of catalog, database, and table name.

Users can set one catalog and one database inside it to be the "current catalog" and "current database".
With them, the first two parts in the 3-parts identifier mentioned above can be optional - if they are not provided,
the current catalog and current database will be referred. Users can switch the current catalog and current database via
table API or SQL.

Identifiers follow SQL requirements which means that they can be escaped with a backtick character (`).

  Java
  
`[code: TableEnvironment tEnv = ...; ... (869 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (897 chars)]`

  Python
  
`[code: # get a TableEnvironment ... (707 chars)]`

## Query a Table

### Table API

The Table API is a language-integrated query API for Scala and Java. In contrast to SQL, queries are not specified as Strings but are composed step-by-step in the host language.

The API is based on the Table class which represents a table (streaming or batch) and offers methods to apply relational operations. These methods return a new Table object, which represents the result of applying the relational operation on the input Table. Some relational operations are composed of multiple method calls such as table.groupBy(...).select(), where groupBy(...) specifies a grouping of table, and select(...) the projection on the grouping of table.

The Table API document describes all Table API operations that are supported on streaming and batch tables.

The following example shows a simple Table API aggregation query:

  Java
  
`[code: // get a TableEnvironment ... (457 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (417 chars)]`

Note: The Scala Table API uses Scala String interpolation that starts with a dollar sign ($) to reference the attributes of a Table. The Table API uses Scala implicits. Make sure to import

- org.apache.flink.table.api._ - for implicit expression conversions

- org.apache.flink.api.scala._ and org.apache.flink.table.api.bridge.scala._ if you want to convert from/to DataStream.

  Python
  
`[code: # get a TableEnvironment ... (441 chars)]`

### SQL

Flink's SQL integration is based on Apache Calcite, which implements the SQL standard. SQL queries are specified as regular Strings.

The SQL document describes Flink's SQL support for streaming and batch tables.

The following example shows how to specify a query and return the result as a Table.

  Java
  
`[code: // get a TableEnvironment ... (393 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (373 chars)]`

  Python
  
`[code: // get a TableEnvironment ... (357 chars)]`

The following example shows how to specify an update query that inserts its result into a registered table.

  Java
  
`[code: // get a TableEnvironment ... (442 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (418 chars)]`

  Python
  
`[code: // get a TableEnvironment ... (411 chars)]`

### Mixing Table API and SQL

Table API and SQL queries can be easily mixed because both return Table objects:

- A Table API query can be defined on the Table object returned by a SQL query.

- A SQL query can be defined on the result of a Table API query by registering the resulting Table in the TableEnvironment and referencing it in the FROM clause of the SQL query.

## Emit a Table

A Table is emitted by writing it to a TableSink. A TableSink is a generic interface to support a wide variety of file formats (e.g. CSV, Apache Parquet, Apache Avro), storage systems (e.g., JDBC, Apache HBase, Apache Cassandra, Elasticsearch), or messaging systems (e.g., Apache Kafka, RabbitMQ).

A batch Table can only be written to a BatchTableSink, while a streaming Table requires either an AppendStreamTableSink, a RetractStreamTableSink, or an UpsertStreamTableSink.

Please see the documentation about Table Sources & Sinks for details about available sinks and instructions for how to implement a custom DynamicTableSink.

The Table.insertInto(String tableName) method defines a complete end-to-end pipeline emitting the source table to a registered sink table.
The method looks up the table sink from the catalog by the name and validates that the schema of the Table is identical to the schema of the sink.
A pipeline can be explained with TablePipeline.explain() and executed invoking TablePipeline.execute().

The following examples shows how to emit a Table:

  Java
  
`[code: // get a TableEnvironment ... (872 chars)]`

  Scala
  
`[code: // get a TableEnvironment ... (838 chars)]`

  Python
  
`[code: // get a TableEnvironment ... (704 chars)]`

### Print the Table

You can print the content of a Table to the console for debugging and development purposes. This is done by calling Table.execute().print().

  Java
  
`[code: Table table = tableEnv.fromValues(1, 2, 3); ... (68 chars)]`

  Scala
  
`[code: val table = tableEnv.fromValues(1, 2, 3) ... (64 chars)]`

  Python
  
`[code: table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'dat ... (98 chars)]`

  This triggers the materialization of the table and collects the content to the client's memory. For large tables, it's recommended to limit the number of rows using Table.limit().

### Collect Results to Client

You can collect the results of a Table to the client using TableResult.collect(). This returns an iterator that you can use to process results programmatically.

  Java
  
`[code: Table table = tableEnv.fromValues(1, 2, 3); ... (192 chars)]`

  Scala
  
`[code: val table = tableEnv.fromValues(1, 2, 3) ... (178 chars)]`

  Python
  
`[code: table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'dat ... (160 chars)]`

  This triggers the materialization of the table and collects the content to the client's memory. For large tables, it's recommended to limit the number of rows using Table.limit().

### Emit Results to Multiple Sink Tables

You can use a StatementSet to emit multiple Tables to multiple sink tables in a single job. This is more efficient than executing multiple separate jobs.

  Java
  
`[code: // create source table ... (512 chars)]`

  Scala
  
`[code: // create source table ... (494 chars)]`

  Python
  
`[code: # create source table ... (793 chars)]`

## Translate and Execute a Query

Table API and SQL queries are translated into DataStream programs whether their input is streaming or batch.
A query is internally represented as a logical query plan and is translated in two phases:

- Optimization of the logical plan,

- Translation into a DataStream program.

A Table API or SQL query is translated when:

- TableEnvironment.executeSql() is called. This method is used for executing a given statement, and the sql query is translated immediately once this method is called.

- TablePipeline.execute() is called. This method is used for executing a source-to-sink pipeline, and the Table API program is translated immediately once this method is called.

- Table.execute() is called. This method is used for collecting the table content to the local client, and the Table API is translated immediately once this method is called.

- StatementSet.execute() is called. A TablePipeline (emitted to a sink through StatementSet.add()) or an INSERT statement (specified through StatementSet.addInsertSql()) will be buffered in StatementSet first. They are transformed once StatementSet.execute() is called. All sinks will be optimized into one DAG.

- A Table is translated when it is converted into a DataStream (see Integration with DataStream). Once translated, it's a regular DataStream program and is executed when StreamExecutionEnvironment.execute() is called.

## Query Optimization

Apache Flink leverages and extends Apache Calcite to perform sophisticated query optimization.
This includes a series of rule and cost-based optimizations such as:

- Subquery decorrelation based on Apache Calcite

- Project pruning

- Partition pruning

- Filter push-down

- Sub-plan deduplication to avoid duplicate computation

- Special subquery rewriting, including two parts:

- Converts IN and EXISTS into left semi-joins

- Converts NOT IN and NOT EXISTS into left anti-join

- Optional join reordering

- Enabled via table.optimizer.join-reorder-enabled

Note: IN/EXISTS/NOT IN/NOT EXISTS are currently only supported in conjunctive conditions in subquery rewriting.

The optimizer makes intelligent decisions, based not only on the plan but also rich statistics available from the data sources and fine-grain costs for each operator such as io, cpu, network, and memory.

Advanced users may provide custom optimizations via a CalciteConfig object that can be provided to the table environment by calling TableEnvironment#getConfig#setPlannerConfig.

## Explaining a Table

The Table API provides a mechanism to explain the logical and optimized query plans to compute a Table.
This is done through the Table.explain() method or StatementSet.explain() method. Table.explain()returns the plan of a Table. StatementSet.explain() returns the plan of multiple sinks. It returns a String describing three plans:

- the Abstract Syntax Tree of the relational query, i.e., the unoptimized logical query plan,

- the optimized logical query plan, and

- the physical execution plan.

TableEnvironment.explainSql() and TableEnvironment.executeSql() support execute a EXPLAIN statement to get the plans, Please refer to EXPLAIN page.

The following code shows an example and the corresponding output for given Table using Table.explain() method:

  Java
  
`[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (602 chars)]`

  Scala
  
`[code: val env = StreamExecutionEnvironment.getExecutionEnvironment ... (355 chars)]`

  Python
  
`[code: env = StreamExecutionEnvironment.get_execution_environment() ... (332 chars)]`

The result of the above example is

  Explain
  
    
`[code:  ... (808 chars)]`

  

The following code shows an example and the corresponding output for multiple-sinks plan using StatementSet.explain() method:

  Java
  
`[code:  ... (1285 chars)]`

  Scala
  
`[code: val settings = EnvironmentSettings.inStreamingMode() ... (1207 chars)]`

  Python
  
`[code: settings = EnvironmentSettings.in_streaming_mode() ... (1230 chars)]`

the result of multiple-sinks plan is

  MultiTable Explain
  
    
`[code:  ... (2313 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/table_environment/
# TableEnvironment

This document is an introduction to the TableEnvironment, the central concept of the Table API.
It includes detailed descriptions of the public interfaces of the TableEnvironment class.

## Create a TableEnvironment

The recommended way to create a TableEnvironment is to create from an EnvironmentSettings object:

  Java
  
`[code: import org.apache.flink.table.api.EnvironmentSettings; ... (317 chars)]`

  Scala
  
`[code: import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironme ... (253 chars)]`

  Python
  
`[code: from pyflink.common import Configuration ... (407 chars)]`

Alternatively, users can create a StreamTableEnvironment from an existing StreamExecutionEnvironment to interoperate with the DataStream API.

  Java
  
`[code: import org.apache.flink.streaming.api.environment.StreamExecutionEnvir ... (378 chars)]`

  Scala
  
`[code: import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment ... (325 chars)]`

  Python
  
`[code: from pyflink.datastream import StreamExecutionEnvironment ... (287 chars)]`

## TableEnvironment API

### Table/SQL Operations

These APIs are used to create/remove Table API/SQL Tables and write queries:

[table: 12 rows]

See the Java API docs and Python API docs for complete API reference.

Deprecated APIs

[table: 5 rows]

### Execute/Explain Jobs

These APIs are used to explain/execute jobs. Note that executeSql / execute_sql can also be used to execute jobs.

[table: 3 rows]

### Create/Drop User Defined Functions

These APIs are used to register UDFs or remove registered UDFs.
Note that executeSql / execute_sql can also be used to register/remove UDFs via SQL DDL.
For more details about UDFs, see User Defined Functions.

[table: 9 rows]

### Dependency Management (Python only)

These APIs are used to manage Python dependencies required by Python UDFs.
See Dependency Management for more details.

[table: 4 rows]

### Configuration

  Java
  
`[code: // get the TableConfig ... (180 chars)]`

See Configuration for all available options.

  Scala
  
`[code: // get the TableConfig ... (167 chars)]`

See Configuration for all available options.

  Python
  
`[code: # get the TableConfig ... (165 chars)]`

See Configuration and Python Configuration for all available options.

### Catalog APIs

These APIs are used to access catalogs and modules. See Modules and Catalogs for more details.

[table: 20 rows]

## Statebackend, Checkpoint and Restart Strategy

You can configure statebackend, checkpointing, and restart strategy by setting key-value options in TableConfig.
See Fault Tolerance, State Backends, and Checkpointing for more details.

  Java
  
`[code: TableConfig config = tableEnv.getConfig(); ... (607 chars)]`

  Scala
  
`[code: val config = tableEnv.getConfig ... (589 chars)]`

  Python
  
`[code: config = table_env.get_config() ... (585 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/overview/
# Functions

Flink Table API & SQL empowers users to do data transformations with functions.

## Types of Functions

There are two dimensions to classify functions in Flink.

One dimension is system (or built-in) functions v.s. catalog functions. System functions have no namespace and can be
referenced with just their names. Catalog functions belong to a catalog and database therefore they have catalog and database
namespaces, they can be referenced by either fully/partially qualified name (catalog.db.func or db.func) or just the
function name.

The other dimension is temporary functions v.s. persistent functions. Temporary functions are volatile and only live up to
lifespan of a session, they are always created by users. Persistent functions live across lifespan of sessions, they are either
provided by the system or persisted in catalogs.

The two dimensions give Flink users 4 categories of functions:

- Temporary system functions

- System functions

- Temporary catalog functions

- Catalog functions

## Referencing Functions

There are two ways users can reference a function in Flink - referencing function precisely or ambiguously.

### Precise Function Reference

Precise function reference empowers users to use catalog functions specifically, and across catalog and across database,
e.g. select mycatalog.mydb.myfunc(x) from mytable and select mydb.myfunc(x) from mytable.

This is only supported starting from Flink 1.10.

### Ambiguous Function Reference

In ambiguous function reference, users just specify the function's name in SQL query, e.g. select myfunc(x) from mytable.

## Function Resolution Order

The resolution order only matters when there are functions of different types but the same name,
e.g. when there're three functions all named "myfunc" but are of temporary catalog, catalog, and system function respectively.
If there's no function name collision, functions will just be resolved to the sole one.

### Precise Function Reference

Because system functions don't have namespaces, a precise function reference in Flink must be pointing to either a temporary catalog
function or a catalog function.

The resolution order is:

- Temporary catalog function

- Catalog function

### Ambiguous Function Reference

The resolution order is:

- Temporary system function

- System function

- Temporary catalog function, in the current catalog and current database of the session

- Catalog function, in the current catalog and current database of the session

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/python-udfs/
# Python User-defined Functions

Python UDF documentation has moved to the PyFlink documentation.

See the 

    Python User-defined Functions

 page in the PyFlink docs.

## (no article)
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/systemfunctions/

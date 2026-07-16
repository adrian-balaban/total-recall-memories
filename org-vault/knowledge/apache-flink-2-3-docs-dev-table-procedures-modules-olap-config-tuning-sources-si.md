---
title: 'Apache Flink 2.3 docs — Dev Table Procedures, Modules, OLAP, Config, Tuning, Sources/Sinks'
tags: [org, flink, flink-2.3, docs, dev, table-api, procedures, modules, olap, config, tuning, sources-sinks, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:17:09.273Z'
updated: '2026-07-08T04:17:09.273Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/procedures/
# Procedures

Flink Table API & SQL empowers users to perform data manipulation and administrative tasks with procedures. Procedures can run FLINK jobs with the provided StreamExecutionEnvironment, making them more powerful and flexible.

## Implementation Guide

To call a procedure, it must be available in a catalog. To provide procedures in a catalog, you need to implement the procedure and then return it using the Catalog.getProcedure(ObjectPath procedurePath) method.

### Procedure Class

An implementation class must implement the interface org.apache.flink.table.procedures.Procedure. The class must be declared public, not abstract, and should be globally accessible. Thus, non-static inner or anonymous classes are not allowed.

### Call Methods

The interface doesn't provide any method, you have to define a method named call in which you can implement the logic of the procedure. The methods must be declared public and take a well-defined set of arguments.

Please note:

- The first parameter of the method call should always be ProcedureContext which provides the method getExecutionEnvironment to get a StreamExecutionEnvironment for running a Flink Job
- The return type should always be an array, like int[], String[], etc

Regular JVM method calling semantics apply. Therefore, it is possible to:
- implement overloaded methods such as call(ProcedureContext, Integer) and call(ProcedureContext, LocalDateTime)
- use var-args such as call(ProcedureContext, Integer...)
- use object inheritance such as call(ProcedureContext, Object) that takes both LocalDateTime and Integer
- and combinations of the above such as call(ProcedureContext, Object...) that takes all kinds of arguments

If you intend to implement procedures in Scala, please add the scala.annotation.varargs annotation in case of variable arguments. Furthermore, it is recommended to use boxed primitives (e.g. java.lang.Integer instead of Int) to support NULL.

### Type Inference

The table ecosystem (similar to the SQL standard) is a strongly typed API. Therefore, both procedure parameters and return types must be mapped to a data type. Flink's procedures implement an automatic type inference extraction that derives data types from the procedure's class and its call methods via reflection. If this implicit reflective extraction approach is not successful, the extraction process can be supported by annotating affected parameters, classes, or methods with @DataTypeHint and @ProcedureHint.

Note: although the return type in call method must be array type T[], if use @DataTypeHint to annotate the return type, it's actually expected to annotate the component type of the array type, which is actually T.

#### Automatic Type Inference (@DataTypeHint, @ProcedureHint)

@DataTypeHint supports automatic extraction inline for parameters and return types. @ProcedureHint can provide a mapping from argument data types to a result data type, enabling annotating entire procedure classes or call methods for input and result data types. One or more annotations can be declared on top of a class or individually for each call method for overloading procedure signatures. All hint parameters are optional. Hint parameters defined on top of a procedure class are inherited by all call methods.

### Named Parameters (@ArgumentHint)

When calling a procedure, you can use parameter names to specify parameter values. Named parameters allow you to pass both the parameter name and value, avoiding confusion caused by wrong parameter order. Named parameters can also omit non-required parameters, which are filled with null by default. Use the @ArgumentHint annotation to specify the name, type, and whether the parameter is required. @ArgumentHint can be applied on parameters of the call method, on the call method, or on the procedure class.

- @ArgumentHint annotation already contains @DataTypeHint annotation, so it cannot be used together with @DataTypeHint in @ProcedureHint. When applied to function parameters, @ArgumentHint cannot be used with @DataTypeHint at the same time, and it is recommended to use @ArgumentHint.
- Named parameters only take effect when the corresponding procedure class does not contain overloaded functions and variable parameter functions, otherwise using named parameters will cause an error.

### Return Procedure in Catalog

After implementing a procedure, the catalog can then return the procedure in method Catalog.getProcedure(ObjectPath procedurePath). Also, it's expected to list all the procedures in method Catalog.listProcedures(String dbName).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/modules/
# Modules

Modules allow users to extend Flink's built-in objects, such as defining functions that behave like Flink built-in functions. They are pluggable, and while Flink provides a few pre-built modules, users can write their own. A module can provide built-in table source and sink factories which disable Flink's default discovery mechanism based on Java's Service Provider Interfaces (SPI), or influence how connectors of temporary tables should be created without a corresponding catalog.

## Module Types

- CoreModule: contains all of Flink's system (built-in) functions and is loaded and enabled by default.
- HiveModule: provides Hive built-in functions as Flink's system functions to SQL and Table API users.
- User-Defined Module: develop custom modules by implementing the Module interface. To use custom modules in SQL CLI, users should develop both a module and its corresponding module factory by implementing the ModuleFactory interface. A module factory defines a set of properties for configuring the module when the SQL CLI bootstraps. Properties are passed to a discovery service where the service tries to match the properties to a ModuleFactory and instantiate a corresponding module instance.

## Module Lifecycle and Resolution Order

A module can be loaded, enabled, disabled and unloaded. When TableEnvironment loads a module initially, it enables the module by default. Flink supports multiple modules and keeps track of the loading order to resolve metadata. Flink only resolves the functions among enabled modules. When two functions of the same name reside in two modules:
- If both of the modules are enabled, Flink resolves the function according to the resolution order of the modules.
- If one of them is disabled, Flink resolves the function to the enabled module.
- If both of the modules are disabled, Flink cannot resolve the function.

Users can change the resolution order by using modules in a different declared order (e.g. USE MODULES hive, core finds functions first in Hive). Users can also disable modules by not declaring them (e.g. USE MODULES hive disables core module, though strongly not recommended). Disable a module does not unload it; users can enable it again by using it. A module can be enabled only when it is loaded already. Using an unloaded module will throw an Exception. The difference between disabling and unloading a module is that TableEnvironment still keeps the disabled modules.

## Namespace

Objects provided by modules are considered part of Flink's system (built-in) objects; thus, they don't have any namespaces.

## How to Load, Unload, Use and List Modules

Users can use SQL to load/unload/use/list modules in both Table API and SQL CLI. YAML-defined modules must provide a type property. Using SQL, module name is used to perform the module discovery; it is parsed as a simple identifier and case-sensitive. Users can also use Java, Scala or Python to load/unload/use/list modules programmatically.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/olap_quickstart/
# OLAP Quickstart

OLAP (OnLine Analysis Processing) is a key technology in the field of data analysis, generally used to perform complex queries on large data sets with latencies in seconds. Now Flink can support streaming and batch computing, and also supports users to deploy it as an OLAP computing service.

## Architecture Introduction

Flink OLAP service consists of three parts: Client, Flink SQL Gateway and Flink Session Cluster.
- Client: Could be any client that can interact with Flink SQL Gateway, such as SQL Client, Flink JDBC Driver and so on.
- Flink SQL Gateway: provides an easy way to parse the sql query, look up the metadata, analyze table stats, optimize the plan and submit JobGraphs to cluster.
- Flink Session Cluster: OLAP queries run on session cluster, mainly to avoid the overhead of cluster startup.

### Advantage

- Massively Parallel Processing: Flink OLAP runs naturally as a massively parallel processing system, which enables planners to easily adjust the job parallelism to fulfill queries' latency requirement under different data sizes.
- Elastic Resource Management: Flink's resource management supports min/max scaling, so the session cluster can allocate resource according to workload dynamically.
- Reuse Connectors: Flink OLAP can reuse the rich Connectors in Flink ecosystem.
- Unified Engine: Unified computing engine for Streaming/Batch/OLAP.

## Deploying in Local Mode

Requires Java 11. Download the latest binary release of Flink, extract, start local cluster with ./bin/start-cluster.sh (web UI at http://localhost:8081), start SQL Client CLI with an embedded gateway via ./bin/sql-client.sh. Execute queries and retrieve results in CLI.

## Deploying in Production

### Client — Flink JDBC Driver

You should use Flink JDBC Driver when submitting queries to SQL Gateway since it provides low-level connection management. Reuse the JDBC connection to avoid frequently creating/closing sessions in the Gateway and reduce E2E query latency.

### Cluster Deployment

- Session Cluster: deploy on Native Kubernetes using session mode (dynamically allocate and de-allocate TaskManagers). Configure slotmanager.number-of-slots.min in session cluster to significantly reduce the cold start time of your query (FLIP-362).
- SQL Gateway: deploy it as a stateless microservice and register the instance on service discovery component so client can balance the query between instances easily.

### Datasource Configurations

- Catalogs: configure FileCatalogStore provided by Catalogs as the catalog used by cluster. Catalog information will not change frequently and should be re-used cross sessions to reduce the cold-start cost.
- Connectors: Both Session Cluster and SQL Gateway rely on connectors to analyze table stats and read data from the configured data source.

### Recommended Cluster Configurations

Tables of recommended options for: SQL&Table Options (4 rows), Runtime Options (5 rows), Scheduling Options (6 rows), Network Options (4 rows), ResourceManager Options (10 rows). Using JDK17 within ZGC can greatly help optimize the metaspace garbage collection issue (FLINK-32746). ZGC can provide close to zero application pause time. OLAP queries need to be executed in BATCH mode because both Pipelined and Blocking edges may appear in the execution plan of an OLAP query. Batch scheduler allows queries to be scheduled in stages, avoiding scheduling deadlocks. Configure slotmanager.number-of-slots.min as reserved resource pool. TaskManager should configure with large resource specification in OLAP scenario (more local computation, less network/deserialization/serialization overhead). JobManager also prefer large resource specification.

## Future Work

Flink OLAP is part of Apache Flink Roadmap. Relevant work tracked in FLINK-25318 and FLINK-32898.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/config/
# Configuration

By default, the Table & SQL API is preconfigured for producing accurate results with acceptable performance. Depending on requirements, it might be necessary to adjust certain parameters for optimization (e.g. unbounded streaming programs may need to ensure required state size is capped).

### Overview

When instantiating a TableEnvironment, EnvironmentSettings can be used to pass the desired configuration for the current session, by passing a Configuration object to the EnvironmentSettings. Additionally, in every table environment, the TableConfig offers options for configuring the current session. For common or important configuration options, the TableConfig provides getters and setters methods with detailed inline documentation. For more advanced configuration, users can directly access the underlying key-value map.

Attention: Because options are read at different point in time when performing operations, it is recommended to set configuration options early after instantiating a table environment.

Note: All configuration options can also be set globally in Flink configuration file and can be later overridden in the application, through EnvironmentSettings, before instantiating the TableEnvironment, or through the TableConfig of the TableEnvironment.

### Execution Options — table of 62 rows (tune performance of query execution)
### Optimizer Options — table of 29 rows (adjust behavior of query optimizer)
### Table Options — table of 17 rows (adjust behavior of the table planner)
### Materialized Table Options — table of 6 rows (adjust behavior of the materialized table)
### SQL Client Options — table of 7 rows (adjust behavior of the sql client)

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/tuning/
# Performance Tuning

SQL is the most widely used language for data analytics. Flink's Table API and SQL enables users to define efficient stream analytics applications in less time and effort. Flink Table API and SQL is effectively optimized, integrating query optimizations and tuned operator implementations. But not all optimizations are enabled by default.

Note: The streaming aggregation optimizations mentioned here are all supported for Group Aggregations and Window TVF Aggregations (except Session Window TVF Aggregation) now.

## MiniBatch Aggregation

By default, group aggregation operators process input records one by one: (1) read accumulator from state, (2) accumulate/retract record to the accumulator, (3) write accumulator back to state, (4) repeat. This may increase overhead of StateBackend (especially RocksDB). Data skew worsens the problem and causes backpressure.

The core idea of mini-batch aggregation is caching a bundle of inputs in a buffer inside of the aggregation operator. When the bundle is triggered, only one operation per key to access state is needed. This significantly reduces state overhead and gets better throughput, but may increase some latency. Trade-off between throughput and latency. Enable via table.exec.mini-batch.enabled, table.exec.mini-batch.allow-latency, table.exec.mini-batch.size.

Note: MiniBatch optimization is always enabled for Window TVF Aggregation, regardless of the above configuration. Window TVF aggregation buffers records in managed memory instead of JVM Heap, so no risk of GC/OOM.

## Local-Global Aggregation

Local-Global solves data skew by dividing a group aggregation into two stages: local aggregation in upstream, then global aggregation in downstream (similar to Combine + Reduce in MapReduce). Local aggregation accumulates a certain amount of inputs with the same key into a single accumulator; global aggregation receives reduced accumulators instead of raw inputs. This significantly reduces network shuffle and state access cost. The number of inputs accumulated by local aggregation every time is based on mini-batch interval, so local-global depends on mini-batch optimization being enabled.

## Split Distinct Aggregation

Local-Global is effective for general aggregation (SUM, COUNT, MAX, MIN, AVG) but not satisfactory for distinct aggregation. COUNT DISTINCT is not good at reducing records if the distinct key is sparse; even with local-global, the accumulator still contains almost all raw records and global aggregation is the bottleneck.

The idea: split distinct aggregation (e.g. COUNT(DISTINCT col)) into two levels. First aggregation shuffled by group key + additional bucket key (HASH_CODE(distinct_key) % BUCKET_NUM, BUCKET_NUM=1024 by default, configurable via table.optimizer.distinct-agg.split.bucket-num). Second aggregation shuffled by original group key, uses SUM to aggregate COUNT DISTINCT values from different buckets. The bucket key plays the role of an additional group key to share the burden of hotspot. Flink supports splitting more complex queries (e.g. COUNT(DISTINCT a), SUM(DISTINCT b), with other non-distinct aggregates).

Note: Currently, the split optimization doesn't support aggregations which contains user defined AggregateFunction.

## Use FILTER Modifier on Distinct Aggregates

When calculating UV from different dimensions, use FILTER syntax instead of CASE WHEN — FILTER is more SQL-standard-compliant and gets more performance improvement. Flink SQL optimizer can recognize different filter arguments on the same distinct key and use just one shared state instance instead of multiple, reducing state access and state size.

## MiniBatch Regular Joins

By default, regular join operator processes input records one by one: (1) lookup associated records from state of counterpart based on join key, (2) update state, (3) output join results. This may increase StateBackend overhead and lead to severe record amplification, especially in cascading join scenarios.

MiniBatch join caches a bundle of inputs in a buffer; once the buffer reaches a specified size or time threshold, records are forwarded to the join process. Two core optimizations: (1) fold records in the buffer to reduce the number of data before join process, (2) try best to suppress outputting redundant results. MiniBatch optimization is disabled by default for regular join. Enable via table.exec.mini-batch.enabled/allow-latency/size.

## Multiple Regular Joins (MultiJoin Operator)

Streaming Flink jobs with multiple non-temporal regular joins often experience operational instability and performance degradation due to large state sizes — the intermediate state created by a chain of joins is much larger than the input state itself. In Flink 2.1, a new multi-join operator significantly reduces state size and improves performance for join pipelines involving record amplification and large intermediate state. It eliminates the need to store intermediate state for joins across multiple tables by processing joins across various input streams simultaneously ("zero intermediate state"). This exchanges a reduction in storage for an increase in computational effort.

The efficiency of the MultiJoin operator largely depends on the size of intermediate state and the selectivity of the common join key(s). In a scenario with record amplification, the MultiJoin operator is more efficient (smaller state, more stable operator, linear state growth instead of polynomial). If a chain of joins produces less state than the original records, binary joins might perform better.

### The MultiJoin Operator benefits
- Considerably smaller state size due to zero intermediate state.
- Improved performance for chained joins with record amplification.
- Improved stability: linear state growth with amount of records processed, instead of polynomial growth with binary joins.
- Pipelines usually have faster initialization and recovery times due to smaller state and fewer nodes.

### When to enable the MultiJoin?

If your job has multiple joins that share at least one common join key, and the intermediate state in the intermediate joins is larger than the input sources, consider enabling. Recommended use cases:
- The common join key(s) have a high selectivity (small number of records per key)
- Statement with several chained joins and considerable intermediate state
- No considerable data skew on the common join key(s)
- Joins are generating large state (state 50+ GB)

If common join key(s) exhibit low selectivity, the MultiJoin operator's required recomputation of intermediate state can severely impact performance; binary joins are recommended (partition data using all join keys).

### How to enable the MultiJoin?

Globally: SET 'table.optimizer.multi-join.enabled' = 'true';
Per query block via hint: SELECT /*+ MULTI_JOIN(t1, t2, t3) */ * FROM t1 ... — the hint approach allows selective application. The configuration setting takes precedence over the hint.

Important: Currently experimental. Supports only streaming INNER/LEFT joins. Due to records partitioning, need at least one key shared between the join conditions:
- Supported: A JOIN B ON A.key = B.key JOIN C ON A.key = C.key (Partition by key)
- Supported: A JOIN B ON A.key = B.key JOIN C ON B.key = C.key (Partition by key via transitivity)
- Not supported: A JOIN B ON A.key1 = B.key1 JOIN C ON B.key2 = C.key2 (No single key allows partitioning A, B, and C together in a single operator. This will be split into multiple MultiJoin operators)

### MultiJoin Operator Example - Benchmark

10-way benchmark between default binary joins and MultiJoin operator. For a 10-way join with record amplification:
- Performance: 2x to over 100x+ increase in processed records when both at 100% busyness.
- State Size: 3x to over 1000x+ smaller as intermediate state grows.

The total state is always smaller with the MultiJoin operator. Benchmark config: 1 record per tenant_id (high selectivity), 10 upsert kafka topics, 10 parallelism, 1 record per second per topic. RocksDB with unaligned + incremental checkpoints. Each job: one TaskManager (8GB process memory, 1GB off-heap, 20% network memory), JobManager 4GB. M1 processor, 32GB RAM, 1TB SSD. Blackhole sink.

## Delta Joins

In streaming jobs, regular joins keep all historical data from both inputs to ensure accuracy. Over time, state grows continuously, increasing resource usage and impacting stability.

Flink introduces the delta join operator. The key idea: replace the large state maintained by regular joins with a bidirectional lookup-based join that directly reuses data from the source tables. Delta joins substantially reduce state size, enhance job stability, and lower resource consumption.

This feature is enabled by default. A regular join will be automatically optimized into a delta join when all the following conditions are met:
- The sql pattern satisfies the optimization criteria.
- The external storage system of the source table provides index information for fast querying for delta joins. Currently, Apache Fluss (Incubating) has provided index information at the table level for Flink, allowing such tables to be used as source tables for delta joins.

### Working Principle

Regular joins store all incoming records from both input sides in state. Delta joins leverage the indexing capabilities of external storage systems — instead of state lookups, delta joins issue efficient index-based queries directly against the external storage to retrieve matching records. This eliminates redundant data storage between the Flink state and the external system.

### Important Configurations

Delta join optimization is enabled by default. Disable: SET 'table.optimizer.delta-join.strategy' = 'NONE';
Fine-tune: table.exec.delta-join.cache-enabled, table.exec.delta-join.left.cache-size, table.exec.delta-join.right.cache-size.

### Supported Features and Limitations

Supported:
- INSERT-only tables as source tables.
- CDC tables without DELETE operations as source tables.
- projection and filter operations between the source and the delta join.
- caching within the delta join operator.
- cascaded delta joins — eligible join nodes sequentially converted into delta join nodes from source to sink.
- lookup join after a delta join.
- non-deterministic functions in projections and filters between the delta join and downstream operators.

Limitations (jobs containing any cannot be optimized into a delta join):
- The index key of the table must be included in the join's equivalence conditions.
- Only INNER JOIN is currently supported.
- The downstream operator must be able to handle duplicate changes, such as a sink operating in UPSERT mode without upsertMaterialize.
- When consuming a CDC stream, the join key must be part of the upsert key.
- When consuming a CDC stream, all filters must be applied on the upsert key.
- Non-deterministic functions are not allowed in projections or filters between the source and the delta join.

Note: Flink supports defining a table-level immutable columns constraint to enrich the upsert key, enabling delta join optimization in more scenarios. The immutable columns constraint declares that certain columns, once set for a given primary key, cannot be modified. The immutable columns are combined with the primary key to form a new upsert key. Apache Fluss (Incubating) is planning to support table-level immutable columns constraint in the future.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sourcessinks/
# User-defined Sources & Sinks

Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and unbounded data in a unified fashion. Because dynamic tables are only a logical concept, Flink does not own the data itself — the content is stored in external systems or files. Dynamic sources and dynamic sinks read and write data from/to an external system. In the documentation, sources and sinks are often summarized under the term connector.

Since Flink v1.16, TableEnvironment introduces a user class loader to have consistent class loading behavior in table programs, SQL Client and SQL Gateway. User-defined connectors should replace Thread.currentThread().getContextClassLoader() with the user class loader to load classes. The user class loader can be accessed via DynamicTableFactory.Context.

## Overview

Implementers don't always need to create a new connector from scratch; sometimes they modify existing connectors or hook into the existing stack. The general architecture of table connectors goes from pure declaration in the API to runtime code executed on the cluster.

### Metadata

Both Table API and SQL are declarative APIs, including declaration of tables. Executing a CREATE TABLE statement results in updated metadata in the target catalog. For most catalog implementations, physical data in the external system is not modified. Connector-specific dependencies don't have to be present in the classpath yet. The options declared in the WITH clause are neither validated nor otherwise interpreted. The metadata for dynamic tables is represented as instances of CatalogTable. A table name will be resolved into a CatalogTable internally when necessary.

### Planning

When planning and optimizing, a CatalogTable needs to be resolved into a DynamicTableSource (for SELECT) and DynamicTableSink (for INSERT INTO). DynamicTableSourceFactory and DynamicTableSinkFactory provide connector-specific logic for translating the metadata of a CatalogTable into instances of DynamicTableSource and DynamicTableSink. A factory's purpose is to validate options, configure encoding/decoding formats (if required), and create a parameterized instance of the table connector.

By default, instances of DynamicTableSourceFactory and DynamicTableSinkFactory are discovered using Java's Service Provider Interfaces (SPI). The connector option must correspond to a valid factory identifier.

DynamicTableSource and DynamicTableSink can also be seen as stateful factories that eventually produce concrete runtime implementation. The planner uses the source and sink instances to perform connector-specific bidirectional communication until an optimal logical plan could be found. Depending on the optionally declared ability interfaces (e.g. SupportsProjectionPushDown or SupportsOverwrite), the planner might apply changes to an instance and thus mutate the produced runtime implementation.

### Runtime

Once logical planning is complete, the planner obtains the runtime implementation. Runtime logic is implemented in Flink's core connector interfaces such as InputFormat or SourceFunction. Those interfaces are grouped by another level of abstraction as subclasses of ScanRuntimeProvider, LookupRuntimeProvider, and SinkRuntimeProvider. For example, both OutputFormatProvider and SinkFunctionProvider are concrete instances of SinkRuntimeProvider.

## Project Configuration

To implement a custom connector or format, dependency flink-table-common is usually sufficient. To develop a connector bridging with DataStream APIs, add flink-table-api-java-bridge. Ship both a thin JAR and an uber JAR; the uber JAR should include all third-party dependencies, excluding the table dependencies listed above.

Note: You should not depend on flink-table-planner_2.12 in production code. With the new module flink-table-planner-loader introduced in Flink 1.15, the application's classpath will not have direct access to org.apache.flink.table.planner classes anymore.

## Extension Points

### Dynamic Table Factories

Dynamic table factories configure a dynamic table connector for an external storage system from catalog and session information. Implement DynamicTableSourceFactory to construct a DynamicTableSource; implement DynamicTableSinkFactory to construct a DynamicTableSink. By default, the factory is discovered using the value of the connector option as the factory identifier and Java's SPI. In JAR files, references to new implementations can be added to META-INF/services/org.apache.flink.table.factories.Factory. The framework checks for a single matching factory uniquely identified by factory identifier and requested base class. The factory discovery process can be bypassed by the catalog implementation via Catalog#getFactory.

### Dynamic Table Source

When reading a dynamic table, the content can be considered as:
- A changelog (finite or infinite) for which all changes are consumed continuously — ScanTableSource.
- A continuously changing or very large external table queried for individual values when necessary — LookupTableSource.
- A table that supports searching via vector — VectorSearchTableSource.

A class can implement all of these interfaces at the same time. The planner decides about their usage depending on the specified query.

#### Scan Table Source

A ScanTableSource scans all rows from an external storage system during runtime. Scanned rows can contain insertions, updates and deletions — so the source can read a (finite or infinite) changelog. The returned changelog mode indicates the set of changes the planner can expect during runtime. For regular batch: bounded stream of insert-only rows. For regular streaming: unbounded stream of insert-only rows. For CDC: bounded or unbounded streams with insert, update, and delete rows.

A table source can implement further ability interfaces (e.g. SupportsProjectionPushDown) that might mutate an instance during planning. The returned scan runtime provider provides the runtime implementation; SourceProvider is the recommended core interface. Records must be emitted as org.apache.flink.table.data.RowData. To support parallelism, the factory should support the optional scan.parallelism option and pass its value to a provider implementing ParallelismProvider.

#### Lookup Table Source

A LookupTableSource looks up rows by one or more keys during runtime. Compared to ScanTableSource, it does not have to read the entire table and can lazily fetch individual values. A LookupTableSource only supports emitting insert-only changes currently. Further abilities are not supported. The runtime implementation is a TableFunction or AsyncTableFunction, called with values for the given lookup keys during runtime.

#### Vector Search Table Source

A VectorSearchTableSource searches an external storage system using an input vector and returns the most similar top-K rows during runtime. Users can determine which algorithm to calculate similarity (Euclidean distance or Cosine distance). Compared to ScanTableSource, lazily fetches individual values; only supports emitting insert-only changes. Compared to LookupTableSource, does not use equality to determine whether a row matches. The runtime implementation is a TableFunction or AsyncTableFunction, called with the given vector values.

#### Source Abilities — table of 9 rows

Attention: The interfaces above are currently only available for ScanTableSource, not for LookupTableSource or VectorSearchTableSource.

### Dynamic Table Sink

When writing a dynamic table, the content can always be considered as a changelog (finite or infinite). The returned changelog mode indicates the set of changes the sink accepts during runtime. For regular batch: solely accept insert-only rows and write bounded streams. For regular streaming: solely accept insert-only rows, write unbounded streams. For CDC: write bounded or unbounded streams with insert, update, and delete rows.

A table sink can implement further ability interfaces (e.g. SupportsOverwrite). The returned sink runtime provider provides the runtime implementation; SinkV2Provider is the recommended core interface. Records must be accepted as org.apache.flink.table.data.RowData. To support parallelism, support the optional sink.parallelism option.

#### Sink Abilities — table of 9 rows

### Encoding / Decoding Formats

Some table connectors accept different formats that encode and decode keys and/or values. Formats work similar to DynamicTableSourceFactory -> DynamicTableSource -> ScanRuntimeProvider. Formats are discovered using Java's SPI. The dynamic table factory searches for a factory corresponding to a factory identifier and connector-specific base class. For example, the Kafka table source requires a DeserializationSchema; the Kafka table source factory uses the value.format option to discover a DeserializationFormatFactory.

Supported format factories: DeserializationFormatFactory, SerializationFormatFactory. The format factory translates options into an EncodingFormat or a DecodingFormat, which produce specialized format runtime logic for the given data type.

## Full Stack Example

Sketches how to implement a scan table source with a decoding format that supports changelog semantics. The example uses a simple single-threaded SourceFunction to open a socket that listens for incoming bytes. The raw bytes are decoded into rows by a pluggable format. The format expects a changelog flag as the first column. Components illustrated:
- SocketDynamicTableFactory — translates the catalog table to a table source; discovers the format using FactoryUtil.
- ChangelogCsvFormatFactory — translates format-specific options to a format; implements DeserializationFormatFactory (could also be used for other connectors that support deserialization formats such as Kafka).
- SocketDynamicTableSource — used during planning; main logic in getScanRuntimeProvider(...) instantiating SourceFunction and DeserializationSchema parameterized to return internal data structures (RowData).
- ChangelogCsvFormat — a decoding format using a DeserializationSchema during runtime; supports emitting INSERT and DELETE changes.
- ChangelogCsvDeserializer — parsing logic converting bytes into Row of Integer and String with a row kind; final conversion step converts into internal data structures.
- SocketSourceFunction — opens a socket and consumes bytes; splits records by the given byte delimiter (\n by default); delegates decoding to a pluggable DeserializationSchema; only works with parallelism of 1.

## (redirect/stub pages — no article body)

- Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/catalogs/ — no article (redirect; see sql/catalogs)
- Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/types/ — no article (redirect; see sql/data-types "Data Types")
- Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/jdbcdriver/ — no article (redirect; see sql/interfaces/jdbc-driver)
- Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sqlclient/ — no article (redirect; see sql/interfaces/sqlclient)
- Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/timezone/ — no article (redirect; see sql/time-zone or concepts/time)
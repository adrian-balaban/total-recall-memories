---
title: Apache Flink 2.3 docs — Concepts
tags: [org, flink, flink-2.3, docs, concepts, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:17:07.453Z'
updated: '2026-07-07T19:17:07.453Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — concepts

Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/ on 2026-07-07. Prose and headings are kept; code blocks are condensed to inline first-line summaries.

---

## Concepts
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/overview/

# Concepts
The Hands-on Training explains the basic concepts of stateful and timely stream processing that underlie Flink's APIs. Stateful stream processing is introduced in the context of Data Pipelines & ETL and further developed in Fault Tolerance. Timely stream processing is introduced in Streaming Analytics. This Concepts in Depth section provides a deeper understanding of how Flink's architecture and runtime implement these concepts.

## Flink's APIs
Flink offers different levels of abstraction for developing streaming/batch applications. The lowest level abstraction offers stateful and timely stream processing, embedded into the DataStream API via the Process Function. It allows users to freely process events from one or more streams, and provides consistent, fault tolerant state. Users can register event time and processing time callbacks.
In practice many applications use the Core APIs: the DataStream API (bounded/unbounded streams) — fluent APIs with transformations, joins, aggregations, windows, state, etc. Data types are represented as classes. The low-level Process Function integrates with the DataStream API.
The Table API is a declarative DSL centered around tables (dynamically changing when representing streams). It follows the (extended) relational model: tables have a schema, operations like select, project, join, group-by, aggregate. Table API programs declaratively define WHAT logical operation should be done rather than HOW. Less expressive than Core APIs but more concise; goes through an optimizer.
One can seamlessly convert between tables and DataStream, mixing Table API with DataStream API.
The highest level abstraction is SQL — similar to Table API in semantics/expressiveness but represented as SQL query expressions; closely interacts with Table API.

---

## Stateful Stream Processing
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/stateful-stream-processing/

# Stateful Stream Processing

## What is State?
Many operations look at one event at a time (event parser); some remember information across multiple events (window operators) — these are stateful. Examples: pattern search stores sequence of events encountered; per-minute/hour/day aggregation holds pending aggregates; ML model training holds current model parameters; historic data management allows efficient access to past events. Flink needs to be aware of state to make it fault tolerant via checkpoints and savepoints. Knowledge of state allows rescaling (redistributing state across parallel instances). Flink provides different state backends.

## Keyed State
Keyed state is maintained in an embedded key/value store. State is partitioned and distributed together with the streams read by stateful operators. Access to key/value state is only possible on keyed streams (after a keyed/partitioned data exchange), restricted to values of the current event's key. Aligning keys of streams and state makes all state updates local operations, guaranteeing consistency without transaction overhead. Flink can redistribute state and adjust stream partitioning transparently.
Keyed State is organized into Key Groups — the atomic unit by which Flink can redistribute Keyed State; there are exactly as many Key Groups as the defined maximum parallelism. Each parallel instance of a keyed operator works with keys for one or more Key Groups.

## State Persistence
Flink implements fault tolerance using stream replay and checkpointing. A checkpoint marks a specific point in each input stream along with corresponding state for each operator. A streaming dataflow can be resumed from a checkpoint while maintaining consistency (exactly-once processing semantics) by restoring operator state and replaying records from the checkpoint point.
The checkpoint interval trades off fault tolerance overhead with recovery time (number of records to replay). Snapshots are very light-weight for small state and can be drawn frequently. State is stored at a configurable place, usually a distributed file system.
On failure, Flink stops the dataflow, restarts operators, resets them to the latest successful checkpoint, and resets input streams to the snapshot point. Records processed as part of the restarted dataflow are guaranteed to not have affected the previously checkpointed state.
By default checkpointing is disabled. The data stream source must be able to rewind the stream to a defined recent point (Apache Kafka has this ability). Because checkpoints are realized through distributed snapshots, "snapshot" and "checkpoint" are used interchangeably; "snapshot" can mean either checkpoint or savepoint.

### Checkpointing
The central part of Flink's fault tolerance is drawing consistent snapshots of the distributed data stream and operator state, described in "Lightweight Asynchronous Snapshots for Distributed Dataflows", inspired by the Chandy-Lamport algorithm. Everything to do with checkpointing can be done asynchronously; barriers don't travel in lock step and operations can asynchronously snapshot state. Since Flink 1.11 checkpoints can be taken with or without alignment. Aligned checkpoints described first.

#### Barriers
A core element are stream barriers injected into the data stream, flowing with records strictly in line (never overtake records). A barrier separates records into the set going into the current snapshot and the next. Each barrier carries the snapshot ID. Barriers don't interrupt the stream flow and are lightweight; multiple barriers from different snapshots can be in the stream concurrently.
Barriers are injected at the stream sources. The point where barriers for snapshot n are injected (S_n) is the position in the source stream the snapshot covers (e.g., last record's offset in a Kafka partition). S_n is reported to the checkpoint coordinator (JobManager).
Barriers flow downstream. When an intermediate operator has received barrier n from all input streams, it emits barrier n into all outgoing streams. Once a sink has received barrier n from all inputs, it acknowledges snapshot n. After all sinks acknowledge, the snapshot is completed; the job never asks the source for records from before S_n.
Operators receiving more than one input stream must align input streams on the barriers: once snapshot barrier n is received from an incoming stream, no further records from that stream are processed until barrier n arrives from other inputs (otherwise records from snapshot n and n+1 would mix). Once the last stream receives barrier n, the operator emits pending outgoing records, then emits barrier n, snapshots state, resumes processing, and writes state asynchronously to the state backend. Alignment is needed for all operators with multiple inputs and for operators after a shuffle.

#### Snapshotting Operator State
Operators snapshot state when they have received all snapshot barriers from inputs and before emitting barriers to outputs — at that point all updates from records before the barriers are made, and no updates from records after. State is stored in a configurable state backend (default JobManager memory; production should use HDFS). After state is stored, the operator acknowledges the checkpoint, emits the barrier, and proceeds. The snapshot contains: for each parallel stream source, the offset/position when the snapshot started; for each operator, a pointer to the stored state.

#### Recovery
On failure, Flink selects the latest completed checkpoint k, re-deploys the dataflow, gives each operator the state from checkpoint k, and sets sources to start reading from position S_k (e.g., Kafka offset). If state was snapshotted incrementally, operators start with the latest full snapshot and apply incremental updates.

### Unaligned Checkpointing
Checkpoints can be performed unaligned: checkpoints can overtake all in-flight data as long as the in-flight data becomes part of operator state. Closer to the Chandy-Lamport algorithm, but Flink still inserts the barrier in the sources to avoid overloading the checkpoint coordinator.
The operator reacts on the first barrier in its input buffers, immediately forwards it to downstream by adding it to the end of output buffers, marks all overtaken records to be stored asynchronously, and creates a snapshot of its own state. The operator only briefly stops processing to mark buffers, forward the barrier, and snapshot. Unaligned checkpointing ensures barriers arrive at the sink as fast as possible — suited for applications with at least one slow moving data path where alignment times can reach hours. Doesn't help when I/O to state backends is the bottleneck. Savepoints are always aligned.

#### Unaligned Recovery
Operators first recover in-flight data before starting processing any data from upstream operators; otherwise same steps as aligned recovery.

### State Backends
The data structures in which key/values indexes are stored depend on the chosen state backend. One stores data in an in-memory hash map; another uses RocksDB. State backends also implement the logic to take a point-in-time snapshot and store it as part of a checkpoint. State backends can be configured without changing application logic.

### Savepoints
All programs using checkpointing can resume execution from a savepoint. Savepoints allow updating programs and Flink cluster without losing state. They are manually triggered checkpoints that take a snapshot and write it to a state backend, relying on the regular checkpointing mechanism. Savepoints are triggered by the user and don't automatically expire when newer checkpoints complete.

### Exactly Once vs. At Least Once
The alignment step may add latency (usually few milliseconds, but outliers can increase). For consistently super low latencies, Flink has a switch to skip stream alignment during a checkpoint; snapshots are still drawn once an operator has seen the barrier from each input. When alignment is skipped, the operator keeps processing all inputs, so it processes elements belonging to checkpoint n+1 before the snapshot for n is taken; on restore these records occur as duplicates. Alignment happens only for operators with multiple predecessors (joins) and operators with multiple senders (after a repartitioning/shuffle). Embarrassingly parallel streaming operations (map, flatMap, filter, …) give exactly-once guarantees even in at-least-once mode.

## State and Fault Tolerance in Batch Programs
Flink executes batch programs as a special case of streaming programs in BATCH ExecutionMode (bounded streams). Concepts apply the same way with minor exceptions: batch fault tolerance does not use checkpointing — recovery happens by fully replaying streams (possible because inputs are bounded), pushing cost towards recovery but making regular processing cheaper; batch state backends use simplified in-memory/out-of-core data structures rather than key/value indexes.

---

## Timely Stream Processing
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/time/

# Timely Stream Processing

## Introduction
Timely stream processing extends stateful stream processing where time plays a role: time series analysis, time-period aggregations (windows), event processing where event-occurrence time matters.

## Notions of Time: Event Time and Processing Time
Processing time: the system time of the machine executing the operation. Time-based operations use the system clock. An hourly processing time window includes records that arrived at a specific operator between full hours. Simplest notion; best performance and lowest latency; but no determinism in distributed/asynchronous environments — susceptible to record arrival speed, flow speed between operators, outages.
Event time: the time each event occurred on its producing device, embedded within records before entering Flink (event timestamp extracted from each record). Progress depends on data, not wall clocks. Event time programs must specify how to generate Event Time Watermarks (the mechanism signaling progress). In a perfect world, event time yields completely consistent and deterministic results regardless of arrival order; unless events arrive in-order, event time incurs latency waiting for out-of-order events, limiting determinism. Assuming all data has arrived, event time operations produce correct, consistent results even with out-of-order/late events or reprocessing historic data. An hourly event time window contains all records with event timestamps falling into that hour, regardless of arrival order. Sometimes event time programs processing live data use processing time operations to guarantee timely progress.

## Event Time and Watermarks
Flink implements techniques from the Dataflow Model. A stream processor supporting event time needs a way to measure progress of event time (e.g., a window operator needs to know when event time passed the end of an hour). Event time can progress independently of processing time. The mechanism to measure progress is watermarks — they flow as part of the data stream and carry a timestamp t. Watermark(t) declares event time has reached t, meaning there should be no more elements with timestamp t' <= t. For in-order streams, watermarks are simply periodic markers. Watermarks are crucial for out-of-order streams: a watermark is a declaration that by that point all events up to a certain timestamp should have arrived. Once a watermark reaches an operator, the operator advances its internal event time clock to the watermark value. Event time is inherited by a freshly created stream element from the event or watermark that produced it.

### Watermarks in Parallel Streams
Watermarks are generated at or directly after source functions; each parallel subtask usually generates watermarks independently. As watermarks flow, they advance event time at operators; whenever an operator advances its event time, it generates a new watermark downstream. Operators consuming multiple input streams (union, after keyBy/partition) have current event time = minimum of input streams' event times.

## Lateness
Some elements violate the watermark condition: even after Watermark(t), more elements with timestamp t' <= t occur. In real-world setups elements can be arbitrarily delayed, making it impossible to specify a time by which all elements of a certain timestamp will have occurred. Streaming programs may explicitly expect some late elements (elements arriving after the system's event time clock passed the element's timestamp). See Allowed Lateness.

## Windowing
Aggregating events on streams works differently than batch: streams are infinite, so aggregates are scoped by windows ("count over the last 5 minutes", "sum of the last 100 elements"). Windows can be time driven (every 30 seconds) or data driven (every 100 elements). Types: tumbling windows (no overlap), sliding windows (overlap), session windows (punctuated by a gap of inactivity).

---

## Flink Architecture
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/

# Flink Architecture
Flink is a distributed system requiring effective allocation/management of compute resources. It integrates with Hadoop YARN and Kubernetes, but can run standalone or as a library.

## Anatomy of a Flink Cluster
The runtime has two types of processes: a JobManager and one or more TaskManagers. The Client is not part of the runtime — it prepares and sends a dataflow to the JobManager, then can disconnect (detached mode) or stay connected (attached mode). The client runs as part of the Java program or via `./bin/flink run ...`.

### JobManager
Responsibilities: deciding when to schedule tasks, reacting to finished/failed tasks, coordinating checkpoints, coordinating recovery. Three components:
- ResourceManager: resource de-/allocation and provisioning; manages task slots (the unit of resource scheduling). Multiple ResourceManagers for YARN, Kubernetes, standalone. In standalone, can only distribute slots of available TaskManagers.
- Dispatcher: provides a REST interface to submit applications and starts a new JobMaster for each submitted job; runs the Flink WebUI.
- JobMaster: responsible for managing execution of a single JobGraph; multiple jobs can run simultaneously, each with its own JobMaster.
At least one JobManager; HA setups may have multiple, one leader, others standby.

### TaskManagers
Execute the tasks of a dataflow, buffer and exchange data streams. At least one required. The smallest unit of resource scheduling is a task slot; the number of task slots indicates concurrent processing tasks. Multiple operators may execute in a task slot.

## Tasks and Operator Chains
Flink chains operator subtasks together into tasks, each executed by one thread. Chaining reduces overhead of thread-to-thread handover and buffering, increases throughput, decreases latency. Chaining behavior is configurable.

## Task Slots and Resources
Each TaskManager is a JVM process and may execute one or more subtasks in separate threads. Task slots (at least one) control how many tasks a TaskManager accepts. Each slot represents a fixed subset of TaskManager resources (e.g., 3 slots → 1/3 managed memory each). Slotting means a subtask won't compete with subtasks from other jobs for managed memory. No CPU isolation; slots only separate managed memory. By default Flink allows subtasks to share slots even from different tasks of the same job — one slot may hold an entire pipeline. Benefits: a cluster needs as many task slots as the highest parallelism in the job; better resource utilization (non-intensive source/map subtasks share with intensive window subtasks).

## Flink Application Execution
A Flink Application is any user program that spawns one or multiple Flink jobs from main(). Execution can be local (LocalEnvironment) or remote (RemoteEnvironment). Jobs can be submitted to a Flink Session Cluster, a Flink Job Cluster (deprecated), or a Flink Application Cluster.

### Flink Application Cluster
Cluster Lifecycle: a dedicated cluster that only executes jobs from one Flink Application, where main() runs on the cluster rather than the client. One-step submission: package application logic and dependencies into a job JAR; the cluster entrypoint calls main() to extract the JobGraph. Lifetime bound to the application. Resource Isolation: ResourceManager and Dispatcher scoped to a single application — better separation than Session Cluster.

### Flink Session Cluster
Cluster Lifecycle: client connects to a pre-existing, long-running cluster accepting multiple submissions; the cluster keeps running until manually stopped; lifetime not bound to any application/job. Resource Isolation: slots allocated by ResourceManager on job submission and released when finished; all jobs share the cluster (competition for resources). If one TaskManager crashes, all jobs with tasks on it fail; a fatal JobManager error affects all jobs. Other: a pre-existing cluster saves time applying for resources/starting TaskManagers — important for short jobs / interactive analysis.

---

## Streaming Concepts
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/overview/

# Streaming Concepts
Flink's Table API and SQL support are unified APIs for batch and stream processing — same semantics regardless of bounded batch or unbounded stream input.

## State Management
Table programs in streaming mode leverage all capabilities of Flink as a stateful stream processor. A table program can be configured with a state backend and checkpointing options. It is possible to take a savepoint and restore.

### State Usage
Due to declarative nature, it is not always obvious where/how much state is used. The planner decides whether state is necessary. Source tables are never kept entirely in state; an implementer deals with logical (dynamic) tables whose state requirements depend on operations.

#### Stateful Operators
Joins, aggregations, deduplication require keeping intermediate results in fault-tolerant storage. A regular SQL join of two tables requires keeping both input tables in state entirely (a matching could occur at any time). Flink provides optimized window and interval joins exploiting watermarks to keep state small. Example: word count query — `word` is the grouping key; the framework maintains a count for each observed word; total state grows continuously. Queries like SELECT...FROM...WHERE (only projections/filters) are usually stateless, but stateful operations can be implicit (e.g., input is a changelog without UPDATE_BEFORE, or via table-exec-source-cdc-events-duplicate). Example: SELECT FROM an upsert kafka source — the source only provides INSERT/UPDATE_AFTER/DELETE, the downstream sink requires a complete changelog (incl. UPDATE_BEFORE), so the planner generates a "ChangelogNormalize" stateful operator.

#### Idle State Retention Time
The parameter table.exec.state.ttl defines how long a key's state is retained without update before removal. By removing a key's state, the continuous query forgets it has seen the key; a record with a removed key is treated as the first record (count starts at 0).

#### Different Ways to Configure State TTL
[table: 4 rows — pipeline-level via config, operator-level via compiled plan, etc.]

#### Configure Operator-level State TTL
Advanced feature. Only suitable when multiple states exist and you need different TTLs per state. Granularity = number of incoming input edges: OneInputStreamOperator configures one TTL; TwoInputStreamOperator (e.g., regular join) configures TTL for left and right states separately; MultipleInputStreamOperator with K inputs → K state TTLs. Use cases: different TTLs for regular join left/right states; different TTLs for different transformations in one pipeline (e.g., ROW_NUMBER deduplication then GROUP BY aggregation → two OneInputStreamOperators). Window-based operations (Window Join, Window Aggregation, Window Top-N) and Interval Joins do not rely on table.exec.state.ttl and cannot be configured at operator-level.
Generate a Compiled Plan: use COMPILE PLAN statement (does not support SELECT...FROM... queries). Produces a JSON file at /path/to/plan.json; supports remote filesystems (hdfs://, s3://) with write access configured.
Modify the Compiled Plan: each stateful operator generates a JSON array "state"; a k-th input stream operator has k-th state. Change the TTL to a positive integer with time unit "ms" (e.g., 3000 ms = 1 hour). TTL of downstream stateful operator should be >= TTL of upstream.
Execute the Compiled Plan: EXECUTE PLAN deserializes the file back to an execution plan and submits the job; the job applies state TTL from the file instead of table.exec.state.ttl.
A Full Example: enriched order shipment info via regular inner join with different state TTL for left (3000 ms) and right (9000 ms) side.

### Stateful Upgrades and Evolution
Table programs in streaming mode are standing queries — defined once, continuously evaluated. Changes to query or planner may lead to a different execution plan, making stateful upgrades challenging. The query implementer must ensure optimized plans before/after change are compatible (use EXPLAIN or table.explain()). New optimizer rules may make upgrades to newer Flink versions incompatible. Savepoints are only supported if both the query and Flink version remain constant. Patch-version upgrades should be safe; major-minor upgrades are not supported. Recommend "warming up" updated table programs with historical data before switching to real-time (community working on a hybrid source).

## Where to go next?
Dynamic Tables, Time attributes, Versioned Tables, Joins in Continuous Queries, Determinism in Continuous Queries, Query configuration.

---

## Determinism In Continuous Queries
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/determinism/

# Determinism In Continuous Queries
Covers: what is determinism, is all batch processing deterministic, two examples of batch queries with non-deterministic results, non-determinism in batch, determinism in streaming, non-determinism in streaming, non-deterministic update in streaming, how to eliminate the impact of NDU.

## 1. What Is Determinism?
'an operation is deterministic if that operation assuredly computes identical results when repeated with identical input values' (SQL standard).

## 2. Is All Batch Processing Deterministic?
In a classic batch scenario, the same query on a bounded data set yields consistent results. In practice, the same query does not always return consistent results on batch.

### 2.1 Two Examples Of Batch Queries With Non-Deterministic Results
Query 1: time filter "last 2 minutes logs" — returns different results depending on submission time. Query 2: `SELECT UUID() AS uuid, * FROM clicks LIMIT 3` — generates a different uuid per row each execution.

### 2.2 Non-Determinism In Batch Processing
Caused by non-deterministic functions (CURRENT_TIMESTAMP, UUID()). Flink inherits function definitions from Apache Calcite: besides deterministic functions, there are non-deterministic functions (executed at runtime, per record) and dynamic functions (values determined when the query plan is generated, not at runtime; different values at different times but same values at the same execution).

## 3. Determinism In Streaming Processing
Core difference: unboundedness. Flink SQL abstracts streaming as continuous query on dynamic tables. The dynamic function in batch is equivalent to a non-deterministic function in streaming (every change in the base table triggers query execution). Example: CURRENT_TIMESTAMP changes over time in stream mode.

### 3.1 Non-Determinism In Streaming
Factors: non-deterministic back read of source connector, query based on Processing Time, clear internal state data based on TTL.
- Non-Deterministic Back Read Of Source Connector: determinism is limited to computation; a source connector that cannot provide deterministic back read brings non-determinism (inconsistent data for multiple reads from the same offset, or requests for data beyond the configured Kafka topic ttl).
- Query Based On Processing Time: processing time does not provide determinism. Operations relying on time attributes: Window Aggregation, Interval Join, Temporal Join, Lookup Join (non-determinism when the accessed external table changes over time).
- Clear Internal State Data Based On TTL: setting state TTL to clean up internal state in Regular Join and Group Aggregation may make computation results non-deterministic. Impact varies: some produce non-deterministic results; some cause incorrect results or runtime errors (the main reason is 'non-deterministic update').

### 3.2 Non-Deterministic Update In Streaming
Flink SQL implements an incremental update mechanism based on 'continuous query on dynamic tables'. All operations generating incremental messages maintain complete internal state; the pipeline relies on correct delivery of update messages between operators, which can be broken by non-determinism.
Update messages (changelog): Insert (I), Delete (D), Update_Before (UB), Update_After (UA). No NDU in insert-only pipelines. When there is an update message, the update key (primary key of the changelog) is deduced from the query:
- when deducible, operators maintain internal state by update key;
- when not deducible (primary key not defined in CDC source/sink, or operations can't be deduced from query semantics), operators process update (D/UB/UA) messages through complete rows; sink nodes work in retract mode; deletes by complete rows.
In update-by-row mode, update messages to state-maintaining operators cannot be interfered by non-deterministic column values, otherwise NDU. Three main sources of NDU on pipelines with update messages and no derivable update key: non-deterministic functions (scalar/table/aggregate, builtin/custom); LookupJoin on an evolving source; CDC source carrying metadata fields (system columns). TTL-based cleaning exceptions handled separately (FLINK-24666).

### 3.3 How To Eliminate The Impact Of Non-Deterministic Update In Streaming
Since 1.16, Flink SQL (FLINK-27849) introduces experimental NDU handling mechanism 'table.optimizer.non-deterministic-update.strategy'. When TRY_RESOLVE mode is enabled, it checks for NDU problems and tries to eliminate NDU from Lookup Join (adds internal materialization); if unresolvable factors remain, Flink SQL gives detailed error messages to prompt SQL adjustment.
Best Practices: enable TRY_RESOLVE before running; when an unsolvable NDU exists, modify SQL per the error prompt (e.g., remove now() or use a deterministic time field). When using Lookup Join, declare the primary key on the lookup source table (prevents Flink from deriving update keys, saving materialization cost). When the lookup source table accessed by Lookup Join is static, TRY_RESOLVE may not be needed — first turn on TRY_RESOLVE to check no other NDU problems, then restore IGNORE mode to avoid unnecessary materialization (must ensure the lookup source table is purely static).

---

## Dynamic Tables
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/dynamic_tables/

# Dynamic Tables
SQL and Table API offer flexible capabilities for real-time data processing. This page describes how relational concepts translate to streaming.

## Relational Queries on Data Streams
Comparison of relational algebra vs stream processing for input data, execution, output results. [table: 4 rows]
Relational queries and SQL provide a powerful toolset. Advanced RDBMS offer Materialized Views — a SQL query caching its result so it doesn't need re-evaluation. A materialized view becomes obsolete when base tables are modified. Eager View Maintenance updates the view as soon as base tables update. Connection to streaming: a database table results from a stream of INSERT/UPDATE/DELETE DML (changelog stream); a materialized view is a SQL query continuously processing base relations' changelog streams; the materialized view is the result of the streaming SQL query.

## Dynamic Tables & Continuous Queries
Dynamic tables are the core concept of Flink's Table API/SQL for streaming. They change over time; queries yield a Continuous Query that never terminates and produces a dynamic result table. A continuous query on a dynamic table is similar to a query defining a materialized view. The output is always semantically equivalent to the batch query on a snapshot of input tables. Relationship: stream → dynamic table → continuous query → dynamic table → stream. Dynamic tables are a logical concept, not necessarily fully materialized.

## Defining a Table on a Stream
Each record of the stream is interpreted as an INSERT modification on the resulting table (INSERT-only changelog stream). The table continuously grows; internally not materialized.

### Continuous Queries
A continuous query never terminates and updates its result table according to input updates; at any point semantically equivalent to the batch query on a snapshot. Example 1: GROUP-BY COUNT on `user` — updates/inserts result rows. Example 2: GROUP-BY COUNT on user + hourly tumbling window — appends result rows per window.

### Update and Append Queries
First query updates previously emitted results (changelog has INSERT and UPDATE); second only appends (INSERT only). Implications: update queries maintain more state; conversion of append-only vs updated table to stream differs.

### Query Restrictions
Some queries are too expensive to evaluate as continuous queries.
- State Size: queries updating previously emitted results must maintain all emitted rows; state may grow over time and cause failure (e.g., per-user count with unique non-registered user names).
- Computing Updates: some queries recompute a large fraction of emitted rows on a single input record (e.g., RANK() over lastAction — all lower-ranked rows need updating on each new row).

## Table to Stream Conversion
Three ways to encode changes of a dynamic table:
- Append-only stream: a table only modified by INSERT changes — emit inserted rows.
- Retract stream: add messages and retract messages; INSERT → add, DELETE → retract, UPDATE → retract (previous row) + add (new row).
- Upsert stream: upsert messages and delete messages; requires a unique key; INSERT/UPDATE → upsert, DELETE → delete. UPDATE encoded with a single message (more efficient than retract). Only append and retract streams supported when converting to DataStream.

---

## Time Attributes
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/time_attributes/

# Time Attributes
Flink can process data based on different notions of time. Processing time = machine system time (epoch time). Event time = processing based on timestamps attached to each row.

## Introduction to Time Attributes
Time attributes can be part of every table schema, defined in CREATE TABLE DDL or from a DataStream. Once defined, can be referenced as a field and used in time-based operations. As long as not modified and simply forwarded, it remains a valid time attribute. Time attributes behave like regular timestamps; when used in calculations they are materialized and act as standard timestamps. Ordinary timestamps cannot be used as/converted to time attributes.

## Event Time
Event time allows a table program to produce results based on timestamps, allowing consistent results despite out-of-order/late events, and ensures replayability. Allows unified syntax for batch and streaming (a time attribute in streaming can be a regular column in batch). To handle out-of-order events, Flink needs the timestamp for each row and regular indications of progress (watermarks). Event time attributes can be defined in CREATE table DDL or during DataStream-to-Table conversion.

### Defining in DDL
The event time attribute is defined using a WATERMARK statement — defines a watermark generation expression on an existing event time field, marking it as the event time attribute. Flink supports defining event time on TIMESTAMP and TIMESTAMP_LTZ columns.
- TIMESTAMP: when the timestamp is year-month-day-hour-minute-second without time-zone (e.g., "2020-04-15 20:13:40.564").
- TIMESTAMP_LTZ: when the timestamp is epoch time (a long value, e.g., 1618989564564).

#### Advanced watermark features
Only source connectors implementing SupportsWatermarkPushDown (e.g., kafka, pulsar) can use these. If a source doesn't implement it, the task runs but parameters don't take effect. Configurable via dynamic table options or the 'OPTIONS' hint (OPTIONS hint preferred).

##### I. Configure watermark emit strategy
Two strategies: on-periodic (emit periodically) and on-event (emit per event). Default in SQL is periodic (200ms, changeable via pipeline.auto-watermark-interval). Configure per-event emission via source table options (`scan.watermark.emit.strategy`) or OPTIONS hint.

##### II. Configure the idle-timeout of source table
If a split/partition/shard doesn't send event data for some time, the WatermarkGenerator gets no new data; the downstream watermark (min of upstream) won't change. Idle timeout marks the split as idle after the timeout; downstream ignores idle sources. Global: table.exec.source.idle-timeout (all sources). Per-table: scan.watermark.idle-timeout (preferred over the global param).

##### III. watermark alignment
Different splits/sources consume at different rates; faster consumers cause state buildup and disorder in slower ones. Watermark alignment ensures a fast split's watermark doesn't increase too fast compared to others. Three parameters: scan.watermark.alignment.group (alignment group name), scan.watermark.alignment.max-drift (max deviation), scan.watermark.alignment.update-interval (how often alignment time is calculated, default 1s). Since 1.17 connectors must implement watermark alignment of source split per FLIP-217; if not, the task errors — set pipeline.watermark-alignment.allow-unaligned-source-splits: true to disable, and alignment works only when number of splits equals source operator parallelism.

### During DataStream-to-Table Conversion
Define event time attribute with .rowtime property during schema definition. Timestamps and watermarks must already be assigned in the DataStream. Flink always derives rowtime attribute as TIMESTAMP WITHOUT TIME ZONE (no time zone notion; treats event time as UTC). Two ways: (1) append as a new column, (2) replace an existing column.

## Processing Time
Processing time allows results based on local machine time — simplest but non-deterministic. No timestamp extraction or watermark generation. Two ways to define:
- Defining in DDL: computed column using PROCTIME() (returns TIMESTAMP_LTZ).
- During DataStream-to-Table Conversion: .proctime property; must only extend the physical schema by an additional logical field (definable only at the end of the schema).

---

## Versioned Tables
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/versioned_tables/

# Versioned Tables
Flink SQL operates over dynamic tables that evolve (append-only or updating). Versioned tables are a special updating table that remembers past values for each key.

## Concept
A key's old value does not become irrelevant when it changes. Flink SQL can define versioned tables over any dynamic table with a PRIMARY KEY constraint and time attribute. A primary key = unique and non-null; on an upserting table, materialized changes for a key represent changes to a single row over time. The time attribute defines when each change occurred. Together, Flink tracks changes to a row over time and maintains the period for which each value was valid. Example: product prices change over time; querying at different times retrieves different results.

## Versioned Table Sources
Defined implicitly for tables whose sources/formats directly define changelogs (upsert Kafka source, debezium, canal). Additional requirement: CREATE table statement must contain a PRIMARY KEY and an event-time attribute.

## Versioned Table Views
Flink supports defining versioned views if the underlying query contains a unique key constraint and event-time attribute. Example: append-only currency rates table (JSON format → append-only, can't define PRIMARY KEY over currency). Flink can reinterpret via a deduplication query producing an ordered changelog stream with inferred primary key (currency) and event time (update_time). General format: `SELECT [column_list] FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY ... ORDER BY time_attr DESC) AS rownum FROM ...) WHERE rownum = 1`. Parameters: ROW_NUMBER() (unique sequential number), PARTITION BY (deduplicate key → primary key), ORDER BY time_attr DESC (must be a time attribute), WHERE rownum = 1 (required for Flink to recognize the versioned table).

---

## Temporal Table Function
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/temporal_table_function/

# Temporal Table Function
A Temporal table function provides access to the version of a temporal table at a specific point in time. Pass a time attribute to determine the version. Uses SQL syntax of table functions. Unlike a versioned table, temporal table functions can only be defined on top of append-only streams (no changelog inputs) and cannot be defined in pure SQL DDL.

## Defining a Temporal Table Function
Defined on top of append-only streams using the Table API. Registered with one or more key columns and a time attribute for versioning. Example: append-only currency rates table, register with `currency` as key and `update_time` as versioning time attribute.

## Temporal Table Function Join
Used as a standard table function. Append-only tables (left/probe side) join with a temporal table (right/build side) to retrieve the value for a key at a particular point in time. Example: convert orders in different currencies to USD.

---

## Glossary
  #
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/glossary/

# Glossary

- Checkpoint Storage: location where the State Backend stores its snapshot during a checkpoint (JobManager Java Heap or Filesystem).
- Flink Application Cluster: dedicated Flink Cluster executing Flink Jobs from one Flink Application; lifetime bound to the application.
- Flink Job Cluster: dedicated cluster executing a single Flink Job; deprecated since 1.15.
- Flink Cluster: distributed system of (typically) one JobManager and one or more TaskManagers.
- Event: a statement about a change of state of the domain modelled by the application; can be input and/or output; a special type of record.
- ExecutionGraph: see Physical Graph.
- Function: implemented by the user, encapsulates application logic; most are wrapped by an Operator.
- Instance: a specific instance of a specific type (Operator/Function) during runtime; "parallel instance" emphasizes multiple instances running in parallel.
- Flink Application: a Java Application submitting one or multiple Flink Jobs from main(); jobs submitted to a Session Cluster, Application Cluster, or Job Cluster.
- Flink Job: the runtime representation of a logical graph (dataflow graph) created by calling execute() in a Flink Application.
- JobGraph: see Logical Graph.
- Flink JobManager: orchestrator of a Flink Cluster; contains ResourceManager, Dispatcher, and one JobMaster per running job.
- Flink JobMaster: component in the JobManager supervising execution of Tasks of a single job.
- JobResultStore: persists results of globally terminated jobs to a filesystem; used to determine recovery in HA clusters.
- ApplicationResultStore: persists results of terminated applications to a filesystem; used to determine recovery in HA clusters.
- History Server: standalone service serving detailed history of completed applications/jobs via Web UI or REST API after the cluster is shut down (unlike JobResultStore/ApplicationResultStore which store minimal metadata).
- Logical Graph: directed graph where nodes are Operators and edges define input/output relationships (data streams/sets); also called dataflow graph.
- Managed State: application state registered with the framework; Flink handles persistence and rescaling.
- Operator: node of a Logical Graph performing an operation, usually executed by a Function; Sources and Sinks are special Operators.
- Operator Chain: two or more consecutive Operators without repartitioning; forward records directly without serialization/network stack.
- Partition: independent subset of the overall data stream/set; consumed by Tasks; changing partitioning is repartitioning.
- Physical Graph: result of translating a Logical Graph for distributed execution; nodes are Tasks, edges indicate input/output relationships or partitions.
- Record: constituent elements of a data set/stream; Operators/Functions receive and emit records.
- (Runtime) Execution Mode: BATCH or STREAMING.
- Flink Session Cluster: long-running cluster accepting multiple Flink Jobs; lifetime not bound to any job.
- State Backend: determines how state is stored on each TaskManager (Java Heap or embedded RocksDB).
- Sub-Task: a Task responsible for processing a partition of the data stream; emphasizes multiple parallel Tasks for the same Operator/Operator Chain.
- Table Program: generic term for pipelines declared with Table API or SQL.
- Task: node of a Physical Graph; basic unit of work executed by the runtime; encapsulates one parallel instance of an Operator/Operator Chain.
- Flink TaskManager: worker processes; Tasks scheduled to them; communicate to exchange data between subsequent Tasks.
- Transformation: applied on data streams/sets, results in output streams/sets; may change per-record, change partitioning, or aggregate; an API concept (most implemented by Operators).
- UID: unique identifier of an Operator (provided by user or determined from structure); converted to a UID hash when the application is submitted.
- UID hash: unique identifier of an Operator at runtime ("Operator ID"/"Vertex ID"), generated from a UID; how operators are identified within savepoints.
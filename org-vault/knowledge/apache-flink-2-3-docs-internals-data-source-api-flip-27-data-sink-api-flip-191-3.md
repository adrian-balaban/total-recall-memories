---
title: Apache Flink 2.3 docs — Internals (Data Source API FLIP-27 / Data Sink API FLIP-191+372 / job scheduling & JM data structures / StreamTask lifecycle / Cluster-Application-Job / FileSystem abstraction / native lineage)
tags: [org, flink, flink-2.3, docs, internals, flink-27-source-api, sink-api, job-scheduling, task-lifecycle, filesystems, data-lineage, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:16:47.488Z'
updated: '2026-07-08T05:16:47.488Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 docs — Internals section (7 pages: sources, sinks, job_scheduling, task_lifecycle, application_lifecycle, filesystems, data_lineage). Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/* via chrome-devtools in-browser fetch. **WHY this matters:** this is the Flink 2.3 architecture & internals reference — the FLIP-27 Data Source API (Split / SplitEnumerator [single JM instance, the "brain", callAsync for periodic discovery, Python NOT supported] / SourceReader [pull-based pollNext returning MORE_AVAILABLE/NOTHING_AVAILABLE/END_OF_INPUT, SourceReaderBase + SplitReader + SplitFetcherManager threading helpers, split-aware watermark alignment via pauseOrResumeSplits with pipeline.watermark-alignment.allow-unaligned-source-splits=false default, SupportsSplitReassignmentOnRecovery]), FLIP-191/FLIP-372 Data Sink API (Sink factory + SinkWriter write/flush/writeWatermark, SupportsWriterState for recovery, SupportsCommitter two-phase commit for exactly-once, custom topology via SupportsPreWriteTopology/SupportsPreCommitTopology/SupportsPostCommitTopology), JobManager data structures (JobGraph→ExecutionGraph, JobVertex→ExecutionVertex per subtask, IntermediateDataSet→IntermediateResultPartition, job states created/running/failing/restarting/finished/failed/cancelling/cancelled/suspended [locally terminal], SlotSharingGroup/CoLocationGroup), StreamTask lifecycle (setup→initializeState→open [operators opened last-to-first]→run processElement/processWatermark→finish→close [closed first-to-last], snapshotState async on CheckpointBarrier, interrupted execution jumps to close, final checkpoint waits for 2PC commit), FLIP-549/FLIP-560 Cluster-Application-Job architecture (AbstractApplication / PackagedProgramApplication / SingleJobApplication, Dispatcher manages all applications), FileSystem abstraction (org.apache.flink.core.fs.FileSystem, only `file://` native + Hadoop-bridged hdfs/s3/s3n/s3a/gcs, fs.hdfs.hadoopconf config, persistence = Visibility + Durability requirements, close-to-open POSIX semantics, LOCAL FS NOT SUITABLE FOR PRODUCTION — checkpoints/savepoints on local FS not recoverable, no append/seek, overwrite via delete+create, S3 only eventual consistency so Flink strictly avoids writing same path twice, thread-safe FileSystem but non-thread-safe streams), and Native Lineage Support (FLIP-314 internal lineage data model + Job Status Listener for OpenLineage integration, LineageVertexProvider/LineageVertex/Lineage Datasets). Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sources/
# Data Sources
This page describes Flink's Data Source API and the concepts and architecture behind it. Read this, if you are interested in how data sources in Flink work, or if you want to implement a new Data Source.
If you are looking for pre-defined source connectors, please check the Connector Docs.
## Data Source Concepts
Core Components
A Data Source has three core components: Splits, the SplitEnumerator, and the SourceReader.
- A Split is a portion of data consumed by the source, like a file or a log partition. Splits are the granularity by which the source distributes the work and parallelizes reading data.
- The SourceReader requests Splits and processes them, for example by reading the file or log partition represented by the Split. The SourceReaders run in parallel on the Task Managers in the SourceOperators and produce the parallel stream of events/records.
- The SplitEnumerator generates the Splits and assigns them to the SourceReaders. It runs as a single instance on the Job Manager and is responsible for maintaining the backlog of pending Splits and assigning them to the readers in a balanced manner.
The `Source` class is the API entry point that ties the above three components together.
Unified Across Streaming and Batch
The Data Source API supports both unbounded streaming sources and bounded batch sources, in a unified way.
The difference between both cases is minimal: In the bounded/batch case, the enumerator generates a fixed set of splits, and each split is necessarily finite. In the unbounded streaming case, one of the two is not true (splits are not finite, or the enumerator keeps generating new splits).
Split Reassignment On Recovery
Under normal circumstances, once the SplitEnumerator assigns Splits to SourceReaders, these splits are not reassigned to other readers again. When the source is recovering from a failure, the splits from the saved state will be added back to the readers immediately.
When a source implements the `SupportsSplitReassignmentOnRecovery` interface, the recovery process behaves differently. On Recovery, instead of immediately reassigning the splits back to the same SourceReaders, all splits are collected and added back to the SplitEnumerator. The SplitEnumerator then takes responsibility for redistributing these splits among the available SourceReaders in a balanced manner. This mechanism enables more flexible and efficient recovery by allowing the central SplitEnumerator to make informed decisions about split distribution.
#### Examples
Bounded File Source: The source has the URI/Path of a directory to read, and a Format. A Split is a file or file region. The SplitEnumerator lists all files, assigns Splits to the next reader that requests, responds NoMoreSplits once all assigned. The SourceReader requests a Split, reads & parses, finishes on NoMoreSplits.
Unbounded Streaming File Source: Same, except SplitEnumerator never responds NoMoreSplits and periodically lists the directory for new files.
Unbounded Streaming Kafka Source: A Split is a Kafka Topic Partition. SplitEnumerator connects to brokers to list topic partitions (optionally repeating to discover new). SourceReader reads via KafkaConsumer + deserializes; splits have no end.
Bounded Kafka Source: Same, except each Split (Topic Partition) has a defined end offset; SourceReader finishes that Split at end offset, finishes overall when all assigned Splits finished.
## The Data Source API
This section describes the major interfaces of the new Source API introduced in FLIP-27.
### Source
The `Source` API is a factory style interface to create: Split Enumerator, Source Reader, Split Serializer, Enumerator Checkpoint Serializer. It also provides the `boundedness` attribute so Flink chooses the appropriate run mode. Source implementations should be serializable (serialized and uploaded to the cluster at runtime).
### SplitEnumerator
The `SplitEnumerator` is the "brain" of the Source. Typical implementations do: SourceReader registration handling; SourceReader failure handling (addSplitsBack() invoked when a SourceReader fails — take back unacknowledged splits); SourceEvent handling (custom events between SplitEnumerator and SourceReader for sophisticated coordination); Split discovery and assignment (in response to new splits, new reader registration, reader failure, etc.).
A SplitEnumeratorContext is provided for retrieval of reader info and coordination actions. For active (proactive) behavior like periodic split discovery, callAsync() in the SplitEnumeratorContext lets implementations act without maintaining their own threads.
`[code: class MySplitEnumerator implements SplitEnumerator<MySplit, MyCheckpoi ... (1128 chars)]` (Java); Python: not supported.
### SourceReader
The `SourceReader` runs in the Task Managers to consume records from Splits. It exposes a pull-based consumption interface: a Flink task calls pollNext(ReaderOutput) in a loop. Return values:
- MORE_AVAILABLE - records available immediately.
- NOTHING_AVAILABLE - no records now, but may have more in the future.
- END_OF_INPUT - exhausted all records, reached end of data; reader can be closed.
A ReaderOutput is provided so a SourceReader can emit multiple records per pollNext() when necessary (e.g., block-granularity external systems). However, avoid emitting multiple records unless necessary — the task thread works in an event-loop and cannot block.
All SourceReader state should be maintained inside the SourceSplits returned at snapshotState() — this allows reassignment to other SourceReaders. A SourceReaderContext lets the reader send SourceEvents to its SplitEnumerator (typical pattern: readers report local info, enumerator with global view decides). The SourceReaderBase class significantly reduces the work to write a SourceReader — highly recommended over writing from scratch.
### Use the Source
`[code: final StreamExecutionEnvironment env = StreamExecutionEnvironment.getE ... (265 chars)]` (Java); `[code: env = StreamExecutionEnvironment.get_execution_environment() ... (171 chars)]` (Python).
## The Split Reader API
The core SourceReader API is fully asynchronous and requires manual async split management. In practice most sources do blocking operations (KafkaConsumer.poll, HDFS/S3 I/O). The `SplitReader` is the high-level API for simple synchronous reading/polling-based implementations. The core is SourceReaderBase, which takes a SplitReader and creates fetcher threads.
### SplitReader
Three methods: a blocking fetch returning `RecordsWithSplitIds`; a non-blocking handle-split-changes method; a non-blocking wake-up method.
### SourceReaderBase
Common SourceReader work done out of the box: pool of threads fetching in blocking way; synchronization between fetching threads and pollNext(); per-split watermark for watermark alignment; per-split state for checkpoint; rate limiting. Inherit from SourceReaderBase, fill in a few methods, implement a high-level SplitReader.
#### Rate Limiting
Pass a `RateLimiterStrategy` to the SourceReaderBase constructor. Default: no rate limiting.
### SplitFetcherManager
SourceReaderBase supports threading models depending on the SplitFetcherManager. It creates/maintains a pool of SplitFetchers and determines how to assign splits to each fetcher. `[code: /** ... (1400 chars)]` (Java, fixed-fetcher-size threading model); Python not supported. `[code: public class FixedFetcherSizeSourceReader<E, T, SplitT extends SourceS ... (1060 chars)]`.
## Event Time and Watermarks
Event Time assignment and Watermark Generation happen as part of the data sources. The event streams leaving Source Readers have event timestamps and (during streaming execution) contain watermarks.
#### API
The WatermarkStrategy is passed to the Source during creation and creates both `TimestampAssigner` and `WatermarkGenerator`. `[code: environment.fromSource( ... (129 chars)]` (Java); `[code: environment.from_source( ... (148 chars)]` (Python). They run transparently as part of ReaderOutput/SourceOutput — source implementors need no timestamp/watermark code.
#### Event Timestamps
Two steps: (1) SourceReader may attach source record timestamp via SourceOutput.collect(event, timestamp) — only for record-based sources with timestamps (Kafka, Kinesis, Pulsar, Pravega); file sources have none. (2) TimestampAssigner (configured by application) assigns the final timestamp, seeing both source record timestamp and event. Note: a source without source record timestamps + selecting source record timestamp as final → events get LONG_MIN (=-9,223,372,036,854,775,808).
#### Watermark Generation
Active only during streaming execution (batch deactivates them — no-ops). Supports running watermark generators individually per split — important to handle event time skew and prevent idle partitions from holding back the whole application. Split Reader API implementations get split-aware watermarks out-of-the-box. Lower-level SourceReader API implementations must output events from different splits to different Split-local SourceOutputs via createOutputForSplit(splitId) / releaseOutputForSplit(splitId).
#### Split Level Watermark Alignment
Source operator watermark alignment is handled by Flink runtime, but the source must additionally implement SourceReader#pauseOrResumeSplits and SplitReader#pauseOrResumeSplits for split-level alignment. Default implementations throw UnsupportedOperationException; pipeline.watermark-alignment.allow-unaligned-source-splits is false by default (throws when >1 split assigned and the split exceeds the alignment threshold). SourceReaderBase contains an implementation for SourceReader#pauseOrResumeSplits, so inheriting sources only need SplitReader#pauseOrResumeSplits.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sinks/
# Data Sinks
This page describes Flink's Data Sink API (FLIP-191 and FLIP-372).
## The Data Sink API
### Sink
The `Sink` API is a factory style interface to create the SinkWriter. Sink implementations should be serializable.
#### Use the Sink
`[code: final StreamExecutionEnvironment env = StreamExecutionEnvironment.getE ... (329 chars)]` — call DataStream.sinkTo(Sink).
### SinkWriter
Three methods:
- write(InputT element, Context context): Adds an element to the writer.
- flush(boolean endOfInput): Called on checkpoint or end of input; setting this flag flushes all pending data for at-least-once. For exactly-once, implement SupportsCommitter.
- writeWatermark(Watermark watermark): Adds a watermark to the writer.
## Advanced Sink API
### SupportsWriterState
Indicates the sink supports writer state → can be recovered from a failure. Requires SinkWriter to implement `StatefulSinkWriter`.
### SupportsCommitter
Indicates the sink supports exactly-once via a two-phase commit protocol. The Sink consists of a CommittingSinkWriter (performs precommits) and a Committer (actually commits). CommittingSinkWriter creates committables on checkpoint or end of input and sends to the Committer. Sink must be serializable; all config validated eagerly; writers/committers are transient, created only in subtasks on TaskManagers.
### Custom sink topology
Advanced developers may specify their own sink operator topology.
#### SupportsPreWriteTopology
Custom operator topology before SinkWriter — process or redistribute input data. E.g., sending data of the same partition to the same SinkWriter of Kafka or Iceberg. Adds PrePartition + PostPartition operators.
#### SupportsPreCommitTopology
Custom operator topology after SinkWriter and before Committer — process or redistribute commit messages. E.g., a CollectCommit operator collects all commit messages from SinkWriters to one subtask, then sends to Committer centrally — reduces server interactions.
#### SupportsPostCommitTopology
Custom operator topology after Committer. E.g., a MergeFile operator merges small files into larger files to speed up filesystem reads.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/job_scheduling/
# Jobs and Scheduling
This document describes how Flink schedules jobs and how it represents and tracks job status on the JobManager.
## Scheduling
Execution resources are defined through Task Slots. Each TaskManager has one or more task slots, each running one pipeline of parallel tasks (e.g., n-th MapFunction + n-th ReduceFunction). Flink often executes successive tasks concurrently — always for Streaming, frequently for Batch. Internally, Flink defines via `SlotSharingGroup` (permissive — which tasks MAY share a slot) and `CoLocationGroup` (strict — which tasks MUST be in the same slot).
## JobManager Data Structures
The JobManager receives the `JobGraph` (data flow = operators `JobVertex` + intermediate results `IntermediateDataSet`; each operator has parallelism + executing code; attached libraries necessary to execute). The JobManager transforms JobGraph into an `ExecutionGraph` (parallel version): per JobVertex, one `ExecutionVertex` per parallel subtask (parallelism 100 → 1 JobVertex + 100 ExecutionVertices). ExecutionVertex tracks a subtask's execution state; all ExecutionVertices from one JobVertex held in `ExecutionJobVertex` (operator-as-a-whole status). ExecutionGraph also contains `IntermediateResult` (tracks IntermediateDataSet) and `IntermediateResultPartition` (tracks each partition).
Each ExecutionGraph has a job status: created → running → finished. On failure: → failing (cancels all running tasks) → failed (if all vertices terminal & not restartable) or → restarting (if restartable) → created. On user cancel: → cancelling (cancels running tasks) → cancelled. finished/canceled/failed are globally terminal (trigger cleanup). `suspended` is only locally terminal — execution terminated on this JobManager but another JM can retrieve the job from the persistent HA store and restart it (so not completely cleaned up).
Each parallel task goes through multiple stages; execution of an ExecutionVertex is tracked in an `Execution` (current Execution + prior Executions, since a task may execute multiple times during recovery).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/task_lifecycle/
# Task Lifecycle
A task is the basic unit of execution — where each parallel instance of an operator runs (parallelism 5 → 5 separate tasks). The StreamTask is the base for all streaming task sub-types.
## Operator Lifecycle in a nutshell
`[code:     // initialization phase ... (537 chars)]` — operator methods in call order (with UDF methods indented beneath, available if operator extends AbstractUdfStreamOperator):
setup() initializes operator-specific machinery (RuntimeContext, metric data-structures). initializeState() gives operator its initial state (BOTH initial-execution state registration AND checkpoint-retrieval-after-failure logic). open() does operator-specific init (e.g., opening the UDF for AbstractUdfStreamOperator).
Then incoming elements: input elements → processElement() (where UDF logic runs, e.g., MapFunction.map()); watermarks → processWatermark(); checkpoint barriers → trigger checkpoint → (async) snapshotState(). On normal fault-free termination: finish() (final bookkeeping — flush buffered data, emit end-of-processing markers) then close() (free resources). On failure/manual cancellation: jumps directly to close(), skipping intermediate phases.
Checkpoints: snapshotState() called asynchronously whenever a checkpoint barrier is received; performed during the processing phase (after open, before close). Stores current operator state to the state backend for retrieval after failure.
## Task Lifecycle
The sequence is mainly in the invoke() method of StreamTask. Split into Normal Execution and Interrupted Execution.
### Normal Execution
`[code:     TASK::setInitialState ... (399 chars)]`
After recovering task config + initializing runtime params, first step is setInitialState() (retrieves task-wide initial state) — important when recovering from failure (restart from last successful checkpoint) or resuming from a savepoint; empty on first execution.
Then invoke(): setup() each operator → task-specific init() (acquires task-wide resources, e.g., OneInputStreamTask initializes connections to input-stream partitions). Then initializeState() (calls each operator's initializeState() — overridden by stateful operators, holds both first-execution and recovery/savepoint logic). Then openAllOperators() calls each operator's open() (operational init, e.g., register retrieved timers). Operators are opened from LAST to FIRST so when the first starts processing, all downstream are ready.
Then task-specific run() until no more input (finite stream) or cancelled — processElement()/processWatermark() called here.
On running-to-completion: exit run() → timer service stops registering new timers, clears not-yet-started timers, awaits executing timers → finishAllOperators() calls each operator's finish() → flush buffered output → if final checkpoint enabled, wait for final checkpoint completion (ensures 2PC-committing operators committed all records) → close() each operator (closed FIRST to LAST, opposite of open) → shut down timer service → task-specific cleanup (clean internal buffers) → generic cleanup (close output channels, clean output buffers).
Checkpoints: performed periodically by a different thread than the main task thread (not in the main lifecycle phases). CheckpointBarriers injected periodically by source tasks, travel with data source→sink. A source injects barriers after it is in running mode and the CheckpointCoordinator is running. On receiving a barrier, a task schedules checkpoint-thread work calling snapshotState(). Input data can still be received during checkpoint but is buffered and only processed/emitted after the checkpoint completes.
### Interrupted Execution
If the task is cancelled at any point, normal execution is interrupted; only timer-service shutdown, task-specific cleanup, operator closing, and general task cleanup are performed from that point on.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/application_lifecycle/
# Application Lifecycle
An application represents a piece of user-defined logic for execution — a unified abstraction for tracking execution status of user main() methods and managing associated jobs. See FLIP-549 and FLIP-560.
## Cluster-Application-Job Architecture
Three-tier: Cluster-Application-Job, unifying deployment modes and providing observability/manageability of user-logic execution. Cluster modes: Application Mode (one cluster per application) or Session Mode (one cluster for multiple applications). An application can contain 0 to N jobs; each job associated with exactly one application.
## Application Implementations
`AbstractApplication` is the base class. Two concrete implementations:
- PackagedProgramApplication: Wraps a user JAR, executes its main(). Application Mode or Session Mode with REST submission via /jars/:jarid/run-application. Lifecycle tied to execution of user's main().
- SingleJobApplication: Wraps submission of a single job as a lightweight main(). Suitable for single-job submission, e.g., Session Mode CLI submission. Lifecycle tied to the job's execution status.
## Application Status
created → running (execution begins) → finished (normal completion + all associated jobs terminal). On failure: → failing (cancels all non-terminal jobs) → failed (after all jobs terminal). On user cancel: → canceling (cancels non-terminal jobs) → canceled. finished/canceled/failed are terminal → trigger archiving and cleanup.
## Application Submission
Application Mode: cluster started for a single application; during startup a PackagedProgramApplication auto-generated from the user JAR; executes immediately after cluster ready; lifecycle tied to main().
Session Mode: multiple applications share the cluster; submit via:
- REST API /jars/:jarid/run-application: creates PackagedProgramApplication from uploaded JAR; lifecycle tied to main().
- REST API /jars/:jarid/run and CLI submission: directly execute user's main(); when it calls execute() to submit a job, the job is wrapped as a SingleJobApplication (lifecycle tied to job status — lightweight wrapper).
The `Dispatcher` manages all applications in the cluster — interfaces for querying status, managing lifecycle, cancellation, recovery.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/filesystems/
# File Systems
Flink has its own file system abstraction via org.apache.flink.core.fs.FileSystem — a common set of operations and minimal guarantees across many file system implementations. The operation set is limited (e.g., no append/mutate existing files) to support a wide range of file systems. Identified by scheme: file://, hdfs://, etc.
# Implementations
Flink implements directly: `file` (machine's local FS). Others bridged to Apache Hadoop's file systems: hdfs (HDFS), s3/s3n/s3a (Amazon S3), gcs (Google Cloud Storage), etc. Flink loads Hadoop's file systems transparently if it finds Hadoop FS classes in the classpath AND a valid Hadoop configuration (looks in classpath by default, or custom location via fs.hdfs.hadoopconf).
# Persistence Guarantees
Used to persistently store data (results + fault tolerance/recovery), so persistence semantics are well defined.
## Definition of Persistence Guarantees
Data written to an output stream is persistent if:
- Visibility Requirement: all other processes/machines/VMs/containers that can access the file see the data consistently given the absolute file path (like POSIX close-to-open, restricted to the file by absolute path).
- Durability Requirement: the file system's specific durability/persistence requirements are met (e.g., LocalFileSystem provides NO durability for hardware/OS crashes; replicated distributed FS like HDFS guarantee durability up to n concurrent node failures where n = replication factor).
Updates to the parent directory (file showing up in listings) are NOT required to be complete for the file-stream data to be persistent — important for eventually-consistent directory-update file systems. FSDataOutputStream must guarantee persistence once close() returns.
## Examples
- Fault-tolerant distributed FS: persistent once received & acknowledged (replicated to a quorum) + absolute path visible to all potential accessor machines. Directory metadata updates need not be consistent (some machines may see the file in listings, others not, as long as absolute-path access works on all nodes).
- Local FS: must support POSIX close-to-open semantics; no fault-tolerance guarantees beyond that. Data may still be in OS cache when considered persistent — crashes losing OS-cache data are fatal to the local machine and NOT covered. → Computed results, checkpoints, and savepoints written ONLY to local FS are NOT guaranteed recoverable from local-machine failure → local FS UNSUITABLE for production setups.
# Updating File Contents
Many file systems don't support overwriting, or don't support consistent visibility of updates. So Flink's FileSystem does NOT support appending to existing files or seeking within output streams to change previously written data.
# Overwriting Files
Overwriting is generally possible (delete + create new file). But some filesystems can't make that synchronously visible (e.g., Amazon S3 guarantees only eventual consistency on file replacement — some machines see old, some see new). To avoid these consistency issues, Flink's failure/recovery mechanisms strictly avoid writing to the same file path more than once.
# Thread Safety
FileSystem implementations MUST be thread-safe (same instance shared across multiple threads, must concurrently create streams + list metadata). FSDataOutputStream/FSDataInputStream are strictly NOT thread-safe and should not be passed between threads between read/write operations (no guarantees about visibility of operations across threads — many operations create no memory fences).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/data_lineage/
# Native Lineage Support
As organisations govern data ecosystems, understanding data lineage (where data comes from / goes to) becomes critical. Flink is widely used for data ingestion and ETL in Streaming Data Lakes, so an end-to-end lineage solution is needed for: Data Quality Assurance (trace data errors to origin), Data Governance (document data origins/transformations for ownership/accountability), Regulatory Compliance (track data flow/transformations for privacy/compliance), Data Optimization (identify redundant steps, optimize flows).
Flink provides native lineage support via an internal lineage data model and Job Status Listener for developers to integrate lineage metadata into external lineage systems (e.g., OpenLineage). When a job is created in Flink runtime, the JobCreatedEvent contains the Lineage Graph metadata sent to Job Status Listeners.
# Lineage Data Model
Two layers: (1) generic interface for all Flink jobs and connectors; (2) extended interfaces for Table and DataStream independently. By default, Table-related lineage interfaces/classes are used in the Flink Table environment, so users don't touch them directly. The Flink community will gradually support common connectors (Kafka, JDBC, Cassandra, Hive). For a customized connector, you need customized source/sink implementations of the LineageVertexProvider interface. Within a LineageVertex, a list of Lineage Datasets are defined as metadata for Flink source/sink.
`[code: @PublicEvolving ... (94 chars)]`
For interface details, see FLIP-314.
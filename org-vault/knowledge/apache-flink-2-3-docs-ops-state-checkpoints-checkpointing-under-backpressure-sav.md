---
title: Apache Flink 2.3 docs — Ops state (checkpoints / checkpointing under backpressure / savepoints / checkpoints vs savepoints / state backends)
tags: [org, flink, flink-2.3, docs, ops, state, checkpoints, savepoints, state-backends, backpressure, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:07:41.737Z'
updated: '2026-07-08T05:07:41.737Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 docs — Ops/state section batch A (5 pages: checkpoints, checkpointing_under_backpressure, savepoints, checkpoints_vs_savepoints, state_backends). Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/* via chrome-devtools in-browser fetch. **WHY this matters:** state management and checkpointing are the core of Flink's fault-tolerance/exactly-once guarantees; this is the operational reference for choosing storage/backends, tuning checkpoint duration under backpressure, and performing savepoint-based job upgrades. Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpoints/
# Checkpoints

## Overview
Checkpoints make state in Flink fault tolerant by allowing state and the corresponding stream positions to be recovered, thereby giving the application the same semantics as a failure-free execution. To understand the differences between checkpoints and savepoints see checkpoints vs. savepoints.

## Checkpoint Storage
When checkpointing is enabled, managed state is persisted to ensure consistent recovery in case of failures. Where the state is persisted during checkpointing depends on the chosen Checkpoint Storage.

## Available Checkpoint Storage Options
Out of the box, Flink bundles these checkpoint storage types:
- JobManagerCheckpointStorage
- FileSystemCheckpointStorage
If a checkpoint directory is configured FileSystemCheckpointStorage will be used, otherwise the system will use the JobManagerCheckpointStorage.

### The JobManagerCheckpointStorage
Stores checkpoint snapshots in the JobManager's heap. Can be configured to fail the checkpoint if it goes over a certain size to avoid OutOfMemoryError's: `[code: new JobManagerCheckpointStorage(MAX_MEM_STATE_SIZE); ... (52 chars)]`
Limitations:
- The size of each individual state is by default limited to 5 MB. This value can be increased in the constructor.
- Irrespective of the configured maximal state size, the state cannot be larger than the Pekko frame size (see Configuration).
- The aggregate state must fit into the JobManager memory.
Encouraged for:
- Local development and debugging
- Jobs that use very little state, such as jobs that consist only of record-at-a-time functions (Map, FlatMap, Filter, …). The Kafka Consumer requires very little state.

### The FileSystemCheckpointStorage
Configured with a file system URL (type, address, path), such as "hdfs://namenode:40010/flink/checkpoints" or "file:///data/flink/checkpoints". Upon checkpointing, it writes state snapshots into files in the configured file system and directory. Minimal metadata is stored in the JobManager's memory (or, in high-availability mode, in the metadata checkpoint). If a checkpoint directory is specified, FileSystemCheckpointStorage will be used to persist checkpoint snapshots.
Encouraged for:
- All high-availability setups.

## Retained Checkpoints
Checkpoints are by default not retained and are only used to resume a job from failures. They are deleted when a program is cancelled. You can, however, configure periodic checkpoints to be retained. Depending on the configuration these retained checkpoints are not automatically cleaned up when the job fails or is canceled. `[code: CheckpointConfig config = env.getCheckpointConfig(); ... (151 chars)]`
The ExternalizedCheckpointRetention mode configures what happens with checkpoints when you cancel the job:
- ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION: Retain the checkpoint when the job is cancelled. Note that you have to manually clean up the checkpoint state after cancellation in this case.
- ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION: Delete the checkpoint when the job is cancelled. The checkpoint state will only be available if the job fails.

### Directory Structure
A checkpoint consists of a meta data file and, depending on the state backend, some additional data files. Stored in the directory configured via execution.checkpointing.dir, also per job in code. Current layout (introduced by FLINK-8531): `[code: /user-defined-checkpoint-dir ... (164 chars)]`
The SHARED directory is for state that is possibly part of multiple checkpoints, TASKOWNED is for state that must never be dropped by the JobManager, and EXCLUSIVE is for state that belongs to one checkpoint only. The checkpoint directory is not part of a public API and can be changed in the future release.

#### Configure globally via configuration files
`[code: execution.checkpointing.dir: hdfs:///checkpoints/ ... (49 chars)]`
#### Configure for per job on the checkpoint configuration
`[code: Configuration config = new Configuration(); ... (218 chars)]`
#### Configure with checkpoint storage instance
Alternatively, checkpoint storage can be set by specifying the desired checkpoint storage instance which allows for setting low level configurations such as write buffer sizes. `[code: Configuration config = new Configuration(); ... (293 chars)]`

### Resuming from a retained checkpoint
A job may be resumed from a checkpoint just as from a savepoint by using the checkpoint's meta data file instead. Note that if the meta data file is not self-contained, the jobmanager needs to have access to the data files it refers to. `[code: $ bin/flink run -s :checkpointMetaDataPath [:runArgs] ... (53 chars)]`

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/
# Checkpointing under backpressure

Normally aligned checkpointing time is dominated by the synchronous and asynchronous parts of the checkpointing process. However, when a Flink job is running under heavy backpressure, the dominant factor in the end-to-end time of a checkpoint can be the time to propagate checkpoint barriers to all operators/subtasks. This can be observed by high alignment time and start delay metrics. When this happens and becomes an issue, there are three ways to address the problem:
- Remove the backpressure source by optimizing the Flink job, by adjusting Flink or JVM configurations, or by scaling up.
- Reduce the amount of buffered in-flight data in the Flink job.
- Enable unaligned checkpoints.
These options are not mutually exclusive and can be combined together.

## Buffer debloating
Flink 1.14 introduced a new tool to automatically control the amount of buffered in-flight data between Flink operators/subtasks. The buffer debloating mechanism can be enabled by setting the property taskmanager.network.memory.buffer-debloat.enabled to true. This feature works with both aligned and unaligned checkpoints and can improve checkpointing times in both cases, but the effect of the debloating is most visible with aligned checkpoints. When using buffer debloating with unaligned checkpoints, the added benefit will be smaller checkpoint sizes and quicker recovery times (there will be less in-flight data to persist and recover). For more information refer to the network memory tuning guide. You can also manually reduce the amount of buffered in-flight data.

## Unaligned checkpoints
Starting with Flink 1.11, checkpoints can be unaligned. Unaligned checkpoints contain in-flight data (i.e., data stored in buffers) as part of the checkpoint state, allowing checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes independent of the current throughput as checkpoint barriers are effectively not embedded into the stream of data anymore.
You should use unaligned checkpoints if your checkpointing durations are very high due to backpressure. Then, checkpointing time becomes mostly independent of the end-to-end latency. Be aware unaligned checkpointing adds to I/O to the state storage, so you shouldn't use it when the I/O to the state storage is actually the bottleneck during checkpointing.
Enable: `[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (180 chars)]` (Java) / `[code: env = StreamExecutionEnvironment.get_execution_environment() ... (156 chars)]` (Python) or in flink-conf.yml: `[code: execution.checkpointing.unaligned: true ... (39 chars)]`

### Aligned checkpoint timeout
After enabling unaligned checkpoints, you can also specify the aligned checkpoint timeout programmatically: `[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (165 chars)]` or in flink-conf.yml: `[code: execution.checkpointing.aligned-checkpoint-timeout: 30 s ... (56 chars)]`. When activated, each checkpoint will still begin as an aligned checkpoint, but when the global checkpoint duration exceeds the aligned-checkpoint-timeout, if the aligned checkpoint has not completed, then the checkpoint will proceed as an unaligned checkpoint.

### Limitations
#### Concurrent checkpoints
Flink currently does not support concurrent unaligned checkpoints. However, due to the more predictable and shorter checkpointing times, concurrent checkpoints might not be needed at all. However, savepoints can also not happen concurrently to unaligned checkpoints, so they will take slightly longer.

#### Interplay with watermarks
Unaligned checkpoints break with an implicit guarantee in respect to watermarks during recovery. Currently, Flink generates the watermark as the first step of recovery instead of storing the latest watermark in the operators to ease rescaling. In unaligned checkpoints, that means on recovery, Flink generates watermarks after it restores in-flight data. If your pipeline uses an operator that applies the latest watermark on each record will produce different results than for aligned checkpoints. If your operator depends on the latest watermark being always available, the workaround is to store the watermark in the operator state. In that case, watermarks should be stored per key group in a union state to support rescaling.

#### Interplay with long-running record processing
Despite that unaligned checkpoints barriers are able to overtake all other records in the queue. The handling of this barrier still can be delayed if the current record takes a lot of time to be processed. This situation can occur when firing many timers all at once, for example in windowed operations. Second problematic scenario might occur when system is being blocked waiting for more than one network buffer availability when processing a single input record. Flink can not interrupt processing of a single input record, and unaligned checkpoints have to wait for the currently processed record to be fully processed. This can cause problems in two scenarios. Either as a result of serialisation of a large record that doesn't fit into single network buffer or in a flatMap operation, that produces many output records for one input record. In such scenarios back pressure can block unaligned checkpoints until all the network buffers required to process the single input record are available. As result, the time of the checkpoint can be higher than expected or it can vary.

#### Certain data distribution patterns are not checkpointed
There are types of connections with properties that are impossible to keep with channel data stored in checkpoints. To preserve these characteristics and ensure no state corruption or unexpected behaviour, unaligned checkpoints are disabled for such connections. All other exchanges still perform unaligned checkpoints.
Pointwise connections: We currently do not have any hard guarantees on pointwise connections regarding data orderliness. However, since data was structured implicitly in the same way as any preceding source or keyby, some users relied on this behaviour to divide compute-intensive tasks into smaller chunks while depending on orderliness guarantees. As long as the parallelism does not change, unaligned checkpoints (UC) retain these properties. With the addition of rescaling of UC that has changed. Consider a job — If we want to rescale from parallelism p = 2 to p = 3, suddenly the records inside the keyby channels need to be divided into three channels according to the key groups. For the forward channels, we lack the key context entirely. No record in the forward channel has any key group assigned; it's also impossible to calculate it as there is no guarantee that the key is still present.
Broadcast connections: Broadcast connections bring another problem to the table. There are no guarantees that records are consumed at the same rate in all channels. This can result in some tasks applying state changes corresponding to a specific broadcasted event while others don't. Broadcast partitioning is often used to implement a broadcast state which should be equal across all operators. Flink implements the broadcast state by checkpointing only a single copy of the state from subtask 0 of the stateful operator. Upon restore, we send that copy to all of the operators. Therefore it might happen that an operator will get the state with changes applied for a record that it will soon consume from its checkpointed channels.

### Troubleshooting
#### Corrupted in-flight data
Actions described below are a last resort as they will lead to data loss. In case of the in-flight data corrupted or by another reason when the job should be restored without the in-flight data, it is possible to use recover-without-channel-state.checkpoint-id property. This property requires to specify a checkpoint id for which in-flight data will be ignored. Do not set this property, unless a corruption inside the persisted in-flight data has lead to an otherwise unrecoverable situation. The property can be applied only after the job will be redeployed which means this operation makes sense only if externalized checkpoint is enabled.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/savepoints/
# Savepoints

## What is a Savepoint?
A Savepoint is a consistent image of the execution state of a streaming job, created via Flink's checkpointing mechanism. You can use Savepoints to stop-and-resume, fork, or update your Flink jobs. Savepoints consist of two parts: a directory with (typically large) binary files on stable storage (e.g. HDFS, S3, …) and a (relatively small) meta data file. The files on stable storage represent the net data of the job's execution state image. The meta data file of a Savepoint contains (primarily) pointers to all files on stable storage that are part of the Savepoint, in form of relative paths. In order to allow upgrades between programs and Flink versions, it is important to check out the following section about assigning IDs to your operators.

## Assigning Operator IDs
It is highly recommended that you specify operator IDs via the uid(String) method. These IDs are used to scope the state of each operator. `[code: DataStream<String> stream = env. ... (333 chars)]`. If you do not specify the IDs manually they will be generated automatically. You can automatically restore from the savepoint as long as these IDs do not change. The generated IDs depend on the structure of your program and are sensitive to program changes. Therefore, it is highly recommended assigning these IDs manually.

### Savepoint State
You can think of a savepoint as holding a map of Operator ID -> State for each stateful operator: `[code: Operator ID | State ... (133 chars)]`. In the above example, the print sink is stateless and hence not part of the savepoint state. By default, we try to map each entry of the savepoint back to the new program.

## Operations
You can use the command line client to trigger savepoints, cancel a job with a savepoint, resume from savepoints, and dispose savepoints. It is also possible to resume from savepoints using the webui.

### Triggering Savepoints
When triggering a savepoint, a new savepoint directory is created where the data as well as the meta data will be stored. The location of this directory can be controlled by configuring a default target directory or by specifying a custom target directory with the trigger commands (see the :targetDirectory argument). Attention: The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system or Object Store.
For example with a FsStateBackend or RocksDBStateBackend: `[code: # Savepoint target directory ... (291 chars)]`. Savepoints can generally be moved by moving (or copying) the entire savepoint directory to a different location, and Flink will be able to restore from the moved savepoint.
There are two exceptions:
- if entropy injection is activated: In that case the savepoint directory will not contain all savepoint data files, because the injected path entropy spreads the files over many directories. Lacking a common savepoint root directory, the savepoints will contain absolute path references, which prevent moving the directory.
- The job contains task-owned state, such as GenericWriteAhreadLog sink.
Unlike savepoints, checkpoints cannot generally be moved to a different location, because checkpoints may include some absolute path references.
If you use statebackend: jobmanager, metadata and savepoint state will be stored in the _metadata file, so don't be confused by the absence of additional data files.
Starting from Flink 1.15 intermediate savepoints (savepoints other than created with stop-with-savepoint) are not used for recovery and do not commit any side effects. This has to be taken into consideration, especially when running multiple jobs in the same checkpointing timeline. It is possible in that solution that if the original job (after taking a savepoint) fails, then it will fall back to a checkpoint prior to the savepoint. However, if we now resume a job from the savepoint, then we might commit transactions that might've never happened because of falling back to a checkpoint before the savepoint (assuming non-determinism). If one wants to be safe in those scenarios, we advise dropping the state of transactional sinks, by changing sinks uids. It should not require any additional steps if there is just a single job running in the same checkpointing timeline, which means that you stop the original job before running a new job from the savepoint.

#### Savepoint format
You can choose between two binary formats of a savepoint:
- canonical format - a format that has been unified across all state backends, which lets you take a savepoint with one state backend and then restore it using another. This is the most stable format, that is targeted at maintaining the most compatibility with previous versions, schemas, modifications etc.
- native format - the downside of the canonical format is that often it is slow to take and restore from. Native format creates a snapshot in the format specific for the used state backend (e.g. SST files for RocksDB).
The possibility to trigger a savepoint in the native format was introduced in Flink 1.15. Up until then savepoints were created in the canonical format.

#### Trigger a Savepoint
`[code: $ bin/flink savepoint :jobId [:targetDirectory] ... (47 chars)]`. This will trigger a savepoint for the job with ID :jobId, and returns the path of the created savepoint. You need this path to restore and dispose savepoints. You can also pass a type: `[code: $ bin/flink savepoint --type [native/canonical] :jobId [:targetDirecto ... (73 chars)]`. When using the above command, the client needs to wait for the savepoint to be completed; may time out when state size is large. In this case, you can trigger the savepoint in detached mode: `[code: $ bin/flink savepoint :jobId [:targetDirectory] -detached ... (57 chars)]`. Monitor the status of the savepoint through the REST API.
#### Trigger a Savepoint with YARN
`[code: $ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId ... (63 chars)]`
#### Stopping a Job with Savepoint
`[code: $ bin/flink stop --type [native/canonical] --savepointPath [:targetDir ... (84 chars)]`. This will atomically trigger a savepoint for the job with ID :jobid and stop the job. If you want to trigger the savepoint in detached mode, add option -detached to the command.

### Resuming from Savepoints
`[code: $ bin/flink run -s :savepointPath [:runArgs] ... (44 chars)]`. This submits a job and specifies a savepoint to resume from. You may give a path to either the savepoint's directory or the _metadata file.
#### Allowing Non-Restored State
By default, the resume operation will try to map all state of the savepoint back to the program you are restoring with. If you dropped an operator, you can allow to skip state that cannot be mapped to the new program via --allowNonRestoredState (short: -n) option. Improper usage could result in significant issues with correctness. Operator UIDs are reassigned based on topological order by default, which may lead to incorrect associations between states and operators. To prevent such mismatches, explicitly assign UIDs to all operators in a DataStream job.
#### Claim mode
The Claim Mode determines who takes ownership of the files that make up a Savepoint or externalized checkpoints after restoring it. Both savepoints and externalized checkpoints behave similarly in this context. Here, they are just called "snapshots" unless explicitly noted otherwise. As mentioned, the claim mode determines who takes over ownership of the files of the snapshots that we are restoring from. Snapshots can be owned either by a user or Flink itself. If a snapshot is owned by a user, Flink will not delete its files, moreover, Flink can not depend on the existence of the files from such a snapshot, as it might be deleted outside of Flink's control. Each claim mode serves a specific purposes. Still, we believe the default NO_CLAIM mode is a good tradeoff in most situations, as it provides clear ownership with a small price for the first checkpoint after the restore. `[code: $ bin/flink run -s :savepointPath -claimMode :mode -n [:runArgs] ... (64 chars)]`
NO_CLAIM (default): In the NO_CLAIM mode Flink will not assume ownership of the snapshot. It will leave the files in user's control and never delete any of the files. In this mode you can start multiple jobs from the same snapshot. In order to make sure Flink does not depend on any of the files from that snapshot, it will force the first (successful) checkpoint to be a full checkpoint as opposed to an incremental one. This only makes a difference for state.backend: rocksdb, because all other state backends always take full checkpoints. Once the first full checkpoint completes, all subsequent checkpoints will be taken as usual/configured. Consequently, once a checkpoint succeeds you can manually delete the original snapshot. You can not do this earlier, because without any completed checkpoints Flink will - upon failure - try to recover from the initial snapshot.
CLAIM: In this mode Flink claims ownership of the snapshot and essentially treats it like a checkpoint: its controls the lifecycle and might delete it if it is not needed for recovery anymore. Hence, it is not safe to manually delete the snapshot or to start two jobs from the same snapshot. Flink keeps around a configured number of checkpoints.
Attention:
- Retained checkpoints are stored in a path like <checkpoint_dir>/<job_id>/chk-<x>. Flink does not take ownership of the <checkpoint_dir>/<job_id> directory, but only the chk-<x>. The directory of the old job will not be deleted by Flink
- Native format supports incremental RocksDB savepoints. For those savepoints Flink puts all SST files inside the savepoints directory. This means such savepoints are self-contained and relocatable. Please note that, when restored in CLAIM mode, subsequent checkpoints might reuse some SST files, which might delay the deletion the savepoints directory.
LEGACY (deprecated): The legacy mode is how Flink worked until 1.15. In this mode Flink will never delete the initial checkpoint. At the same time, it is not clear if a user can ever delete it as well. The problem here, is that Flink might immediately build an incremental checkpoint on top of the restored one. Therefore, subsequent checkpoints depend on the restored checkpoint. Overall, the ownership is not well-defined. Attention: The LEGACY mode is deprecated and will be removed in Flink 2.0. Please use CLAIM or NO_CLAIM mode instead.

### Disposing Savepoints
`[code: $ bin/flink savepoint -d :savepointPath ... (39 chars)]`. This disposes the savepoint stored in :savepointPath. Note that it is possible to also manually delete a savepoint via regular file system operations without affecting other savepoints or checkpoints (recall that each savepoint is self-contained).

### Configuration
You can configure a default savepoint target directory via the execution.checkpointing.savepoint-dir key or StreamExecutionEnvironment. When triggering savepoints, this directory will be used to store the savepoint. You can overwrite the default by specifying a custom target directory with the trigger commands. `[code: # Default savepoint target directory ... (100 chars)]` (config.yaml) / `[code: env.setDefaultSavepointDir("hdfs:///flink/savepoints"); ... (55 chars)]` (Java). If you neither configure a default nor specify a custom target directory, triggering the savepoint will fail. The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system.

## F.A.Q
- Should I assign IDs to all operators in my job? As a rule of thumb, yes. Strictly speaking, it is sufficient to only assign IDs via the uid method to the stateful operators in your job. The savepoint only contains state for these operators and stateless operator are not part of the savepoint. In practice, it is recommended to assign it to all operators, because some of Flink's built-in operators like the Window operator are also stateful and it is not obvious which built-in operators are actually stateful and which are not.
- What happens if I add a new operator that requires state to my job? When you add a new operator to your job, it will be initialized without any state. Savepoints contain the state of each stateful operator. Stateless operators are simply not part of the savepoint. The new operator behaves similar to a stateless operator.
- What happens if I delete an operator that has state from my job? By default, a savepoint restore will try to match all state back to the restored job. If you restore from a savepoint that contains state for an operator that has been deleted, this will therefore fail. You can allow non restored state by setting the --allowNonRestoredState (short: -n) with the run command: `[code: $ bin/flink run -s :savepointPath -n [:runArgs] ... (47 chars)]`
- What happens if I reorder stateful operators in my job? If you assigned IDs to these operators, they will be restored as usual. If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.
- What happens if I add or delete or reorder operators that have no state in my job? If you assigned IDs to your stateful operators, the stateless operators will not influence the savepoint restore. If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering.
- What happens when I change the parallelism of my program when restoring? You can simply restore the program from a savepoint and specify a new parallelism.
- Can I move the Savepoint files on stable storage? The quick answer to this question is currently "yes". Savepoints are self-contained and relocatable. You can move the file and restore from any location.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpoints_vs_savepoints/
# Checkpoints vs. Savepoints

## Overview
Conceptually, Flink's savepoints are different from checkpoints in a way that's analogous to how backups are different from recovery logs in traditional database systems.
The primary purpose of checkpoints is to provide a recovery mechanism in case of unexpected job failures. A checkpoint's lifecycle is managed by Flink, i.e. a checkpoint is created, owned, and released by Flink - without user interaction. Because checkpoints are being triggered often, and are relied upon for failure recovery, the two main design goals for the checkpoint implementation are i) being as lightweight to create and ii) being as fast to restore from as possible. Optimizations towards those goals can exploit certain properties, e.g., that the job code doesn't change between the execution attempts.
- Checkpoints are automatically deleted if the application is terminated by the user (except if checkpoints are explicitly configured to be retained).
- Checkpoints are stored in state backend-specific (native) data format (may be incremental depending on the specific backend).
Although savepoints are created internally with the same mechanisms as checkpoints, they are conceptually different and can be a bit more expensive to produce and restore from. Their design focuses more on portability and operational flexibility, especially with respect to changes to the job. The use case for savepoints is for planned, manual operations. For example, this could be an update of your Flink version, changing your job graph, and so on.
- Savepoints are created, owned and deleted solely by the user. That means, Flink does not delete savepoints neither after job termination nor after restore.
- Savepoints are stored in a state backend independent (canonical) format (Note: Since Flink 1.15, savepoints can be also stored in the backend-specific native format which is faster to create and restore but comes with some limitations.

### Capabilities and limitations
The following table gives an overview of capabilities and limitations for the various types of savepoints and checkpoints.
- ✓ - Flink fully support this type of the snapshot
- x - Flink doesn't support this type of the snapshot
- ! - While these operations currently work, Flink doesn't officially guarantee support for them, so there is a certain level of risk associated with them
[table: 11 rows]
- State backend change - configuring a different State Backend than was used when taking the snapshot.
- State Processor API (writing) - the ability to create a new snapshot of this type via the State Processor API.
- State Processor API (reading) - the ability to read states from an existing snapshot of this type via the State Processor API.
- Self-contained and relocatable - the one snapshot folder contains everything it needs for recovery and it doesn't depend on other snapshots which means it can be easily moved to another place if needed.
- Schema evolution - the state data type can be changed if it uses a serializer that supports schema evolution (e.g., POJOs and Avro types)
- Arbitrary job upgrade - the snapshot can be restored even if the partitioning types(rescale, rebalance, map, etc.) or in-flight record types for the existing operators have changed.
- Non-arbitrary job upgrade - restoring the snapshot is possible with updated operators if the job graph topology and in-flight record types remain unchanged.
- Flink minor version upgrade - restoring a snapshot taken with an older minor version of Flink (1.x → 1.y).
- Flink bug/patch version upgrade - restoring a snapshot taken with an older patch version of Flink (1.14.x → 1.14.y).
- Rescaling - restoring the snapshot with a different parallelism than was used during the snapshot creation.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/
# State Backends

Programs written in the Data Stream API often hold state in various forms:
- Windows gather elements or aggregates until they are triggered
- Transformation functions may use the key/value state interface to store values
- Transformation functions may implement the CheckpointedFunction interface to make their local variables fault tolerant
When checkpointing is activated, such state is persisted upon checkpoints to guard against data loss and recover consistently. How the state is represented internally, and how and where it is persisted upon checkpoints depends on the chosen State Backend.

## Available State Backends
Out of the box, Flink bundles these state backends:
- HashMapStateBackend
- EmbeddedRocksDBStateBackend
If nothing else is configured, the system will use the HashMapStateBackend.

### The HashMapStateBackend
The HashMapStateBackend holds data internally as objects on the Java heap. Key/value state and window operators hold hash tables that store the values, triggers, etc. Encouraged for:
- Jobs with large state, long windows, large key/value states.
- All high-availability setups.
It is also recommended to set managed memory to zero. This will ensure that the maximum amount of memory is allocated for user code on the JVM. Unlike EmbeddedRocksDBStateBackend, the HashMapStateBackend stores data as objects on the heap so that it is unsafe to reuse objects.

### The EmbeddedRocksDBStateBackend
The EmbeddedRocksDBStateBackend holds in-flight data in a RocksDB database that is (per default) stored in the TaskManager local data directories. Unlike storing java objects in HashMapStateBackend, data is stored as serialized byte arrays, which are mainly defined by the type serializer, resulting in key comparisons being byte-wise instead of using Java's hashCode() and equals() methods. The EmbeddedRocksDBStateBackend always performs asynchronous snapshots.
Limitations of the EmbeddedRocksDBStateBackend:
- As RocksDB's JNI bridge API is based on byte[], the maximum supported size per key and per value is 2^31 bytes each. States that use merge operations in RocksDB (e.g. ListState) can silently accumulate value sizes > 2^31 bytes and will then fail on their next retrieval. This is currently a limitation of RocksDB JNI.
Encouraged for:
- Jobs with very large state, long windows, large key/value states.
- All high-availability setups.
Note that the amount of state that you can keep is only limited by the amount of disk space available. This allows keeping very large state, compared to the HashMapStateBackend that keeps state in memory. This also means, however, that the maximum throughput that can be achieved will be lower with this state backend. All reads/writes from/to this backend have to go through de-/serialization to retrieve/store the state objects, which is also more expensive than always working with the on-heap representation as the heap-based backends are doing. It's safe for EmbeddedRocksDBStateBackend to reuse objects due to the de-/serialization. EmbeddedRocksDBStateBackend is currently the only backend that offers incremental checkpoints. Certain RocksDB native metrics are available but disabled by default. The total memory amount of RocksDB instance(s) per slot can also be bounded.

## The ForStStateBackend
The ForStStateBackend is a state backend that is based on ForSt project, which is also a LSM-tree structured key-value store and built on top of the RocksDB. It is designed for disaggregated state management. Most importantly, it can hold its sst files on remote file systems that Flink supports, such as HDFS, S3, etc. This allows Flink to scale the state size beyond the local disk capacity of the TaskManager. Moreover, by putting the sst files on remote file systems, it can also provide a more lightweight way to perform checkpoint and recovery.
The ForStStateBackend is still in the experimental stage and is not fully available for production. It always performs asynchronous incremental snapshots.
Encouraged for:
- Jobs with very large state, long windows, large key/value states. Local disk may not be enough to store the state.
- All high-availability setups.
- Asynchronous state access is preferred. Since the ForStStateBackend is the only one supporting asynchronous state access.
- Jobs that require lightweight checkpoint and recovery, such as cloud-native applications.
Limitations of the ForStStateBackend (for now):
- Same as EmbeddedRocksDBStateBackend, the maximum supported size per key and per value is 2^31 bytes each.
- Does not support canonical savepoint, full snapshot, changelog and file-merging checkpoints. Always perform incremental snapshots.
Compared with EmbeddedRocksDBStateBackend, ForStStateBackend stores data on remote file system, thus the amount of state that you can keep is unlimited. The local disk of TaskManager is only used to store cache of file, to provide better performance. Note that when most of the active state is on remote file system, the performance of state access may be affected by the network latency. Flink introduces asynchronous state access to mitigate this issue. If you are using the asynchronous state methods in State API V2, you can benefit from the asynchronous state access.

## Choose The Right State Backend
When deciding between HashMapStateBackend and RocksDB, it is a choice between performance and scalability. HashMapStateBackend is very fast as each state access and update operates on objects on the Java heap; however, state size is limited by available memory within the cluster. On the other hand, RocksDB can scale based on available disk space. However, each state access and update requires (de-)serialization and potentially reading from disk which leads to average performance that is an order of magnitude slower than the memory state backends. If you are handling very large state even exceeding the available disk space, or you prefer a fast rescale under cloud-native setup, you should consider using ForStStateBackend. In Flink 1.13 we unified the binary format of Flink's savepoints. That means you can take a savepoint and then restore from it using a different state backend. All the state backends produce a common format only starting from version 1.13. Therefore, if you want to switch the state backend you should first upgrade your Flink version then take a savepoint with the new version, and only after that can you restore it with a different state backend.

## Configuring a State Backend
The default state backend, if you specify nothing, is the HashMapStateBackend. If you wish to establish a different default for all jobs on your cluster, you can do so by defining a new default state backend in Flink configuration file. The default state backend can be overridden on a per-job basis.

### Setting the Per-job State Backend
The per-job state backend is set on the StreamExecutionEnvironment of the job: `[code: Configuration config = new Configuration(); ... (124 chars)]` (Java) / `[code: config = Configuration() ... (142 chars)]` (Python). If you want to use the EmbeddedRocksDBStateBackend in your IDE or configure it programmatically, you will have to add the following dependency: `[code: <dependency> ... (179 chars)]`. Same for ForStStateBackend: `[code: <dependency> ... (177 chars)]`. Since RocksDB and ForSt is part of the default Flink distribution, you do not need this dependency if you are not using any RocksDB code in your job and configure the state backend via state.backend.type and further checkpointing and RocksDB-specific or ForSt-specific parameters in your Flink configuration file.

### Setting Default State Backend
A default state backend can be configured in the Flink configuration file, using the configuration key state.backend.type. Possible values for the config entry are hashmap (HashMapStateBackend), rocksdb (EmbeddedRocksDBStateBackend), forst (ForStStateBackend) or the fully qualified class name of the class that implements the state backend factory StateBackendFactory, such as org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackendFactory for EmbeddedRocksDBStateBackend and org.apache.flink.state.forst.ForStStateBackendFactory for ForStStateBackend. The execution.checkpointing.dir option defines the directory to which all backends write checkpoint data and meta data files. A sample section in the configuration file: `[code: # The backend that will be used to store operator state checkpoints ... (196 chars)]`

## RocksDB State Backend Details
### Incremental Checkpoints
RocksDB supports Incremental Checkpoints, which can dramatically reduce the checkpointing time in comparison to full checkpoints. Instead of producing a full, self-contained backup of the state backend, incremental checkpoints only record the changes that happened since the latest completed checkpoint. An incremental checkpoint builds upon (typically multiple) previous checkpoints. Flink leverages RocksDB's internal compaction mechanism in a way that is self-consolidating over time. As a result, the incremental checkpoint history in Flink does not grow indefinitely, and old checkpoints are eventually subsumed and pruned automatically. Recovery time of incremental checkpoints may be longer or shorter compared to full checkpoints. If your network bandwidth is the bottleneck, it may take a bit longer to restore from an incremental checkpoint, because it implies fetching more data (more deltas). Restoring from an incremental checkpoint is faster, if the bottleneck is your CPU or IOPs, because restoring from an incremental checkpoint means not re-building the local RocksDB tables from Flink's canonical key/value snapshot format (used in savepoints and full checkpoints). While we encourage the use of incremental checkpoints for large state, you need to enable this feature manually:
- Setting a default in your Flink configuration file: execution.checkpointing.incremental: true will enable incremental checkpoints, unless the application overrides this setting in the code.
- You can alternatively configure this directly in the code (overrides the config default): EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
Notice that once incremental checkpoint is enabled, the Checkpointed Data Size showed in web UI only represents the delta checkpointed data size of that checkpoint instead of full state size.

### Memory Management
Flink aims to control the total process memory consumption to make sure that the Flink TaskManagers have a well-behaved memory footprint. That means staying within the limits enforced by the environment (Docker/Kubernetes, Yarn, etc) to not get killed for consuming too much memory, but also to not under-utilize memory (unnecessary spilling to disk, wasted caching opportunities, reduced performance). To achieve that, Flink by default configures RocksDB's memory allocation to the amount of managed memory of the TaskManager (or, more precisely, task slot). This should give good out-of-the-box experience for most applications. The primary mechanism for improving memory-related performance issues would be to simply increase Flink's managed memory. Users can choose to deactivate that feature and let RocksDB allocate memory independently per ColumnFamily (one per state per operator). This offers expert users ultimately more fine grained control over RocksDB, but means that users need to take care themselves that the overall memory consumption does not exceed the limits of the environment.
Managed Memory for RocksDB: This feature is active by default and can be (de)activated via the state.backend.rocksdb.memory.managed configuration key. Flink does not directly manage RocksDB's native memory allocations, but configures RocksDB in a certain way to ensure it uses exactly as much memory as Flink has for its managed memory budget. This is done on a per-slot level (managed memory is accounted per slot). To set the total memory usage of RocksDB instance(s), Flink leverages a shared cache and write buffer manager among all instances in a single slot. The shared cache will place an upper limit on the three components that use the majority of memory in RocksDB: block cache, index and bloom filters, and MemTables. For advanced tuning, Flink also provides two parameters to control the division of memory between the write path (MemTable) and read path (index & filters, remaining cache). When you see that RocksDB performs badly due to lack of write buffer memory (frequent flushes) or cache misses, you can use these parameters to redistribute the memory.
- state.backend.rocksdb.memory.write-buffer-ratio, by default 0.5, which means 50% of the given memory would be used by write buffer manager.
- state.backend.rocksdb.memory.high-prio-pool-ratio, by default 0.1, which means 10% of the given memory would be set as high priority for index and filters in shared block cache. We strongly suggest not to set this to zero, to prevent index and filters from competing against data blocks for staying in cache and causing performance issues. Moreover, the L0 level filter and index are pinned into the cache by default to mitigate performance problems.
When the above described mechanism (cache and write buffer manager) is enabled, it will override any customized settings for block caches and write buffers done via PredefinedOptions and RocksDBOptionsFactory.
Expert Mode: To control memory manually, you can set state.backend.rocksdb.memory.managed to false and configure RocksDB via ColumnFamilyOptions. Alternatively, you can use the above mentioned cache/buffer-manager mechanism, but set the memory size to a fixed amount independent of Flink's managed memory size (state.backend.rocksdb.memory.fixed-per-slot or state.backend.rocksdb.memory.fixed-per-tm options). Note that in both cases, users need to ensure on their own that enough memory is available outside the JVM for RocksDB.

### Timers (Heap vs. RocksDB)
Timers are used to schedule actions for later (event-time or processing-time), such as firing a window, or calling back a ProcessFunction. When selecting the RocksDB State Backend, timers are by default also stored in RocksDB. That is a robust and scalable way that lets applications scale to many timers. However, maintaining timers in RocksDB can have a certain cost, which is why Flink provides the option to store timers on the JVM heap instead, even when RocksDB is used to store other states. Heap-based timers can have a better performance when there is a smaller number of timers. Set the configuration option state.backend.rocksdb.timer-service.factory to heap (rather than the default, rocksdb) to store timers on heap. The combination RocksDB state backend with heap-based timers currently does NOT support asynchronous snapshots for the timers state. Other state like keyed state is still snapshotted asynchronously. When using RocksDB state backend with heap-based timers, checkpointing and taking savepoints is expected to fail if there are operators in application that write to raw keyed state. This is only relevant to advanced users who are writing custom stream operators.

### Enabling RocksDB Native Metrics
You can optionally access RockDB's native metrics through Flink's metrics system, by enabling certain metrics selectively. Enabling RocksDB's native metrics may have a negative performance impact on your application.

### Advanced RocksDB Memory Turning
Flink offers sophisticated default memory management for RocksDB that should work for most use-cases. The below mechanisms should mainly be used for expert tuning or trouble shooting.
#### Predefined Per-ColumnFamily Options
With Predefined Options, users can apply some predefined config profiles on each RocksDB Column Family, configuring for example memory use, thread, compaction settings, etc. There is currently one Column Family per each state in each operator. Two ways to select predefined options: Set the option's name in Flink configuration file via state.backend.rocksdb.predefined-options; or set programmatically: EmbeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM). The default value for this option is DEFAULT which translates to PredefinedOptions.DEFAULT. Predefined options set programmatically would override the ones configured via Flink configuration file.
#### Reading Column Family Options from Flink configuration file
RocksDB State Backend picks up all config options defined here. Hence, you can configure low-level Column Family options simply by turning off managed memory for RocksDB and putting the relevant entries in the configuration.
#### Passing Options Factory to RocksDB
To manually control RocksDB's options, you need to configure an RocksDBOptionsFactory. Two ways: Configure options factory class name in the Flink configuration file via state.backend.rocksdb.options-factory; or set programmatically, e.g. EmbeddedRocksDBStateBackend.setRocksDBOptions(new MyOptionsFactory()). Options factory which set programmatically would override the one configured via Flink configuration file, and options factory has a higher priority over the predefined options. RocksDB is a native library that allocates memory directly from the process, and not from the JVM. Any memory you assign to RocksDB will have to be accounted for, typically by decreasing the JVM heap size of the TaskManagers by the same amount. Not doing that may result in YARN/etc terminating the JVM processes for allocating more memory than configured. Example custom ConfigurableOptionsFactory: `[code: public class MyOptionsFactory implements ConfigurableRocksDBOptionsFac ... (1329 chars)]` (Java) / `[code: Still not supported in Python API. ... (34 chars)]` (Python).

## Enabling Changelog
### Introduction
Changelog is a feature that aims to decrease checkpointing time and, therefore, end-to-end latency in exactly-once mode. Most commonly, checkpoint duration is affected by:
- Barrier travel time and alignment, addressed by Unaligned checkpoints and Buffer debloating
- Snapshot creation time (so-called synchronous phase), addressed by asynchronous snapshots (mentioned above)
- Snapshot upload time (asynchronous phase)
Upload time can be decreased by incremental checkpoints. However, most incremental state backends perform some form of compaction periodically, which results in re-uploading the old state in addition to the new changes. In large deployments, the probability of at least one task uploading lots of data tends to be very high in every checkpoint. With Changelog enabled, Flink uploads state changes continuously and forms a changelog. On checkpoint, only the relevant part of this changelog needs to be uploaded. The configured state backend is snapshotted in the background periodically. Upon successful upload, the changelog is truncated. As a result, asynchronous phase duration is reduced, as well as synchronous phase - because no data needs to be flushed to disk. In particular, long-tail latency is improved. At the same time, some other benefits could be got:
- More Stable and Lower End-to-end Latency.
- Less Data Replay after Failover.
- More Stable Utilization of Resources.
However, resource usage is higher: more files are created on DFS; more IO bandwidth is used to upload state changes; more CPU used to serialize state changes; more memory used by Task Managers to buffer state changes. It is worth noting that changelog adds a small amount of daily CPU and network bandwidth resources, but reduces peak CPU and network bandwidth usage. Recovery time is another thing to consider. Depending on the state.changelog.periodic-materialize.interval setting, the changelog can become lengthy and replaying it may take more time. However, recovery time combined with checkpoint duration will likely still be lower than in non-changelog setups, providing lower end-to-end latency even in failover case. However, it's also possible that the effective recovery time will increase, depending on the actual ratio of the aforementioned times. For more details, see FLIP-158.
### Installation
Changelog JARs are included into the standard Flink distribution. Make sure to add the necessary filesystem plugins.
### Configuration
Example configuration in YAML: `[code: state.changelog.enabled: true ... (227 chars)]`. Please keep the following defaults (see limitations): `[code: execution.checkpointing.max-concurrent-checkpoints: 1 ... (53 chars)]`. Changelog can also be enabled or disabled per job programmatically: `[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (125 chars)]` (Java) / `[code: env = StreamExecutionEnvironment.get_execution_environment() ... (100 chars)]` (Python).
### Monitoring
Available metrics are listed here. If a task is backpressured by writing state changes, it will be shown as busy (red) in the UI.
### Upgrading existing jobs
Enabling Changelog: Resuming from both savepoints and checkpoints is supported: given an existing non-changelog job, take either a savepoint or a checkpoint, alter configuration (enable Changelog), resume from the taken snapshot. Disabling Changelog: same in reverse.
### Limitations
- At most one concurrent checkpoint
- As of Flink 1.15, only filesystem changelog implementation is available
- NO_CLAIM mode not supported

## Migrating from Legacy Backends
Beginning in Flink 1.13, the community reworked its public state backend classes to help users better understand the separation of local state storage and checkpoint storage. This change does not affect the runtime implementation or characteristics of Flink's state backend or checkpointing process; it is simply to communicate intent better. Users can migrate existing applications to use the new API without losing any state or consistency.
### MemoryStateBackend
The legacy MemoryStateBackend is equivalent to using HashMapStateBackend and JobManagerCheckpointStorage. Config: `[code: state.backend: hashmap ... (188 chars)]`; Code: `[code: Configuration config = new Configuration(); ... (191 chars)]` (Java) / `[code: config = Configuration() ... (209 chars)]` (Python).
### FsStateBackend
The legacy FsStateBackend is equivalent to using HashMapStateBackend and FileSystemCheckpointStorage. Config: `[code: state.backend: hashmap ... (240 chars)]`; Code: `[code: Configuration config = new Configuration(); ... (486 chars)]` (Java) / `[code: config = Configuration() ... (495 chars)]` (Python).
### RocksDBStateBackend
The legacy RocksDBStateBackend is equivalent to using EmbeddedRocksDBStateBackend and FileSystemCheckpointStorage. Config: `[code: state.backend: rocksdb ... (240 chars)]`; Code: `[code: Configuration config = new Configuration(); ... (591 chars)]` (Java) / `[code: config = Configuration() ... (599 chars)]` (Python).
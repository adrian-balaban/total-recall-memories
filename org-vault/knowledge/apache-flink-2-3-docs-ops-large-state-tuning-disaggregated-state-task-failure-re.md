---
title: Apache Flink 2.3 docs — Ops (large state tuning / disaggregated state / task failure recovery / metrics / traces)
tags: [org, flink, flink-2.3, docs, ops, state, large-state, disaggregated-state, task-failure, metrics, traces, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:09:05.160Z'
updated: '2026-07-08T05:09:05.160Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 docs — Ops/state batch B + metrics/traces (5 pages: large_state_tuning, disaggregated_state, task_failure_recovery, metrics, traces). Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/* via chrome-devtools in-browser fetch. **WHY this matters:** this is the operational reference for tuning checkpoints at scale, the new Flink 2.0 disaggregated-state model (ForSt + State V2 + async I/O, Rust rewrite roadmap), restart/failover strategies, the full system-metrics catalog, and the tracing system — the day-to-day ops and SRE reference for running large-state Flink jobs. Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/large_state_tuning/
# Tuning Checkpoints and Large State

This page gives a guide how to configure and tune applications that use large state.

## Overview
For Flink applications to run reliably at large scale, two conditions must be fulfilled:
- The application needs to be able to take checkpoints reliably
- The resources need to be sufficient catch up with the input data streams after a failure
The first sections discuss how to get well performing checkpoints at scale. The last section explains some best practices concerning planning how many resources to use.

## Monitoring State and Checkpoints
The easiest way to monitor checkpoint behavior is via the UI's checkpoint section. The two numbers (both exposed via Task level metrics and in the web interface) that are of particular interest when scaling up checkpoints are:
- The time until operators receive their first checkpoint barrier. When the time to trigger the checkpoint is constantly very high, it means that the checkpoint barriers need a long time to travel from the source to the operators. That typically indicates that the system is operating under a constant backpressure.
- The alignment duration, which is defined as the time between receiving first and the last checkpoint barrier. During unaligned exactly-once checkpoints and at-least-once checkpoints subtasks are processing all of the data from the upstream subtasks without any interruptions. However with aligned exactly-once checkpoints, the channels that have already received a checkpoint barrier are blocked from sending further data until all of the remaining channels catch up and receive theirs checkpoint barriers (alignment time).
Both of those values should ideally be low - higher amounts means that checkpoint barriers traveling through the job graph slowly, due to some back-pressure (not enough resources to process the incoming records). This can also be observed via increased end-to-end latency of processed records. Note that those numbers can be occasionally high in the presence of a transient backpressure, data skew, or network issues. Unaligned checkpoints can be used to speed up the propagation time of the checkpoint barriers. However please note, that this does not solve the underlying problem that's causing the backpressure in the first place (and end-to-end records latency will remain high).

## Tuning Checkpointing
Checkpoints are triggered at regular intervals that applications can configure. When a checkpoint takes longer to complete than the checkpoint interval, the next checkpoint is not triggered before the in-progress checkpoint completes. By default the next checkpoint will then be triggered immediately once the ongoing checkpoint completes. When checkpoints end up frequently taking longer than the base interval, the system is constantly taking checkpoints (new ones are started immediately once ongoing once finish). That can mean that too many resources are constantly tied up in checkpointing and that the operators make too little progress. This behavior has less impact on streaming applications that use asynchronously checkpointed state, but may still have an impact on overall application performance. To prevent such a situation, applications can define a minimum duration between checkpoints: StreamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(milliseconds). This duration is the minimum time interval that must pass between the end of the latest checkpoint and the beginning of the next. Note: Applications can be configured (via the CheckpointConfig) to allow multiple checkpoints to be in progress at the same time. For applications with large state in Flink, this often ties up too many resources into the checkpointing. When a savepoint is manually triggered, it may be in process concurrently with an ongoing checkpoint.

## Tuning RocksDB or ForSt
The state storage workhorse of many large scale Flink streaming applications is the RocksDB State Backend. The backend scales well beyond main memory and reliably stores large keyed state. If you are handling very large state, even exceeding the local disk space of the TaskManagers, you may want to consider using the disaggregated state store ForStStateBackend. This backend stores the state in a separate storage system, such as HDFS or S3, and only keeps the state metadata and cache in the TaskManagers. And the State API V2 is also recommended to cooperate with ForStStateBackend for large state applications. RocksDB's performance can vary with configuration, this section outlines some best-practices for tuning jobs that use the RocksDB State Backend. The design of ForSt is very similar to RocksDB, and the configurable options are almost the same, so you can refer to following sections to configure ForSt. The following article is introduced from the perspective of RocksDB. If you want to configure ForSt in a similar way, you need to use the corresponding configuration under ForSt.

### Incremental Checkpoints
When it comes to reducing the time that checkpoints take, activating incremental checkpoints should be one of the first considerations. Incremental checkpoints can dramatically reduce the checkpointing time in comparison to full checkpoints, because incremental checkpoints only record the changes compared to the previous completed checkpoint, instead of producing a full, self-contained backup of the state backend. See Incremental Checkpoints in RocksDB for more background information.

### Timers in RocksDB or on JVM Heap
Timers are stored in RocksDB by default, which is the more robust and scalable choice. When performance-tuning jobs that have few timers only (no windows, not using timers in ProcessFunction), putting those timers on the heap can increase performance. Use this feature carefully, as heap-based timers may increase checkpointing times and naturally cannot scale beyond memory. See this section for details on how to configure heap-based timers.

### Tuning RocksDB Memory
The performance of the RocksDB State Backend much depends on the amount of memory that it has available. To increase performance, adding memory can help a lot, or adjusting to which functions memory goes. By default, the RocksDB State Backend uses Flink's managed memory budget for RocksDBs buffers and caches (state.backend.rocksdb.memory.managed: true). To tune memory-related performance issues, the following steps may be helpful:
- The first step to try and increase performance should be to increase the amount of managed memory. This usually improves the situation a lot, without opening up the complexity of tuning low-level RocksDB options. Especially with large container/process sizes, much of the total memory can typically go to RocksDB, unless the application logic requires a lot of JVM heap itself. The default managed memory fraction (0.4) is conservative and can often be increased when using TaskManagers with multi-GB process sizes.
- The number of write buffers in RocksDB depends on the number of states you have in your application (states across all operators in the pipeline). Each state corresponds to one ColumnFamily, which needs its own write buffers. Hence, applications with many states typically need more memory for the same performance.
- You can try and compare the performance of RocksDB with managed memory to RocksDB with per-column-family memory by setting state.backend.rocksdb.memory.managed: false. Especially to test against a baseline (assuming no- or gracious container memory limits) or to test for regressions compared to earlier versions of Flink, this can be useful. Compared to the managed memory setup (constant memory pool), not using managed memory means that RocksDB allocates memory proportional to the number of states in the application (memory footprint changes with application changes). As a rule of thumb, the non-managed mode has (unless ColumnFamily options are applied) an upper bound of roughly "140MB * num-states-across-all-tasks * num-slots". Timers count as state as well!
- If your application has many states and you see frequent MemTable flushes (write-side bottleneck), but you cannot give more memory you can increase the ratio of memory going to the write buffers (state.backend.rocksdb.memory.write-buffer-ratio).
- An advanced option (expert mode) to reduce the number of MemTable flushes in setups with many states, is to tune RocksDB's ColumnFamily options (arena block size, max background flush threads, etc.) via a RocksDBOptionsFactory: `[code: public class MyOptionsFactory implements ConfigurableRocksDBOptionsFac ... (837 chars)]`

## Capacity Planning
The basic rules of thumb for capacity planning are:
- Normal operation should have enough capacity to not operate under constant back pressure.
- Provision some extra resources on top of the resources needed to run the program back-pressure-free during failure-free time. These resources are needed to "catch up" with the input data that accumulated during the time the application was recovering. How much that should be depends on how long recovery operations usually take (which depends on the size of the state that needs to be loaded into the new TaskManagers on a failover) and how fast the scenario requires failures to recover. Important: The base line should to be established with checkpointing activated, because checkpointing ties up some amount of resources (such as network bandwidth).
- Temporary back pressure is usually okay, and an essential part of execution flow control during load spikes, during catch-up phases, or when external systems (that are written to in a sink) exhibit temporary slowdown.
- Certain operations (like large windows) result in a spiky load for their downstream operators: In the case of windows, the downstream operators may have little to do while the window is being built, and have a load to do when the windows are emitted. The planning for the downstream parallelism needs to take into account how much the windows emit and how fast such a spike needs to be processed.
Important: In order to allow for adding resources later, make sure to set the maximum parallelism of the data stream program to a reasonable number. The maximum parallelism defines how high you can set the programs parallelism when re-scaling the program (via a savepoint). Flink's internal bookkeeping tracks parallel state in the granularity of max-parallelism-many key groups. Flink's design strives to make it efficient to have a very high value for the maximum parallelism, even if executing the program with a low parallelism.

## Compression
Flink offers optional compression (default: off) for all checkpoints and savepoints. Currently, compression always uses the snappy compression algorithm (version 1.1.10.x). Compression works on the granularity of key-groups in keyed state, i.e. each key-group can be decompressed individually, which is important for rescaling. Compression can be activated through the ExecutionConfig: `[code: ExecutionConfig executionConfig = new ExecutionConfig(); ... (105 chars)]`. Note The compression option has no impact on incremental snapshots, because they are using RocksDB's internal format which is always using snappy compression out of the box.

## Task-Local Recovery
### Motivation
In Flink's checkpointing, each task produces a snapshot of its state that is then written to a distributed store. Each task acknowledges a successful write of the state to the job manager by sending a handle that describes the location of the state in the distributed store. The job manager, in turn, collects the handles from all tasks and bundles them into a checkpoint object. In case of recovery, the job manager opens the latest checkpoint object and sends the handles back to the corresponding tasks, which can then restore their state from the distributed storage. Using a distributed storage to store state has two important advantages. First, the storage is fault tolerant and second, all state in the distributed store is accessible to all nodes and can be easily redistributed (e.g. for rescaling). However, using a remote distributed store has also one big disadvantage: all tasks must read their state from a remote location, over the network. In many scenarios, recovery could reschedule failed tasks to the same task manager as in the previous run, but we still have to read remote state. This can result in long recovery time for large states, even if there was only a small failure on a single machine.

### Approach
Task-local state recovery targets exactly this problem of long recovery time and the main idea is the following: for every checkpoint, each task does not only write task states to the distributed storage, but also keep a secondary copy of the state snapshot in a storage that is local to the task (e.g. on local disk or in memory). Notice that the primary store for snapshots must still be the distributed store, because local storage does not ensure durability under node failures and also does not provide access for other nodes to redistribute state, this functionality still requires the primary copy. However, for each task that can be rescheduled to the previous location for recovery, we can restore state from the secondary, local copy and avoid the costs of reading the state remotely. Given that many failures are not node failures and node failures typically only affect one or very few nodes at a time, it is very likely that in a recovery most tasks can return to their previous location and find their local state intact. This is what makes local recovery effective in reducing recovery time. Please note that this can come at some additional costs per checkpoint for creating and storing the secondary local state copy, depending on the chosen state backend and checkpointing strategy. For example, in most cases the implementation will simply duplicate the writes to the distributed store to a local file.

### Relationship of primary (distributed store) and secondary (task-local) state snapshots
Task-local state is always considered a secondary copy, the ground truth of the checkpoint state is the primary copy in the distributed store. This has implications for problems with local state during checkpointing and recovery:
- For checkpointing, the primary copy must be successful and a failure to produce the secondary, local copy will not fail the checkpoint. A checkpoint will fail if the primary copy could not be created, even if the secondary copy was successfully created.
- Only the primary copy is acknowledged and managed by the job manager, secondary copies are owned by task managers and their life cycles can be independent from their primary copies. For example, it is possible to retain a history of the 3 latest checkpoints as primary copies and only keep the task-local state of the latest checkpoint.
- For recovery, Flink will always attempt to restore from task-local state first, if a matching secondary copy is available. If any problem occurs during the recovery from the secondary copy, Flink will transparently retry to recover the task from the primary copy. Recovery only fails, if primary and the (optional) secondary copy failed. In this case, depending on the configuration Flink could still fall back to an older checkpoint.
- It is possible that the task-local copy contains only parts of the full task state (e.g. exception while writing one local file). In this case, Flink will first try to recover local parts locally, non-local state is restored from the primary copy. Primary state must always be complete and is a superset of the task-local state.
- Task-local state can have a different format than the primary state, they are not required to be byte identical. For example, it could be even possible that the task-local state is an in-memory consisting of heap objects, and not stored in any files.
- If a task manager is lost, the local state from all its task is lost.

### Configuring task-local recovery
Task-local recovery is deactivated by default and can be activated through Flink's configuration with the key state.backend.local-recovery as specified in CheckpointingOptions.LOCAL_RECOVERY. The value for this setting can either be true to enable or false (default) to disable local recovery. Note that unaligned checkpoints currently do not support task-local recovery.

### Details on task-local recovery for different state backends
Limitation: Currently, task-local recovery only covers keyed state backends. Keyed state is typically by far the largest part of the state. In the near future, we will also cover operator state and timers. The following state backends can support task-local recovery.
- HashMapStateBackend: task-local recovery is supported for keyed state. The implementation will duplicate the state to a local file. This can introduce additional write costs and occupy local disk space. In the future, we might also offer an implementation that keeps task-local state in memory.
- EmbeddedRocksDBStateBackend: task-local recovery is supported for keyed state. For full checkpoints, state is duplicated to a local file. This can introduce additional write costs and occupy local disk space. For incremental snapshots, the local state is based on RocksDB's native checkpointing mechanism. This mechanism is also used as the first step to create the primary copy, which means that in this case no additional cost is introduced for creating the secondary copy. We simply keep the native checkpoint directory around instead of deleting it after uploading to the distributed store. This local copy can share active files with the working directory of RocksDB (via hard links), so for active files also no additional disk space is consumed for task-local recovery with incremental snapshots. Using hard links also means that the RocksDB directories must be on the same physical device as all the configure local recovery directories that can be used to store local state, or else establishing hard links can fail (see FLINK-10954). Currently, this also prevents using local recovery when RocksDB directories are configured to be located on more than one physical device.

### Allocation-preserving scheduling
Task-local recovery assumes allocation-preserving task scheduling under failures, which works as follows. Each task remembers its previous allocation and requests the exact same slot to restart in recovery. If this slot is not available, the task will request a new, fresh slot from the resource manager. This way, if a task manager is no longer available, a task that cannot return to its previous location will not drive other recovering tasks out of their previous slots. Our reasoning is that the previous slot can only disappear when a task manager is no longer available, and in this case some tasks have to request a new slot anyways. With our scheduling strategy we give the maximum number of tasks a chance to recover from their local state and avoid the cascading effect of tasks stealing their previous slots from one another.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/disaggregated_state/
# Disaggregated State Management

## Overview
For the first ten years of Flink, the state management is based on memory or local disk of the TaskManager. This approach works well for most use cases, but it has some limitations:
- Local Disk Constraints: The state size is limited by the memory or disk size of the TaskManager.
- Spiky Resource Usage: The local state model triggers periodic CPU and network I/O bursts during checkpointing or SST files compaction.
- Heavy Recovery: State needs to be downloaded during recovery. The recovery time is proportional to the state size, which can be slow for large state sizes.
In Flink 2.0, we introduced the disaggregated state management. This feature allows users to store the state in external storage systems like S3, HDFS, etc. This is useful when the state size is extremely large. It could be used to store the state in a more cost-effective way, or to persist or recovery the state in a more lightweight way. The benefits of disaggregated state management are:
- Unlimited State Size: The state size is only limited by the external storage system.
- Stable Resource Usage: The state is stored in external storage, thus the checkpoint could be very lightweight. And the SST files compaction could be done remotely (TODO).
- Fast Recovery: No need to download the state during recovery. The recovery time is independent of the state size.
- Flexible: Users can easily choose different external storage systems or I/O performance levels, or scale the storage based on their requirements without change their hardware.
- Cost-effective: External storage are usually cheaper than local disk. Users can flexibly adjust computing resources and storage resources independently if there is any bottleneck.
The disaggregated state management contains three parts:
- ForSt State Backend: A state backend that stores the state in external storage systems. It can also leverage the local disk for caching and buffering. The asynchronous I/O model is used to read and write the state. For more details, see ForSt State Backend.
- New State APIs: The new state APIs (State V2) are introduced to perform asynchronous state reads and writes, which is essential for overcoming the high network latency when accessing the disaggregated state. For more details, see New State APIs.
- SQL Support: Many SQL operators are rewritten to support the disaggregated state management and asynchronous state access. User can easily enable these by setting the configuration.
Disaggregated state and asynchronous state access are encouraged for large state. However, when the state size is small, the local state management with synchronous state access is a better choice. The disaggregated state management is still in experimental state. We are working on improving the performance and stability of this feature. The APIs and configurations may change in future release.

## Quick Start
### For SQL Jobs
To enable the disaggregated state management in SQL jobs, you can set the following configurations: `[code: state.backend.type: forst ... (402 chars)]`. Thus, you could leverage the disaggregated state management and asynchronous state access in your SQL jobs. We haven't implemented the full support for the asynchronous state access in SQL yet. If the SQL operators you are using are not supported, the operator will fall back to the synchronous state implementation automatically. The performance may not be optimal in this case. The supported stateful operators are:
- Rank (Top1, Append TopN)
- Row Time Deduplicate
- Aggregate (without distinct)
- Join
- Window Join
- Tumble / Hop / Cumulative Window Aggregate

### For DataStream Jobs
To enable the disaggregated state management in DataStream jobs, firstly you should use the ForStStateBackend. Configure via code in per-job mode: `[code: Configuration config = new Configuration(); ... (280 chars)]`. Or configure via config.yaml: `[code: state.backend.type: forst ... (187 chars)]`. Then, you should write your datastream jobs with the new state APIs. For more details, see State V2.

## Advanced Tuning Options
### Tuning ForSt State Backend
The ForStStateBackend has many configurations to tune the performance. The design of ForSt is very similar to RocksDB, and the configurable options are almost the same, so you can refer to large state tuning to tune the ForSt state backend. Besides that, the following sections introduce some unique configurations for ForSt.

#### ForSt Primary Storage Location
By default, ForSt stores the state in the checkpoint directory. In this case, ForSt could perform lightweight checkpoints and fast recovery. However, users may want to store the state in a different location, e.g., a different bucket in S3. You can set the following configuration to specify the primary storage location: `[code: state.backend.forst.primary-dir: s3://your-bucket/forst-state ... (61 chars)]`. Note: If you set this configuration, you may not be able to leverage the lightweight checkpoint and fast recovery, since the ForSt will perform file copy between the primary storage location and the checkpoint directory during checkpointing and recovery.

#### ForSt Local Storage Location
By default, ForSt will ONLY disaggregate state when asynchronous APIs (State V2) are used. When using synchronous state APIs in DataStream and SQL jobs, ForSt will only serve as local state store. Since a job may contain multiple ForSt instances with mixed API usage, synchronous local state access along with asynchronous remote state access could help achieve better overall throughput. If you want the operators with synchronous state APIs to store state in remote, the following configuration will help: `[code: state.backend.forst.sync.enforce-local: false ... (45 chars)]`. And you can specify the local storage location via: `[code: state.backend.forst.local-dir: path-to-local-dir ... (48 chars)]`.

#### ForSt File Cache
ForSt uses the local disk for caching and buffering. The granularity of the cache is whole file. This is enabled by default, except when the primary storage location is set to local. There are two capacity limit policies for the cache:
- Size-based: The cache will evict the oldest files when the cache size exceeds the limit.
- Reserved-based: The cache will evict the oldest files when the reserved space on disk (the disk where cache directory is) is not enough.
Corresponding configurations are: `[code: state.backend.forst.cache.size-based-limit: 1GB ... (92 chars)]`. Those can take effect together. If so, the cache will evict the oldest files when the cache size exceeds either the size-based limit or the reserved size limit. One can also specify the cache directory via: `[code: state.backend.forst.cache.dir: /tmp/forst-cache ... (47 chars)]`.

#### ForSt Asynchronous Threads
ForSt uses asynchronous I/O to read and write the state. There are three types of threads:
- Coordinator thread: The thread that coordinates the asynchronous read and write.
- Read thread: The thread that reads the state asynchronously.
- Write thread: The thread that writes the state asynchronously.
The number of asynchronous threads is configurable. Typically, you don't need to adjust these values since the default values are good enough for most cases. In case for special needs, you can set the following configuration to specify the number of asynchronous threads:
- state.backend.forst.executor.read-io-parallelism: The number of asynchronous threads for read. Default is 3.
- state.backend.forst.executor.write-io-parallelism: The number of asynchronous threads for write. Default is 1.
- state.backend.forst.executor.inline-write: Whether to inline the write operation in the coordinator thread. Default is true. Setting this to false will raise the CPU usage.
- state.backend.forst.executor.inline-coordinator: Whether to let task thread be the coordinator thread. Default is true. Setting this to false will raise the CPU usage.
ForStStateBackend utilizes ForSt as its underlying database core. The current version of ForSt is forked from frocksdb, architected as an embedded database core specifically for Flink local state management. While transitioning the db toward a disaggregated architecture, we encountered significant architectural and engineering constraints within the existing framework. To address these challenges, community members are now working on a next-generation, cloud-native ForSt DB written in Rust.
#### Key Advantages of the New ForSt DB:
- Architectural Simplicity: A streamlined codebase designed for high extensibility.
- Stream-Native Design: Optimized specifically for the unique demands of large-scale stream processing.
- Cloud-Native: Built from the ground up to support disaggregation.
#### Roadmap & Maintenance:
- Release Schedule: The first stable open-source version is projected for later 2026 (August, optimistically)
- Deprecation Notice: As we shift our focus to the new Rust-based implementation, the frocksdb-based version of ForSt is no longer under active development and will be phased out (deprecated) following the new release.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/task_failure_recovery/
# Task Failure Recovery

When a task failure happens, Flink needs to restart the failed task and other affected tasks to recover the job to a normal state. Restart strategies and failover strategies are used to control the task restarting. Restart strategies decide whether and when the failed/affected tasks can be restarted. Failover strategies decide which tasks should be restarted to recover the job.

## Restart Strategies
The cluster can be started with a default restart strategy which is always used when no job specific restart strategy has been defined. In case that the job is submitted with a restart strategy, this strategy overrides the cluster's default setting. The default restart strategy is set via Flink configuration file. The configuration parameter restart-strategy.type defines which strategy is taken. If checkpointing is not enabled, the no restart strategy is used. If checkpointing is activated and the restart strategy has not been configured, the exponential-delay restart strategy and the default values of exponential-delay related config options will be used. [table: 2 rows]
Each restart strategy comes with its own set of parameters which control its behaviour. These values are also set in the configuration file. The following example shows how we can set a fixed delay restart strategy for our job. In case of a failure the system tries to restart the job 3 times and waits 10 seconds in-between successive restart attempts. `[code: Configuration config = new Configuration(); ... (415 chars)]` (Java) / `[code: config = Configuration() ... (318 chars)]` (Python).

### Fixed Delay Restart Strategy
The fixed delay restart strategy attempts a given number of times to restart the job. If the maximum number of attempts is exceeded, the job eventually fails. In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time. `[code: restart-strategy.type: fixed-delay ... (34 chars)]`. [table: 3 rows]. Example: `[code: restart-strategy.fixed-delay.attempts: 3 ... (81 chars)]`. Can also be set programmatically.

### Exponential Delay Restart Strategy
In-between two consecutive restart attempts, the exponential delay restart strategy keeps exponentially increasing until the maximum number is reached. Then, it keeps the delay at the maximum number. When the job executes correctly, the exponential delay value resets after some time; this threshold is configurable. `[code: restart-strategy.type: exponential-delay ... (40 chars)]`. [table: 7 rows]. Example: `[code: restart-strategy.exponential-delay.initial-backoff: 10 s ... (359 chars)]`.
#### Example
`[code: restart-strategy.exponential-delay.initial-backoff: 1 s ... (277 chars)]`
- initial-backoff = 1s: when an exception occurs for the first time, the job will be delayed for 1 second before retrying.
- backoff-multiplier = 2: when the job has continuous exceptions, the delay time is doubled each time.
- max-backoff = 10 s: the retry delay is at most 10 seconds.
Behavior: 1st retry 1s, 2nd 2s, 3rd 4s, 4th 8s, 5th 10s (capped). After reaching max-backoff, delay stays at 10s. `[code: restart-strategy.exponential-delay.jitter-factor: 0.1 ... (187 chars)]`
- jitter-factor = 0.1: each delay time will be added or subtracted by a random value, ratio range within 0.1. 3rd retry: 3.6s–4.4s; 4th retry: 7.2s–8.8s. Random values prevent multiple jobs restarting at the same time — not recommended to set jitter-factor to 0 in production.
- attempts-before-reset-backoff = 8: if the job still encounters exceptions after 8 consecutive retries, it will fail (no more retries).
- reset-backoff-threshold = 6 min: when the job runs for 6 minutes without an exception, the delay time and retry counter will be reset.

### Failure Rate Restart Strategy
The failure rate restart strategy restarts job after failure, but when failure rate (failures per time interval) is exceeded, the job eventually fails. In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time. `[code: restart-strategy.type: failure-rate ... (35 chars)]`. [table: 4 rows]. `[code: restart-strategy.failure-rate.max-failures-per-interval: 3 ... (159 chars)]`.

### No Restart Strategy
The job fails directly and no restart is attempted. `[code: restart-strategy.type: none ... (27 chars)]`.

### Fallback Restart Strategy
The cluster defined restart strategy is used. This is helpful for streaming programs which enable checkpointing. By default, the exponential delay restart strategy is chosen if there is no other restart strategy defined.

### Default restart strategy
When Checkpoint is enabled and the user does not specify a restart strategy, Exponential delay restart strategy is the current default restart strategy. We strongly recommend Flink users to use the exponential delay restart strategy because by using this strategy, jobs can be retried quickly when exceptions occur occasionally, and avalanches of external components can be avoided when exceptions occur frequently. The reasons: all restart strategies delay some time to avoid frequent retries; delay time for all except exponential is fixed; too-short delay causes avalanche of external services (e.g. Kafka cluster crashes → many Flink jobs retry simultaneously); too-long delay reduces availability; exponential delay increases exponentially until max; short initial delay = fast occasional retries; reduces frequency under frequent failures; jitter-factor lets identically-configured jobs restart at different times.

## Failover Strategies
Flink supports different failover strategies which can be configured via jobmanager.execution.failover-strategy. [table: 3 rows]
### Restart All Failover Strategy
This strategy restarts all tasks in the job to recover from a task failure.
### Restart Pipelined Region Failover Strategy
This strategy groups tasks into disjoint regions. When a task failure is detected, this strategy computes the smallest set of regions that must be restarted to recover from the failure. For some jobs this can result in fewer tasks that will be restarted compared to the Restart All Failover Strategy. A region is a set of tasks that communicate via pipelined data exchanges. That is, batch data exchanges denote the boundaries of a region. DataStream/Table/SQL job data exchanges are determined by the ExecutionMode, which can be set through ExecutionConfig, which are pipelined in Streaming Mode, are batched by default in Batch Mode. The regions to restart are decided as below:
- The region containing the failed task will be restarted.
- If a result partition is not available while it is required by a region that will be restarted, the region producing the result partition will be restarted as well.
- If a region is to be restarted, all of its consumer regions will also be restarted. This is to guarantee data consistency because nondeterministic processing or partitioning can result in different partitions.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/
# Metrics

Flink exposes a metric system that allows gathering and exposing metrics to external systems.

## Registering metrics
You can access the metric system from any user function that extends RichFunction by calling getRuntimeContext().getMetricGroup(). This method returns a MetricGroup object on which you can create and register new metrics.

### Metric types
Flink supports Counters, Gauges, Histograms and Meters.
#### Counter
A Counter is used to count something. The current value can be in- or decremented using inc()/inc(long n) or dec()/dec(long n). You can create and register a Counter by calling counter(String name) on a MetricGroup. `[code:  ... (362 chars)]` (Java) / `[code:  ... (326 chars)]` (Python). Alternatively you can also use your own Counter implementation: `[code:  ... (389 chars)]` (Java) / `[code: Still not supported in Python API. ... (34 chars)]` (Python).
#### Gauge
A Gauge provides a value of any type on demand. In order to use a Gauge you must first create a class that implements the org.apache.flink.metrics.Gauge interface. There is no restriction for the type of the returned value. You can register a gauge by calling gauge(String name, Gauge gauge) on a MetricGroup. `[code:  ... (474 chars)]` (Java) / `[code:  ... (349 chars)]` (Python). Note that reporters will turn the exposed object into a String, which means that a meaningful toString() implementation is required.
#### Histogram
A Histogram measures the distribution of long values. You can register one by calling histogram(String name, Histogram histogram) on a MetricGroup. `[code: public class MyMapper extends RichMapFunction<Long, Long> { ... (392 chars)]` (Java) / `[code: Still not supported in Python API. ... (34 chars)]` (Python). Flink does not provide a default implementation for Histogram, but offers a Wrapper that allows usage of Codahale/DropWizard histograms. To use this wrapper add the following dependency: `[code: <dependency> ... (155 chars)]`. You can then register a Codahale/DropWizard histogram like this: `[code: public class MyMapper extends RichMapFunction<Long, Long> { ... (559 chars)]` (Java).
#### Meter
A Meter measures an average throughput. An occurrence of an event can be registered with the markEvent() method. Occurrence of multiple events at the same time can be registered with markEvent(long n) method. You can register a meter by calling meter(String name, Meter meter) on a MetricGroup. `[code: public class MyMapper extends RichMapFunction<Long, Long> { ... (362 chars)]` (Java) / `[code:  ... (424 chars)]` (Python). Flink offers a Wrapper that allows usage of Codahale/DropWizard meters. Add dependency: `[code: <dependency> ... (155 chars)]`.

## Scope
Every metric is assigned an identifier and a set of key-value pairs under which the metric will be reported. The identifier is based on 3 components: a user-defined name when registering the metric, an optional user-defined scope and a system-provided scope. For example, if A.B is the system scope, C.D the user scope and E the name, then the identifier for the metric will be A.B.C.D.E. You can configure which delimiter to use for the identifier (default: .) by setting the metrics.scope.delimiter key in Flink configuration file.

### User Scope
You can define a user scope by calling MetricGroup#addGroup(String name), MetricGroup#addGroup(int name) or MetricGroup#addGroup(String key, String value). These methods affect what MetricGroup#getMetricIdentifier and MetricGroup#getScopeComponents return.

### System Scope
The system scope contains context information about the metric, for example in which task it was registered or what job that task belongs to. Which context information should be included can be configured by setting the following keys in Flink configuration file. Each of these keys expect a format string that may contain constants (e.g. "taskmanager") and variables (e.g. "<task_id>") which will be replaced at runtime.
- metrics.scope.jm — Default: <host>.jobmanager — Applied to all metrics that were scoped to a job manager.
- metrics.scope.jm-job — Default: <host>.jobmanager.<job_name> — Applied to all metrics that were scoped to a job manager and job.
- metrics.scope.tm — Default: <host>.taskmanager.<tm_id> — Applied to all metrics that were scoped to a task manager.
- metrics.scope.tm-job — Default: <host>.taskmanager.<tm_id>.<job_name> — Applied to all metrics that were scoped to a task manager and job.
- metrics.scope.task — Default: <host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index> — Applied to all metrics that were scoped to a task.
- metrics.scope.operator — Default: <host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index> — Applied to all metrics that were scoped to an operator.
There are no restrictions on the number or order of variables. Variables are case sensitive. The default scope for operator metrics will result in an identifier akin to localhost.taskmanager.1234.MyJob.MyOperator.0.MyMetric. Note that for this format string an identifier clash can occur should the same job be run multiple times concurrently, which can lead to inconsistent metric data. As such it is advised to either use format strings that provide a certain degree of uniqueness by including IDs (e.g <job_id>) or by assigning unique names to jobs and operators.

### List of all Variables
- JobManager: <host>
- TaskManager: <host>, <tm_id>
- Job: <job_id>, <job_name>
- Task: <task_id>, <task_name>, <task_attempt_id>, <task_attempt_num>, <subtask_index>
- Operator: <operator_id>,<operator_name>, <subtask_index>
Important: For the Batch API, <operator_id> is always equal to <task_id>.

### User Variables
You can define a user variable by calling MetricGroup#addGroup(String key, String value). Important: User variables cannot be used in scope formats.

### Additional Variables for operators
You can define custom variables that will be assigned to all metrics reported by a given operator using Transformation.addMetricVariable. Will assign table_name variable with respective values Foo and Bar to all metrics reported by the KafkaSource, like numRecordsOut or currentOutputWatermark. If supported by your chosen metric reporter, those additional variables will be then converted to labels or tags.

## Reporter
For information on how to set up Flink's metric reporters please take a look at the metric reporters documentation.

## System metrics
By default Flink gathers several metrics that provide deep insights on the current state. This section is a reference of all these metrics. The tables below generally feature 5 columns: Scope, Infix, Metrics, Description, Type. Note that all dots in the infix/metric name columns are still subject to the "metrics.delimiter" setting.
### CPU [table: 3 rows]
### Memory — The memory-related metrics require Oracle's memory management (also included in OpenJDK's Hotspot implementation) to be in place. Some metrics might not be exposed when using other JVM implementations (e.g. IBM's J9). [table: 18 rows]
### File Descriptors [table: 3 rows]
### Threads [table: 2 rows]
### GarbageCollection [table: 4 rows]
### ClassLoader [table: 3 rows]
### Network — Deprecated: use Default shuffle service metrics [table: 13 rows]
### Default shuffle service — Metrics related to data exchange between task executors using netty network communication. [table: 28 rows]
### Cluster [table: 6 rows]
### Availability — The metrics in this table are available for each of the following job states: INITIALIZING, CREATED, RUNNING, RESTARTING, CANCELLING, FAILING. Whether these metrics are reported depends on the metrics.job.status.enable setting. Evolving The semantics of these metrics may change in later releases. [table: 4 rows]
Experimental — While the job is in the RUNNING state the metrics in this table provide additional details on what the job is currently doing. Whether these metrics are reported depends on the metrics.job.status.enable setting. [table: 4 rows] *A job is considered to be deploying tasks when: for streaming jobs, any task is in the DEPLOYING state; for batch jobs, if at least 1 task is in the DEPLOYING state, and there are no INITIALIZING/RUNNING tasks [table: 5 rows]
### Checkpointing — Note that for failed checkpoints, metrics are updated on a best efforts basis and may be not accurate. [table: 19 rows]
### State Access Latency [table: 28 rows]
### State Size [table: 28 rows]
### RocksDB — Certain RocksDB native metrics are available but disabled by default
### ForSt — Certain ForSt native metrics are available but disabled by default. Besides that, we support the following metrics: [table: 7 rows]
### State Changelog — Note that the metrics are only available via reporters. [table: 17 rows]
### IO [table: 48 rows]
### Connectors
#### Kafka Connectors — Please refer to Kafka monitoring.
#### Kinesis Source [table: 10 rows]
#### Kinesis Sink [table: 4 rows]
#### HBase Connectors [table: 2 rows]
### System resources — System resources reporting is disabled by default. When metrics.system-resource is enabled additional metrics listed below will be available on Job- and TaskManager. System resources metrics are updated periodically and they present average values for a configured interval (metrics.system-resource-probing-interval). System resources reporting requires an optional dependency to be present on the classpath (for example placed in Flink's lib directory): com.github.oshi:oshi-core:6.1.5 (licensed under MIT license). Including it's transitive dependencies: net.java.dev.jna:jna-platform:jar:5.10.0, net.java.dev.jna:jna:jar:5.10.0. Failures in this regard will be reported as warning messages like NoClassDefFoundError logged by SystemResourcesMetricsInitializer during the startup.
#### System CPU [table: 14 rows]
#### System memory [table: 5 rows]
#### System network [table: 3 rows]
### Speculative Execution — Metrics below can be used to measure the effectiveness of speculative execution. [table: 3 rows]
### Async State Processing [table: 5 rows]

## End-to-End latency tracking
Flink allows to track the latency of records travelling through the system. This feature is disabled by default. To enable the latency tracking you must set the latencyTrackingInterval to a positive number in either the Flink configuration or ExecutionConfig. At the latencyTrackingInterval, the sources will periodically emit a special record, called a LatencyMarker. The marker contains a timestamp from the time when the record has been emitted at the sources. Latency markers can not overtake regular user records, thus if records are queuing up in front of an operator, it will add to the latency tracked by the marker. Note that the latency markers are not accounting for the time user records spend in operators as they are bypassing them. In particular the markers are not accounting for the time records spend for example in window buffers. Only if operators are not able to accept new records, thus they are queuing up, the latency measured using the markers will reflect that. The LatencyMarkers are used to derive a distribution of the latency between the sources of the topology and each downstream operator. These distributions are reported as histogram metrics. The granularity of these distributions can be controlled in the Flink configuration. For the highest granularity subtask Flink will derive the latency distribution between every source subtask and every downstream subtask, which results in quadratic (in the terms of the parallelism) number of histograms. Currently, Flink assumes that the clocks of all machines in the cluster are in sync. We recommend setting up an automated clock synchronisation service (like NTP) to avoid false latency results. Warning Enabling latency metrics can significantly impact the performance of the cluster (in particular for subtask granularity). It is highly recommended to only use them for debugging purposes.

## State access latency tracking
Flink also allows to track the keyed state access latency for standard Flink state-backends or customized state backends which extending from AbstractStateBackend. This feature is disabled by default. To enable this feature you must set the state.latency-track.keyed-state-enabled to true in the Flink configuration. Once tracking keyed state access latency is enabled, Flink will sample the state access latency every N access, in which N is defined by state.latency-track.sample-interval. This configuration has a default value of 100. A smaller value will get more accurate results but have a higher performance impact since it is sampled more frequently. As the type of this latency metrics is histogram, state.latency-track.history-size will control the maximum number of recorded values in history, which has the default value of 128. Warning Enabling state-access-latency metrics may impact the performance. It is recommended to only use them for debugging purposes.

## State key/value size tracking
Flink also allows to track the keyed state key/value size for standard Flink state-backends or customized state backends which extending from AbstractStateBackend. This feature is disabled by default. To enable this feature you must set the state.size-track.keyed-state-enabled to true in the Flink configuration. Once tracking keyed state key/value size is enabled, Flink will sample the state size every N access, in which N is defined by state.size-track.sample-interval. Default 100. state.size-track.history-size controls max recorded values (default 128). Warning Enabling state-size metrics may impact the performance. If state.ttl is enabled, the size of the value will include the size of the TTL-related timestamp. The value size of AggregatingState is not accounted for because AggregatingState returns a result processed by a user-defined AggregateFunction, whereas currently, only the actual stored data size in the state can be tracked.

## REST API integration
Metrics can be queried through the Monitoring REST API. All endpoints are of the sample form http://hostname:8081/jobmanager/metrics, below we list only the path part of the URLs. Values in angle brackets are variables.
Request metrics for a specific entity:
- /jobmanager/metrics
- /taskmanagers/<taskmanagerid>/metrics
- /jobs/<jobid>/metrics
- /jobs/<jobid>/vertices/<vertexid>/subtasks/<subtaskindex>
Request metrics aggregated across all entities of the respective type:
- /taskmanagers/metrics
- /jobs/metrics
- /jobs/<jobid>/vertices/<vertexid>/subtasks/metrics
- /jobs/<jobid>/vertices/<vertexid>/jm-operator-metrics
Request metrics aggregated over a subset of all entities of the respective type:
- /taskmanagers/metrics?taskmanagers=A,B,C
- /jobs/metrics?jobs=D,E,F
- /jobs/<jobid>/vertices/<vertexid>/subtasks/metrics?subtask=1,2,3
Warning Metric names can contain special characters that you need to escape when querying metrics. For example, "a_+_b" would be escaped to "a_%2B_b". [table: 10 rows] (list of chars to escape)
Request a list of available metrics: GET /jobmanager/metrics `[code: [ ... (60 chars)]`
Request the values for specific (unaggregated) metrics: GET taskmanagers/ABCDE/metrics?get=metric1,metric2 `[code: [ ... (97 chars)]`
Request aggregated values for specific metrics: GET /taskmanagers/metrics?get=metric1,metric2 `[code: [ ... (177 chars)]`
Request specific aggregated values for specific metrics: GET /taskmanagers/metrics?get=metric1,metric2&agg=min,max `[code: [ ... (118 chars)]`

## Dashboard integration
Metrics that were gathered for each task or operator can also be visualized in the Dashboard. On the main page for a job, select the Metrics tab. After selecting one of the tasks in the top graph you can select metrics to display using the Add Metric drop-down menu. Task metrics are listed as <subtask_index>.<metric_name>. Operator metrics are listed as <subtask_index>.<operator_name>.<metric_name>. Each metric will be visualized as a separate graph, with the x-axis representing time and the y-axis the measured value. All graphs are automatically updated every 10 seconds, and continue to do so when navigating to another page. There is no limit as to the number of visualized metrics; however only numeric metrics can be visualized.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/traces/
# Traces

Flink exposes a tracing system that allows gathering and exposing traces to external systems.

## Reporting traces
You can access the tracing system from any user function that extends RichFunction by calling getRuntimeContext().getMetricGroup(). This method returns a MetricGroup object via which you can report a new single trace with tree of spans.

### Reporting single Span
A Span represents some process that happened in Flink at certain point of time for a certain duration, that will be reported to a TraceReporter. To report a Span you can use the MetricGroup#addSpan(SpanBuilder) method. Currently, we support traces with a single tree of spans, but all the children spans have to be reported all at once in one MetricGroup#addSpan call. You can not report child or parent spans independently. `[code: public class MyClass { ... (520 chars)]` (Java) / `[code: Currently reporting Spans from Python is not supported. ... (55 chars)]` (Python).

## Reporter
For information on how to set up Flink's trace reporters please take a look at the trace reporters documentation.

## System traces
Flink reports traces listed below. The tables below generally feature 5 columns: Scope, Name, Attributes, Description.
### Checkpointing and initialization
Flink reports a single span trace for the whole checkpoint and job initialization events once that event reaches a terminal state: COMPLETED or FAILED. [table: 21 rows]
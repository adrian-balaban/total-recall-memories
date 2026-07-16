---
title: Apache Flink 2.3 docs — Ops (events / batch shuffle / JobMaster failure recovery / debugging event-time+classloading / flame graphs / profiler / application profiling / REST API / checkpoint monitoring)
tags: [org, flink, flink-2.3, docs, ops, batch-shuffle, hybrid-shuffle, jobmaster-recovery, classloading, flame-graphs, profiler, rest-api, checkpoint-monitoring, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:12:26.228Z'
updated: '2026-07-08T05:12:26.228Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 docs — Ops batch B (10 pages: events, batch_shuffle, recovery_from_job_master_failure, debugging_event_time, debugging_classloading, flame_graphs, profiler, application_profiling, rest_api, checkpoint_monitoring). Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/* via chrome-devtools in-browser fetch. **WHY this matters:** this is the operational reference for batch shuffle modes (Blocking Hash/Sort + experimental Hybrid), batch-job progress recovery after JobMaster failover (JobEventStore), classloading/dependency-conflict debugging, native flame graphs & async-profiler, and the full REST + checkpoint-monitoring UI — the day-to-day ops and debugging reference for running Flink jobs. Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/events/
# Events
Flink exposes an event reporting system that allows gathering and exposing events to external systems.
## Reporting events
You can access the event system from any user function that extends RichFunction by calling getRuntimeContext().getMetricGroup(). This method returns a MetricGroup object via which you can report a new single event.
### Reporting single Event
An Event represents something that happened in Flink at certain point of time, that will be reported to a TraceReporter. To report an Event you can use the MetricGroup#addEvent(EventBuilder) method.
- Java: `[code: public class MyClass { ... (295 chars)]`
- Python: `[code: Currently reporting Events from Python is not supported. ... (56 chars)]`
## Reporter
For information on how to set up Flink's event reporters please take a look at the event reporters documentation.
## System traces
Flink reports events listed below. Tables feature 5 columns: Scope (what scope reported), Name (name of reported trace), Attributes (names of all attributes reported), Description (what attribute reports). [table: 21 rows]

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/batch/batch_shuffle/
# Batch Shuffle
## Overview
Flink supports a batch execution mode in both DataStream API and Table / SQL for jobs executing across bounded input. In batch execution mode, Flink offers two modes for network exchanges: Blocking Shuffle and Hybrid Shuffle.
- Blocking Shuffle is the default data exchange mode for batch executions. It persists all intermediate data, and can be consumed only after fully produced.
- Hybrid Shuffle is the next generation data exchange mode for batch executions. It persists data more smartly, and allows consuming while being produced. This feature is still experimental and has some known limitations.
## Blocking Shuffle
Unlike the pipeline shuffle used for streaming applications, blocking exchanges persists data to some storage. Downstream tasks then fetch these values via the network. Such an exchange reduces the resources required to execute the job as it does not need the upstream and downstream tasks to run simultaneously.
As a whole, Flink provides two different types of blocking shuffles: Hash shuffle and Sort shuffle.
### Hash Shuffle
The default blocking shuffle implementation for 1.14 and lower, Hash Shuffle, has each upstream task persist its results in a separate file for each downstream task on the local disk of the TaskManager. When the downstream tasks run, they will request partitions from the upstream TaskManager's, which read the files and transmit data via the network.
Hash Shuffle provides different mechanisms for writing and reading files:
- file: Writes files with the normal File IO, reads and transmits files with Netty FileRegion. FileRegion relies on sendfile system call to reduce the number of data copies and memory consumption.
- mmap: Writes and reads files with mmap system call.
- auto: Writes files with the normal File IO, for file reading, it falls back to normal file option on 32 bit machine and use mmap on 64 bit machine. This is to avoid file size limitation of java mmap implementation on 32 bit machine.
The different mechanism could be chosen via TaskManager configurations.
- This option is experimental and might be changed future.
- If SSL is enabled, the file mechanism can not use FileRegion and instead uses an un-pooled buffer to cache data before transmitting. This might cause direct memory OOM. Additionally, since the synchronous file reading might block Netty threads for some time, the SSL handshake timeout needs to be increased to avoid connection reset errors.
- The memory usage of mmap is not accounted for by configured memory limits, but some resource frameworks like Yarn will track this memory usage and kill the container if memory exceeds some threshold.
Hash Shuffle works well for small scale jobs with SSD, but it also has some disadvantages:
- If the job scale is large, it might create too many files, and it requires a large write buffer to write these files at the same time.
- On HDD, when multiple downstream tasks fetch their data simultaneously, it might incur the issue of random IO.
### Sort Shuffle
Sort Shuffle is another blocking shuffle implementation introduced in version 1.13 and it becomes the default blocking shuffle implementation in 1.15. Different from Hash Shuffle, Sort Shuffle writes only one file for each result partition. When the result partition is read by multiple downstream tasks concurrently, the data file is opened only once and shared by all readers. As a result, the cluster uses fewer resources like inode and file descriptors, which improves stability. Furthermore, by writing fewer files and making a best effort to read data sequentially, Sort Shuffle can achieve better performance than Hash Shuffle, especially on HDD. Additionally, Sort Shuffle uses extra managed memory as data reading buffer and does not rely on sendfile or mmap mechanism, thus it also works well with SSL. Please refer to FLINK-19582 and FLINK-19614 for more details about Sort Shuffle.
Here are some config options that might need adjustment when using sort blocking shuffle:
- taskmanager.network.sort-shuffle.min-buffers: Config option to control data writing buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough. Because this memory is allocated from network memory, to increase this value, you may also need to increase the total network memory by adjusting taskmanager.memory.network.fraction, taskmanager.memory.network.min and taskmanager.memory.network.max to avoid the potential "Insufficient number of network buffers" error.
- taskmanager.memory.framework.off-heap.batch-shuffle.size: Config option to control data reading buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough. Because this memory is cut from the framework off-heap memory, to increase this value, you need also to increase the total framework off-heap memory by adjusting taskmanager.memory.framework.off-heap.size to avoid the potential direct memory OOM error.
- Currently Sort Shuffle only sort records by partition index instead of the records themselves, that is to say, the sort is only used as a data clustering algorithm.
### Choices of Blocking Shuffle
As a summary,
- For small scale jobs running on SSD, both implementation should work.
- For large scale jobs or for jobs running on HDD, Sort Shuffle should be more suitable.
To switch between Sort Shuffle and Hash Shuffle, you need to adjust this config option: taskmanager.network.sort-shuffle.min-parallelism. It controls which shuffle implementation to use based on the parallelism of downstream tasks, if the parallelism is lower than the configured value, Hash Shuffle will be used, otherwise Sort Shuffle will be used. For versions lower than 1.15, its default value is Integer.MAX_VALUE, so Hash Shuffle will be used by default. Since 1.15, its default value is 1, so Sort Shuffle will be used by default.
## Hybrid Shuffle
- This feature is still experimental and has some known limitations.
Hybrid shuffle is the next generation of batch data exchanges. It combines the advantages of blocking shuffle and pipelined shuffle (in streaming mode).
- Like blocking shuffle, it does not require upstream and downstream tasks to run simultaneously, which allows executing a job with little resources.
- Like pipelined shuffle, it does not require downstream tasks to be executed after upstream tasks finish, which reduces the overall execution time of the job when given sufficient resources.
- It adapts to custom preferences between persisting less data and restarting less tasks on failures, by providing different spilling strategies.
To use hybrid shuffle mode, you need to configure the execution.batch-shuffle-mode to ALL_EXCHANGES_HYBRID_FULL (full spilling strategy) or ALL_EXCHANGES_HYBRID_SELECTIVE (selective spilling strategy).
### Spilling Strategy
Hybrid shuffle provides two spilling strategies:
- Selective Spilling Strategy persists data only if they are not consumed by downstream tasks timely. This reduces the amount of data to persist, at the price that in case of failures upstream tasks need to be restarted to reproduce the complete intermediate results.
- Full Spilling Strategy persists all data, no matter they are consumed by downstream tasks or not. In case of failures, the persisted complete intermediate result can be re-consumed, without having to restart upstream tasks.
To use hybrid shuffle mode, you need to configure the execution.batch-shuffle-mode to ALL_EXCHANGES_HYBRID_FULL (full spilling strategy) or ALL_EXCHANGES_HYBRID_SELECTIVE (selective spilling strategy).
### Data Consumption Constraints
Hybrid shuffle divides the partition data consumption constraints between producer and consumer into the following three cases:
- ALL_PRODUCERS_FINISHED : hybrid partition data can be consumed only when all producers are finished.
- ONLY_FINISHED_PRODUCERS : hybrid partition can only consume data from finished producers.
- UNFINISHED_PRODUCERS : hybrid partition can consume data from unfinished producers.
These could be configured via jobmanager.partition.hybrid.partition-data-consume-constraint.
- For AdaptiveBatchScheduler : The default constraint is UNFINISHED_PRODUCERS to perform pipelined-like shuffle. If the value is set to ALL_PRODUCERS_FINISHED or ONLY_FINISHED_PRODUCERS, performance may be degraded.
- If SpeculativeExecution is enabled : The default constraint is ONLY_FINISHED_PRODUCERS to bring some performance optimization compared with blocking shuffle. Since producers and consumers have the opportunity to run at the same time, more speculative execution tasks may be created, and the cost of failover will also increase. If you want to fall back to the same behavior as blocking shuffle, you can configure this value to ALL_PRODUCERS_FINISHED. It is also important to note that UNFINISHED_PRODUCERS is not supported in this mode.
### Remote Storage Support
Hybrid shuffle supports to store the shuffle data to the remote storage. The remote storage path can be configured by taskmanager.network.hybrid-shuffle.remote.path. This feature supports various remote storage systems, including OSS, HDFS, S3, etc. See Flink Filesystem for more information about the Flink supported filesystems.
### Limitations
Hybrid shuffle mode is still experimental and has some known limitations, which the Flink community is still working on eliminating.
- No support for Slot Sharing. In hybrid shuffle mode, Flink currently forces each task to be executed in a dedicated slot exclusively. If slot sharing is explicitly specified, an error will occur.
- No pipelined execution for dynamic graph. If auto-parallelism (dynamic graph) is enabled, Adaptive Batch Scheduler will wait until upstream tasks finish to decide parallelism of downstream tasks, which means hybrid shuffle effectively fallback to blocking shuffle (ALL_PRODUCERS_FINISHED constraint).
## Performance Tuning
The following guidelines may help you to achieve better performance especially for large scale batch jobs:
- Blocking Shuffle
- Always use Sort Shuffle on HDD because Sort Shuffle can largely improve stability and IO performance. Since 1.15, Sort Shuffle is already the default blocking shuffle implementation, for 1.14 and lower version, you need to enable it manually by setting taskmanager.network.sort-shuffle.min-parallelism to 1.
- For both blocking shuffle implementations, you may consider enabling data compression to improve the performance unless the data is hard to compress. Since 1.15, data compression is already enabled by default, for 1.14 and lower version, you need to enable it manually.
- When Sort Shuffle is used, decreasing the number of exclusive buffers per channel and increasing the number of floating buffers per gate can help. For 1.14 and higher version, it is suggested to set taskmanager.network.memory.buffers-per-channel to 0 and set taskmanager.network.memory.floating-buffers-per-gate to a larger value (for example, 4096). This setting has two main advantages: 1) It decouples the network memory consumption from parallelism so for large scale jobs, the possibility of "Insufficient number of network buffers" error can be decreased; 2) Networker buffers are distributed among different channels according to needs, which can improve the network buffer utilization and further improve performance.
- Increase the total size of network memory. Currently, the default network memory size is pretty modest. For large scale jobs, it's suggested to increase the total network memory fraction to at least 0.2 to achieve better performance. At the same time, you may also need to adjust the lower bound and upper bound of the network memory size, please refer to the memory configuration document for more information.
- Increase the memory size for shuffle data write. As mentioned in the above section, for large scale jobs, it's suggested to increase the number of write buffers per result partition to at least (2 * parallelism) if you have enough memory. Note that you may also need to increase the total size of network memory to avoid the "Insufficient number of network buffers" error after you increase this config value.
- Increase the memory size for shuffle data read. As mentioned in the above section, for large scale jobs, it's suggested to increase the size of the shared read memory to a larger value (for example, 256M or 512M). Because this memory is cut from the framework off-heap memory, you must increase taskmanager.memory.framework.off-heap.size by the same size to avoid the direct memory OOM error.
- Hybrid Shuffle
- Increase the total size of network memory. Currently, the default network memory size is pretty modest. For large scale jobs, it's suggested to increase the total network memory fraction to at least 0.2 to achieve better performance. At the same time, you may also need to adjust the lower bound and upper bound of the network memory size, please refer to the memory configuration document for more information.
- Increase the memory size for shuffle data write. For large scale jobs, it's suggested to increase the total size of network memory, the larger the memory that can be used in the shuffle write phase, the more opportunities downstream to read data directly from memory. Note that if you use the legacy Hybrid shuffle mode, you need to ensure that each Result Partition can be allocated to at least numSubpartition + 1 buffers, otherwise the "Insufficient number of network buffers" will be encountered.
- Increase the memory size for shuffle data read. For large scale jobs, it's suggested to increase the size of the shared read memory to a larger value (for example, 256M or 512M). Because this memory is cut from the framework off-heap memory, you must increase taskmanager.memory.framework.off-heap.size by the same size to avoid the direct memory OOM error.
- When the legacy Hybrid shuffle mode is used, decreasing the number of exclusive buffers per channel will seriously affect the performance. Therefore, this value should not be set to 0, and for large-scale job, this can be appropriately increased. It should be also noted that, for hybrid shuffle, taskmanager.network.memory.read-buffer.required-per-gate.max has been set to Integer.MAX_VALUE by default. It is better not to adjust this value, otherwise there is a risk of performance degradation.
## Trouble Shooting
Here are some exceptions you may encounter (rarely) and the corresponding solutions that may help:
- Blocking Shuffle [table: 10 rows]
- Hybrid Shuffle [table: 7 rows]

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/batch/recovery_from_job_master_failure/
# Batch jobs progress recovery from job master failures
## Background
Previously, if the JobMaster fails and is terminated, one of the following two situations will occur:
- If high availability (HA) is disabled, the job will fail.
- If HA is enabled, a JobMaster failover will happen and the job will be restarted. Streaming jobs can resume from the latest successful checkpoints. Batch jobs, however, do not have checkpoints and have to start over from the beginning, losing all previously made progress. This represents a significant regression for long-running batch jobs.
To address this issue, a batch job recovery mechanism is introduced to enable batch jobs to recover as much progress as possible after a JobMaster failover, avoiding the need to rerun tasks that have already been finished.
To implement this feature, a JobEventStore component is introduced to record state change events of the JobMaster (such as ExecutionGraph, OperatorCoordinator, etc.) to an external filesystem. During the crash and subsequent restart of the JobMaster, TaskManagers will retain the intermediate result data produced by the job and attempt to reconnect continuously. Once the JobMaster restarts, it will re-establish connections with TaskManagers and recover the job state based on the retained intermediate results and the events previously recorded in the JobEventStore, thereby resuming the job's execution progress.
## Usage
This section explains how to enable recovery of batch jobs from JobMaster failures, how to tune it, and how to develop sources to work with batch jobs progress recovery.
### How to enable batch jobs progress recovery from job master failures
- Enable cluster high availability: To enable the recovery of batch jobs from JobMaster failures, it is essential to first ensure that cluster high availability (HA) is enabled. Flink supports HA services backed by ZooKeeper or Kubernetes. More details of the configuration can be found in the High Availability page.
- Configure execution.batch.job-recovery.enabled: true. Note that currently only Adaptive Batch Scheduler supports this feature. And Flink batch jobs will use this scheduler by default unless another scheduler is explicitly configured.
### Optimization
To enable batch jobs to recover as much progress as possible after a JobMaster failover, and avoid rerunning tasks that have already been finished, you can configure the following options for optimization:
- execution.batch.job-recovery.snapshot.min-pause: This setting determines the minimum pause time allowed between snapshots for the OperatorCoordinator and ShuffleMaster. This parameter could be adjusted based on the expected I/O load of your cluster and the tolerable amount of state regression. Reduce this interval if smaller state regressions are preferred and a higher I/O load is acceptable.
- execution.batch.job-recovery.previous-worker.recovery.timeout: This setting determines the timeout duration allowed for Shuffle workers to reconnect. During the recovery process, Flink requests the retained intermediate result data information from the Shuffle Master. If the timeout is reached, Flink will use all the acquired intermediate result data to recover the state.
- job-event.store.write-buffer.flush-interval: This setting determines the flush interval for the JobEventStore's write buffers.
- job-event.store.write-buffer.size: This setting determines the write buffer size in the JobEventStore. When the buffer is full, its contents are flushed to the external filesystem.
### Enable batch jobs progress recovery for sources
Currently, only the new source (FLIP-27) supports progress recovery for batch jobs. To achieve this functionality, the SplitEnumerator of the new source (FLIP-27) must be able to take state snapshots in batch processing scenarios (where the checkpointId is set to -1) and implement the SupportsBatchSnapshot interface. This allows it to recover to the progress before the job master failure. Otherwise, to ensure data accuracy, one of the following two situations will occur after a job master failover:
- If not all tasks of this source are finished, we will reset and re-run all these tasks.
- If all tasks of this source are finished, no additional action is required, and the job can continue to run. However, if any of these tasks need to be restarted at some point in the future (for example, due to a PartitionNotFound exception), then all subtasks of this source will need to be reset and rerun.
## Limitations
- Only working with the new source (FLIP-27): Since the legacy source has been deprecated, this feature only supports the new source.
- Exclusive to the Adaptive Batch Scheduler: Currently, only the Adaptive Batch Scheduler supports the recovery of batch jobs after a JobMaster failover. As a result, the feature inherits all the limitations of the Adaptive Batch Scheduler.
- Not working when using remote shuffle services.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/debugging_event_time/
# Debugging Windows & Event Time
## Monitoring Current Event Time
Flink's event time and watermark support are powerful features for handling out-of-order events. However, it's harder to understand what exactly is going on because the progress of time is tracked within the system.
Low watermarks of each task can be accessed through Flink web interface or metrics system.
Each Task in Flink exposes a metric called currentInputWatermark that represents the lowest watermark received by this task. This long value represents the "current event time". The value is calculated by taking the minimum of all watermarks received by upstream operators. This means that the event time tracked with watermarks is always dominated by the furthest-behind source.
The low watermark metric is accessible using the web interface, by choosing a task in the metric tab, and selecting the <taskNr>.currentInputWatermark metric. In the new box you'll now be able to see the current low watermark of the task.
Another way of getting the metric is using one of the metric reporters, as described in the documentation for the metrics system. For local setups, we recommend using the JMX metric reporter and a tool like VisualVM.
## Handling Event Time Stragglers
- Approach 1: Watermark stays late (indicated completeness), windows fire early
- Approach 2: Watermark heuristic with maximum lateness, windows accept late data

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/debugging_classloading/
# Debugging Classloading
## Overview of Classloading in Flink
When running Flink applications, the JVM will load various classes over time. These classes can be divided into three groups based on their origin:
- The Java Classpath: This is Java's common classpath, and it includes the JDK libraries, and all code in Flink's /lib folder (the classes of Apache Flink and some dependencies). They are loaded by AppClassLoader.
- The Flink Plugin Components: The plugins code in folders under Flink's /plugins folder. Flink's plugin mechanism will dynamically load them once during startup.
- The Dynamic User Code: These are all classes that are included in the JAR files of dynamically submitted jobs, (via REST, CLI, web UI). They are loaded (and unloaded) dynamically by FlinkUserCodeClassLoader per job.
As a general rule, whenever you start the Flink processes first and submit jobs later, the job's classes are loaded dynamically. If the Flink processes are started together with the job/application, or if the application spawns the Flink components (JobManager, TaskManager, etc.), then all job's classes are in the Java classpath.
Code in plugin components is loaded dynamically once by a dedicated class loader per plugin.
In the following are some more details about the different deployment modes:
- Session Mode (Standalone/Yarn/Kubernetes): When starting a Flink session(Standalone/Yarn/Kubernetes) cluster, the JobManagers and TaskManagers are started with the Flink framework classes in the Java classpath. The classes from all jobs/applications that are submitted against the session (via REST / CLI) are loaded dynamically by FlinkUserCodeClassLoader.
- Application Mode (Standalone/Yarn/Kubernetes): When run a Standalone/Kubernetes Flink cluster in Application Mode, the user jars (the JAR file specified in startup command and all JAR files in Flink's usrlib folder) will be loaded dynamically by FlinkUserCodeClassLoader. When run a Yarn Flink cluster in Application Mode, the user jars (the JAR file specified in startup command and all JAR files in Flink's usrlib folder) will be included into the system classpath (the AppClassLoader) by default. When setting the yarn.classpath.include-user-jar to DISABLED, Flink will include the user jars in the user classpath and load them dynamically by FlinkUserCodeClassLoader.
## Inverted Class Loading and ClassLoader Resolution Order
In setups where dynamic classloading is involved (plugin components, Flink jobs in session setups), there is a hierarchy of typically two ClassLoaders: (1) Java's application classloader, which has all classes in the classpath, and (2) the dynamic plugin/user code classloader. for loading classes from the plugin or the user-code jar(s). The dynamic ClassLoader has the application classloader as its parent.
By default, Flink inverts classloading order, meaning it looks into the dynamic classloader first, and only looks into the parent (application classloader) if the class is not part of the dynamically loaded code.
The benefit of inverted classloading is that plugins and jobs can use different library versions than Flink's core itself, which is very useful when the different versions of the libraries are not compatible. The mechanism helps to avoid the common dependency conflict errors like IllegalAccessError or NoSuchMethodError. Different parts of the code simply have separate copies of the classes (Flink's core or one of its dependencies can use a different copy than the user code or plugin code). In most cases, this works well and no additional configuration from the user is needed.
However, there are cases when the inverted classloading causes problems (see below, "X cannot be cast to X"). For user code classloading, you can revert back to Java's default mode by configuring the ClassLoader resolution order via classloader.resolve-order in the Flink config to parent-first (from Flink's default child-first).
Please note that certain classes are always resolved in a parent-first way (through the parent ClassLoader first), because they are shared between Flink's core and the plugin/user code or the plugin/user-code facing APIs. The packages for these classes are configured via classloader.parent-first-patterns.default and classloader.parent-first-patterns.additional. To add new packages to be parent-first loaded, please set the classloader.parent-first-patterns.additional config option.
## Avoiding Dynamic Classloading for User Code
All components (JobManager, TaskManager, Client, ApplicationMaster, …) log their classpath setting on startup. They can be found as part of the environment information at the beginning of the log.
When running a setup where the JobManager and TaskManagers are exclusive to one particular job, one can put user code JAR files directly into the /lib folder to make sure they are part of the classpath and not loaded dynamically.
It usually works to put the job's JAR file into the /lib directory. The JAR will be part of both the classpath (the AppClassLoader) and the dynamic class loader (FlinkUserCodeClassLoader). Because the AppClassLoader is the parent of the FlinkUserCodeClassLoader (and Java loads parent-first, by default), this should result in classes being loaded only once.
For setups where the job's JAR file cannot be put to the /lib folder (for example because the setup is a session that is used by multiple jobs), it may still be possible to put common libraries to the /lib folder, and avoid dynamic class loading for those.
## Manual Classloading in User Code
In some cases, a transformation function, source, or sink needs to manually load classes (dynamically via reflection). To do that, it needs the classloader that has access to the job's classes.
In that case, the functions (or sources or sinks) can be made a RichFunction (for example RichMapFunction or RichWindowFunction) and access the user code class loader via getRuntimeContext().getUserCodeClassLoader().
## X cannot be cast to X exceptions
In setups with dynamic classloading, you may see an exception in the style com.foo.X cannot be cast to com.foo.X. This means that multiple versions of the class com.foo.X have been loaded by different class loaders, and types of that class are attempted to be assigned to each other.
One common reason is that a library is not compatible with Flink's inverted classloading approach. You can turn off inverted classloading to verify this (set classloader.resolve-order: parent-first in the Flink config) or exclude the library from inverted classloading (set classloader.parent-first-patterns.additional in the Flink config).
Another cause can be cached object instances, as produced by some libraries like Apache Avro, or by interning objects (for example via Guava's Interners). The solution here is to either have a setup without any dynamic classloading, or to make sure that the respective library is fully part of the dynamically loaded code. The latter means that the library must not be added to Flink's /lib folder, but must be part of the application's fat-jar/uber-jar
## Unloading of Dynamically Loaded Classes in User Code
All scenarios that involve dynamic user code classloading (sessions) rely on classes being unloaded again. Class unloading means that the Garbage Collector finds that no objects from a class exist and more, and thus removes the class (the code, static variable, metadata, etc).
Whenever a TaskManager starts (or restarts) a task, it will load that specific task's code. Unless classes can be unloaded, this will become a memory leak, as new versions of classes are loaded and the total number of loaded classes accumulates over time. This typically manifests itself though a OutOfMemoryError: Metaspace.
Common causes for class leaks and suggested fixes:
- Lingering Threads: Make sure the application functions/sources/sinks shuts down all threads. Lingering threads cost resources themselves and additionally typically hold references to (user code) objects, preventing garbage collection and unloading of the classes.
- Interners: Avoid caching objects in special structures that live beyond the lifetime of the functions/sources/sinks. Examples are Guava's interners, or Avro's class/object caches in the serializers.
- JDBC: JDBC drivers leak references outside the user code classloader. To ensure that these classes are only loaded once you should add the driver jars to Flink's lib/ folder instead of bundling them in the user-jar. If you can't guarantee that none of your user-jars bundle the driver, you have to additionally add the driver classes to the list of parent-first loaded classes via classloader.parent-first-patterns.additional.
A helpful tool for unloading dynamically loaded classes are the user code class loader release hooks. These are hooks which are executed prior to the unloading of a classloader. It is generally recommended to shutdown and unload resources as part of the regular function lifecycle (typically the close() methods). But in some cases (for example for static fields), it is better to unload once a classloader is certainly not needed anymore.
Class loader release hooks can be registered via the RuntimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent() method.
## Resolving Dependency Conflicts with Flink using the maven-shade-plugin.
A way to address dependency conflicts from the application developer's side is to avoid exposing dependencies by shading them away.
Apache Maven offers the maven-shade-plugin, which allows one to change the package of a class after compiling it (so the code you are writing is not affected by the shading). For example if you have the com.amazonaws packages from the aws sdk in your user code jar, the shade plugin would relocate them into the org.myorg.shaded.com.amazonaws package, so that your code is calling your aws sdk version.
This documentation page explains relocating classes using the shade plugin.
Note that most of Flink's dependencies, such as guava, netty, jackson, etc. are shaded away by the maintainers of Flink, so users usually don't have to worry about it.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/flame_graphs/
# Flame Graphs
Flame Graphs are a visualization that effectively surfaces answers to questions like:
- Which methods are currently consuming CPU resources?
- How does consumption by one method compare to the others?
- Which series of calls on the stack led to executing a particular method?
Flame Graphs are constructed by sampling stack traces a number of times. Each method call is presented by a bar, where the length of the bar is proportional to the number of times it is present in the samples.
Starting with Flink 1.13, Flame Graphs are natively supported in Flink. In order to produce a Flame Graph, navigate to the job graph of a running job, select an operator of interest and in the menu to the right click on the Flame Graph tab:
- Any measurement process in and of itself inevitably affects the subject of measurement (see the double-split experiment). Sampling CPU stack traces is no exception. In order to prevent unintended impacts on production environments, Flame Graphs are currently available as an opt-in feature. To enable it, you'll need to set rest.flamegraph.enabled: true in Flink configuration file. We recommend enabling it in development and pre-production environments, but you should treat it as an experimental feature in production.
Apart from the On-CPU Flame Graphs, Off-CPU and Mixed visualizations are available and can be switched between by using the selector at the top of the pane:
The Off-CPU Flame Graph visualizes blocking calls found in the samples. A distinction is made as follows:
- On-CPU: Thread.State in [RUNNABLE, NEW]
- Off-CPU: Thread.State in [TIMED_WAITING, WAITING, BLOCKED]
Mixed mode Flame Graphs are constructed from stack traces of threads in all possible states.
## Sampling process
The collection of stack traces is done purely within the JVM, so only method calls within the Java runtime are visible (no system calls).
Flame Graph construction is performed at the level of an individual operator by default, i.e. all task threads of that operator are sampled in parallel and their stack traces are combined. If a method call consumes 100% of the resources in one of the parallel tasks but none in the others, the bottleneck might be obscured by being averaged out.
Starting with Flink 1.17, Flame Graph provides "drill down" visualizations to the task level. Select a subtask of interest, and you can see the flame graph of the corresponding subtask.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/profiler/
# Profiler
Since Flink 1.19, we support profiling the JobManager/TaskManager process interactively with async-profiler via Flink Web UI, which allows users to create a profiling instance with arbitrary intervals and event modes, e.g ITIMER, CPU, Lock, Wall-Clock and Allocation.
- CPU: In this mode the profiler collects stack trace samples that include Java methods, native calls, JVM code and kernel functions.
- ALLOCATION: In allocation profiling mode, the top frame of every call trace is the class of the allocated object, and the counter is the heap pressure (the total size of allocated TLABs or objects outside TLAB).
- Wall-clock: Wall-Clock option tells async-profiler to sample all threads equally every given period of time regardless of thread status: Running, Sleeping or Blocked. For instance, this can be helpful when profiling application start-up time.
- Lock: In lock profiling mode the top frame is the class of lock/monitor, and the counter is number of nanoseconds it took to enter this lock/monitor.
- ITIMER: You can fall back to itimer profiling mode. It is similar to cpu mode, but does not require perf_events support. As a drawback, there will be no kernel stack traces.
- Any measurement process in and of itself inevitably affects the subject of measurement. In order to prevent unintended impacts on production environments, Profiler are currently available as an opt-in feature. To enable it, you'll need to set rest.profiling.enabled: true in Flink configuration file. We recommend enabling it in development and pre-production environments, but you should treat it as an experimental feature in production.
## Requirements
Since the Profiler is powered by the Async-profiler, it is required to work on platforms that are supported by the Async-profiler. [table: 3 rows]. Profiling on platforms beyond those listed above will fail with an error message in the Message column.
## Usage
Flink users can complete the profiling submission and result export via Flink Web UI conveniently.
For example,
- First, you should find out the candidate TaskManager/JobManager with performance bottleneck for profiling, and switch to the corresponding TaskManager/JobManager page (profiler tab).
- You can submit a profiling instance with a specified duration and mode by simply clicking on the button Create Profiling Instance. (The description of the profiling mode will be shown when hovering over the corresponding mode.)
- Once the profiling instance is complete, you can easily download the interactive HTML file by clicking on the link.
## Troubleshooting
- Failed to profiling in CPU mode: No access to perf events. Try –fdtransfer or –all-user option or 'sysctl kernel.perf_event_paranoid=1'. That means perf_event_open() syscall has failed. By default, Docker container restricts the access to perf_event_open syscall. The recommended solution is to fall back to ITIMER profiling mode. It is similar to CPU mode, but does not require perf_events support. As a drawback, there will be no kernel stack traces.
- Failed to profiling in Allocation mode: No AllocTracer symbols found. Are JDK debug symbols installed? The OpenJDK debug symbols are required for allocation profiling. See Installing Debug Symbols for more details.
- You can refer to the Troubleshooting page of async-profiler for more cases.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/application_profiling/
# Application Profiling & Debugging
## Overview of Custom Logging with Apache Flink
Each standalone JobManager, TaskManager, HistoryServer, and ZooKeeper daemon redirects stdout and stderr to a file with a .out filename suffix and writes internal logging to a file with a .log suffix. Java options configured by the user in env.java.opts.all, env.java.opts.jobmanager, env.java.opts.taskmanager, env.java.opts.historyserver and env.java.opts.client can likewise define log files with use of the script variable FLINK_LOG_PREFIX and by enclosing the options in double quotes for late evaluation. Log files using FLINK_LOG_PREFIX are rotated along with the default .out and .log files.
## Profiling with Java Flight Recorder
Java Flight Recorder is a profiling and event collection framework built into the Oracle JDK. Java Mission Control is an advanced set of tools that enables efficient and detailed analysis of the extensive of data collected by Java Flight Recorder. Example configuration: `[code: env.java.opts.all: "-XX:+UnlockCommercialFeatures -XX:+UnlockDiagnosti ... (228 chars)]`
## Profiling with JITWatch
JITWatch is a log analyser and visualizer for the Java HotSpot JIT compiler used to inspect inlining decisions, hot methods, bytecode, and assembly. Example configuration: `[code: env.java.opts.all: "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoad ... (149 chars)]`
## Analyzing Out of Memory Problems
If you encounter OutOfMemoryExceptions with your Flink application, then it is a good idea to enable heap dumps on out of memory errors. `[code: env.java.opts.all: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$ ... (95 chars)]`. The heap dump will allow you to analyze potential memory leaks in your user code. If the memory leak should be caused by Flink, then please reach out to the dev mailing list.
## Analyzing Memory & Garbage Collection Behaviour
Memory usage and garbage collection can have a profound impact on your application. The effects can range from slight performance degradation to a complete cluster failure if the GC pauses are too long. If you want to better understand the memory and GC behaviour of your application, then you can enable memory logging on the TaskManagers. `[code: taskmanager.debug.memory.log: true ... (95 chars)]`. If you are interested in more detailed GC statistics, then you can activate the JVM's GC logging via: `[code: env.java.opts.all: "-Xloggc:${FLINK_LOG_PREFIX}.gc.log -XX:+PrintGCApp ... (252 chars)]`

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/rest_api/
# REST API
Flink has a monitoring API that can be used to query status and statistics of running jobs, as well as recent completed jobs. This monitoring API is used by Flink's own dashboard, but is designed to be used also by custom monitoring tools.
The monitoring API is a REST-ful API that accepts HTTP requests and responds with JSON data.
## Overview
The monitoring API is backed by a web server that runs as part of the JobManager. By default, this server listens at port 8081, which can be configured in Flink configuration file via rest.port. Note that the monitoring API web server and the web dashboard web server are currently the same and thus run together at the same port. They respond to different HTTP URLs, though.
In the case of multiple JobManagers (for high availability), each JobManager will run its own instance of the monitoring API, which offers information about completed and running job while that JobManager was elected the cluster leader.
## Developing
The REST API backend is in the flink-runtime project. The core class is org.apache.flink.runtime.webmonitor.WebMonitorEndpoint, which sets up the server and the request routing.
We use Netty and the Netty Router library to handle REST requests and translate URLs. This choice was made because this combination has lightweight dependencies, and the performance of Netty HTTP is very good.
To add new requests, one needs to
- add a new MessageHeaders class which serves as an interface for the new request,
- add a new AbstractRestHandler class which handles the request according to the added MessageHeaders class,
- add the handler to org.apache.flink.runtime.webmonitor.WebMonitorEndpoint#initializeHandlers().
A good example is the org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler that uses the org.apache.flink.runtime.rest.messages.JobExceptionsHeaders.
## API
The REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. Prefixes are always of the form v[version_number]. For example, to access version 1 of /foo/bar one would query /v1/foo/bar.
If no version is specified Flink will default to the oldest version supporting the request. Querying unsupported/non-existing versions will return a 404 error.
There exist several async operations among these APIs, e.g. trigger savepoint, rescale a job. They would return a triggerid to identify the operation you just POST and then you need to use that triggerid to query for the status of the operation.
For (stop-with-)savepoint operations you can control this triggerId by setting it in the body of the request that triggers the operation. This allow you to safely* retry such operations without triggering multiple savepoints.
- The retry is only safe until the async operation store duration has elapsed.
### JobManager
OpenAPI specification — The OpenAPI specification is still experimental.
#### API reference — v1: [many endpoint tables — overview/listing condensed: ~80 tables of 5-9 rows each covering cluster config, job lifecycle, savepoints, cancel/stop, backpressure, flame graph, profiling, taskmanager, metrics, accumulators, exceptions, plan submission, jar management, etc. Full OpenAPI spec available at the live REST endpoint.]

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/checkpoint_monitoring/
# Monitoring Checkpointing
## Overview
Flink's web interface provides a tab to monitor the checkpoints of jobs. These stats are also available after the job has terminated. There are four different tabs to display information about your checkpoints: Overview, History, Summary, and Configuration. The following sections will cover all of these in turn.
## Monitoring
### Overview Tab
The overview tabs lists the following statistics. Note that these statistics don't survive a JobManager loss and are reset if your JobManager fails over.
- Checkpoint Counts
- Triggered: The total number of checkpoints that have been triggered since the job started.
- In Progress: The current number of checkpoints that are in progress.
- Completed: The total number of successfully completed checkpoints since the job started.
- Failed: The total number of failed checkpoints since the job started.
- Restored: The number of restore operations since the job started. This also tells you how many times the job has restarted since submission. Note that the initial submission with a savepoint also counts as a restore and the count is reset if the JobManager was lost during operation.
- Latest Completed Checkpoint: The latest successfully completed checkpoints. Clicking on More details gives you detailed statistics down to the subtask level.
- Latest Failed Checkpoint: The latest failed checkpoint. Clicking on More details gives you detailed statistics down to the subtask level.
- Latest Savepoint: The latest triggered savepoint with its external path. Clicking on More details gives you detailed statistics down to the subtask level.
- Latest Restore: There are two types of restore operations.
- Restore from Checkpoint: We restored from a regular periodic checkpoint.
- Restore from Savepoint: We restored from a savepoint.
### History Tab
The checkpoint history keeps statistics about recently triggered checkpoints, including those that are currently in progress.
Note that for failed checkpoints, metrics are updated on a best efforts basis and may be not accurate.
- ID: The ID of the triggered checkpoint. The IDs are incremented for each checkpoint, starting at 1.
- Status: The current status of the checkpoint, which is either In Progress, Completed, or Failed. If the triggered checkpoint is a savepoint, you will see a floppy-disk symbol.
- Acknowledged: The number of acknowledged subtask with total subtask.
- Trigger Time: The time when the checkpoint was triggered at the JobManager.
- Latest Acknowledgement: The time when the latest acknowledgement for any subtask was received at the JobManager (or n/a if no acknowledgement received yet).
- End to End Duration: The duration from the trigger timestamp until the latest acknowledgement (or n/a if no acknowledgement received yet). This end to end duration for a complete checkpoint is determined by the last subtask that acknowledges the checkpoint. This time is usually larger than single subtasks need to actually checkpoint the state.
- Checkpointed Data Size: The persisted data size during the sync and async phases of that checkpoint, the value could be different from full checkpoint data size if incremental checkpoint or changelog is enabled.
- Full Checkpoint Data Size: The accumulated checkpoint data size over all acknowledged subtasks.
- Processed (persisted) in-flight data: The approximate number of bytes processed/persisted during the alignment (time between receiving the first and the last checkpoint barrier) over all acknowledged subtasks. Persisted data could be larger than zero only if the unaligned checkpoints are enabled.
For subtasks there are a couple of more detailed stats available.
- Sync Duration: The duration of the synchronous part of the checkpoint. This includes snapshotting state of the operators and blocks all other activity on the subtask (processing records, firing timers, etc).
- Async Duration: The duration of the asynchronous part of the checkpoint. This includes time it took to write the checkpoint on to the selected filesystem. For unaligned checkpoints this also includes also the time the subtask had to wait for last of the checkpoint barriers to arrive (alignment duration) and the time it took to persist the in-flight data.
- Alignment Duration: The time between processing the first and the last checkpoint barrier. For aligned checkpoints, during the alignment, the channels that have already received checkpoint barrier are blocked from processing more data.
- Start Delay: The time it took for the first checkpoint barrier to reach this subtask since the checkpoint barrier has been created.
- Unaligned Checkpoint: Whether the checkpoint for the subtask is completed as an unaligned checkpoint. An aligned checkpoint can switch to an unaligned checkpoint if the alignment timeouts.
#### History Size Configuration
You can configure the number of recent checkpoints that are remembered for the history via the following configuration key. The default is 10. `[code: # Number of recent checkpoints that are remembered ... (78 chars)]`
### Summary Tab
The summary computes a simple min/average/maximum statistics over all completed checkpoints for the End to End Duration, Incremental Checkpoint Data Size, Full Checkpoint Data Size, and Bytes Buffered During Alignment (see History for details about what these mean).
Note that these statistics don't survive a JobManager loss and are reset if your JobManager fails over.
### Configuration Tab
The configuration list your streaming configuration:
- Checkpointing Mode: Either Exactly Once or At least Once.
- Interval: The configured checkpointing interval. Trigger checkpoints in this interval.
- Timeout: Timeout after which a checkpoint is cancelled by the JobManager and a new checkpoint is triggered.
- Minimum Pause between Checkpoints: Minimum required pause between checkpoints. After a checkpoint has completed successfully, we wait at least for this amount of time before triggering the next one, potentially delaying the regular interval.
- Maximum Concurrent Checkpoints: The maximum number of checkpoints that can be in progress concurrently.
- Persist Checkpoints Externally: Enabled or Disabled. If enabled, furthermore lists the cleanup config for externalized checkpoints (delete or retain on cancellation).
### Checkpoint Details
When you click on a More details link for a checkpoint, you get a Minimum/Average/Maximum summary over all its operators and also the detailed numbers per single subtask.
#### Summary per Operator
#### All Subtask Statistics
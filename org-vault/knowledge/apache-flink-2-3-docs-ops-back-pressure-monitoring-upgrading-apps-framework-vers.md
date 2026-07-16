---
title: Apache Flink 2.3 docs — Ops (back pressure monitoring / upgrading apps + framework versions / production readiness checklist)
tags: [org, flink, flink-2.3, docs, ops, back-pressure, upgrading, savepoint-migration, production-readiness, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:14:56.137Z'
updated: '2026-07-08T05:14:56.137Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 docs — Ops batch C (3 pages: back_pressure, upgrading, production_ready). Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/* via chrome-devtools in-browser fetch. **WHY this matters:** this is the operational reference for diagnosing back pressure (backPressuredTimeMsPerSecond / idleTimeMsPerSecond / busyTimeMsPerSecond adding to ~1000ms, OK/LOW/HIGH thresholds), the upgrade & savepoint-migration playbook (API stability annotations Public/PublicEvolving/Experimental, source vs binary compatibility, FLIP-321 deprecation migration periods, DataStream operator-state matching via uid(), Table API/SQL state incompatibility caveats, in-place vs shadow-copy upgrade, RocksDB semi-async savepoint migration NOT supported, savepoint compatibility table), and the production-readiness checklist (explicit max parallelism 0<p<=max<=2^15 with default 128 or MIN(nextPowerOfTwo(p+p/2),2^15), set uids, state backend choice, checkpoint interval = SLA expression, JobManager HA, cluster security/RBAC/TLS — Flink supports remote code execution so never expose to public internet). Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/back_pressure/
# Monitoring Back Pressure
Flink's web interface provides a tab to monitor the back pressure behaviour of running jobs.
## Back Pressure
If you see a back pressure warning (e.g. High) for a task, this means that it is producing data faster than the downstream operators can consume. Records in your job flow downstream (e.g. from sources to sinks) and back pressure is propagated in the opposite direction, up the stream.
Take a simple Source -> Sink job as an example. If you see a warning for Source, this means that Sink is consuming data slower than Source is producing. Sink is back pressuring the upstream operator Source.
## Task performance metrics
Every parallel instance of a task (subtask) is exposing a group of three metrics:
- backPressuredTimeMsPerSecond, time that subtask spent being back pressured
- idleTimeMsPerSecond, time that subtask spent waiting for something to process
- busyTimeMsPerSecond, time that subtask was busy doing some actual work
At any point of time these three metrics are adding up approximately to 1000ms.
These metrics are being updated every couple of seconds, and the reported value represents the average time that subtask was back pressured (or idle or busy) during the last couple of seconds. Keep this in mind if your job has a varying load. For example, a subtask with a constant load of 50% and another subtask that is alternating every second between fully loaded and idling will both have the same value of busyTimeMsPerSecond: around 500ms.
Internally, back pressure is judged based on the availability of output buffers. If a task has no available output buffers, then that task is considered back pressured. Idleness, on the other hand, is determined by whether or not there is input available.
## Example
The WebUI aggregates the maximum value of the back pressure and busy metrics from all of the subtasks and presents those aggregated values inside the JobGraph. Besides displaying the raw values, tasks are also color-coded to make the investigation easier.
Idling tasks are blue, fully back pressured tasks are black, and fully busy tasks are colored red. All values in between are represented as shades between those three colors.
### Back Pressure Status
In the Back Pressure tab next to the job overview you can find more detailed metrics.
For subtasks whose status is OK, there is no indication of back pressure. HIGH, on the other hand, means that a subtask is back pressured. Status is defined in the following way:
- OK: 0% <= back pressured <= 10%
- LOW: 10% < back pressured <= 50%
- HIGH: 50% < back pressured <= 100%
Additionally, you can find the percentage of time each subtask is back pressured, idle, or busy.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/upgrading/
# Upgrading Applications and Flink Versions
Flink DataStream programs are typically designed to run for long periods of time such as weeks, months, or even years. As with all long-running services, Flink streaming applications need to be maintained, which includes fixing bugs, implementing improvements, or migrating an application to a Flink cluster of a later version.
This document describes how to update a Flink streaming application and how to migrate a running streaming application to a different Flink cluster.
## API compatibility guarantees
The classes & members of the Java APIs that are intended for users are annotated with the following stability annotations:
- Public
- PublicEvolving
- Experimental
  Annotations on a class also apply to all members of that class, unless otherwise annotated.
Any API without such an annotation is considered internal to Flink, with no guarantees being provided.
An API that is source compatible means that code written against the API will continue to compile against a later version.
An API that is binary compatible means that code compiled against the API will continue to run against a later version.
This table lists the source / binary compatibility guarantees for each annotation when upgrading to a particular release:
[table: 4 rows]
Example
Consider the code written against a Public API in 1.15.2:
- The code can continue to run when upgrading to Flink 1.15.3 without recompiling, because patch version upgrades for Public APIs guarantee binary compatibility.
- The same code may have to be recompiled when upgrading from 1.15.x to 1.16.0, because minor version upgrades for Public APIs only provide source compatibility, not binary compatibility.
- Code change may be required when upgrading from 1.x to 2.x because major version upgrades for Public APIs provide neither source nor binary compatibility.
Consider the code written against a PublicEvolving API in 1.15.2:
- The code can continue to run when upgrading to Flink 1.15.3 without recompiling, because patch version upgrades for PublicEvolving APIs guarantee binary compatibility.
- A code change may be required when upgrading from 1.15.x to Flink 1.16.0, because minor version upgrades for PublicEvolving APIs provide neither source nor binary compatibility.
### Deprecated API Migration Period
When an API is deprecated, it is marked with the @Deprecated annotation and a deprecation message is added to the Javadoc. According to FLIP-321, starting from release 1.18, each deprecated API will have a guaranteed migration period depending on the API stability level:
[table: 4 rows]
The source code of a deprecated API will be kept for at least the guaranteed migration period, and may be removed at any point after the migration period has passed.
Example
Assuming a release sequence of 1.18, 1.19, 1.20, 2.0, 2.1, …, 3.0,
- if a Public API is deprecated in 1.18, it will not be removed until 2.0.
- if a Public API is deprecated in 1.20, the source code will be kept in 2.0 because the migration period is 2 minor releases. Also, because a Public API must maintain source compatibility throughout a major version, the source code will be kept for all the 2.x versions and removed in 3.0 at the earliest.
- if a PublicEvolving API is deprecated in 1.18, it will be removed in 1.20 at the earliest.
- if a PublicEvolving API is deprecated in 1.20, the source code will be kept in 2.0 because the migration period is 1 minor releases. The source code may be removed in 2.1 at the earliest.
- if an Experimental API is deprecated in 1.18.0, the source code will be kept for 1.18.1 and removed in 1.18.2 at the earliest. Also, the source code can be removed in 1.19.0.
Please check the FLIP-321 wiki for more details.
## Restarting Streaming Applications
The line of action for upgrading a streaming application or migrating an application to a different cluster is based on Flink's Savepoint feature. A savepoint is a consistent snapshot of the state of an application at a specific point in time.
There are two ways of taking a savepoint from a running streaming application.
- Taking a savepoint and continue processing.
`[code: > ./bin/flink savepoint <jobID> [pathToSavepoint] ... (49 chars)]`
It is recommended to periodically take savepoints in order to be able to restart an application from a previous point in time. If you want to trigger a savepoint in detached mode, just add the option -detached.
- Taking a savepoint and stopping the application as a single action.
`[code: > ./bin/flink cancel -s [pathToSavepoint] <jobID> ... (49 chars)]`
This means that the application is canceled immediately after the savepoint completed, i.e., no other checkpoints are taken after the savepoint.
Given a savepoint taken from an application, the same or a compatible application (see Application State Compatibility section below) can be started from that savepoint. Starting an application from a savepoint means that the state of its operators is initialized with the operator state persisted in the savepoint. This is done by starting an application using a savepoint.
`[code: > ./bin/flink run -d -s [pathToSavepoint] ~/application.jar ... (59 chars)]`
The operators of the started application are initialized with the operator state of the original application (i.e., the application the savepoint was taken from) at the time when the savepoint was taken. The started application continues processing from exactly this point on.
Note: Even though Flink consistently restores the state of an application, it cannot revert writes to external systems. This can be an issue if you resume from a savepoint that was taken without stopping the application. In this case, the application has probably emitted data after the savepoint was taken. The restarted application might (depending on whether you changed the application logic or not) emit the same data again. The exact effect of this behavior can be very different depending on the SinkFunction and storage system. Data that is emitted twice might be OK in case of idempotent writes to a key-value store like Cassandra but problematic in case of appends to a durable log such as Kafka. In any case, you should carefully check and test the behavior of a restarted application.
## Application State Compatibility
When upgrading an application in order to fix a bug or to improve the application, usually the goal is to replace the application logic of the running application while preserving its state. We do this by starting the upgraded application from a savepoint which was taken from the original application. However, this does only work if both applications are state compatible, meaning that the operators of upgraded application are able to initialize their state with the state of the operators of original application.
In this section, we discuss how applications can be modified to remain state compatible.
### DataStream API
#### Matching Operator State
When an application is restarted from a savepoint, Flink matches the operator state stored in the savepoint to stateful operators of the started application. The matching is done based on operator IDs, which are also stored in the savepoint. Each operator has a default ID that is derived from the operator's position in the application's operator topology. Hence, an unmodified application can always be restarted from one of its own savepoints. However, the default IDs of operators are likely to change if an application is modified. Therefore, modified applications can only be started from a savepoint if the operator IDs have been explicitly specified. Assigning IDs to operators is very simple and done using the uid(String) method as follows:
`[code: DataStream<String> mappedEvents = events ... (89 chars)]`
Note: Since the operator IDs stored in a savepoint and IDs of operators in the application to start must be equal, it is highly recommended to assign unique IDs to all operators of an application that might be upgraded in the future. This advice applies to all operators, i.e., operators with and without explicitly declared operator state, because some operators have internal state that is not visible to the user. Upgrading an application without assigned operator IDs is significantly more difficult and may only be possible via a low-level workaround using the setUidHash() method.
Important: As of 1.3.x this also applies to operators that are part of a chain.
By default all state stored in a savepoint must be matched to the operators of a starting application. However, users can explicitly agree to skip (and thereby discard) state that cannot be matched to an operator when starting a application from a savepoint. Stateful operators for which no state is found in the savepoint are initialized with their default state. Users may enforce best practices by calling ExecutionConfig#disableAutoGeneratedUIDs which will fail the job submission if any operator does not contain a custom unique ID.
#### Stateful Operators and User Functions
When upgrading an application, user functions and operators can be freely modified with one restriction. It is not possible to change the data type of the state of an operator. This is important because, state from a savepoint can (currently) not be converted into a different data type before it is loaded into an operator. Hence, changing the data type of operator state when upgrading an application breaks application state consistency and prevents the upgraded application from being restarted from the savepoint.
Operator state can be either user-defined or internal.
- User-defined operator state: In functions with user-defined operator state the type of the state is explicitly defined by the user. Although it is not possible to change the data type of operator state, a workaround to overcome this limitation can be to define a second state with a different data type and to implement logic to migrate the state from the original state into the new state. This approach requires a good migration strategy and a solid understanding of the behavior of key-partitioned state.
- Internal operator state: Operators such as window or join operators hold internal operator state which is not exposed to the user. For these operators the data type of the internal state depends on the input or output type of the operator. Consequently, changing the respective input or output type breaks application state consistency and prevents an upgrade. The following table lists operators with internal state and shows how the state data type relates to their input and output types. For operators which are applied on a keyed stream, the key type (KEY) is always part of the state data type as well.
[table: 7 rows]
#### Application Topology
Besides changing the logic of one or more existing operators, applications can be upgraded by changing the topology of the application, i.e., by adding or removing operators, changing the parallelism of an operator, or modifying the operator chaining behavior.
When upgrading an application by changing its topology, a few things need to be considered in order to preserve application state consistency.
- Adding or removing a stateless operator: This is no problem unless one of the cases below applies.
- Adding a stateful operator: The state of the operator will be initialized with the default state unless it takes over the state of another operator.
- Removing a stateful operator: The state of the removed operator is lost unless another operator takes it over. When starting the upgraded application, you have to explicitly agree to discard the state.
- Changing of input and output types of operators: When adding a new operator before or behind an operator with internal state, you have to ensure that the input or output type of the stateful operator is not modified to preserve the data type of the internal operator state (see above for details).
- Changing operator chaining: Operators can be chained together for improved performance. When restoring from a savepoint taken since 1.3.x it is possible to modify chains while preserving state consistency. It is possible a break the chain such that a stateful operator is moved out of the chain. It is also possible to append or inject a new or existing stateful operator into a chain, or to modify the operator order within a chain. However, when upgrading a savepoint to 1.3.x it is paramount that the topology did not change in regards to chaining. All operators that are part of a chain should be assigned an ID as described in the Matching Operator State section above.
### Table API & SQL
Due to the declarative nature of Table API & SQL programs, the underlying operator topology and state representation are mostly determined and optimized by the table planner.
Be aware that any change to both the query and the Flink version could lead to state incompatibility. Every new major-minor Flink version (e.g. 1.12 to 1.13) might introduce new optimizer rules or more specialized runtime operators that change the execution plan. However, the community tries to keep patch versions state-compatible (e.g. 1.13.1 to 1.13.2).
See the table state management section for more information.
## Upgrading the Flink Framework Version
This section describes the general way of upgrading Flink across versions and migrating your jobs between the versions.
In a nutshell, this procedure consists of 2 fundamental steps:
- Take a savepoint in the previous, old Flink version for the jobs you want to migrate.
- Resume your jobs under the new Flink version from the previously taken savepoints.
Besides those two fundamental steps, some additional steps can be required that depend on the way you want to change the Flink version. In this guide we differentiate two approaches to upgrade across Flink versions: in-place upgrade and shadow copy upgrade.
For in-place update, after taking savepoints, you need to:
- Stop/cancel all running jobs.
- Shutdown the cluster that runs the old Flink version.
- Upgrade Flink to the newer version on the cluster.
- Restart the cluster under the new version.
For shadow copy, you need to:
- Before resuming from the savepoint, setup a new installation of the new Flink version besides your old Flink installation.
- Resume from the savepoints with the new Flink installation.
- If everything runs ok, stop and shutdown the old Flink cluster.
#### Preconditions
Before starting the migration, please check that the jobs you are trying to migrate are following the best practices for savepoints.
In particular, we advise you to check that explicit uids were set for operators in your job.
This is a soft precondition, and restore should still work in case you forgot about assigning uids. If you run into a case where this is not working, you can manually add the generated legacy vertex ids from previous Flink versions to your job using the setUidHash(String hash) call. For each operator (in operator chains: only the head operator) you must assign the 32 character hex string representing the hash that you can see in the web ui or logs for the operator.
Besides operator uids, there are currently two hard preconditions for job migration that will make migration fail:
- We do not support migration for state in RocksDB that was checkpointed using semi-asynchronous mode. In case your old job was using this mode, you can still change your job to use fully-asynchronous mode before taking the savepoint that is used as the basis for the migration.
- Another important precondition is that all the savepoint data must be accessible from the new installation under the same (absolute) path. This also includes access to any additional files that are referenced from inside the savepoint file (the output from state backend snapshots), including, but not limited to additional referenced savepoints from modifications with the State Processor API.
#### STEP 1: Stop the existing job with a savepoint
`[code: $ bin/flink stop [--savepointPath :savepointPath] :jobId ... (56 chars)]`
If you want to trigger the savepoint in detached mode, add option -detached to the command.
#### STEP 2: Update your cluster to the new Flink version.
Replace the content of the Flink installation with the new version.
#### STEP 3: Resume the job under the new Flink version from savepoint.
`[code: $ bin/flink run -s :savepointPath [:runArgs] ... (44 chars)]`
## Compatibility Table
Savepoints are compatible across Flink versions as indicated by the table below:
Note: "Compatible" here refers specifically to compatibility of the internal data format in savepoints. It does not cover compatibility of SQL operators or other Upper-level changes. In practice, provided there are no changes to Flink SQL semantics, this state format compatibility typically also ensures job-level compatibility.
[table: 14 rows]

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/production_ready/
# Production Readiness Checklist
The production readiness checklist provides an overview of configuration options that should be carefully considered before bringing an Apache Flink job into production. While the Flink community has attempted to provide sensible defaults for each configuration, it is important to review this list and ensure the options chosen are sufficient for your needs.
### Set An Explicit Max Parallelism
The max parallelism, set on a per-job and per-operator granularity, determines the maximum parallelism to which a stateful operator can scale. There is currently no way to change the maximum parallelism of an operator after a job has started without discarding that operators state. The reason maximum parallelism exists, versus allowing stateful operators to be infinitely scalable, is that it has some impact on your application's performance and state size. Flink has to maintain specific metadata for its ability to rescale state which grows linearly with max parallelism. In general, you should choose max parallelism that is high enough to fit your future needs in scalability, while keeping it low enough to maintain reasonable performance.
  Maximum parallelism must fulfill the following conditions: 0 < parallelism  <= max parallelism <= 2^15
You can explicitly set maximum parallelism by using setMaxParallelism(int maxparallelism). If no max parallelism is set Flink will decide using a function of the operators parallelism when the job is first started:
- 128 : for all parallelism <= 128.
- MIN(nextPowerOfTwo(parallelism + (parallelism / 2)), 2^15) : for all parallelism > 128.
### Set UUIDs For All Operators
As mentioned in the documentation for savepoints, users should set uids for each operator in their DataStream. Uids are necessary for Flink's mapping of operator states to operators which, in turn, is essential for savepoints. By default, operator uids are generated by traversing the JobGraph and hashing specific operator properties. While this is comfortable from a user perspective, it is also very fragile, as changes to the JobGraph (e.g., exchanging an operator) results in new UUIDs. To establish a stable mapping, we need stable operator uids provided by the user through setUid(String uid).
### Choose The Right State Backend
See the description of state backends for choosing the right one for your use case.
### Choose The Right Checkpoint Interval
Checkpointing is Flink's primary fault-tolerance mechanism, wherein a snapshot of your job's state persisted periodically to some durable location. In the case of failure, Flink will restart from the most recent checkpoint and resume processing. A jobs checkpoint interval configures how often Flink will take these snapshots. While there is no single correct answer on the perfect checkpoint interval, the community can guide what factors to consider when configuring this parameter.
- What is the SLA of your service: Checkpoint interval is best understood as an expression of the jobs service level agreement (SLA). In the worst-case scenario, where a job fails one second before the next checkpoint, how much data can you tolerate reprocessing? A checkpoint interval of 5 minutes implies that Flink will never reprocess more than 5 minutes worth of data after a failure.
- How often must your service deliver results: Exactly once sinks, such as Kafka or the FileSink, only make results visible on checkpoint completion. Shorter checkpoint intervals make results available more quickly but may also put additional pressure on these systems. It is important to work with stakeholders to find a delivery time that meet product requirements without putting undue load on your sinks.
- How much load can your Task Managers sustain: All of Flinks' built-in state backends support asynchronous checkpointing, meaning the snapshot process will not pause data processing. However, it still does require CPU cycles and network bandwidth from your machines. Incremental checkpointing can be a powerful tool to reduce the cost of any given checkpoint.
And most importantly, test and measure your job. Every Flink application is unique, and the best way to find the appropriate checkpoint interval is to see how yours behaves in practice.
### Configure JobManager High Availability
The JobManager serves as a central coordinator for each Flink deployment, being responsible for both scheduling and resource management of the cluster. It is a single point of failure within the cluster, and if it crashes, no new jobs can be submitted, and running applications will fail. Configuring High Availability, in conjunction with Apache Zookeeper or Flinks Kubernetes based service, allows for a swift recovery and is highly recommended for production setups.
### Secure Flink Cluster Access
Flink is intentionally designed to support remote code execution. To mitigate risks of malicious code execution, ensure that Flink clusters are properly secured. Avoid exposing clusters directly to the public internet. Instead, restrict access via company intranet or secure clusters with TLS, authentication, role-based access control (RBAC) etc. For more details, see the Flink Security FAQ.
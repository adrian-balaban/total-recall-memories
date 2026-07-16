---
title: 'Apache Flink 2.3 docs — Deployment memory (TM/JM setup, tuning, troubleshooting, migration)'
tags: [org, flink, flink-2.3, docs, deployment, memory, taskmanager, jobmanager, tuning, troubleshooting, migration, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:55:27.081Z'
updated: '2026-07-08T04:55:27.081Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 deployment memory configuration deep-dive — TaskManager memory setup, JobManager memory setup, memory tuning guide, memory troubleshooting, and the migration guide from legacy (≤1.9 TM / ≤1.10 JM) to the modern memory model (≥1.10 TM / ≥1.11 JM). **WHY this matters:** memory misconfiguration is the single most common cause of Flink startup failures (IllegalConfigurationException from conflicting subsets), container kills on Yarn/K8s (unaccounted native/RocksDB memory), and OutOfMemoryErrors. This page set is the authoritative reference for sizing each component and for migrating old configs. Captured near-verbatim from nightlies.apache.org/flink/flink-docs-release-2.3/docs (English only) as an org reference; code blocks condensed to `[code: <first-line> ... (N chars)]`, tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_setup_tm/

# Set up TaskManager Memory

TM runs user code. Config applicable since 1.10. Only for TaskManagers (more sophisticated than JM).

## Configure Total Memory
Total process memory = total Flink memory (JVM Heap + managed memory + other direct/native) + JVM overhead. Local execution (IDE, no cluster): only a subset relevant. Simplest: configure total memory; rest adjusted automatically.

## Configure Heap and Managed Memory
Alternative: specify both task heap AND managed memory explicitly (more control). If done, set NEITHER total process nor total Flink memory (conflicts). 

### Task (Operator) Heap Memory
`taskmanager.memory.task.heap.size` — guarantees JVM Heap for operators running user code; added to JVM Heap.

### Managed Memory
Managed by Flink, native (off-heap). Used by: RocksDB state backend (streaming); sorting/hash tables/caching intermediate results (streaming+batch); Python UDF processes. Size via `taskmanager.memory.managed.size` (explicit) OR `taskmanager.memory.managed.fraction` (of total Flink memory). Size overrides fraction; default fraction if neither set.

#### Consumer Weights
`taskmanager.memory.managed.consumer-weights` — share managed memory across consumer types. Valid types: OPERATOR (built-in algorithms), STATE_BACKEND (RocksDB streaming), PYTHON (Python processes). E.g. STATE_BACKEND:70,PYTHON:30 → 70%/30%. Flink reserves only for types the job actually uses (heap state backend doesn't consume managed → all goes to Python). Warning: types NOT in weights get NO reservation → allocation failures if needed. Default: all types included.

## Configure Off-heap Memory (direct or native)
User-code off-heap → `taskmanager.memory.task.off-heap.size`. Framework off-heap adjustable only if Flink needs more. Framework + task off-heap included in JVM direct memory limit. Note: native non-direct memory counted as off-heap → higher JVM direct limit. Network memory also part of JVM direct but managed by Flink (never exceeds configured size; resizing won't help).

## Detailed Memory Model
[table: 9 rows] — all TM components + config options affecting each.

## Framework Memory
Don't change framework heap/off-heap without reason (high parallelism, Hadoop deps may need more). Note: Flink doesn't isolate framework vs task heap/off-heap yet (future optimization).

## Local Execution
Single java program no cluster → all components ignored except [table: 5 rows]. Task heap/off-heap infinite (Long.MAX_VALUE); managed memory default 128MB local. Task heap size NOT real heap (depends on JVM args -Xmx/-Xms).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_setup_jobmanager/

# Set up JobManager Memory

JM = ResourceManager + Dispatcher + one JobMaster per running job. Config applicable since 1.11. Simpler than TM.

## Configure Total Memory
Simplest: configure total process memory. Local execution → no need (options ignored).

## Detailed configuration
[table: 5 rows].

### Configure JVM Heap
`jobmanager.memory.heap.size` — used by Flink framework + user code (job submission batch sources, checkpoint completion callbacks). Size driven by number/structure of running jobs. If set explicitly, set NEITHER total process nor total Flink memory (conflicts). Flink scripts set via -Xms/-Xmx.

### Configure Off-heap Memory
`jobmanager.memory.off-heap.size` — JVM direct + native. Enable JVM Direct limit via `jobmanager.memory.enable-jvm-direct-memory-limit` (sets -XX:MaxDirectMemorySize). Sources: Flink framework deps (Pekko network), user code (job submission/checkpoint callbacks). If Total Flink Memory + JVM Heap set but Off-heap NOT → Off-heap derived as Total Flink − Heap (default ignored).

## Local Execution
JM memory config ignored.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_tuning/

# Memory tuning guide

## Configure memory for standalone deployment
Configure total Flink memory (`taskmanager.memory.flink.size` / `jobmanager.memory.flink.size`) or components; adjust JVM metaspace if problematic. Total process memory NOT relevant (JVM overhead not controlled; only physical machine resources matter).

## Configure memory for containers
Configure total process memory (`taskmanager.memory.process.size` / `jobmanager.memory.process.size`) for K8s/Yarn = requested container size. If total Flink memory configured, Flink implicitly adds JVM components → derived total process memory → requests that container. WARNING: unmanaged off-heap (native) beyond container size → job killed by deployment env.

## Configure memory for Netty4
Pekko update → Flink RPC uses Netty4 (pooled byte buffers, better perf, slightly more memory).
### Configure byte buffer allocator type
Affects BOTH Flink RPC and shuffle. JVM property `org.apache.flink.shaded.netty4.io.netty.allocator.type`: pooled (PooledByteBufAllocator.DEFAULT), unpooled (UnpooledByteBufAllocator.DEFAULT), adaptive (AdaptiveByteBufAllocator). Example `[code: # In <flink-root-dir>/conf/config.yaml ... (230 chars)]`.
### Enable reflection in JDK >= 11
Default `--add-opens=java.base/java.lang.reflect=ALL-UNNAMED`. `org.apache.flink.shaded.netty4.io.netty.tryReflectionSetAccessible` enables optimizations reducing GC pressure. Example `[code: ... (246 chars)]`.

## Configure memory for state backends (TaskManagers only)
### HashMap state backend
Stateless/HashMapStateBackend → set managed memory to zero (max heap for user code).
### RocksDB state backend
EmbeddedRocksDBStateBackend uses native memory; default limits to managed memory size. Reserve enough managed memory; disabling control risks TM kills in containerized deployments (RocksDB above container = total process memory). See `state.backend.rocksdb.memory.managed`.

## Configure memory for batch jobs (TaskManagers only)
Batch operators use managed memory (some ops on raw data without deserialization). Flink allocates up to configured managed memory, never beyond (prevents OOM); spills to disk if insufficient.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_trouble/

# Troubleshooting

## IllegalConfigurationException
From TaskExecutorProcessUtils/JobManagerProcessUtils → invalid value (negative size, fraction >1) or config conflicts. Check docs for components in exception.

## OutOfMemoryError: Java heap space
JVM Heap too small. Increase total memory, or directly task heap (TM) / JVM Heap (JM). Framework heap (TM) only if Flink framework needs more.

## OutOfMemoryError: Direct buffer memory
JVM direct limit too small or direct memory leak. Check user code/deps; increase direct off-heap memory. See off-heap config for TM/JM + JVM args.

## OutOfMemoryError: Metaspace
JVM metaspace too small. Increase `*.memory.jvm-metaspace.size` (TM/JM).

## IOException: Insufficient number of network buffers (TM only)
Network memory too small. Increase `taskmanager.memory.network.min` / `.max` / `.fraction`.

## Container Memory Exceeded
Container allocates beyond requested size (Yarn/K8s) → not enough native reserved. Observe via monitoring or kill error messages. JM: enable `jobmanager.memory.enable-jvm-direct-memory-limit` to exclude direct leak. RocksDBStateBackend: if control disabled → increase managed memory; if control enabled + non-heap grows during savepoint/full checkpoints → glibc allocator bug → set `MALLOC_ARENA_MAX=1` for TMs. Alternatively increase JVM Overhead.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_migration/

# Migration Guide

Memory setup changed in 1.10 (TM) / 1.11 (JM). Many options removed/changed semantics. Migrate TM ≤1.9→≥1.10, JM ≤1.10→≥1.11. Review required — legacy vs new can change component sizes → behavior/perf/failures. Pre-1.10/1.11 all options had defaults; new config requires at least one subset of [table: 4 rows] configured explicitly or fails. Default config sets `taskmanager.memory.process.size` (since 1.10) + `jobmanager.memory.process.size` (since 1.11).

## Migrate Task Manager Memory Configuration
### Changes in Configuration Options (1.10)
Completely removed (ignored if used) [table: 4 rows]. Deprecated but interpreted as new (backwards compat) [table: 6 rows]. Network memory verify (fraction base may change). Container cut-off options (`containerized.heap-cutoff-ratio`, `containerized.heap-cutoff-min`) no effect for TMs.

### Total Memory (Previously Heap Memory)
Legacy `taskmanager.heap.size`/`.mb` included heap + off-heap (despite naming) → deprecated. Without new options, translated to: total Flink memory (`taskmanager.memory.flink.size`) for standalone; total process memory (`taskmanager.memory.process.size`) for containerized (Yarn).

### JVM Heap Memory
Previously heap = managed (if on-heap) + rest. Now if only total Flink/process configured → JVM Heap = total − all other components. Direct control via `taskmanager.memory.task.heap.size`. Also used by heap state backends (MemoryStateBackend/FsStateBackend). Framework heap always reserved (`taskmanager.memory.framework.heap.size`).

### Managed Memory
#### Explicit Size
`taskmanager.memory.size` → renamed `taskmanager.memory.managed.size` (deprecated old).
#### Fraction
Legacy `taskmanager.memory.fraction` (of total minus network minus container cut-off, Yarn only) → completely removed. Use `taskmanager.memory.managed.fraction` (of total Flink memory, if size not set).
#### RocksDB state
RocksDB native consumption now in managed memory; limited by managed size (prevents Yarn container kills). Disable via `state.backend.rocksdb.memory.managed=false`.
#### Other changes
Managed memory always off-heap now (`taskmanager.memory.off-heap` removed). Uses native (not direct) → NOT in JVM direct limit. Always lazily allocated (`taskmanager.memory.preallocate` removed).

## Migrate Job Manager Memory Configuration
Legacy `jobmanager.heap.size`/`.mb` = JVM Heap for standalone; for containerized (K8s/Yarn) included off-heap, reduced by container cut-off (removed after 1.11). Deprecated. Without new options → JVM Heap (`jobmanager.memory.heap.size`) standalone; total process memory (`jobmanager.memory.process.size`) containerized. Now if only total Flink/process → JVM Heap derived as rest. Direct control via `jobmanager.memory.heap.size`.

## Flink JVM process memory limits
Since 1.10 Flink sets JVM Metaspace + Direct limits for TM (JVM args). Since 1.11 also JM Metaspace. JM Direct limit via `jobmanager.memory.enable-jvm-direct-memory-limit`. Simplifies leak debugging, avoids container OOM.

## Container Cut-Off Memory
Previously for containerized: cut-off memory for unaccounted allocations (RocksDB, JVM internals). No longer available; `containerized.heap-cutoff-ratio`/`-min` no effect. New specific components replace it.
### for TaskManagers
RocksDB native → managed memory (limited by managed size). Other off-heap via: task off-heap (`taskmanager.memory.task.off-heap.size`), framework off-heap (`taskmanager.memory.framework.off-heap.size`), JVM metaspace (`taskmanager.memory.jvm-metaspace.size`), JVM overhead.
### for JobManagers
Off-heap via: off-heap (`jobmanager.memory.off-heap.size`), JVM metaspace (`jobmanager.memory.jvm-metaspace.size`), JVM overhead.

## Default Configuration in Flink configuration file
TM total: `taskmanager.heap.size` → `taskmanager.memory.process.size`, value 1024MB→1728MB. JM total: `jobmanager.heap.size` → `jobmanager.memory.process.size`, value 1024MB→1600MB. WARNING: new default can change component sizes → performance changes.
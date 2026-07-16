---
title: 'Apache Flink 2.3 docs — Dev DataStream Joining, Process Function, Async I/O, Full Window Partition'
tags: [org, flink, flink-2.3, docs, dev, datastream-api, joining, process-function, async-io, timers, coprocessfunction, full-window-partition, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:26:05.802Z'
updated: '2026-07-08T04:26:05.802Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/joining/
# Joining

## Window Join
Joins elements of two streams sharing common key and lying in same window (defined by window assigner, evaluated on elements from both streams). Elements from both sides passed to user-defined JoinFunction or FlatJoinFunction. Semantics: behaves like inner-join (elements from one stream not emitted if no corresponding element in other); joined elements get timestamp = largest timestamp still in window (e.g. window [5,10) → timestamp 9).
- Tumbling Window Join: all elements with common key + common tumbling window joined as pairwise combinations. Inner join — elements without match in tumbling window not emitted.
- Sliding Window Join: common key + common sliding window, pairwise. Some elements joined in one sliding window but not another.
- Session Window Join: same key that "combined" fulfill session criteria joined pairwise. Inner join — session with only one stream's elements → no output.

## Interval Join
Joins elements of two streams A & B with common key where B's timestamps lie in relative time interval to A's: b.timestamp ∈ [a.timestamp+lowerBound; a.timestamp+upperBound]. Both bounds can be negative/positive as long as lowerBound <= upperBound. Currently only inner joins. Pair passed to ProcessJoinFunction gets larger timestamp (via Context). Currently only supports event time. Bounds inclusive by default; .lowerBoundExclusive()/.upperBoundExclusive() to change.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
# Process Function

## The ProcessFunction
Low-level stream processing operation giving access to basic building blocks of all (acyclic) streaming applications: events (stream elements); state (fault-tolerant, consistent, only on keyed stream); timers (event time and processing time, only on keyed stream). Like FlatMapFunction with access to keyed state and timers; invoked for each event. Keyed state via RuntimeContext. Timers react to processing/event time changes. Each processElement(...) gets Context (element's event time timestamp, TimerService). TimerService registers callbacks for future event/processing-time instants. Event-time timers: onTimer(...) called when current watermark advanced up to/beyond timer timestamp. Processing-time timers: onTimer(...) when wall clock reaches specified time. During onTimer, all states scoped to key with which timer created. Keyed state + timers require applying ProcessFunction on keyed stream: stream.keyBy(...).process(new MyProcessFunction()).

## Low-level Joins
CoProcessFunction or KeyedCoProcessFunction — bound to two inputs, individual processElement1(...)/processElement2(...) calls. Pattern: create state object for one/both inputs; update state upon receiving elements from its input; upon receiving elements from other input, probe state and produce joined result. For complete/deterministic joins with out-of-order events, use timer to evaluate/emit join when watermark for one stream passed time of the other.

## Example
KeyedProcessFunction maintains counts per key, emits key/count pair whenever a minute passes (event time) without update: count/key/last-modification-timestamp in ValueState (implicitly scoped by key); for each record increment counter + set last-modification timestamp; schedule callback one minute into future (event time); on callback check timestamp against last-modification, emit if match. (Could have used session windows; used to illustrate pattern.) Note: before Flink 1.4.0, processing-time timer onTimer() set current processing time as event-time timestamp — harmful (indeterministic, not aligned with watermarks); fixed in 1.4.0, jobs using incorrect timestamp fail on upgrade.

## The KeyedProcessFunction
Extension of ProcessFunction — gives access to key of timers in onTimer(...).

## Timers
Both types internally maintained by TimerService, enqueued for execution. TimerService deduplicates timers per key and timestamp (at most one timer per key+timestamp; multiple registrations for same timestamp → onTimer() called once). Flink synchronizes onTimer() and processElement() — no concurrent modification of state worries.

### Fault Tolerance
Timers fault tolerant, checkpointed with state; restored on failure recovery/savepoint start. Checkpointed processing-time timers supposed to fire before restoration fire immediately. Timers always asynchronously checkpointed, EXCEPT RocksDB backend + incremental snapshots + heap-based timers (FLINK-10026). Large numbers of timers increase checkpointing time (timers part of checkpointed state) — see Timer Coalescing.

### Timer Coalescing
Only one timer per key+timestamp → reduce timers by reducing timer resolution. E.g. 1-second resolution: round down target time to full seconds; fire at most 1 second earlier, not later than millisecond accuracy; at most one timer per key per second. Event-time timers only fire with watermarks → schedule/coalesce with next watermark using current one: ctx.timerService().currentWatermark() + 1. Timers can be stopped/removed (processing-time and event-time); stopping has no effect if no such timer registered.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/asyncio/
# Asynchronous I/O for External Data Access

FLIP-12 (design), FLIP-232 (retry support).

## The need for Asynchronous I/O Operations
Interacting with external systems (e.g. enriching stream events with DB data): communication delay must not dominate total work. Naive synchronous access in MapFunction (request → wait for response) → waiting vast majority of time. Async interaction → single parallel function instance handles many requests concurrently, receives responses concurrently; waiting overlaid with other requests; much higher throughput. Scaling MapFunction to high parallelism possible but high resource cost (more tasks/threads/Flink network connections/DB connections/buffers/bookkeeping).

## Prerequisites
Requires a DB client supporting async requests. Without one, turn synchronous client into limited concurrent client via multiple clients + thread pool (usually less efficient than proper async client).

## Async I/O API
Three parts: AsyncFunction implementation dispatching requests; callback taking result → ResultFuture (Java) / await result (Python); applying async I/O on DataStream with/without retry. ResultFuture completed with first ResultFuture.complete call (Java); subsequent calls ignored. Three control parameters:
- Timeout: max duration from first invocation to final completion (may include multiple retry attempts); guards against dead/failed requests.
- Capacity: how many async requests in progress per parallel instance (subtask); limits backlog, triggers backpressure when exhausted.
- AsyncRetryStrategy: conditions triggering delayed retry + delay strategy (fixed-delay, exponential-backoff, custom).

### Timeout Handling
Default: timeout → exception, job restarted. Override AsyncFunction#timeout to handle. Java: call ResultFuture.complete()/completeExceptionally() (or complete(Collections.emptyList()) to emit nothing). Python: return collection or raise exception (return [] for nothing).

### Order of Results
Concurrent requests complete in undefined order. Two modes:
- Unordered: result records emitted as soon as request finishes; stream order different after operator; lowest latency/overhead with processing time. AsyncDataStream.unorderedWait(...).
- Ordered: stream order preserved; result emitted in same order as requests triggered; operator buffers result until all preceding records emitted/timed out; extra latency + checkpointing overhead. AsyncDataStream.orderedWait(...).

### Event Time
With event time, watermarks handled correctly:
- Unordered: watermarks don't overtake records and vice versa (order boundary); records emitted unordered only between watermarks; record after watermark emitted only after that watermark emitted; watermark emitted only after all result records from inputs before it emitted. With watermarks, unordered mode introduces some same latency/overhead as ordered (depends on watermark frequency).
- Ordered: order of watermarks and records preserved; no significant overhead change vs processing time. (Ingestion Time = special case of event time with auto-generated watermarks from source processing time.)

### Fault Tolerance Guarantees
Full exactly-once. Stores records for in-flight async requests in checkpoints; restores/re-triggers requests on failure recovery.

### Retry Support
Built-in mechanism transparent to user's AsyncFunction. AsyncRetryStrategy: retry condition AsyncRetryPredicate + interfaces to determine whether to continue retry and retry interval based on attempt number. After trigger condition met, may abandon retry (attempt exceeds limit) or force-terminate at task end (last result/exception = final state). AsyncRetryPredicate: triggered based on return result or execution exception.

### Implementation Tips
Futures with Executor for callbacks → use DirectExecutor (callback does minimal work, avoids thread-to-thread handover; callback only hands result to ResultFuture → output buffer; heavy logic in dedicated thread-pool). DirectExecutor via org.apache.flink.util.concurrent.Executors.directExecutor() or com.google.common.util.concurrent.MoreExecutors.directExecutor(). (Java only; Python just awaits result.)

### Caveats
AsyncFunction NOT called multi-threaded — only one instance, called sequentially per record in partition. Unless asyncInvoke(...) returns fast and relies on callback, not proper async I/O. Blocking patterns: DB client whose lookup blocks until result; blocking/waiting on future-type objects inside asyncInvoke. AsyncFunction(AsyncWaitOperator) usable anywhere except cannot chain to SourceFunction/SourceStreamTask.
May need larger queue capacity if retry enabled: approx max = inputRate * retryRate * avgRetryDuration. E.g. 100 rec/sec * 1% * 60s = 60. Ordered mode: head element key point — longer uncompleted → longer processing delay; retry may increase incomplete time of head element. Growing queue capacity (eases backpressure) increases OOM risk; ListState theoretical upper limit Integer.MAX_VALUE but don't increase too big in production — increase task parallelism instead.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/full_window_partition/
# Full Window Partition Processing on DataStream

FLIP-380. Both keyed and non-keyed DataStream can directly transform into PartitionWindowedStream. PartitionWindowedStream = collecting all records of each subtask separately into a full window. Four APIs: mapPartition, sortPartition, aggregate, reduce.
- MapPartition: collect all records of each subtask into full window, process with MapPartitionFunction within each subtask (called at end of inputs). E.g. sum of elements per subtask.
- SortPartition: collect all records per subtask into full window, sort by given comparator at end of inputs.
- Aggregate: collect all records per subtask into full window, apply AggregateFunction (called per element, incremental).
- Reduce: apply reduce transformation on all records in partition; ReduceFunction called for every record in window.
---
title: Apache Flink 2.3 docs — Dev DataStream State V2 (Async APIs) and Broadcast State Pattern
tags: [org, flink, flink-2.3, docs, dev, datastream-api, state-v2, async-state, forst, broadcast-state, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:21:33.739Z'
updated: '2026-07-08T04:21:33.739Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_v2/
# Working with State V2 (New APIs)

The new state API is more flexible than the previous API. User can perform asynchronous state operations, making it more powerful and efficient. Asynchronous state access is essential for the state backend to handle large state sizes and spill to remote file systems when necessary — called "disaggregated state management".

## Keyed DataStream

Specify a key via keyBy(KeySelector) → KeyedStream. Perform enableAsyncState() on the KeyedStream to enable asynchronous state operations. Key selector takes a single record and returns the key (any type, deterministic). Keys are "virtual" (functions over actual data).

## Using Keyed State V2

The new state API is designed for asynchronous state access. Each state type gives two API versions: synchronous (blocking, waits for completion) and asynchronous (non-blocking, returns a StateFuture completed when state access done). Asynchronous API is more efficient and should be used whenever possible. Highly not recommended to mix synchronous and asynchronous state access in the same user function. New API set only available on KeyedStream with enableAsyncState() invoked.

### The Return Values

StateFuture<T>: completed with result of state access. Methods:
- thenAccept(Consumer<T>) → StateFuture<Void> (called with result; finished when Consumer done).
- thenApply(Function<T,R>) → StateFuture<R> (return value of function is result of following StateFuture).
- thenCompose(Function<T,StateFuture<R>>) → StateFuture<R> (function returns a StateFuture; finished when inner finished).
- thenCombine(StateFuture<U>, BiFunction<T,U,R>) → StateFuture<R> (called with results of both; finished when BiFunction done).

Similar to CompletableFuture. StateFuture does NOT provide get() to block (blocking may cause recursive blocking). Also provides conditional versions: thenConditionallyAccept/Apply/Compose/Combine (logic split into two branches based on result).

StateIterator<T>: iterate over state elements. Methods:
- boolean isEmpty() (synchronous).
- StateFuture<Void> onNext(Consumer<T>) (called with next element); function version StateFuture<Collection<R>> onNext(Function<T,R>).

StateFutureUtils utility methods:
- completedFuture(T) — completed StateFuture with given value (useful in thenCompose for constants).
- completedVoidFuture() — completed StateFuture with null.
- combineAll(Collection<StateFuture<T>>) → StateFuture<Collection<T>> (completed when all input futures completed).
- toIterable(StateFuture<StateIterator<T>>) → StateFuture<Iterable<T>> (no good reason to do so — may disable lazy loading; only when further calculation depends on whole data).

### State Primitives

- ValueState<T>: asyncUpdate(T), StateFuture<T> asyncValue().
- ListState<T>: asyncAdd(T) / asyncAddAll(List<T>), StateFuture<StateIterator<T>> asyncGet(), asyncUpdate(List<T>).
- ReducingState<T>: asyncAdd(T) reduces via specified ReduceFunction.
- AggregatingState<IN,OUT>: asyncAdd(IN) aggregates via specified AggregateFunction (aggregate type may differ).
- MapState<UK,UV>: asyncPut(UK,UV) / asyncPutAll(Map), asyncGet(UK), asyncEntries()/asyncKeys()/asyncValues(), asyncIsEmpty().

All have asyncClear() (clears state for active key). Use StateDescriptors under org.apache.flink.api.common.state.v2 package (note v2). Accessed via RuntimeContext (rich functions only): getState/getReducingState/getListState/getAggregatingState/getMapState. CountWindowAverage example: keys by first field, stores count and running sum in ValueState, emits average and clears when count reaches 2.

### Execution Order

State access methods executed asynchronously (do not block current thread). With synchronous APIs, executed in order called; with asynchronous APIs, executed out of order (especially for different incoming elements). For flatMap invoked for elements A and B: asyncGet for A then asyncGet for B, but finish order not guaranteed → continuation order not guaranteed → asyncClear/asyncUpdate order not determined. However, user code in processElement/flatMap/thenXXxx following state access runs in a single thread (the task thread) — no concurrency issue for user code.

Rules Flink ensures:
- Execution order of user code entry flatMap for same-key elements strictly in order of element arrival.
- Consumers/functions passed to thenXXxx executed in order they are chained. If not chained or multiple chains, order not guaranteed.

### Best practice of asynchronous APIs

- Avoid mixing synchronous and asynchronous state access.
- Use chaining of thenXXxx methods to handle results and chain further state access. Divide logic into multiple steps split by thenXXxx.
- Avoid accessing mutable members of the user function (RichFlatMapFunction). Since state access executes out of order, mutable members may be accessed unpredictably. Instead use result of state access to pass data between steps (StateFutureUtils.completedFuture or thenApply), or use a captured container (AtomicReference) initialized for each flatMap invoke to share between lambdas.

### State Time-To-Live (TTL)

TTL assignable to keyed state of any type; expired values cleaned up on best-effort basis. All state collection types support per-entry TTLs (list elements and map entries expire independently). Build StateTtlConfig and enable in any state descriptor.

Config options:
- First param of newBuilder: time-to-live value (mandatory).
- Update type (default OnCreateAndWrite): OnCreateAndWrite or OnReadAndWrite. [Note: with StateVisibility.ReturnExpiredIfNotCleanedUp, state read cache disabled → PyFlink performance loss.]
- State visibility (default NeverReturnExpired): NeverReturnExpired (expired never returned; read/write cache disabled in PyFlink → performance loss) or ReturnExpiredIfNotCleanedUp.

Notes:
- State backends store timestamp of last modification → increases storage. Heap: additional Java object + primitive long. RocksDB/ForSt: 8 bytes per stored value, list entry, map entry.
- Only processing-time TTLs currently supported.
- Trying to restore state previously configured without TTL using TTL-enabled descriptor (or vice versa) leads to compatibility failure and StateMigrationException. (Note: differs from V1 — V2 does NOT yet have the 2.2.0 seamless migration.)
- TTL config not part of checkpoints/savepoints.
- Not recommended to restore checkpoint state with TTL adjusted short→long (potential data errors).
- Map state with TTL supports null user values only if serializer handles null (else NullableSerializer, +1 byte).
- With TTL enabled, deprecated defaultValue in StateDescriptor no longer takes effect.

#### Cleanup of Expired State

Default: expired values explicitly removed on read and periodically GC'd in background if supported. Background cleanup can be disabled. ForSt State Backend only cleans up state in compaction process. For other state backends and cleanup strategies, refer to State V1 documentation.

##### Cleanup during compaction (ForSt)
ForSt compaction filter called for background cleanup. ForSt periodically runs asynchronous compactions to merge state updates and reduce storage. Flink compaction filter checks expiration timestamp and excludes expired values. Queries current timestamp from Flink every N entries (cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries); default 1000). More frequent updates improve cleanup speed but decrease compaction performance (JNI call). Periodic compaction (cleanupInRocksdbCompactFilter(long, Duration periodicCompactionTime)) speeds up cleanup for rarely-accessed entries; files older than this value picked up and re-written to same level; default 30 days; 0 to turn off or small value to speed up (more compactions). Debug via log4j.logger.org.forstdb.FlinkCompactionFilter=DEBUG.
Notes:
- TTL filter during compaction slows it down (parses timestamp per stored state entry per key; for list/map, per stored element).
- With list state of non-fixed byte length elements, native TTL filter calls Flink java type serializer over JNI per state entry where at least first element expired.
- Can be activated/deactivated anytime.
- Periodic compaction only works when TTL enabled.

## Operator State

Operator State (non-keyed state) bound to one parallel operator instance (e.g. Kafka consumer maintains map of topic partitions and offsets). Supports redistribution among parallel instances when parallelism changed. In typical stateful applications you don't need it — mostly source/sink implementations and scenarios with no key to partition by.

## Broadcast State

Special type of Operator State. Records of one stream broadcasted to all downstream tasks to maintain same state among all subtasks, then accessed while processing records of a second stream (e.g. low-throughput rules stream evaluated against elements of another stream). Differs: map format; only available to specific operators with a broadcasted stream and a non-broadcasted one; such an operator can have multiple broadcast states with different names.

## Using Operator State

Implement CheckpointedFunction (snapshotState + initializeState). initializeState called on first init or recovery. List-style operator state supported. Redistribution schemes:
- Even-split: list evenly divided into as many sublists as parallel operators.
- Union: each operator gets the complete list. Do not use with high cardinality (offset per list entry → RPC framesize or OOM).
Access method naming: getUnionListState(descriptor) = union; getListState(descriptor) = even-split. Use isRestored() to check recovery.

## Migrate from the Old State API

Steps:
- Invoke enableAsyncState() on the KeyedStream.
- Replace StateDescriptors with new ones under v2 package; replace old state handles with new ones under v2 package.
- Rewrite old state access methods with new asynchronous ones.
- Recommended to use ForSt State Backend for the new state API (performs async state access). Other state backends only support synchronous execution (but can be used with new state API).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/broadcast_state/
# The Broadcast State Pattern

## Provided APIs

Example: stream of Item (Color, Shape) and a stream of Rules; find pairs of same color following a pattern (e.g. rectangle followed by triangle). Set of interesting patterns evolves over time.

Key the items by Color (same color → same physical machine). Broadcast the rules stream to all downstream tasks; tasks store them locally via a MapStateDescriptor to evaluate against incoming Items. Then: connect the two streams and specify match-detecting logic.

connect() on the non-broadcasted stream with BroadcastStream as argument → BroadcastConnectedStream, on which call process() with a special CoProcessFunction. Function type depends on non-broadcasted stream type:
- if keyed → KeyedBroadcastProcessFunction
- if non-keyed → BroadcastProcessFunction

### BroadcastProcessFunction and KeyedBroadcastProcessFunction

Two process methods: processBroadcastElement() (broadcasted stream) and processElement() (non-broadcasted). Differ in context: non-broadcast side has ReadOnlyContext; broadcasted side has Context. Both contexts:
- give access to broadcast state: ctx.getBroadcastState(MapStateDescriptor<K,V>) (Java) / ctx.get_broadcast_state (Python)
- query element timestamp: ctx.timestamp()
- current watermark: ctx.currentWatermark() / ctx.current_watermark()
- current processing time: ctx.currentProcessingTime() / ctx.current_processing_time()
- emit to side-outputs: ctx.output(OutputTag<X>, X) (Java) / yield output_tag, value (Python)

The stateDescriptor in getBroadcastState() should be identical to the one in .broadcast(ruleStateDescriptor). Broadcasted side has read-write access; non-broadcast side has read-only access. Reason: no cross-task communication in Flink; to guarantee broadcast state contents are the same across all parallel instances, read-write access only to the broadcast side (which sees same elements across all tasks), and computation on each incoming element must be identical across all tasks. Ignoring this breaks consistency guarantees → inconsistent, hard-to-debug results. The logic in processBroadcastElement() must have the same deterministic behavior across all parallel instances.

KeyedBroadcastProcessFunction (operating on keyed stream) exposes functionality not available to BroadcastProcessFunction:
- ReadOnlyContext in processElement() gives access to timer service → register event/processing time timers. When timer fires, onTimer() invoked with OnTimerContext (same as ReadOnlyContext plus ability to ask if timer was event or processing time, and query key associated with timer).
- Context in processBroadcastElement() contains applyToKeyedState(StateDescriptor, KeyedStateFunction) — register a KeyedStateFunction applied to all states of all keys associated with the descriptor. (apply_to_keyed_state not supported in PyFlink yet.)

Registering timers only possible at processElement() of KeyedBroadcastProcessFunction (not in processBroadcastElement() — no key associated with broadcasted elements).

## Important Considerations

- No cross-task communication: only broadcast side can modify broadcast state. User must ensure all tasks modify contents in the same way for each incoming element.
- Order of events in Broadcast State may differ across tasks: broadcasting guarantees all elements eventually go to all downstream tasks, but elements may arrive in different order. State updates for each incoming element MUST NOT depend on ordering of incoming events.
- All tasks checkpoint their broadcast state: although all tasks have same elements when checkpoint takes place (checkpoint barriers do not overpass elements), all tasks checkpoint (not just one) — design decision to avoid all tasks reading from same file during restore (avoid hotspots), at expense of increasing checkpointed state by factor of p (parallelism). Flink guarantees no duplicates and no missing data on restore/rescale. Recovery with same/smaller parallelism: each task reads its checkpointed state. Scaling up: each task reads its own state, remaining tasks (p_new-p_old) read checkpoints of previous tasks round-robin.
- No RocksDB state backend: broadcast state kept in-memory at runtime; memory provisioning accordingly. Holds for all operator states.
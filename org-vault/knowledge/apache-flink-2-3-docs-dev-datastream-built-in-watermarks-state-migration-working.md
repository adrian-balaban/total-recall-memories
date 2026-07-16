---
title: 'Apache Flink 2.3 docs — Dev DataStream Built-in Watermarks, State Migration, Working with State'
tags: [org, flink, flink-2.3, docs, dev, datastream-api, watermarks, state, ttl, operator-state, broadcast-state, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:20:58.857Z'
updated: '2026-07-08T04:20:58.857Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/built_in/
# Builtin Watermark Generators

Flink provides pre-implemented timestamp assigners / watermark generators to ease programming effort (implement WatermarkGenerator for custom). Their implementations also serve as examples.

## Monotonously Increasing Timestamps

Special case: timestamps seen by a given source task occur in ascending order → current timestamp can always act as the watermark. Only necessary that timestamps are ascending per parallel data source task (e.g., per Kafka partition if one partition read by one parallel source instance). Flink's watermark merging mechanism generates correct watermarks whenever parallel streams are shuffled, unioned, connected, or merged.
- Java: `WatermarkStrategy.forMonotonousTimestamps();`
- Python: `WatermarkStrategy.for_monotonous_timestamps()`

## Fixed Amount of Lateness

Periodic generation where watermark lags behind the max event-time timestamp by a fixed amount. Covers scenarios where max lateness is known in advance (e.g. custom source with timestamps spread within a fixed period for testing). Flink provides BoundedOutOfOrdernessWatermarks generator taking maxOutOfOrderness — the maximum amount of time an element is allowed to be late before being ignored when computing the final result for the window. Lateness = t_w - t (t = event-time timestamp of an element, t_w = previous watermark). If lateness > 0, element is considered late and by default ignored.
- Java: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));`
- Python: `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_migration/
# State TTL Migration Compatibility

Starting with Apache Flink 2.2.0, the system supports seamless enabling or disabling of State Time-to-Live (TTL) for existing state. This removes prior limitations where a change in TTL configuration could cause a StateMigrationException during restore. Full TTL state migration support across all major state backends available from Flink 2.2.0 onwards.

## Motivation

In earlier Flink versions, switching TTL on/off in a StateDescriptor resulted in incompatibility errors because TTL-enabled state used a different serialization format than non-TTL state.

## Compatibility Behavior

With changes introduced across versions 2.0.0 to 2.2.0: Flink can now restore state created without TTL using a descriptor with TTL enabled, and vice versa. Serializers and state backends transparently handle the presence or absence of TTL metadata.

## Limitations

- Changes to TTL parameters (expiration time, update behavior) are not always compatible; may require serializer migration.
- TTL is not applied retroactively. Existing entries restored from non-TTL state will only expire after their next access or update.
- This compatibility assumes no other incompatible changes to the state serializer.

## FAQ

- Can I disable TTL after previously enabled? Yes — Flink restores values and ignores any TTL expiration metadata.
- Supported in RocksDB and Heap backends? Yes — RocksDB since 2.1.0, Heap since 2.2.0, but ForSt not yet added.
- Which version fully supports TTL migration? Flink 2.2.0 (first version with all necessary support).
- Need to change savepoint? No — migration handled internally by Flink, provided serializers are otherwise compatible.
- Related: FLINK-32955.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/
# Working with State

## Keyed DataStream

To use keyed state, specify a key on a DataStream via keyBy(KeySelector) (Java) / key_by(KeySelector) (Python) → yields a KeyedStream allowing keyed-state operations. A key selector takes a single record and returns the key; key can be any type, must be derived from deterministic computations. Flink's data model is not based on key-value pairs — keys are "virtual" (functions over actual data to guide the grouping operator).

Tuple keys and expression keys (Java API only, not Python): specify keys using tuple field indices or expressions. Not recommended today; a KeySelector function is strictly superior (easy with Java lambdas, potentially less runtime overhead).

## Using Keyed State

Keyed state interfaces provide access to different state types scoped to the key of the current input element. Only usable on a KeyedStream. Available state primitives:
- ValueState<T>: keeps a value (one per key). update(T), T value().
- ListState<T>: keeps a list. add(T) / addAll(List<T>), Iterable<T> get(), update(List<T>).
- ReducingState<T>: keeps a single value = aggregation of all added values. add(T) reduces via specified ReduceFunction.
- AggregatingState<IN, OUT>: keeps a single value = aggregation; aggregate type may differ from added type. add(IN) aggregates via specified AggregateFunction.
- MapState<UK, UV>: keeps mappings. put(UK,UV) / putAll(Map), get(UK), entries()/keys()/values(), isEmpty().

All types have clear() — clears state for the currently active key. State objects are only for interfacing; state is not necessarily stored inside (may reside on disk). Value depends on key of input element (can differ across invocations if keys differ).

To get a state handle, create a StateDescriptor (holds name, type of values, possibly a user function). State is accessed using RuntimeContext — only possible in rich functions. RuntimeContext methods: getState(ValueStateDescriptor), getReducingState(...), getListState(...), getAggregatingState(...), getMapState(...). Example: CountWindowAverage RichFlatMapFunction keys tuples by first field, stores count and running sum in ValueState, emits average and clears state when count reaches 2.

### State Time-To-Live (TTL)

A TTL can be assigned to keyed state of any type. If a TTL is configured and a state value has expired, the stored value is cleaned up on a best-effort basis. All state collection types support per-entry TTLs (list elements and map entries expire independently). Build a StateTtlConfig and enable in any state descriptor.

Configuration options:
- First param of newBuilder: time-to-live value (mandatory).
- Update type (when TTL refreshed, default OnCreateAndWrite): OnCreateAndWrite (only on creation and write access) or OnReadAndWrite (also on read access). [Note: with StateVisibility.ReturnExpiredIfNotCleanedUp, state read cache disabled → performance loss in PyFlink.]
- State visibility (whether expired value returned on read if not cleaned up, default NeverReturnExpired): NeverReturnExpired (expired value never returned; useful for privacy-sensitive data; read/write cache disabled in PyFlink → performance loss) or ReturnExpiredIfNotCleanedUp (returned if still available).

Notes:
- State backends store timestamp of last modification along with user value → increases state storage. Heap: additional Java object with reference + primitive long. RocksDB: 8 bytes per stored value, list entry, or map entry.
- Only TTLs in reference to processing time currently supported.
- TTL configuration is not part of checkpoints/savepoints but a way Flink treats state in the currently running job.
- Not recommended to restore checkpoint state with adjusting TTL from short to long value (potential data errors).
- Map state with TTL supports null user values only if the user value serializer can handle null values (else wrap with NullableSerializer, +1 byte).
- With TTL enabled, the deprecated defaultValue in StateDescriptor no longer takes effect.

#### Cleanup of Expired State

Default: expired values explicitly removed on read (ValueState#value) and periodically garbage collected in background if supported by state backend. Background cleanup can be disabled. Heap backend relies on incremental cleanup; RocksDB uses compaction filter.

##### Cleanup in full snapshot
Activate cleanup at moment of taking full state snapshot → reduces size. Local state not cleaned up but won't include removed expired state on restoration. Configurable in StateTtlConfig. Not applicable for incremental checkpointing in RocksDB. Can be activated/deactivated anytime (e.g. after restart from savepoint).

##### Incremental cleanup
Trigger cleanup of some state entries incrementally. Trigger = callback from each state access and/or each record processing. Storage backend keeps a lazy global iterator over all entries; advanced on each trigger; traversed entries checked and expired ones cleaned up. Two params: number of checked entries per cleanup trigger (always per state access), and whether to trigger additionally per record processing. Default heap background cleanup checks 5 entries without cleanup per record processing.
Notes:
- If no access / no records processed, expired state persists.
- Time spent increases record processing latency.
- Only implemented for Heap state backend (no effect on RocksDB).
- With heap + synchronous snapshotting, global iterator keeps a copy of all keys (concurrent modifications unsupported) → increased memory. Asynchronous snapshotting has no issue.
- Can be activated/deactivated anytime.

##### Cleanup during RocksDB compaction
With RocksDB, a Flink-specific compaction filter called for background cleanup. RocksDB periodically runs asynchronous compactions to merge state updates and reduce storage. Flink compaction filter checks expiration timestamp and excludes expired values. Query current timestamp from Flink every N entries (configurable via cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries); default 1000). More frequent updates improve cleanup speed but decrease compaction performance (JNI call from native code). Periodic compaction (cleanupInRocksdbCompactFilter(long, Duration periodicCompactionTime)) speeds up cleanup for rarely-accessed entries; files older than this value picked up for compaction and re-written to same level; default 30 days; set 0 to turn off or small value to speed up (more compactions). Debug via log4j.logger.org.rocksdb.FlinkCompactionFilter=DEBUG.
Notes:
- TTL filter during compaction slows it down (parses timestamp of last access per stored state entry per key; for list/map, per stored element).
- With list state of non-fixed byte length elements, native TTL filter calls Flink java type serializer over JNI per state entry where at least first element expired (to determine offset of next unexpired element).
- Can be activated/deactivated anytime.
- Periodic compaction only works when TTL enabled.

### TTL Migration Compatibility

From Flink 2.2.0, supports seamless migration between state with and without TTL enabled. Safe to enable/disable without restore-time errors. (See separate TTL Migration Compatibility page.)

## Operator State

Operator State (non-keyed state) is bound to one parallel operator instance. Example: Kafka Connector — each parallel instance of the Kafka consumer maintains a map of topic partitions and offsets as its Operator State. Supports redistributing state among parallel operator instances when parallelism changed (different redistribution schemes). In typical stateful Flink applications you don't need operator state — mostly used in source/sink implementations and scenarios where there's no key to partition state by. Note: Operator state not supported in Python DataStream API.

## Broadcast State

Special type of Operator State. Supports use cases where records of one stream are broadcasted to all downstream tasks to maintain the same state among all subtasks, then accessed while processing records of a second stream (e.g. low-throughput stream of rules evaluated against elements of another stream). Differs from rest of operator states: it has a map format; only available to specific operators with a broadcasted stream and a non-broadcasted one as inputs; such an operator can have multiple broadcast states with different names.

## Using Operator State

A stateful function can implement CheckpointedFunction interface.

### CheckpointedFunction

Provides access to non-keyed state with different redistribution schemes. Requires snapshotState(FunctionSnapshotContext) and initializeState(FunctionInitializationContext). snapshotState() called on checkpoint; initializeState() called every time the user-defined function is initialized (first init or recovering from earlier checkpoint) — so it's where state is initialized AND where recovery logic lives.

Currently list-style operator state is supported (List of serializable objects, independent, eligible for redistribution upon rescaling). Redistribution schemes:
- Even-split redistribution: each operator returns a List; whole state = concatenation; on restore, list evenly divided into as many sublists as parallel operators; each gets a sublist (possibly empty). E.g. parallelism 1 with [element1, element2] → parallelism 2: element1 → instance 0, element2 → instance 1.
- Union redistribution: each operator returns a List; whole state = concatenation; on restore, each operator gets the complete list. Do not use if list may have high cardinality (checkpoint metadata stores offset to each list entry → RPC framesize or OOM errors).

Access method naming: redistribution pattern followed by state structure. getUnionListState(descriptor) = union scheme. getListState(descriptor) (no pattern) = basic even-split. Use context.isRestored() to check if recovering. BufferingSink example: ListState cleared of previous checkpoint objects then filled with new ones in snapshotState(). Keyed state can also be initialized in initializeState() via FunctionInitializationContext.

### Stateful Source Functions

Stateful sources require more care: to make state updates and output collection atomic (required for exactly-once on failure/recovery), user must get a lock from the source's context (CounterSource example). For operators needing info when a checkpoint is fully acknowledged, see org.apache.flink.api.common.state.CheckpointListener.
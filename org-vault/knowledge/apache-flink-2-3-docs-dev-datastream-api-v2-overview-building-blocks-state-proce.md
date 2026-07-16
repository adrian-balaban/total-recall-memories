---
title: 'Apache Flink 2.3 docs — Dev DataStream API V2 (Overview, Building Blocks, State Processing, Timer Services, Windows, Joining, Watermark)'
tags: [org, flink, flink-2.3, docs, dev, datastream-v2, experimental, building-blocks, state-v2, timer-service, event-time, windows, joining, watermark, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:30:01.152Z'
updated: '2026-07-08T04:30:01.152Z'
importanceScore: 1
---

## Executive Summary

Note: DataStream API V2 is a NEW set of APIs to gradually replace the original DataStream API. Currently experimental — NOT fully available for production. (Applies to all pages in this memory.)

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream-v2/overview/
# Flink DataStream API V2 Programming Guide
DataStream = immutable collections of data (can contain duplicates), finite or unbounded, same API. Like a Java Collection but immutable; cannot add/remove/inspect elements — only work on them via transformations (process, connectAndProcess). Create initial DataStream by adding a source; derive/combine via API methods.
## Fundamental Primitives vs High-Level Extensions
- Fundamental primitives (basic semantics Flink MUST provide): data stream, partitioning, process function, state, processing timer service, watermark. See Building Blocks, State Processing, Time Processing # Processing Timer Service, Watermark.
- High-Level extensions (shortcuts/sugars; achievable with fundamentals but easier with builtin support): event timer service, window, join. See Time Processing # Event Timer Service, Builtin Functions.
## Anatomy of a Flink DataStream Program
1. Obtain an Execution Environment: ExecutionEnvironment.getInstance() — local env in IDE/regular Java program; cluster env when JAR invoked via CLI (cluster manager executes main).
2. Load/create initial data: ExecutionEnvironment.fromSource(source, sourceName). Use FLIP-27 based source via DataStreamV2SourceUtils.wrapSource(source), or DataStreamV2SourceUtils.fromData(collection) for testing/debugging. Source has no clear partitioning → gives a NonKeyedPartitionStream.
3. Specify transformations: call methods on DataStream with a ProcessFunction (lambda for simplicity). E.g. map converting String→Integer.
4. Specify where to put results: create a sink. Use SinkV2 based sink via DataStreamV2SinkUtils.wrapSink(sink). E.g. parsed.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>())).
5. Trigger execution: execute() on ExecutionEnvironment — waits for job to finish. All Flink programs executed LAZILY: main() builds a dataflow graph; execution triggered only by execute(). Lazy evaluation lets you construct sophisticated programs executed as one holistically planned unit.

Source: .../dev/datastream-v2/building_blocks/
# Building Blocks
DataStream, Partitioning, ProcessFunction = most fundamental elements (types of data streams / how partitioned / how to process).
## DataStream (categories)
- Global Stream: force single partition/parallelism; correctness depends on this.
- Partition Stream: divide into multiple partitions; state only available within partition; one partition processed by one task, one task handles one or multiple partitions. Subtypes:
  - Keyed Partition Stream: each key is a partition; partition of data is deterministic.
  - Non-Keyed Partition Stream: each parallelism is a partition; partition of data is nondeterministic.
- Broadcast Stream: each partition contains the same data.
## Partitioning (conversions between partition types)
- KeyBy: repartition all data by specified key.
- Shuffle: repartition and shuffle all data.
- Global: merge all partitions into one.
- Broadcast: force partitions broadcast data to downstream.
Note: broadcast can only be used in conjunction with other inputs, cannot be directly converted to other streams. E.g. non-keyed → keyed via KeyBy.
## ProcessFunction
Only entrypoint for defining all processing on data streams. Classified by number of input/output (OneInput/TwoInput, OneOutput/TwoOutput; two-input has broadcast vs non-broadcast variants). DataStream has process and connectAndProcess methods. Input/output stream compatibility tables govern valid combinations. Config Process: return value of process/connectAndProcess is both a stream AND a handle to configure the previous processing (withXXX methods — e.g. parallelism, name).

Source: .../dev/datastream-v2/context_and_state_processing/
# Context and State Processing
## Context
Some info (e.g. current key) only obtainable when process function executes. Runtime Context = unified entrypoint bridging process functions and execution engine. Context parts: JobInfo (job name, execution mode), TaskInfo (parallelism), MetricGroup (registering metrics), State Manager (accessing state), Watermark Manager (triggering watermark), ProcessingTime Manager (current processing time). Two categories:
- NonPartitionedContext: JobInfo, TaskInfo, MetricGroup, WatermarkManager (provided when no specific partition relevant — e.g. init/cleanup).
- PartitionedContext: StateManager, ProcessingTimeManager (provided when processing within a specific partition — e.g. on record/timer).
## State Processing
Principle: "declare first, use later." Three steps: (1) Define state as StateDeclaration; (2) Declare state in ProcessFunction#usesStates; (3) Get/update state via StateManager.
### Define State
StateDeclaration needs: Name (unique id) + RedistributionMode (how state redistributed between partitions). Keyed partition stream → no redistribution needed (state bounded within partition). Non-keyed → partition changes with parallelism, must define redistribution. Three RedistributionMode: NONE (no redistribution), REDISTRIBUTABLE (safely redistributed, strategy determined by state itself), IDENTICAL (states identical across partitions, redistribution not a problem).
Six StateDeclaration types: ValueStateDeclaration (ValueState<T>: update(T)/T value()), ListStateDeclaration (ListState<T>: add(T)/addAll(List<T>)/Iterable<T> get()/update(List<T>)), ReducingStateDeclaration (ReducingState<T>: add(T) reduced via ReduceFunction), AggregatingStateDeclaration (AggregatingState<IN,OUT>: add(IN) aggregated via AggregateFunction; aggregate type may differ from input), MapStateDeclaration (MapState<UK,UV>: put/putAll/get(UK)/entries()/keys()/values()/isEmpty()), BroadcastStateDeclaration (BroadcastState<K,V>: store state of a BroadcastStream, same elements to all instances). StateDeclarations auxiliary class + TypeDescriptors (INT, LONG, BOOLEAN, STRING, LIST, MAP; custom via TypeDescriptor interface) for type info.
### Declare State
ProcessFunction#usesStates() must be overridden for stateful functions; each state declared upfront → Flink optimizes execution. Flink checks legality at compile time (illegal state declaration in usesStates → exception at job compile time). Legality depends on input stream type (K/NK/G/B = Keyed/Non-Keyed/Global/Broadcast).
### Get and Update State
StateManager (obtained via context#getStateManager()) — getState/getStateOptional + getCurrentKey. State access methods use state v2 APIs (see Using Keyed State V2).

Source: .../dev/datastream-v2/time-processing/processing_timer_service/
# Processing Timer Service
Fundamental primitive. Register timers for calculations at specific processing time points. ProcessingTimerManager obtained via PartitionedContext#getProcessingTimeManager. Methods: registerTimer, deleteTimer, currentTime. When target time arrives → ProcessFunction#onProcessingTimer (user logic). Notes: same-target-time timers deduplicated (only one retained, onProcessingTimer invoked once); ProcessingTimerManager only usable in Keyed Partitioned Stream.

Source: .../dev/datastream-v2/time-processing/event_timer_service/
# Event Timer Service
High-level extension. Register timers at event-time points; helps determine when to trigger windows. Uses special Watermark to denote event-time progression = event time watermark; idle status watermark indicates input/source idle. (Collectively "event-time related watermarks.")
## Generate Event-Time related Watermarks (two methods)
### By EventTimeWatermarkGeneratorBuilder (recommended)
Four configurable aspects:
- [Required] EventTimeExtractor: extract event time from each record.
- [Optional] Input Idle Timeout (default 0): if input idle for duration, Flink ignores it when combining watermarks (prevents stalling). See Dealing With Idle Inputs/Sources.
- [Optional] Out-of-Order Time (default 0): max out-of-order time for event time watermark. See Fixed Amount of Lateness. Lateness = t_w - t (t = element timestamp, t_w = previous watermark); lateness > 0 → element late, ignored by default.
- [Optional] Generation Frequency: (a) no watermarks; (b) periodic (default, interval via "pipeline.auto-watermark-interval"); (c) per event.
Builds a ProcessFunction extracting event time + generating watermarks. Attention: timestamps + event time watermarks = milliseconds since Java epoch 1970-01-01T00:00:00Z.
#### Dealing With Idle Inputs/Sources
If an input split/partition/shard carries no events, watermark generator gets no new info = idle input/source. Problem: event time watermark computed as minimum over all parallel watermarks → held back. Configure idleness timeout via withIdleness → marks input idle; emits idle status watermark; downstream disregards this input when combining.
### By Custom ProcessFunction
Steps: declare EventTimeWatermarkDeclaration (+ IdleStatusWatermarkDeclaration if idle support); create event-time related watermark; send via WatermarkManager.
## Handle Event-Time related Watermarks (two ways)
### By EventTimeProcessFunction (recommended — simple/efficient, quick register/unregister event timers)
Wrapper for ProcessFunction needing event time. Custom function implements OneInputEventTimeStreamProcessFunction / TwoInputBroadcastEventTimeStreamProcessFunction / TwoInputNonBroadcastEventTimeStreamProcessFunction / TwoOutputEventTimeStreamProcessFunction (instead of the non-event-time variants). Three methods: initEventTimeProcessFunction (obtain EventTimeManager — current event time + create/delete event timers; event timer only in Keyed Partition Stream), onEventTimeWatermark (received event time watermark; event time watermarks handled here, other watermarks by onWatermark), onEventTimer (callback triggered by event timer; access key + event time). Wrap custom function with EventTimeUtils#wrapProcessFunction (provides EventTimeManager + declares built-in state for timers).
### By Custom ProcessFunction
Evaluate in ProcessFunction#onWatermark whether watermark is event time or idle status; execute logic. Return WatermarkHandlingResult#PEEK → framework chooses processing based on watermark definition (forwards event time + idle status watermarks downstream). Return #POP → framework does NOT send downstream (may lose watermark, or send manually).
## Example
Full event-timer-service example using EventTimeUtils wrapping.

Source: .../dev/datastream-v2/builtin-funcs/windows/
# Windows
Heart of processing infinite streams — split stream into "buckets" of finite size for computations. Three steps: (1) Declare Window; (2) Define WindowProcessFunction; (3) Combine into a ProcessFunction via BuiltinFuncs.window.
## Declare Window (three built-in types)
### Time Window (supported ONLY in Keyed Partition Stream)
Divided by time ranges; data allocated by timestamp. Two types: tumbling (fixed size, no overlap) and sliding (size + slide; overlapping if slide < size → elements assigned to multiple windows). Time semantics: event time or processing time. Intervals via Duration.ofMillis/Seconds/Minutes. Allowed Lateness (event-time): by default late elements dropped when watermark past window end; max allowed lateness specifies grace period (default 0). Elements arriving after window end but before end+allowedLateness → added to window, window fires again; late+dropped → WindowProcessFunction#onLateRecord. Flink keeps window state until allowed lateness expires.
### Session Window (supported ONLY in Global Stream and Keyed Partition Stream)
Groups by sessions of activity; no overlap, no fixed start/end; closes when no elements for a gap (session gap). Configured with session gap.
### Global Window (compatible with Global Stream, Keyed Partition Stream, Non-Keyed Partition Stream)
All elements to a single unified window; useful for bounded streams (triggered once all inputs conclude).
## Define WindowProcessFunction
### Window Lifecycle
Window created when first belonging element arrives; completely removed when time passes end timestamp + allowed lateness. E.g. 5-min tumbling + 1 min allowed lateness: window [12:00,12:05) removed when watermark passes 12:06. Four key methods: onRecord (window received a record), onTrigger (window triggered), onClear (window cleared), onLateRecord (record received after window cleared). Notes: windows can trigger multiple times (onRecord may be called after onTrigger); GlobalWindow cleared when stream ends, time/session windows cleared after boundary + allowedLateness elapsed; onLateRecord cannot access window state (already cleared).
### Window State (two types)
- Partitioned State: partition-related; NonKeyed→shared among task, Keyed→shared among same key. Declared via ProcessFunction#usesStates, used via PartitionedContext#getStateManager. User must clear unneeded data in onClear.
- Window State: window-bound (e.g. state for key in 10:00-11:00 differs from 11:00-12:00). Declared via WindowProcessFunction#usesWindowStates, used via WindowContext#getWindowState. Cleared by framework regardless of manual clearing.
### Access Built State of Window
Built-in window state stores input data via WindowContext#putRecord / WindowContext#getAllRecords; cleared when window cleared. Default onRecord stores data; users retrieve all via getAllRecords on trigger. Override onRecord to do pre-aggregation (declare window state, aggregate in onRecord, output in onTrigger) → eliminates cost of caching all data.
## Build a ProcessFunction
BuiltinFuncs.window(windowDeclaration, windowProcessFunction) → ProcessFunction. Flink auto-manages state + timers for caching window data.
## Example: Count sales of each product every hour
Event time extension → 1-hour tumbling window → count on trigger. Pre-aggregation variant (CountSalesQuantityWithPreAggregation) maintains ValueState per product, updates on record, outputs on trigger — avoids storing all input data.

Source: .../dev/datastream-v2/builtin-funcs/joining/
# Joining
Merge two streams by matching elements on common key, compute on matched. Currently supports ONLY non-window INNER joins (interval/lookup/window joins future).
## Non-Window Join
Specify: left/right streams, KeySelector of both (join key), JoinFunction (processing logic on matched data).
### APIs (two approaches)
- BuiltinFuncs.join: (a) if both Keyed Partition Stream → combine directly with JoinFunction; (b) if both Non-Keyed → incorporate join-key KeySelector + JoinFunction.
- Convert JoinFunction to ProcessFunction via KeyedPartitionStream#connectAndProcess (requires both inputs be Keyed Partition Stream).
### JoinFunction
Interface with one method processRecord — get matched elements, compute, output. Example joins student personal info with exam scores.

Source: .../dev/datastream-v2/watermark/
# Watermark (V2)
IMPORTANT: Watermark in DataStream V2 does NOT refer to original event-time-progress watermark — it is a SPECIAL EVENT customizable by user, propagatable along streams. Three steps: Define/Declare, Emit, Handle.
## Define and Declare Watermark
Four aspects:
- [Required] Watermark Identifier: String, case-sensitive, globally unique within job.
- [Required] Watermark Data Type: Long or Bool.
- [Required] Combine Function + combineWaitForAllChannels: combine watermarks from multiple input channels. Long: MIN/MAX. Bool: AND/OR. combineWaitForAllChannels (default false): wait until received from all upstream channels (e.g. event time watermark waits for all inputs so time doesn't decrease).
- [Optional] WatermarkHandlingStrategy by Framework: when user ProcessFunction pops watermark — IGNORE (framework no action) or FORWARD (framework sends downstream).
WatermarkBuilder creates WatermarkDeclaration. Declare in ProcessFunction#declareWatermarks or Source#declareWatermarks. Each type declared once per job.
## Emit Watermark
Create watermark from declaration (e.g. Long value 1). Emit in ProcessFunction via nonPartitionedContext.getWatermarkManager().emitWatermark(watermark); in Source via sourceReaderContext.emitWatermark(watermark).
## Handle Watermark
Framework invokes ProcessFunction#onWatermark. Return WatermarkHandlingResult:
- PEEK (default): ProcessFunction only peeks; framework handles per WatermarkHandlingStrategy (forward/ignore).
- POLL: ProcessFunction sends to downstream itself; framework does no additional processing.
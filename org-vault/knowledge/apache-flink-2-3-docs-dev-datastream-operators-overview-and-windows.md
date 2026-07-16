---
title: Apache Flink 2.3 docs — Dev DataStream Operators Overview and Windows
tags: [org, flink, flink-2.3, docs, dev, datastream-api, operators, windows, triggers, evictors, partitioning, chaining, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:25:30.451Z'
updated: '2026-07-08T04:25:30.451Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/overview/
# Operators

Operators transform one/more DataStreams into a new DataStream; multiple transformations combine into dataflow topologies. Covers basic transformations, effective physical partitioning, operator chaining.

## DataStream Transformations
- Map (DataStream→DataStream): one element → one element.
- FlatMap (DataStream→DataStream): one element → zero/one/more.
- Filter (DataStream→DataStream): boolean function, retains true.
- KeyBy (DataStream→KeyedStream): logically partitions into disjoint partitions; same key → same partition. Hash partitioning internally. A type cannot be a key if: POJO type but doesn't override hashCode() (relies on Object.hashCode()); array of any type.
- Reduce (KeyedStream→DataStream): rolling reduce on keyed stream; combines current element with last reduced value, emits new value.
- Window (KeyedStream→WindowedStream): windows on partitioned KeyedStreams; group data per key by characteristic (e.g. last 5 seconds).
- WindowAll (DataStream→AllWindowedStream): windows on regular DataStreams; NON-PARALLEL — all records gathered in one task.
- Window Apply (WindowedStream/AllWindowedStream→DataStream): general function on whole window; use AllWindowFunction for windowAll.
- WindowReduce (WindowedStream→DataStream): functional reduce on window.
- Union (DataStream*→DataStream): two/more streams → new stream with all elements. Union with itself → each element twice.
- Window Join (DataStream,DataStream→DataStream): join on key + common window. (Python: not supported)
- Interval Join (KeyedStream,KeyedStream→DataStream): join e1,e2 of two keyed streams with common key over time interval: e1.timestamp+lowerBound <= e2.timestamp <= e1.timestamp+upperBound. (Python: not supported)
- Window CoGroup (DataStream,DataStream→DataStream): cogroup on key + common window. (Python: not supported)
- Connect (DataStream,DataStream→ConnectedStream): connects two streams retaining types; shared state between streams.
- CoMap, CoFlatMap (ConnectedStream→DataStream): like map/flatMap on connected stream.
- Cache (DataStream→CachedDataStream): cache intermediate result; only batch execution mode. Generated lazily on first compute; reused by later jobs; recomputed if lost.
- Full Window Partition (DataStream→PartitionWindowedStream): collects all records of each partition separately into a full window; emission triggered at end of inputs. Primarily batch. Non-keyed: partition=all records of a subtask. Keyed: partition=all records of a key.

## Physical Partitioning
- Custom Partitioning (DataStream→DataStream): user-defined Partitioner selects target task per element. partitionCustom(partitioner, "someKey").
- Random Partitioning: shuffle() — uniform distribution.
- Rescaling: rescale() — round-robin to a SUBSET of downstream operations. Fan out from each source parallel instance to subset of mappers (distribute load without full rebalance); only local data transfers (depending on slots). Subset depends on parallelism of both upstream/downstream. E.g. upstream 2, downstream 6 → one upstream distributes to 3 downstream, other to other 3. Upstream 6, downstream 2 → 3 upstream to one downstream, 3 to other. Non-multiples → differing number of inputs.
- Broadcasting: broadcast() — elements to every partition.

## Task Chaining and Resource Groups
Chaining two subsequent transformations = co-locating in same thread for performance. Default chains if possible (e.g. two subsequent maps). disableOperatorChaining() for whole job. Fine-grained control functions only usable right after a DataStream transformation (refer to previous transformation): someStream.map(...).startNewChain() valid, someStream.startNewChain() invalid.
- Start New Chain: begin new chain starting with this operator.
- Disable Chaining: don't chain this operator.
- Set Slot Sharing Group: slotSharingGroup("name") — operations with same group in same slot; isolates slots. Inherited from input operations if all inputs in same group. Default group "default".
Resource group = a slot in Flink.

## Name And Description
Operators/job vertices have name + description (both intro to what operator/vertex does, used differently). Name: web UI, thread name, logging, metrics; job vertex name constructed from operators' names; needs to be concise (avoid pressure on external systems). Description: execution plan, details of job vertex in web UI; can contain detail for debugging. name("filter").setDescription("..."). Job vertex description format = tree string by default; set pipeline.vertex-description-mode=CASCADING for former cascading format. Flink SQL operators: name = type+id by default, detailed description; set table.exec.simplify-operator-name-enabled=false for name=detailed description. Complex topology: pipeline.vertex-name-include-index-prefix=true adds topological index to vertex name (find vertex via logs/metrics tags).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/
# Windows

Windows = heart of processing infinite streams; split stream into "buckets" of finite size for computations. General structure: keyed windows (keyBy(...) + window(...)) vs non-keyed (windowAll(...)). Optional parts in square brackets. (Evictor not supported in Python DataStream API.)

## Window Lifecycle
Window created when first element that belongs to it arrives; completely removed when time (event/processing) passes end timestamp + user-specified allowed lateness. Flink guarantees removal only for time-based windows, not others (e.g. global windows). E.g. tumbling every 5 min, allowed lateness 1 min → window 12:00-12:05 created when first element in interval arrives, removed when watermark passes 12:06. Each window has Trigger + function (ProcessWindowFunction/ReduceFunction/AggregateFunction). Function = computation; Trigger = conditions for ready. Trigger can purge window contents anytime between creation and removal (purging = elements only, not metadata; new data can still be added). Optional Evictor removes elements after trigger fires and before/after function applied.

## Keyed vs Non-Keyed Windows
keyBy(...) splits infinite stream into logical keyed streams (any attribute can be key). Keyed → windowed computation parallel by multiple tasks (each keyed stream independent); same key → same parallel task. Non-keyed → original stream not split, all windowing logic by single task (parallelism 1).

## Window Assigners
Defines how elements assigned to windows (WindowAssigner in window(...)/windowAll()). Assigns each element to one/more windows. Built-in: tumbling, sliding, session, global. Custom by extending WindowAssigner. All built-in (except global) time-based (processing or event time). Time-based windows have start (inclusive) + end (exclusive). Flink uses TimeWindow with start/end query methods + maxTimestamp() (largest allowed timestamp).

### Tumbling Windows
Each element to window of specified size. Fixed size, no overlap. Optional offset changes alignment (e.g. offset 15 min → 1:15-2:14:59.999). Use case: adjust to timezones other than UTC-0 (China: Duration.ofHours(-8)).

### Sliding Windows
Fixed length, window size + slide parameter (how frequently started). Overlapping if slide < size → elements assigned to multiple windows. Optional offset (same as tumbling).

### Session Windows
Groups by sessions of activity. No overlap, no fixed start/end. Closes when no elements for a period (gap of inactivity). Static session gap or session gap extractor function (dynamic). Evaluated differently: creates new window per arriving record, merges windows closer than gap. Requires merging Trigger + merging Window Function (ReduceFunction/AggregateFunction/ProcessWindowFunction).

### Global Windows
All elements with same key to same single global window. Only useful with custom trigger (no natural end); else no computation.

## Window Functions
ReduceFunction/AggregateFunction/ProcessWindowFunction. First two more efficient (incremental aggregation). ProcessWindowFunction gets Iterable of all elements + meta info; less efficient (Flink buffers all elements). Mitigate by combining ProcessWindowFunction with Reduce/Aggregate (incremental + window metadata).
- ReduceFunction: combines two elements → output of same type; incremental.
- AggregateFunction: generalized ReduceFunction with IN/ACC/OUT types; create initial accumulator, add input, merge accumulators, extract output; incremental. Example: average.
- ProcessWindowFunction: Iterable of all elements + Context (time/state info). KEY = key extracted via KeySelector; tuple-index/string-field keys → always Tuple (manual cast). Inefficient for simple aggregates like count.
- ProcessWindowFunction with Incremental Aggregation: combine with Reduce/Aggregate; on window close, ProcessWindowFunction gets aggregated result. Legacy WindowFunction also usable.
- per-window state in ProcessWindowFunction: keyed state scoped to current window. Two "windows": defined window (e.g. tumbling 1hr) vs actual instance for a key (e.g. 12:00-13:00 for user xyz). Per-window state tied to latter. Context methods: globalState() (keyed state not scoped to window), windowState() (keyed state scoped to window). Helpful for multiple firings (late firings, speculative early firings). Clean up in clear() method.
- WindowFunction (Legacy): older ProcessWindowFunction, less context, no per-window keyed state; will be deprecated.

## Triggers
Determines when window ready for processing. Each WindowAssigner has default Trigger; custom via trigger(...). Five methods: onElement() (each element added), onEventTime() (event-time timer fires), onProcessingTime() (processing-time timer fires), onMerge() (stateful triggers, merges states when windows merge — session windows), clear() (window removal). First three return TriggerResult: CONTINUE (nothing), FIRE (trigger computation), PURGE (clear elements), FIRE_AND_PURGE (trigger + clear). Any can register processing/event-time timers.

### Fire and Purge
Trigger fires → returns FIRE/FIRE_AND_PURGE → window operator emits result. ProcessWindowFunction: all elements passed to it (possibly after evictor). Reduce/Aggregate: emit eagerly aggregated result. FIRE keeps contents; FIRE_AND_PURGE removes content. Default pre-implemented triggers FIRE without purging. Purging removes contents, leaves meta-info + trigger state intact.

### Default Triggers of WindowAssigners
Event-time window assigners → EventTimeTrigger (fires when watermark passes window end). GlobalWindow → NeverTrigger (never fires; must define custom trigger). trigger() overwrites default (e.g. CountTrigger on TumblingEventTimeWindows → firings by count not time). Custom trigger for both time+count.

### Built-in and Custom Triggers
EventTimeTrigger (event-time via watermarks), ProcessingTimeTrigger (processing time), CountTrigger (element count exceeds limit), PurgingTrigger (wraps another trigger into purging). Custom: extend abstract Trigger class (API evolving).

## Evictors
Optional, removes elements after trigger fires and before/after window function. evictBefore() (before function — evicted elements not processed), evictAfter() (after function). Three pre-implemented: CountEvictor (keeps up to N elements, discards rest from beginning), DeltaEvictor (DeltaFunction + threshold, removes delta >= threshold from last element), TimeEvictor (interval ms, finds max_ts, removes elements with ts < max_ts - interval). Default: apply before window function. Specifying evictor prevents pre-aggregation (all elements passed to evictor) → significantly more state. (Python: not supported.) Flink gives no guarantee on order of elements within a window (evictor removing from "beginning" not necessarily first/last arrived).

## Allowed Lateness
Event-time: elements arrive late (watermark past window end). Default: late elements dropped. allowedLateness specifies how late before dropped (default 0). Elements arriving after watermark past end but before end+allowed lateness still added to window; may cause window to fire again (EventTimeTrigger). Flink keeps window state until allowed lateness expires, then removes window + deletes state. GlobalWindows: no data ever late (end = Long.MAX_VALUE).
- Getting late data as side output: sideOutputLateData(OutputTag) on windowed stream → get side-output stream of discarded late data.
- Late elements considerations: allowed lateness > 0 → window+content kept after watermark passes end; late non-dropped element may trigger late firing (vs main/first firing). Session windows: late firings can merge windows (bridge gap). Late firing emitted elements = updated results of previous computation → stream contains multiple results for same computation; deduplicate as needed.

## Working with window results
Result is DataStream; no windowed-operation info retained in result elements (manually encode in ProcessWindowFunction). Element timestamp set to max allowed timestamp of processed window (end-1, since end exclusive). True for both event-time and processing-time windows. For event-time windows, this + watermark interaction enables consecutive windowed operations with same window sizes.

### Interaction of watermarks and windows
Watermark arriving at window operator: (1) triggers computation of all windows where max timestamp (end-1) < new watermark; (2) watermark forwarded as-is to downstream. Watermark "flushes" windows that would be late in downstream.

### Consecutive windowed operations
Timestamp computation + watermark interaction allows stringing consecutive windowed operations (different keys but same upstream window → same downstream window). E.g. results for [0,5) first op end up in [0,5) subsequent op → sum per key then top-k within same window.

## Useful state size considerations
- Flink creates one copy of each element per window it belongs to. Tumbling: one copy (unless dropped late). Sliding: several copies (size 1 day, slide 1 second = bad idea).
- ReduceFunction/AggregateFunction significantly reduce storage (eager aggregation, one value per window). ProcessWindowFunction alone requires accumulating all elements.
- Evictor prevents pre-aggregation (all elements through evictor) → more state.
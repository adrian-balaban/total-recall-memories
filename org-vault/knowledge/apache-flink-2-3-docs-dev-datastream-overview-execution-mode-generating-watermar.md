---
title: 'Apache Flink 2.3 docs — Dev DataStream Overview, Execution Mode, Generating Watermarks'
tags: [org, flink, flink-2.3, docs, dev, datastream-api, overview, execution-mode, event-time, watermarks, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:20:13.120Z'
updated: '2026-07-08T04:20:13.120Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/overview/
# Flink DataStream API Programming Guide

DataStream programs in Flink are regular programs that implement transformations on data streams (e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various sources (e.g., message queues, socket streams, files). Results are returned via sinks. Flink programs run in a variety of contexts, standalone, or embedded in other programs. The execution can happen in a local JVM, or on clusters of many machines.

## What is a DataStream?

The DataStream API gets its name from the special DataStream class used to represent a collection of data in a Flink program. Think of them as immutable collections of data that can contain duplicates. This data can either be finite or unbounded, the API that you use to work on them is the same. A DataStream is similar to a regular Java Collection but quite different in key ways: immutable (cannot add/remove elements once created), and you cannot simply inspect the elements but only work on them using DataStream API operations (transformations). You create an initial DataStream by adding a source, then derive new streams using API methods such as map, filter.

## Anatomy of a Flink Program

Every program consists of: Obtain an execution environment; Load/create the initial data; Specify transformations; Specify where to put results; Trigger program execution. All core classes of the Java DataStream API are in org.apache.flink.streaming.api.

The StreamExecutionEnvironment is the basis for all Flink programs. Obtain via getExecutionEnvironment() (does the right thing depending on context: local in IDE/regular Java program, cluster when invoked via command line JAR). For data sources, several methods to read from files. Read a text file line by line via env.readTextFile(path) / readFile(...). Apply transformations by calling methods on DataStream with transformation functions (e.g., map). Create sinks via stream.sinkTo(...) etc. Trigger execution via env.execute() (waits for job to finish, returns JobExecutionResult with execution times and accumulator results) or env.executeAsync() (returns JobClient to communicate with the submitted job).

All Flink programs are executed lazily: when the program's main method is executed, data loading and transformations do not happen directly. Each operation is added to a dataflow graph. Operations are actually executed when execution is explicitly triggered by execute(). The lazy evaluation lets you construct sophisticated programs that Flink executes as one holistically planned unit.

## Example Program

Streaming window word count application that counts words coming from a web socket in 5 second windows. Run with netcat `nc -lk 9999`.

## Data Sources

Attach a source via StreamExecutionEnvironment.addSource(sourceFunction). Flink comes with pre-implemented source functions; write custom by implementing SourceFunction (non-parallel), ParallelSourceFunction, or extending RichParallelSourceFunction.

File-based:
- fromSource(FileSource.forRecordStreamFormat(format, paths).build()) — Read record-by-record from files.
- readFile(fileInputFormat, path) — Reads (once) files as dictated by the specified file input format.
- readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo) — reads files based on fileInputFormat; watchType may be PROCESS_CONTINUOUSLY (periodically monitor every interval ms) or PROCESS_ONCE (process current data and exit).

IMPLEMENTATION: Flink splits file reading into two sub-tasks: directory monitoring (single non-parallel task, parallelism=1) and data reading (multiple parallel tasks, parallelism = job parallelism). The monitoring task scans the directory, finds files, divides them in splits, assigns splits to downstream readers. Each split is read by only one reader; a reader can read multiple splits.

IMPORTANT NOTES:
- PROCESS_CONTINUOUSLY: when a file is modified, its contents are re-processed entirely. This can break "exactly-once" semantics (appending data at end leads to all contents re-processed).
- PROCESS_ONCE: source scans path once and exits, without waiting for readers to finish. No more checkpoints after that point → slower recovery after node failure (resume from last checkpoint).

Socket-based: socketTextStream — Reads from a socket; elements separated by a delimiter.

Collection-based:
- fromData(Collection) — Creates a data stream from a Java.util.Collection (all elements same type).
- fromData(T ...) — from a sequence of objects.
- fromParallelCollection(SplittableIterator, Class) — from an iterator, in parallel.
- fromSequence(from, to) — generates sequence of numbers in interval, in parallel.

Custom: addSource — e.g. addSource(new FlinkKafkaConsumer<>(...)).

## DataStream Transformations

See operators for available stream transformations.

## Data Sinks

Data sinks consume DataStreams and forward to files, sockets, external systems, or print. Built-in output formats encapsulated behind operations:
- sinkTo(FileSink.forRowFormat(new Path("outputPath"), new SimpleStringEncoder<>()).build()) — writes elements line-wise as Strings (toString()).
- print() / printToErr() — prints toString() value to stdout/stderr; optional prefix; if parallelism > 1, output prepended with task identifier.
- writeUsingOutputFormat() / FileOutputFormat — method and base class for custom file outputs.
- writeToSocket — writes elements to a socket per SerializationSchema.
- addSink — invokes a custom sink function (e.g. Kafka connectors).

Note: write*() methods mainly for debugging — not participating in checkpointing (at-least-once semantics). For reliable, exactly-once delivery into a file system, use the FileSink. Custom implementations through .addSink(...) can participate in checkpointing for exactly-once.

## Execution Parameters

StreamExecutionEnvironment contains ExecutionConfig for job-specific runtime config. DataStream-specific: setAutoWatermarkInterval(long milliseconds) — set interval for automatic watermark emission; getAutoWatermarkInterval().

### Controlling Latency

By default, elements are not transferred one-by-one but buffered. Buffer size set in Flink config files. Use env.setBufferTimeout(timeoutMillis) (or on individual operators) to set max wait time for buffers to fill. Default 100 ms. setBufferTimeout(-1) maximizes throughput (remove timeout, flush only when full). Near-0 (5–10 ms) minimizes latency. A buffer timeout of 0 should be avoided (severe performance degradation).

## Debugging

### Local Execution Environment

A LocalStreamEnvironment starts a Flink system within the same JVM process. Set breakpoints in IDE. Created via StreamExecutionEnvironment.createLocalEnvironment().

### Collection Data Sources

Special data sources backed by Java collections to ease testing. Once tested, sources/sinks easily replaced. Note: collection data source requires data types and iterators implement Serializable; cannot be executed in parallel (parallelism = 1).

### Iterator Data Sink

A sink to collect DataStream results for testing and debugging.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution_mode/
# Execution Mode (Batch/Streaming)

The DataStream API supports different runtime execution modes chosen depending on requirements and job characteristics.

- STREAMING execution mode: the "classic" execution behavior. Should be used for unbounded jobs requiring continuous incremental processing, expected to stay online indefinitely.
- BATCH execution mode: batch-style execution reminiscent of MapReduce. Should be used for bounded jobs with known fixed input that do not run continuously.

Apache Flink's unified approach to stream and batch means a DataStream application executed over bounded input will produce the same final results regardless of configured execution mode. "Final" matters: a STREAMING job might produce incremental updates (upserts) while a BATCH job produces one final result at the end. Final result the same if interpreted correctly but the way to get there can differ. BATCH enables additional optimizations possible only when input is known bounded (different join/aggregation strategies, different shuffle implementation for more efficient task scheduling and failure recovery).

## When can/should I use BATCH execution mode?

BATCH only for bounded Jobs/Flink Programs. Boundedness is a property of a data source (all input known before execution vs new data indefinitely). A job is bounded if all its sources are bounded, unbounded otherwise. STREAMING can be used for both bounded and unbounded jobs. Rule of thumb: use BATCH when bounded (more efficient); use STREAMING when unbounded (only mode general enough for continuous streams).

Outliers: (1) bootstrap job state for an unbounded job via bounded job in STREAMING mode → savepoint → restore on unbounded job (may become obsolete when BATCH can produce a savepoint as additional output). (2) bounded job in STREAMING mode to test code that will run with unbounded sources.

## Configuring BATCH execution mode

Via execution.runtime-mode: STREAMING (default), BATCH, AUTOMATIC (system decides based on source boundedness). Configure via command line `bin/flink run -Dexecution.runtime-mode=BATCH <jarFile>` or programmatically via StreamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH). Recommendation: do NOT set runtime mode in program; set via command-line when submitting (configuration-free application can run in any mode).

## Execution Behavior

### Task Scheduling And Network Shuffle

Flink jobs = operations connected in a dataflow graph. System decides how to schedule operations on TaskManagers and how data is shuffled between them. Multiple operators can be chained (chaining). A group of one or multiple chained operators Flink considers as a unit of scheduling = task. Subtask = individual instance of a task running in parallel.

Operations with 1-to-1 connection pattern (map(), flatMap(), filter()) can forward data straight → chained together (no network shuffle). Operations like keyBy() or rebalance() require data shuffled between parallel instances → network shuffle.

STREAMING Execution Mode: all tasks need to be online/running all the time (immediate processing through whole pipeline, low latency). TaskManagers need enough resources to run all tasks simultaneously. Network shuffles are pipelined (records immediately sent downstream with buffering). No natural points where data could be materialized between tasks.

BATCH Execution Mode: tasks separated into stages executed one after another (input bounded → fully process one stage before next). Requires Flink to materialize intermediate results to non-ephemeral storage so downstream tasks read them after upstream tasks gone offline. Increases latency but: allows backtracking to latest available results on failure instead of restarting whole job; BATCH jobs can execute on fewer resources (execute tasks sequentially). TaskManagers keep intermediate results at least until downstream consumed them; kept longer as space allows for backtracking.

### State Backends / State

STREAMING: Flink uses a StateBackend to control state storage and checkpointing. BATCH: configured state backend is ignored. Input of a keyed operation is grouped by key (using sorting) then all records of a key processed in turn. Only state of one key kept at a time. State for a given key discarded when moving to next key. (FLIP-140)

### Order of Processing

STREAMING: UDFs should not make assumptions about incoming records' order; data processed as soon as it arrives. BATCH: some operations guarantee order (side effect of task scheduling/shuffle/state backend, or conscious choice). Three input types: broadcast input (from broadcast stream), regular input (neither broadcast nor keyed), keyed input (from KeyedStream). Functions consuming multiple input types process in order: broadcast first, regular second, keyed last. Multiple regular/broadcast inputs (e.g. CoProcessFunction): Flink may process any input of that type in any order. Multiple keyed inputs (e.g. KeyedCoProcessFunction): Flink processes all records for a single key from all keyed inputs before moving to next.

### Event Time / Watermarks

STREAMING builds on pessimistic assumption events may come out-of-order (event t may come after t+1). System can never be sure no more elements with timestamp t < T will come. Uses Watermarks heuristic: a watermark with timestamp T signals no element with timestamp t < T will follow. BATCH: input dataset known in advance → no heuristic needed; elements can be sorted by timestamp ("perfect watermarks"). Only need a MAX_WATERMARK at end of input per key (or end of input if not keyed). All registered timers fire at end of time; user-defined WatermarkAssigners/WatermarkGenerators ignored. Specifying a WatermarkStrategy still important — its TimestampAssigner still used to assign timestamps.

### Processing Time

Processing time = wall-clock time on the machine that a record is processed. Results based on processing time are not reproducible. In STREAMING, processing time useful (correlation between event time and processing time; 1h event time ≈ 1h processing time; useful for early incomplete firings). This correlation does not exist in BATCH (input static). BATCH: users can request current processing time and register processing time timers, but (like Event Time) all timers fire at end of input. Conceptually processing time does not advance during execution; fast-forward to end of time when whole input processed.

### Failure Recovery

STREAMING: Flink uses checkpoints for failure recovery; restarts all running tasks from a checkpoint on failure (costlier). BATCH: Flink tries to backtrack to previous processing stages with intermediate results still available; potentially only failed tasks (or predecessors) restarted → improved efficiency.

## Important Considerations

Behavior Change in BATCH mode: "Rolling" operations such as reduce() or sum() emit incremental update per record in STREAMING; in BATCH not "rolling" — emit only final result.

Unsupported in BATCH mode: Checkpointing and any operations that depend on checkpointing. CheckpointListener and, as a result, Kafka's EXACTLY_ONCE mode or File Sink's OnCheckpointRollingPolicy won't work. For a transactional sink in BATCH mode, use Unified Sink API (FLIP-143). You can still use all state primitives; failure recovery mechanism is different.

### Writing Custom Operators

Custom operators advanced; consider (keyed-)process function instead. Remember BATCH assumptions: do not cache last seen watermark within an operator (BATCH processes key by key; Watermark switches MAX_VALUE→MIN_VALUE between each key; do not assume Watermark always ascending). Timers fire first in key order then in timestamp order within each key. Operations that change a key manually are not supported.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/
# Generating Watermarks

## Introduction to Watermark Strategies

To work with event time, Flink needs to know event timestamps — each element needs its event timestamp assigned (usually by accessing/extracting a field via a TimestampAssigner). Timestamp assignment goes hand-in-hand with generating watermarks (tell system about progress in event time) via a WatermarkGenerator. The Flink API expects a WatermarkStrategy that contains both a TimestampAssigner and WatermarkGenerator. Common strategies available as static methods on WatermarkStrategy; users can build custom. Interface: `public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T>` with createTimestampAssigner(ctx) and createWatermarkGenerator(ctx).

Usually use static helpers (e.g. bounded-out-of-orderness watermarks with lambda timestamp assigner). Specifying a TimestampAssigner is optional (e.g. Kafka/Kinesis provide timestamps directly). Attention: Both timestamps and watermarks are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.

## Using Watermark Strategies

Two places: 1) directly on sources (preferable — sources exploit knowledge about shards/partitions/splits; finer watermark tracking; more accurate overall watermark; requires source-specific interface), 2) after non-source operation (only if cannot set directly on source). Using after operations takes a stream and produces a new stream with timestamped elements and watermarks; if original had timestamps/watermarks, the timestamp assigner overwrites them.

## Dealing With Idle Sources

If one input split/partition/shard does not carry events for a while, the WatermarkGenerator gets no new info → idle input/source. Problem: watermark held back (computed as minimum over all parallel watermarks) even if other partitions carry events. Use WatermarkStrategy.withIdleness(...) to detect idleness and mark input as idle.

## Watermark alignment

Opposite of idle: a split/shard/source may process records very fast and increase its watermark faster than others. Downstream operators using watermarks (windowed joins, aggregations) may need to buffer excessive data from fast inputs (minimal watermark held back by lagging input → uncontrolled state growth). Enable watermark alignment to ensure no sources/splits/shards/partitions increase watermarks too far ahead. Enable per source via WatermarkStrategy.withWatermarkAlignment(group, maxDrift, updateInterval). Note: only for FLIP-27 sources; does not work for legacy or after assignTimestampsAndWatermarks.

Provide a group label (binds sources sharing it), maximal drift from current minimal watermark across group, and update interval (frequent updates = more RPC between TMs and JM). Flink pauses consuming from the source/task whose watermark is too far ahead; continues reading others to move combined watermark forward and unblock the faster one. As of Flink 1.17, split level watermark alignment supported by FLIP-27 source framework (connectors implement pause/resume splits interface). Upgrading from 1.15.x–1.16.x: disable split level alignment via pipeline.watermark-alignment.allow-unaligned-source-splits=true (only works properly when splits/shards/partitions count equals source operator parallelism; each subtask single unit of work). Flink also supports aligning across tasks of same/different sources (e.g. Kafka and File at different speeds).

## Writing WatermarkGenerators

WatermarkGenerator interface: onEvent(event, eventTimestamp, output), onPeriodicEmit(output), getCurrentWatermark() (deprecated). Two styles: periodic (observes events via onEvent(), emits watermark when framework calls onPeriodicEmit()) and punctuated (looks at events in onEvent(), waits for special marker/punctuation events carrying watermark info, emits watermark immediately; usually don't emit from onPeriodicEmit()).

### Writing a Periodic WatermarkGenerator

Interval (every n ms) defined via ExecutionConfig.setAutoWatermarkInterval(...). onPeriodicEmit() called each interval; new watermark emitted if returned non-null and larger than previous. Flink ships with BoundedOutOfOrdernessWatermarks (similar to example BoundedOutOfOrdernessGenerator). Not supported in Python API.

### Writing a Punctuated WatermarkGenerator

Emits watermark whenever an event indicates it carries a certain marker. Note: possible to generate watermark on every single event, but excessive watermarks degrade performance (each causes downstream computation).

## Watermark Strategies and the Kafka Connector

Each Kafka partition may have a simple event time pattern, but multiple partitions consumed in parallel interleave events and destroy per-partition patterns. Use Kafka-partition-aware watermark generation: watermarks generated inside the Kafka consumer per Kafka partition; per-partition watermarks merged same way as on stream shuffles. If timestamps strictly ascending per Kafka partition, per-partition ascending timestamps generator → perfect overall watermarks. No TimestampAssigner needed (Kafka record timestamps used).

## How Operators Process Watermarks

Operators required to completely process a given watermark before forwarding it downstream. E.g. WindowOperator evaluates all windows that should fire, produces all output, then sends the watermark downstream (all elements produced due to a watermark emitted before the watermark). TwoInputStreamOperator: current watermark = minimum of both inputs. Behavior defined by OneInputStreamOperator#processWatermark, TwoInputStreamOperator#processWatermark1/2.

## The Deprecated AssignerWithPeriodicWatermarks and AssignerWithPunctuatedWatermarks

Prior to WatermarkStrategy/TimestampAssigner/WatermarkGenerator, Flink used these. Still in API but recommended to use new interfaces (clearer separation of concerns, unify periodic and punctuated styles).
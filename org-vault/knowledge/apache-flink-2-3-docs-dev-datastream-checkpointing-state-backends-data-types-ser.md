---
title: 'Apache Flink 2.3 docs — Dev DataStream Checkpointing, State Backends, Data Types & Serialization'
tags: [org, flink, flink-2.3, docs, dev, datastream-api, checkpointing, state-backends, serialization, types, typeinformation, pojo, kryo, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:23:07.072Z'
updated: '2026-07-08T04:23:07.072Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/
# Checkpointing

Every function/operator in Flink can be stateful. To make state fault tolerant, Flink checkpoints state — checkpoints let Flink recover state and stream positions to give the application the same semantics as a failure-free execution.

## Prerequisites
- A persistent/durable data source that can replay records for a certain time (Kafka, RabbitMQ, Kinesis, PubSub, or file systems HDFS/S3/GFS/NFS/Ceph).
- Persistent storage for state, typically a distributed filesystem (HDFS/S3/GFS/NFS/Ceph).

## Enabling and Configuring Checkpointing
Disabled by default. enableCheckpointing(n) on StreamExecutionEnvironment, n = interval in ms. Parameters:
- Checkpoint storage: where snapshots are made durable. Default JobManager heap; for production use durable filesystem (job-wide & cluster-wide config).
- exactly-once vs at-least-once: pass a mode. Exactly-once preferable for most apps; at-least-once for super-low-latency (few ms) apps.
- checkpoint timeout: time after which an in-progress checkpoint is aborted.
- minimum time between checkpoints: ensures progress between checkpoints; next checkpoint starts no sooner than N after previous completed. Implies checkpoint interval never smaller than this. Implies number of concurrent checkpoints is one. Often easier to configure than interval (not susceptible to checkpoints taking longer than average). 
- tolerable checkpoint failure number: how many consecutive failures tolerated before whole job failover. Default 0 (fail on first). Applies only to: IOException on JobManager, failures in async phase on TaskManagers, checkpoint expiration due to timeout. Failures from sync phase on TaskManagers always force failover. Subsumed checkpoints ignored.
- number of concurrent checkpoints: default no checkpoint while one in progress. Multiple overlapping checkpoints interesting for pipelines with processing delay that still want very frequent checkpoints. Cannot be used when minimum time between checkpoints is defined.
- externalized checkpoints: persist periodic checkpoints externally; metadata written to persistent storage, NOT auto-cleaned on job failure. Resume from these on failure.
- unaligned checkpoints: greatly reduce checkpoint times under backpressure. Only exactly-once, only one concurrent checkpoint.
- checkpoints with finished tasks: default Flink continues checkpoints even if parts of DAG finished. See important considerations.

Java/Python enableCheckpointing examples. Related config options: [table: 34 rows].

## Selecting Checkpoint Storage
Stores consistent snapshots of all state in timers and stateful operators (connectors, windows, user-defined state). Location (JobManager memory, filesystem, database) depends on configured CheckpointStorage. Default JobManager memory. Configurable per-job. Strongly encouraged to use highly-available filesystem for production.

## State Checkpoints in Iterative Jobs
Flink only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job throws an exception. To force: env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force = true). Records in flight in loop edges (and associated state changes) will be lost during failure.

## Checkpointing with parts of the graph finished
Since Flink 1.14, can continue checkpoints even if parts of job graph finished (bounded sources). Enabled by default since 1.15; disable via feature flag. Once tasks/subtasks finished, they no longer contribute to checkpoints — important when implementing custom operators/UDFs. Adjusted task lifecycle, introduced StreamOperator#finish — clear cutoff point for flushing remaining buffered state. Checkpoints after finish() should be empty (mostly) and contain no buffered data (no way to emit). Exception: operators with pointers to transactions in external systems (exactly-once) — checkpoints after finish() should keep pointer to last transaction(s) committed in final checkpoint. Built-in examples: exactly-once sinks, TwoPhaseCommitSinkFunction.

### Impact on operator state
Special handling for UnionListState (often used for global view over offsets, e.g. Kafka partition offsets): checkpoints succeed only if none or all subtasks using UnionListState are finished (else would lose offsets for partitions of a finished subtask). ListState: any state checkpointed after close() is discarded and not available after restore. Any operator prepared to be rescaled works with partially-finished tasks; restoring from a checkpoint where only a subset finished == restoring with new subtask count equal to running tasks.

### Waiting for the final checkpoint before task exit
For two-phase commit operators, tasks wait for the final checkpoint to complete successfully after all operators finished. Final checkpoint triggered immediately after all operators reach end-of-data, without waiting for periodic triggering, but job waits for it to complete.

## Unify file merging mechanism for checkpoints (Experimental)
Introduced Flink 1.20 (MVP). Scattered small checkpoint files written into larger files, reducing file creations/deletions, alleviating filesystem metadata pressure from file flooding. Enable execution.checkpointing.file-merging.enabled=true. Trade-off: space amplification (actual occupation larger than state size); limit via execution.checkpointing.file-merging.max-space-amplification. Applies to keyed state, operator state, channel state. Subtask-level merging for shared scope; TaskManager-level for private scope. execution.checkpointing.file-merging.max-subtasks-perfile limits subtasks per file. Cross-checkpoint merging: execution.checkpointing.file-merging.across-checkpoint-boundary=true. File pool for concurrent writing: non-blocking mode (always provides usable file, may create many) vs blocking mode (blocks until file available); execution.checkpointing.file-merging.pool-blocking true=blocking.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_backends/
# State Backends

Flink provides different state backends specifying how/where state is stored. State on Java heap or off-heap. Depending on backend, Flink can manage state memory (possibly spilling to disk) for very large state. Default backend determined by Flink config file. Default can be overridden per-job (Java/Python Configuration examples). For available backends, advantages, limitations, config params see Deployment & Operations section.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/serialization/types_serialization/
# Data Types & Serialization

Flink handles data types/serialization uniquely — own type descriptors, generic type extraction, type serialization framework.

## Supported Data Types
Eight categories: Java Tuples, Java POJOs, Primitive Types, Common Collection Types, Regular Classes, Values, Hadoop Writables, Special Types.

#### Tuples
Composite, fixed number of fields, various types. Java API Tuple1..Tuple25. Fields arbitrary Flink types incl. nested tuples. Access via tuple.f4 or tuple.getField(int position); indices start at 0 (contrast to Scala tuples, consistent with Java indexing).

#### POJOs
Treated as POJO type if: class public; public no-arg constructor; all fields public or accessible via getFoo()/setFoo() getters/setters; field type supported by registered serializer. Java records recognized as POJO types since Flink 1.19 (FLINK-32380) — public record serialized by PojoSerializer via canonical constructor (no-arg constructor and getter/setter requirements waived). POJOs represented with PojoTypeInfo, serialized with PojoSerializer (Kryo as configurable fallback). Exception: Avro types (Specific Records or "Avro Reflect Types") → AvroTypeInfo + AvroSerializer. Can register custom serializers. Flink analyzes POJO structure → easier to use and more efficient than general types. Test via PojoTestUtils#assertSerializedAsPojo(); assertSerializedAsPojoWithoutKryo() to ensure no Kryo.

#### Primitive Types
All Java primitives (Integer, String, Double).

#### Common Collection Types
Dedicated serialization for Map, List, Set and super-interface Collection — more efficient than general purpose (avoids type metadata analysis). Requirements: concrete type arguments (List<String> not List/List<T>/List<?>); interface types (List<String> not LinkedList<String> — Flink does not preserve implementation types across serialization). Non-qualified → general class types. If implementation types must be preserved, register custom serializer.

#### General Class Types
Most Java classes (API and custom). Restrictions: no unserializable fields (file pointers, I/O streams, native resources). Java Beans conventions work well. Classes not identified as POJO → general class types, treated as black boxes (no content access for efficient sorting), de/serialized via Kryo.

#### Values
Describe serialization/deserialization manually — implement org.apache.flink.types.Value with read/write methods. Reasonable when general purpose serialization highly inefficient (e.g. sparse vector — special encoding for non-zero elements). CopyableValue supports manual internal cloning. Pre-defined Value types: ByteValue, ShortValue, IntValue, LongValue, FloatValue, DoubleValue, StringValue, CharValue, BooleanValue — mutable variants of basic types (alterable, reuse objects, reduce GC pressure).

#### Hadoop Writables
Implement org.apache.hadoop.Writable; serialization logic in write()/readFields().

#### Special Types
Custom Either implementation — value of two possible types Left or Right (like Scala's Either). Useful for error handling or operators outputting two different record types.

#### Type Erasure & Type Inference
(Java only.) Java compiler throws away generic type info after compilation (type erasure). At runtime DataStream<String> and DataStream<Long> look the same to JVM. Flink needs type info when preparing program for execution. Java API tries to reconstruct erased type info, stores it explicitly; retrieve via DataStream.getType() → TypeInformation. Inference has limits, needs programmer cooperation (e.g. fromCollection() takes type arg; MapFunction<I,O> may need extra info). ResultTypeQueryable interface lets input formats/functions tell API their return type explicitly.

## Type handling in Flink
Flink infers data type info exchanged/stored during distributed computation (like a database inferring schema). Usually seamless. Benefits: better serialization/data layout (important for memory paradigm — work on serialized data on/off heap, cheap serialization); spares users from worrying about serialization frameworks. Type info needed during pre-flight phase (DataStream calls, before execute()/print()/count()/collect()).

## Most Frequent Issues
- Registering subtypes: function signatures describe supertypes but use subtypes → register serializers per subtype via pipeline.serialization-config (programmatically via Configuration).
- Registering custom serializers: Flink falls back to Kryo for types not handled transparently. Not all types work with Kryo (e.g. Guava collections). Register additional serializers via pipeline.serialization-config.
- Adding Type Hints: when Flink cannot infer generic types, pass type hint (Java API).
- Manually creating TypeInformation: for API calls where inference impossible due to type erasure.

## Flink's TypeInformation class
Base class for all type descriptors. Reveals basic type properties, generates serializers and (in specializations) comparators. Comparators do more than define order — basically the utility to handle keys. Internal distinctions: Basic types (Java primitives + boxed, void, String, Date, BigDecimal, BigInteger); Primitive/Object arrays; Composite types; Flink Java Tuples (max 25 fields, null fields not supported); Row (arbitrary fields, supports nulls); POJOs; Auxiliary types (Option, Either, Lists, Maps); Generic types (serialized by Kryo). POJOs of particular interest — complex types, transparent to runtime, handled efficiently.

### Rules for POJO types
Recognized as POJO if: class public and standalone (no non-static inner class); public no-arg constructor; all non-static non-transient fields (and superclasses) public non-final OR have public getter/setter following Java beans naming. Java records also recognized — must be public, no-arg constructor and getter/setter rules waived, instantiated via canonical constructor. If not recognized as POJO → GenericType, serialized with Kryo.

### Creating a TypeInformation or TypeSerializer
Java erases generic type info. Non-generic: TypeInformation.of(String.class). Generic: TypeInformation.of(new TypeHint<Tuple2<String,Double>>(){}) — anonymous subclass captures generic info. Two ways to create TypeSerializer: typeInfo.createSerializer(config) (config=SerializerConfig, holds registered custom serializers); getRuntimeContext().createSerializer(typeInfo) within a Rich Function.

## Type Information in the Java API
Java erases generics; Flink reconstructs via reflection (function signatures, subclass info), with simple type inference for return type depending on input type. Cases where reconstruction incomplete → type hints.

### Type Hints in the Java API
returns() tells system the produced type. Supports: Classes (non-parameterized); TypeHints returns(new TypeHint<Tuple2<Integer,SomeType>>(){}).

### Type extraction for Java 8 lambdas
Different from non-lambdas (no implementing class extending function interface). Flink figures out which method implements the lambda, uses Java's generic signatures for param/return types — but signatures not generated for lambdas by all compilers. Unexpected behavior → manually specify via returns().

### Serialization of POJO types
PojoTypeInfo creates serializers for all fields. Standard types handled by Flink serializers; others fall back to Kryo. If Kryo can't handle: ask PojoTypeInfo to serialize via Avro (include flink-avro, set pipeline.force-avro: true). Flink auto-serializes Avro-generated POJOs with Avro serializer. Force entire POJO via Kryo: pipeline.force-kryo: true. Add custom Kryo serializer via pipeline.serialization-config.

## Disabling Kryo Fallback
Set pipeline.generic-types: false — exception raised whenever a data type would go through Kryo. Ensures all types efficiently serialized via Flink's own or user-defined custom serializers.

## Defining Type Information using a Factory
TypeInfoFactory lets you plug user-defined type info into Flink type system. Implement org.apache.flink.api.common.typeinfo.TypeInfoFactory. Called during type extraction. Closest factory chosen traversing upwards in hierarchy; built-in factory has highest precedence; factory also has higher precedence than Flink built-in types. Associate via pipeline.serialization-config OR @org.apache.flink.api.common.typeinfo.TypeInfo annotation (config option has higher precedence). createTypeInfo(Type, Map<String,TypeInformation<?>>) creates type info; params give type info and generic type parameters. If type has generic parameters derived from a function's input type, implement TypeInformation#getGenericParameters for bidirectional mapping.
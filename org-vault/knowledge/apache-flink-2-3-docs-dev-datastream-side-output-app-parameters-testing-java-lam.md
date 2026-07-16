---
title: 'Apache Flink 2.3 docs — Dev DataStream Side Output, App Parameters, Testing, Java Lambdas, Execution Config, Parallel Execution'
tags: [org, flink, flink-2.3, docs, dev, datastream-api, side-output, parameter-tool, testing, test-harness, java-lambdas, execution-config, parallelism, packaging, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:28:38.572Z'
updated: '2026-07-08T04:28:38.572Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/side_output/
# Side Outputs
Produce any number of additional side output result streams in addition to main stream. Type need not match main stream; side output types can differ. Useful to split a stream where you'd normally replicate + filter. Define an OutputTag to identify a side output stream, typed per side-output element type. Emit data to side output from: ProcessFunction, KeyedProcessFunction, CoProcessFunction, KeyedCoProcessFunction, ProcessWindowFunction, ProcessAllWindowFunction — use the Context parameter to emit data to a side output identified by an OutputTag. Retrieve side output stream via getSideOutput(OutputTag) on the result of the DataStream operation (typed to the side output). Python note: if it produces side output, get_side_output(OutputTag) must be called in Python API — otherwise side output streams into main stream (unexpected, may fail job when types differ).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/application_parameters/
# Handling Application Parameters
Almost all Flink apps rely on external config parameters (input/output sources, system params like parallelism, app-specific params). ParameterTool = simple utility (internally expects Map<String,String>; integrate with own config style). Other frameworks (Commons CLI, argparse4j) also work.
### Getting config into ParameterTool
- From .properties files: ParameterTool.fromPropertiesFile(propertiesFilePath).
- From command line args: ParameterTool.fromArgs(args) — gets args like --input hdfs:///mydata --elements 42.
- From system properties: ParameterTool.fromSystemProperties() (pass -Dinput=hdfs:///mydata to JVM).
### Using parameters
- Directly from ParameterTool (has access methods). Use return values directly in main() of submitting client — e.g. set operator parallelism.
- ParameterTool is serializable → pass it to functions; use inside function for getting command-line values.
- Register globally: register as global job parameters in ExecutionConfig → accessible as config values from JobManager web interface and in all user functions. Access in any rich user function via getRuntimeContext().

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/testing/
# Testing
## Testing UDFs
Assume Flink produces correct results outside a UDF → unit-test classes with main business logic as much as possible.
### Unit Testing Stateless, Timeless UDFs
E.g. stateless MapFunction — unit test by passing args, verifying output. UDF using org.apache.flink.util.Collector (FlatMapFunction/ProcessFunction) → test by providing a mock object instead of a real collector.
### Unit Testing Stateful or Timely UDFs & Custom Operators
Involves testing interaction between user code and Flink runtime. Test harnesses:
- OneInputStreamOperatorTestHarness (operators on DataStreams)
- KeyedOneInputStreamOperatorTestHarness (operators on KeyedStreams)
- TwoInputStreamOperatorTestHarness (operators of ConnectedStreams of two DataStreams)
- KeyedTwoInputStreamOperatorTestHarness (operators on ConnectedStreams of two KeyedStreams)
Push records/watermarks into UDFs or custom operators, control processing time, assert on output (incl. side outputs). KeyedOne/Two additionally provide a KeySelector + TypeInformation for the key class. Note: AbstractStreamOperatorTestHarness and derived classes are NOT part of public API, subject to change.
#### Unit Testing ProcessFunction
ProcessFunctionTestHarnesses — test harness factory for easier instantiation. Needs dependencies mentioned in last section. Examples for KeyedProcessFunction, KeyedCoProcessFunction, BroadcastProcessFunction etc. in ProcessFunctionTestHarnessesTest.
## Testing Flink Jobs
### JUnit Rule MiniClusterWithClientResource
Tests complete jobs against a local embedded mini cluster. One additional test-scoped dependency needed. Remarks:
- Make sources/sinks pluggable in production code; inject test sources/sinks in tests (don't copy whole pipeline).
- Static variable in CollectSink works because Flink serializes all operators before distributing; communicating with operators instantiated by local mini cluster via static variables is one workaround. Alternatively write data to files in temp dir.
- Custom parallel source function for emitting watermarks if job uses event-time timers.
- Always test pipelines locally with parallelism > 1 to surface parallel-only bugs.
- Prefer @ClassRule over @Rule so multiple tests share the same Flink cluster (startup/shutdown dominate execution time).
- Test custom state correctness by enabling checkpointing + restarting job within mini cluster — trigger failure by throwing exception from a test-only UDF.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/experimental/
# Experimental Features
Still evolving — unstable, incomplete, or subject to heavy change.
## Reinterpreting a pre-partitioned data stream as keyed stream
Re-interpret a pre-partitioned data stream as a keyed stream to avoid shuffling. WARNING: re-interpreted stream MUST already be pre-partitioned EXACTLY as Flink's keyBy would partition w.r.t. key-group assignment. Use-case: materialized shuffle between two jobs — first job performs keyBy shuffle + materializes each output into a partition; second job's sources read corresponding partitions and re-interpret as keyed streams (e.g. apply windowing). Makes second job embarrassingly parallel → fine-grained recovery. Exposed via DataStreamUtils.reinterpretAsKeyedStream(baseStream, keySelector, typeInformation).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/java_lambdas/
# Java Lambda Expressions
Flink supports lambda expressions for all operators of the Java API; but whenever a lambda uses Java generics you must declare type information explicitly. Example: inline map() squaring input — types inferred by Java compiler (OUT not generic → Integer extracted automatically). flatMap() with signature void flatMap(IN value, Collector<OUT> out) compiles to void flatMap(IN value, Collector out) → Flink cannot infer output type → throws InvalidTypesException; specify type info explicitly (otherwise output treated as Object → inefficient serialization). Same for map() with generic return type (Tuple2<Integer,Integer> map(Integer) erasured to Tuple2 map(Integer)). Solutions: use org.apache.flink.api.common.typeinfo.Types, .returns(TypeInformation), or a subclass (anonymous class) instead of lambda.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/execution_configuration/
# Execution Configuration
StreamExecutionEnvironment contains ExecutionConfig for job-specific runtime config. To change defaults affecting all jobs, see Configuration. Options (default bold):
- setClosureCleanerLevel() — default ClosureCleanerLevel.RECURSIVE. Removes unneeded references to surrounding class of anonymous functions. NONE (disable), TOP_LEVEL (clean top-level only), RECURSIVE (all fields). Disabled → anonymous function may reference non-Serializable surrounding class → serializer exceptions.
- getParallelism()/setParallelism(int) — default job parallelism.
- getMaxParallelism()/setMaxParallelism(int) — default max parallelism; upper limit for dynamic scaling.
- getNumberOfExecutionRetries()/setNumberOfExecutionRetries(int) — times failed tasks re-executed; 0 disables fault tolerance; -1 = system default. DEPRECATED, use restart strategies.
- getExecutionRetryDelay()/setExecutionRetryDelay(long) — delay ms after job failed before re-execution (starts after all tasks stopped). DEPRECATED, use restart strategies.
- getExecutionMode()/setExecutionMode() — default PIPELINED; batch vs pipelined data exchanges.
- enableForceKryo()/disableForceKryo — default not forced; forces GenericTypeInformation to use Kryo for POJOs even when analyzable (e.g. when Flink internal serializers fail on a POJO).
- enableForceAvro()/disableForceAvro — default not forced; forces Flink AvroTypeInfo to use Avro serializer instead of Kryo for Avro POJOs.
- enableObjectReuse()/disableObjectReuse() — default not reused; enables runtime to reuse user objects for performance (can cause bugs if user-code not aware).
- getGlobalJobParameters()/setGlobalJobParameters() — custom objects as global config (ExecutionConfig accessible in all UDFs).
- addDefaultKryoSerializer(Class, Serializer) / addDefaultKryoSerializer(Class, Class) — register Kryo serializer instance/class for a type.
- registerTypeWithKryoSerializer(Class, Serializer) — register type with Kryo + serializer (more efficient).
- registerKryoType(Class) — if type serialized with Kryo, registered so only tags (integer IDs) written; unregistered → entire class-name serialized per instance (higher I/O).
- registerPojoType(Class) — registers with serialization stack; if serialized as POJO → registered with POJO serializer; if Kryo → registered so only tags written. Note: types registered with registerKryoType() are NOT available to Flink's POJO serializer instance.
- disableAutoTypeRegistration() — auto type registration on by default (registers all types incl sub-types used by usercode with Kryo + POJO serializer).
- setTaskCancellationInterval(long) — interval ms between consecutive interrupt() attempts when canceling a running task; default 30000 ms (30s).
RuntimeContext (accessible in Rich* functions via getRuntimeContext()) also allows accessing ExecutionConfig in all UDFs.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/packaging/
# Program Packaging and Distributed Execution
Programs packaged into JAR files for execution (prerequisite for command line / web interface execution). Use environment from StreamExecutionEnvironment.getExecutionEnvironment() — acts as cluster's environment when JAR submitted; acts like local environment otherwise. Export all involved classes as JAR; manifest must point to entry-point class (public main method) via main-class attribute (same as JVM uses for java -jar). Most IDEs include automatically.
### Summary (two steps)
- JAR's manifest searched for main-class or program-class attribute. If both found, program-class takes precedence. CLI + web interface support a parameter to pass entry point class manually when manifest has neither.
- System invokes the main method of the class.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/parallel/
# Parallel Execution
A Flink program = multiple tasks (transformations/operators, sources, sinks). A task split into parallel instances; number of parallel instances = its parallelism. For savepoints, consider setting a maximum parallelism (max parallelism) — when restoring from a savepoint you can change operator/program parallelism; this setting is an upper bound. Required because Flink internally partitions state into key-groups (cannot have +Inf key-groups — detrimental to performance).
## Setting the Parallelism (4 levels)
- Operator Level: setParallelism() on individual operator/source/sink.
- Execution Environment Level: setParallelism() on env defines default for all operators (overridden by operator-level).
- Client Level: set at Client when submitting. CLI: -p (e.g. ./bin/flink run -p 10 ../examples/*WordCount-java*.jar). In a client program via StreamExecutionEnvironment. (Python API: not supported.)
- System Level: parallelism.default property in Flink config file.
## Setting the Maximum Parallelism
Set where you can set parallelism (except client level and system level) via setMaxParallelism(). Default ~ operatorParallelism + (operatorParallelism/2), lower bound 128, upper bound 32768. Very large max parallelism detrimental to performance (some state backends keep internal data structures scaling with key-groups). Changing max parallelism explicitly when recovering from original job → state incompatibility.

## (no article) Source: .../dev/datastream/sinks/ — stub page (real content in Deployment/connectors).
## (no article) Source: .../dev/datastream/sources/ — stub page (real content in connectors/sources).
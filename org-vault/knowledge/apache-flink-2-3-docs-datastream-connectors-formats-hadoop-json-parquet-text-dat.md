---
title: Apache Flink 2.3 docs — DataStream connectors (formats hadoop/json/parquet/text + datagen + dynamic-kafka + kafka + cassandra + dynamodb + elasticsearch)
tags: [org, flink, flink-2.3, docs, connectors, datastream, kafka, cassandra, dynamodb, elasticsearch, parquet, datagen, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:44:25.634Z'
updated: '2026-07-08T04:44:25.634Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Captured Apache Flink 2.3 docs DataStream connectors batch conn5 (indices 211-220): remaining datastream/formats (hadoop, json, parquet, text_files), datastream/datagen, datastream/dynamic-kafka (experimental), datastream/kafka (the big one), datastream/cassandra, datastream/dynamodb (source + sink), datastream/elasticsearch (v6/v7/v8). WHY: ongoing ingestion of full Flink 2.3 EN docs into org memories. Prose near-verbatim; code `[code: <first-line> ... (N chars)]`; tables `[table: N rows]`.

## Source: .../datastream/formats/hadoop/ — Hadoop formats
flink-hadoop-compatibility Maven module. Dep `<dependency> ... (142 chars)`; local IDE also needs hadoop-client `<dependency> ... (168 chars)`. Use Hadoop InputFormats: wrap via HadoopInputs.readHadoopFile (FileInputFormat-derived) or createHadoopInput (general). Result DataStream = 2-tuples (key, value). Example TextInputFormat `[code: StreamExecutionEnvironment env ... (337 chars)]`.

## Source: .../datastream/formats/json/ — Json format
Add Flink JSON dep `<dependency> ... (151 chars)`. PyFlink direct. Read/write via JsonSerializationSchema/JsonDeserializationSchema (Jackson; supports POJOs/ObjectNode). JsonDeserializationSchema with KafkaSource POJO `[code: JsonDeserializationSchema<SomePojo> ... (219 chars)]`. JsonSerializationSchema with KafkaSink `[code: ... (307 chars)]`. Custom Mapper: SerializableSupplier<ObjectMapper> factory for full Jackson control `[code: ... (223 chars)]`. Python: JsonRowSerializationSchema/JsonRowDeserializationSchema built-in for Row, used in KafkaSource/KafkaSink `[code: row_type_info ... (260 chars)]` `[code: ... (405 chars)]`.

## Source: .../datastream/formats/parquet/ — Parquet format
Read Parquet → Flink RowData + Avro records. Dep flink-parquet `<dependency> ... (129 chars)`; Avro records need parquet-avro `<dependency> ... (477 chars)`. PyFlink deps [table: 2 rows]. Compatible with new Source — bounded (list+read all files) + unbounded (monitor dir for new files; call AbstractFileSourceBuilder.monitorContinuously(Duration)). Vectorized reader (Java/Python `[code:  ... (260/274 chars)]`), Avro Parquet reader (Java/Python `[code:  ... (272/286 chars)]`).
## Flink RowData: schema projected to fields f7/f4/f99, batches of 500, 1st bool=timestamp-as-UTC, 2nd bool=case-sensitive field names, no watermark. Java `[code: final LogicalType[] fieldTypes ... (735 chars)]`, Python `[code: row_type ... (484 chars)]`.
## Avro Records (3 types; PyFlink only Generic): Generic (JSON schema `[code: {"namespace"...} ... (242 chars)]`, parse + AvroParquetReaders `[code: // parsing avro schema ... (1036 chars)]` / Py `[code: ... (595 chars)]`); Specific (Avro codegen, .avsc `[code: [...] ... (343 chars)]`, generated Address class `[code: @AvroGenerated ... (192 chars)]`, `[code: final FileSource<GenericRecord> ... (450 chars)]`); Reflect (Java POJO reflection → Avro schema, Datum POJO `[code: public class Datum ... (675 chars)]`, `[code: ... (447 chars)]`).
### Prerequisite for Parquet files (Reflect): Parquet file must contain meta info; Avro schema must contain namespace pointing to concrete Java class package. User schema w/ namespace `[code: // avro schema with namespace ... (638 chars)]`, resulting parquet meta `[code: creator: parquet-mr ... (1180 chars)]`, User class `[code: public class User ... (627 chars)]`, read `[code: ... (430 chars)]`.

## Source: .../datastream/formats/text_files/ — Text files format
Read text lines via TextLineInputFormat (Java InputStreamReader, various charsets). Dep Flink Connector Files `<dependency> ... (137 chars)`. Compatible with new Source — bounded (batch) + continuous (streaming, monitor dir for new files). Bounded example (Java `[code: final FileSource<String> ... (234 chars)]` / Py `[code: source = FileSource.for_record_stream_format ... (175 chars)]`). Continuous example: monitor new files each second (Java `[code: ... (283 chars)]` / Py `[code: source = FileSource \ ... (240 chars)]`).

## Source: .../datastream/datagen/ — DataGen Connector
Source for generating input data; useful for local dev/demos without external systems (Kafka). Built-in, no extra deps. DataGeneratorSource produces N data points in parallel, splits sequence into parallel sub-sequences per subtask, drives via Long "index" → GeneratorFunction. Example: ["Number: 0".."Number: 999"] `[code: GeneratorFunction<Long,String> ... (369 chars)]`. Order depends on parallelism (parallelism=1 → in-order).
## Rate Limiting: built-in, e.g. ≤100 events/sec overall `[code: ... (291 chars)]`. Additional strategies (per-checkpoint) in RateLimiterStrategy.
## Boundedness: always bounded; Long.MAX_VALUE → effectively unbounded. Finite → consider BATCH mode.
## Notes: at-least-once/exactly-once if GeneratorFunction deterministic (same Long → same output). Can produce deterministic watermarks via custom WatermarkStrategy.

## Source: .../datastream/dynamic-kafka/ — Dynamic Kafka Source (Experimental)
Reads Kafka topics from one/more clusters; discovers clusters/topics via Kafka metadata service; dynamic — change topics/clusters without job restart (cluster migration/failover, Hybrid Source integration). NOTE: no connector yet available for Flink 2.3 (not in binary dist).
## Dynamic Kafka Source (new data source API). Usage: build DynamicKafkaSource, earliest offset of "input-stream", deserialize value as string, MyKafkaMetadataService resolves cluster(s)/topic(s) `[code:  ... (540 chars)]` / Py `[code: metadata_service = SingleClusterTopicMetadataService ... (500 chars)]`. Required: KafkaMetadataService, stream ids (subscription), deserializer.
### Offsets Initialization: global starting/stopping offsets via builder (stopping only bounded mode); cluster metadata may carry per-cluster offset initializers overriding globals `[code: Properties cluster0Props ... (1281 chars)]`.
### Split Assignment Mode: per_cluster (default, assigns within each cluster; owner=startIndex=((topic.hashCode()*31)&0x7FFFFFFF)%P, owner=(startIndex+partition)%P) OR global (one cursor: owner=knownActiveSplitIds.size()%numReaders; forward-looking, doesn't proactively migrate active splits; rebalance via restore+parallelism change) `[code: DynamicKafkaSource<String> ... (374 chars)]`.
### Watermark Alignment: forwards split pause/resume to underlying Kafka readers — works like regular Kafka Source.
### Kafka Stream Subscription: set of stream ids `[code: ...setStreamIds(Set.of("stream-a","stream-b")) ... (74 chars)]` / Py, OR regex pattern `[code: ...setStreamPattern(Pattern.of("stream.*")) ... (70 chars)]`.
### Kafka Metadata Service: interface resolving logical streams → physical topics/clusters; typically internal infra service or in-memory impl (example in tests). Source polls periodically, reconciles readers. Cluster metadata may carry per-cluster offset initializers.
### Additional Properties: DynamicKafkaSourceOptions `[table: 4 rows]` + regular Kafka connector properties.
### Metrics: `[table: 6 rows]` + KafkaSourceReader metrics.
### Behind the Scene (FLIP-246): Source Split = cluster id + Kafka Source Split (TopicPartition, start/stop offset). Split Enumerator discovers metadata, initializes KafkaSourceEnumerators, polls metadata service, clears outdated metrics on cluster removal. Source Reader reads from one/more clusters via KafkaSourceReader, reconciles metadata (restarts KafkaSourceReader on new topics/clusters). Kafka Metadata Service = source of truth; removed metadata = non-active.

## Source: .../datastream/kafka/ — Apache Kafka Connector
Read/write Kafka with exactly-once guarantees. Universal connector tracks latest Kafka client (backwards compatible w/ brokers ≥2.1.0). NOTE: no connector yet for Flink 2.3. PyFlink deps `[table: 2 rows]`.
## Kafka Source (new data source API). Usage: build KafkaSource, earliest offset "input-topic", group "my-group", deserialize value as string `[code: KafkaSource<String> source ... (350 chars)]` / Py `[code: source = KafkaSource.builder() \ ... (345 chars)]`. Required: bootstrap servers, topics/partitions subscription, deserializer.
### Topic-partition Subscription: (1) topic list `[code: ...setTopics("topic-a","topic-b") ... (54 chars)]`, (2) topic pattern (regex) `[code: ...setTopicPattern("topic.*") ... (49 chars)]`, (3) partition set `[code: final HashSet<TopicPartition> ... (282 chars)]` / Py `[code: partition_set = ... (148 chars)]`.
### Deserializer: setDeserializer(KafkaRecordDeserializationSchema) or setValueOnlyDeserializer(DeserializationSchema); can use Kafka Deserializer (e.g. StringDeserializer) `[code: import ...StringDeserializer ... (191 chars)]`. PyFlink only set_value_only_deserializer `[code: ... (71 chars)]`.
### Starting Offset: OffsetsInitializer (earliest default); built-ins `[code: KafkaSource.builder() ... (712 chars)]` / Py `[code: ... (765 chars)]`; custom initializer not in PyFlink.
### Boundedness: default streaming (never stops); setBounded(OffsetsInitializer) → batch mode (stops at offset); setUnbounded(OffsetsInitializer) → streaming but stops at offset.
### Additional Properties: client.id.prefix, partition.discovery.interval.ms (dynamic partition discovery), register.consumer.metrics, commit.offsets.on.checkpoint. Overridden keys: auto.offset.reset.strategy (by OffsetsInitializer), partition.discovery.interval.ms (→-1 when bounded).
### Dynamic Partition Discovery: partition.discovery.interval.ms (default 5 min; disable via non-positive) `[code: KafkaSource.builder() ... (125 chars)]` / Py.
### Event Time and Watermarks: default uses Kafka ConsumerRecord timestamp; custom WatermarkStrategy `[code: env.fromSource(kafkaSource, new CustomWatermarkStrategy(), ... (106 chars)]` (not PyFlink).
### Idleness: not auto-idle if parallelism > partitions; lower parallelism or WatermarkStrategy#withIdleness.
### Consumer Offset Committing: commits on checkpoint completion (consistency w/ Flink state); without checkpointing relies on Kafka auto-commit (enable.auto.commit, auto.commit.interval.ms). NOTE: Flink does NOT rely on committed offsets for fault tolerance — only for monitoring.
### Monitoring: metrics `[table: 9 rows]` (incl. instantaneous last-record latency) + KafkaSourceReader.KafkaConsumer.* (register.consumer.metrics default true). InstanceAlreadyExistsException warning → unique client.id.prefix per KafkaSource.
### Security: configure as properties (SASL PLAIN JAAS `[code: ... (272 chars)]` / Py; SASL_SSL+SCRAM-SHA-256 `[code: ... (832 chars)]` / Py). NOTE: login module class path may differ if Kafka client deps relocated in job JAR.
## Kafka Rack Awareness: setRackIdSupplier() sets client.rack (consumer reads closest brokers — reduced cost/latency) `[code: .setRackIdSupplier(() -> System.getenv("TM_NODE_AZ")) ... (53 chars)]`.
### Behind the Scene (FLIP-27): Source Split = TopicPartition + start/stop offset (+ current consuming offset in state). Split Enumerator discovers+assigns splits round-robin, pushes eagerly. Source Reader extends SourceReaderBase, single-thread-multiplexed (one KafkaConsumer/SplitReader for multiple splits), KafkaRecordEmitter assigns event time.
## Kafka SourceFunction: FlinkKafkaConsumer DEPRECATED (removed Flink 1.17) → use KafkaSource.
## Kafka Sink: write stream to one/more topics. Builder, String records, at-least-once `[code: DataStream<String> stream ... (447 chars)]` / Py `[code: sink = KafkaSink.builder() \ ... (373 chars)]`. Required: bootstrap servers, record serializer, transactionalIdPrefix (if EXACTLY_ONCE).
### Serializer: KafkaRecordSerializationSchema (builder for key/value/topic/partitioning) `[code: KafkaRecordSerializationSchema.builder() ... (286 chars)]` / Py; or setKafkaKeySerializer/setValueSerializer. Must set value serialization + topic.
### Fault Tolerance: 3 DeliveryGuarantees — NONE (may lose/duplicate), AT_LEAST_ONCE (waits for Kafka ack on checkpoint; may duplicate on Flink restart), EXACTLY_ONCE (Kafka transaction committed on checkpoint; consumer reads committed only via isolation.level; delays visibility; unique transactionalIdPrefix across apps; tune transaction.timeout.ms > max checkpoint + max restart duration). Checkpointing required for AT_LEAST_ONCE/EXACTLY_ONCE. Default NONE.
### Monitoring: `[table: 2 rows]`.
## Kafka Producer: FlinkKafkaProducer DEPRECATED (removed Flink 1.15) → use KafkaSink.
## Kafka Connector Metrics: producers/consumers export Kafka internal metrics via Flink metric system; disable via register.consumer.metrics (source) / register.producer.metrics=false (sink).
## Enabling Kerberos Authentication: configure flink-conf.yaml — security.kerberos.login.use-ticket-cache (default true; won't work on YARN), .keytab/.principal, append KafkaClient to security.kerberos.login.contexts. Then set security.protocol=SASL_PLAINTEXT (or SASL_SSL standalone), sasl.kerberos.service.name=kafka (must match broker).
## Upgrading to Latest Connector Version: don't upgrade Flink+Kafka connector simultaneously; configure group.id; setCommitOffsetsOnCheckpoints(true) before savepoint; setStartFromGroupOffsets(true); change source/sink uid; start with --allow-non-restored-state.
## Troubleshooting: Data loss (acks/log.flush.* defaults); UnknownTopicOrPartitionException (new leader election, retriable; retries may reorder → max.in.flight=1); ProducerFencedException (transaction timeout; KAFKA-6119 fences producerId/epoch).

## Source: .../datastream/cassandra/ — Apache Cassandra Connector
Sinks writing to Cassandra. Dep `<dependency> ... (155 chars)`. Not in binary dist.
## Installing Cassandra: Getting Started page or Official Docker.
## Cassandra Source: FLIP-27 bounded source, returns DataStream<Entity>; entity via Cassandra mapper (MappingManager) from annotated POJO. Usage `[code: ClusterBuilder clusterBuilder ... (1239 chars)]`. Perf: numSplits=tableSize/maxSplitMemorySize; fallback numSplits=parallelism.
## Cassandra Sinks: CassandraSink.addSink(DataStream) → CassandraSinkBuilder. Config methods: setQuery (CQL upsert; DO for Tuple, DON'T for POJO), setClusterBuilder, setHost, setMapperOptions (POJO only), setMaxConcurrentRequests (only w/o WAL), enableWriteAheadLog (exactly-once for non-deterministic), setFailureHandler, setDefaultKeyspace, enableIgnoreNullFields (null=unset, no tombstones), build.
### Write-ahead Log: CheckpointCommitter stores completed-checkpoint info (CassandraCommitter → separate table, NOT cleaned by Flink). Exactly-once if query idempotent + checkpointing; WAL needed for non-deterministic programs (replay identical to first attempt; latency impact). Experimental.
### Checkpointing: at-least-once delivery of action requests.
## Examples: supports Tuple + POJO (auto-detected). Keyspace example + Table wordcount `[code: CREATE KEYSPACE ... (222 chars)]`. Tuple example (CQL upsert via setQuery, PreparedStatement) Java `[code: // get the execution environment ... (1281 chars)]` / Scala `[code: ... (783 chars)]`. POJO example (DataStax @Table/@Column annotations, Mapper) Java `[code: ... (2150 chars)]`.

## Source: .../datastream/dynamodb/ — Amazon DynamoDB Connector
Source: read CDC stream from DynamoDB tables via DynamoDB Streams. Sink: write via BatchWriteItem API. NOTE: no connector yet for Flink 2.3.
## Amazon DynamoDB Streams Source: AWS v2 SDK. Events depend on StreamViewType.
### Usage: DynamoDbStreamsSource builder `[code: // Configure the DynamodbStreamsSource ... (1454 chars)]` / Scala `[code: ... (1238 chars)]`. Stream ARN, Flink Configuration (AWSConfigOptions + DynamodbStreamsSourceConfigConstants), starting position TRIM_HORIZON, SimpleStringSchema, UniformShardAssigner, increasing WatermarkStrategy (approximateCreationDateTime event time, 1s idle).
### Configuring Starting Position: STREAM_INITIAL_POSITION — LATEST (from latest) or TRIM_HORIZON (earliest; data trimmed after 24h).
### Deserialization Schema: DynamoDbStreamsDeserializationSchema<T> (Record from DynamoDB model; content depends on StreamViewType).
### Event Ordering: events within same primary key → same shard lineage; ordering maintained across shard splits if parent fully read before children. Source assigns shards respecting parent-child ordering.
### Shard Assignment Strategy: UniformShardAssigner (even allocation; new shards → subtask with fewest); custom via DynamoDbStreamsShardAssigner.
### Configuration: Retry Strategy (DYNAMODB_STREAMS_RETRY_COUNT, _EXPONENTIAL_BACKOFF_MIN/MAX_DELAY); Shard Discovery (every 60s default; SHARD_DISCOVERY_INTERVAL; inconsistency auto-retried, DESCRIBE_STREAM_INCONSISTENCY_RESOLUTION_RETRY_COUNT).
## Amazon DynamoDB Sink: AWS v2 SDK, BatchWriteItem. Java `[code: Properties sinkProperties ... (1237 chars)]` / Scala `[code: val sinkProperties ... (1162 chars)]`.
### Configurations: DynamoDBSink.<InputType>builder() — setDynamoDbProperties (required), setTableName (required), setElementConverter (required), setOverwriteByPartitionKeys (dedup within batch, default []), setFailOnError (default false), setMaxBatchSize (default 25), setMaxInFlightRequests (default 50), setMaxBufferedRequests (default 10_000), setMaxBatchSizeInBytes (N/A FLINK-29854), setMaxTimeInBufferMS (default 5000), setMaxRecordSizeInBytes (N/A FLINK-29854), build.
### Element Converter: custom or DefaultDynamoDbElementConverter (extracts schema from composite type Pojo/Tuple/Row) or DynamoDbTypeInformedElementConverter(TypeInformation.of(...)) or DynamoDbBeanElementConverter (@DynamoDbBean).
### Using Custom Endpoints: override AWS_ENDPOINT + AWS_REGION (for VPC endpoint / Localstack testing) `[code: Properties producerConfig ... (353 chars)]` / Scala `[code: ... (341 chars)]`.

## Source: .../datastream/elasticsearch/ — Elasticsearch Connector
Sinks requesting document actions to ES index. Deps per ES version `[table: 4 rows]`; PyFlink deps `[table: 3 rows]`. Not in binary dist.
## Installing Elasticsearch: cluster setup.
## Elasticsearch Sink: ES6/7/8 Java+Scala+Python examples (static + dynamic index). ES8 uses co.elastic.clients bulk IndexOperation. Internally BulkProcessor (buffers, sends bulk one-at-a-time; no concurrent flushes). Java ES6 `[code: import ...MapFunction ... (1053 chars)]`, ES7 `[code: ... (1028 chars)]`, ES8 `[code: import co.elastic.clients...IndexOperation ... (872 chars)]`. Python ES6 static `[code: from pyflink...Elasticsearch6 ... (584 chars)]`, dynamic `[code: ... (448 chars)]`, ES7 static/dynamic.
### Fault Tolerance: checkpointing → at-least-once (waits for pending BulkProcessor actions at checkpoint). ES8 uses Async Sink (FLIP-171). Enable checkpointing `[code: final StreamExecutionEnvironment env ... (154 chars)]`. Default delivery AT_LEAST_ONCE but checkpointing NOT default → buffers until finish/auto-flush (default 1000 actions). Exactly-once achievable via UpdateRequests + deterministic ids + upsert.
### Handling Failing Requests: backoff-policy retry (not ES8) — Java/Scala/Python examples ES6/7 `[code: DataStream<String> input ... (488 chars)]`. Re-adds on resource constraints; fails on malformed. No strategy/NONE → fails on any error. NOTE: re-adding → longer checkpoints.
### Configuring Internal Bulk Processor (ES6/7 builder): setBulkFlushMaxActions, setBulkFlushMaxSizeMb, setBulkFlushInterval, setBulkFlushBackoffStrategy(CONSTANT/EXPONENTIAL, maxRetries, delayMillis).
### Configuring Internal Writer (ES8 AsyncSinkWriter via Elasticsearch8AsyncSinkBuilder): setMaxBatchSize, setMaxInFlightRequests, setMaxBufferedRequests, setMaxBatchSizeInBytes, setMaxTimeInBufferMS, setMaxRecordSizeInBytes.
## Packaging: uber-jar recommended, or put connector jar in Flink lib/ for system-wide.

[Stored as part of ongoing Apache Flink 2.3 EN docs ingestion — related [[org/knowledge/apache-flink-2-3-docs-connectors-hive-catalog-hive-read-write-hive-functions-dow]] (#34). Next: connectors 221-235, then deployment/ops/internals.]
---
title: Apache Flink 2.3 docs — DataStream connectors (filesystem/rabbitmq/pubsub/hybridsource/pulsar)
tags: [org, flink, flink-2.3, docs, connectors, datastream, filesystem, rabbitmq, pubsub, hybridsource, pulsar, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:48:26.015Z'
updated: '2026-07-08T04:48:26.015Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Apache Flink 2.3 DataStream connector docs for FileSystem (unified Source/Sink, BATCH+STREAMING, exactly-once), RabbitMQ (source 3 guarantee levels, sink), Google Cloud PubSub (source/sink, at-least-once), HybridSource (sequential heterogeneous sources, FLIP-150), and Apache Pulsar (source/sink, exactly-once via transactions, schema evolution, end-to-end encryption). Captured verbatim-condensed from nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/* English pages. **WHY:** org reference for Flink 2.3 streaming connector configuration, bucketing, part-file lifecycle, delivery semantics, and Pulsar/PubSub/RabbitMQ fault-tolerance. Note many connectors note "There is no connector (yet) available for Flink version 2.3".

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/filesystem/
# FileSystem
Unified Source and Sink for BATCH and STREAMING reading/writing (partitioned) files to Flink FileSystem abstraction (POSIX, S3, HDFS). Exactly-once for STREAMING. Combine with format (Avro, CSV, Parquet).

## File Source
Based on Source API — SplitEnumerator (discovers/identifies files, assigns to SourceReader) + SourceReader (requests and reads files).
- Bounded: lists all files (recursive dir list, hidden filtered), reads all.
- Unbounded: periodic file discovery; repeats enumeration at interval; filters previously detected files, sends only new ones.
### Usage
FileSource.FileSourceBuilder; bounded/batch by default; monitorContinuously(Duration) for streaming. Java/Python examples.
### Format Types
- StreamFormat: reads from file stream; simplest; out-of-the-box checkpointing but limited optimizations.
- BulkFormat: reads batches; lowest level; greatest flexibility.
#### TextLine Format
StreamFormat; Java InputStreamReader + charset decoders; NO optimized recovery — re-reads and discards lines processed before last checkpoint (offsets untrackable through charset decoders).
#### SimpleStreamFormat
For non-splittable formats. CsvReaderFormat.forPojo(SomePojo) — schema auto-derived via Jackson (may need @JsonPropertyOrder). Lower-level forSchema static factory for fine-grained control.
#### Bulk Format
Reads/decodes batches (ORC, Parquet). BulkFormat.Reader created in createReader; restoreReader for checkpointed streaming. StreamFormatAdapter wraps SimpleStreamFormat into BulkFormat.
### Customizing File Enumeration (Java example ~1318 chars)
### Current Limitations
Watermarking poor for large backlogs (watermarks advance within file; next file may have later data). Unbounded enumerator remembers all processed file paths (large state) — compressed tracking planned.

## File Sink
Writes into buckets; unbounded data → part files of finite size. Default time-based bucketing (new bucket every hour). Each bucket ≥1 part file per subtask that received data; additional per rolling policy. Row-encoded: rolls on size/timeout/inactivity. Bulk-encoded: rolls on every checkpoint + optional size/time.
IMPORTANT: Checkpointing MUST be enabled in STREAMING mode — part files finalized only on successful checkpoints; without it, files stay in-progress/pending forever.
### Format Types
- Row-encoded: FileSink.forRowFormat(basePath, rowEncoder)
- Bulk-encoded: FileSink.forBulkFormat(basePath, bulkWriterFactory)
#### Row-encoded Formats
Encoder serializes rows to OutputStream. RowFormatBuilder: Custom RollingPolicy, bucketCheckInterval (default 1 min). Example rolls on: ≥15 min data, 5 min inactivity, 1 GB size.
#### Bulk-encoded Formats
Specify BulkWriter.Factory. 5 built-in: ParquetWriterFactory, AvroWriterFactory, SequenceFileWriterFactory, CompressWriterFactory, OrcBulkWriterFactory. Bulk Formats rolling policy MUST extend CheckpointRollingPolicy (rolls every checkpoint + optional size/processing time).
##### Parquet format
AvroParquetWriters convenience methods; custom via ParquetBuilder. Dependency required. PyFlink deps table 2 rows. Avro/Protobuf→Parquet examples. PyFlink ParquetBulkWriters for Rows.
##### Avro format
AvroWriters convenience methods. Dependency required. PyFlink deps. Custom via AvroWriterFactory + AvroBuilder (e.g. compression).
##### ORC Format
OrcBulkWriterFactory + Vectorizer (override vectorize(T, VectorizedRowBatch); transform to ColumnVectors). Uses ORC VectorizedRowBatch. Dependency required. Hadoop Configuration/Properties supported. addUserMetadata(...) for ORC user metadata. PyFlink OrcBulkWriters.
##### Hadoop SequenceFile format
Dependency required. SequenceFileWriterFactory supports compression constructor params.
### Bucket Assignment
DateTimeBucketAssigner (default, hourly, system default tz, format yyyy-MM-dd--HH; configurable). Custom via .withBucketAssigner. Built-in: DateTimeBucketAssigner, BasePathBucketAssigner (single global bucket). PyFlink only supports these two.
### Rolling Policy
Defines when in-progress → pending → finished. Finished = safe to read, valid data, won't revert. STREAMING: rolling policy + checkpoint interval control availability/size/number. BATCH: visible at end of job. Built-in: DefaultRollingPolicy, OnCheckpointRollingPolicy. PyFlink only supports these two.
### Part file lifecycle
3 states: In-progress (being written), Pending (closed, waiting commit), Finished (on checkpoint STREAMING / end of input BATCH). Only finished safe to read. Each subtask: 1 in-progress per active bucket; multiple pending/finished. Example with 2 subtasks (code). Part file naming: In-progress/Pending = part-<uid>-<idx>.inprogress.uid; Finished = part-<uid>-<idx>. uid random, not fault-tolerant (regenerated on recovery). OutputFileConfig for prefix/suffix.
### Compaction (since 1.15)
Smaller checkpoint interval without many small files (esp. bulk formats rolling on checkpoint). Enable via builder. Happens between pending and commit: pending → temp files (path starts with .) → compacted → new pending → committed → source files removed. Requires FileCompactStrategy (when/which: target file size + checkpoint count) + FileCompactor (how). Two types:
- OutputStreamBasedFileCompactor (e.g. ConcatFileCompactor — concat directly).
- RecordWiseFileCompactor (read records one-by-one, write via CompactingFileWriter).
Note 1: must explicitly disableCompact to disable once enabled. Note 2: written files wait longer before visible. PyFlink only supports ConcatFileCompactor + IdenticalFileCompactor.
### Important Considerations
#### General
1. Hadoop <2.7 → use OnCheckpointRollingPolicy (truncate() unsupported pre-2.7; FileSink may truncate on recovery).
2. Sinks/UDFs don't differentiate normal vs failure termination → last in-progress files NOT finished on normal termination.
3. Flink/FileSink never overwrites committed data → restoring from old checkpoint/savepoint with committed in-progress file → exception (can't locate).
4. FileSink only supports HDFS, S3, OSS, ABFS, Local (exception at runtime otherwise).
#### BATCH-specific
1. Writer = user parallelism; Committer = parallelism 1.
2. Pending → Finished after whole input processed.
3. With HA, JobManager failure during commit → duplicates (FLIP-147 fixing).
#### S3-specific
1. Only Hadoop-based FileSystem (not Presto); use s3a:// for sink, s3p:// for checkpointing; s3:// ambiguous.
2. Uses S3 Multi-part Upload (MPU) for exactly-once + efficiency; aggressive bucket lifecycle abort rule → MPU timeout before savepoint restart → restore fails.
#### OSS-specific
Uses MPU (similar to S3).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/rabbitmq/
# RabbitMQ Connector
## License
Maven dep on "RabbitMQ AMQP Java Client" triple-licensed MPL 1.1 / GPL v2 / ASL 2.0. Flink neither reuses source nor packages binaries. Derivative work redistribution subject to those licenses.
## Connector
Access to RabbitMQ data streams. No connector for 2.3. PyFlink deps table 2 rows. Streaming connectors not in binary distribution.
### Installing RabbitMQ
Per RabbitMQ download page; server auto-starts after install.
### RabbitMQ Source
RMQSource — 3 guarantee levels:
- Exactly-once: requires (a) checkpointing enabled (messages acked only on checkpoint completion), (b) correlation ids (set in message properties; source deduplicates reprocessed messages on restore), (c) non-parallel source (parallelism 1, due to RabbitMQ dispatching from single queue to multiple consumers).
- At-least-once: checkpointing enabled but no correlation ids or parallel source.
- No guarantee: no checkpointing; auto-ack on receive/process.
Java/Scala/Python examples.
#### QoS / Consumer Prefetch
basicQos via RMQConnectionConfig; one connection/channel per parallel source → prefetch × parallelism = total unacked. Override RMQSource#setupChannel for complex config. Prefetch unset by default (unlimited); set in production. High volume + checkpointing needs tuning. Java/Scala/Python examples.
### RabbitMQ Sink
RMQSink class. Java/Scala/Python examples.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pubsub/
# Google Cloud PubSub
Source + Sink for Google Cloud PubSub. No connector for 2.3. Recently added, not widespread testing. Not in binary distribution.
## Consuming or Producing PubSubMessages
Google PubSub at-least-once → connector same.
### PubSub SourceFunction
PubSubSource.newBuilder(...). Bare minimum: Google project, Pubsub subscription, deserializer. Java example. Source pulls messages (push endpoints not supported).
### PubSub Sink
PubSubSink.newBuilder(...). Similar to source. Java example.
### Google Credentials
Default: GOOGLE_APPLICATION_CREDENTIALS env var → credentials file. Manual: .withCredentials(...).
### Integration testing
Use docker container (PubSub emulator) instead of real PubSub. Example reading from emulator + sending back (~1400 chars).
### At least once guarantee
#### SourceFunction
Messages may be sent multiple times (PubSub failure, or acknowledgement deadline passed). PubSubSource acks only on successful checkpoint → if checkpoint interval > ack deadline, messages processed multiple times. Recommend checkpoint interval << ack deadline. Metric PubSubMessagesProcessedNotAcked = messages waiting for next checkpoint.
#### SinkFunction
Buffers messages briefly; flushes before each checkpoint; checkpoint succeeds only if delivered to PubSub.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/hybridsource/
# Hybrid Source
Source containing a list of concrete sources; sequentially reads from heterogeneous sources into single input stream. E.g. bootstrap: bounded S3 files then unbounded Kafka — switches FileSource→KafkaSource when bounded input finishes without interrupting app. Prior: multiple sources + user-land switching (complex, inefficient). With HybridSource: single source in job graph + DataStream API. FLIP-150. Dependency: flink-connector-base (transitive with concrete sources).
## Start position for next source
All sources except last must be bounded (need start+end position). Last may be bounded (HybridSource bounded) or unbounded.
#### Fixed start position at graph construction time
E.g. read till pre-determined switch time from files, then Kafka. Each source covers known range, created upfront. Java/Python examples.
#### Dynamic start position at switch time
E.g. file source reads large backlog (longer than next source retention); switch at "current time - X"; start time set at switch time. Requires transfer of end position from previous file enumerator via SourceFactory (deferred KafkaSource construction). Enumerators must support getting end timestamp (may require source customization). FileSource dynamic end position tracked in FLINK-23633. Java example; Python not supported.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pulsar/
# Apache Pulsar Connector
Read/write Pulsar topics with exactly-once guarantees. Pulsar 2.10.0+; use latest. Compatibility in PIP-72. No connector for 2.3. PyFlink deps. Not in binary distribution.
## Pulsar Source (new data source API)
### Usage
PulsarSource.builder() — consumes from earliest cursor of "persistent://public/default/my-topic" in Exclusive subscription (my-subscription), deserializes raw payload as strings. Java/Python examples.
Required: setServiceUrl, setAdminUrl, setSubscriptionName, topics/partitions, deserializer. Recommended setConsumerName (unique in Pulsar stats dashboard).
### Topic-partition Subscription
- Topic list: setTopics("t1","t2").
- Topic pattern: setTopicPattern("topic-.*").
#### Flexible Topic Naming
Since Pulsar 2.0, internal form {persistent|non-persistent}://tenant/namespace/topic; short names allowed for partitioned topics (default type/tenant/namespace). Mapping tables. Non-persistent topics need full name (short names don't apply).
#### Subscribing Pulsar Topic Partition
Partitioned topic = set of non-partitioned topics per partition. Can consume partitions directly via non-partitioned names (e.g. setTopics("sample/flink/simple-string-partition-1",...)).
#### Setting Topic Patterns
Regex subscribes under one tenant+namespace; topic type not determined by regex — use RegexSubscriptionMode (NonPersistentOnly, PersistentOnly). Only topic name part can be regex. Pulsar 2.11.0 bug: doesn't return non-persistent topics correctly (can't regex-filter non-persistent).
### Deserializer
PulsarDeserializationSchema decodes Message<byte[]>. setDeserializationSchema. Predefined PulsarDeserializationSchema — 3 methods:
- Pulsar's Schema (KeyValue/Struct need type info via extra APIs).
- Flink's DeserializationSchema.
- Flink's TypeInformation.
Message<byte[]> has extra properties (key, publish time, message time, app key/value pairs). Implement PulsarDeserializationSchema for property-based deserialization; ensure getProducedType() TypeInformation correct.
#### Schema Evolution in Source
enableSchemaEvolution() with Pulsar's Schema → broker schema validation. Without → schema check bypassed (errors with wrong schema).
#### Use Auto Consume Schema
Schema.AUTO_CONSUME() for multi-schema topics → Pulsar auto-decodes to GenericRecord. setDeserializationSchema(Schema) doesn't support AUTO_CONSUME → use GenericRecordDeserializer. Auto consume supports AVRO, JSON, Protobuf only.
### Define a RangeGenerator
For consuming subset of keys; RangeGenerator generates key hash ranges. FixedKeysRangeGenerator (no hash calc); key hash not 1:1 so filter after source.
### Starting Position
setStartCursor(StartCursor). Built-in: earliest(), latest(), fromMessageId(MessageId) [included; latest if not exist], fromMessageId(MessageId, boolean) [include/exclude], fromMessageTime(long) [deprecated, use fromPublishTime], fromPublishTime(long). Priority: checkpoint > existed subscription position > StartCursor. Force start with pulsar.source.resetSubscriptionCursor (start without saved checkpoint). Checkpoint always highest priority. MessageId = ledger+entry+partition; create via DefaultImplementation.newMessageId.
### Boundedness
Default unbounded. setUnboundedStopCursor / setBoundedStopCursor. Built-in StopCursor: never(), latest(), atMessageId, afterMessageId (include), atEventTime (exclude), afterEventTime (include), atPublishTime (exclude), afterPublishTime (include).
### Source Configurable Options
setConfig(ConfigOption, T) / setConfig(Configuration) / setConfig(Properties) for PulsarClient, PulsarAdmin, Consumer, PulsarSource.
- PulsarClient Options (table 47 rows) — extracts ClientConfigurationData in PulsarOptions.
- PulsarAdmin Options (table 9 rows) — topic metadata query + topic discovery; shares with client API.
- Pulsar Consumer Options (table 26 rows) — Consumer API (not Reader API); ConsumerConfigurationData in PulsarSourceOptions.
- PulsarSource Options (table 12 rows) — performance + ack behavior; ignore if no perf issues.
### Dynamic Partition Discovery
Periodic new partition discovery (topic scaling-out/creation without restart). PULSAR_PARTITION_DISCOVERY_INTERVAL_MS. Enabled by default (5 min); negative disables; disabled for bounded data.
### Event Time and Watermarks
Default: Pulsar message timestamp as event time. Custom WatermarkStrategy to extract event time.
### Message Acknowledgement
Subscription created → Pulsar retains all messages (even disconnected); discarded only on connector ack. Default Exclusive subscription (cumulative acknowledgment — ack latest consumed message, all before marked consumed). Source acks current message on checkpoint completion (consistency with broker committed position). No checkpointing → periodic ack (PULSAR_AUTO_COMMIT_CURSOR_INTERVAL). Source does NOT rely on committed positions for fault tolerance — ack only for progress exposure/monitoring.

## Pulsar Sink (new data sink API, FLIP-191)
Writes to one or more topics or specified partitions. Legacy SinkFunction / Flink ≤1.14 → StreamNative pulsar-flink.
### Usage
PulsarSink.builder(); String record to topic at-least-once. Java/Python examples.
Required: setServiceUrl, setAdminUrl, topics/partitions, serializer. Recommended setProducerName.
### Producing to topics
Mix-in: list of topics, partitions, or both. Auto partition discovery (PULSAR_TOPIC_METADATA_REFRESH_INTERVAL). Custom TopicRouter for routing. Topic+partition both given → merges, uses only topic.
#### Dynamic Topics by incoming messages
Custom TopicRouter; PulsarSinkContext.topicMetadata(String) (cached, expires in refresh interval). Non-existent topic → connector tries to create (enable allowAutoTopicCreation=true in broker.conf). allowAutoTopicCreationType: non-partitioned (default) or partitioned (defaultNumPartitions).
### Serializer
PulsarSerializationSchema (Flink's SerializationSchema or Pulsar's Schema; Schema.AUTO_PRODUCE_BYTES() not supported). Predefined — 2 methods: Pulsar's Schema, Flink's SerializationSchema.
#### Schema Evolution in Sink
enableSchemaEvolution() with Pulsar's Schema → broker validation. Without → target topic has Schema.BYTES (not stored; auto-created topic presents no schema; consumers handle deserialization themselves).
#### PulsarMessage<byte[]> validation
Schema.BYTES bypasses validation. pulsar.sink.validateSinkMessageBytes → uses Schema.AUTO_PRODUCE_BYTES() for extra check (queries latest schema, validates bytes). Some schemas don't support validation → disabled by default.
#### Custom serializer
Implement PulsarSerializationSchema → returns PulsarMessage (builder, not direct constructor). 3 types: with Pulsar Schema (checks compatibility), without schema (byte[] only, no validation), tombstone (empty payload).
### Message Routing
Partition-level. Collects all partitions from topics, routes within all. 2 built-in:
- KeyHashTopicRouter: hashcode of message key (PulsarSerializationSchema.key); no key → random partition. Hash: MessageKeyHash.JAVA_HASH or MURMUR3_32_HASH (PULSAR_MESSAGE_KEY_HASH).
- RoundRobinRouter: round-robin; batch size PULSAR_BATCHING_MAX_MESSAGES.
Custom via TopicRouter (serializable; can return partitions not in pre-discovered list → no setTopics needed).
### Delivery Guarantee
3 semantics:
- NONE: fire-and-forget; data loss possible; highest throughput.
- AT_LEAST_ONCE: no data loss; duplicates after restart.
- EXACTLY_ONCE: no data loss; each record sent once; uses Pulsar transaction + 2PC. Requires Flink checkpoint + Pulsar transaction enabled. Messages in pending transaction, committed after checkpoint. Pending transaction messages not visible until committed.
### Delayed message delivery
Delay message consumption; sink sends immediately, delivered after delay. Only Shared subscription (Exclusive/Failover dispatch immediately). MessageDelayer (default never; fixed(Duration); custom). Dispatch time via PulsarSinkContext.processTime().
### Sink Configurable Options
PulsarClient, PulsarAdmin, Producer, PulsarSink. Producer Options (table 14 rows) — ProducerConfigurationData in PulsarSinkOptions. PulsarSink Options (table 9 rows) — performance + message sending.
### Brief Design Rationale
#### Stateless SinkWriter
EXACTLY_ONCE: no transaction info in checkpoint → new transactions after restart; previous pending aborted/timed out (never visible to downstream).
#### Pulsar Schema Evolution
Reuse same Flink job after "allowed" data model changes (add/delete field in AVRO Pojo); specify validation rules + auto schema update.

## Monitor the Metrics
Pulsar client stats refresh every 60s default; minimum 1s (PULSAR_STATS_INTERVAL_SECONDS).
### Source Metrics (FLIP-33)
Enable pulsar.source.enableMetrics. Table 16 rows.
### Sink Metrics (FLIP-33)
First 5 metrics exposed by default; enable pulsar.sink.enableMetrics for rest. Table 24 rows. numBytesOut/numRecordsOut/numRecordsOutErrors from Pulsar client metrics. Per-second rates calculated (fixed 60s window). currentSendTime = sendAync→broker ack (not available in NONE).

## End-to-end encryption
Pulsar encryption: encrypt on sink, decrypt on source. Public/private key pair required.
### How to enable
1. Generate key pairs (ECDSA or RSA; multiple allowed, random selection).
2. Implement CryptoKeyReader (getPublicKey/getPrivateKey by key name). DefaultCryptoKeyReader.builder(); key files on Flink environment.
3. (Optional) Implement MessageCrypto<MessageMetadata, MessageMetadata> (ECDSA/RSA out of box; custom for other methods; see MessageCryptoBc).
4. Create PulsarCrypto instance (builder).
### Decrypt on source
Pass PulsarCrypto to PulsarSource.builder(); choose ConsumerCryptoFailureAction: FAIL (crash), DISCARD (silently drop), CONSUME (pass undecrypted, decrypt in PulsarDeserializationSchema via Message.getEncryptionCtx()).
### Encrypt on sink
Pass PulsarCrypto to PulsarSink.builder(); choose ProducerCryptoFailureAction: FAIL (crash), SEND (send unencrypted).

## Upgrading
Generic steps in upgrading guide. Pulsar connector stores no state on Flink side (all on Pulsar). Limitations: don't upgrade connector + broker simultaneously; always use newer Pulsar client.
## Troubleshooting
Flink wraps PulsarClient/PulsarAdmin; problems may be independent of Flink (upgrade/reconfigure brokers or connector).
## Known Issues
### Unstable on Java 11
Recommend Java 8.
### No TransactionCoordinatorNotFound, but automatic reconnect
Pulsar transactions active development, not stable. Pulsar 2.9.2 break change in transactions; older client → TransactionCoordinatorNotFound. Use latest pulsar-client-all.
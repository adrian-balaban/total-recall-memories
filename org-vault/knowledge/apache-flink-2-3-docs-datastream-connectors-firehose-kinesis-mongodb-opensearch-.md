---
title: Apache Flink 2.3 docs — DataStream connectors (firehose/kinesis/mongodb/opensearch/prometheus/sqs)
tags: [org, flink, flink-2.3, docs, connectors, datastream, firehose, kinesis, mongodb, opensearch, prometheus, sqs, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:47:22.805Z'
updated: '2026-07-08T04:47:22.805Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Apache Flink 2.3 DataStream connector docs for Amazon Kinesis Data Firehose (sink), Amazon Kinesis Data Streams (source/sink/producer), MongoDB (source/sink), Opensearch (sink), Prometheus (sink via Remote Write), and Amazon SQS (sink). Captured verbatim-condensed from nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/* English pages. **WHY:** org reference for Flink 2.3 streaming connector configuration, fault-tolerance semantics, and AWS client setup — note many connectors note "There is no connector (yet) available for Flink version 2.3" (jars not built for 2.3 at doc-build time).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/firehose/
# Amazon Kinesis Data Firehose Sink

The Firehose sink writes to Amazon Kinesis Data Firehose. Follow the Amazon Kinesis Data Firehose Developer Guide to setup a delivery stream. Maven dependency required. There is no connector (yet) available for Flink version 2.3. PyFlink dependencies table: 2 rows.

The KinesisFirehoseSink uses AWS v2 SDK for Java to write data from a Flink stream into a Firehose delivery stream. Java/Scala/Python builder examples (code blocks condensed).

## Configurations
Flink's Firehose sink is created by static builder KinesisFirehoseSink.<InputType>builder().
- setFirehoseClientProperties(Properties) — Required. Credentials, region, other params.
- setSerializationSchema(SerializationSchema) — Required. Serialize elements before sending.
- setDeliveryStreamName(String) — Required. Name of delivery stream.
- setFailOnError(boolean) — Optional. Default: false. Failed requests treated as fatal exceptions.
- setMaxBatchSize(int) — Optional. Default: 500.
- setMaxInFlightRequests(int) — Optional. Default: 50. Backpressure threshold.
- setMaxBufferedRequests(int) — Optional. Default: 10_000.
- setMaxBatchSizeInBytes(int) — Optional. Default: 4 * 1024 * 1024.
- setMaxTimeInBufferMS(int) — Optional. Default: 5000.
- setMaxRecordSizeInBytes(int) — Optional. Default: 1000 * 1024. Larger records auto-rejected.
- build()

## Using Custom Firehose Endpoints
Override AWS endpoint via AWSConfigConstants.AWS_ENDPOINT and AWSConfigConstants.AWS_REGION (region signs endpoint URL). Useful for VPC endpoints or non-AWS endpoints like Localstack. Java/Scala/Python examples.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kinesis/
# Amazon Kinesis Data Streams Connector

Reads from and writes to Amazon Kinesis Data Streams. No connector (yet) for Flink 2.3.

## Kinesis Streams Source
KinesisStreamsSource is an exactly-once, parallel streaming source based on FLIP-27 source interface. Subscribes to a single Kinesis Data stream, reads events, maintaining order within a specific Kinesis partitionId. Discovers shards, reads each eligible shard in parallel per operator parallelism. Transparently handles discovery of new shards during resharding.

Note: ensure Kinesis Data Stream is ACTIVE before consuming.

Fluent builder (Java/Scala examples). Key points:
- Stream specified via Kinesis Stream ARN.
- Config via Flink Configuration — keys from AWSConfigOptions (AWS-specific) and KinesisSourceConfigOptions (Kinesis Source).
- Starting position TRIM_HORIZON example.
- Deserialization SimpleStringSchema.
- Shard distribution via UniformShardAssigner.
- Increasing WatermarkStrategy using approximateArrivalTimestamp; subtasks idle if no record after 1 second.

### Configuring Access to Kinesis with IAM
IAM policy required. Default AUTO Credentials Provider; BASIC if access key ID + secret key set. AWSConfigConstants.AWS_CREDENTIALS_PROVIDER optional. Providers:
- AUTO (default): ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE, EC2/ECS chain.
- BASIC: access key ID + secret key in config.
- ENV_VAR: AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY env vars.
- SYS_PROP: aws.accessKeyId / aws.secretKey Java system properties.
- CUSTOM: custom user class.
- PROFILE: AWS credentials profile file.
- ASSUME_ROLE: assume a role (credentials for assuming must be supplied).
- WEB_IDENTITY_TOKEN: assume role using Web Identity Token.

### Configuring Starting Position
KinesisSourceConfigOptions.STREAM_INITIAL_POSITION:
- LATEST (default): latest record.
- TRIM_HORIZON: earliest possible (may be trimmed by retention).
- AT_TIMESTAMP: specified timestamp via STREAM_INITIAL_TIMESTAMP; date pattern either non-negative double (seconds since epoch) or SimpleDateFormat pattern (STREAM_TIMESTAMP_DATE_FORMAT; default yyyy-MM-dd'T'HH:mm:ss.SSSXXX).
Configured starting position ignored when restarting from checkpoint/savepoint.

### Fault Tolerance for Exactly-Once User-Defined State Update Semantics
With checkpointing, source checkpoints each shard's progress. On failure, restores to latest complete checkpoint and re-consumes from checkpointed progress. Starting positions ignored on restore. If checkpoint/savepoint stale (shards expired past retention), source does not fail — starts reading from earliest possible (effectively TRIM_HORIZON). To respect configured starting position on restore, change the uid of the KinesisStreamsSource operator to restore without state (Flink best practice).

### Shard Assignment Strategy
UniformShardAssigner (default) — uniform distribution of records across parallel subtasks, prevents data skew. Custom via KinesisShardAssigner interface. Uses HashKeyRange of each shard to decide subtask. Handles mixture of Open/Closed shards during rescaling.

### Record ordering
Kinesis maintains write order per partitionId. Source reads in same order within partitionId, even through resharding — checks shard's parents (up to 2) completely read before reading the shard.

### Deserialization Schema
Accepts Flink DeserializationSchema and custom KinesisDeserializationSchema (provides Kinesis-specific metadata per record). Out-of-the-box:
- SimpleStringSchema and JsonSerializationSchema.
- TypeInformationSerializationSchema — based on Flink TypeInformation; performant Flink-specific; useful if data written and read by Flink.
- GlueSchemaRegistryJsonDeserializationSchema — looks up writer's schema in AWS Glue Schema Registry; transforms to JsonDataWithSchema or JAVA POJO (mbknor-jackson-jsonSchema). No connector for 2.3.
- AvroDeserializationSchema — reads Avro with statically provided schema. forSpecific (Avro generated classes) or forGeneric (GenericRecords with manual schema; expects NO embedded schema). Can use AWS Glue Schema Registry via GlueSchemaRegistryAvroDeserializationSchema.forGeneric/forSpecific. Additional dependency required (code: dependency block ~135 chars).

### Parallelism and Number of Shards
Parallelism independent of total shards.
- parallelism < shards: single subtask handles multiple shards.
- parallelism > shards: some subtasks read no shards — set withIdleness on WatermarkStrategy or watermark generation blocks.

### Watermark Handling in the source
Supplies approximateArrivalTimestamp (Kinesis server-side timestamp) as event time. No guarantees about accuracy or order correctness (timestamps may not be ascending).

### Event Time Alignment for Shard Readers
Supports split-specific watermark alignment — pauses reading from shards whose watermark is too far ahead, resumes when others catch up.

### Threading Model
Multiple threads for shard discovery and consumption.
- Shard Discovery: SplitEnumerator on JobManager periodically discovers new shards via ListShard API (default 10s interval), confirms parents completed, assigns to SplitReader on TaskManagers.
- Polling (default) Split Reader: single thread per parallel subtask; open threads scale with parallelism.
- Enhanced Fan-Out Split Reader: same (one thread per subtask) plus additional thread pools for async Kinesis communication; AWS SDK v2.x KinesisAsyncClient uses Netty threads; one KinesisAsyncClient instance per subtask (parallelism 10 → 10 instances); separate client for registering/deregistering stream consumers.

### Using Enhanced Fan-Out
EFO increases max concurrent consumers per stream; each consumer gets dedicated read quota per shard (scales with consumers); additional cost. Required params:
- READER_TYPE: EFO or POLLING (default POLLING).
- EFO_CONSUMER_NAME: unique per stream (not across streams); reusing terminates existing subscriptions.
EFO Stream Consumer Lifecycle (KinesisSourceConfigOptions.EFO_CONSUMER_LIFECYCLE):
- JOB_MANAGED (default): registered on job start, reused if exists, de-registered on graceful stop. Preferred for most apps.
- SELF_MANAGED: source does not register/deregister; must be done externally via AWS CLI/SDK (RegisterStreamConsumer); ARNs provided via consumer config.

### Internally Used Kinesis APIs
Uses AWS v2 SDK; competes with other non-Flink consumers for service limits.
- Retry Strategy: AWSConfigOptions.RETRY_STRATEGY_MAX_ATTEMPTS_OPTION (max retries before job restart), RETRY_STRATEGY_MIN_DELAY_OPTION (base for exponential backoff), RETRY_STRATEGY_MAX_DELAY_OPTION (max delay). Used for all API requests except DescribeStreamConsumer (separate EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY* options, only at startup).
- Shard Discovery: ListShards (SplitEnumerator, default 10s; tune via SHARD_DISCOVERY_INTERVAL — impacts max delay discovering new shards).
- Polling Split Reader: GetShardIterator (once per shard; rate limit per shard), GetRecords (constantly called; retries on data size/transaction limit exceeded).
- Enhanced Fan-Out Split Reader: SubscribeToShard (per shard; subscription ~5 min, re-acquired on recoverable errors; receives SubscribeToShardEvents stream), DescribeStreamConsumer (startup per subtask; retrieves consumerArn for ACTIVE consumer), RegisterStreamConsumer (once per stream unless SELF_MANAGED), DeregisterStreamConsumer (once per stream unless SELF_MANAGED).

## Kinesis Consumer (deprecated)
Old org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer deprecated — use Kinesis Source. No state compatibility between FlinkKinesisConsumer and KinesisStreamsSource.
### Migrating
No state compatibility — starting position lost. Consider AT_TIMESTAMP slightly before FlinkKinesisConsumer stopped (may re-process some records). If using uid best practice, change uid and enable allowNonRestoredState on savepoint restore.

## Kinesis Streams Sink
Uses AWS v2 SDK to write to Kinesis stream. Stream must be ACTIVE. CloudWatch access needed for monitoring. Java/Scala/Python examples. Need serialization schema + partition key generation logic. If failOnError on, runtime exception on failed records; otherwise requeued for retry. Metrics via Flink metrics system. Default max record size 1MB, max batch size 5MB (KDS maximums).
### Kinesis Sinks and Fault Tolerance
At-least-once via checkpointing — completes in-flight requests during checkpoint. On restore, data written since checkpoint written again (duplicates). Uses PutRecords API (does not guarantee order).
### Backpressure
Arises as buffer fills; reduce by increasing internal queue size (Java/Python examples).

## Kinesis Producer (deprecated)
Old FlinkKinesisProducer deprecated — use Kinesis Sink. New sink uses AWS v2 SDK (old used Kinesis Producer Library); new sink does not support aggregation.

## Using Custom Kinesis Endpoints
Override via AWSConfigConstants.AWS_ENDPOINT and AWSConfigConstants.AWS_REGION (region signs URL). Useful for VPC endpoints or non-AWS (Kinesalite). Java/Scala/Python examples.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/mongodb/
# MongoDB Connector

Read/write MongoDB collections with at-least-once guarantees. No connector for 2.3.

## MongoDB Source
Java example (code ~1552 chars).
### Configurations — MongoSource.<OutputType>builder()
- setUri(String) — Required. Connection string.
- setDatabase(String) — Required.
- setCollection(String) — Required.
- setFetchSize(int) — Optional. Default: 2048. Docs per round-trip.
- setNoCursorTimeout(boolean) — Optional. Default: true. MongoDB times out idle cursors after 10 min; server marks sessions expired after 30 min idle (kills cursors including noCursorTimeout ones).
- setPartitionStrategy(PartitionStrategy) — Optional. Default: DEFAULT. Available: SINGLE, SAMPLE, SPLIT_VECTOR, SHARDED, DEFAULT.
- setPartitionSize(MemorySize) — Optional. Default: 64mb.
- setSamplesPerPartition(int) — Optional. Default: 10. Only for SAMPLE. Total samples = samples per partition * (count of documents / documents per partition).
- setLimit(int) — Optional. Default: -1. Limit per reader; parallelism>1 → max = parallelism * limit.
- setProjectedFields(String…) — Optional. Projection fields.
- setDeserializationSchema(MongoDeserializationSchema) — Required. Parse BSON documents.

### Partition Strategies
- SINGLE: entire collection as single partition.
- SAMPLE: samples collection, fast but possibly uneven.
- SPLIT_VECTOR: splitVector command for non-sharded collections, fast and even; requires splitVector permission.
- SHARDED: reads config.chunks as partitions; only for sharded collections, fast and even; requires read permission of config database.
- DEFAULT: sharded strategy for sharded collections, otherwise split vector.

## MongoDB Sink
Java example (code ~775 chars).
### Configurations — MongoSink.<InputType>builder()
- setUri(String) — Required.
- setDatabase(String) — Required.
- setCollection(String) — Required.
- setBatchSize(int) — Optional. Default: 1000. Max actions buffered per batch; -1 disables batching.
- setBatchIntervalMs(long) — Optional. Default: 1000. Flush interval ms; -1 disables.
- setMaxRetries(int) — Optional. Default: 3.
- setDeliveryGuarantee(DeliveryGuarantee) — Optional. Default: AT_LEAST_ONCE. EXACTLY_ONCE not supported yet.
- setSerializationSchema(MongoSerializationSchema) — Required. Parse input record to WriteModel.

### Fault Tolerance
At-least-once via checkpointing — waits for pending writes at checkpoint time. Checkpointing not enabled by default but default delivery guarantee AT_LEAST_ONCE → buffers until finish or MongoWriter auto-flushes (default 1000 ops). Using WriteModel with deterministic ids + upsert → effectively exactly-once with AT_LEAST_ONCE configured.

### Configuring the Internal Mongo Writer
- setBatchSize(int): max ops before flush; -1 disables.
- setBatchIntervalMs(long): flush interval regardless of size; -1 disables.
Writing behaviors:
- Flush when time interval or batch size exceed limit: batchSize > 1 and batchInterval > 0.
- Flush only on checkpoint: batchSize == -1 and batchInterval == -1.
- Flush for every single write: batchSize == 1 or batchInterval == 0.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/opensearch/
# Opensearch Connector

Sinks request document actions to an Opensearch Index. Dependency table: 3 rows. Streaming connectors not part of binary distribution — package with libraries for cluster execution.

## Installing Opensearch
Setup instructions in Opensearch docs.

## Opensearch Sink
Java/Scala examples. OpensearchEmitter can perform different request types (DeleteRequest, UpdateRequest, etc.). Each parallel instance uses BulkProcessor to send action requests; buffers elements, sends in bulk; executes bulk requests one at a time (no concurrent flushes).

### Opensearch Sinks and Fault Tolerance
At-least-once via checkpointing — waits for pending action requests in BulkProcessor at checkpoint. Checkpointing not enabled by default but default AT_LEAST_ONCE → buffers until finish or BulkProcessor auto-flushes (default 1000 actions). Using UpdateRequests with deterministic IDs + upsert → effectively exactly-once with AT_LEAST_ONCE configured.

### Handling Failing Opensearch Requests
Retry via backoff-policy (Java/Scala examples). Re-adds requests failed due to resource constraints (e.g. queue saturation); fails for other errors (malformed documents). No BulkFlushBackoffStrategy (or FlushBackoffType.NONE) → fails for any error. IMPORTANT: re-adding requests leads to longer checkpoints (waits for re-added requests to flush); with EXPONENTIAL, checkpoints wait until node queues have capacity or max retries reached.

### Configuring the Internal Bulk Processor
- setBulkFlushMaxActions(int): max actions before flushing.
- setBulkFlushMaxSizeMb(int): max size (MB) before flushing.
- setBulkFlushInterval(long): flush interval regardless of amount/size.
- setBulkFlushBackoffStrategy(FlushBackoffType, int maxRetries, long delayMillis): CONSTANT or EXPONENTIAL; for constant = delay between retries; for exponential = initial base delay.

## Packaging the Opensearch Connector into an Uber-Jar
Build uber-jar with all dependencies, or put connector jar in Flink's lib/ folder for system-wide availability.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/prometheus/
# Prometheus Sink

Writes to Prometheus-compatible storage via Remote Write 1.0 standard API (endpoint must be enabled). NOT for sending internal Flink metrics to Prometheus — use Metric Reporters for that. No connector for 2.3.

## Usage
PrometheusSink.builder() — only required config is prometheusRemoteWriteUrl. If parallelism > 1, key stream using PrometheusTimeSeriesLabelsAndMetricNameKeySelector so all samples of same time-series in same partition (order not lost).

### Input data objects
Expects PrometheusTimeSeries records (convert via map/flatMap). Immutable, cannot be reused; use builder. Represents a single time-series record sent to Remote Write; may contain multiple samples. "time-series" overloaded (series with unique labels vs record sent to interface). Each record contains:
- One metricName (→ __name__ label value).
- Zero or more Label entries (key+value, both String; duplicate keys not allowed).
- One or more Sample (double value, long timestamp ms from Epoch; duplicate timestamps not allowed).
Labels + metricName = unique identifier of database time-series; composite is the partition key.

### Populating a PrometheusTimeSeries
Builder; .addSample(...) per sample (order retained); max samples per record = maxBatchSizeInSamples. Aggregating samples improves write performance.

## Prometheus remote-write constraints
Strict constraints on format and ordering; violating requests rejected. Connector does NOT enforce constraints — user responsible. Behavior depends on Prometheus implementation/config (constraints may be relaxed).
### Ordering constraints
- Labels within record in lexicographical order by key.
- Samples within record in timestamp order (older→newer).
- All samples of same time-series written in timestamp order.
- No duplicate timestamps within same time-series.
Out-of-order time windows (if supported and enabled) relax sample ordering.
### Format constraints
- metricName defined and non-empty.
- Label names regex [a-zA-Z:_]([a-zA-Z0-9_:]); no @, $, !, . (dot), or punctuation (except colon and hyphen).
- Label names must not begin with __ (reserved).
- No duplicate Label names.
- Label values and metricName may contain any UTF-8.
- Label values cannot be empty.
Builder does not enforce these.

### User responsibilities
Send records respecting format/ordering constraints; connector does no validation/reordering. Sample ordering by timestamp critical; same Labels+metricName → same partition in order. Malformed/out-of-order records rejected and dropped (data loss); may cause other records in same write-request to be dropped.

### Sink parallelism and keyed streams
Each sub-task single thread, writes in received order. Key stream with PrometheusTimeSeriesLabelsAndMetricNameKeySelector to ensure same time-series → same subtask. User responsible for ordering before sink.

## Error handling
Four error types:
- Retryable: 5xx or 429, connectivity issues (temporary).
- Non-retryable: 4xx (except 429, 403, 404) — data violating constraints, malformed, out-of-order.
- Fatal: 403 (auth failure), 404 (incorrect endpoint path).
- Other unexpected I/O failures.
### On-error behaviors
- FAIL: throw unhandled exception, job fails.
- DISCARD_AND_CONTINUE: discard offending request, continue.
On discard: WARN log with cause (+ endpoint response payload), increase counter metrics (rejected samples, write requests), drop entire write request (batch may contain multiple PrometheusTimeSeries), continue. Prometheus Remote Write does not support partial failures — single offending record → entire batch discarded.
#### Retryable errors (e.g. 429 throttling)
Retries with configurable backoff; on max retries exceeded depends on onMaxRetryExceeded:
- FAIL (default): job fails, restarts from checkpoint.
- DISCARD_AND_CONTINUE: drop request, continue.
#### Non-retryable errors (e.g. 400 bad request)
Always DISCARD_AND_CONTINUE (not configurable).
#### Fatal errors (403, 404)
Always FAIL (not configurable).
#### Other I/O errors
Always FAIL (not configurable).

### Error handling configuration
onMaxRetryExceeded (default FAIL); onPrometheusNonRetryableError (only DISCARD_AND_CONTINUE allowed).

### Retry configuration
Exponential backoff:
- initialRetryDelayMS (default 30): doubles each retry up to max.
- maxRetryDelayMS (default 5000): must be > initial.
- maxRetryCount (default 100): Integer.MAX_VALUE for practically forever.
On maxRetryCount exceeded → onMaxRetryExceeded behavior.

## Batching
Multiple PrometheusTimeSeries batched per write request; based on samples per request + max buffering time. Starts 1 record/request, increases up to maxBatchSizeInSamples. Buffered records stored in Flink state (not lost on restart).
- maxBatchSizeInSamples (default 500): max samples per write request.
- maxTimeInBufferMS (default 5000): max buffer time before emitting.
- maxRecordSizeInSamples (default 500): max samples per single record; must be ≤ maxBatchSizeInSamples. Exceeding → exception, job fails + restarts (endless loop risk).
Larger batches improve throughput but increase records lost on DISCARD_AND_CONTINUE; default 500 maximizes ingestion throughput.

## Request Signer
Remote Write spec has no auth scheme (delegated to backend). Connector allows PrometheusRequestSigner to add headers (based on request body or existing headers). Implement PrometheusRequestSigner interface (Serializable).
### Amazon Managed Prometheus (AMP) request signer
Provided; additional dependency required (no connector for 2.3). Uses DefaultCredentialsProvider, signs every request.

## HTTP client configuration
- socketTimeoutMs (default 5000).
- httpUserAgent (default Flink-Prometheus).

## Connector metrics
Custom metrics counting written + dropped data (table: 8 rows). numByteSend should be ignored (does not measure bytes due to AsyncSink limitations) — use numSamplesOut and numWriteRequestsOut. Metric group name "Prometheus" by default (changeable).

## Connector guarantees
At-most-once. Data loss possible (malformed/out-of-order data, DISCARD_AND_CONTINUE on max retries or non-retryable errors). Due to Prometheus Remote Write not allowing out-of-order writes in same time-series — discard-and-continue prevents endless fail/restart loop (also needed for checkpoint recovery). Sink guarantees order retained per partition; key-by with PrometheusTimeSeriesLabelsAndMetricNameKeySelector prevents accidental reordering; user responsible for partitioning before sink.

## Example application
Full app in connector tests: org.apache.flink.connector.prometheus.sink.examples.DataStreamExample (generates random data, writes to Prometheus).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/sqs/
# Amazon SQS Sink

Writes to Amazon SQS using AWS v2 SDK. No connector for 2.3. Java/Scala examples (code ~1816/1541 chars).

## Configurations — SqsSink.<String>builder()
- setSqsClientProperties(Properties) — Required. Credentials, region, other params.
- setSerializationSchema(SerializationSchema) — Required. Serialize elements before sending.
- setSqsUrl(String) — Required. SQS URL.
- setFailOnError(boolean) — Optional. Default: false. Failed requests cause Flink Job restart.
- setMaxBatchSize(int) — Optional. Default: 10.
- setMaxInFlightRequests(int) — Optional. Default: 50. Backpressure threshold.
- setMaxBufferedRequests(int) — Optional. Default: 5_000.
- setMaxBatchSizeInBytes(int) — Optional. Default: 256 * 1024.
- setMaxTimeInBufferMS(int) — Optional. Default: 5000.
- setMaxRecordSizeInBytes(int) — Optional. Default: 256 * 1024. Larger records auto-rejected.
- build()
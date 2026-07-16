---
title: Apache Flink 2.3 docs — Connectors (formats ogg/parquet/orc/raw + kafka/upsert-kafka/dynamic-kafka/dynamodb/firehose/kinesis)
tags: [org, flink, flink-2.3, docs, connectors, kafka, upsert-kafka, dynamodb, firehose, kinesis, parquet, orc, raw, ogg, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:38:48.459Z'
updated: '2026-07-08T04:38:48.459Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Captured Apache Flink 2.3 docs Connectors section batch conn2 (indices 181-190): the remaining table formats (ogg CDC, parquet/orc columnar, raw bytes) and the SQL table connectors (kafka, upsert-kafka, dynamic-kafka, dynamodb, firehose, kinesis). WHY: ongoing ingestion of the full Flink 2.3 EN docs into org memories for verbatim reference; this completes the table-formats family and the core AWS/Kafka SQL connector set. Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`; tables to `[table: N rows]`. NOTE: every connector page states "There is no connector (yet) available for Flink version 2.3" — these connectors ship out-of-binary-distribution and must be linked for cluster execution; dynamic-kafka/dynamodb/firehose/kinesis all repeat this note.

## Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/ogg/
# Ogg Format — Changelog-Data-Capture Format (Serialization/Deserialization Schema)
Oracle GoldenGate (ogg) is a managed service providing a real-time data mesh platform using replication for HA and real-time analysis. Ogg provides a format schema for changelog and serializes messages using JSON. Flink interprets Ogg JSON as INSERT/UPDATE/DELETE messages into Flink SQL — useful for synchronizing incremental DB data to other systems, auditing logs, real-time materialized views on databases, temporal join changing history of a DB table. Flink can also encode INSERT/UPDATE/DELETE as Ogg JSON to external systems like Kafka, BUT cannot combine UPDATE_BEFORE + UPDATE_AFTER into a single UPDATE — encodes them as DELETE and INSERT Ogg messages.
Dependencies [table: 2 rows]. Note: refer to Ogg Kafka Handler docs for setting up an Ogg Kafka handler to synchronize changelog to Kafka topics.
How to use: unified format for changelog; example update event on Oracle PRODUCTS (id,name,description,weight) where weight of id=111 changes 5.18→5.15, synchronized to Kafka topic products_ogg. DDL consumes topic interpreting change events `[code: CREATE TABLE topic_products ( ... (346 chars)]`. After registering, consume Ogg messages as changelog source `[code: -- a real-time materialized view on the Oracle "PRODUCTS" ... (390 chars)]`.
Available Metadata: format metadata exposed as read-only VIRTUAL columns. Attention: format metadata fields only available if connector forwards format metadata — currently only the Kafka connector exposes metadata fields for its value format. [table: 5 rows]. Example accessing Ogg metadata in Kafka `[code: CREATE TABLE KafkaTable ( ... (591 chars)]`.
Format Options [table: 7 rows].
Data Type Mapping: Ogg uses JSON for serialization/deserialization — refer to JSON Format docs for data type mapping.

## Source: .../formats/parquet/
# Parquet Format (Serialization/Deserialization Schema)
The Apache Parquet format reads and writes Parquet data.
Dependencies [table: 2 rows].
How to create: example using Filesystem connector + Parquet `[code: CREATE TABLE user_behavior ( ... (250 chars)]`.
Format Options [table: 5 rows]. Parquet also supports config from ParquetOutputFormat, e.g. parquet.compression=GZIP for gzip.
Data Type Mapping: type mapping compatible with Apache Hive but by default NOT Apache Spark:
- Timestamp: maps timestamp type to int96 whatever the precision.
- Spark compatibility requires int64 via config option write.int64.timestamp.
- Decimal: maps decimal type to fixed-length byte array according to precision.
[table: 18 rows] (Flink type → Parquet type).

## Source: .../formats/orc/
# Orc Format (Serialization/Deserialization Schema)
The Apache Orc format reads and writes Orc data.
Dependencies [table: 2 rows].
How to create: example using Filesystem connector + Orc `[code: CREATE TABLE user_behavior ( ... (246 chars)]`.
Format Options [table: 2 rows]. Orc also supports table properties, e.g. orc.compress=SNAPPY for snappy compression.
Data Type Mapping: compatible with Apache Hive. [table: 18 rows] (Flink type → Orc type).

## Source: .../formats/raw/
# Raw Format (Serialization/Deserialization Schema)
The Raw format reads and writes raw (byte-based) values as a single column.
Note: encodes null values as null of byte[] type — has limitation in upsert-kafka (treats null values as tombstone/DELETE on key). Recommend avoiding upsert-kafka + raw as value.format if the field can be null.
The Raw connector is built-in — no additional dependencies.
Example: raw log data in Kafka `[code: 47.29.201.179 - - [28/Feb/2019:13:17:10 +0000] "GET /?p=1 HTTP/2.0" 20 ... (214 chars)]`. Table reading Kafka topic as anonymous UTF-8 string value via raw format `[code: CREATE TABLE nginx_log ( ... (209 chars)]`. Read raw data as pure string and split into fields with UDF `[code: SELECT t.hostname, t.datetime, t.url, t.browser, ... ... (104 chars)]`. Can also write single STRING column into Kafka topic as anonymous UTF-8 string value.
Format Options [table: 4 rows].
Data Type Mapping [table: 11 rows] (SQL types with serializer/deserializer classes).

## Source: .../table/kafka/ — Apache Kafka SQL Connector
Scan Source: Unbounded; Sink: Streaming Append Mode. Reads from and writes to Kafka topics.
Dependencies: "There is no connector (yet) available for Flink version 2.3." Not part of binary distribution — link it for cluster execution.
How to create a Kafka table `[code: CREATE TABLE KafkaTable ( ... (355 chars)]`.
Available Metadata: R/W column defines readable (R) and/or writable (W). Read-only columns must be VIRTUAL to exclude during INSERT INTO. [table: 8 rows]. Extended example `[code: CREATE TABLE KafkaTable ( ... (439 chars)]`. Format Metadata: connector exposes value format metadata for reading, keys prefixed with 'value.'. Example accessing Kafka + Debezium metadata `[code: CREATE TABLE KafkaTable ( ... (663 chars)]`.
Connector Options [table: 26 rows].
### Key and Value Formats
Both key and value parts serialized/deserialized from raw bytes using formats.
Value Format: 'format' is synonym for 'value.format'. Example value-only `[code: CREATE TABLE KafkaTable ( ... (238 chars)]`, configured data type `[code: ROW<`user_id` BIGINT, `item_id` BIGINT, `behavior` STRING> ... (58 chars)]`.
Key and Value Format: prefixes 'key'/'value' + format identifier `[code: CREATE TABLE KafkaTable ( ... (392 chars)]`. Key format includes 'key.fields' (semicolon-delimited) in order `[code: ROW<`user_id` BIGINT, `item_id` BIGINT> ... (39 chars)]`. With 'value.fields-include'='ALL', key fields also in value format `[code: ROW<...> ... (58 chars)]`.
Overlapping Format Fields: 'key.fields-prefix' gives key columns unique name while keeping original names for key format `[code: CREATE TABLE KafkaTable ( ... (341 chars)]`. Value format must be EXCEPT_KEY mode `[code: key format: ... (119 chars)]`.
### Topic and Partition Discovery
topic (semicolon-separated list) or topic-pattern (regex). scan.topic-partition-discovery.interval (non-negative) enables dynamic discovery of new topics. Topic list/pattern only work in sources; sinks support a single topic.
### Start Reading Position — scan.startup.mode:
- group-offsets (default): committed offsets in ZK/brokers of consumer group
- earliest-offset; latest-offset
- timestamp: requires scan.startup.timestamp-millis (ms since 1970 GMT)
- specific-offsets: requires scan.startup.specific-offsets e.g. partition:0,offset:42;partition:1,offset:300
### Bounded Ending Position — scan.bounded.mode:
- group-offsets; latest-offset; timestamp (scan.bounded.timestamp-millis); specific-offsets (scan.bounded.specific-offsets)
If unset → unbounded table.
### CDC Changelog Source
Flink natively supports Kafka as CDC changelog source via formats: debezium, canal, maxwell.
### Sink Partitioning — sink.partitioner:
default uses Kafka default partitioner (sticky for null keys, murmur2 hash for keyed); 'fixed' writes same Flink partition → same Kafka partition (reduces network connections); custom partitioner supported.
### Consistency guarantees
at-least-once with checkpointing enabled by default. With checkpointing, exactly-once possible via sink.delivery-guarantee:
- none: no guarantees (lost or duplicated)
- at-least-once (default): no loss (may duplicate)
- exactly-once: Kafka transactions; set isolation.level (read_uncommitted/read_committed — latter default) for consumers
### Source Per-Partition Watermarks
Watermarks generated inside Kafka consumer, merged like streaming shuffles; output watermark = min across partitions. Idle partitions stall watermark — set table.exec.source.idle-timeout.
### Security
security configs with "properties." prefix. PLAIN SASL example `[code: ... (418 chars)]`; SASL_SSL + SCRAM-SHA-256 `[code: ... (972 chars)]`. Note: login module class path may differ if Kafka client deps relocated — SQL client JAR relocates to org.apache.flink.kafka.shaded.org.apache.kafka, so plain login module path becomes org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule.
Data Type Mapping: Kafka stores keys/values as bytes (no schema); mapping determined by formats (csv, json, avro).

## Source: .../table/upsert-kafka/ — Upsert Kafka SQL Connector
Scan Source: Unbounded; Sink: Streaming Upsert Mode. Reads/writes Kafka in upsert fashion.
As source: produces changelog stream — value = UPDATE of last value for same key (or INSERT if no key); null value = DELETE.
As sink: consumes changelog stream — INSERT/UPDATE_AFTER as normal messages, DELETE as null-value (tombstone). Flink guarantees ordering on primary key by partitioning on primary-key values so update/delete on same key → same partition.
Dependencies: "no connector (yet) available for Flink version 2.3"; not in binary distribution.
Full Example `[code: CREATE TABLE pageviews_per_region ( ... (779 chars)]`. Attention: define primary key in DDL.
Available Metadata: see regular Kafka connector.
Connector Options [table: 15 rows].
### Key and Value Formats: requires both key and value format; key fields derived from PRIMARY KEY constraint `[code: CREATE TABLE KafkaTable ( ... (410 chars)]`.
### Primary Key Constraints: always upsert fashion, requires primary key. Materialized changelog unique on PK; PK controls which fields in Kafka key.
### Consistency Guarantees: at-least-once default; upsert mode means last record on key takes effect when read back → idempotent writes like HBase sink. With checkpointing, exactly-once via sink.delivery-guarantee (none/at-least-once/exactly-once — transactions, set isolation.level).
### Source Per-Partition Watermarks: same as Kafka; set table.exec.source.idle-timeout for idle partitions.
Data Type Mapping: determined by formats.

## Source: .../table/dynamic-kafka/ — Dynamic Kafka SQL Connector
Scan Source: Unbounded. Reads Kafka topics that can move across clusters without restarting the job; streams resolved via Kafka metadata service — useful for cluster migrations and dynamic topic/cluster changes.
Dependencies: "no connector (yet) available for Flink version 2.3".
How to create: built-in single-cluster metadata service (stream ids = topics in single cluster) `[code: CREATE TABLE DynamicKafkaTable ( ... (536 chars)]`. Supports custom metadata service via metadata-service option — class implements KafkaMetadataService with public no-arg constructor or constructor accepting Properties; connector passes Kafka properties (all properties.* options) into constructor.
Available Metadata: exposes all Kafka connector metadata + one dynamic-specific column: kafka_cluster (STRING NOT NULL, read-only) — cluster id resolved by metadata service. Example `[code: CREATE TABLE DynamicKafkaTable ( ... (494 chars)]`.
Connector Options [table: 8 rows]. Supports same format options and Kafka client properties as Kafka connector.

## Source: .../table/dynamodb/ — Amazon DynamoDB SQL Connector
Sink: Batch; Sink: Streaming Append & Upsert Mode. Writes data into Amazon DynamoDB.
Dependencies: "no connector (yet) available for Flink version 2.3".
How to create: set up DynamoDB table per AWS docs; minimum options `[code: CREATE TABLE DynamoDbTable ( ... (215 chars)]`.
Connector Options [table: 29 rows].
Authorization: create appropriate IAM policy for writing to table.
### Authentication — aws.credentials.provider:
- AUTO (default): AWS Credentials Provider chain order ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE, EC2/ECS. If access key ID + secret key in deployment config → uses BASIC.
- BASIC: access key ID + secret key in config.
- ENV_VAR: AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY env vars.
- SYS_PROP: Java system props aws.accessKeyId/aws.secretKey.
- PROFILE: AWS credentials profile.
- ASSUME_ROLE: assume a role (credentials for assuming must be supplied).
- WEB_IDENTITY_TOKEN: assume role using Web Identity Token.
- CUSTOM: class implementing AWSCredentialsProvider with constructor MyCustomClass(java.util.Properties config); all connector properties passed via constructor.
### Sink Partitioning: client-side dedup via PARTITIONED BY; only latest record per composite key within a batch `[code: CREATE TABLE DynamoDbTable ( ... (242 chars)]`.
### Notice: write-only — no source query implementation. `SELECT * FROM DynamoDbTable;` errors "Connector dynamodb can only be used as a sink. It cannot be used as a [source]."

## Source: .../table/firehose/ — Amazon Kinesis Data Firehose SQL Connector
Sink: Batch; Sink: Streaming Append Mode. Writes into Amazon Kinesis Data Firehose (KDF).
Dependencies: "no connector (yet) available for Flink version 2.3".
How to create: set up KDF delivery stream per AWS docs; minimum options `[code: CREATE TABLE FirehoseTable ( ... (240 chars)]`.
Connector Options [table: 31 rows].
Authorization: IAM policy for reading/writing to KDF delivery stream.
### Authentication — aws.credentials.provider: same set as DynamoDB (AUTO/BASIC/ENV_VAR/SYS_PROP/PROFILE/ASSUME_ROLE/WEB_IDENTITY_TOKEN/CUSTOM).
Data Type Mapping: KDF stores records as Base64-encoded binary (no internal structure); deserialized/serialized by formats (avro/csv/json) — pick format with format keyword.
### Notice: only KDF-backed sinks, no source. `SELECT * FROM FirehoseTable;` errors "Connector firehose can only be used as a sink. It cannot be used as a [source]."

## Source: .../table/kinesis/ — Amazon Kinesis Data Streams SQL Connector
Scan Source: Unbounded; Sink: Batch; Sink: Streaming Append Mode. Reads/writes Amazon Kinesis Data Streams (KDS).
Dependencies: "no connector (yet) available for Flink version 2.3"; not in binary distribution.
### Versioning
Two Table API/SQL distributions due to ongoing migration from deprecated SourceFunction/SinkFunction to new Source/Sink interfaces. Only one TableFactory per connector identifier — only one kinesis TableFactory allowed in deps. [table: 5 rows]. Only include one artifact (flink-sql-connector-aws-kinesis-streams OR flink-sql-connector-kinesis); including both → clashing TableFactory names. Docs target v5.x onwards; main config targets kinesis identifier; legacy config under Configuration (kinesis-legacy).
### Migrating v4.x → v5.x
No state compatibility between 4.x and 5.x (underlying implementation changed). Start v5.x with source.init.position=AT_TIMESTAMP slightly before v4.x job stop time (may re-process some records).
How to create: set up Kinesis stream per AWS docs `[code: CREATE TABLE KinesisTable ( ... (372 chars)]`.
Available Metadata: known bug — VIRTUAL columns NOT supported in kinesis table Source; use kinesis-legacy until fixed. Metadata (kinesis-legacy only) [table: 4 rows]. Example `[code: CREATE TABLE KinesisTable ( ... (544 chars)]`.
Connector Options [table: 48 rows].
### Features
### Sink Partitioning — sink.partitioner:
- fixed: PartitionKey from Flink subtask index → each Flink partition in at most one Kinesis partition (assuming no re-sharding).
- random: random PartitionKey (default for tables without PARTITION BY).
- Custom FixedKinesisPartitioner subclass e.g. 'org.mycompany.MyPartitioner'.
Tables with PARTITION BY always partition on concatenated projection of PARTITION BY fields — sink.partitioner cannot modify (configuration error); use sink.partitioner-field-delimiter for delimiter (empty string valid).
Data Type Mapping: Kinesis stores records as Base64-encoded binary; serialized/deserialized by formats (avro/csv/json).
Connector Options (kinesis-legacy) [table: 87 rows].

[Stored as part of ongoing Apache Flink 2.3 EN docs ingestion — see related [[org/knowledge/apache-flink-2-3-docs-connectors-table-overview-formats-csv-json-avro-confluent-]] (conn1 #31) and [[org/knowledge/apache-flink-2-3-docs-libraries-cep-state-processor-api]] (#30). Next batches: connectors 191-235, deployment 236-278, ops 279-301, internals 302-308.]
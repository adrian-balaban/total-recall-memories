---
title: 'Apache Flink 2.3 docs — Connectors (table overview + formats: CSV/JSON/Avro/Confluent-Avro/Protobuf/Debezium/Canal/Maxwell)'
tags: [org, flink, flink-2.3, docs, connectors, table-connectors, formats, csv, json, avro, protobuf, debezium, canal, maxwell, cdc, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:35:50.963Z'
updated: '2026-07-08T04:35:50.963Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — Connectors: Table overview + formats (CSV/JSON/Avro/Confluent-Avro/Protobuf/Debezium/Canal/Maxwell)

**Executive summary:** 10 connector/format pages captured near-verbatim (code → `[code: ...]`, tables → `[table: N rows]`). WHY: faithful reference for the Flink 2.3 Table API/SQL connector ecosystem — how to register external system sources/sinks via CREATE TABLE, schema mapping (physical/metadata/computed columns, primary keys NOT ENFORCED, proctime/rowtime + watermark strategies), SPI factory discovery (ServicesResourceTransformer to merge META-INF/services in uber-jars), and the format layer (CSV/JSON/Avro schema derived from table schema; Confluent Avro Schema Registry via Kafka/Upsert-Kafka only; Protobuf proto2/proto3/Editions, protobuf-java 4.32.1; CDC formats Debezium/Canal/Maxwell interpret JSON/Avro as INSERT/UPDATE/DELETE changelog, UPDATE_BEFORE/AFTER encoded as DELETE+INSERT, dedup caveat table.exec.source.cdc-events-duplicate + PRIMARY KEY, Debezium Postgres REPLICA IDENTITY FULL). Sources: nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/overview + .../formats/{overview,csv,json,avro,avro-confluent,protobuf,debezium,canal,maxwell}. English only. Part of [[apache-flink-2-3-docs-*]] series (#31, connectors section indices 171-180, batch conn1). Related: SQL DDL/CDC in #8/#12, deployment connector JARs in #29.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/overview/
# Table & SQL Connectors

Flink's Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables. A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system). A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Avro, Parquet, or ORC.

This page describes how to register table sources and table sinks in Flink using the natively supported connectors. After a source or sink has been registered, it can be accessed by Table API & SQL statements.

If you want to implement your own custom table source or sink, have a look at the user-defined sources & sinks page.

## Supported Connectors

Flink natively support various connectors. The following tables list all available connectors.
[table: 12 rows]

Please refer to the configuration section on how to add connectors as a dependency.

## How to use connectors

Flink supports using SQL CREATE TABLE statements to register tables. One can define the table name, the table schema, and the table options for connecting to an external system.

See the SQL section for more information about creating a table.

The following code shows a full example of how to connect to Kafka for reading and writing JSON records.
  SQL
`[code: CREATE TABLE MyUserTable ( ... (688 chars)]`

The desired connection properties are converted into string-based key-value pairs. Factories will create configured table sources, table sinks, and corresponding formats from the key-value pairs based on factory identifiers (kafka and json in this example). All factories that can be found via Java's Service Provider Interfaces (SPI) are taken into account when searching for exactly one matching factory for each component.

If no factory can be found or multiple factories match for the given properties, an exception will be thrown with additional information about considered factories and supported properties.

## Transform table connector/format resources

Flink uses Java's Service Provider Interfaces (SPI) to load the table connector/format factories by their identifiers. Since the SPI resource file named org.apache.flink.table.factories.Factory for every table connector/format is under the same directory META-INF/services, these resource files will override each other when build the uber-jar of the project which uses more than one table connector/format, which will cause Flink to fail to load table connector/format factories.

In this situation, the recommended way is transforming these resource files under the directory META-INF/services by ServicesResourceTransformer of maven shade plugin. Given the pom.xml file content of example that contains connector flink-sql-connector-hive-3.1.3 and format flink-parquet in a project.
`[code:  ... (1652 chars)]`

After configured the ServicesResourceTransformer, the table connector/format resource files under the directory META-INF/services would be merged rather than overwritten each other when build the uber-jar of above project.

## Schema Mapping

The body clause of a SQL CREATE TABLE statement defines the names and types of physical columns, constraints and watermarks. Flink doesn't hold the data, thus the schema definition only declares how to map physical columns from an external system to Flink's representation. The mapping may not be mapped by names, it depends on the implementation of formats and connectors. For example, a MySQL database table is mapped by field names (not case sensitive), and a CSV filesystem is mapped by field order (field names can be arbitrary). This will be explained in every connector.

The following example shows a simple schema without time attributes and one-to-one field mapping of input/output to table columns.
  SQL
`[code: CREATE TABLE MyTable ( ... (93 chars)]`

### Metadata
Some connectors and formats expose additional metadata fields that can be accessed in metadata columns next to the physical payload columns. See the CREATE TABLE section for more information about metadata columns.

### Primary Key
Primary key constraints tell that a column or a set of columns of a table are unique and they do not contain nulls. Primary key uniquely identifies a row in a table.

The primary key of a source table is a metadata information for optimization. The primary key of a sink table is usually used by the sink implementation for upserting.

SQL standard specifies that a constraint can either be ENFORCED or NOT ENFORCED. This controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data the only mode we want to support is the NOT ENFORCED mode. Its up to the user to ensure that the query enforces key integrity.
  SQL
`[code: CREATE TABLE MyTable ( ... (179 chars)]`

### Time Attributes
Time attributes are essential when working with unbounded streaming tables. Therefore both proctime and rowtime attributes can be defined as part of the schema.

For more information about time handling in Flink and especially event-time, we recommend the general event-time section.

#### Proctime Attributes
In order to declare a proctime attribute in the schema, you can use Computed Column syntax to declare a computed column which is generated from PROCTIME() builtin function. The computed column is a virtual column which is not stored in the physical data.
  SQL
`[code: CREATE TABLE MyTable ( ... (152 chars)]`

#### Rowtime Attributes
In order to control the event-time behavior for tables, Flink provides predefined timestamp extractors and watermark strategies.

Please refer to CREATE TABLE statements for more information about defining time attributes in DDL.

The following timestamp extractors are supported:
  DDL
`[code: -- use the existing TIMESTAMP(3) field in schema as the rowtime attrib ... (394 chars)]`

The following watermark strategies are supported:
  DDL
`[code: -- Sets a watermark strategy for strictly ascending rowtime attributes ... (984 chars)]`

Make sure to always declare both timestamps and watermarks. Watermarks are required for triggering time-based operations.

### SQL Types
Please see the Data Types page about how to declare a type in SQL.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/overview/
# Formats

Flink provides a set of table formats that can be used with table connectors. A table format is a storage format that defines how to map binary data onto table columns.

Flink supports the following formats:
[table: 13 rows]

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/csv/
# CSV Format

Format: Serialization Schema / Format: Deserialization Schema

The CSV format allows to read and write CSV data based on an CSV schema. Currently, the CSV schema is derived from table schema.

## Dependencies
[table: 2 rows]

## How to create a table with CSV format
`[code: CREATE TABLE user_behavior ( ... (363 chars)]`

## Format Options
[table: 16 rows]

## Data Type Mapping
Currently, the CSV schema is always derived from table schema. Explicitly defining an CSV schema is not supported yet. Flink CSV format uses jackson databind API to parse and generate CSV string.
[table: 17 rows]

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/json/
# JSON Format

Format: Serialization Schema / Format: Deserialization Schema

The JSON format allows to read and write JSON data based on an JSON schema. Currently, the JSON schema is derived from table schema.

The JSON format supports append-only streams, unless you're using a connector that explicitly support retract streams and/or upsert streams like the Upsert Kafka connector. If you need to write retract streams and/or upsert streams, we suggest you to look at CDC JSON formats like Debezium JSON and Canal JSON.

## Dependencies
[table: 2 rows]

## How to create a table with JSON format
`[code: CREATE TABLE user_behavior ( ... (374 chars)]`

## Format Options
[table: 10 rows]

## Data Type Mapping
Flink JSON format uses jackson databind API to parse and generate JSON string.
[table: 19 rows]

## Features
### Allow top-level JSON Arrays
Usually, we assume the top-level of JSON string is a stringified JSON object. Then this stringified JSON object can be converted into one SQL row. There are some cases that, the top-level of JSON string is a stringified JSON array, and we want to explode the array into multiple records. Each element within the array is a JSON object, the schema of every such JSON object is the same as defined in SQL, and each of these JSON objects can be converted into one row. Flink JSON Format supports reading such data.
`[code: CREATE TABLE user_behavior ( ... (94 chars)]`
Flink JSON Format will produce 2 rows (123, "a") and (456, "b") with both of following two JSON string. The top-level is JSON Array: `[code: [{"col1": 123, "col2": "a"}, {"col1": 456, "col2": "b"}] ... (56 chars)]`. The top-level is JSON Object: `[code: {"col1": 123, "col2": "a"} ... (53 chars)]`.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/avro/
# Avro Format

Format: Serialization Schema / Format: Deserialization Schema

The Apache Avro format allows to read and write Avro data based on an Avro schema. Currently, the Avro schema is derived from table schema.

## Dependencies
[table: 2 rows]

## How to create a table with Avro format
`[code: CREATE TABLE user_behavior ( ... (295 chars)]`

## Format Options
[table: 5 rows]

## Data Type Mapping
Currently, the Avro schema is always derived from table schema. Explicitly defining an Avro schema is not supported yet.
[table: 18 rows]

In addition to the types listed above, Flink supports reading/writing nullable types. Flink maps nullable types to Avro union(something, null), where something is the Avro type converted from Flink type.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/avro-confluent/
# Confluent Avro Format

Format: Serialization Schema / Format: Deserialization Schema

The Avro Schema Registry (avro-confluent) format allows you to read records that were serialized by the io.confluent.kafka.serializers.KafkaAvroSerializer and to write records that can in turn be read by the io.confluent.kafka.serializers.KafkaAvroDeserializer.

When reading (deserializing) a record with this format the Avro writer schema is fetched from the configured Confluent Schema Registry based on the schema version id encoded in the record while the reader schema is inferred from table schema.

When writing (serializing) a record with this format the Avro schema is inferred from the table schema and used to retrieve a schema id to be encoded with the data. The lookup is performed with in the configured Confluent Schema Registry under the subject given in avro-confluent.subject.

The Avro Schema Registry format can only be used in conjunction with the Apache Kafka SQL connector or the Upsert Kafka SQL Connector.

## Dependencies
[table: 2 rows]

For Maven, SBT, Gradle, or other build automation tools, please also ensure that Confluent's maven repository at https://packages.confluent.io/maven/ is configured in your project's build files.

## How to create tables with Avro-Confluent format
`[code: CREATE TABLE user_created ( ... (605 chars)]` / `[code: INSERT INTO user_created ... (172 chars)]` / `[code: CREATE TABLE user_created ( ... (1201 chars)]` / `[code: CREATE TABLE user_created ( ... (1005 chars)]`

## Format Options
[table: 14 rows]

## Data Type Mapping
Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet. See the Apache Avro Format for the mapping between Avro and Flink DataTypes. Flink maps nullable types to Avro union(something, null).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/protobuf/
# Protobuf Format

Format: Serialization Schema / Format: Deserialization Schema

The Protocol Buffers Protobuf format allows you to read and write Protobuf data, based on Protobuf generated classes.

## Dependencies
[table: 2 rows]

## How to create a table with Protobuf format
`[code: syntax = "proto2"; ... (736 chars)]`
- Use protoc command to compile the .proto file to java classes
- Then compile and package the classes (there is no need to package proto-java into the jar)
- Finally you should provide the jar in your classpath, e.g. pass it using -j in sql-client
`[code: CREATE TABLE simple_test ( ... (517 chars)]`

## Format Options
[table: 6 rows]

## Data Type Mapping
[table: 13 rows]

## Null Values
As protobuf does not permit null values in maps and array, we need to auto-generate default values when converting from Flink Rows to Protobuf.
[table: 7 rows]

## OneOf field
In the serialization process, there's no guarantee that the Flink fields of the same one-of group only contain at most one valid value. When serializing, each field is set in the order of Flink schema, so the field in the higher position will override the field in lower position in the same one-of group.

## Supported Protobuf Versions
Flink uses protobuf-java 4.32.1 (corresponding to Protocol Buffers version 32), which includes support for:
- Proto2 and Proto3 syntax: Traditional syntax = "proto2" and syntax = "proto3" definitions
- Protobuf Editions: The new edition = "2023" and edition = "2024" syntax introduced in Protocol Buffers v27+
- Improved proto3 field presence detection: Better handling of optional fields without the limitations of older protobuf versions

### Using Protobuf Editions
`[code: edition = "2023"; ... (254 chars)]`
Editions allow fine-grained control over feature behavior at the file, message, or field level, while maintaining backward compatibility with proto2 and proto3.

## Additional Resources
Language Guide (proto2), (proto3), (Editions), Protobuf Editions Overview.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/debezium/
# Debezium Format

Changelog-Data-Capture Format / Format: Serialization Schema / Format: Deserialization Schema

Debezium is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL, PostgreSQL, Oracle, Microsoft SQL Server and many other databases into Kafka. Debezium provides a unified format schema for changelog and supports to serialize messages using JSON and Apache Avro.

Flink supports to interpret Debezium JSON and Avro messages as INSERT/UPDATE/DELETE messages into Flink SQL system. This is useful in many cases to leverage this feature, such as synchronizing incremental data from databases to other systems, auditing logs, real-time materialized views on databases, temporal join changing history of a database table and so on.

Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Debezium JSON or Avro messages, and emit to external systems like Kafka. However, currently Flink can't combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UDPATE_AFTER as DELETE and INSERT Debezium messages.

## Dependencies
#### Debezium Confluent Avro
[table: 2 rows]
#### Debezium Json
[table: 2 rows]

## How to use Debezium format
`[code: { ... (318 chars)]`
The MySQL products table has 4 columns (id, name, description and weight). The above JSON message is an update change event on the products table where the weight value of the row with id = 111 is changed from 5.18 to 5.15. Assuming this messages is synchronized to Kafka topic products_binlog, then we can use the following DDLs (for Debezium JSON and Debezium Confluent Avro) to consume this topic and interpret the change events.
#### Debezium JSON DDL
`[code: CREATE TABLE topic_products ( ... (424 chars)]`
In some cases, users may setup the Debezium Kafka Connect with the Kafka configuration 'value.converter.schemas.enable' enabled to include schema in the message. Then the Debezium JSON message may look like this: `[code: { ... (388 chars)]`. In order to interpret such messages, you need to add the option 'debezium-json.schema-include' = 'true' into above DDL WITH clause (false by default). Usually, this is not recommended to include schema because this makes the messages very verbose and reduces parsing performance.
#### Debezium Confluent Avro DDL
`[code: CREATE TABLE topic_products ( ... (547 chars)]`

#### Producing Results
`[code: -- a real-time materialized view on the MySQL "products" ... (388 chars)]`

## Available Metadata
Attention Format metadata fields are only available if the corresponding connector forwards format metadata. Currently, only the Kafka connector is able to expose metadata fields for its value format.
[table: 8 rows]
`[code: CREATE TABLE KafkaTable ( ... (760 chars)]`

## Format Options
Flink provides debezium-avro-confluent and debezium-json formats. Use format debezium-avro-confluent to interpret Debezium Avro messages and format debezium-json to interpret Debezium JSON messages.
  Debezium Avro [table: 14 rows] / Debezium Json [table: 9 rows]

## Caveats
### Duplicate change events
Under normal operating scenarios, the Debezium application delivers every change event exactly-once. However, Debezium application works in at-least-once delivery if any failover happens. That means, in the abnormal situations, Debezium may deliver duplicate change events to Kafka and Flink will get the duplicate events. This may cause Flink query to get wrong results or unexpected exceptions. Thus, it is recommended to set job configuration table.exec.source.cdc-events-duplicate to true and define PRIMARY KEY on the source in this situation. Framework will generate an additional stateful operator, and use the primary key to deduplicate the change events and produce a normalized changelog stream.

### Consuming data produced by Debezium Postgres Connector
If you are using Debezium Connector for PostgreSQL to capture the changes to Kafka, please make sure the REPLICA IDENTITY configuration of the monitored PostgreSQL table has been set to FULL which is by default DEFAULT. Otherwise, Flink SQL currently will fail to interpret the Debezium data. In FULL strategy, the UPDATE and DELETE events will contain the previous values of all the table's columns. In other strategies, the "before" field of UPDATE and DELETE events will only contain primary key columns or null if no primary key. You can change the REPLICA IDENTITY by running ALTER TABLE <your-table-name> REPLICA IDENTITY FULL.

## Data Type Mapping
Currently, the Debezium format uses JSON and Avro format for serialization and deserialization. Please refer to JSON Format documentation and Confluent Avro Format documentation for more details about the data type mapping.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/canal/
# Canal Format

Changelog-Data-Capture Format / Format: Serialization Schema / Format: Deserialization Schema

Canal is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL into other systems. Canal provides a unified format schema for changelog and supports to serialize messages using JSON and protobuf (protobuf is the default format for Canal).

Flink supports to interpret Canal JSON messages as INSERT/UPDATE/DELETE messages into Flink SQL system. Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Canal JSON messages, and emit to storage like Kafka. However, currently Flink can't combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UPDATE_AFTER as DELETE and INSERT Canal messages.

Note: Support for interpreting Canal protobuf messages is on the roadmap.

## Dependencies
[table: 2 rows]

## How to use Canal format
`[code: { ... (596 chars)]`
`[code: CREATE TABLE topic_products ( ... (380 chars)]`
`[code: -- a real-time materialized view on the MySQL "products" ... (389 chars)]`

## Available Metadata
[table: 7 rows]
`[code: CREATE TABLE KafkaTable ( ... (793 chars)]`

## Format Options
[table: 9 rows]

## Caveats
### Duplicate change events
Under normal operating scenarios, the Canal application delivers every change event exactly-once. However, Canal application works in at-least-once delivery if any failover happens. Thus, it is recommended to set job configuration table.exec.source.cdc-events-duplicate to true and define PRIMARY KEY on the source. Framework will generate an additional stateful operator, and use the primary key to deduplicate the change events and produce a normalized changelog stream.

## Data Type Mapping
Currently, the Canal format uses JSON format for serialization and deserialization.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/maxwell/
# Maxwell Format

Changelog-Data-Capture Format / Format: Serialization Schema / Format: Deserialization Schema

Maxwell is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL into Kafka, Kinesis and other streaming connectors. Maxwell provides a unified format schema for changelog and supports to serialize messages using JSON.

Flink supports to interpret Maxwell JSON messages as INSERT/UPDATE/DELETE messages into Flink SQL system. Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Maxwell JSON messages, and emit to external systems like Kafka. However, currently Flink can't combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UDPATE_AFTER as DELETE and INSERT Maxwell messages.

## Dependencies
[table: 2 rows]

## How to use Maxwell format
`[code: { ... (440 chars)]`
`[code: CREATE TABLE topic_products ( ... (347 chars)]`
`[code: -- a real-time materialized view on the MySQL "products" ... (388 chars)]`

## Available Metadata
[table: 5 rows]
`[code: CREATE TABLE KafkaTable ( ... (608 chars)]`

## Format Options
[table: 8 rows]

## Caveats
### Duplicate change events
The Maxwell application allows to deliver every change event exactly-once. If Maxwell application works in at-least-once delivery, it may deliver duplicate change events to Kafka. Thus, it is recommended to set job configuration table.exec.source.cdc-events-duplicate to true and define PRIMARY KEY on the source. Framework will generate an additional stateful operator, and use the primary key to deduplicate the change events and produce a normalized changelog stream.

## Data Type Mapping
Currently, the Maxwell format uses JSON for serialization and deserialization.
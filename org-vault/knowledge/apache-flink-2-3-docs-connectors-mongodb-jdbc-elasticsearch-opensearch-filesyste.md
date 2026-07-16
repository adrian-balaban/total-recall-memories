---
title: Apache Flink 2.3 docs — Connectors (mongodb/jdbc/elasticsearch/opensearch/filesystem/hbase/datagen/print/blackhole/hive-overview)
tags: [org, flink, flink-2.3, docs, connectors, mongodb, jdbc, elasticsearch, opensearch, filesystem, hbase, datagen, print, blackhole, hive, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:40:08.911Z'
updated: '2026-07-08T04:40:08.911Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Captured Apache Flink 2.3 docs Connectors section batch conn3 (indices 191-200): SQL table connectors mongodb, jdbc (incl. JDBC catalog for Postgres/MySQL), elasticsearch, opensearch, filesystem (rich — partition files/rolling policy/file compaction/partition commit), hbase, datagen, print, blackhole, and the hive/overview page. WHY: ongoing ingestion of full Flink 2.3 EN docs into org memories for verbatim reference. Prose near-verbatim; code blocks condensed `[code: <first-line> ... (N chars)]`; tables `[table: N rows]`. NOTE: mongodb/jdbc/elasticsearch/opensearch/hbase/hive all state "There is no connector (yet) available for Flink version 2.3" — not in binary distribution; link for cluster execution. filesystem/datagen/print/blackhole are built-in.

## Source: .../table/mongodb/ — MongoDB SQL Connector
Scan Source: Bounded; Lookup Source: Sync Mode; Sink: Batch; Sink: Streaming Append & Upsert Mode. Reads/writes MongoDB. Upsert mode if primary key defined (UPDATE/DELETE); append-only (INSERT) if no PK.
Dependencies: "no connector (yet) available for Flink version 2.3"; not in binary distribution.
How to create `[code: -- register a MongoDB table 'users' in Flink SQL ... (694 chars)]`.
Connector Options [table: 24 rows].
### Key handling
PK defined → upsert (consume UPDATE/DELETE); no PK → append (INSERT only). In MongoDB the PK calculates the document _id — must be unique, immutable, any BSON type other than Array; if _id has subfields, subfield names cannot begin with $. Index key limit <1024 bytes before MongoDB 4.2; removed in 4.2. Connector generates _id by compositing all PK fields in DDL order: single field → bson value as _id; multiple fields → bson document _id: {f1:v1, f2:v2}. Ambiguous if _id field in DDL but PK not declared as _id — use _id as key or rename.
### Partitioned Scan strategies: single (whole collection one partition); sample (fast, possibly uneven); split-vector (splitVector command, fast+even, non-sharded, requires splitVector permission); sharded (reads config.chunks, sharded only, fast+even, requires read perm on config db); default (sharded for sharded else split-vector).
### Lookup Cache: sync lookup only. lookup.cache=PARTIAL enables; each TaskManager holds cache; expire on max rows (lookup.partial-cache.max-rows) or TTL (expire-after-write/expire-after-access). Tradeoff throughput vs correctness. Caches empty query result for PK by default (toggle lookup.partial-cache.caching-missing-key=false).
### Idempotent Writes: uses db.connection.update(<query>,<update>,{upsert:true}) rather than insert() if PK defined. INSERT OVERWRITE forces upsert — rejects if no PK. Upsert recommended to avoid constraint violations/dupes on re-processing.
### Upsert on sharded collection: upsert:true filter must include shard key; documents can miss shard key (use null equality match + another filter). In Flink SQL declare shard keys via PARTITIONED BY. LIMITATION: shard key must remain immutable (though mutable since MongoDB 4.2) — upsert to sharded can only get updated shard key value, not original → duplicate record error.
### Filters Pushdown: simple comparisons/logical filters → MongoDB query operators [table: 11 rows].
Data Type Mapping: BSON → Flink SQL [table: 14 rows]; Extended JSON for specific types → STRING [table: 5 rows].

## Source: .../table/jdbc/ — JDBC SQL Connector
Scan Source: Bounded; Lookup Source: Sync Mode; Sink: Batch; Sink: Streaming Append & Upsert Mode. Reads/writes any relational DB with JDBC driver. Upsert mode if PK defined else append.
Dependencies: "no connector (yet)"; not in binary distribution. Driver dependency required — supported drivers [table: 6 rows].
How to create `[code: -- register a MySQL table 'users' ... (650 chars)]`.
Connector Options [table: 24 rows]. Deprecated Options [table: 4 rows].
### Key handling: upsert if PK defined (insert new or update existing → idempotence; PK should be unique key/PK of underlying table); append if no PK (INSERT may fail on PK/unique violation).
### Partitioned Scan: all options must be specified together — scan.partition.column (numeric/date/timestamp), scan.partition.num, scan.partition.lower-bound, scan.partition.upper-bound (decide stride + filter rows).
### Lookup Cache: sync only; lookup.cache=PARTIAL; same cache semantics as MongoDB (max-rows, expire-after-write/access, caching-missing-key).
### Idempotent Writes: upsert semantics if PK defined (atomically add or update on unique-constraint violation). Database-specific DML for upsert [table: 5 rows].
## JDBC Catalog
JdbcCatalog connects Flink to relational DBs over JDBC. Two implementations: Postgres Catalog, MySQL Catalog. Supported methods `[code: // The supported methods by Postgres & MySQL Catalog. ... (241 chars)]`.
Options: name, default-database, username, password, base-url (no DB name; Postgres "jdbc:postgresql://<ip>:<port>", MySQL "jdbc:mysql://<ip>:<port>"). Create `[code: CREATE CATALOG my_catalog WITH( ... (183 chars)]` + Java/Scala/Python/YAML examples.
### JDBC Catalog for PostgreSQL — Metaspace Mapping: Postgres has database→schema→table (default schema "public"). Use schema_name.table_name or just table_name (defaults public). [table: 4 rows]. Full path "<catalog>.<db>.`<schema.table>`" (escaped). Examples `[code: -- scan table 'test_table' of 'public' schema ... (467 chars)]`.
### JDBC Catalog for MySQL — Metaspace Mapping: MySQL instance→database→table. Use database.table_name or just table_name (default = default-database). [table: 4 rows]. Full path "`<catalog>`.`<db>`.`<table>`". Examples `[code: -- scan table 'test_table', the default database is 'mydb'. ... (312 chars)]`.
Data Type Mapping: MySQL/Oracle/PostgreSQL/Derby dialects (Derby for testing). [table: 16 rows].

## Source: .../table/elasticsearch/ — Elasticsearch SQL Connector
Sink: Batch; Sink: Streaming Append & Upsert Mode. Writes into Elasticsearch index. Upsert if PK defined else append.
Dependencies: "no connector (yet)"; not in binary distribution.
How to create `[code: CREATE TABLE myUserTable ( ... (231 chars)]`.
Connector Options [table: 21 rows].
### Key Handling: PK → document id (string ≤512 bytes, no whitespaces); concatenated PK fields in DDL order with document-id.key-delimiter. BYTES/ROW/ARRAY/MAP not allowed as PK (no good string repr). No PK → ES auto-generates document id.
### Dynamic Index: static (plain string e.g. 'myusers') or dynamic — {field_name} references field value; {field_name|date_format_string} converts TIMESTAMP/DATE/TIME (Java DateTimeFormatter compatible); {now()|date_format_string} uses current system time (TIMESTAMP_WITH_LTZ, formatted with table.local-time-zone; NOW()/now()/CURRENT_TIMESTAMP/current_timestamp). NOTE: dynamic index from system time → no guarantee same PK → same index name → only supports append-only stream.
Data Type Mapping: ES stores JSON; uses built-in 'json' format.

## Source: .../table/opensearch/ — Opensearch SQL Connector
Sink: Batch; Sink: Streaming Append & Upsert Mode. Identical structure to Elasticsearch connector (writes into Opensearch index). Upsert if PK defined else append.
Dependencies: "no connector (yet)"; not in binary distribution.
How to create `[code: CREATE TABLE myUserTable ( ... (226 chars)]`. Connector Options [table: 21 rows].
### Key Handling: same as ES — PK → document id (≤512 bytes, no whitespace), document-id.key-delimiter, disallowed PK types (BYTES/ROW/ARRAY/MAP), auto-generate if no PK.
### Dynamic Index: same as ES (static/dynamic, {field_name}, {field_name|date_format}, {now()|date_format}, system-time dynamic index only append-only).
Data Type Mapping: uses built-in 'json' format.

## Source: .../table/filesystem/ — FileSystem SQL Connector
Access to partitioned files in Flink FileSystem abstraction. Built-in (jar in /lib). Format required for read/write.
`[code: CREATE TABLE MyUserTable ( ... (1474 chars)]`. NOTE: path = directory not file; can't get human-readable file in declared path. Include Flink File System specific dependencies.
## Partition Files: hive format; partitions discovered/inferred from directory structure (no pre-registration). `[code: path ... (207 chars)]`. Supports partition inserting + overwrite inserting (overwrite only corresponding partition, not whole table).
## File Formats: CSV (RFC-4180); JSON (newline-delimited, not typical JSON file); Avro (compression via avro.codec); Parquet (Hive-compatible); Orc (Hive-compatible); Debezium-JSON; Canal-JSON; Raw.
## Source: reads single files or entire directories (no defined ingestion order for directory).
### Directory watching: bounded by default (scan once, close). Continuous watching via source.monitor-interval [table: 2 rows].
### Available Metadata: read-only [table: 5 rows]. Example `[code: CREATE TABLE MyUserTableWithFilepath ( ... (220 chars)]`.
## Streaming Sink: row-encoded (CSV, JSON) + bulk-encoded (Parquet, ORC, Avro).
### Rolling Policy: part files per subtask per partition; rolls by size + timeout [table: 4 rows]. Bulk formats: rolling policy + checkpoint interval (pending→finished on next checkpoint). Row formats: sink.rolling-policy.file-size/rollover-interval + execution.checkpointing.interval.
### File Compaction [table: 3 rows]: merges small files into larger by target size. Caveats: only files in single checkpoint compacted (≥ #checkpoints files); pre-merge files invisible (visibility = checkpoint interval + compaction time); long compaction backpressures + extends checkpoint.
### Partition Commit: notify downstream (add partition to Hive metastore or _SUCCESS file). Trigger + Policy. Only works in dynamic partition inserting.
#### Partition commit trigger [table: 4 rows]: (1) partition processing time (no watermark needed; commit by partition creation time + system time; universal but imprecise — delay/failover → premature commit). (2) partition-time (watermark + time extracted from partition values; requires hourly/daily partition). Quick commit: process-time + delay 0s (may commit multiple times). Accurate: partition-time + delay 1h. No watermark: process-time + delay 1h. Late data → re-trigger commit.
#### Partition Time Extractor [table: 5 rows]: default timestamp pattern from partition fields; custom via PartitionTimeExtractor `[code: ... (311 chars)]`.
#### Partition Commit Policy [table: 5 rows]: metastore (Hive only) or success file (empty file in partition dir). Custom via interface `[code: ... (794 chars)]`.
## Sink Parallelism [table: 2 rows]: default = upstream parallelism; only supported if changelog mode INSERT-ONLY else exception.
## Full Example: Kafka → filesystem streaming + batch read-back `[code: ... (760 chars)]`. For TIMESTAMP_LTZ watermark + partition-time commit, set sink.partition-commit.watermark-time-zone to session time zone `[code: ... (1097 chars)]`.

## Source: .../table/hbase/ — HBase SQL Connector
Scan Source: Bounded; Lookup Source: Sync Mode; Sink: Batch; Sink: Streaming Upsert Mode. Always upsert mode; PK must be on HBase rowkey field (rowkey field must be declared); if no PRIMARY KEY clause, rowkey taken as PK by default.
Dependencies: "no connector (yet)"; not in binary distribution.
How to use: column families declared as ROW type (field name = column family, nested = qualifier); single atomic field (STRING/BIGINT) = rowkey (arbitrary name, backtick if reserved). `[code: -- register the HBase table 'mytable' ... (864 chars)]`.
Available Metadata [table: 3 rows] (R/W; read-only must be VIRTUAL).
Connector Options [table: 19 rows]. Deprecated Options [table: 3 rows].
Data Type Mapping: HBase stores byte arrays; uses org.apache.hadoop.hbase.util.Bytes. Null → empty bytes (except string type uses null-string-literal option). [table: 17 rows].

## Source: .../table/datagen/ — DataGen SQL Connector
Scan Source: Bounded & Unbounded. In-memory data generation for local query dev without external systems. Built-in. Supports Computed Column syntax.
By default unbounded random rows; bounded if total rows specified. Length handling: fixed-length (char/binary) schema-only; variable-length (varchar/varbinary) schema-defined, custom ≤ schema; super-long (string/bytes) default 100, settable <2^31. Sequence generator (start/end); if any column sequence → bounded, ends when first sequence completes. Time types = local system time. `[code: CREATE TABLE Orders ( ... (204 chars)]`. Often used with LIKE clause to mock physical tables `[code: ... (339 chars)]`. Variable-length generation toggle `[code: ... (271 chars)]`. Collection sizes `[code: ... (211 chars)]`.
Types [table: 24 rows]. Connector Options [table: 14 rows].

## Source: .../table/print/ — Print SQL Connector
Sink only. Writes every row to stdout/stderr. For easy streaming test + production debugging. Built-in. Four format options [table: 5 rows]. Output format "$row_kind(f0,f1,f2…)", e.g. "+I(1,1)". Attention: observe task log.
How to create `[code: CREATE TABLE print_table ( ... (107 chars)]` or via LIKE `[code: CREATE TABLE print_table WITH ('connector' = 'print') ... (87 chars)]`. Connector Options [table: 5 rows].

## Source: .../table/blackhole/ — BlackHole SQL Connector
Sink: Bounded & Unbounded. Swallows all input records (like /dev/null). For high-performance testing + UDF output without substantive sink. Built-in.
How to create `[code: CREATE TABLE blackhole_table ( ... (115 chars)]` or via LIKE `[code: CREATE TABLE blackhole_table WITH ('connector' = 'blackhole') ... (95 chars)]`. Connector Options [table: 2 rows].

## Source: .../table/hive/overview/ — Apache Hive
Hive = data warehousing focal point (SQL engine + data management platform). Flink two-fold integration: (1) Hive Metastore as persistent catalog via HiveCatalog (store Flink-specific metadata e.g. Kafka/ES tables); (2) Flink as alternative engine for reading/writing Hive tables. HiveCatalog "out of the box" compatible — no Hive Metastore/data placement/partitioning changes.
## Supported Hive Versions: 2.3, 2.3.0–2.3.10, 3.1, 3.1.0–3.1.3. Version-specific Hive features (not Flink-caused): built-in functions 1.2.0+; column constraints (PRIMARY KEY/NOT NULL) 3.1.0+; alter table statistics 1.2.0+; DATE column statistics 1.2.0+; writing ORC tables NOT supported in 2.0.x.
### Dependencies: "no connector (yet)"; not in binary distribution. Add extra deps to /lib or dedicated folder (-C/-l). Hive built on Hadoop → set HADOOP_CLASSPATH=`[code: export HADOOP_CLASSPATH=`hadoop classpath` ... (42 chars)]`. Two ways to add Hive deps: bundled Hive jars (recommended) or separate jars. Bundled jars [table: 3 rows]. User-defined: Hive 2.3.4 `[code: ... (301 chars)]`, Hive 3.1.0 `[code: /flink-2.3.0 ... (356 chars)]`.
### Program maven: deps for own program (don't include in resulting jar; add at runtime) `[code: <!-- Flink Dependency --> ... (573 chars)]`.
## Connecting To Hive: via catalog interface + HiveCatalog (Table env or YAML). Java/Scala/Python/YAML/SQL examples. HiveCatalog options [table: 7 rows].
## DDL: recommended to use Hive dialect to create Hive tables/views/partitions/functions within Flink.
## DML: Flink supports DML writing to Hive tables (see Reading & Writing HiveTables).

[Stored as part of ongoing Apache Flink 2.3 EN docs ingestion — related [[org/knowledge/apache-flink-2-3-docs-connectors-table-overview-formats-csv-json-avro-confluent-]] (#31), [[org/knowledge/apache-flink-2-3-docs-connectors-formats-ogg-parquet-orc-raw-kafka-upsert-kafka-]] (#32). Next: connectors 201-235, then deployment/ops/internals.]
---
title: 'Apache Flink 2.3 docs — SQL Reference: DML (INSERT/TRUNCATE/UPDATE/DELETE)'
tags: [org, flink, flink-2.3, docs, sql, sql-reference, dml, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:29:52.608Z'
updated: '2026-07-07T19:29:52.608Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 docs — SQL Reference DML: INSERT / TRUNCATE / UPDATE / DELETE statements. Captured 2026-07-07 from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/dml/ (English). Prose/headings verbatim; code condensed to `[code: ... (N chars)]`. Run-a-statement boilerplate (Java executeSql()/Scala/Python/SQL CLI) is identical across DML: single statement via executeSql()/execute_sql() submits Flink job immediately and returns TableResult; multiple via StatementSet addInsertSql()/add_insert_sql() (lazy, executed on StatementSet.execute()).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/dml/insert/
# INSERT Statement
Add rows to a table.

## Insert from select queries
Syntax: `[code:  ... (242 chars)]`.
- OVERWRITE: INSERT OVERWRITE overwrites existing data in table/partition; otherwise appended.
- PARTITION: contains static partition columns of the insert.
- COLUMN LIST: `INSERT INTO T(c, b) SELECT x, y FROM S` writes x→c, y→b, a→NULL (if a nullable). Connector developers can use `DynamicTableSink$Context.getTargetColumns()` to handle partial column updates without overwriting non-target columns with null.

## Insert values into tables
`[code: [EXECUTE] INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_n ... (148 chars)]`. INSERT...VALUES inserts directly from SQL. OVERWRITE overwrites existing data.

## Insert into multiple tables
STATEMENT SET inserts into multiple tables in one statement. Syntax: `EXECUTE STATEMENT SET ...`.

## ON CONFLICT clause
When query produces an updating table with an upsert key differing from sink's primary key, multiple records with different upsert keys may map to same PK. ON CONFLICT specifies how to resolve.

### When is ON CONFLICT required?
By default Flink requires explicit ON CONFLICT whenever query upsert key differs from sink PK; without it, query fails at planning time. Controlled by `table.exec.sink.require-on-conflict` (default true; false = legacy behavior, may give non-deterministic results). Alternatively disable sink upsert materializer via `table.exec.sink.upsert-materialize=NONE` (removes materializer operator; no buffering/compaction/conflict resolution; records passed directly to sink).

Syntax: `[code: [EXECUTE] INSERT INTO [catalog_name.][db_name.]table_name ... (175 chars)]`.

### Strategies
- DO ERROR: throws runtime exception if multiple records with different upsert keys map to same PK. Use when no real conflict exists (planner couldn't prove equivalence but logically equivalent). Buffered records compacted on watermark progression before conflict checking (transient disorder from changelog reordering doesn't cause false errors).
- DO NOTHING: keeps first record per PK, silently discards subsequent conflicting records. Also uses watermark-based compaction.
- DO DEDUPLICATE: maintains full history of changes per PK in state to support rollback on retraction. Most correct when true multi-source updates to same PK occur and correctness can't be sacrificed. Significantly higher state usage. Does NOT use watermark-based compaction.

### How conflicts happen
Conflict occurs when query's upsert key differs from sink PK. Example: join whose result has upsert key from join condition, but target table has different PK. Retraction (-U) and update (+U) may travel different paths and arrive out of order. DO ERROR / DO NOTHING use watermark-based compaction to wait for consistent changes before resolution.

### Watermark-based compaction
Buffers incoming records keyed by PK + upsert key. On watermark advance, buffered records with timestamps ≤ watermark are compacted: matching insert/retraction pairs for same upsert key cancel (e.g. +I and -D, or -U and +U). Example: order 1 changes Laptop→Phone while order 3 is Laptop; without compaction two active records for PK Laptop with different upsert keys = false conflict; with compaction, -U[Laptop,1] cancels +I[Laptop,1] (same upsert key order_id=1), leaving +I[Laptop,3] = no conflict. After compaction, 0/1 records per PK = no conflict; multiple with different upsert keys = genuine conflict resolved by chosen strategy. DO DEDUPLICATE maintains full history in state instead.

### Examples
Source/dimension tables example. Two records with different upsert keys (order_id=1 and order_id=3) target same PK (product_name='Laptop'):
- DO ERROR — runtime exception.
- DO NOTHING — keeps first, discards conflict [table: 3 rows].
- DO DEDUPLICATE — accepts both; last arriving value visible [table: 3 rows].
Retraction behavior: if order 3 deleted, join emits -D[Laptop,3]:
- DO NOTHING — no effect ((Laptop,3) never written); Laptop row remains last_order_id=1.
- DO DEDUPLICATE — rolls back to previous value; Laptop falls back to order 1, producing {(Laptop,1),(Phone,2)}. Full history enables correct rollback.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/dml/truncate/
# TRUNCATE Statements
Batch mode only. Deletes all rows without dropping the table. Requires target table connector implements `SupportsTruncate` interface for row-level delete; exception thrown otherwise. Syntax: `TRUNCATE TABLE [catalog_name.][db_name.]table_name`.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/dml/update/
# UPDATE Statements
Row-level update on target table per optional filter. Batch mode only. Requires target table connector implements `SupportsRowLevelUpdate` interface; exception otherwise. Currently no Flink-maintained connector supports UPDATE yet. Syntax: `UPDATE [catalog_name.][db_name.]table_name SET column_name1 = expression1 [, ...] [ WHERE condition ]`.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/dml/delete/
# DELETE Statements
Row-level deletion on target table per optional filter. Batch mode only. Requires target table connector implements `SupportsRowLevelDelete` interface; exception otherwise. Currently no Flink-maintained connector supports DELETE yet. Syntax: `DELETE FROM [catalog_name.][db_name.]table_name [ WHERE condition ]`.
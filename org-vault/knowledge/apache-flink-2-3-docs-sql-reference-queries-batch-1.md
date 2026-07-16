---
title: 'Apache Flink 2.3 docs — SQL Reference: Queries (batch 1)'
tags: [org, flink, flink-2.3, docs, sql, sql-reference, queries, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:37:08.385Z'
updated: '2026-07-07T19:37:08.385Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL Reference — Queries (batch 1). Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`. Covers: overview, hints, with, select, select-distinct, window-tvf, model-inference, vector-search, window-agg, changelog, group-agg, over-agg.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/overview/
# Queries

SELECT statements and VALUES statements are specified with the sqlQuery() method of the TableEnvironment. The method returns the result of the SELECT statement (or the VALUES statements) as a Table. A Table can be used in subsequent SQL and Table API queries, be converted into a DataStream, or written to a TableSink. SQL and Table API queries can be seamlessly mixed and are holistically optimized and translated into a single program.

In order to access a table in a SQL query, it must be registered in the TableEnvironment. A table can be registered from a TableSource, Table, CREATE TABLE statement, DataStream. Alternatively, users can also register catalogs in a TableEnvironment to specify the location of the data sources.

For convenience, Table.toString() automatically registers the table under a unique name in its TableEnvironment and returns the name. So, Table objects can be directly inlined into SQL queries as shown in the examples below.

Note: Queries that include unsupported SQL features cause a TableException. The supported features of SQL on batch and streaming tables are listed in the following sections.

## Specifying a Query

The following examples show how to specify a SQL queries on registered and inlined tables.

  Java
`[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (1541 chars)]`

  Scala
`[code: val env = StreamExecutionEnvironment.getExecutionEnvironment ... (1371 chars)]`

  Python
`[code: env = StreamExecutionEnvironment.get_execution_environment() ... (993 chars)]`

## Execute a Query

A SELECT statement or a VALUES statement can be executed to collect the content to local through the TableEnvironment.executeSql() method. The method returns the result of the SELECT statement (or the VALUES statement) as a TableResult. Similar to a SELECT statement, a Table object can be executed using the Table.execute() method to collect the content of the query to the local client. TableResult.collect() method returns a closeable row iterator. The select job will not be finished unless all result data has been collected. We should actively close the job to avoid resource leak through the CloseableIterator#close() method. We can also print the select result to client console through the TableResult.print() method. The result data in TableResult can be accessed only once. Thus, collect() and print() must not be called after each other.

TableResult.collect() and TableResult.print() have slightly different behaviors under different checkpointing settings (to enable checkpointing for a streaming job, see checkpointing config).

- For batch jobs or streaming jobs without checkpointing, TableResult.collect() and TableResult.print() have neither exactly-once nor at-least-once guarantee. Query results are immediately accessible by the clients once they're produced, but exceptions will be thrown when the job fails and restarts.

- For streaming jobs with exactly-once checkpointing, TableResult.collect() and TableResult.print() guarantee an end-to-end exactly-once record delivery. A result will be accessible by clients only after its corresponding checkpoint completes.

- For streaming jobs with at-least-once checkpointing, TableResult.collect() and TableResult.print() guarantee an end-to-end at-least-once record delivery. Query results are immediately accessible by the clients once they're produced, but it is possible for the same result to be delivered multiple times.

  Java
`[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (722 chars)]`

  Scala
`[code: val env = StreamExecutionEnvironment.getExecutionEnvironment() ... (800 chars)]`

  Python
`[code: env = StreamExecutionEnvironment.get_execution_environment() ... (607 chars)]`

## Syntax

Flink parses SQL using Apache Calcite, which supports standard ANSI SQL.

The following BNF-grammar describes the superset of supported SQL features in batch and streaming queries. The Operations section shows examples for the supported features and indicates which features are only supported for batch or streaming queries.

  Grammar
`[code: query: ... (3165 chars)]`

Flink SQL uses a lexical policy for identifier (table, attribute, function names) similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. SELECT a AS `my field` FROM t).

String literals must be enclosed in single quotes (e.g., SELECT 'Hello World'). Duplicate a single quote for escaping (e.g., SELECT 'It''s me').
`[code: Flink SQL> SELECT 'Hello World', 'It''s me'; ... (187 chars)]`

Unicode characters are supported in string literals. If explicit unicode code points are required, use the following syntax:
- Use the backslash (\) as escaping character (default): SELECT U&'\263A'
- Use a custom escaping character: SELECT U&'#263A' UESCAPE '#'

Starting Flink 2.0 there is C-style escape available
[table: 9 rows]

Example: SELECT e'a\x61\141' AS c or SELECT E'a\x61\141' AS c;

## Operations
- WITH clause
- SELECT & WHERE
- SELECT DISTINCT
- Windowing TVF
- Window Aggregation
- Group Aggregation
- Over Aggregation
- Joins
- Set Operations
- ORDER BY clause
- LIMIT clause
- Top-N
- Window Top-N
- Deduplication
- Pattern Recognition
- Time Travel

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/hints/
# SQL Hints
Batch / Streaming

SQL hints can be used with SQL statements to alter execution plans. This chapter explains how to use hints to force various approaches.

Generally a hint can be used to:
- Enforce planner: there's no perfect planner, so it makes sense to implement hints to allow user better control the execution;
- Append meta data(or statistics): some statistics like "table index for scan" and "skew info of some shuffle keys" are somewhat dynamic for the query, it would be very convenient to config them with hints because our planning metadata from the planner is very often not that accurate;
- Operator resource constraints: for many cases, we would give a default resource configuration for the execution operators, i.e. min parallelism or managed memory (resource consuming UDF) or special resource requirement (GPU or SSD disk) and so on, it would be very flexible to profile the resource with hints per query(instead of the Job).

## Dynamic Table Options

Dynamic table options allows to specify or override table options dynamically, different with static table options defined with SQL DDL or connect API, these options can be specified flexibly in per-table scope within each query. Thus it is very suitable to use for the ad-hoc queries in interactive terminal, for example, in the SQL-CLI, you can specify to ignore the parse error for a CSV source just by adding a dynamic option /*+ OPTIONS('csv.ignore-parse-errors'='true') */.

### Syntax
In order to not break the SQL compatibility, we use the Oracle style SQL hint syntax:
`[code: table_path /*+ OPTIONS(key=val [, key=val]*) */ ... (94 chars)]`

### Examples
`[code:  ... (657 chars)]`

## Query Hints

Query hints can be used to suggest the optimizer to affect query execution plan within a specified query scope. Their effective scope is current Query block(What are query blocks ?) which Query Hints are specified. Now, Flink Query Hints only support Join Hints.

### Syntax
The Query Hints syntax in Flink follows the syntax of Query Hints in Apache Calcite:
`[code: # Query Hints: ... (379 chars)]`

### Conflict Cases In Query Hints

#### Resolution of Key-value Hint Conflicts
For key-value hints, which are provided in the following syntax:
`[code: hintName '(' optionKey '=' optionVal [, optionKey '=' optionVal ]* ')' ... (70 chars)]`
When Flink encounters conflicting in key-value hints, it adopts a last-write-wins strategy. This means that if multiple hint values are provided for the same key, Flink will use the value from the last hint specified in the query. For instance, consider the following SQL query with conflicting 'max-attempts' values in the LOOKUP hint:
`[code: SELECT /*+ LOOKUP('table'='D', 'max-attempts'='3', 'max-attempts'='4') ... (131 chars)]`
In this case, Flink will resolve the conflict by selecting the last specified value for 'max-attempts'. Therefore, the effective hint for 'max-attempts' will be '4'.

#### Resolution of List Hint Conflicts
List hints are provided using the following syntax:
`[code: hintName '(' hintOption [, hintOption ]* ')' ... (44 chars)]`
With list hints, Flink resolves conflicts by adopting a first-accept strategy. This means that the first specified hint in the list will take precedence and be effective. For example, consider the following SQL query with conflicting BROADCAST hints:
`[code: SELECT /*+ BROADCAST(t2, t1), BROADCAST(t1, t2) */ * FROM t1 JOIN t2 O ... (86 chars)]`
In this scenario, Flink will choose the BROADCAST hint that is listed first. Therefore, the effective broadcast hint is BROADCAST(t2, t1).

### Join Hints

Join Hints allow users to suggest the join strategy to optimizer in order to get a more high-performance execution plan. Now Flink Join Hints support BROADCAST, SHUFFLE_HASH, SHUFFLE_MERGE and NEST_LOOP.

Note:
- The table specified in Join Hints must exist. Otherwise, a table not exists error will be thrown.
- Flink Join Hints only support one hint block in a query block, if multiple hint blocks are specified like /*+ BROADCAST(t1) */ /*+ SHUFFLE_HASH(t1) */, an exception will be thrown when parse this query statement.
- In one hint block, specifying multiple tables in a single Join Hint like /*+ BROADCAST(t1, t2, ..., tn) */ or specifying multiple Join Hints like /*+ BROADCAST(t1), BROADCAST(t2), ..., BROADCAST(tn) */ are both supported.
- For multiple tables in a single Join Hints or multiple Join Hints in a hint block, Flink Join Hints may conflict. If the conflicts occur, Flink will choose the most matching table or join strategy. (See: Conflict Cases In Join Hints)

#### BROADCAST
Batch. BROADCAST suggests that Flink uses BroadCast join. The join side with the hint will be broadcast regardless of table.optimizer.join.broadcast-threshold, so it performs well when the data volume of the hint side of table is very small.
  Note: BROADCAST only supports join with equivalence join condition, and it doesn't support Full Outer Join.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (929 chars)]`

#### SHUFFLE_HASH
Batch. SHUFFLE_HASH suggests that Flink uses Shuffle Hash join. The join side with the hint will be the join build side, it performs well when the data volume of the hint side of table is not too large.
  Note: SHUFFLE_HASH only supports join with equivalence join condition.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (707 chars)]`

#### SHUFFLE_MERGE
Batch. SHUFFLE_MERGE suggests that Flink uses Sort Merge join. This type of Join Hint is recommended for using in the scenario of joining between two large tables or the scenario that the data at both sides of the join is already in order.
  Note: SHUFFLE_MERGE only supports join with equivalence join condition.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (658 chars)]`

#### NEST_LOOP
Batch. NEST_LOOP suggests that Flink uses Nested Loop join. This type of join hint is not recommended without special scenario requirements.
  Note: NEST_LOOP supports both equivalent and non-equivalent join condition.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (500 chars)]`

#### MULTI_JOIN
Streaming. MULTI_JOIN suggests that Flink uses the MultiJoin operator to process multiple regular joins simultaneously. This type of join hint is recommended when you have multiple joins that share at least one common join key and experience large intermediate state or record amplification. The MultiJoin operator eliminates intermediate state by processing joins across various input streams simultaneously, which can significantly reduce state size and improve performance in some cases.

For more details on the MultiJoin operator, including when to use it and configuration options, see Multiple Regular Joins.

Note:
- The MULTI_JOIN hint can specify table names or table aliases. If a table has an alias, the hint must use the alias name.
- At least one key must be shared between the join conditions for the MultiJoin operator to be applied.
- When specified, the MULTI_JOIN hint applies to the tables listed in the hint within the current query block.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (934 chars)]`

#### LOOKUP
Streaming. The LOOKUP hint allows users to suggest the Flink optimizer to:
- use synchronous(sync) or asynchronous(async) lookup function
- configure the async parameters
- enable delayed retry strategy for lookup

#### LOOKUP Hint Options:
[table: 11 rows]

Note:
- 'table' option is required, only table name is supported(keep consistent with which in the FROM clause), note that only alias name can be used if table has an alias name.
- async options are all optional, will use default value if not configured.
- there is no default value for retry options, all retry options should be set to valid values when need to enable retry.

#### 1. Use Sync And Async Lookup Function
If the connector has both capabilities of async and sync lookup, users can give the option value 'async'='false' to suggest the planner to use the sync lookup or 'async'='true' to use the async lookup:
`[code: -- suggest the optimizer to use sync lookup ... (178 chars)]`
  Note: the optimizer prefers async lookup if no 'async' option is specified, it will always use sync lookup when:
- the connector only implements the sync lookup
- user enables 'TRY_RESOLVE' mode of 'table.optimizer.non-deterministic-update.strategy' and the optimizer has checked there's correctness issue caused by non-deterministic update.

#### 2. Configure The Async Parameters
Users can configure the async parameters via async options on async lookup mode.
`[code: -- configure the async parameters: 'output-mode', 'capacity', 'timeout ... (220 chars)]`
  Note: the async options are consistent with the async options in job level Execution Options, will use job level configuration if not set. Another difference is that the scope of the LOOKUP hint is smaller, limited to the table name corresponding to the hint option set in the current lookup operation (other lookup operations will not be affected by the LOOKUP hint).
e.g., if the job level configuration is:
`[code: table.exec.async-lookup.output-mode: ORDERED ... (127 chars)]`
then the following hints:
`[code: 1. LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_un ... (144 chars)]`
are equivalent to:
`[code: 1. LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_un ... (223 chars)]`

#### 3. Enable Delayed Retry Strategy For Lookup
Delayed retry for lookup join is intended to solve the problem of delayed updates in external system which cause unexpected enrichment with stream data. The hint option 'retry-predicate'='lookup_miss' can enable retry on both sync and async lookup, only fixed delay retry strategy is supported currently.
Options of fixed delay retry strategy:
`[code: 'retry-strategy'='fixed_delay' ... (271 chars)]`
Example:
- enable retry on async lookup
`[code: LOOKUP('table'='Customers', 'async'='true', 'retry-predicate'='lookup_ ... (148 chars)]`
- enable retry on sync lookup
`[code: LOOKUP('table'='Customers', 'async'='false', 'retry-predicate'='lookup ... (149 chars)]`
If the lookup source only has one capability, then the 'async' mode option can be omitted:
`[code: LOOKUP('table'='Customers', 'retry-predicate'='lookup_miss', 'retry-st ... (132 chars)]`

#### 4. Enable Custom Data Distribution
By default, the data distribution of Lookup Join's input stream is arbitrary, so sources may not make effective use of caches to accelerate lookups. By enabling custom shuffle as follows, the sources would be able to decide the distribution of the input data on their own and use this prior knowledge to optimize their caches and lookup strategy.
`[code: LOOKUP('table'='Customers', 'shuffle'='true') ... (45 chars)]`
In order to make full use of this feature, the target lookup source should have supported custom shuffle. For connector developers, this could be achieved by having the LookupTableSource subclass implement SupportsLookupCustomShuffle. Even if the source has not provided such support yet, users can still enable this feature first, and then Flink will try best to apply a hash partitioning, which should also bring performance improvement.

#### Further Notes

#### Effect Of Enabling Caching On Retries
FLIP-221 adds caching support for lookup source, which has PARTIAL and FULL caching mode(the mode NONE means disable caching). When FULL caching is enabled, there'll be no retry at all(because it's meaningless to retry lookup via a full cached mirror of lookup source). When PARTIAL caching is enabled, it will lookup from local cache first for a coming record and will do an external lookup via backend connector if cache miss(if cache hit, then return the record immediately), and this will trigger a retry when lookup result is empty(same with caching disabled), the final lookup result is determined when retry completed(in PARTIAL caching mode, it will also update local cache).

#### Note On Lookup Keys And 'retry-predicate'='lookup_miss' Retry Conditions
For different connectors, the index-lookup capability maybe different, e.g., builtin HBase connector can lookup on rowkey only (without secondary index), while builtin JDBC connector can provide more powerful index-lookup capabilities on arbitrary columns, this is determined by the different physical storages. The lookup key mentioned here is the field or combination of fields for the index-lookup, as the example of lookup join, where c.id is the lookup key of the join condition "ON o.customer_id = c.id":
`[code: SELECT o.order_id, o.total, c.country, c.zip ... (145 chars)]`
if we change the join condition to "ON o.customer_id = c.id and c.country = 'US'":
`[code: SELECT o.order_id, o.total, c.country, c.zip ... (166 chars)]`
both c.id and c.country will be used as lookup key when Customers table was stored in MySql:
`[code: CREATE TEMPORARY TABLE Customers ( ... (206 chars)]`
only c.id can be the lookup key when Customers table was stored in HBase, and the remaining join condition c.country = 'US' will be evaluated after lookup result returned
`[code: CREATE TEMPORARY TABLE Customers ( ... (169 chars)]`

Accordingly, the above query will have different retry effects on different storages when enable 'lookup_miss' retry predicate and the fixed-delay retry strategy. e.g., if there is a row in the Customers table:
`[code: id=100, country='CN' ... (20 chars)]`
When processing an record with 'id=100' in the order stream, in 'jdbc' connector, the corresponding lookup result is null (country='CN' does not satisfy the condition c.country = 'US') because both c.id and c.country are used as lookup keys, so this will trigger a retry.
When in 'hbase' connector, only c.id will be used as the lookup key, the corresponding lookup result will not be empty(it will return the record id=100, country='CN'), so it will not trigger a retry (the remaining join condition c.country = 'US' will be evaluated as not true for returned record).

Currently, based on SQL semantic considerations, only the 'lookup_miss' retry predicate is provided, and when it is necessary to wait for delayed updates of the dimension table (where a historical version record already exists in the table, rather than not), users can try two solutions:
- implements a custom retry predicate with the new retry support in DataStream Async I/O (allows for more complex judgments on returned records).
- enable delayed retry by adding another join condition including comparison on some kind of data version generated by timestamp for the above example, assume the Customers table is updated every hour, we can add a new time-dependent version field update_version, which is reserved to hourly precision, e.g., update time '2022-08-15 12:01:02' of record will store the update_version as '2022-08-15 12:00'
`[code: CREATE TEMPORARY TABLE Customers ( ... (281 chars)]`
append an equal condition on both time field of Order stream and Customers.update_version to the join condition:
`[code: ON o.customer_id = c.id AND DATE_FORMAT(o.order_timestamp, 'yyyy-MM-dd ... (97 chars)]`
then we can enable delayed retry when Order's record can not lookup the new record with '12:00' version in Customers table.

#### Trouble Shooting
When turning on the delayed retry lookup, it is more likely to encounter a backpressure problem in the lookup node, this can be quickly confirmed via the 'Thread Dump' on the 'Task Manager' page of web ui. From async and sync lookups respectively, call stack of thread sleep will appear in:
- async lookup: RetryableAsyncLookupFunctionDelegator
- sync lookup: RetryableLookupFunctionDelegator

Note:
- async lookup with retry is not capable for fixed delayed processing for all input data (should use other lighter ways to solve, e.g., pending source consumption or use sync lookup with retry)
- delayed waiting for retry execution in sync lookup is fully synchronous, i.e., processing of the next record does not begin until the current record has completed.
- in async lookup, if 'output-mode' is 'ORDERED' mode, the probability of backpressure caused by delayed retry maybe higher than 'UNORDERED' mode, in which case increasing async 'capacity' may not be effective in reducing backpressure, and it may be necessary to consider reducing the delay duration.

#### Conflict Cases In Join Hints
If the Join Hints conflicts occur, Flink will choose the most matching one.
- First, Join Hints will follow the logic of Flink query hint for resolving conflicts (see: Conflict Cases In Query Hints)
- Conflict in one same Join Hint strategy, Flink will choose the first matching table for a join.
- Conflict in different Join Hints strategies, Flink will choose the first matching hint for a join.
##### Examples
`[code: CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...); ... (1586 chars)]`

### State TTL Hints
Streaming. For stateful computation Regular Join and Group Aggregation, users can use STATE_TTL hint to specify operator-level Idle State Retention Time, which enables the aforementioned operators to have a different TTL against the pipeline level configuration table.exec.state.ttl.

##### Regular Join Examples
`[code: CREATE TABLE orders ( ... (1154 chars)]`

##### Group Aggregation Examples
`[code: -- table name as hint key ... (509 chars)]`

  Note:
- Users can choose either table/view name or table alias as the hint key. However, once the alias is specified, the STATE_TTL must be hinted on the alias.
- For cascade joins, the specified state TTLs will be interpreted as the left and right state TTL for the first join operator and the right state TTL for the second join operator (from a bottom-up order). The left state TTL for the second join operator will be retrieved from the configuration table.exec.state.ttl. If users need to set a specific TTL value for the left state of the second join operator, the query needs to be split into query blocks like
`[code: CREATE TEMPORARY VIEW V AS  ... (169 chars)]`
- STATE_TTL hint only applies on the underlying query block.
- When the STATE_TTL hint key is duplicated, the value is applied from the last occurrence. For example, in cases like SELECT /*+ STATE_TTL('A' = '1d', 'A' = '2d')*/ * FROM ..., the TTL for input A will be taken as 2d.
- When there are multiple STATE_TTL hints appear with duplicated hint key, the value is applied from the first occurrence. For example, in cases like SELECT /*+ STATE_TTL('A' = '1d', 'B' = '2d'), STATE_TTL('C' = '12h', 'A' = '6h')*/ * FROM ..., the TTL for input A will be taken as 1d.

### What are query blocks ?
A query block is a basic unit of SQL. For example, any inline view or sub-query of a SQL statement are considered separate query block to the outer query.

#### Examples
An SQL statement can consist of several sub-queries. The sub-query can be a SELECT, INSERT or DELETE. A sub-query can contain other sub-queries in the FROM clause, the WHERE clause, or a sub-select of a UNION or UNION ALL. For these different sub-queries or view types, they can be composed of several query blocks, For example: The simple query below has just one sub-query, but it has two query blocks - one for the outer SELECT and another for the sub-query SELECT. The query below is a union query, which contains two query blocks - one for the first SELECT and another for the second SELECT. The query below contains a view, and it has two query blocks - one for the outer SELECT and another for the view.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/with/
# WITH clause
Batch / Streaming

WITH provides a way to write auxiliary statements for use in a larger query. These statements, which are often referred to as Common Table Expression (CTE), can be thought of as defining temporary views that exist just for one query.

The syntax of WITH statement is:
`[code: WITH <with_item_definition> [ , ... ] ... (145 chars)]`

The following example defines a common table expression orders_with_total and use it in a GROUP BY query.
`[code: WITH orders_with_total AS ( ... (157 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/
# SELECT & WHERE clause
Batch / Streaming

The general syntax of the SELECT statement is:
`[code: SELECT select_list FROM table_expression [ WHERE boolean_expression ] ... (69 chars)]`

The table_expression refers to any source of data. It could be an existing table, view, VALUES, or VALUE clause, the joined results of multiple existing tables, or a subquery. Assuming that the table is available in the catalog, the following would read all rows from Orders.
`[code: SELECT * FROM Orders ... (20 chars)]`

The select_list specification * means the query will resolve all columns. However, usage of * is discouraged in production because it makes queries less robust to catalog changes. Instead, a select_list can specify a subset of available columns or make calculations using said columns. For example, if Orders has columns named order_id, price, and tax you could write the following query:
`[code: SELECT order_id, price + tax FROM Orders ... (40 chars)]`

Queries can also consume from inline data using the VALUES clause. Each tuple corresponds to one row and an alias may be provided to assign names to each column.
`[code: SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1))  AS t (order_i ... (79 chars)]`

Rows can be filtered based on a WHERE clause.
`[code: SELECT price + tax FROM Orders WHERE id = 10 ... (44 chars)]`

Additionally, built-in and user-defined scalar functions can be invoked on the columns of a single row. User-defined functions must be registered in a catalog before use.
`[code: SELECT PRETTY_PRINT(order_id) FROM Orders ... (41 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select-distinct/
# SELECT DISTINCT
Batch / Streaming

If SELECT DISTINCT is specified, all duplicate rows are removed from the result set (one row is kept from each group of duplicates).
`[code: SELECT DISTINCT id FROM Orders ... (30 chars)]`

For streaming queries, the required state for computing the query result might grow infinitely. State size depends on number of distinct rows. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See query configuration for details

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/
# Windowing table-valued functions (Windowing TVFs)
Batch / Streaming

Windows are at the heart of processing infinite streams. Windows split the stream into "buckets" of finite size, over which we can apply computations. This document focuses on how windowing is performed in Flink SQL and how the programmer can benefit to the maximum from its offered functionality.

Apache Flink provides several window table-valued functions (TVF) to divide the elements of your table into windows, including:
- Tumble Windows
- Hop Windows
- Cumulate Windows
- Session Windows (Only supported in streaming mode now)

Note that each element can logically belong to more than one window, depending on the windowing table-valued function you use. For example, HOP windowing creates overlapping windows wherein a single element can be assigned to multiple windows.

Windowing TVFs are Flink defined Polymorphic Table Functions (abbreviated PTF). PTF is part of the SQL 2016 standard, a special table-function, but can have a table as a parameter. PTF is a powerful feature to change the shape of a table. Because PTFs are used semantically like tables, their invocation occurs in a FROM clause of a SELECT statement.

Windowing TVFs is a replacement of legacy Grouped Window Functions. Windowing TVFs is more SQL standard compliant and more powerful to support complex window-based computations, e.g. Window TopN, Window Join. However, Grouped Window Functions can only support Window Aggregation.

See more how to apply further computations based on windowing TVF:
- Window Aggregation
- Window TopN
- Window Join
- Window Deduplication

## Window Functions

Apache Flink provides 4 built-in windowing TVFs: TUMBLE, HOP, CUMULATE and SESSION. The return value of windowing TVF is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. In streaming mode, the "window_time" field is a time attributes of the window. In batch mode, the "window_time" field is an attribute of type TIMESTAMP or TIMESTAMP_LTZ based on input time field type. The "window_time" field can be used in subsequent time-based operations, e.g. another windowing TVF, or interval joins, over aggregations. The value of window_time always equal to window_end - 1ms.

### TUMBLE
The TUMBLE function assigns each element to a window of specified window size. Tumbling windows have a fixed size and do not overlap. For example, suppose you specify a tumbling window with a size of 5 minutes. In that case, Flink will evaluate the current window, and a new window started every five minutes, as illustrated by the following figure.

The TUMBLE function assigns a window for each row of a relation based on a time attribute field. In streaming mode, the time attribute field must be either event or processing time attributes. In batch mode, the time attribute field of window table function must be an attribute of type TIMESTAMP or TIMESTAMP_LTZ. The return value of TUMBLE is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after window TVF.

TUMBLE function takes three required parameters, one optional parameter:
`[code: TUMBLE(TABLE data, DESCRIPTOR(timecol), size [, offset ]) ... (57 chars)]`
- data: is a table parameter that can be any relation with a time attribute column.
- timecol: is a column descriptor indicating which time attributes column of data should be mapped to tumbling windows.
- size: is a duration specifying the width of the tumbling windows.
- offset: is an optional parameter to specify the offset which window start would be shifted by.

Here is an example invocation on the Bid table:
`[code: -- tables must have time attribute, e.g. `bidtime` in this table ... (3002 chars)]`

Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. 2020-04-15 08:05 should be displayed as 2020-04-15 08:05:00.000 in Flink SQL Client if the type is TIMESTAMP(3).

### HOP
The HOP function assigns elements to windows of fixed length. Like a TUMBLE windowing function, the size of the windows is configured by the window size parameter. An additional window slide parameter controls how frequently a hopping window is started. Hence, hopping windows can be overlapping if the slide is smaller than the window size. In this case, elements are assigned to multiple windows. Hopping windows are also known as "sliding windows".

For example, you could have windows of size 10 minutes that slides by 5 minutes. With this, you get every 5 minutes a window that contains the events that arrived during the last 10 minutes, as depicted by the following figure.

The HOP function assigns windows that cover rows within the interval of size and shifting every slide based on a time attribute field. In streaming mode, the time attribute field must be either event or processing time attributes. In batch mode, the time attribute field of window table function must be an attribute of type TIMESTAMP or TIMESTAMP_LTZ. The return value of HOP is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after windowing TVF.

HOP takes four required parameters, one optional parameter:
`[code: HOP(TABLE data, DESCRIPTOR(timecol), slide, size [, offset ]) ... (61 chars)]`
- data: is a table parameter that can be any relation with an time attribute column.
- timecol: is a column descriptor indicating which time attributes column of data should be mapped to hopping windows.
- slide: is a duration specifying the duration between the start of sequential hopping windows
- size: is a duration specifying the width of the hopping windows.
- offset: is an optional parameter to specify the offset which window start would be shifted by.

Here is an example invocation on the Bid table:
`[code: > SELECT * FROM HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUT ... (2601 chars)]`

### CUMULATE
Cumulating windows are very useful in some scenarios, such as tumbling windows with early firing in a fixed window interval. For example, a daily dashboard draws cumulative UVs from 00:00 to every minute, the UV at 10:00 represents the total number of UV from 00:00 to 10:00. This can be easily and efficiently implemented by CUMULATE windowing.

The CUMULATE function assigns elements to windows that cover rows within an initial interval of step size and expand to one more step size (keep window start fixed) every step until the max window size. You can think CUMULATE function as applying TUMBLE windowing with max window size first, and split each tumbling windows into several windows with same window start and window ends of step-size difference. So cumulating windows do overlap and don't have a fixed size.

For example, you could have a cumulating window for 1 hour step and 1 day max size, and you will get windows: [00:00, 01:00), [00:00, 02:00), [00:00, 03:00), …, [00:00, 24:00) for every day.

The CUMULATE functions assigns windows based on a time attribute column. In streaming mode, the time attribute field must be either event or processing time attributes. In batch mode, the time attribute field of window table function must be an attribute of type TIMESTAMP or TIMESTAMP_LTZ. The return value of CUMULATE is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after window TVF.

CUMULATE takes four required parameters, one optional parameter:
`[code: CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size) ... (53 chars)]`
- data: is a table parameter that can be any relation with an time attribute column.
- timecol: is a column descriptor indicating which time attributes column of data should be mapped to cumulating windows.
- step: is a duration specifying the increased window size between the end of sequential cumulating windows.
- size: is a duration specifying the max width of the cumulating windows. size must be an integral multiple of step.
- offset: is an optional parameter to specify the offset which window start would be shifted by.

Here is an example invocation on the Bid table:
`[code: > SELECT * FROM  ... (3339 chars)]`

### SESSION
  Note:
- Session Window TVF is not supported in batch mode now.
- Session Window Aggregation does not support any optimization in Performance Tuning now.
- Session Window Join, Session Window TopN and Session Window Deduplication are conceptually supported and in beta mode. Issues can be reported in JIRA.

The SESSION function groups elements by sessions of activity. In contrast to TUMBLE windows and HOP windows, session windows do not overlap and do not have a fixed start and end time. Instead, a session window closes when it doesn't receive elements for a certain period of time, i.e., when a gap of inactivity occurred. A session window should be configured with a static session gap which defines how long the period of inactivity is. When this period expires, the current session closes and subsequent elements are assigned to a new session window.

For example, you could have windows of gap 10 minutes. With this, when the interval between two events of the same user is less than 10 minutes, these events will be grouped into the same session window. If there is no data after 10 minutes following the latest event, then this session window will close and be sent downstream. Subsequent events will be assigned to a new session window.

The SESSION function assigns windows that cover rows based on datetime. In streaming mode, the time attribute field must be either event or processing time attributes. The return value of SESSION is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after windowing TVF.

SESSION takes three required parameters and one optional parameter:
`[code: SESSION(TABLE data [PARTITION BY(keycols, ...)], DESCRIPTOR(timecol),  ... (74 chars)]`
- data: is a table parameter that can be any relation with an time attribute column.
- keycols: is a column descriptor indicating which columns should be used to partition the data prior to session windows.
- timecol: is a column descriptor indicating which time attributes column of data should be mapped to session windows.
- gap: is the maximum interval in timestamp for two events to be considered part of the same session window.

Here is an example invocation on the Bid table:
`[code: -- tables must have time attribute, e.g. `bidtime` in this table ... (4854 chars)]`

## Window Offset

Offset is an optional parameter which could be used to change the window assignment. It could be positive duration and negative duration. Default values for window offset is 0. The same record maybe assigned to the different window if set different offset value.

For example, which window would be assigned to for a record with timestamp 2021-06-30 00:00:04 for a Tumble window with 10 MINUTE as size?
- If offset value is -16 MINUTE, the record assigns to window [2021-06-29 23:54:00, 2021-06-30 00:04:00).
- If offset value is -6 MINUTE, the record assigns to window [2021-06-29 23:54:00, 2021-06-30 00:04:00).
- If offset is -4 MINUTE, the record assigns to window [2021-06-29 23:56:00, 2021-06-30 00:06:00).
- If offset is 0, the record assigns to window [2021-06-30 00:00:00, 2021-06-30 00:10:00).
- If offset is 4 MINUTE, the record assigns to window [2021-06-29 23:54:00, 2021-06-30 00:04:00).
- If offset is 6 MINUTE, the record assigns to window [2021-06-29 23:56:00, 2021-06-30 00:06:00).
- If offset is 16 MINUTE, the record assigns to window [2021-06-29 23:56:00, 2021-06-30 00:06:00).
We could find that, some windows offset parameters may have same effect on the assignment of windows. In the above case, -16 MINUTE, -6 MINUTE and 4 MINUTE have same effect for a Tumble window with 10 MINUTE as size.

Note: The effect of window offset is just for updating window assignment, it has no effect on Watermark.

We show an example to describe how to use offset in Tumble window in the following SQL.
`[code: -- NOTE: Currently Flink doesn't support evaluating individual window  ... (2253 chars)]`

Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. 2020-04-15 08:05 should be displayed as 2020-04-15 08:05:00.000 in Flink SQL Client if the type is TIMESTAMP(3).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/model-inference/
# Model Inference
Streaming

Flink SQL provides the ML_PREDICT table-valued function (TVF) to perform model inference in SQL queries. This function allows you to apply machine learning models to your data streams directly in SQL. See Model Creation about how to create a model.

## ML_PREDICT Function

The ML_PREDICT function takes a table input, applies a model to it, and returns a new table with the model's predictions. The function offers support for synchronous/asynchronous inference modes when the underlying model permits both.

### Syntax
`[code: SELECT * FROM ... (133 chars)]`

### Parameters
- input_table: The input table containing the data to be processed
- model_name: The name of the model to use for inference
- feature_columns: A descriptor specifying which columns from the input table should be used as features for the model
- config: (Optional) A map of configuration options for the model inference

### Configuration Options
The following configuration options can be specified in the config map:
[table: 5 rows]

### Example
`[code: -- Basic usage ... (478 chars)]`

### Output
The output table contains all columns from the input table plus the model's prediction columns. The prediction columns are added based on the model's output schema.

### Notes
- The model must be registered in the catalog before it can be used with ML_PREDICT.
- The number of feature columns specified in the descriptor must match the model's input schema.
- If column names in the output conflict with existing column names in the input table, an index will be added to the output column names to avoid conflicts. For example, if the output column is named prediction, it will be renamed to prediction0 if a column with that name already exists in the input table.
- For asynchronous inference, the model provider must support the AsyncPredictRuntimeProvider interface.
- ML_PREDICT only supports append-only tables. CDC (Change Data Capture) tables are not supported because ML_PREDICT results are non-deterministic.

### Model Provider
The ML_PREDICT function uses a ModelProvider to perform the actual model inference. The provider is looked up based on the provider identifier specified when registering the model. There are two types of model providers:
- PredictRuntimeProvider: For synchronous model inference. Implements the createPredictFunction method to create a synchronous prediction function. Used when async is set to false in the config.
- AsyncPredictRuntimeProvider: For asynchronous model inference. Implements the createAsyncPredictFunction method to create an asynchronous prediction function. Used when async is set to true in the config. Requires additional configuration for timeout and buffer capacity.

If async is not set in the config, the system will pick either sync or async model provider and prefer async model provider if both exist.

### Error Handling
The function will throw an exception in the following cases:
- The model does not exist in the catalog
- The number of feature columns does not match the model's input schema
- The model parameter is missing
- Too few or too many arguments are provided

### Performance Considerations
- For high-throughput scenarios, consider using asynchronous inference mode.
- Configure appropriate timeout and buffer capacity values for asynchronous inference.
- The function's performance depends on the underlying model provider implementation.

### Related Statements
- Model Creation
- Model Alteration

### Supported Model Providers
Flink currently supports the following model providers:
- OpenAI: For calling OpenAI API services. See OpenAI Model Documentation for details.
- Triton: For calling NVIDIA Triton Inference Server. See Triton Model Documentation for details.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/vector-search/
# Vector Search
Batch / Streaming

Flink SQL provides the VECTOR_SEARCH table-valued function (TVF) to perform a vector search in SQL queries. This function allows you to search similar rows according to the high-dimension vectors.

## VECTOR_SEARCH Function

The VECTOR_SEARCH uses a processing-time attribute to correlate rows to the latest version of data in an external table. It's very similar to a lookup join in Flink SQL, however, the difference is VECTOR_SEARCH uses the input data vector to compare the similarity with data in the external table and return the top-k most similar rows.

### Syntax
`[code: SELECT *  ... (191 chars)]`

### Parameters
- input_table: The input table containing the data to be processed
- vector_table: The name of external table that allows searching via vector
- vector_column: The name of the column in the input table, its type should be FLOAT ARRAY or DOUBLE ARRAY
- index_column: A descriptor specifying which column from the vector table should be used to compare the similarity with the input data
- top_k: The number of top-k most similar rows to return
- config: (Optional) A map of configuration options for the vector search

### Configuration Options
The following configuration options can be specified in the config map:
[table: 5 rows]

### Example
`[code: -- Basic usage ... (813 chars)]`

### Output
The output table contains all columns from the input table, the vector search table columns and a column named score to indicate the similarity between the input row and matched row.

### Notes
- The implementation of the vector table must implement interface org.apache.flink.table.connector.source.VectorSearchTableSource. Please refer to Vector Search Table Source for details.
- VECTOR_SEARCH only supports to consume append-only tables.
- VECTOR_SEARCH does not require the LATERAL keyword when the function call has no correlation with other tables. For example, if the search column is a constant or literal value, LATERAL can be omitted.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/
# Window Aggregation

## Window TVF Aggregation
Batch / Streaming

Window aggregations are defined in the GROUP BY clause contains "window_start" and "window_end" columns of the relation applied Windowing TVF. Just like queries with regular GROUP BY clauses, queries with a group by window aggregation will compute a single result row per group.
`[code: SELECT ... ... (105 chars)]`

Unlike other aggregations on continuous tables, window aggregation do not emit intermediate results but only a final result, the total aggregation at the end of the window. Moreover, window aggregations purge all intermediate state when no longer needed.

### Windowing TVFs

Flink supports TUMBLE, HOP, CUMULATE and SESSION types of window aggregations. In streaming mode, the time attribute field of a window table-valued function must be on either event or processing time attributes. See Windowing TVF for more windowing functions information. In batch mode, the time attribute field of a window table-valued function must be an attribute of type TIMESTAMP or TIMESTAMP_LTZ.

  Note: SESSION Window Aggregation is not supported in batch mode now.

Here are some examples for TUMBLE, HOP, CUMULATE and SESSION window aggregations.
`[code: -- tables must have time attribute, e.g. `bidtime` in this table ... (4876 chars)]`

Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. 2020-04-15 08:05 should be displayed as 2020-04-15 08:05:00.000 in Flink SQL Client if the type is TIMESTAMP(3).

### GROUPING SETS

Window aggregations also support GROUPING SETS syntax. Grouping sets allow for more complex grouping operations than those describable by a standard GROUP BY. Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple GROUP BY clauses.

Window aggregations with GROUPING SETS require both the window_start and window_end columns have to be in the GROUP BY clause, but not in the GROUPING SETS clause.
`[code: Flink SQL> SELECT window_start, window_end, supplier_id, SUM(price) AS ... (903 chars)]`

Each sublist of GROUPING SETS may specify zero or more columns or expressions and is interpreted the same way as though used directly in the GROUP BY clause. An empty grouping set means that all rows are aggregated down to a single group, which is output even if no input rows were present.

References to the grouping columns or expressions are replaced by null values in result rows for grouping sets in which those columns do not appear.

#### ROLLUP
ROLLUP is a shorthand notation for specifying a common type of grouping set. It represents the given list of expressions and all prefixes of the list, including the empty list. Window aggregations with ROLLUP requires both the window_start and window_end columns have to be in the GROUP BY clause, but not in the ROLLUP clause. For example, the following query is equivalent to the one above.
`[code: SELECT window_start, window_end, supplier_id, SUM(price) AS total_pric ... (195 chars)]`

#### CUBE
CUBE is a shorthand notation for specifying a common type of grouping set. It represents the given list and all of its possible subsets - the power set. Window aggregations with CUBE requires both the window_start and window_end columns have to be in the GROUP BY clause, but not in the CUBE clause. For example, the following two queries are equivalent.
`[code: SELECT window_start, window_end, item, supplier_id, SUM(price) AS tota ... (519 chars)]`

### Selecting Group Window Start and End Timestamps
The start and end timestamps of group windows can be selected with the grouped window_start and window_end columns.

### Cascading Window Aggregation
The window_start and window_end columns are regular timestamp columns, not time attributes. Thus they can't be used as time attributes in subsequent time-based operations. In order to propagate time attributes, you need to additionally add window_time column into GROUP BY clause. The window_time is the third column produced by Windowing TVFs which is a time attribute of the assigned window. Adding window_time into GROUP BY clause makes window_time also to be group key that can be selected. Then following queries can use this column for subsequent time-based operations, such as cascading window aggregations and Window TopN.

The following shows a cascading window aggregation where the first window aggregation propagates the time attribute for the second window aggregation.
`[code: -- tumbling 5 minutes for each supplier_id ... (810 chars)]`

## Group Window Aggregation
Batch / Streaming

  Warning: Group Window Aggregation is deprecated. It's encouraged to use Window TVF Aggregation which is more powerful and effective.

Compared to Group Window Aggregation, Window TVF Aggregation have many advantages, including:
- Have all performance optimizations mentioned in Performance Tuning.
- Support standard GROUPING SETS syntax.
- Can apply Window TopN after window aggregation result.
- and so on.

Group Window Aggregations are defined in the GROUP BY clause of a SQL query. Just like queries with regular GROUP BY clauses, queries with a GROUP BY clause that includes a group window function compute a single result row per group. The following group windows functions are supported for SQL on batch and streaming tables.

### Group Window Functions
[table: 4 rows]

### Time Attributes
In streaming mode, the time_attr argument of the group window function must refer to a valid time attribute that specifies the processing time or event time of rows. See the documentation of time attributes to learn how to define time attributes. In batch mode, the time_attr argument of the group window function must be an attribute of type TIMESTAMP.

### Selecting Group Window Start and End Timestamps
The start and end timestamps of group windows as well as time attributes can be selected with the following auxiliary functions:
[table: 5 rows]

Note: Auxiliary functions must be called with exactly same arguments as the group window function in the GROUP BY clause.

The following examples show how to specify SQL queries with group windows on streaming tables.
`[code: CREATE TABLE Orders ( ... (339 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/changelog/
# Changelog Conversion
Streaming

Flink SQL provides built-in process table functions (PTFs) for working with changelog streams.
[table: 3 rows]

## FROM_CHANGELOG

The FROM_CHANGELOG PTF converts an append-only table with an explicit operation code column into a (potentially updating) dynamic table. Each input row is expected to have a string column that indicates the change operation. The operation column is interpreted by the engine and removed from the output.

This is useful when consuming Change Data Capture (CDC) streams from systems like Debezium where events arrive as flat append-only records with an explicit operation field. It's also useful to be used in combination with the TO_CHANGELOG function, when converting the append-only table back into an updating table after doing some specific transformation to the events.

Note: This version requires that your CDC data encodes updates using a full image (i.e. providing separate events for before and after the update). Please double-check whether your source provides both UPDATE_BEFORE and UPDATE_AFTER events. FROM_CHANGELOG is a very powerful function but might produce incorrect results in subsequent operations and tables, if not configured correctly.

### Syntax
`[code: SELECT * FROM FROM_CHANGELOG( ... (228 chars)]`

### Parameters
[table: 4 rows]

#### Default op_mapping
When op_mapping is omitted, the following standard names are used. They allow a reverse conversion from TO_CHANGELOG by default.
[table: 5 rows]

Any input row whose op code is not present in the active mapping (default or user-defined) fails the job at runtime with a TableRuntimeException.

### Output Schema
The output contains all input columns except the operation code (e.g., op) column, which is interpreted by Flink's SQL engine and removed. Each output row carries the appropriate change operation (INSERT, UPDATE_BEFORE, UPDATE_AFTER, or DELETE).
`[code: [all_input_columns_without_op] ... (30 chars)]`

### Examples

#### Basic usage with standard op names
`[code: -- Input (append-only): ... (558 chars)]`

#### Custom operation column name
`[code: -- Source schema: id INT, operation STRING, name STRING ... (212 chars)]`

#### Table API
`[code: Table cdcStream = ...; ... (495 chars)]`

## TO_CHANGELOG

The TO_CHANGELOG PTF converts a dynamic table (i.e. an updating table) into an append-only table with an explicit operation code column. Each input row - regardless of its original change operation (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) - is emitted as an INSERT-only row with a string column indicating the original operation.

This is useful when you need to materialize changelog events into a downstream system that only supports appends (e.g., a message queue, log store, or append-only file sink). It is also useful to filter out certain types of updates, for example DELETEs.

### Syntax
`[code: SELECT * FROM TO_CHANGELOG( ... (155 chars)]`

### Parameters
[table: 4 rows]

#### Default op_mapping
When op_mapping is omitted, all four change operations are mapped to their standard names:
[table: 5 rows]

### Output Schema
The output columns are ordered as:
`[code: [op_column, all_input_columns] ... (30 chars)]`
All output rows have INSERT - the table is always append-only.

### Examples

#### Basic usage
`[code: -- Input: retract table from an aggregation ... (351 chars)]`

#### Custom operation column name
`[code: SELECT * FROM TO_CHANGELOG( ... (150 chars)]`

#### Custom operation codes with filtering
`[code: SELECT * FROM TO_CHANGELOG( ... (306 chars)]`

#### Deletion flag pattern
`[code: SELECT * FROM TO_CHANGELOG( ... (296 chars)]`

#### Table API
`[code: // Default: adds 'op' column and supports all changelog modes ... (541 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/
# Group Aggregation
Batch / Streaming

Like most data systems, Apache Flink supports aggregate functions; both built-in and user-defined. User-defined functions must be registered in a catalog before use.

An aggregate function computes a single result from multiple input rows. For example, there are aggregates to compute the COUNT, SUM, AVG (average), MAX (maximum) and MIN (minimum) over a set of rows.
`[code: SELECT COUNT(*) FROM Orders ... (27 chars)]`

For streaming queries, it is important to understand that Flink runs continuous queries that never terminate. Instead, they update their result table according to the updates on its input tables. For the above query, Flink will output an updated count each time a new row is inserted into the Orders table.

Apache Flink supports the standard GROUP BY clause for aggregating data.
`[code: SELECT COUNT(*) ... (45 chars)]`

For streaming queries, the required state for computing the query result might grow infinitely. State size depends on the number of groups and the number and type of aggregation functions. For example MIN/MAX are heavy on state size while COUNT is cheap. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See query configuration for details.

Apache Flink provides a set of performance tuning ways for Group Aggregation, see more Performance Tuning.

## DISTINCT Aggregation

Distinct aggregates remove duplicate values before applying an aggregation function. The following example counts the number of distinct order_ids instead of the total number of rows in the Orders table.
`[code: SELECT COUNT(DISTINCT order_id) FROM Orders ... (43 chars)]`

For streaming queries, the required state for computing the query result might grow infinitely. State size is mostly depends on the number of distinct rows and the time that a group is maintained, short lived group by windows are not a problem. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See query configuration for details.

## GROUPING SETS

Grouping sets allow for more complex grouping operations than those describable by a standard GROUP BY. Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple GROUP BY clauses.
`[code: SELECT supplier_id, rating, COUNT(*) AS total ... (305 chars)]`
Results:
`[code: +-------------+--------+-------+ ... (362 chars)]`

Each sublist of GROUPING SETS may specify zero or more columns or expressions and is interpreted the same way as though it was used directly in the GROUP BY clause. An empty grouping set means that all rows are aggregated down to a single group, which is output even if no input rows were present.

References to the grouping columns or expressions are replaced by null values in result rows for grouping sets in which those columns do not appear.

For streaming queries, the required state for computing the query result might grow infinitely. State size depends on number of group sets and type of aggregation functions. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See query configuration for details.

### ROLLUP
ROLLUP is a shorthand notation for specifying a common type of grouping set. It represents the given list of expressions and all prefixes of the list, including the empty list. For example, the following query is equivalent to the one above.
`[code: SELECT supplier_id, rating, COUNT(*) ... (268 chars)]`

### CUBE
CUBE is a shorthand notation for specifying a common type of grouping set. It represents the given list and all of its possible subsets - the power set. For example, the following two queries are equivalent.
`[code: SELECT supplier_id, rating, product_id, COUNT(*) ... (888 chars)]`

## HAVING

HAVING eliminates group rows that do not satisfy the condition. HAVING is different from WHERE: WHERE filters individual rows before the GROUP BY while HAVING filters group rows created by GROUP BY. Each column referenced in condition must unambiguously reference a grouping column unless it appears within an aggregate function.
`[code: SELECT SUM(amount) ... (69 chars)]`

The presence of HAVING turns a query into a grouped query even if there is no GROUP BY clause. It is the same as what happens when the query contains aggregate functions but no GROUP BY clause. The query considers all selected rows to form a single group, and the SELECT list and HAVING clause can only reference table columns from within aggregate functions. Such a query will emit a single row if the HAVING condition is true, zero rows if it is not true.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/over-agg/
# Over Aggregation
Batch / Streaming

OVER aggregates compute an aggregated value for every input row over a range of ordered rows. In contrast to GROUP BY aggregates, OVER aggregates do not reduce the number of result rows to a single row for every group. Instead OVER aggregates produce an aggregated value for every input row.

The following query computes for every order the sum of amounts of all orders for the same product that were received within one hour before the current order.
`[code: SELECT order_id, order_time, amount, ... (212 chars)]`

The syntax for an OVER window is summarized below.
`[code: SELECT ... (158 chars)]`

You can define multiple OVER window aggregates in a SELECT clause. However, for streaming queries, the OVER windows for all aggregates must be identical due to current limitation.

### ORDER BY
OVER windows are defined on an ordered sequence of rows. Since tables do not have an inherent order, the ORDER BY clause is mandatory. For streaming queries, Flink currently supports OVER windows that are defined with an ascending time attribute or ascending non-time attribute. Additional orderings are not supported.

### PARTITION BY
OVER windows can be defined on a partitioned table. In presence of a PARTITION BY clause, the aggregate is computed for each input row only over the rows of its partition.

### Range Definitions
The range definition specifies how many rows are included in the aggregate. The range is defined with a BETWEEN clause that defines a lower and an upper boundary. All rows between these boundaries are included in the aggregate. Flink only supports CURRENT ROW as the upper boundary.

There are two options to define the range, ROWS intervals and RANGE intervals.

#### RANGE intervals
A RANGE interval is defined on the values of the ORDER BY column, which (in case of Flink) is either a time or non-time attribute.
The following RANGE interval defines that all rows with a time attribute of at most 30 minutes less than the current row are included in the aggregate.
`[code: RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW ... (60 chars)]`
The following RANGE interval defines that all rows with a non-time attribute of unbounded rows preceding the current row are included in the aggregate.
`[code: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ... (49 chars)]`

#### ROW intervals
A ROWS interval is a count-based interval. It defines exactly how many rows are included in the aggregate. The following ROWS interval defines that the 10 rows preceding the current row and the current row (so 11 rows in total) are included in the aggregate.
`[code: ROWS BETWEEN 10 PRECEDING AND CURRENT ROW ... (48 chars)]`

The WINDOW clause can be used to define an OVER window outside of the SELECT clause. It can make queries more readable and also allows us to reuse the window definition for multiple aggregates.
`[code: SELECT order_id, order_time, amount, ... (239 chars)]`
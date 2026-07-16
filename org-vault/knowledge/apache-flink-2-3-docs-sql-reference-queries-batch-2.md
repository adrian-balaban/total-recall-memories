---
title: 'Apache Flink 2.3 docs — SQL Reference: Queries (batch 2)'
tags: [org, flink, flink-2.3, docs, sql, sql-reference, queries, joins, match-recognize, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:40:16.777Z'
updated: '2026-07-07T19:40:16.777Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL Reference — Queries (batch 2). Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`. Covers: joins, window-join, set-ops, orderby, limit, topn, window-topn, deduplication, window-deduplication, match_recognize, time-travel.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/
# Joins

Flink SQL supports complex and flexible join operations over dynamic tables. By default, the order of joins is not optimized; tables are joined in the order specified in FROM. Order tables lowest update frequency first, highest last. Avoid cross joins (Cartesian product) — not supported, cause query to fail.

## Regular Joins
Most generic: any new record or change to either side is visible and affects the whole join result. Streaming: flexible grammar, supports insert/update/delete input. Requires keeping both sides in state forever — state grows infinitely. Use state TTL (affects correctness).

### INNER Equi-JOIN
Cartesian product restricted by join condition. Only equi-joins supported (≥1 conjunctive equality predicate). No cross/theta joins.
`[code: SELECT * ... (73 chars)]`

### OUTER Equi-JOIN
All qualified rows plus one copy of each outer-table row that didn't match. Supports LEFT/RIGHT/FULL. Only equi-joins.
`[code: SELECT * ... (227 chars)]`

### Multiple Regular Joins
If facing performance issues with chained joins generating lots of state, use MultiJoin operator (see tuning multiple regular joins).

## Interval Joins
Cartesian product restricted by join condition AND a time constraint. Requires ≥1 equi-join predicate + a time-bounding condition on both sides: range predicates (<,<=,>=,>), BETWEEN, or single equality on same-type time attributes (processing or event time).
Example: orders joined with shipments if shipped 4 hours after order received.
`[code: SELECT * ... (132 chars)]`
Valid interval join predicates:
- ltime = rtime
- ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE
- ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND
Streaming: only append-only tables with time attributes. Since time attributes are quasi-monotonic, Flink can remove old values from state without affecting correctness.

## Temporal Joins
A temporal table evolves over time (dynamic table). Rows associated with temporal periods; all Flink tables are temporal. Contains versioned snapshots — changing history (changelog, all snapshots) or changing dimensioned (latest snapshot).

### Event Time Temporal Join
Join against a versioned table — enrich with changing metadata, retrieve value at a point in time. Probe (left) correlated to build-side (right) version. Uses SQL:2011 `FOR SYSTEM_TIME AS OF`.
`[code: SELECT [column_list] ... (176 chars)]`
Event-time rowtime attribute retrieves key value as it was in the past. Versioned table stores all versions since last watermark, identified by time.
Example: orders with prices in different currencies normalized to USD using rate at order time.
`[code: -- Create a table of orders ... (1281 chars)]`
Notes:
- Triggered by watermark from BOTH left and right sides. INTERVAL subtraction waits for late events. Ensure both sides set watermark correctly.
- Requires primary key contained in the equivalence condition (e.g. currency_rates.currency must be in orders.currency = currency_rates.currency).
- Previous results not affected by build-side changes (unlike regular joins).
- No time window (unlike interval joins). Probe rows join build version at time attribute. Old versions pruned from state as time passes.

### Processing Time Temporal Join
Processing-time attribute correlates rows to latest version of key in external versioned table. Always returns most up-to-date value. Think of lookup table as HashMap<K,V> of latest records. Lets Flink work against external systems directly when materializing isn't feasible.
Example: append-only orders joined with LatestRates dimension table (HBase). At 10:15, 10:30 rates equal; at 10:52 Euro changes 114→116.
`[code: amount currency     rate   amount*rate ... (277 chars)]`
Currently FOR SYSTEM_TIME AS OF with latest version of any view/table not supported — use temporal table function syntax:
`[code: SELECT ... (108 chars)]`
Note: semantic reason for not supporting FOR SYSTEM_TIME AS OF on latest table/view — join processing doesn't wait for complete snapshot of temporal table, could mislead. Processing-time temporal table function has same issue but supported for compatibility.
Result not deterministic for processing-time. Most often used to enrich stream with external dimension table.

### Temporal Table Function Join
Syntax same as Join with Table Function. Only inner join and left outer join with temporal tables supported.
`[code: SELECT ... (108 chars)]`
Differences between Temporal Table DDL and Temporal Table Function:
- DDL definable in SQL; function cannot.
- Both support temporal join versioned table; only function can temporal join latest version of any table/view.

## Lookup Join
Enrich a table with data queried from external system. Requires one table to have a processing time attribute, the other backed by a lookup source connector. Uses Processing Time Temporal Join syntax with right table backed by lookup source.
`[code: -- Customers is backed by the JDBC connector ... (480 chars)]`
FOR SYSTEM_TIME AS OF + processing time ensures each Orders row joined with Customers rows matching at processing time; prevents join result updating when joined Customer updated later. Requires mandatory equality join predicate (e.g. o.customer_id = c.id).

## Array, Multiset and Map Expansion
Unnest returns new row for each element. Supports CROSS JOIN and LEFT JOIN.
`[code: -- Returns a new row for each element in a constant array ... (434 chars)]`
WITH ORDINALITY supported (CROSS JOIN only, not LEFT JOIN). Returns element + 1-indexed position. Array order guaranteed; maps/multisets unordered.
`[code: -- Returns a new row for each element in a constant array and its posi ... (761 chars)]`
`[code: -- Returns a new row each key/value pair in the map ... (1305 chars)]`

## Table Function
Joins table with results of a table function. Each left row joined with all rows from corresponding call. UDTFs must be registered.
### INNER JOIN
Left row dropped if table function call returns empty.
`[code: SELECT order_id, res ... (76 chars)]`
### LEFT OUTER JOIN
If table function returns empty, outer row preserved with nulls. Left outer join against lateral table requires TRUE literal in ON clause.
`[code: SELECT order_id, res ... (101 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-join/
# Window Join

Adds time dimension into join criteria. Joins elements of two streams sharing a common key and in the same window. Semantics same as DataStream window join. Streaming: doesn't emit intermediate results, only final results at window end; purges state when no longer needed. Usually used with Windowing TVF. Can follow Window Aggregation, Window TopN, Window Join.
Note: SESSION Window Join not supported in batch mode.
Requires join condition contains window starts equality AND window ends equality of input tables. Supports INNER/LEFT/RIGHT/FULL OUTER/ANTI/SEMI JOIN.

## INNER/LEFT/RIGHT/FULL OUTER
`[code: SELECT ... ... (170 chars)]`
Example: Tumble Window Join. Scoping into 5-min intervals chops datasets into [12:00,12:05) and [12:05,12:10). L2 and R2 can't join — separate windows.
`[code: Flink SQL> desc LeftTable; ... (3125 chars)]`

## SEMI
Returns left row if ≥1 matching right row within common window.
`[code: Flink SQL> SELECT * ... (1775 chars)]`

## ANTI
Obverse of Inner Window Join — all unjoined rows within each common window.
`[code: Flink SQL> SELECT * ... (1975 chars)]`

## Limitation
### Join clause
Requires window starts equality AND window ends equality. Future: simplify to window start equality only for TUMBLE/HOP.
### Windowing TVFs of inputs
Windowing TVFs must be the same on left and right. Future: e.g. tumbling join sliding with same size.
### Window Join following Windowing TVFs directly
If follows directly, TVF must be Tumble/Hop/Cumulate, not Session.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/
# Set Operations

## UNION
UNION and UNION ALL return rows found in either table. UNION takes distinct rows; UNION ALL keeps duplicates.
`[code: Flink SQL> create view t1(s) as values ('c'),('a'),('b'),('b'),('c ... (403 chars)]`

## INTERSECT
Rows in both tables. INTERSECT distinct; INTERSECT ALL keeps duplicates.
`[code: Flink SQL> (SELECT s FROM t1) INTERSECT (SELECT s FROM t2); ... (202 chars)]`

## EXCEPT
Rows in one table but not other. EXCEPT distinct; EXCEPT ALL keeps duplicates.
`[code: Flink SQL> (SELECT s FROM t1) EXCEPT (SELECT s FROM t2); ... (184 chars)]`

## IN
True if expression exists in table sub-query (sub-query must have one column of same type). Optimizer rewrites to join+group. Streaming state grows infinitely; use TTL (affects correctness).
`[code: SELECT user, amount ... (88 chars)]`

## EXISTS
True if sub-query returns ≥1 row. Only supported if rewritable to join+group.
`[code: SELECT user, amount ... (92 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/
# ORDER BY clause
Sorts result rows by specified expressions. Equal rows compared by next expression. Streaming: primary sort order must be ascending on a time attribute; subsequent orders freely chosen. Batch: no such limitation.
`[code: SELECT * ... (50 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/limit/
# LIMIT clause
Batch only. Constrains number of rows. Generally used with ORDER BY for deterministic results.
`[code: SELECT * ... (47 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/topn/
# Top-N
N smallest or largest values ordered by columns. Useful for N bottom/top records. Uses OVER window + filter condition; PARTITION BY supports per-group Top-N. Supported batch and streaming.
`[code: SELECT [column_list], ... (171 chars)]`
Parameters:
- ROW_NUMBER(): unique sequential number per row per partition. Currently only ROW_NUMBER supported (RANK/DENSE_RANK future).
- PARTITION BY col1[,...]: partition columns; each partition has a Top-N result.
- ORDER BY col1 [asc|desc][,...]: ordering columns; directions can differ.
- WHERE rownum <= N: required for Flink to recognize Top-N.
- [AND conditions]: other conditions combined with rownum <= N via AND only.
Note: pattern must be followed exactly or optimizer won't translate.
Result Updating: Flink sorts input stream; changed top-N records sent as retraction/update. Recommend update-capable sink. Unique keys = partition columns + rownum. Can also derive upstream unique key. Example: top 5 products per category by max sales in realtime.
`[code: CREATE TABLE ShopSales ( ... (272 chars)]`

#### No Ranking Output Optimization
rownum field as unique key causes many writes (e.g. rank 9→1 writes ranks 1-9 as updates). Optimization: omit rownum in outer SELECT. Top N is usually small; consumers can sort. Only changed record sent downstream — reduces IO.
`[code: CREATE TABLE ShopSales ( ... (350 chars)]`
Attention: external storage must have same unique key as Top-N query (e.g. product_id).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/
# Window Top-N
Special Top-N returning N smallest/largest per window + partitioned keys. Streaming: no intermediate results, only final result at window end; purges state. Better performance when results-per-record not needed. Used with Windowing TVF directly, or after Window Aggregation/TopN/Join.
Note: SESSION Window Top-N not supported in batch mode.
Same syntax as regular Top-N, but PARTITION BY must contain window_start and window_end columns of Windowing TVF/Aggregation relation. Else optimizer won't translate.
`[code: SELECT [column_list] ... (284 chars)]`
## Example
### After Window Aggregation
Top 3 suppliers with highest sales per tumbling 10-min window.
`[code: -- tables must have time attribute ... (2699 chars)]`
### After Windowing TVF
Top 3 items with highest price per tumbling 10-min window.
`[code: Flink SQL> SELECT * ... (1252 chars)]`
## Limitation
Only supports Window Top-N after Windowing TVF with Tumble/Hop/Cumulate. Session support coming.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/deduplication/
# Deduplication
Removes rows duplicating over a set of columns, keeping first or last. Useful when upstream ETL not end-to-end exactly-once causing duplicate sink records on failover; duplicates affect SUM/COUNT correctness. Uses ROW_NUMBER() like Top-N; theory: special case of Top-N where N=1, ordered by processing/event time.
`[code: SELECT [column_list] ... (129 chars)]`
Parameters:
- ROW_NUMBER(): unique sequential number.
- PARTITION BY col1[,...]: deduplicate key.
- ORDER BY time_attr [asc|desc]: ordering column must be time attribute (processing or event). ASC = keep first; DESC = keep last.
- WHERE rownum = 1: required for Flink to recognize deduplication.
Note: pattern must be followed exactly.
`[code: CREATE TABLE Orders ( ... (451 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-deduplication/
# Window Deduplication
Streaming only. Special Deduplication removing duplicates, keeping first/last per window + partitioned keys. No intermediate results, only final at window end; purges state. Used with Windowing TVF directly, or after Window Aggregation/TopN/Join. Same syntax as regular Deduplication, but PARTITION BY must contain window_start and window_end. Uses ROW_NUMBER() like Window Top-N (N=1).
`[code: SELECT [column_list] ... (294 chars)]`
Parameters:
- ROW_NUMBER(): unique sequential number.
- PARTITION BY window_start, window_end [, col_key1...]: must include window_start, window_end, and other keys.
- ORDER BY time_attr [asc|desc]: must be time attribute. ASC=first, DESC=last.
- WHERE (rownum = 1 | rownum <=1 | rownum < 2): required for optimizer recognition.
Note: pattern must be followed exactly.
## Example
Keep last record for every 10-min tumbling window.
`[code: -- tables must have time attribute ... (2053 chars)]`
## Limitation
### Following Windowing TVFs directly
TVF must be Tumble/Hop/Cumulate, not Session (coming).
### Time attribute of order key
Must be event time attribute (processing-time support coming).

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/match_recognize/
# Pattern Recognition
Streaming. Searches for event patterns in data streams. Flink CEP library + SQL API relational way. ISO/IEC TR 19075-5:2016 Row Pattern Recognition in SQL. MATCH_RECOGNIZE clause consolidates CEP and SQL.
Tasks: PARTITION BY/ORDER BY (partition+order), PATTERN (regex-like patterns), DEFINE (logical components/conditions), MEASURES (output expressions).
`[code: SELECT T.aid, T.bid, T.cid ... (308 chars)]`
Flink's implementation is a subset of the standard. Only documented features supported.

## Introduction and Examples
### Installation Guide
Uses Flink CEP library internally. Maven dependency required (or cluster classpath). SQL Client includes by default.
`[code: <dependency> ... (128 chars)]`
### SQL Semantics
Clauses: PARTITION BY (logical partitioning, like GROUP BY), ORDER BY (ordering — essential for patterns), MEASURES (output, like SELECT), ONE ROW PER MATCH (output mode), AFTER MATCH SKIP (where next match starts), PATTERN (regex-like), DEFINE (conditions).
Attention: MATCH_RECOGNIZE only applicable to append table; always produces append table.
### Examples
Ticker table: symbol, rowtime, price, tax, ticker. Task: find periods of constantly decreasing price.
`[code: SELECT * ... (674 chars)]`
PATTERN: START_ROW followed by ≥1 PRICE_DOWN, concluded with PRICE_UP. After match skip to last PRICE_UP. DEFINE: PRICE_DOWN = price < last PRICE_DOWN price (or START_ROW for initial); PRICE_UP = price > last PRICE_DOWN price. ONE ROW PER MATCH output.
Output: symbol, start_tstamp, bottom_tstamp, end_tstamp.

## Partitioning
PARTITION BY for patterns in partitioned data (e.g. per ticker/user). Highly advised — otherwise translated to non-parallel operator for global ordering.

## Order of Events
Processing or event time. Event time: events sorted before pattern state machine; output correct regardless of append order. Assumes time attribute ascending as first ORDER BY arg. `ORDER BY rowtime ASC, price DESC` valid; `ORDER BY price, rowtime` or `ORDER BY rowtime DESC, price ASC` not.

## Define & Measures
DEFINE/MEASURES similar to WHERE/SELECT. MEASURES: output of matching pattern; projects columns, defines expressions; row count depends on output mode. DEFINE: conditions rows must fulfill to classify to pattern variable; undefined variable → default true.
### Aggregations
Built-in and UDF in DEFINE and MEASURES. Applied to subsets mapped to a match. Example: longest period where average price didn't drop below threshold.
`[code: SELECT * ... (386 chars)]`
Aggregations reference single pattern variable only: SUM(A.price*A.tax) valid; AVG(A.price*B.tax) not. DISTINCT aggregations not supported.

## Defining a Pattern
Built from pattern variables with operators. Whole pattern in brackets. Example: `PATTERN (A B+ C*)`.
Operators:
- Concatenation (A B): strict contiguity — no unmapped rows between.
- Quantifiers: * (0+), + (1+), ? (0/1), {n} (exactly n>0), {n,} (n≥0+), {n,m} (n..m inclusive, 0≤n≤m, 0<m), {,m} (0..m inclusive, m>0).
Patterns that can produce empty match not supported: PATTERN (A*), PATTERN (A? B*), PATTERN (A{0,} B{0,} C*), etc.

### Greedy & Reluctant Quantifiers
Greedy (default) matches as many as possible; reluctant matches fewest. Reluctant: append ? (e.g. B*?).
Greedy quantifier for last pattern variable not allowed (e.g. (A B*) invalid). Workaround: artificial state C with negated B condition: `PATTERN (A B* C)` DEFINE C as NOT B.
Attention: optional reluctant quantifier (A??, A{0,1}?) not supported.

### Time constraint
WITHIN clause (non-standard) limits pattern duration — limits state size. If time between first and last event of potential match exceeds interval, match not appended. Encouraged for memory management; state pruned at threshold. May change in future.
`[code: SELECT * ... (405 chars)]`

## Output Mode
- ALL ROWS PER MATCH
- ONE ROW PER MATCH (only supported mode). One summary row per match. Schema = [partitioning columns] + [measures columns].

## Pattern Navigation
### Pattern Variable Referencing
A.price = rows mapped to A plus current row if matching A. Single-row expression selects last value. No variable (SUM(price)) references default * (all variables + current row).
Example table: #, price, [A.price]/[B.price]/[price], Classifier, evaluated expressions.
[table: 6 rows]
### Logical Offsets
FIRST/LAST navigation within mapped events.
[table: 3 rows]
LAST(B.price,1)/LAST(B.price,2) examples, default variable with offsets, multiple refs (must use same variable: LAST(A.price*A.tax) ok, LAST(A.price*B.tax) not).

## After Match Strategy
AFTER MATCH SKIP specifies where new matching starts:
- SKIP PAST LAST ROW: next row after last row of current match. Event belongs to ≤1 match.
- SKIP TO NEXT ROW: next row after starting row.
- SKIP TO LAST variable: last row mapped to variable.
- SKIP TO FIRST variable: first row mapped to variable.
SKIP TO FIRST variable on starting variable → infinite loop → runtime exception. If no rows mapped to variable (e.g. A*) → runtime exception (standard requires valid row).

## Time attributes
Functions to select time attributes for subsequent queries:
[table: 3 rows]

## Controlling Memory Consumption
Potential matches built breadth-first; pattern must finish; reasonable rows must fit in memory. Pattern must not have unbounded quantifier accepting every row. Fix by negating C condition or using reluctant B+?.
Attention: MATCH_RECOGNIZE does NOT use configured state retention time. Use WITHIN clause.

## Known Limitations
Unsupported:
- Pattern groups (quantifiers on subsequences): (A (B C)+) invalid.
- Alterations: PATTERN((A B | C D) E).
- PERMUTE operator.
- Anchors ^, $ (no meaning in streaming).
- Exclusion {- A -} (only ALL ROWS PER MATCH).
- Reluctant optional quantifier A??.
- ALL ROWS PER MATCH output mode (→ only FINAL MEASURES semantic; no CLASSIFIER function).
- SUBSET (logical groups of pattern variables).
- Physical offsets PREV/NEXT.
- MATCH_RECOGNIZE only in SQL, no Table API equivalent.
- Distinct aggregations.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/time-travel/
# Time Travel
Batch / Streaming. Query historical data — specify point in time, query corresponding table data. Requires catalog implementing `getTable(ObjectPath tablePath, long timestamp)`. See Catalog.
`[code: SELECT select_list FROM table_name FOR SYSTEM_TIME AS OF timestamp_exp ... (77 chars)]`
Parameters:
- FOR SYSTEM_TIME AS OF timestamp_expression: query data at specific point in time. timestamp_expression reducible to a constant; only physical tables (not views/sub-queries).
## Example
`[code: --use timestamp constant expression ... (307 chars)]`
## Limitation
timestamp_expression supports expressions reducible to TIMESTAMP constants: constant TIMESTAMP expressions, timestamp add/subtract, some built-in functions and UDFs. Some UDF expressions can't be reduced at parse time → exception.
`[code: --use expression with functions that can not be reduced ... (158 chars)]`
Exception: `Unsupported time travel expression: TO_TIMESTAMP_LTZ(0, 3) for the exp ... (120 chars)`
## Time Zone Handling
TIMESTAMP expression generates TIMESTAMP type, but time travel clause converts TIMESTAMP to LONG based on local time zone. Same query may vary across time zones.
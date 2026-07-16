---
title: 'Apache Flink 2.3 docs — SQL Hive Compatibility / Hive Dialect (batch 1: overview + queries)'
tags: [org, flink, flink-2.3, docs, sql, hive-compatibility, hive-dialect, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:06:22.270Z'
updated: '2026-07-08T04:06:22.270Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL — Hive Compatibility / Hive Dialect (batch 1: overview + queries). Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`. Covers: Hive dialect overview (use via SQL Client / SQL Gateway HiveServer2 / Table API; requires HiveCatalog + HiveModule; 2-part identifiers only; batch mode mainly), queries overview (DQL subset, SELECT syntax, WHERE/GROUP BY/ORDER BY/CLUSTER-DISTRIBUTE-SORT BY, ALL/DISTINCT, LIMIT), Sort/Cluster/Distributed BY (SORT BY per-partition order, DISTRIBUTE BY repartition, CLUSTER BY = both), Group By (GROUPING SETS/ROLLUP/CUBE, GROUPING__ID, position alias flags), Join (INNER/LEFT/RIGHT/FULL/LEFT SEMI/CROSS), Set Operations (UNION/INTERSECT/EXCEPT-MINUS with ALL/DISTINCT), Lateral View (UDTF explode, OUTER, multiple), Window Functions (LEAD/LAG/FIRST_VALUE/LAST_VALUE/RANK/ROW_NUMBER/DENSE_RANK/CUME_DIST/PERCENT_RANK/NTILE + aggregates; window frame defaults), Sub-Queries (FROM clause + WHERE IN/NOT IN/EXISTS), CTE (WITH clause, not in sub-query, no recursion).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/overview/
# Hive Dialect
Flink allows users to write SQL statements in Hive syntax when Hive dialect is used. By providing compatibility with Hive syntax, we aim to improve the interoperability with Hive and reduce the scenarios when users need to switch between Flink and Hive in order to execute different statements.
## Use Hive Dialect
Flink currently supports two SQL dialects: default and hive. You need to switch to Hive dialect before you can write in Hive syntax. The following describes how to set dialect using SQL Client, SQL Gateway configured with HiveServer2 Endpoint and Table API. Also notice that you can dynamically switch dialect for each statement you execute. There's no need to restart a session to use a different dialect.
Note:
- To use Hive dialect, you have to add dependencies related to Hive. Please refer to Hive dependencies for how to add the dependencies.
- Please make sure the current catalog is HiveCatalog. Otherwise, it will fall back to Flink's default dialect. When using SQL Gateway configured with HiveServer2 Endpoint, the current catalog will be a HiveCatalog by default.
- In order to have better syntax and semantic compatibility, it's highly recommended to load HiveModule and place it first in the module list, so that Hive built-in functions can be picked up during function resolution. Please refer here for how to change resolution order. But when using SQL Gateway configured with HiveServer2 Endpoint, the Hive module will be loaded automatically.
- Hive dialect only supports 2-part identifiers, so you can't specify catalog for an identifier.
- While all Hive versions support the same syntax, whether a specific feature is available still depends on the Hive version you use. For example, updating database location is only supported in Hive-2.4.0 or later.
- The Hive dialect is mainly used in batch mode. Some Hive's syntax (Sort/Cluster/Distributed BY, Transform, etc.) haven't been supported in streaming mode yet.
### SQL Client
SQL dialect can be specified via the table.sql-dialect property. Therefore，you can set the dialect after the SQL Client has launched.
`[code: Flink SQL> SET table.sql-dialect = hive; -- to use Hive dialect ... (216 chars)]`
### SQL Gateway Configured With HiveServer2 Endpoint
When using the SQL Gateway configured with HiveServer2 Endpoint, the dialect will be Hive dialect by default, so you don't need to do anything if you want to use Hive dialect. But you can still change the dialect to Flink default dialect.
`[code: # assuming has connected to SQL Gateway with beeline ... (195 chars)]`
### Table API
You can set dialect for your TableEnvironment with Table API.
  Java
`[code: EnvironmentSettings settings = EnvironmentSettings.inStreamingMode(); ... (292 chars)]`
  Python
`[code: from pyflink.table import * ... (278 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/overview/
# Queries
## Description
Hive dialect supports a commonly-used subset of Hive's DQL. The following lists some parts of HiveQL supported by the Hive dialect.
- Sort/Cluster/Distributed BY
- Group By
- Join
- Set Operation
- Lateral View
- Window Functions
- Sub-Queries
- CTE
- Transform
- Table Sample
## Syntax
The following section describes the overall query syntax. The SELECT clause can be part of a query which also includes common table expressions (CTE), set operations, and various other clauses.
`[code: [WITH CommonTableExpression [ , ... ]] ... (278 chars)]`
- The SELECT statement can be part of a set query or a sub-query of another query
- CommonTableExpression is a temporary result set derived from a query specified in a WITH clause
- table_reference indicates the input to the query. It can be a regular table, a view, a join or a sub-query.
- Table names and column names are case-insensitive
### WHERE Clause
The WHERE condition is a boolean expression. Hive dialect supports a number of operators and UDFs in the WHERE clause. Some types of sub queries are supported in WHERE clause.
### GROUP BY Clause
Please refer to GROUP BY for more details.
### ORDER BY Clause
The ORDER BY clause is used to return the result rows in a sorted manner in the user specified order. Different from SORT BY, ORDER BY clause guarantees a global order in the output.
  Note: To guarantee global order, there has to be single one task to sort the final output. So if the number of rows in the output is too large, it could take a very long time to finish.
## CLUSTER/DISTRIBUTE/SORT BY
Please refer to Sort/Cluster/Distributed BY for more details.
### ALL and DISTINCT Clauses
The ALL and DISTINCT options specify whether duplicate rows should be returned or not. If none of these two options are given, the default is ALL (all matching rows are returned). DISTINCT specifies removal of duplicate rows from the result set.
### LIMIT Clause
The LIMIT clause can be used to constrain the number of rows returned by the SELECT statement. LIMIT takes one or two numeric arguments, which must both be non-negative integer constants. The first argument specifies the offset of the first row to return and the second specifies the maximum number of rows to return. When a single argument is given, it stands for the maximum number of rows and the offset defaults to 0.
## Examples
Following is an example of using hive dialect to run some queries.
  Note: Hive dialect no longer supports Flink SQL queries. Please switch to default dialect if you'd like to write in Flink syntax.
`[code: Flink SQL> create catalog myhive with ('type' = 'hive', 'hive-conf-dir ... (1513 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by/
# Sort/Cluster/Distributed by Clause
## Sort By
### Description
Unlike ORDER BY which guarantees a total order of output, SORT BY only guarantees the result rows with each partition is in the user specified order. So when there's more than one partition, SORT BY may return result that's partially ordered.
### Syntax
`[code: query: SELECT expression [ , ... ] FROM src sortBy ... (122 chars)]`
### Parameters
- colOrder — it's used specified the order of returned rows. The default order is ASC.
### Examples
`[code: SELECT x, y FROM t SORT BY x; ... (69 chars)]`
## Distribute By
### Description
The DISTRIBUTE BY clause is used to repartition the data. The data with same value evaluated by the specified expression will be in same partition.
### Syntax
`[code: distributeBy: DISTRIBUTE BY expression [ , ... ] ... (105 chars)]`
### Examples
`[code: -- only use DISTRIBUTE BY clause ... (206 chars)]`
## Cluster By
### Description
CLUSTER BY is a short-cut for both DISTRIBUTE BY and SORT BY. The CLUSTER BY is used to first repartition the data based on the input expressions and sort the data with each partition. Also, this clause only guarantees the data is sorted within each partition.
### Syntax
`[code: clusterBy: CLUSTER BY expression [ , ... ] ... (96 chars)]`
### Examples
`[code: SELECT x, y FROM t CLUSTER BY x; ... (70 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/group-by/
# Group By Clause
## Description
The Group by clause is used to compute a single result from multiple input rows with given aggregation function. Hive dialect also supports enhanced aggregation features to do multiple aggregations based on the same record by using ROLLUP/CUBE/GROUPING SETS.
## Syntax
`[code: group_by_clause:  ... (404 chars)]`
In group_expression, columns can be also specified by position number. But please remember:
- For Hive 0.11.0 through 2.1.x, set hive.groupby.orderby.position.alias to true (the default is false)
- For Hive 2.2.0 and later, set hive.groupby.position.alias to true (the default is false)
## Parameters
### GROUPING SETS
GROUPING SETS allow for more complex grouping operations than those describable by a standard GROUP BY. Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple GROUP BY clauses. All GROUPING SET clauses can be logically expressed in terms of several GROUP BY queries connected by UNION.
For example:
`[code: SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS ( (a, b),  ... (81 chars)]`
is equivalent to
`[code: SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b ... (205 chars)]`
When aggregates are displayed for a column its value is null. This may conflict in case the column itself has some null values. There needs to be some way to identify NULL in column, which means aggregate and NULL in column, which means GROUPING__ID function is the solution to that. This function returns a bitvector corresponding to whether each column is present or not. For each column, a value of "1" is produced for a row in the result set if that column has been aggregated in that row, otherwise the value is "0". This can be used to differentiate when there are nulls in the data. For more details, please refer to Hive's docs Grouping__ID function.
Also, there's Grouping function indicates whether an expression in a GROUP BY clause is aggregated or not for a given row. The value 0 represents a column that is part of the grouping set, while the value 1 represents a column that is not part of the grouping set.
### ROLLUP
ROLLUP is a shorthand notation for specifying a common type of grouping set. It represents the given list of expressions and all prefixes of the list, including the empty list.
For example: `[code: GROUP BY a, b, c WITH ROLLUP ... (28 chars)]` is equivalent to `[code: GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( )). ... (62 chars)]`
### CUBE
CUBE is a shorthand notation for specifying a common type of grouping set. It represents the given list and all of its possible subsets - the power set.
For example: `[code: GROUP BY a, b, c WITH CUBE ... (26 chars)]` is equivalent to `[code: GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a ... (87 chars)]`
## Examples
`[code: -- use group by expression ... (526 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/join/
# Join
## Description
JOIN is used to combine rows from two relations based on join condition.
## Syntax
Hive Dialect supports the following syntax for joining tables:
`[code: join_table: ... (512 chars)]`
## JOIN Type
### INNER JOIN
INNER JOIN returns the rows matched in both join sides. INNER JOIN is the default join type.
### LEFT JOIN
LEFT JOIN returns all the rows from the left join side and the matched values from the right join side. It will concat the values from both sides. If there's no match in right join side, it will append NULL value. LEFT JOIN is equivalent to LEFT OUTER JOIN.
### RIGHT JOIN
RIGHT JOIN returns all the rows from the right join side and the matched values from the left join side. It will concat the values from both sides. If there's no match in left join side, it will append NULL value. RIGHT JOIN is equivalent to RIGHT OUTER JOIN.
### FULL JOIN
FULL JOIN returns all the rows from both join sides. It will concat the values from both sides. If there's one side does not match the row, it will append NULL value. FULL JOIN is equivalent to FULL OUTER JOIN.
### LEFT SEMI JOIN
LEFT SEMI JOIN returns the rows from the left join side that have matching in right join side. It won't concat the values from the right side.
### CROSS JOIN
CROSS JOIN returns the Cartesian product of two join sides.
## Examples
`[code: -- INNER JOIN ... (583 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/set-op/
# Set Operations
Set Operations are used to combine multiple SELECT statements into a single result set. Hive dialect supports the following operations:
- UNION
- INTERSECT
- EXCEPT/MINUS
## UNION
### Description
UNION/UNION DISTINCT/UNION ALL returns the rows that are found in either side. UNION and UNION DISTINCT only returns the distinct rows, while UNION ALL does not duplicate.
### Syntax
`[code: <query> { UNION [ ALL | DISTINCT ] } <query> [ .. ] ... (51 chars)]`
### Examples
`[code: SELECT x, y FROM t1 UNION DISTINCT SELECT x, y FROM t2; ... (153 chars)]`
## INTERSECT
### Description
INTERSECT/INTERSECT DISTINCT/INTERSECT ALL returns the rows that are found in both side. INTERSECT/INTERSECT DISTINCT only returns the distinct rows, while INTERSECT ALL does not duplicate.
### Syntax
`[code: <query> { INTERSECT [ ALL | DISTINCT ] } <query> [ .. ] ... (55 chars)]`
### Examples
`[code: SELECT x, y FROM t1 INTERSECT DISTINCT SELECT x, y FROM t2; ... (165 chars)]`
## EXCEPT/MINUS
### Description
EXCEPT/EXCEPT DISTINCT/EXCEPT ALL returns the rows that are found in left side but not in right side. EXCEPT/EXCEPT DISTINCT only returns the distinct rows, while EXCEPT ALL does not duplicate. MINUS is synonym for EXCEPT.
### Syntax
`[code: <query> { EXCEPT [ ALL | DISTINCT ] } <query> [ .. ] ... (52 chars)]`
### Examples
`[code: SELECT x, y FROM t1 EXCEPT DISTINCT SELECT x, y FROM t2; ... (156 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/lateral-view/
# Lateral View Clause
## Description
Lateral view clause is used in conjunction with user-defined table generating functions(UDTF) such as explode(). A UDTF generates zero or more output rows for each input row. A lateral view first applies the UDTF to each row of base and then joins results output rows to the input rows to form a virtual table having the supplied table alias.
## Syntax
`[code: lateralView: LATERAL VIEW [ OUTER ] udtf( expression ) tableAlias AS c ... (137 chars)]`
The column alias can be omitted. In this case, aliases are inherited from fields name of StructObjectInspector which is returned from UDTF.
## Parameters
- Lateral View Outer — User can specify the optional OUTER keyword to generate rows even when a LATERAL VIEW usually would not generate a row. This happens when the UDTF used does not generate any rows which happens easily with when the column to explode is empty. In this case, the source row would never appear in the results. OUTER can be used to prevent that and rows will be generated with NULL values in the columns coming from UDTF.
- Multiple Lateral Views — A FROM clause can have multiple LATERAL VIEW clauses. Subsequent LATERAL VIEWS can reference columns from any of the tables appearing to the left of the LATERAL VIEW.
## Examples
Assuming you have one table:
`[code: CREATE TABLE pageAds(pageid string, addid_list array<int>); ... (59 chars)]`
And the table contains two rows:
`[code: front_page, [1, 2, 3]; ... (47 chars)]`
Now, you can use LATERAL VIEW to convert the column addid_list into separate rows:
`[code: SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTab ... (181 chars)]`
Also, if you have one table:
`[code: CREATE TABLE t1(c1 array<int>, c2 array<int>); ... (46 chars)]`
You can use multiple lateral view clauses to convert the column c1 and c2 into separate rows:
`[code: SELECT myc1, myc2 FROM t1 ... (110 chars)]`
When the UDTF doesn't produce rows, then LATERAL VIEW won't produce rows. You can use LATERAL VIEW OUTER to still produce rows, with NULL filling the corresponding column.
`[code: SELECT * FROM t1 LATERAL VIEW OUTER explode(array()) C AS a; ... (60 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/window-functions/
# Window Functions
## Description
Window functions are a kind of aggregation for a group of rows, referred as a window. It will return the aggregation value for each row based on the group of rows.
## Syntax
`[code: window_function OVER ( [ { PARTITION | DISTRIBUTE }  BY colName ( [, . ... (181 chars)]`
## Parameters
### window_function
Hive dialect supports the following window functions:
- Windowing functions: LEAD, LAG, FIRST_VALUE, LAST_VALUE (Note: For FIRST_VALUE/LAST_VALUE, use parameter to control skip null values or respect null values isn't supported yet. And they will always skip null values)
- Analytic functions: RANK, ROW_NUMBER, DENSE_RANK, CUME_DIST, PERCENT_RANK, NTILE
- Aggregate Functions: COUNT, SUM, MIN, MAX, AVG
### window_frame
It's used to specified which row to start on and where to end it. Window frame supports the following formats:
`[code: (ROWS | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECED ... (278 chars)]`
When ORDER BY is specified, but missing window_frame, the window frame defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. When both ORDER BY and window_frame are missing, the window frame defaults to ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
  Note: Distinct is not supported in window function yet.
## Examples
`[code: -- PARTITION BY with one partitioning column, no ORDER BY or window sp ... (897 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/sub-queries/
# Sub-Queries
## Sub-Queries in the FROM Clause
### Description
Hive dialect supports sub-queries in the FROM clause. The sub-query has to be given a name because every table in a FROM clause must have a name. Columns in the sub-query select list must have unique names. The columns in the sub-query select list are available in the outer query just like columns of a table. The sub-query can also be a query expression with UNION. Hive dialect supports arbitrary levels of sub-queries.
### Syntax
`[code: select_statement FROM ( select_statement ) [ AS ] name ... (54 chars)]`
### Example
`[code: SELECT col ... (52 chars)]`
## Sub-Queries in the WHERE Clause
### Description
Hive dialect also supports some types of sub-queries in the WHERE clause.
### Syntax
`[code: select_statement FROM table WHERE { colName { IN | NOT IN }  ... (148 chars)]`
### Examples
`[code: SELECT * FROM t1 WHERE t1.x IN (SELECT y FROM t2); ... (120 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/cte/
# Common Table Expression (CTE)
## Description
A Common Table Expression (CTE) is a temporary result set derived from a query specified in a WITH clause, which immediately precedes a SELECT or INSERT keyword. The CTE is defined only with the execution scope of a single statement, and can be referred in the scope.
## Syntax
`[code: withClause: WITH cteClause [ , ... ] ... (78 chars)]`
  Note:
- The WITH clause is not supported within Sub-Query block
- CTEs are supported in Views, CTAS and INSERT statement
- Recursive Queries are not supported
## Examples
`[code: WITH q1 AS ( SELECT key FROM src WHERE key = '5') ... (442 chars)]`
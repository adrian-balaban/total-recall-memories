---
title: 'Apache Flink 2.3 docs — SQL Hive Compatibility / Hive Dialect (batch 2: DDL/DML + misc statements)'
tags: [org, flink, flink-2.3, docs, sql, hive-compatibility, hive-dialect, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:07:32.689Z'
updated: '2026-07-08T04:07:32.689Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL — Hive Compatibility / Hive Dialect (batch 2: DDL/DML + misc statements). Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`. Covers: TRANSFORM clause (MAP/REDUCE shorthand, row formats, record writer/reader, default STRING/TAB/\N handling, output schema defaults), TABLESAMPLE (num_rows ROWS only), CREATE statements (DATABASE, TABLE [EXTERNAL/partitioned/CTAS not supported temp], VIEW, MACRO, FUNCTION [temp/permanent + USING JAR]), DROP statements (DATABASE [RESTRICT/CASCADE], TABLE, VIEW, MACRO, FUNCTION; hive.exec.drop.ignorenonexistent), ALTER statements (DATABASE props/location [Hive-2.4.0+], TABLE rename/props/SerDe/partition add-rename-drop-location/column change/add-replace [CASCADE/RESTRICT], VIEW props/AS), INSERT (TABLE [OVERWRITE], dynamic partition inserts — Flink always nonstrict, INSERT OVERWRITE DIRECTORY [LOCAL/STORED AS/row_format], Multiple Inserts single-scan), LOAD DATA [LOCAL/OVERWRITE/PARTITION — full partition spec only], SHOW (DATABASES/TABLES/VIEWS/PARTITIONS/FUNCTIONS), ADD JAR (single jar only), SET (hiveconf: prefix needed for Hive Conf; key/value unquoted).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/transform/
# Transform Clause
## Description
The TRANSFORM clause allows user to transform inputs using user-specified command or script.
## Syntax
`[code: query: ... (756 chars)]`
  Note:
- MAP .. and REDUCE .. are syntactic transformations of SELECT TRANSFORM ( ... ) in Hive dialect for such query. So you can use MAP / REDUCE to replace SELECT TRANSFORM.
## Parameters
- inRowFormat — Specific use what row format to feed to input data into the running script. By default, columns will be transformed to STRING and delimited by TAB before feeding to the user script; Similarly, all NULL values will be converted to the literal string \N in order to differentiate NULL values from empty strings.
- outRowFormat — Specific use what row format to read the output from the running script. By default, the standard output of the user script will be treated as TAB-separated STRING columns, any cell containing only \N will be re-interpreted as a NULL, and then the resulting STRING column will be cast to the data type specified in the table declaration in the usual way.
- inRecordWriter — Specific use what writer(fully-qualified class name) to write the input data. The default is org.apache.hadoop.hive.ql.exec.TextRecordWriter
- outRecordReader — Specific use what reader(fully-qualified class name) to read the output data. The default is org.apache.hadoop.hive.ql.exec.TextRecordReader
- command_or_script — Specifies a command or a path to script to process data.
  Note:
- Add a script file and then transform input using the script is not supported yet.
- The script used must be a local script and should be accessible on all hosts in the cluster.
- colType — Specific the output of the command/script should be cast what data type. By default, it will be STRING data type.
For the clause ( AS colName ( colType )? [, ... ] )?, please be aware the following behavior:
- If the actual number of output columns is less than user specified output columns, additional user specified out columns will be filled with NULL.
- If the actual number of output columns is more than user specified output columns, the actual output will be truncated, keeping the corresponding columns.
- If user don't specific the clause ( AS colName ( colType )? [, ... ] )?, the default output schema is (key: STRING, value: STRING). The key column contains all the characters before the first tab and the value column contains the remaining characters after the first tab. If there is no tab, it will return the NULL value for the second column value. Note that this is different from specifying AS key, value because in that case, value will only contain the portion between the first tab and the second tab if there are multiple tabs.
## Examples
`[code: CREATE TABLE src(key string, value string); ... (705 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/queries/table-sample/
# Table Sample
## Description
The TABLESAMPLE statement is used to sample rows from the table.
### Syntax
`[code: TABLESAMPLE ( num_rows ROWS ) ... (29 chars)]`
  Note: Currently, only sample specific number of rows is supported.
### Parameters
- num_rows ROWS — num_rows is a constant positive to specify how many rows to sample.
### Examples
`[code: SELECT * FROM src TABLESAMPLE (5 ROWS) ... (38 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/create/
# CREATE Statements
With Hive dialect, the following CREATE statements are supported for now: CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE MARCO, CREATE FUNCTION.
## CREATE DATABASE
### Description
CREATE DATABASE statement is used to create a database with the specified name.
### Syntax
`[code: CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name ... (165 chars)]`
### Examples
`[code: CREATE DATABASE db1; ... (149 chars)]`
## CREATE TABLE
### Description
CREATE TABLE statement is used to define a table in an existing database.
### Syntax
`[code: CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name ... (1409 chars)]`
  NOTE: Create temporary table is not supported yet.
### Examples
`[code: -- creaet non-partition table ... (533 chars)]`
## CREATE VIEW
### Description
CREATE VIEW creates a view with the given name. If no column names are supplied, the names of the view's columns will be derived automatically from the defining SELECT expression. (If the SELECT contains un-aliased scalar expressions such as x+y, the resulting view column names will be generated in the form _C0, _C1, etc.) When renaming columns, column comments can also optionally be supplied. (Comments are not automatically inherited from underlying columns.)
Note that a view is a purely logical object with no associated storage. When a query references a view, the view's definition is evaluated in order to produce a set of rows for further processing by the query.
### Syntax
`[code: CREATE VIEW [IF NOT EXISTS] [db_name.]view_name [(column_name, ...) ] ... (167 chars)]`
### Examples
`[code: CREATE VIEW IF NOT EXISTS v1 ... (134 chars)]`
## CREATE MARCO
### Description
CREATE TEMPORARY MACRO statement creates a macro using the given optional list of columns as inputs to the expression. Macros exists for the duration of the current session.
### Syntax
`[code: CREATE TEMPORARY MACRO macro_name([col_name col_type, ...]) expression ... (71 chars)]`
### Examples
`[code: CREATE TEMPORARY MACRO fixed_number() 42; ... (165 chars)]`
## CREATE FUNCTION
### Description
 CREATE FUNCTION statement creates a function that is implemented by the class_name.
### Syntax
#### Create Temporary Function
`[code: CREATE TEMPORARY FUNCTION function_name AS class_name [USING [JAR|ARTI ... (88 chars)]`
The function exists for the duration of the current session.
#### Create Permanent Function
`[code: CREATE FUNCTION [db_name.]function_name AS class_name ... (90 chars)]`
The function is registered to metastore and will exist in all session unless the function is dropped.
### Parameter
- [USING [JAR|ARTIFACT] 'file_uri'] — User can use the clause to add Jar that contains the implementation of the function along with its dependencies while creating the function. The file_uri can be on local file or distributed file system. Flink will automatically download the jars for remote jars when the function is used in queries. The downloaded jars will be removed when the session exits.
### Examples
`[code: -- create a function assuming the class `SimpleUdf` has existed in cla ... (421 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/drop/
# DROP Statements
With Hive dialect, the following DROP statements are supported for now: DROP DATABASE, DROP TABLE, DROP VIEW, DROP MARCO, DROP FUNCTION.
## DROP DATABASE
### Description
DROP DATABASE statement is used to drop a database as well as the tables/directories associated with the database.
### Syntax
`[code: DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE]; ... (68 chars)]`
The use of SCHEMA and DATABASE are interchangeable - they mean the same thing. The default behavior is RESTRICT, where DROP DATABASE will fail if the database is not empty. To drop the tables in the database as well, use DROP DATABASE ... CASCADE. DROP returns an error if the database doesn't exist, unless IF EXISTS is specified or the configuration variable hive.exec.drop.ignorenonexistent is set to true.
### Examples
`[code: DROP DATABASE db1 CASCADE; ... (26 chars)]`
## DROP TABLE
### Description
DROP TABLE statement removes metadata and data for this table. The data is actually moved to the .Trash/Current directory if Trash is configured. The metadata is completely lost. When drop an EXTERNAL table, data in the table will not be deleted from the filesystem.
### Syntax
`[code: DROP TABLE [IF EXISTS] table_name; ... (34 chars)]`
DROP returns an error if the table doesn't exist, unless IF EXISTS is specified or the configuration variable hive.exec.drop.ignorenonexistent is set to true.
### Examples
`[code: DROP TABLE IF EXISTS t1; ... (24 chars)]`
## DROP VIEW
### Description
DROP VIEW statement is used to removed metadata for the specified view.
### Syntax
`[code: DROP VIEW [IF EXISTS] [db_name.]view_name; ... (42 chars)]`
DROP returns an error if the view doesn't exist, unless IF EXISTS is specified or the configuration variable hive.exec.drop.ignorenonexistent is set to true.
### Examples
`[code: DROP VIEW IF EXISTS v1; ... (23 chars)]`
## DROP MARCO
DROP MARCO statement is used to drop the existing MARCO. Please refer to CREATE MARCO for how to create MARCO.
### Syntax
`[code: DROP TEMPORARY MACRO [IF EXISTS] macro_name; ... (44 chars)]`
DROP returns an error if the macro doesn't exist, unless IF EXISTS is specified.
### Examples
`[code: DROP TEMPORARY MACRO IF EXISTS m1; ... (34 chars)]`
## DROP FUNCTION
DROP FUNCTION statement is used to drop the existing FUNCTION.
### Syntax
`[code: --- Drop temporary function ... (148 chars)]`
DROP returns an error if the function doesn't exist, unless IF EXISTS is specified or the configuration variable hive.exec.drop.ignorenonexistent is set to true.
### Examples
`[code: DROP FUNCTION IF EXISTS f1; ... (27 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/alter/
# ALTER Statements
With Hive dialect, the following ALTER statements are supported for now: ALTER DATABASE, ALTER TABLE, ALTER VIEW.
## ALTER DATABASE
### Description
ALTER DATABASE statement is used to change the properties or location of a database.
### Syntax
`[code: -- alter database's properties ... (215 chars)]`
### Synopsis
- The uses of SCHEMA and DATABASE are interchangeable - they mean the same thing.
- The ALTER DATABASE .. SET LOCATION statement is only supported in Hive-2.4.0 or later. The statement doesn't move the contents of the database's current directory to the newly specified location. It does not change the locations associated with any tables/partitions under the specified database. It only changes the default parent-directory where new tables will be added for this database. This behaviour is analogous to how changing a table-directory does not move existing partitions to a different location.
### Examples
`[code: -- alter database's properties ... (167 chars)]`
## ALTER TABLE
### Description
ALTER TABLE statement changes the schema or properties of a table.
### Rename Table
The RENAME TABLE statement allows user to change the name of a table to a different name.
#### Syntax
`[code: ALTER TABLE table_name RENAME TO new_table_name; ... (48 chars)]`
#### Examples
`[code: ALTER TABLE t1 RENAME TO t2; ... (28 chars)]`
### Alter Table Properties
The ALTER TABLE PROPERTIES statement allows user add own metadata to tables. Currently, last_modified_user, last_modified_time properties are automatically added and managed by Hive.
#### Syntax
`[code: ALTER TABLE table_name SET TBLPROPERTIES table_properties; ... (153 chars)]`
#### Examples
`[code: ALTER TABLE t1 SET TBLPROPERTIES ('p1' = 'v1', 'p2' = 'v2'); ... (60 chars)]`
### Add / Remove SerDe Properties
The statement enable user to change a table's SerDe or add/move user-defined metadata to the table's SerDe Object. The SerDe properties are passed to the table's SerDe to serialize and deserialize data. So users can store any information required for their custom SerDe here. Refer to the Hive's SerDe docs and Hive SerDe for more details.
#### Syntax
Add SerDe Properties: `[code: ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_clas ... (302 chars)]`
Remove SerDe Properties: `[code: ALTER TABLE table_name [PARTITION partition_spec] UNSET SERDEPROPERTIE ... (94 chars)]`
#### Examples
`[code: -- add serde properties ... (163 chars)]`
### Alter Partition
ALTER TABLE ... PARTITION .. statement is used to add/rename/drop partitions.
#### Add Partitions
ALTER TABLE .. ADD PARTITION statement is used to add partitions. Partition values should be quoted only if they are strings. The location must be a directory inside which data files reside. (ADD PARTITION changes the table metadata, but does not load data. If the data does not exist in the partition's location, queries will not return any results.) An error is thrown if the partition_spec for the table already exists. You can use IF NOT EXISTS to skip the error.
##### Syntax
`[code: ALTER TABLE table_name ADD [IF NOT EXISTS] ... (254 chars)]`
##### Examples
`[code: ALTER TABLE t1 ADD PARTITION (dt='2022-08-08', country='china') locati ... (196 chars)]`
#### Rename Partitions
ALTER TABLE .. PARTITION ... RENAME TO ... statement is used to rename partition.
##### Syntax
`[code: ALTER TABLE table_name PARTITION partition_spec RENAME TO PARTITION pa ... (83 chars)]`
##### Examples
`[code: ALTER TABLE t1 PARTITION (dt='2022-08-08', country='china') ... (120 chars)]`
#### Drop Partitions
ALTER TABLE .. DROP PARTITION ... statement is used to drop partition. This removes the data and metadata for this partition. The data is actually moved to the .Trash/Current directory if Trash is configured, but the metadata is completed lost.
##### Syntax
`[code: ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec[, PAR ... (97 chars)]`
##### Examples
`[code: ALTER TABLE t1 DROP IF EXISTS PARTITION (dt='2022-08-08', country='chi ... (75 chars)]`
#### Alter Location / File Format
ALTER TABLE SET command can also be used for changing the file location and file format for existing tables.
##### Syntax
`[code: --- Alter File Location ... (203 chars)]`
##### Examples
`[code: -- alter file localtion ... (248 chars)]`
### Alter Column
#### Rules for Column Names
Column names are case-insensitive. Backtick quotation enables the use of reserved keywords for column names, as well as table names.
#### Change Column's Definition
The statement allow users to change a column's name, data type, comment, or position, or an arbitrary combination of them.
##### Syntax
`[code: ALTER TABLE table_name [PARTITION partition_spec] CHANGE [COLUMN] col_ ... (173 chars)]`
##### Examples
`[code: ALTER TABLE t1 CHANGE COLUMN c1 new_c1 STRING FIRST; ... (108 chars)]`
#### Add/Replace Columns
The statement allow users to add new columns or replace the existing columns with the new columns.
##### Syntax
`[code: ALTER TABLE table_name  ... (159 chars)]`
ADD COLUMNS will add new columns to the end of the existing columns before the partition columns. REPLACE COLUMNS will remove all existing columns and add the new set of columns.
##### Synopsis
ALTER TABLE ... COLUMNS with CASCADE command changes the columns of a table's metadata, and cascades the same change to all the partition metadata. RESTRICT is the default, limiting column changes only to table metadata.
##### Examples
`[code: -- add column ... (158 chars)]`
## ALTER VIEW
### Alter View Properties
ALTER VIEW ... SET TBLPROPERTIES .. allow user to add own metadata to a view.
#### Syntax
`[code: ALTER VIEW [db_name.]view_name SET TBLPROPERTIES table_properties; ... (160 chars)]`
#### Examples
`[code: ALTER VIEW v1 SET TBLPROPERTIES ('p1' = 'v1'); ... (46 chars)]`
### Alter View As Select
ALTER VIEW ... AS .. allow user to change the definition of a view, which must exist.
#### Syntax
`[code: ALTER VIEW [db_name.]view_name AS select_statement; ... (51 chars)]`
#### Examples
`[code: ALTER VIEW v1 AS SELECT * FROM t2; ... (34 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/insert/
# INSERT Statements
## INSERT TABLE
### Description
The INSERT TABLE statement is used to insert rows into a table or overwrite the existing data in the table. The row to be inserted can be specified by value expressions or result from query.
### Syntax
`[code: -- Stardard syntax ... (222 chars)]`
### Parameters
- OVERWRITE TABLE — If specify OVERWRITE TABLE, it will overwrite any existing data in the table or partition.
- PARTITION ( ... ) — An option to specify insert data into table's specific partitions. If the PARTITION clause is specified, the table should be a partitioned table.
- VALUES ( value [, ..] ) [, ( ... ) ] — Specifies the values to be inserted explicitly. A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.
- select_statement — A statement for query. See more details in queries.
### Synopsis
#### Dynamic Partition Inserts
When writing data into Hive table's partition, users can specify the list of partition column names in the PARTITION clause with optional column values. If all the partition columns' value are given, we call this a static partition, otherwise it is a dynamic partition.
Each dynamic partition column has a corresponding input column from the select statement. This means that the dynamic partition creation is determined by the value of the input column.
The dynamic partition columns must be specified last among the columns in the SELECT statement and in the same order in which they appear in the PARTITION() clause.
  Note: In Hive, by default, users must specify at least one static partition in case of accidentally overwriting all partitions, and users can set the configuration hive.exec.dynamic.partition.mode to nonstrict to allow all partitions to be dynamic. But in Flink's Hive dialect, it'll always be nonstrict mode which means all partitions are allowed to be dynamic.
### Examples
`[code: -- insert into table using values ... (544 chars)]`
## INSERT OVERWRITE DIRECTORY
### Description
Query results can be inserted into filesystem directories by using a slight variation of the syntax above:
`[code: -- Standard syntax ... (483 chars)]`
### Parameters
- directory_path — The path for the directory to be inserted can be a full URI. If scheme or authority are not specified, it'll use the scheme and authority from the Flink configuration variable fs.default-scheme that specifies the filesystem scheme.
- LOCAL — The LOCAL keyword is optional. If LOCAL keyword is used, Flink will write data to the directory on the local file system.
- VALUES ( value [, ..] ) [, ( ... ) ] — Specifies the values to be inserted explicitly. A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.
- select_statement — A statement for query. See more details in queries.
- STORED AS file_format — Specifies the file format to use for the insert. The data will be stored as specific file format. The valid value are TEXTFILE, ORC, PARQUET, AVRO, RCFILE, SEQUENCEFILE, JSONFILE. For more details, please refer to Hive's doc Storage Formats.
- row_format — Specifies the row format for this insert. The data will be serialized to file with the specific property. For more details, please refer to Hive's doc RowFormat.
### Examples
`[code: --- insert directory with specific format ... (347 chars)]`
## Multiple Inserts
Hive dialect enables users to insert into multiple destinations in one single statement. Users can mix inserting into table and inserting into directory in one single statement. In such syntax, Flink will minimize the number of data scans requires. Flink can insert data into multiple tables/directories by scanning the input data just once.
### Syntax
`[code: -- multiple insert into table ... (890 chars)]`
### Examples
`[code: -- multiple insert into table ... (702 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/load-data/
# Load Data Statements
## Description
The LOAD DATA statement is used to load the data into a Hive table from the user specified directory or file. The load operation are currently pure copy/move operations that move data files into locations corresponding to Hive tables.
## Syntax
`[code: LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [ ... (116 chars)]`
## Parameters
- filepath — The filepath can be: a relative path (warehouse/data1), an absolute path (/user/hive/warehouse/data1), a full URL with schema and authority (hdfs://namenode:9000/user/hive/warehouse/data1). The filepath can refer to a file (single file loaded) or a directory (all files loaded).
- LOCAL — If specify LOCAL keyword, then: it will look for filepath in the local file system (relative to users' current working directory; full URI for local files allowed e.g. file:///user/hive/warehouse/data1); it will try to copy all the files addressed by filepath to the target file system inferred from the table location, then moved to the table location. If not, then: if schema/authority not specified, uses fs.default.name; if path not absolute, relative to /user/; tries to move files into the table/partition.
- OVERWRITE — By default, files appended. If OVERWRITE, original data replaced.
- PARTITION ( ... ) — An option to specify load data into table's specific partitions. If the PARTITION clause is specified, the table should be a partitioned table.
NOTE: For loading data into partition, the partition specifications must be full partition specifications. Partial partition specification is not supported yet.
## Examples
`[code: -- load data into table ... (212 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/show/
# SHOW Statements
With Hive dialect, the following SHOW statements are supported for now: SHOW DATABASES, SHOW TABLES, SHOW VIEWS, SHOW PARTITIONS, SHOW FUNCTIONS.
## SHOW DATABASES
SHOW DATABASES statement is used to list all the databases defined in the metastore.
### Syntax
`[code: SHOW (DATABASES|SCHEMAS); ... (25 chars)]`
The use of SCHEMA and DATABASE are interchangeable - they mean the same thing.
## SHOW TABLES
SHOW TABLES statement lists all the base tables and views in the current database.
### Syntax
`[code: SHOW TABLES; ... (12 chars)]`
## SHOW VIEWS
SHOW VIEWS statement lists all the views in the current database.
### Syntax
`[code: SHOW VIEWS; ... (11 chars)]`
## SHOW PARTITIONS
SHOW PARTITIONS lists all the existing partitions or the partitions matching the specified partition spec for a given base table.
### Syntax
`[code: SHOW PARTITIONS table_name [ partition_spec ]; ... (152 chars)]`
### Parameter
- partition_spec — The optional partition_spec is used to what kind of partition should be returned. When specified, the partitions that match the partition_spec specification are returned. The partition_spec can be partial which means you can specific only part of partition columns for listing the partitions.
### Examples
`[code: -- list all partitions ... (287 chars)]`
## SHOW FUNCTIONS
SHOW FUNCTIONS statement is used to list all the user defined and builtin functions.
### Syntax
`[code: SHOW FUNCTIONS; ... (15 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/add/
# ADD Statements
With Hive dialect, the following ADD statements are supported for now: ADD JAR.
## ADD JAR
### Description
ADD JAR statement is used to add user jars into the classpath. Add multiple jars file in single ADD JAR statement is not supported.
### Syntax
`[code: ADD JAR <jar_path>; ... (19 chars)]`
### Parameters
- jar_path — The path of the JAR file to be added. It could be either on a local file or distributed file system.
### Examples
`[code: -- add a local jar ... (99 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/hive-compatibility/hive-dialect/set/
# SET Statements
## Description
The SET statement sets a property which provide a ways to set variables for a session and configuration property including system variable and Hive configuration. But environment variable can't be set via SET statement. The behavior of SET with Hive dialect is compatible to Hive's.
## EXAMPLES
`[code: -- set Flink's configuration ... (388 chars)]`
  Note:
- In Hive, the SET command SET xx=yy whose key has no prefix is equivalent to SET hiveconf:xx=yy, which means it'll set it to Hive Conf. But in Flink, with Hive dialect, such SET command set xx=yy will set xx with value yy to Flink's configuration. So, if you want to set configuration to Hive's Conf, please add the prefix hiveconf:, using the SET command like SET hiveconf:xx=yy.
- In Hive dialect, the key/value to be set shouldn't be quoted.
---
title: 'Apache Flink 2.3 docs — SQL Functions, Catalogs, Time Zone'
tags: [org, flink, flink-2.3, docs, sql, sql-functions, catalogs, timezone, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:46:15.029Z'
updated: '2026-07-07T19:46:15.029Z'
importanceScore: 1
---

## Executive Summary

Apache Flink 2.3 SQL Reference — Functions, Catalogs, Time Zone. Captured verbatim (English) from nightlies.apache.org/flink/flink-docs-release-2.3/docs. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`. Covers: built-in functions (scalar/aggregate/column/named args), user-defined functions, catalogs (types, API, store, modification listener), time zone (TIMESTAMP vs TIMESTAMP_LTZ, usage, time attributes, DST).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/functions/built-in-functions/
# System (Built-in) Functions

Flink Table API & SQL provides users with a set of built-in functions for data transformations. This page gives a brief overview of them.
If a function that you need is not supported yet, you can implement a user-defined function.
If you think that the function is general enough, please open a Jira issue for it with a detailed description.

## Scalar Functions
The scalar functions take zero, one or more values as the input and return a single value as the result.
### Comparison Functions
[table: 24 rows]
### Logical Functions
[table: 10 rows]
### Arithmetic Functions
[table: 44 rows]
### String Functions
[table: 52 rows]
### Temporal Functions
[table: 47 rows]
### Conditional Functions
[table: 13 rows]
### Type Conversion Functions
[table: 4 rows]
### Collection Functions
[table: 28 rows]
### JSON Functions
JSON functions make use of JSON path expressions as described in ISO/IEC TR 19075-6 of the SQL standard. Their syntax is inspired by and adopts many features of ECMAScript, but is neither a subset nor superset thereof.
Path expressions come in two flavors, lax and strict. When omitted, it defaults to the strict mode. Strict mode is intended to examine data from a schema perspective and will throw errors whenever data does not adhere to the path expression. However, functions like JSON_VALUE allow defining fallback behavior if an error is encountered. Lax mode, on the other hand, is more forgiving and converts errors to empty sequences.
The special character $ denotes the root node in a JSON path. Paths can access properties ($.a), array elements ($.a[0].b), or branch over all elements in an array ($.a[*].b).
Known Limitations:
- Not all features of Lax mode are currently supported correctly. This is an upstream bug (CALCITE-4717). Non-standard behavior is not guaranteed.
[table: 9 rows]
### Variant Functions
[table: 6 rows]
### Value Construction Functions
[table: 7 rows]
### Value Access Functions
[table: 3 rows]
### Grouping Functions
[table: 3 rows]
### Hash Functions
[table: 8 rows]
### Bitmap Functions
[table: 11 rows]
### Auxiliary Functions
[table: 3 rows]
## Aggregate Functions
The aggregate functions take an expression across all the rows as the input and return a single aggregated value as the result.
[table: 29 rows]
### Bitmap Aggregate Functions
Performance Tips:
- It is strongly recommended to enable MiniBatch aggregation or use bitmap aggregate functions within window aggregations to optimize state access overhead and significantly improve performance.
- Bitmap aggregate functions perform best with append-only input. Performance degrades noticeably with retraction input, so avoid multi-level GROUP BY aggregations on BITMAP columns when possible.
- For cardinality-only scenarios where the intermediate bitmap is not needed, prefer BITMAP_XX_CARDINALITY_AGG() over BITMAP_CARDINALITY(BITMAP_XX_AGG()). They are functionally equivalent, but the former avoids materializing the intermediate bitmap and performs better.
[table: 9 rows]
## Time Interval and Point Unit Specifiers
The following table lists specifiers for time interval and time point units.
For Table API, please use _ for spaces (e.g., DAY_TO_HOUR).
Plural works for SQL only.
[table: 36 rows]
## Column Functions
The column functions are used to select or deselect table columns.
  Column functions are only used in Table API.
[table: 4 rows]
The detailed syntax is as follows:
`[code: columnFunction: ... (343 chars)]`
The usage of the column function is illustrated in the following table. (Suppose we have a table with 5 columns: (a: Int, b: Long, c: String, d:String, e: String)):
[table: 8 rows]
The column functions can be used in all places where column fields are expected, such as select, groupBy, orderBy, UDFs etc. e.g.:
  Java
`[code: table ... (130 chars)]`
  Scala
`[code: table ... (117 chars)]`
  Python
`[code: table \ ... (140 chars)]`
## Named Arguments
By default, values and expressions are mapped to a function's arguments based on the position in the function call, for example f(42, true). All functions in both SQL and Table API support position-based arguments.
If the function declares a static signature, named arguments are available as a convenient alternative. The framework is able to reorder named arguments and consider optional arguments accordingly, before passing them into the function call. Thus, the order of arguments doesn't matter when calling a function and optional arguments don't have to be provided.
In DESCRIBE FUNCTION and documentation a static signature is indicated by the => assignment operator, for example f(left => INT, right => BOOLEAN). Note that not every function supports named arguments. Named arguments are not available for signatures that are overloaded, use varargs, or any other kind of input type strategy. User-defined functions with a single eval() method usually qualify for named arguments.
Named arguments can be used as shown below:
  SQL
`[code: SELECT MyUdf(input => my_column, threshold => 42) ... (49 chars)]`
  Table API
`[code: table.select( ... (121 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/functions/user-defined-functions/
# User-Defined Functions
Flink SQL supports user-defined functions (UDFs) to extend the built-in functionality. You can create custom scalar functions, table functions, and aggregate functions to use in your SQL queries.
## Registering UDFs in SQL
Once you have developed a UDF, you can register it in SQL using the CREATE FUNCTION statement:
`[code: CREATE FUNCTION myudf AS 'com.example.MyScalarFunction'; ... (56 chars)]`
See the CREATE FUNCTION documentation for the full syntax.
## Developing UDFs
Writing user-defined functions requires Java, Scala, or Python programming. For detailed information on how to develop UDFs, see the Table API documentation:
- User-Defined Functions Overview: How to implement and register scalar, table, and aggregate functions.
- Process Table Functions: Advanced table functions for complex event processing.

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/catalogs/
# Catalogs
Catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems.
One of the most crucial aspects of data processing is managing metadata. It may be transient metadata like temporary tables, or UDFs registered against the table environment. Or permanent metadata, like that in a Hive Metastore. Catalogs provide a unified API for managing metadata and making it accessible from the Table API and SQL Queries.
Catalog enables users to reference existing metadata in their data systems, and automatically maps them to Flink's corresponding metadata. For example, Flink can map JDBC tables to Flink table automatically, and users don't have to manually re-writing DDLs in Flink. Catalog greatly simplifies steps required to get started with Flink with users' existing system, and greatly enhanced user experiences.
## Catalog Types
### GenericInMemoryCatalog
The GenericInMemoryCatalog is an in-memory implementation of a catalog. All objects will be available only for the lifetime of the session.
### JdbcCatalog
The JdbcCatalog enables users to connect Flink to relational databases over JDBC protocol. Postgres Catalog and MySQL Catalog are the only two implementations of JDBC Catalog at the moment. See JdbcCatalog documentation for more details on setting up the catalog.
### HiveCatalog
The HiveCatalog serves two purposes; as persistent storage for pure Flink metadata, and as an interface for reading and writing existing Hive metadata. Flink's Hive documentation provides full details on setting up the catalog and interfacing with an existing Hive installation.
  The Hive Metastore stores all meta-object names in lower case. This is unlike GenericInMemoryCatalog which is case-sensitive
### User-Defined Catalog
Catalogs are pluggable and users can develop custom catalogs by implementing the Catalog interface.
In order to use custom catalogs with Flink SQL, users should implement a corresponding catalog factory by implementing the CatalogFactory interface. The factory is discovered using Java's Service Provider Interfaces (SPI). Classes that implement this interface can be added to  META_INF/services/org.apache.flink.table.factories.Factory in JAR files. The provided factory identifier will be used for matching against the required type property in a SQL CREATE CATALOG DDL statement.
  Since Flink v1.16, TableEnvironment introduces a user class loader to have a consistent class loading behavior in table programs, SQL Client and SQL Gateway. The user classloader manages all user jars such as jar added by ADD JAR or CREATE FUNCTION .. USING JAR .. statements. User-defined catalogs should replace Thread.currentThread().getContextClassLoader() with the user class loader to load classes. Otherwise, ClassNotFoundException maybe thrown. The user class loader can be accessed via CatalogFactory.Context#getClassLoader.
#### Interface in Catalog for supporting time travel
Starting from version 1.18, the Flink framework supports time travel to query historical data of a table. To query the historical data of a table, users should implement getTable(ObjectPath tablePath, long timestamp) method for the catalog that the table belongs to.
`[code: public class MyCatalogSupportTimeTravel implements Catalog { ... (1467 chars)]`
## How to Create and Register Flink Tables to Catalog
### Using SQL DDL
Users can use SQL DDL to create tables in catalogs in both Table API and SQL.
  Java
`[code: TableEnvironment tableEnv = ...; ... (479 chars)]`
  Scala
`[code: val tableEnv = ... ... (456 chars)]`
  Python
`[code: from pyflink.table.catalog import HiveCatalog ... (461 chars)]`
  SQL Client
`[code: // the catalog should have been registered via yaml file ... (201 chars)]`
For detailed information, please check out Flink SQL CREATE DDL.
### Using Java, Scala or Python
Users can use Java, Scala or Python to create catalog tables programmatically.
  Java
`[code: import org.apache.flink.table.api.*; ... (861 chars)]`
  Scala
`[code: import org.apache.flink.table.api._ ... (820 chars)]`
  Python
`[code: from pyflink.table import * ... (862 chars)]`
## Catalog API
Note: only catalog program APIs are listed here. Users can achieve many of the same functionalities with SQL DDL. For detailed DDL information, please refer to SQL CREATE DDL.
### Database operations
  Java/Scala
`[code: // create database ... (394 chars)]`
  Python
`[code: from pyflink.table.catalog import CatalogDatabase ... (486 chars)]`
### Table operations
  Java/Scala
`[code: // create table ... (562 chars)]`
  Python
`[code: from pyflink.table import * ... (983 chars)]`
### View operations
  Java/Scala
`[code: // create view ... (534 chars)]`
  Python
`[code: from pyflink.table import * ... (915 chars)]`
### Partition operations
  Java/Scala
`[code: // create view ... (1059 chars)]`
  Python
`[code: from pyflink.table.catalog import ObjectPath, CatalogPartitionSpec, Ca ... (1044 chars)]`
### Function operations
  Java/Scala
`[code: // create function ... (490 chars)]`
  Python
`[code: from pyflink.table.catalog import ObjectPath, CatalogFunction ... (591 chars)]`
## Table API and SQL for Catalog
### Registering a Catalog
Users have access to a default in-memory catalog named default_catalog, that is always created by default. This catalog by default has a single database called default_database. Users can also register additional catalogs into an existing Flink session.
  Java/Scala
`[code: tableEnv.registerCatalog(new CustomCatalog("myCatalog")); ... (57 chars)]`
  Python
`[code: t_env.register_catalog(catalog) ... (31 chars)]`
  YAML
All catalogs defined using YAML must provide a type property that specifies the type of catalog. The following types are supported out of the box.
[table: 3 rows]
`[code: catalogs: ... (80 chars)]`
### Changing the Current Catalog And Database
Flink will always search for tables, views, and UDF's in the current catalog and database.
  Java/Scala
`[code: tableEnv.useCatalog("myCatalog"); ... (63 chars)]`
  Python
`[code: t_env.use_catalog("myCatalog") ... (57 chars)]`
  SQL
`[code: Flink SQL> USE CATALOG myCatalog; ... (54 chars)]`
Metadata from catalogs that are not the current catalog are accessible by providing fully qualified names in the form catalog.database.object.
  Java/Scala
`[code: tableEnv.from("not_the_current_catalog.not_the_current_db.my_table"); ... (69 chars)]`
  Python
`[code: t_env.from_path("not_the_current_catalog.not_the_current_db.my_table") ... (70 chars)]`
  SQL
`[code: Flink SQL> SELECT * FROM not_the_current_catalog.not_the_current_db.my ... (77 chars)]`
### List Available Catalogs
  Java/Scala `[code: tableEnv.listCatalogs(); ... (24 chars)]`
  Python `[code: t_env.list_catalogs() ... (21 chars)]`
  SQL `[code: Flink SQL> show catalogs; ... (25 chars)]`
### List Available Databases
  Java/Scala `[code: tableEnv.listDatabases(); ... (25 chars)]`
  Python `[code: t_env.list_databases() ... (22 chars)]`
  SQL `[code: Flink SQL> show databases; ... (26 chars)]`
### List Available Tables
  Java/Scala `[code: tableEnv.listTables(); ... (22 chars)]`
  Python `[code: t_env.list_tables() ... (19 chars)]`
  SQL `[code: Flink SQL> show tables; ... (23 chars)]`
## Catalog Modification Listener
Flink supports registering customized listener for catalog modification, such as database and table ddl. Flink will create a CatalogModificationEvent event for ddl and notify CatalogModificationListener. You can implement a listener and do some customized operations when receiving the event, such as report the information to some external meta-data systems.
### Implement Catalog Listener
There are two interfaces for the catalog modification listener: CatalogModificationListenerFactory to create the listener and CatalogModificationListener to receive and process the event. You need to implement these interfaces and below is an example.
`[code: /** Factory used to create a {@link CatalogModificationListener} insta ... (971 chars)]`
You need to create a file org.apache.flink.table.factories.Factory in META-INF/services with the content of the full name of YourCatalogListenerFactory for your customized catalog listener factory. After that, you can package the codes into a jar file and add it to lib of Flink cluster.
### Register Catalog Listener
After implemented above catalog modification factory and listener, you can register it to the table environment.
`[code: Configuration configuration = new Configuration(); ... (498 chars)]`
For sql-gateway, you can add the option table.catalog-modification.listeners in the Flink configuration file and start the gateway, or you can also start sql-gateway with dynamic parameter, then you can use sql-client to perform ddl directly.
## Catalog Store
Catalog Store is used to store the configuration of catalogs. When using Catalog Store, the configurations of catalogs created in the session will be persisted in the corresponding external system of Catalog Store. Even if the session is reconstructed, previously created catalogs can still be retrieved from Catalog Store.
### Configure Catalog Store
Users can configure the Catalog Store in different ways, one is to use the Table API, and another is to use YAML configuration.
Register a catalog store using catalog store instance:
`[code: // Initialize a catalog Store instance ... (302 chars)]`
Register a catalog store using configuration:
`[code: // Set up configuration ... (467 chars)]`
In SQL Gateway, it is recommended to configure the settings in a yaml file so that all sessions can automatically use the pre-created Catalog. Usually, you need to configure the kind of Catalog Store and other required parameters for the Catalog Store.
`[code: table.catalog-store.kind: file ... (92 chars)]`
### Catalog Store Type
Flink has two built-in Catalog Stores, namely GenericInMemoryCatalogStore and FileCatalogStore, but the Catalog Store model is extendable, so users can also implement their own custom Catalog Store.
#### GenericInMemoryCatalogStore
GenericInMemoryCatalogStore is an implementation of CatalogStore that saves configuration information in memory. All catalog configurations are only available within the sessions' lifecycle, and the stored catalog configurations will be automatically cleared after the session is closed.
  By default, if no Catalog Store related configuration is specified, the system uses this implementation.
#### FileCatalogStore
FileCatalogStore can save the Catalog configuration to a file. To use FileCatalogStore, you need to specify the directory where the Catalog configurations needs to be saved. Each Catalog will have its own file named the same as the Catalog Name.
The FileCatalogStore implementation supports both local and remote file systems that are available via the Flink FileSystem abstraction. If the given Catalog Store path does not exist either completely or partly, FileCatalogStore will try to create the missing directories.
  If the given Catalog Store path does not exist and FileCatalogStore fails to create a directory, the Catalog Store cannot be initialized, hence an exception will be thrown. In case the FileCatalogstore initialization is not successful, both SQL Client and SQL Gateway will be broken.
Here is an example directory structure representing the storage of Catalog configurations using FileCatalogStore:
`[code: - /path/to/save/the/catalog/ ... (82 chars)]`
#### Catalog Store Configuration
The following options can be used to adjust the Catalog Store behavior.
[table: 3 rows]
#### Custom Catalog Store
Catalog Store is extensible, and users can customize Catalog Store by implementing its interface. If SQL CLI or SQL Gateway needs to use Catalog Store, the corresponding CatalogStoreFactory interface also needs to be implemented for this Catalog Store.
`[code: public class CustomCatalogStoreFactory implements CatalogStoreFactory  ... (1959 chars)]`

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/timezone/
# Time Zone
Flink provides rich data types for Date and Time, including DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND (please see Date and Time for detailed information). Flink supports setting time zone in session level (please see table.local-time-zone for detailed information). These timestamp data types and time zone support of Flink make it easy to process business data across time zones.
## TIMESTAMP vs TIMESTAMP_LTZ
### TIMESTAMP type
- TIMESTAMP(p) is an abbreviation for TIMESTAMP(p) WITHOUT TIME ZONE, the precision p supports range is from 0 to 9, 6 by default.
- TIMESTAMP describes a timestamp represents year, month, day, hour, minute, second and fractional seconds.
- TIMESTAMP can be specified from a string literal, e.g.
`[code: Flink SQL> SELECT TIMESTAMP '1970-01-01 00:00:04.001'; ... (138 chars)]`
### TIMESTAMP_LTZ type
- TIMESTAMP_LTZ(p) is an abbreviation for TIMESTAMP(p) WITH LOCAL TIME ZONE, the precision p supports range is from 0 to 9, 6 by default.
- TIMESTAMP_LTZ describes an absolute time point on the time-line, it stores a long value representing epoch-milliseconds and an int representing nanosecond-of-millisecond. The epoch time is measured from the standard Java epoch of 1970-01-01T00:00:00Z. Every datum of TIMESTAMP_LTZ type is interpreted in the local time zone configured in the current session for computation and visualization.
- TIMESTAMP_LTZ has no literal representation and thus can not specify from literal, it can derives from a long epoch time(e.g. The long time produced by Java System.currentTimeMillis())
`[code: Flink SQL> CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3); ... (527 chars)]`
- TIMESTAMP_LTZ can be used in cross time zones business because the absolute time point (e.g. above 4001 milliseconds) describes a same instantaneous point in different time zones. Giving a background that at a same time point, the System.currentTimeMillis() of all machines in the world returns same value (e.g. the 4001 milliseconds in above example), this is absolute time point meaning.
## Time Zone Usage
The local time zone defines current session time zone id. You can config the time zone in Sql Client or Applications.
  SQL Client `[code: -- set to UTC time zone ... (256 chars)]`
  Java `[code: EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode( ... (411 chars)]`
  Scala `[code: val envSetting = EnvironmentSettings.inStreamingMode() ... (365 chars)]`
  Python `[code: env_setting = EnvironmentSettings.in_streaming_mode() ... (344 chars)]`
The session time zone is useful in Flink SQL, the main usages are:
### Decide time functions return value
The following time functions are influenced by the configured time zone: LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME()
`[code: Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau'; ... (246 chars)]`
`[code: +------------------------+-----------------------------+-------+-----+ ... (1103 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (81 chars)]`
`[code: +-----------+-------------------------+--------------+--------------+- ... (869 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (91 chars)]`
`[code: +-----------+-------------------------+--------------+--------------+- ... (869 chars)]`
### TIMESTAMP_LTZ string representation
The session timezone is used when represents a TIMESTAMP_LTZ value to string format, i.e print the value, cast the value to STRING type, cast the value to TIMESTAMP, cast a TIMESTAMP value to TIMESTAMP_LTZ:
`[code: Flink SQL> CREATE VIEW MyView2 AS SELECT TO_TIMESTAMP_LTZ(4001, 3) AS  ... (144 chars)]`
`[code: +------+------------------+-------+-----+--------+-----------+ ... (377 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (81 chars)]`
`[code: +-------------------------+-------------------------+ ... (269 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (91 chars)]`
`[code: +-------------------------+-------------------------+ ... (269 chars)]`
`[code: Flink SQL> CREATE VIEW MyView3 AS SELECT ltz, CAST(ltz AS TIMESTAMP(3) ... (142 chars)]`
`[code: Flink SQL> DESC MyView3; ... (816 chars)]`
`[code: Flink SQL> SELECT * FROM MyView3; ... (33 chars)]`
`[code: +-------------------------+---------------------------+--------------- ... (699 chars)]`
## Time Attribute and Time Zone
Please see Time Attribute for more information about time attribute.
### Processing Time and Time Zone
Flink SQL defines process time attribute by function PROCTIME(), the function return type is TIMESTAMP_LTZ.
  Before Flink 1.13, the function return type of PROCTIME() is TIMESTAMP, and the return value is the TIMESTAMP in UTC time zone, e.g. the wall-clock shows 2021-03-01 12:00:00 at Shanghai, however the PROCTIME() displays 2021-03-01 04:00:00 which is wrong. Flink 1.13 fixes this issue and uses TIMESTAMP_LTZ type as return type of PROCTIME(), users don't need to deal time zone problems anymore.
The PROCTIME() always represents your local timestamp value, using TIMESTAMP_LTZ type can also support DayLight Saving Time well.
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (77 chars)]`
`[code: +-------------------------+ ... (139 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (87 chars)]`
`[code: +-------------------------+ ... (139 chars)]`
`[code: Flink SQL> CREATE TABLE MyTable1 ( ... (800 chars)]`
`[code: +-----------------+-----------------------------+-------+-----+------- ... (764 chars)]`
Use the following command to ingest data for MyTable1 in a terminal:
`[code: > nc -lk 9999 ... (43 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (81 chars)]`
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (91 chars)]`
Returns the different window start, window end and window proctime compared to calculation in UTC timezone.
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
  Processing time window is non-deterministic, so each run will get different windows and different aggregations. The above example is just for explaining how time zone affects processing time window.
### Event Time and Time Zone
Flink supports defining event time attribute on TIMESTAMP column and TIMESTAMP_LTZ column.
#### Event Time Attribute on TIMESTAMP
If the timestamp data in the source is represented as year-month-day-hour-minute-second, usually a string value without time-zone information, e.g. 2020-04-15 20:13:40.564, it's recommended to define the event time attribute as a TIMESTAMP column:
`[code: Flink SQL> CREATE TABLE MyTable2 ( ... (855 chars)]`
`[code: +----------------+------------------------+------+-----+--------+----- ... (701 chars)]`
Use the following command to ingest data for MyTable2 in a terminal:
`[code: > nc -lk 9999 ... (169 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (81 chars)]`
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (91 chars)]`
Returns the same window start, window end and window rowtime compared to calculation in UTC timezone.
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
#### Event Time Attribute on TIMESTAMP_LTZ
If the timestamp data in the source is represented as a epoch time, usually a long value, e.g. 1618989564564, it's recommended to define event time attribute as a TIMESTAMP_LTZ column.
`[code: Flink SQL> CREATE TABLE MyTable3 ( ... (944 chars)]`
`[code: +----------------+----------------------------+-------+-----+--------+ ... (746 chars)]`
The input data of MyTable3 is:
`[code: A,1.1,1618495260000  # The corresponding utc timestamp is 2021-04-15 1 ... (467 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'UTC'; ... (81 chars)]`
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
`[code: Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai'; ... (91 chars)]`
Returns the different window start, window end and window rowtime compared to calculation in UTC timezone.
`[code: +-------------------------+-------------------------+----------------- ... (692 chars)]`
## Daylight Saving Time Support
Flink SQL supports defining time attributes on TIMESTAMP_LTZ column, base on this, Flink SQL gracefully uses TIMESTAMP and TIMESTAMP_LTZ type in window processing to support the Daylight Saving Time.
Flink use timestamp literal to split the window and assigns window to data according to the epoch time of the each row. It means Flink uses TIMESTAMP type for window start and window end (e.g. TUMBLE_START and TUMBLE_END), uses TIMESTAMP_LTZ for window time attribute (e.g. TUMBLE_PROCTIME, TUMBLE_ROWTIME).
Given an example of tumble window, the DaylightTime in Los_Angeles starts at time 2021-03-14 02:00:00:
`[code: long epoch1 = 1615708800000L; // 2021-03-14 00:00:00 ... (248 chars)]`
The tumble window [2021-03-14 00:00:00,  2021-03-14 00:04:00] will collect 3 hours' data in Los_angele time zone, but it collect 4 hours' data in other non-DST time zones, what user to do is only define time attribute on TIMESTAMP_LTZ column.
All windows in Flink like Hop window, Session window, Cumulative window follow this way, and all operations in Flink SQL support TIMESTAMP_LTZ well, thus Flink gracefully supports the Daylight Saving Time zone.
## Difference between Batch and Streaming Mode
The following time functions: LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, NOW() — Flink evaluates their values according to execution mode. They are evaluated for each record in streaming mode. But in batch mode, they are evaluated once as the query starts and uses the same result for every row.
The following time functions are evaluated for each record no matter in batch or streaming mode: CURRENT_ROW_TIMESTAMP(), PROCTIME()
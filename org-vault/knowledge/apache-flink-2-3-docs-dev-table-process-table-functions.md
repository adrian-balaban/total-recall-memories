---
title: Apache Flink 2.3 docs — Dev Table Process Table Functions
tags: [org, flink, flink-2.3, docs, dev, table-api, ptf, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:14:08.349Z'
updated: '2026-07-08T04:14:08.349Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/ptfs/
# Process Table Functions (PTFs)

Process Table Functions (PTFs) are the most powerful function kind for Flink SQL and Table API. They enable implementing
user-defined operators that can be as feature-rich as built-in operations. PTFs can take (partitioned) tables to produce
a new table. They have access to Flink's managed state, event-time and timer services, and underlying table changelogs.

Conceptually, a PTF is itself a user-defined function that is a superset of all
other user-defined functions. It maps zero, one, or multiple tables to zero, one, or multiple rows (or structured types).
Scalar arguments are supported. Due to its stateful nature, implementing aggregating behavior is possible as well.

A PTF enables the following tasks:

- Apply transformations on each row of a table.

- Logically partition the table into distinct sets and apply transformations per set.

- Store seen events for repeated access.

- Continue the processing at a later point in time enabling waiting, synchronization, or timeouts.

- Buffer and aggregate events using complex state machines or rule-based conditional logic.

## Polymorphic Table Functions

The PTF query syntax and semantics are derived from SQL:2016's Polymorphic Table Functions.
Detailed information on the expected behavior and integration of polymorphic table functions within the SQL language can
be found in ISO/IEC 19075-7:2021 (Part 7). A publicly available
summary is provided in Section 3
of the related SIGMOD paper.

While both share the same abbreviation (PTF), process table functions in Flink enhance polymorphic table functions by
incorporating Flink-specific features, such as state management, time, and timer services. Call characteristics, including
table arguments with row or set semantics, descriptor arguments, and processing concepts related to virtual processors,
are aligned with the SQL standard.

## Motivating Examples

The following examples demonstrate how a PTF can accept and transform tables. The @ArgumentHint specifies that the function
accepts a table as an argument, rather than just a scalar row value. In both examples, the eval() method is invoked for
each row in the input table. Additionally, the @ArgumentHint not only indicates that the function can process a table,
but also defines how the function interprets the table - whether using row or set semantics.

### Greeting

An example that adds a greeting to each incoming customer.

  Java

`[code: import org.apache.flink.table.annotation.*; ... (1120 chars)]`

The results of both Table API and SQL look similar to:

`[code: +----+--------------------------------+ ... (279 chars)]`

### Greeting with Memory

An example that adds a greeting for each incoming customer, taking into account whether the customer has been greeted
previously.

  Java

`[code: import org.apache.flink.table.annotation.*; ... (1441 chars)]`

The results of both Table API and SQL look similar to:

`[code: +----+--------------------------------+------------------------------- ... (510 chars)]`

### Greeting with Follow Up

An example that adds a greeting for each incoming customer and sends out a follow-up notification after some time. The
example illustrates the use of time and unnamed timers.

  Java

`[code: import org.apache.flink.table.annotation.*; ... (2207 chars)]`

The results of both Table API and SQL look similar to:

`[code: +----+--------------------------------+------------------------------- ... (993 chars)]`

## Table Semantics and Virtual Processors

PTFs can produce a new table by consuming tables as arguments. For scalability, input tables are distributed across
so-called "virtual processors". A virtual processor, as defined by the SQL standard, executes a PTF instance and has
access only to a portion of the entire table. The argument declaration decides about the size of the portion and
co-location of data. Conceptually, tables can be processed either "per row" (i.e. with row semantics) or "per set"
(i.e. with set semantics).

Note: Accessing, in this context, means that the individual rows are streamed through the virtual processor. It is
the responsibility of the PTF to store past events for repeated access using state.

### Table Argument with Row Semantics

A PTF that takes a table with row semantics assumes that there is no correlation between rows and each row can be
processed independently. The framework is free in how to distribute rows across virtual processors and each virtual
processor has access only to the currently processed row.

### Table Argument with Set Semantics

A PTF that takes a table with set semantics assumes that there is a correlation between rows. When calling the function,
the PARTITION BY clause defines the columns for correlation. The framework ensures that all rows belonging to same set
are co-located. A PTF instance is able to access all rows belonging to the same set. In other words: The virtual processor
is scoped by a key context.

It is also possible not to provide a key (if the argument is declared with ArgumentTrait.OPTIONAL_PARTITION_BY), in
which case only one virtual processor handles the entire table, thereby losing scalability benefits.

## Call Syntax

When invoking a PTF, the system automatically adds implicit arguments for state and time management alongside the
user-defined input arguments.

Given a PTF TableFilter that has been implemented as:

  Java

`[code: @DataTypeHint("ROW<threshold INT, score BIGINT>") ... (338 chars)]`

The effective call signature is:

`[code: TableFilter(input => {TABLE, ROW SEMANTIC TABLE}, threshold => INT NOT ... (114 chars)]`

Both on_time and uid are optional by default. The on_time is required if time semantics are needed. The uid must
be provided for stateful transformations.

Both SQL and Table API users can call the function using either position-based or name-based syntax. For better readability
and future function evolution, we recommend the name-based syntax. It offers better support for optional arguments and
eliminates the need to maintain a specific argument order.

SQL:

`[code: -- Position-based ... (312 chars)]`

Table API:

`[code: // Position-based ... (574 chars)]`

### Function Chaining

Multiple PTFs can be invoked after each other.

In SQL, we recommend using common table expressions (i.e., WITH) to enhance code readability by applying the Divide and
Conquer strategy, breaking the problem down into smaller, more manageable parts:

`[code: WITH ... (221 chars)]`

In the Table API, the framework enables a consecutive application of functions:

`[code: env.from("t") ... (184 chars)]`

## Implementation Guide

  PTFs follow similar implementation principles as other user-defined functions in Flink. Please see
implementation guide for user-defined functions
for more details. This page focuses on PTF specifics.

In order to define a process table function, one has to extend the base class ProcessTableFunction in org.apache.flink.table.functions
and implement an evaluation method named eval(...). The eval() method declares the supported input arguments, and
state entries in case of stateful PTF.

The signature for eval() should follow the pattern:

`[code: eval( <context>? , <state entry>* , <call argument>* ) ... (54 chars)]`

The evaluation method must be declared publicly and should not be static. Overloading is not supported.

For storing a user-defined function in a catalog, the class must have a default constructor and must be instantiable during
runtime. Anonymous, inline functions in Table API can only be persisted if the function object is not stateful (i.e. containing
only transient and static fields).

### Data Types

By default, input and output data types are automatically extracted using reflection. This includes the generic argument
T of the class for determining an output data type. Input arguments are derived from the eval() method. If the reflective
information is not sufficient, it can be supported and enriched with @FunctionHint, @ArgumentHint, and
@DataTypeHint annotations.

In contrast to scalar functions, the evaluation method itself must not have a return type, instead, table functions provide
a collect(T) method that can be called within the evaluation method for emitting zero, one, or more records. A returned
record may consist of one or more fields. If an output record consists of only a single field, the structured record can
be omitted, and a scalar value can be emitted that will be implicitly wrapped into a row by the runtime.

The following examples show how to specify data types:

  Java

`[code: // Function that accepts two scalar INT arguments and emits them as an ... (907 chars)]`

### Arguments

The @ArgumentHint annotation enables declaring the name, data type, and traits of each argument.

In most cases, the system can automatically infer the name and data type reflectively, so they do not need to be
specified. However, traits must be provided explicitly, particularly when defining the argument's kind. The argument
of a PTF can be set to ArgumentTrait.SCALAR, ArgumentTrait.ROW_SEMANTIC_TABLE, or ArgumentTrait.SET_SEMANTIC_TABLE. By default,
arguments are treated as scalar values.

The following examples show usages of the @ArgumentHint annotation:

  Java

`[code: // Function that has two arguments: ... (658 chars)]`

#### Table Arguments

The traits ArgumentTrait.SET_SEMANTIC_TABLE and ArgumentTrait.ROW_SEMANTIC_TABLE define table arguments.

Table arguments can declare a concrete data type (of either row or structured type) or accept any type of row in a
polymorphic fashion.

  The Row class might be declared by a table argument or scalar argument.

For scalar arguments, the data type must be fully specified and values have to be
provided, e.g. f(my_scalar_arg => ROW(12).

For table arguments, a full data type is optional and expects a table instead of one row,
e.g. f(my_table_arg => TABLE t).

  Java

`[code: // Function with explicit table argument type of row ... (1178 chars)]`

### Context

A Context can be added as a first argument to the eval() method for additional
information about the input tables and other services provided by the framework.

  Java

`[code: // Function that accesses the Context for reading the PARTITION BY col ... (643 chars)]`

## State

A PTF that takes set semantic tables can be stateful. Intermediate results can be buffered,
cached, aggregated, or simply stored for repeated access. A function can have one or more state
entries which are managed by the framework. Flink takes care of storing and restoring those
during failures or restarts (i.e. Flink managed state).

A state entry is partitioned by a key and cannot be accessed globally. The partitioning (or a
single partition in case of no partitioning) is defined by the corresponding function call. In
other words: Similar to how a virtual processor has access only to a portion of the entire table,
a PTF has access only to a portion of the entire state defined by the PARTITION BY clause. In
Flink, this concept is also known as keyed state.

State entries can be added as a mutable parameter to the eval() method. In order to
distinguish them from call arguments, they must be declared before any other argument, but after
an optional Context parameter. Furthermore, they must be annotated either via @StateHint or declared
as part of @FunctionHint(state = ...).

For read and write access, only row or structured types (i.e. POJOs with default constructor)
qualify as a data type. If no state is present, all fields are set to null (in case of a row
type) or fields are set to their default value (in case of a structured type). For state
efficiency, it is recommended to keep all fields nullable.

  Java

`[code: // Function that counts and stores its intermediate result in the Coun ... (1337 chars)]`

### State TTL

A time-to-live (TTL) duration can be specified for each state entry, and Flink's state backend will automatically clean
up the entry once the TTL expires.

The @StateHint(ttl = "...") annotation specifies a minimum time interval for how long idle state (i.e., state which was not
updated by a create or write operation) will be retained. State will never be cleared until it was idle for less than the
minimum time, and will be cleared at some time after it was idle.

Use TTL for being able to efficiently manage an ever-growing state size or for complying with data protection requirements.

The cleanup is based on processing time, which effectively corresponds to the wall clock time as defined by System.currentTimeMillis().

The provided string must use Flink's duration syntax (e.g., "3 days", "45 min", "3 hours", "60 s"). If no unit is specified,
the value is interpreted as milliseconds. The TTL setting on a state entry has higher precedence than the global state TTL
configuration table.exec.state.ttl for the entire pipeline.

By default, the TTL is set to Long.MAX_VALUE to allow for future adjustment of a reasonable value in the state layout.
If state size is a concern and TTL is unnecessary, it can be set to 0, effectively excluding the TTL from the state layout.

  Java

`[code: // Function with 3 state entries each using a different TTL. ... (429 chars)]`

### Large State

Flink's state backends provide different types of state to efficiently handle large state.

Currently, PTFs support three types of state:

- Value state: Represents a single value.

- List state: Represents a list of values, supporting operations like appending, removing, and iterating.

- Map state: Represents a map (key-value pair) for efficient lookups, modifications, and removal of individual entries.

By default, state entries in a PTF are represented as value state. This means that every state entry is fully read from
the state backend when the evaluation method is called, and the value is written back to the state backend once the
evaluation method finishes.

To optimize state access and avoid unnecessary (de)serialization, state entries can be declared as:

- org.apache.flink.table.api.dataview.ListView (for list state)

- org.apache.flink.table.api.dataview.MapView (for map state)

These provide direct views to the underlying Flink state backend.

For example, when using a MapView, accessing a value via MapView#get will only deserialize the value associated with
the specified key. This allows for efficient access to individual entries without needing to load the entire map. This
approach is particularly useful when the map does not fit entirely into memory.

  State TTL is applied individually to each entry in a list or map, allowing for fine-grained expiration control over state
elements.

The following example demonstrates how to declare and use a MapView. It assumes the PTF processes a table with the
schema (userId, eventId, ...), partitioned by userId, with a high cardinality of distinct eventId values. For this
use case, it is generally recommended to partition the table by both userId and eventId. For example purposes, the
large state is stored as a map state.

  Java

`[code: // Function that uses a map view for storing a large map for an event  ... (586 chars)]`

Similar to other data types, reflection is used to extract the necessary type information. If reflection is not
feasible - such as when a Row object is involved - type hints can be provided. Use the ARRAY data type for list views
and the MAP data type for map views.

  Java

`[code: // Function that uses a list view of rows ... (290 chars)]`

### Efficiency and Design Principles

A stateful function also means that data layout and data retention should be well thought
through. An ever-growing state can happen by an unlimited number of partitions (i.e. an open
keyspace) or even within a partition. Consider setting a @StateHint(ttl = ... ) or
call Context.clearAllState() eventually.

  Java

`[code: // Function that waits for a second event coming in BUT with better st ... (553 chars)]`

## Time and Timers

A PTF natively supports event time. Time-based operations can be accessed via Context#timeContext(Class).

The time context is always scoped to the currently processed event. The event could be either the current input row
or a firing timer.

Timestamps for time and timers can be represented as either java.time.Instant, java.time.LocalDateTime, or Long.
These timestamps are based on milliseconds since the epoch and do not account for the local session timezone. The time
class can be passed as an argument to timeContext().

### Time

Every PTF takes an optional on_time argument. The on_time argument in the function call declares the time attribute
column for which a watermark has been declared. When processing a table's row, this timestamp can be accessed via
TimeContext#time() and the watermark via TimeContext#currentWatermark()/TimeContext#tableWatermark()
respectively.

Specifying an on_time argument in the function call instructs the framework to return a rowtime column in the
function's output for subsequent time-based operations.

SQL syntax for declaring an on_time attribute:

`[code: SELECT * FROM f(..., on_time => DESCRIPTOR(`my_timestamp`)); ... (60 chars)]`

Table API for declaring an on_time attribute:

`[code: .process(MyFunction.class, ..., descriptor("my_timestamp").asArgument( ... (82 chars)]`

The ArgumentTrait.REQUIRE_ON_TIME makes the on_time argument mandatory if necessary.

Once an on_time argument is provided, timers can be used. The following motivating example illustrates how eval() and
onTimer() work together:

  Java

`[code: // Function that sends out a ping for the given key. ... (1465 chars)]`

The result will look similar to:

`[code: | op |                             id |                         EXPR$0 ... (593 chars)]`

With the on_time attribute declared, the output includes a rowtime column at the end. This column represents another
watermarked time attribute, which can be used for subsequent PTF calls or time-based operations. The data type of rowtime
is derived from the input's time attribute.

#### Current Timestamp

TimeContext#time() returns the timestamp of the currently processed event.

An event can be either the row of a table or a firing timer:

1. Row event timestamp

The timestamp of the row currently being processed within the eval() method.

Powered by the function call's on_time argument, this method will return the content of the referenced time attribute
column. Returns null if the on_time argument doesn't reference a time attribute column in the currently processed
table.

2. Timer event timestamp

The timestamp of the firing timer currently being processed within the onTimer() method.

#### Table Watermark

TimeContext#tableWatermark() returns the event-time watermark of the input table currently being processed.

Watermarks are generated in sources and sent through the topology for advancing the logical clock. The current watermark
of an input table is the minimum watermark of all upstream Flink subtasks producing the table.

In multi-input scenarios, each input table can have its own independent watermark. This method returns the watermark
specific to the input table that is currently being processed in the eval() method, rather than the global minimum
watermark across all input tables (which is returned by currentWatermark()).

This is particularly useful for late event detection on a per-input basis.

It returns the current watermark of the input table being processed. null if called within the onTimer() method or a
watermark has not yet been received from all upstream Flink subtasks producing the table.

#### Current Watermark

TimeContext#currentWatermark() returns the current event-time watermark at this PTF instance.

Watermarks are generated in sources and sent through the topology for advancing the logical clock. The current watermark
of a PTF instance is the global minimum watermark of all input tables (i.e., across all upstream Flink subtasks and
table partitions).

This method returns the current watermark of the Flink subtask that evaluates the PTF. Thus, the returned timestamp
represents the entire Flink subtask, independent of the currently processed input table and partition. This behavior is
similar to a call to SELECT CURRENT_WATERMARK(...) in SQL.

It returns the current watermark at the PTF instance across all upstream Flink subtasks and table partitions. A null
value is returned if no minimum logical time could be calculated across all inputs; this happens during startup
or recovery when one or more active (i.e. not idle) inputs haven't sent a watermark yet.

### Timers

A PTF that takes set semantic tables can support timers. Timers allow for continuing the processing at a later point in
time. This makes waiting, synchronization, or timeouts possible. A timer fires for the registered time when the watermark
progresses the logical clock.

Timers can be named (TimeContext#registerOnTime(String, TimeType)) or unnamed (TimeContext#registerOnTime(TimeType)).
The name of a timer can be useful for replacing or deleting an existing timer, or for identifying multiple timers via
OnTimerContext#currentTimer() when they fire.

An onTimer() method must be declared next to the eval() method for reacting to timer events. The signature of the
onTimer() method must contain an optional OnTimerContext followed by all state entries (as declared in the eval()
method).

The signature for onTimer() should follow the pattern:

`[code: onTimer( <on timer context>? , <state entry>* ) ... (47 chars)]`

Flink takes care of storing and restoring timers during failures or restarts. Thus, timers are a special kind of state.
Similarly, timers are scoped to a virtual processor defined by the PARTITION BY clause. A timer can only be registered
and deleted in the current virtual processor.

The following example illustrates how to register and clear timers:

  Java

`[code: // Function that waits for a second event or timeouts after 60 seconds ... (802 chars)]`

### Handling of Late Records

A late record is a record with a time attribute value that is less than or equal to the current
watermark. PTFs handle late records just like non-late records by calling the eval() method. If
the on_time argument is specified, the late timestamp is preserved in the output. This behavior is
the same for PTFs with row and set semantics.

Registering a timer for a time that is less than or equal to the current watermark is allowed.
If registered from within eval(), the timer fires on the next watermark advance. If registered
from within onTimer(), the timer fires immediately after the current timer finishes. Note that
unconditionally re-registering a past-time timer from within onTimer() causes an infinite loop.

### Efficiency and Design Principles

Registering too many timers might affect performance. An ever-growing timer state can happen
by an unlimited number of partitions (i.e. an open keyspace) or even within a partition. Thus,
reduce the number of registered timers to a minimum and consider cleaning up timers if they are
not needed anymore via Context#clearAllTimers() or TimeContext#clearTimer(String).

## Ordering

A PTF that takes a table with set semantics can optionally specify an ORDER BY clause in the
function call to define the order in which rows are processed within each partition. The ORDER BY
clause guarantees that rows are delivered to the eval() method in the specified order.

The ORDER BY clause requires that the first column is a time attribute column (i.e., a
TIMESTAMP or TIMESTAMP_LTZ column with a watermark declaration). The first ORDER BY column must
be specified in ascending order. This ensures that rows are processed in event-time order.
Additional columns can be specified as secondary sort keys to define the ordering of
rows with the same timestamp.

  SQL

`[code: SELECT * FROM my_ptf( ... (141 chars)]`

  Java

`[code: env.from("source_table") ... (181 chars)]`

### Difference Between ORDER BY and on_time Argument

While both ORDER BY and the on_time argument relate to time attributes, they serve
different purposes:

- on_time: Declares which time attribute column powers the time context (TimeContext#time()) and
output timestamp. It does NOT affect the processing order of rows.

- ORDER BY: Physically buffers and sorts rows within each partition to guarantee ordered delivery
to the eval() method. If both ORDER BY and on_time are specified for the same table argument, they
must reference the same time attribute column.

### Ordering Guarantees and Late Events

When ORDER BY is specified on a time attribute column, the framework maintains a sort buffer
per partition and input table to reorder out-of-order events. The sort buffer is flushed when the
watermark for the given input table advances, at which point all buffered rows with timestamps
less than or equal to the watermark are delivered to the eval() method in sorted order. Late
events (arriving after the watermark) are dropped to maintain the ordering guarantee.

The following example demonstrates ordered processing with secondary sorting. First, the function implementation:

`[code: // Function that processes events in order and captures the ordering ... (905 chars)]`

The function can be called using either SQL or Table API:

  SQL

`[code: -- Create a watermarked table ... (453 chars)]`

  Java

`[code: TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inS ... (534 chars)]`

In this example:

- Events are first sorted by ts (ascending), ensuring event-time order

- Events with the same timestamp are then sorted by score

- Late events (with timestamp less than the watermark) are automatically dropped

- The TableSemantics API provides runtime access to the ordering configuration

- The output is an ever-growing list of sorted input events

## Multiple Tables

A PTF can process multiple tables simultaneously. This enables a variety of use cases, including:

- Implementing custom joins that efficiently manage state.

- Enriching the main table with information from dimension tables as side inputs.

- Sending control events to the keyed virtual processor during runtime.

The eval() method can specify multiple table arguments to support multiple inputs. All table arguments must be declared
with set semantics and use consistent partitioning. In other words, the number of columns and their data types in the
PARTITION BY clause must match across all involved table arguments.

Rows from either input are passed to the function one at a time. Thus, only one table argument is non-null at a time. Use
null checks to determine which input is currently being processed.

  The system decides which input row is streamed through the virtual processor next. If not handled properly in the PTF,
this can lead to race conditions between inputs and, consequently, to non-deterministic results. It is recommended to
design the function in such a way that the join is either time-based (i.e., waiting for all rows to arrive up to a given
watermark) or condition-based, where the PTF buffers one or more input rows until a specific condition is met.

### Example: Custom Join

The following example illustrates how to implement a custom join between two tables:

  Java

`[code: TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inS ... (1503 chars)]`

The result will look similar to:

`[code: +----+--------------------------------+------------------------------- ... (741 chars)]`

### Efficiency and Design Principles

A high number of input tables can negatively impact a single TaskManager or subtask. Network buffers must be allocated
for each input, resulting in increased memory consumption which is why the number of table arguments is limited to a
maximum of 20 tables.

Unevenly distributed keys may overload a single virtual processor, leading to backpressure. It is important to select
appropriate partition keys.

## Query Evolution with UIDs

Unlike other SQL operators, PTFs support stateful query evolution.

From the planner's perspective, a PTF is a stateful building block that remains unoptimized, while the planner optimizes
the surrounding operators of the query. The state entries of a PTF can be persisted and restored by Flink, even if the
surrounding query or the PTF itself changes. As long as the schema of the state entries remains unchanged.

For future query evolution, the framework enforces a unique identifier (UID) for all PTFs that operate on tables with set
semantics. The UID can be provided through the implicit uid string argument. It is used when persisting the PTF's state
entries to checkpoints or savepoints. If the uid argument is not specified, the function name will be used by the framework,
ensuring one unique PTF invocation per statement. If a PTF is invoked multiple times, validation will require a manually
specified UID to ensure it is unique across the entire Flink job.

Additionally, the UID helps the optimizer determine whether to merge common parts of the pipeline. A shared UID enables
fan-out behavior while maintaining a single stateful PTF.

### Fan-out Example

In the following example, the optimizer detects the shared pipeline part SELECT * FROM f(..., uid => 'same')
in both INSERT INTO statements, allowing it to maintain a single stateful PTF operator. The result of the PTF is then
split and sent to two destinations based on a filter condition.

`[code: EXECUTE STATEMENT SET ... (249 chars)]`

Different UIDs disable this optimization and two stateful blocks are maintained that consume a shared table t.

`[code: EXECUTE STATEMENT SET ... (249 chars)]`

## Pass-Through Columns

Depending on the table semantics and whether an on_time argument has been defined, the system adds additional columns for
every function output.

For table arguments with set semantics, the output is prefixed with the PARTITION BY columns.

For invocations with on_time arguments, the output is suffixed with rowtime.

To summarize, the default pattern is as follows:

`[code: <PARTITION BY keys> | <function output> | <rowtime> ... (51 chars)]`

The ArgumentTrait.PASS_COLUMNS_THROUGH instructs the system to include all columns of a table argument in the output of
the PTF.

Given a table t (containing columns k and v), and a PTF f() (producing columns c1 and c2),
the output of a SELECT * FROM f(table_arg => TABLE t PARTITION BY k) uses the following order:

`[code: Default: | k | c1 | c2 | ... (71 chars)]`

This allows the PTF to focus on the main aggregation without the need to manually forward input columns.

Note: Pass-through columns are only available for append-only PTFs taking a single table argument and don't use timers.

## Updates and Changelogs

By default, PTFs assume that table arguments are backed by append-only tables, where new records are inserted to the table
without any updates to existing records. PTFs then produce new append-only tables as output.

While append-only tables are ideal and work seamlessly with event-time and watermarks, there are scenarios that require
working with updating tables. In these cases, records can be updated or deleted after their initial insertion.
This impacts several aspects:

- State Management: Operations must accommodate the possibility that any record can be updated again, potentially requiring
a larger state footprint.

- Pipeline Complexity: Since records are not final and can be changed subsequently, the entire pipeline result remains
in-flight.

- Downstream Systems: In-flight data can lead to issues, not only in Flink but also in downstream systems where consistency
and finality of data are critical.

  For efficient and high-performance data processing, it is recommended to design pipelines using append-only tables whenever
feasible to simplify state management and avoid complexities associated with updating tables.

A PTF can consume and/or produce updating tables if it is configured to do so. This section provides a brief overview of
CDC (Change Data Capture) with PTFs.

### Change Data Capture Basics

Under the hood, tables in Flink's SQL engine are backed by changelogs. These changelogs encode CDC (Change Data Capture)
information containing INSERT (+I), UPDATE_BEFORE (-U), UPDATE_AFTER (+U), or DELETE (-D) messages.

The existence of these flags in the changelog constitutes the Changelog Mode of a consumer or producer:

Append Mode {+I}

- All messages are insert-only.

- Every insertion message is an immutable fact.

- Messages can be distributed in an arbitrary fashion across partitions and processors because they are unrelated.

Upsert Mode {+I, +U, -D}

- Messages can contain updates leading to an updating table.

- Updates are related using a key (i.e. the upsert key).

- Every message is either an upsert or delete message for a result under the upsert key.

- Messages for the same upsert key should land at the same partition and processor.

- Deletions can contain only values for upsert key columns (i.e. partial deletes) or values for
all columns (i.e. full deletes).

- The mode is also known as partial image in the literature because -U messages are missing.

Retract Mode {+I, -U, +U, -D}

- Messages can contain updates leading to an updating table.

- Every insertion or update event is a fact that can be "undone" (i.e. retracted).

- Updates are related by all columns. In simplified words: The entire row is kind of the key but duplicates are supported.
For example: +I['Bob', 42] is related to -D['Bob', 42] and +U['Alice', 13] is related to -U['Alice', 13].

- Thus, every message is either an insertion (+) or its retraction (-).

- The mode is known as full image in the literature.

### Updating Input Tables

The ArgumentTrait.SUPPORTS_UPDATES instructs the system that updates are allowed as input to the given table argument.
By default, a table argument is insert-only and updates will be rejected.

Input tables become updating when sub queries such as aggregations or outer join force an incremental computation. For
example, the following query only works if the function is able to digest retraction messages:

`[code: // The change +I[1] followed by -U[1], +U[2], -U[2], +U[3] will enter  ... (250 chars)]`

If updates should be supported, ensure that the data type of the table argument is chosen in a way that it can encode
changes. In other words: choose a Row type that exposes the RowKind change flag.

The changelog of the backing input table decides which kinds of changes enter the function. The function receives {+I}
when the input table is append-only. The function receives {+I,+U,-D} if the input table is upserting using the same
upsert key as the partition key. Otherwise, retractions {+I,-U,+U,-D} (i.e. including RowKind.UPDATE_BEFORE) enter
the function. Use ArgumentTrait.REQUIRE_UPDATE_BEFORE to enforce retractions for all updating cases.

For upserting tables, if the changelog contains key-only deletions (also known as partial deletions), only upsert key
fields are set when a row enters the function. Non-key fields are set to null, regardless of NOT NULL constraints.
Use ArgumentTrait.REQUIRE_FULL_DELETE to enforce that only full deletes enter the function.

The SUPPORTS_UPDATES trait is intended for advanced use cases. Please note that inputs are always insert-only in batch
mode. Thus, if the PTF should produce the same results in both batch and streaming mode, results should be emitted based
on watermarks and event-time.

#### Enforcing Retract Mode

The ArgumentTrait.REQUIRE_UPDATE_BEFORE instructs the system that a table argument which SUPPORT_UPDATES should include
a RowKind.UPDATE_BEFORE message when encoding updates. In other words: it enforces presenting the updating table in
retract changelog mode.

By default, updates are encoded as emitted by the input operation. Thus, the updating table might be encoded in upsert
changelog mode and deletes might only contain keys.

The following example shows how the input changelog encodes updates differently:

`[code: // Given a table UpdatingTable(name STRING PRIMARY KEY, score INT) ... (551 chars)]`

#### Enforcing Upserts with Full Deletes

The ArgumentTrait.REQUIRE_FULL_DELETE instructs the system that a table argument which SUPPORTS_UPDATES should include
all fields in the RowKind.DELETE message if the updating table is backed by an upsert changelog.

For upserting tables, if the changelog contains key-only deletes (also known as partial deletes), only upsert key fields
are set when a row enters the function. Non-key fields are set to null, regardless of NOT NULL constraints.

The following example shows how the input changelog encodes updates differently:

`[code: // Given a table UpdatingTable(name STRING PRIMARY KEY, score INT) ... (526 chars)]`

#### Example: Changelog Filtering

The following function demonstrates how a PTF can transform an updating table into an append-only table. Instead of
applying updates encoded in each Row, it incorporates the changelog flag into the payload. The rows emitted by the PTF
are guaranteed to be of RowKind.INSERT. By preserving the original changelog flag in the payload, it permits filtering
of specific update types. In this example, it filters out all deletions.

  Java

`[code: TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inS ... (1439 chars)]`

The PTF produces the following output when debugging in a console. The op section indicates that the result is append-only. The
original flag is encoded in the flag column.

`[code: +----+--------------------------------+------------------------------- ... (608 chars)]`

#### Limitations

- The ArgumentTrait.PASS_COLUMNS_THROUGH is not supported if ArgumentTrait.SUPPORTS_UPDATES is declared.

- The on_time argument is not supported if the PTF receives updates.

### Updating Function Output

The ChangelogFunction interface makes it possible for a function to declare the types of changes (e.g., inserts, updates,
deletes) that it may emit, allowing the planner to make informed decisions during query planning.

  The interface is intended for advanced use cases and should be implemented with care. Emitting an incorrect changelog
from the PTF may lead to undefined behavior in the overall query.

The resulting changelog mode can be influenced by:

- The changelog mode of the input table arguments, accessible via ChangelogContext.getTableChangelogMode(int).

- The changelog mode required by downstream operators, accessible via ChangelogContext.getRequiredChangelogMode().

Changelog mode inference in the planner involves several steps. The getChangelogMode(ChangelogContext) method is
called for each step:

- The planner checks whether the PTF emits updates or inserts-only.

- If updates are emitted, the planner determines whether the updates include {@link
RowKind#UPDATE_BEFORE} messages (retract mode), or whether {@link RowKind#UPDATE_AFTER}
messages are sufficient (upsert mode). For this, {@link #getChangelogMode} might be called
twice to query both retract mode and upsert mode capabilities as indicated by {@link
ChangelogContext#getRequiredChangelogMode()}.

- If in upsert mode, the planner checks whether {@link RowKind#DELETE} messages contain all
fields (full deletes) or only key fields (partial deletes). In the case of partial deletes,
only the upsert key fields are set when a row is removed; all non-key fields are null,
regardless of nullability constraints. {@link ChangelogContext#getRequiredChangelogMode()}
indicates whether a downstream operator requires full deletes.

Emitting changelogs is only valid for PTFs that take table arguments with set semantics (see ArgumentTrait.SET_SEMANTIC_TABLE).
In case of upserts, the upsert key must be equal to the PARTITION BY key.

It is perfectly valid for a ChangelogFunction implementation to return a fixed ChangelogMode, regardless of the
ChangelogContext. This approach may be appropriate when the PTF is designed for a specific scenario or pipeline setup,
and does not need to adapt dynamically to different input modes. Note that in such cases, the PTFs applicability is limited,
as it may only function correctly within the predefined context for which it was designed.

In some cases, this interface should be used in combination with SpecializedFunction
to reconfigure the PTF after the final changelog mode for the specific call location has been
determined. The final changelog mode is also available during runtime via
ProcessTableFunction.Context.getChangelogMode().

#### Example: Custom Aggregation

The following function demonstrates how a PTF can implement an aggregation function that is able to emit updates based
on custom conditional logic. The function takes a table of score results partitioned by name and maintains a sum per
partition. Scores that are lower than 0 are treated as incorrect and invalidate the entire aggregation for this key.

  Java

`[code: TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inS ... (2033 chars)]`

The PTF produces the following output when debugging in a console. The op section indicates that the result is updating. However,
no updates to Bob are forwarded after the invalid -1 is received, causing the PTF to ignore the update with value 45.
The aggregation results for Alice contain only valid scores and are preserved in a materialized table.

`[code: +----+--------------------------------+-------------+ ... (485 chars)]`

#### Limitations

- The on_time argument is not supported if the PTF emits updates.

- Currently, it is difficult to test upsert PTFs because debugging sinks such as collect() operate in retract mode and
will request retract support from any PTF. Upserting PTFs have to be tested with upserting sinks (e.g. kafka-upsert connector).

## Advanced Examples

### Shopping Cart

The following example shows a typical PTF use case for modelling a shopping cart. Different events influence the content
of the cart. In this example, each user might ADD or REMOVE items. In the success case, the user completes the transaction
with CHECKOUT.

Take the following input table:

`[code: +----+--------------------------------+------------------------------- ... (1097 chars)]`

The CheckoutProcessor PTF is designed to process these events and store the shopping cart content in state until checkout
is completed. It also incorporates reminder and timeout logic. If the user remains inactive for a specified duration, a
REMINDER event with the current cart content is emitted. Upon receiving the CHECKOUT event, the PTF is cleared and
the checkout event is sent.

  Java

`[code: // Function that implements the core business logic of a shopping cart ... (2691 chars)]`

The output could look similar to the following. Here we assume a very short reminder interval of 1 second.

`[code: +----+--------------------------------+------------------------------- ... (791 chars)]`

In a real-world scenario, the output would likely be split across two separate systems. Reminders may be placed in an email
notification queue, while the checkout process would be finalized by a separate downstream system.

`[code: CREATE VIEW Checkouts AS SELECT * FROM CheckoutProcessor( ... (453 chars)]`

By defining a view and using the same uid in both INSERT INTO paths, the resulting Flink job uses a split behavior
while the PTF exists once in the pipeline.

### Payment Joining

The following example shows how a PTF can be used for joining. Additionally, it also showcases how a PTF can be used as
a data generator for creating bounded tables with dummy data.

  Java

`[code: // --------------------------- ... (1990 chars)]`

After generating the data, the stateful Joiner buffers events until a matching pair is found. Any duplicates in either
of the input tables are ignored.

  Java

`[code: // Function that buffers one object of each side to find exactly one j ... (1098 chars)]`

The output could look similar to the following. Duplicate events for payment 999997870 have been filtered out. A match
for Charly could not be found.

`[code: +----+-------------+-------------+--------------------------------+--- ... (706 chars)]`

## Limitations

PTFs are in an early stage. The following limitations apply:

- PTFs cannot run in batch mode.

- Broadcast state

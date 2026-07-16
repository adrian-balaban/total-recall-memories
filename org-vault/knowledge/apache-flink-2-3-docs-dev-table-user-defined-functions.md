---
title: Apache Flink 2.3 docs — Dev Table User-Defined Functions
tags: [org, flink, flink-2.3, docs, dev, table-api, udf, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:12:57.163Z'
updated: '2026-07-08T04:12:57.163Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/
# User-defined Functions

User-defined functions (UDFs) are extension points to call frequently used logic or custom logic that cannot be expressed otherwise in queries.

User-defined functions can be implemented in a JVM language (such as Java or Scala) or Python.
An implementer can use arbitrary third party libraries within a UDF.
This page will focus on JVM-based languages, please refer to the PyFlink documentation
for details on writing general
and vectorized UDFs in Python.

## Overview

Currently, Flink distinguishes between the following kinds of functions:

- Scalar functions map scalar values to a new scalar value.

- Asynchronous scalar functions asynchronously map scalar values to a new scalar value.

- Table functions map scalar values to new rows.

- Async Table functions asynchronously map scalar values to new rows and can be used for table sources that perform a lookup.

- Aggregate functions map scalar values of multiple rows to a new scalar value.

- Table aggregate functions map scalar values of multiple rows to new rows.

- Process table functions map tables to new rows. Enabling user-defined operators with state and timers.

The following example shows how to create a simple scalar function and how to call the function in both Table API and SQL.

For SQL queries, a function must always be registered under a name. For Table API, a function can be registered or directly used inline.

  Java

`[code: import org.apache.flink.table.api.*; ... (1023 chars)]`

  Scala

`[code: import org.apache.flink.table.api._ ... (770 chars)]`

For interactive sessions, it is also possible to parameterize functions before using or
registering them. In this case, function instances instead of function classes can be
used as temporary functions.

It requires that the parameters are serializable for shipping
function instances to the cluster.

  Java

`[code: import org.apache.flink.table.api.*; ... (815 chars)]`

  Scala

`[code: import org.apache.flink.table.api._ ... (603 chars)]`

You can use star * expression as one argument of the function call to act as a wildcard in Table API,
all columns in the table will be passed to the function at the corresponding position.

  Java

`[code: import org.apache.flink.table.api.*; ... (798 chars)]`

  Scala

`[code: import org.apache.flink.table.api._ ... (684 chars)]`


TableEnvironment provides two overload methods to create temporary system function with an UserDefinedFunction:

- createTemporarySystemFunction(
String name, Class<? extends UserDefinedFunction> functionClass)

- createTemporarySystemFunction(String name, UserDefinedFunction functionInstance)

It is recommended to use functionClass over functionInstance as far as user-defined functions provide no args constructor,
because Flink as the framework underneath can add more logic to control the process of creating new instance.
Current built-in standard logic in TableEnvironmentImpl will validate the class and methods in the class
based on different subclass types of UserDefinedFunction, e.g. ScalarFunction, TableFunction.
More logic or optimization could be added in the framework in the future with no need to change any users' existing code.

## Implementation Guide

Independent of the kind of function, all user-defined functions follow some basic implementation principles.

### Function Class

An implementation class must extend from one of the available base classes (e.g. org.apache.flink.table.functions.ScalarFunction).

The class must be declared public, not abstract, and should be globally accessible. Thus, non-static inner or anonymous classes are not allowed.

For storing a user-defined function in a persistent catalog, the class must have a default constructor
and must be instantiable during runtime. Anonymous functions in Table API can only be persisted if the
function is not stateful (i.e. containing only transient and static fields).

### Evaluation Methods

The base class provides a set of methods that can be overridden such as open(), close(), isDeterministic() or supportsConstantFolding().

However, in addition to those declared methods, the main runtime logic that is applied to every incoming record must be implemented through specialized evaluation methods.

Depending on the function kind, evaluation methods such as eval(), accumulate(), or retract() are called by code-generated operators during runtime.

The methods must be declared public and take a well-defined set of arguments.

Regular JVM method calling semantics apply. Therefore, it is possible to:

- implement overloaded methods such as eval(Integer) and eval(LocalDateTime),

- use var-args such as eval(Integer...),

- use object inheritance such as eval(Object) that takes both LocalDateTime and Integer,

- and combinations of the above such as eval(Object...) that takes all kinds of arguments.

If you intend to implement functions in Scala, please add the scala.annotation.varargs annotation in
case of variable arguments. Furthermore, it is recommended to use boxed primitives (e.g. java.lang.Integer
instead of Int) to support NULL.

The following snippets shows an example of an overloaded function:

  Java

`[code: import org.apache.flink.table.functions.ScalarFunction; ... (472 chars)]`

  Scala

`[code: import org.apache.flink.table.functions.ScalarFunction ... (485 chars)]`

### Type Inference

The table ecosystem (similar to the SQL standard) is a strongly typed API. Therefore, both function parameters and return types must be mapped to a data type.

From a logical perspective, the planner needs information about expected types, precision, and scale. From a JVM perspective, the planner needs information about how internal data structures are represented as JVM objects when calling a user-defined function.

The logic for validating input arguments and deriving data types for both the parameters and the result of a function is summarized under the term type inference.

Flink's user-defined functions implement an automatic type inference extraction that derives data types from the function's class and its evaluation methods via reflection. If this implicit reflective extraction approach is not successful, the extraction process can be supported by annotating affected parameters, classes, or methods with @DataTypeHint and @FunctionHint. More examples on how to annotate functions are shown below.

If more advanced type inference logic is required, an implementer can explicitly override the getTypeInference() method in every user-defined function. However, the annotation approach is recommended because it keeps custom type inference logic close to the affected locations and falls back to the default behavior for the remaining implementation.

#### Automatic Type Inference

The automatic type inference inspects the function's class and evaluation methods to derive data types for the arguments and result of a function. @DataTypeHint and @FunctionHint annotations support the automatic extraction.

For a full list of classes that can be implicitly mapped to a data type, see the data type extraction section.

@DataTypeHint

In many scenarios, it is required to support the automatic extraction inline for parameters and return types of a function

The following example shows how to use data type hints. More information can be found in the documentation of the annotation class.

  Java

`[code: import org.apache.flink.table.annotation.DataTypeHint; ... (993 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.DataTypeHint ... (996 chars)]`

@FunctionHint

In some scenarios, it is desirable that one evaluation method handles multiple different data types at the same time. Furthermore, in some scenarios, overloaded evaluation methods have a common result type that should be declared only once.

The @FunctionHint annotation can provide a mapping from argument data types to a result data type. It enables annotating entire function classes or evaluation methods for input, accumulator, and result data types. One or more annotations can be declared on top of a class or individually for each evaluation method for overloading function signatures. All hint parameters are optional. If a parameter is not defined, the default reflection-based extraction is used. Hint parameters defined on top of a function class are inherited by all evaluation methods.

The following example shows how to use function hints. More information can be found in the documentation of the annotation class.

  Java

`[code: import org.apache.flink.table.annotation.DataTypeHint; ... (1300 chars)]`

  Scala

`[code:  ... (1341 chars)]`

#### Custom Type Inference

For most scenarios, @DataTypeHint and @FunctionHint should be sufficient to model user-defined functions. However, by overriding the automatic type inference defined in getTypeInference(), implementers can create arbitrary functions that behave like built-in system functions.

The following example implemented in Java illustrates the potential of a custom type inference logic. It uses a string literal argument to determine the result type of a function. The function takes two string arguments: the first argument represents the string to be parsed, the second argument represents the target type.

  Java

`[code: import org.apache.flink.table.api.DataTypes; ... (1716 chars)]`

For more examples of custom type inference, see also the flink-examples-table module with

    advanced function implementation

.

### Named Parameters

When calling a function, you can use parameter names to specify the values of the parameters. Named parameters allow passing both the parameter name and value to a function, avoiding confusion caused by incorrect parameter order and improving code readability and maintainability. In addition, named parameters can also omit optional parameters, which are filled with null by default.
We can use the @ArgumentHint annotation to specify the name, type, and whether a parameter is required or not.

@ArgumentHint

The following 3 examples demonstrate how to use @ArgumentHint in different scopes. More information can be found in the documentation of the annotation class.

- Using @ArgumentHint annotation on the parameters of the eval method of the function.

  Java

`[code: import com.sun.tracing.dtrace.ArgsAttributes; ... (599 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.ArgumentHint; ... (572 chars)]`

- Using @ArgumentHint annotation on the eval method of the function.

  Java

`[code: import org.apache.flink.table.annotation.ArgumentHint; ... (586 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.ArgumentHint; ... (689 chars)]`

- Using @ArgumentHint annotation on the class of the function.

  Java

`[code: import org.apache.flink.table.annotation.ArgumentHint; ... (682 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.ArgumentHint; ... (675 chars)]`


- @ArgumentHint annotation already contains @DataTypeHint annotation, so it cannot be used together with @DataTypeHint in @FunctionHint. When applied to function parameters, @ArgumentHint cannot be used with @DataTypeHint at the same time, and it is recommended to use @ArgumentHint.

- Named parameters only take effect when the corresponding class does not contain overloaded functions and variable parameter functions, otherwise using named parameters will cause an error.

### Determinism

Every user-defined function class can declare whether it produces deterministic results or not by overriding
the isDeterministic() method. If the function is not purely functional (like random(), date(), or now()),
the method must return false. By default, isDeterministic() returns true.

Furthermore, the isDeterministic() method might also influence the runtime behavior. A runtime
implementation might be called at two different stages:

1. During planning (i.e. pre-flight phase): If a function is called with constant expressions
or constant expressions can be derived from the given statement, a function is pre-evaluated
for constant expression reduction and might not be executed on the cluster anymore. Unless
isDeterministic() is used to disable constant expression reduction in this case. For example,
the following calls to ABS are executed during planning: SELECT ABS(-1) FROM t and
SELECT ABS(field) FROM t WHERE field = -1; whereas SELECT ABS(field) FROM t is not.

2. During runtime (i.e. cluster execution): If a function is called with non-constant expressions
or isDeterministic() returns false.

#### System (Built-in) Function Determinism

The determinism of system (built-in) functions are immutable. There exists two kinds of functions which are not deterministic:
dynamic function and non-deterministic function, according to Apache Calcite's SqlOperator definition:

`[code:   /** ... (409 chars)]`

isDeterministic indicates the determinism of a function, will be evaluated per record during runtime if returns false.
isDynamicFunction implies the function can only be evaluated at query-start if returns true,
it will be only pre-evaluated during planning for batch mode, while for streaming mode, it is equivalent to a non-deterministic
function because of the query is continuously being executed logically(the abstraction of continuous query over the dynamic tables),
so the dynamic functions are also re-evaluated for each query execution(equivalent to per record in current implementation).

The following system functions are always non-deterministic(evaluated per record during runtime both in batch and streaming mode):

- UUID

- RAND

- RAND_INTEGER

- CURRENT_DATABASE

- UNIX_TIMESTAMP

- CURRENT_ROW_TIMESTAMP

The following system temporal functions are dynamic, which will be pre-evaluated during planning(query-start) for batch mode and evaluated per record for streaming mode:

- CURRENT_DATE

- CURRENT_TIME

- CURRENT_TIMESTAMP

- NOW

- LOCALTIME

- LOCALTIMESTAMP

Note: isDynamicFunction is only applicable for system functions.

### Constant Expression Reduction

User-defined functions can declare whether they allow for constant expression reduction by
overriding the method supportsConstantFolding(). Calls to functions with constant arguments can be
reduced and simplified in some cases. An example could be the user-defined function call PlusOne(10) might just be simplified to 11 in an
expression. This optimization happens at planning time, resulting in a plan only utilizing the
reduced value. This generally is desirable, and therefore is enabled by default, though there are some
cases where it should be disabled.

One is if the function call is not deterministic, which is covered in more detail in the
Determinism section above. Setting a function as non deterministic will have the effect of
preventing function call expression reduction, even if supportsConstantFolding() is true.

A function call may also have some side effects, even if it always returns deterministic results.
This may mean that the correctness of the query within Flink may allow for constant expression reduction, but it may not
be desired anyway. In this case, setting the method supportsConstantFolding() to return false also
has the effect of preventing constant expression reduction and ensuring invocation at runtime.

### Runtime Integration

Sometimes it might be necessary for a user-defined function to get global runtime information or do some setup/clean-up work before the actual work. User-defined functions provide open() and close() methods that can be overridden and provide similar functionality as the methods in RichFunction of DataStream API.

The open() method is called once before the evaluation method. The close() method after the last call to the evaluation method.

The open() method provides a FunctionContext that contains information about the context in which user-defined functions are executed, such as the metric group, the distributed cache files, or the global job parameters.

The following information can be obtained by calling the corresponding methods of FunctionContext:

[table: 5 rows]

Note: Depending on the context in which the function is executed, not all methods from above might be available. For example,
during constant expression reduction adding a metric is a no-op operation.

The following example snippet shows how to use FunctionContext in a scalar function for accessing a global job parameter:

  Java

`[code: import org.apache.flink.table.api.*; ... (939 chars)]`

  Scala

`[code: import org.apache.flink.table.api._ ... (841 chars)]`

## Scalar Functions

A user-defined scalar function maps zero, one, or multiple scalar values to a new scalar value. Any data type listed in the data types section can be used as a parameter or return type of an evaluation method.

In order to define a scalar function, one has to extend the base class ScalarFunction in org.apache.flink.table.functions and implement one or more evaluation methods named eval(...).

The following example shows how to define your own hash code function and call it in a query. See the Implementation Guide for more details.

  Java

`[code: import org.apache.flink.table.annotation.InputGroup; ... (881 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.InputGroup ... (785 chars)]`

If you intend to implement or call functions in Python, please refer to the Python Scalar Functions documentation for more details.

## Asynchronous Scalar Functions

When interacting with external systems (for example when enriching stream events with data stored in a database), one needs to take care that network or other latency does not dominate the streaming application's running time.

Naively accessing data in the external database, for example using a ScalarFunction, typically means synchronous interaction: A request is sent to the database and the ScalarFunction waits until the response has been received. In many cases, this waiting makes up the vast majority of the function's time.

To address this inefficiency, there is an AsyncScalarFunction. Asynchronous interaction with the database means that a single function instance can handle many requests concurrently and receive the responses concurrently. That way, the waiting time can be overlaid with sending other requests and receiving responses. At the very least, the waiting time is amortized over multiple requests. This leads in most cases to much higher streaming throughput.

#### Defining an AsyncScalarFunction

A user-defined asynchronous scalar function maps zero, one, or multiple scalar values to a new scalar value. Any data type listed in the data types section can be used as a parameter or return type of an evaluation method.

In order to define an asynchronous scalar function, extend the base class AsyncScalarFunction in org.apache.flink.table.functions and implement one or more evaluation methods named eval(...).  The first argument must be a CompletableFuture<...> which is used to return the result, with subsequent arguments being the parameters passed to the function.

The number of outstanding calls to eval may be configured by table.exec.async-scalar.max-concurrent-operations.

The following example shows how to do work on a thread pool in the background, though any libraries exposing an async interface may be directly used to complete the CompletableFuture from a callback. See the Implementation Guide for more details.

`[code: import org.apache.flink.table.api.*; ... (2658 chars)]`

#### Asynchronous Semantics

While calls to an AsyncScalarFunction may be completed out of the original input order, to maintain correct semantics, the outputs of the function are guaranteed to maintain that input order to downstream components of the query. The data itself could reveal completion order (e.g. by containing fetch timestamps), so the user should consider whether this is acceptable for their use-case.

#### Error Handling

The primary way for a user to indicate an error is to call CompletableFuture.completeExceptionally(Throwable). Similarly, if an exception is encountered by the system when invoking eval, that will also result in an error. When an error occurs, the system will consider the retry strategy, configured by table.exec.async-scalar.retry-strategy. If this is NO_RETRY, the job is failed. If it is set to FIXED_DELAY, a period of table.exec.async-scalar.retry-delay will be waited, and the function call will be retried. If there have been table.exec.async-scalar.max-attempts failed attempts or if the timeout table.exec.async-scalar.timeout expires (including all retry attempts), the job will fail.

#### AsyncScalarFunction vs. ScalarFunction

One thing to consider is if the UDF contains CPU intensive logic with no blocking calls. If so, it likely doesn't require asynchronous functionality and could use a ScalarFunction. If the logic involves waiting for things like network or background operations (e.g. database lookups, RPCs, or REST calls), this may be a useful way to speed things up. There are also some queries that don't support AsyncScalarFunction, so when in doubt, ScalarFunction should be used.

## Table Functions

Similar to a user-defined scalar function, a user-defined table function (UDTF) takes zero, one, or multiple scalar values as input arguments. However, it can return an arbitrary number of rows (or structured types) as output instead of a single value. The returned record may consist of one or more fields. If an output record consists of only a single field, the structured record can be omitted, and a scalar value can be emitted that will be implicitly wrapped into a row by the runtime.

In order to define a table function, one has to extend the base class TableFunction in org.apache.flink.table.functions and implement one or more evaluation methods named eval(...). Similar to other functions, input and output data types are automatically extracted using reflection. This includes the generic argument T of the class for determining an output data type. In contrast to scalar functions, the evaluation method itself must not have a return type, instead, table functions provide a collect(T) method that can be called within every evaluation method for emitting zero, one, or more records.

In the Table API, a table function is used with .joinLateral(...) or .leftOuterJoinLateral(...). The joinLateral operator (cross) joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator). The leftOuterJoinLateral operator joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator) and preserves outer rows for which the table function returns an empty table.

In SQL, use LATERAL TABLE(<TableFunction>) with JOIN or LEFT JOIN with an ON TRUE join condition.

The following example shows how to define your own split function and call it in a query. See the Implementation Guide for more details.

  Java

`[code: import org.apache.flink.table.annotation.DataTypeHint; ... (2059 chars)]`

  Scala

`[code: import org.apache.flink.table.annotation.DataTypeHint ... (1923 chars)]`

If you intend to implement functions in Scala, do not implement a table function as a Scala object. Scala objects are singletons and will cause concurrency issues.

If you intend to implement or call functions in Python, please refer to the Python Table Functions documentation for more details.

## Asynchronous Table Functions

Similar to AsyncScalarFunction, there also exists an AsyncTableFunction for returning multiple row results rather than a single scalar value. Similarly, this is most useful when interacting with external systems (for example when enriching stream events with data stored in a database).

Asynchronous interaction with an external system means that a single function instance can handle many requests concurrently and receive the responses concurrently. That way, the waiting time can be overlaid with sending other requests and receiving responses. At the very least, the waiting time is amortized over multiple requests. This leads in most cases to much higher streaming throughput.

#### Defining an AsyncTableFunction

A user-defined asynchronous table function maps zero, one, or multiple scalar values to zero, one, or multiple Rows. Any data type listed in the data types section can be used as a parameter or return type of an evaluation method.

In order to define an asynchronous table function, extend the base class AsyncTableFunction in org.apache.flink.table.functions and implement one or more evaluation methods named eval(...).  The first argument must be a CompletableFuture<...> which is used to return the result, with subsequent arguments being the parameters passed to the function.

The number of outstanding calls to eval may be configured by table.exec.async-table.max-concurrent-operations.

#### Asynchronous Semantics

While calls to an AsyncTableFunction may be completed out of the original input order, to maintain correct semantics, the outputs of the function are guaranteed to maintain that input order to downstream components of the query. The data itself could reveal completion order (e.g. by containing fetch timestamps), so the user should consider whether this is acceptable for their use-case.

#### Error Handling

The primary way for a user to indicate an error is to call completableFuture.completeExceptionally(throwable). Similarly, if an exception is encountered by the system when invoking eval, that will also result in an error. When an error occurs, the system will consider the retry strategy, configured by table.exec.async-table.retry-strategy. If this is NO_RETRY, the job is failed. If it is set to FIXED_DELAY, a period of table.exec.async-table.retry-delay will be waited, and the function call will be retried. If there have been table.exec.async-table.max-attempts failed attempts or if the timeout table.exec.async-table.timeout expires (including all retry attempts), the job will fail.

The following example shows how to do work on a thread pool in the background, though any libraries exposing an async interface may be directly used to complete the CompletableFuture from a callback. See the Implementation Guide for more details.

`[code: import org.apache.flink.table.api.*; ... (1834 chars)]`

## Aggregate Functions

A user-defined aggregate function (UDAGG) maps scalar values of multiple rows to a new scalar value.

The behavior of an aggregate function is centered around the concept of an accumulator. The accumulator
is an intermediate data structure that stores the aggregated values until a final aggregation result
is computed.

For each set of rows that needs to be aggregated, the runtime will create an empty accumulator by calling
createAccumulator(). Subsequently, the accumulate(...) method of the function is called for each input
row to update the accumulator. Once all rows have been processed, the getValue(...) method of the
function is called to compute and return the final result.

The following example illustrates the aggregation process:

In the example, we assume a table that contains data about beverages. The table consists of three columns (id, name,
and price) and 5 rows. We would like to find the highest price of all beverages in the table, i.e., perform
a max() aggregation. We need to consider each of the 5 rows. The result is a single numeric value.

In order to define an aggregate function, one has to extend the base class AggregateFunction in
org.apache.flink.table.functions and implement one or more evaluation methods named accumulate(...).
An accumulate method must be declared publicly and not static. Accumulate methods can also be overloaded
by implementing multiple methods named accumulate.

By default, input, accumulator, and output data types are automatically extracted using reflection. This
includes the generic argument ACC of the class for determining an accumulator data type and the generic
argument T for determining an accumulator data type. Input arguments are derived from one or more
accumulate(...) methods. See the Implementation Guide for more details.

If you intend to implement or call functions in Python, please refer to the Python Functions
documentation for more details.

The following example shows how to define your own aggregate function and call it in a query.

  Java

`[code: import org.apache.flink.table.api.*; ... (2073 chars)]`

  Scala

`[code: import org.apache.flink.table.api._ ... (2036 chars)]`

The accumulate(...) method of our WeightedAvg class takes three inputs. The first one is the accumulator
and the other two are user-defined inputs. In order to calculate a weighted average value, the accumulator
needs to store the weighted sum and count of all the data that has been accumulated. In our example, we
define a class WeightedAvgAccumulator to be the accumulator. Accumulators are automatically managed
by Flink's checkpointing mechanism and are restored in case of a failure to ensure exactly-once semantics.

### Mandatory and Optional Methods

The following methods are mandatory for each AggregateFunction:

- createAccumulator()

- accumulate(...)

- getValue(...)

Additionally, there are a few methods that can be optionally implemented. While some of these methods
allow the system more efficient query execution, others are mandatory for certain use cases. For instance,
the merge(...) method is mandatory if the aggregation function should be applied in the context of a
session group window (the accumulators of two session windows need to be joined when a row is observed
that "connects" them).

The following methods of AggregateFunction are required depending on the use case:

- retract(...) is required for aggregations on OVER windows.

- merge(...) is required for many bounded aggregations and session window and hop window aggregations. Besides, this method is also helpful for optimizations. For example, two phase aggregation optimization requires all the AggregateFunction support merge method.

If the aggregate function can only be applied in an OVER window, this can be declared by returning the
requirement FunctionRequirement.OVER_WINDOW_ONLY in getRequirements().

If an accumulator needs to store large amounts of data, org.apache.flink.table.api.dataview.ListView
and org.apache.flink.table.api.dataview.MapView provide advanced features for leveraging Flink's state
backends in unbounded data scenarios. Please see the docs of the corresponding classes for more information
about this advanced feature.

Since some of the methods are optional, or can be overloaded, the runtime invokes aggregate function
methods via generated code. This means the base class does not always provide a signature to be overridden
by the concrete implementation. Nevertheless, all mentioned methods must be declared publicly, not static,
and named exactly as the names mentioned above to be called.

Detailed documentation for all methods that are not declared in AggregateFunction and called by generated
code is given below.

accumulate(...)

  Java

`[code: /* ... (487 chars)]`

  Scala

`[code: /* ... (486 chars)]`

retract(...)

  Java

`[code: /* ... (577 chars)]`

  Scala

`[code: /* ... (576 chars)]`

merge(...)

  Java

`[code: /* ... (711 chars)]`

  Scala

`[code: /* ... (711 chars)]`

If you intend to implement or call functions in Python, please refer to the Python Aggregate Functions documentation for more details.

## Table Aggregate Functions

A user-defined table aggregate function (UDTAGG) maps scalar values of multiple rows to zero, one,
or multiple rows (or structured types). The returned record may consist of one or more fields. If an
output record consists of only a single field, the structured record can be omitted, and a scalar value
can be emitted that will be implicitly wrapped into a row by the runtime.

Similar to an aggregate function, the behavior of a table aggregate is centered
around the concept of an accumulator. The accumulator is an intermediate data structure that stores
the aggregated values until a final aggregation result is computed.

For each set of rows that needs to be aggregated, the runtime will create an empty accumulator by calling
createAccumulator(). Subsequently, the accumulate(...) method of the function is called for each
input row to update the accumulator. Once all rows have been processed, the emitValue(...) or emitUpdateWithRetract(...)
method of the function is called to compute and return the final result.

The following example illustrates the aggregation process:

In the example, we assume a table that contains data about beverages. The table consists of three columns (id, name,
and price) and 5 rows. We would like to find the 2 highest prices of all beverages in the table, i.e.,
perform a TOP2() table aggregation. We need to consider each of the 5 rows. The result is a table
with the top 2 values.

In order to define a table aggregate function, one has to extend the base class TableAggregateFunction in
org.apache.flink.table.functions and implement one or more evaluation methods named accumulate(...).
An accumulate method must be declared publicly and not static. Accumulate methods can also be overloaded
by implementing multiple methods named accumulate.

By default, input, accumulator, and output data types are automatically extracted using reflection. This
includes the generic argument ACC of the class for determining an accumulator data type and the generic
argument T for determining an accumulator data type. Input arguments are derived from one or more
accumulate(...) methods. See the Implementation Guide for more details.

If you intend to implement or call functions in Python, please refer to the Python Functions
documentation for more details.

The following example shows how to define your own table aggregate function and call it in a query.

  Java

`[code: import org.apache.flink.api.java.tuple.Tuple2; ... (2458 chars)]`

  Scala

`[code: import java.lang.Integer ... (2340 chars)]`

The accumulate(...) method of our Top2 class takes two inputs. The first one is the accumulator
and the second one is the user-defined input. In order to calculate a result, the accumulator needs to
store the 2 highest values of all the data that has been accumulated. Accumulators are automatically managed
by Flink's checkpointing mechanism and are restored in case of a failure to ensure exactly-once semantics.
The result values are emitted together with a ranking index.

### Mandatory and Optional Methods

The following methods are mandatory for each TableAggregateFunction:

- createAccumulator()

- accumulate(...)

- emitValue(...) or emitUpdateWithRetract(...)

Additionally, there are a few methods that can be optionally implemented. While some of these methods
allow the system more efficient query execution, others are mandatory for certain use cases. For instance,
the merge(...) method is mandatory if the aggregation function should be applied in the context of a
session group window (the accumulators of two session windows need to be joined when a row is observed
that "connects" them).

The following methods of TableAggregateFunction are required depending on the use case:

- retract(...) is required for aggregations on OVER windows.

- merge(...) is required for many bounded aggregations and unbounded session and hop window aggregations.

- emitValue(...) is required for bounded and window aggregations.

The following methods of TableAggregateFunction are used to improve the performance of streaming jobs:

- emitUpdateWithRetract(...) is used to emit values that have been updated under retract mode.

The emitValue(...) method always emits the full data according to the accumulator. In unbounded scenarios,
this may bring performance problems. Take a Top N function as an example. The emitValue(...) would emit
all N values each time. In order to improve the performance, one can implement emitUpdateWithRetract(...) which
outputs data incrementally in retract mode. In other words, once there is an update, the method can retract
old records before sending new, updated ones. The method will be used in preference to the emitValue(...)
method.

If the table aggregate function can only be applied in an OVER window, this can be declared by returning the
requirement FunctionRequirement.OVER_WINDOW_ONLY in getRequirements().

If an accumulator needs to store large amounts of data, org.apache.flink.table.api.dataview.ListView
and org.apache.flink.table.api.dataview.MapView provide advanced features for leveraging Flink's state
backends in unbounded data scenarios. Please see the docs of the corresponding classes for more information
about this advanced feature.

Since some of methods are optional or can be overloaded, the methods are called by generated code. The
base class does not always provide a signature to be overridden by the concrete implementation class. Nevertheless,
all mentioned methods must be declared publicly, not static, and named exactly as the names mentioned above
to be called.

Detailed documentation for all methods that are not declared in TableAggregateFunction and called by generated
code is given below.

accumulate(...)

  Java

`[code: /* ... (487 chars)]`

  Scala

`[code: /* ... (486 chars)]`

retract(...)

  Java

`[code: /* ... (577 chars)]`

  Scala

`[code: /* ... (576 chars)]`

merge(...)

  Java

`[code: /* ... (711 chars)]`

  Scala

`[code: /* ... (711 chars)]`

emitValue(...)

  Java

`[code: /* ... (472 chars)]`

  Scala

`[code: /* ... (472 chars)]`

emitUpdateWithRetract(...)

  Java

`[code: /* ... (1218 chars)]`

  Scala

`[code: /* ... (1218 chars)]`

### Retraction Example

The following example shows how to use the emitUpdateWithRetract(...) method to emit only incremental
updates. In order to do so, the accumulator keeps both the old and new top 2 values.

  Note: Do not update accumulator within emitUpdateWithRetract because after function#emitUpdateWithRetract is invoked, GroupTableAggFunction will not re-invoke function#getAccumulators to update the latest accumulator to state.

  Java

`[code: import org.apache.flink.api.java.tuple.Tuple2; ... (1675 chars)]`

  Scala

`[code: import org.apache.flink.api.java.tuple.Tuple2 ... (1643 chars)]`

## Process Table Functions

Process Table Functions (PTFs) are the most powerful function kind for Flink SQL and Table API. They enable implementing
user-defined operators that can be as feature-rich as built-in operations. PTFs can take (partitioned) tables to produce
a new table. They have access to Flink's managed state, event-time and timer services, and underlying table changelogs.

Conceptually, a PTF is a superset of all other user-defined functions. It maps zero, one, or multiple tables to zero, one,
or multiple rows (or structured types). Scalar arguments are supported. Due to its stateful nature, implementing aggregating
behavior is possible as well.

A PTF enables the following tasks:

- Apply transformations on each row of a table.

- Logically partition the table into distinct sets and apply transformations per set.

- Store seen events for repeated access.

- Continue the processing at a later point in time enabling waiting, synchronization, or timeouts.

- Buffer and aggregate events using complex state machines or rule-based conditional logic.

See the dedicated page for PTFs for more details.
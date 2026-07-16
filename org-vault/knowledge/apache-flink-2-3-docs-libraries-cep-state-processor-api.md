---
title: 'Apache Flink 2.3 docs — Libraries (CEP, State Processor API)'
tags: [org, flink, flink-2.3, docs, libs, cep, complex-event-processing, state-processor-api, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:34:31.181Z'
updated: '2026-07-08T04:34:31.181Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — Libraries (CEP, State Processor API)

**Executive summary:** Two Flink 2.3 library pages captured near-verbatim (code condensed to `[code: ...]`, tables to `[table: N rows]`). WHY: serve as faithful reference for the CEP Pattern API (detect event patterns in unbounded streams) and the State Processor API (read/write/modify savepoints & checkpoints offline via DataStream/Table API under BATCH execution). Sources: nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/cep/ and .../libs/state_processor_api/. English content only. Part of the [[apache-flink-2-3-docs-*]] series (this = #30, the libs section, indices 169-170). Related: state/operator semantics covered in dev-datastream memories #20-29; savepoint/checkpoint mechanics in #23.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/cep/
# FlinkCEP - Complex event processing for Flink

FlinkCEP is the Complex Event Processing (CEP) library implemented on top of Flink. It allows you to detect event patterns in an endless stream of events, giving you the opportunity to get hold of what's important in your data.

This page describes the API calls available in Flink CEP. We start by presenting the Pattern API, which allows you to specify the patterns that you want to detect in your stream, before presenting how you can detect and act upon matching event sequences. We then present the assumptions the CEP library makes when dealing with lateness in event time and how you can migrate your job from an older Flink version to Flink-1.13.

## Getting Started

If you want to jump right in, set up a Flink program and add the FlinkCEP dependency to the pom.xml of your project.

  Java
`[code: <dependency> ... (134 chars)]`
  Copied to clipboard!

FlinkCEP is not part of the binary distribution. See how to link with it for cluster execution here.

Now you can start writing your first CEP program using the Pattern API.

  The events in the DataStream to which you want to apply pattern matching must implement proper equals() and hashCode() methods because FlinkCEP uses them for comparing and matching events.

`[code: DataStream<Event> input = ...; ... (791 chars)]`

## The Pattern API

The pattern API allows you to define complex pattern sequences that you want to extract from your input stream.

Each complex pattern sequence consists of multiple simple patterns, i.e. patterns looking for individual events with the same properties. From now on, we will call these simple patterns patterns, and the final complex pattern sequence we are searching for in the stream, the pattern sequence. You can see a pattern sequence as a graph of such patterns, where transitions from one pattern to the next occur based on user-specified conditions, e.g. event.getName().equals("end"). A match is a sequence of input events which visits all patterns of the complex pattern graph, through a sequence of valid pattern transitions.

  Each pattern must have a unique name, which you use later to identify the matched events.
  Pattern names CANNOT contain the character ":".

In the rest of this section we will first describe how to define Individual Patterns, and then how you can combine individual patterns into Complex Patterns.

### Individual Patterns

A Pattern can be either a singleton or a looping pattern. Singleton patterns accept a single event, while looping patterns can accept more than one. In pattern matching symbols, the pattern "a b+ c? d" (or "a", followed by one or more "b"'s, optionally followed by a "c", followed by a "d"), a, c?, and d are singleton patterns, while b+ is a looping one. By default, a pattern is a singleton pattern and you can transform it to a looping one by using Quantifiers. Each pattern can have one or more Conditions based on which it accepts events.

#### Quantifiers

In FlinkCEP, you can specify looping patterns using these methods: pattern.oneOrMore(), for patterns that expect one or more occurrences of a given event (e.g. the b+ mentioned before); and pattern.times(#ofTimes), for patterns that expect a specific number of occurrences of a given type of event, e.g. 4 a's; and pattern.times(#fromTimes, #toTimes), for patterns that expect a specific minimum number of occurrences and a maximum number of occurrences of a given type of event, e.g. 2-4 as.

You can make looping patterns greedy using the pattern.greedy() method, but you cannot yet make group patterns greedy. You can make all patterns, looping or not, optional using the pattern.optional() method.

For a pattern named start, the following are valid quantifiers:
`[code: // expecting 4 occurrences ... (1109 chars)]`

#### Conditions

For every pattern you can specify a condition that an incoming event has to meet in order to be "accepted" into the pattern e.g. its value should be larger than 5, or larger than the average value of the previously accepted events. You can specify conditions on the event properties via the pattern.where(), pattern.or() or pattern.until() methods. These can be either IterativeConditions or SimpleConditions.

Iterative Conditions: This is the most general type of condition. This is how you can specify a condition that accepts subsequent events based on properties of the previously accepted events or a statistic over a subset of them.

Below is the code for an iterative condition that accepts the next event for a pattern named "middle" if its name starts with "foo", and if the sum of the prices of the previously accepted events for that pattern plus the price of the current event do not exceed the value of 5.0. Iterative conditions can be powerful, especially in combination with looping patterns, e.g. oneOrMore().

`[code: middle.oneOrMore() ... (537 chars)]`

  The call to ctx.getEventsForPattern(...) finds all the previously accepted events for a given potential match. The cost of this operation can vary, so when implementing your condition, try to minimize its use.

Described context gives one access to event time characteristics as well. For more info see Time context.

Simple Conditions: This type of condition extends the aforementioned IterativeCondition class and decides whether to accept an event or not, based only on properties of the event itself.

`[code: start.where(SimpleCondition.of(value -> value.getName().startsWith("fo ... (76 chars)]`

Finally, you can also restrict the type of the accepted event to a subtype of the initial event type (here Event) via the pattern.subtype(subClass) method.

`[code: start.subtype(SubEvent.class) ... (98 chars)]`

Combining Conditions: As shown above, you can combine the subtype condition with additional conditions. This holds for every condition. You can arbitrarily combine conditions by sequentially calling where(). The final result will be the logical AND of the results of the individual conditions. To combine conditions using OR, you can use the or() method, as shown below.

`[code: pattern.where(SimpleCondition.of(value -> ... /*some condition*/)) ... (132 chars)]`

Stop condition: In case of looping patterns (oneOrMore() and oneOrMore().optional()) you can also specify a stop condition, e.g. accept events with value larger than 5 until the sum of values is smaller than 50.

To better understand it, have a look at the following example. Given
- pattern like "(a+ until b)" (one or more "a" until "b")
- a sequence of incoming events "a1" "c" "a2" "b" "a3"
- the library will output results: {a1 a2} {a1} {a2} {a3}.

As you can see {a1 a2 a3} or {a2 a3} are not returned due to the stop condition.

#### where(condition)
Defines a condition for the current pattern. To match the pattern, an event must satisfy the condition. Multiple consecutive where() clauses lead to their conditions being ANDed.
`[code: pattern.where(new IterativeCondition<Event>() { ... (180 chars)]`

#### or(condition)
Adds a new condition which is ORed with an existing one. An event can match the pattern only if it passes at least one of the conditions.
`[code: pattern.where(new IterativeCondition<Event>() { ... (356 chars)]`

#### until(condition)
Specifies a stop condition for a looping pattern. Meaning if event matching the given condition occurs, no more events will be accepted into the pattern. Applicable only in conjunction with oneOrMore(). NOTE: It allows for cleaning state for corresponding pattern on event-based condition.
`[code: pattern.oneOrMore().until(new IterativeCondition<Event>() { ... (199 chars)]`

#### subtype(subClass)
Defines a subtype condition for the current pattern. An event can only match the pattern if it is of this subtype.
`[code: pattern.subtype(SubEvent.class); ... (32 chars)]`

#### oneOrMore()
Specifies that this pattern expects at least one occurrence of a matching event. By default a relaxed internal contiguity (between subsequent events) is used. For more info on internal contiguity see consecutive. It is advised to use either until() or within() to enable state clearing.
`[code: pattern.oneOrMore(); ... (20 chars)]`

#### timesOrMore(#times)
Specifies that this pattern expects at least #times occurrences of a matching event. By default a relaxed internal contiguity (between subsequent events) is used. For more info on internal contiguity see consecutive.
`[code: pattern.timesOrMore(2); ... (23 chars)]`

#### times(#ofTimes)
Specifies that this pattern expects an exact number of occurrences of a matching event. By default a relaxed internal contiguity (between subsequent events) is used. For more info on internal contiguity see consecutive.
`[code: pattern.times(2); ... (17 chars)]`

#### times(#fromTimes, #toTimes)
Specifies that this pattern expects occurrences between #fromTimes and #toTimes of a matching event. By default a relaxed internal contiguity (between subsequent events) is used. For more info on internal contiguity see consecutive.
`[code: pattern.times(2, 4); ... (20 chars)]`

#### optional()
Specifies that this pattern is optional, i.e. it may not occur at all. This is applicable to all aforementioned quantifiers.
`[code: pattern.oneOrMore().optional(); ... (31 chars)]`

#### greedy()
Specifies that this pattern is greedy, i.e. it will repeat as many as possible. This is only applicable to quantifiers and it does not support group pattern currently.
`[code: pattern.oneOrMore().greedy(); ... (29 chars)]`

### Combining Patterns

Now that you've seen what an individual pattern can look like, it is time to see how to combine them into a full pattern sequence.

A pattern sequence has to start with an initial pattern, as shown below:
`[code: Pattern<Event, ?> start = Pattern.<Event>begin("start"); ... (56 chars)]`

Next, you can append more patterns to your pattern sequence by specifying the desired contiguity conditions between them. FlinkCEP supports the following forms of contiguity between events:

- Strict Contiguity: Expects all matching events to appear strictly one after the other, without any non-matching events in-between.
- Relaxed Contiguity: Ignores non-matching events appearing in-between the matching ones.
- Non-Deterministic Relaxed Contiguity: Further relaxes contiguity, allowing additional matches that ignore some matching events.

To apply them between consecutive patterns, you can use:
- next(), for strict,
- followedBy(), for relaxed, and
- followedByAny(), for non-deterministic relaxed contiguity.

or
- notNext(), if you do not want an event type to directly follow another
- notFollowedBy(), if you do not want an event type to be anywhere between two other event types.

  A pattern sequence cannot end with notFollowedBy() if the time interval is not defined via withIn().
  A NOT pattern cannot be preceded by an optional one.

`[code:  ... (498 chars)]`

Relaxed contiguity means that only the first succeeding matching event will be matched, while with non-deterministic relaxed contiguity, multiple matches will be emitted for the same beginning. As an example, a pattern "a b", given the event sequence "a", "c", "b1", "b2", will give the following results:
- Strict Contiguity between "a" and "b": {} (no match), the "c" after "a" causes "a" to be discarded.
- Relaxed Contiguity between "a" and "b": {a b1}, as relaxed continuity is viewed as "skip non-matching events till the next matching one".
- Non-Deterministic Relaxed Contiguity between "a" and "b": {a b1}, {a b2}, as this is the most general form.

It's also possible to define a temporal constraint for the pattern to be valid. For example, you can define that a pattern should occur within 10 seconds via the pattern.within() method. Temporal patterns are supported for both processing and event time.

  A pattern sequence can only have one temporal constraint. If multiple such constraints are defined on different individual patterns, then the smallest is applied.

`[code: next.within(Duration.ofSeconds(10)); ... (36 chars)]`

Notice that a pattern sequence can end with notFollowedBy() with temporal constraint E.g. a pattern like:
`[code: Pattern.<Event>begin("start") ... (250 chars)]`

#### Contiguity within looping patterns

You can apply the same contiguity condition as discussed in the previous section within a looping pattern. The contiguity will be applied between elements accepted into such a pattern. To illustrate the above with an example, a pattern sequence "a b+ c" ("a" followed by any(non-deterministic relaxed) sequence of one or more "b"'s followed by a "c") with input "a", "b1", "d1", "b2", "d2", "b3" "c" will have the following results:
- Strict Contiguity: {a b1 c}, {a b2 c}, {a b3 c} - there are no adjacent "b"s.
- Relaxed Contiguity: {a b1 c}, {a b1 b2 c}, {a b1 b2 b3 c}, {a b2 c}, {a b2 b3 c}, {a b3 c} - "d"'s are ignored.
- Non-Deterministic Relaxed Contiguity: {a b1 c}, {a b1 b2 c}, {a b1 b3 c}, {a b1 b2 b3 c}, {a b2 c}, {a b2 b3 c}, {a b3 c} - notice the {a b1 b3 c}, which is the result of relaxing contiguity between "b"'s.

For looping patterns (e.g. oneOrMore() and times()) the default is relaxed contiguity. If you want strict contiguity, you have to explicitly specify it by using the consecutive() call, and if you want non-deterministic relaxed contiguity you can use the allowCombinations() call.

#### consecutive()
Works in conjunction with oneOrMore() and times() and imposes strict contiguity between the matching events, i.e. any non-matching element breaks the match (as in next()). If not applied a relaxed contiguity (as in followedBy()) is used.
E.g. a pattern like:
`[code: Pattern.<Event>begin("start") ... (323 chars)]`
Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B
with consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
without consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}.

#### allowCombinations()
Works in conjunction with oneOrMore() and times() and imposes non-deterministic relaxed contiguity between the matching events (as in followedByAny()). If not applied a relaxed contiguity (as in followedBy()) is used.
E.g. a pattern like:
`[code: Pattern.<Event>begin("start") ... (329 chars)]`
Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B.
with combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A3 B}, {C A1 A4 B}, {C A1 A2 A3 B}, {C A1 A2 A4 B}, {C A1 A3 A4 B}, {C A1 A2 A3 A4 B}
without combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}.

### Groups of patterns

It's also possible to define a pattern sequence as the condition for begin, followedBy, followedByAny and next. The pattern sequence will be considered as the matching condition logically and a GroupPattern will be returned and it is possible to apply oneOrMore(), times(#ofTimes), times(#fromTimes, #toTimes), optional(), consecutive(), allowCombinations() to the GroupPattern.

`[code:  ... (683 chars)]`

#### begin(#name)
Defines a starting pattern.
`[code: Pattern<Event, ?> start = Pattern.<Event>begin("start"); ... (56 chars)]`

#### begin(#pattern_sequence)
Defines a starting pattern
`[code: Pattern<Event, ?> start = Pattern.<Event>begin( ... (127 chars)]`

#### next(#name)
Appends a new pattern. A matching event has to directly succeed the previous matching event (strict contiguity).
`[code: Pattern<Event, ?> next = start.next("middle"); ... (46 chars)]`

#### next(#pattern_sequence)
Appends a new pattern. A sequence of matching events have to directly succeed the previous matching event (strict contiguity).
`[code: Pattern<Event, ?> next = start.next( ... (116 chars)]`

#### followedBy(#name)
Appends a new pattern. Other events can occur between a matching event and the previous matching event (relaxed contiguity).
`[code: Pattern<Event, ?> followedBy = start.followedBy("middle"); ... (58 chars)]`

#### followedBy(#pattern_sequence)
Appends a new pattern. Other events can occur between a matching event and the previous matching event (relaxed contiguity).
`[code: Pattern<Event, ?> followedBy = start.followedBy( ... (128 chars)]`

#### followedByAny(#name)
Appends a new pattern. Other events can occur between a matching event and the previous matching event, and alternative matches will be presented for every alternative matching event (non-deterministic relaxed contiguity).
`[code: Pattern<Event, ?> followedByAny = start.followedByAny("middle"); ... (64 chars)]`

#### followedByAny(#pattern_sequence)
Appends a new pattern. Other events can occur between a matching event and the previous matching event, and alternative matches will be presented for every alternative matching event (non-deterministic relaxed contiguity).
`[code: Pattern<Event, ?> next = start.next( ... (116 chars)]`

#### notNext()
Appends a new negative pattern. A matching (negative) event has to directly succeed the previous matching event (strict contiguity) for the partial match to be discarded.
`[code: Pattern<Event, ?> notNext = start.notNext("not"); ... (49 chars)]`

#### notFollowedBy()
Appends a new negative pattern. A partial matching event sequence will be discarded even if other events occur between the matching (negative) event and the previous matching event (relaxed contiguity).
`[code: Pattern<Event, ?> notFollowedBy = start.notFollowedBy("not"); ... (61 chars)]`

#### within(time)
Defines the maximum time interval for an event sequence to match the pattern. If a non-completed event sequence exceeds this time, it is discarded.
`[code: pattern.within(Duration.ofSeconds(10)); ... (39 chars)]`

### After Match Skip Strategy

For a given pattern, the same event may be assigned to multiple successful matches. To control to how many matches an event will be assigned, you need to specify the skip strategy called AfterMatchSkipStrategy. There are five types of skip strategies, listed as follows:
- NO_SKIP: Every possible match will be emitted.
- SKIP_TO_NEXT: Discards every partial match that started with the same event, emitted match was started.
- SKIP_PAST_LAST_EVENT: Discards every partial match that started after the match started but before it ended.
- SKIP_TO_FIRST: Discards every partial match that started after the match started but before the first event of PatternName occurred.
- SKIP_TO_LAST: Discards every partial match that started after the match started but before the last event of PatternName occurred.

Notice that when using SKIP_TO_FIRST and SKIP_TO_LAST skip strategy, a valid PatternName should also be specified.

For example, for a given pattern b+ c and a data stream b1 b2 b3 c, the differences between these four skip strategies are as follows:
[table: 6 rows]

Have a look also at another example to better see the difference between NO_SKIP and SKIP_TO_FIRST:
Pattern: (a | b | c) (b | c) c+.greedy d and sequence: a b c1 c2 c3 d Then the results will be:
[table: 3 rows]

To better understand the difference between NO_SKIP and SKIP_TO_NEXT take a look at following example:
Pattern: a b+ and sequence: a b1 b2 b3 Then the results will be:
[table: 3 rows]

To specify which skip strategy to use, just create an AfterMatchSkipStrategy by calling:
[table: 6 rows]

Then apply the skip strategy to a pattern by calling:
`[code: AfterMatchSkipStrategy skipStrategy = ...; ... (86 chars)]`

  For SKIP_TO_FIRST/LAST there are two options how to handle cases when there are no events mapped to the PatternName. By default a NO_SKIP strategy will be used in this case. The other option is to throw exception in such situation. One can enable this option by:
`[code: AfterMatchSkipStrategy.skipToFirst(patternName).throwExceptionOnMiss() ... (71 chars)]`

## Detecting Patterns

After specifying the pattern sequence you are looking for, it is time to apply it to your input stream to detect potential matches. To run a stream of events against your pattern sequence, you have to create a PatternStream. Given an input stream input, a pattern pattern and an optional comparator comparator used to sort events with the same timestamp in case of EventTime or that arrived at the same moment, you create the PatternStream by calling:
`[code: DataStream<Event> input = ...; ... (195 chars)]`

The input stream can be keyed or non-keyed depending on your use-case.
  Applying your pattern on a non-keyed stream will result in a job with parallelism equal to 1.

### Selecting from Patterns

Once you have obtained a PatternStream you can apply transformation to detected event sequences. The suggested way of doing that is by PatternProcessFunction.

A PatternProcessFunction has a processMatch method which is called for each matching event sequence. It receives a match in the form of Map<String, List<IN>> where the key is the name of each pattern in your pattern sequence and the value is a list of all accepted events for that pattern (IN is the type of your input elements). The events for a given pattern are ordered by timestamp. The reason for returning a list of accepted events for each pattern is that when using looping patterns (e.g. oneToMany() and times()), more than one event may be accepted for a given pattern.

`[code: class MyPatternProcessFunction<IN, OUT> extends PatternProcessFunction ... (358 chars)]`

The PatternProcessFunction gives access to a Context object. Thanks to it, one can access time related characteristics such as currentProcessingTime or timestamp of current match (which is the timestamp of the last element assigned to the match). For more info see Time context. Through this context one can also emit results to a side-output.

#### Handling Timed Out Partial Patterns

Whenever a pattern has a window length attached via the within keyword, it is possible that partial event sequences are discarded because they exceed the window length. To act upon a timed out partial match one can use TimedOutPartialMatchHandler interface. The interface is supposed to be used in a mixin style. This mean you can additionally implement this interface with your PatternProcessFunction. The TimedOutPartialMatchHandler provides the additional processTimedOutMatch method which will be called for every timed out partial match.

`[code: class MyPatternProcessFunction<IN, OUT> extends PatternProcessFunction ... (482 chars)]`

Note The processTimedOutMatch does not give one access to the main output. You can still emit results through side-outputs though, through the Context object.

#### Convenience API

The aforementioned PatternProcessFunction was introduced in Flink 1.8 and since then it is the recommended way to interact with matches. One can still use the old style API like select/flatSelect, which internally will be translated into a PatternProcessFunction.

`[code: PatternStream<Event> patternStream = CEP.pattern(input, pattern); ... (843 chars)]`

## Time in CEP library

### Handling Lateness in Event Time

In CEP the order in which elements are processed matters. To guarantee that elements are processed in the correct order when working in event time, an incoming element is initially put in a buffer where elements are sorted in ascending order based on their timestamp, and when a watermark arrives, all the elements in this buffer with timestamps smaller than that of the watermark are processed. This implies that elements between watermarks are processed in event-time order.

  The library assumes correctness of the watermark when working in event time.

To guarantee that elements across watermarks are processed in event-time order, Flink's CEP library assumes correctness of the watermark, and considers as late elements whose timestamp is smaller than that of the last seen watermark. Late elements are not further processed. Also, you can specify a sideOutput tag to collect the late elements come after the last seen watermark, you can use it like this.

`[code: PatternStream<Event> patternStream = CEP.pattern(input, pattern); ... (405 chars)]`

### Time context

In PatternProcessFunction as well as in IterativeCondition user has access to a context that implements TimeContext as follows:
`[code: /** ... (627 chars)]`

This context gives user access to time characteristics of processed events (incoming records in case of IterativeCondition and matches in case of PatternProcessFunction). Call to TimeContext#currentProcessingTime always gives you the value of current processing time and this call should be preferred to e.g. calling System.currentTimeMillis().

In case of TimeContext#timestamp() the returned value is equal to assigned timestamp in case of EventTime. In ProcessingTime this will equal to the point of time when said event entered cep operator (or when the match was generated in case of PatternProcessFunction). This means that the value will be consistent across multiple calls to that method.

## Optional Configuration

Options to configure the cache capacity of Flink CEP SharedBuffer. It could accelerate the CEP operate process speed and limit the number of elements of cache in pure memory.

Note It's only effective to limit usage of memory when state.backend.type was set as rocksdb, which would transport the elements exceeded the number of the cache into the rocksdb state storage instead of memory state storage. The configuration items are helpful for memory limitation when the state.backend.type is set as rocksdb. By contrast, when the state.backend.type is set as not rocksdb, the cache would cause performance decreased. Compared with old cache implemented with Map, the state part will contain more elements swapped out from new guava-cache, which would make it heavier to copy on write for state.
[table: 4 rows]

## Examples

The following example detects the pattern start, middle(name = "error") -> end(name = "critical") on a keyed data stream of Events. The events are keyed by their ids and a valid pattern has to occur within 10 seconds. The whole processing is done with event time.
`[code: StreamExecutionEnvironment env = ...; ... (834 chars)]`

## Migrating from an older Flink version(pre 1.5)

### Migrating from Flink <= 1.5

In Flink 1.13 we dropped direct savepoint backward compatibility with Flink <= 1.5. If you want to restore from a savepoint taken from an older version, migrate it first to a newer version (1.6-1.12), take a savepoint and then use that savepoint to restore with Flink >= 1.13.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/state_processor_api/
# State Processor API

Apache Flink's State Processor API provides powerful functionality for reading, writing, and modifying savepoints and checkpoints using Flink's DataStream API and Table API under BATCH execution. Due to the interoperability of DataStream and Table API, you can even use relational Table API or SQL queries to analyze and process state data.

For example, you can take a savepoint of a running stream processing application and analyze it with a DataStream batch program to verify that the application behaves correctly. Or you can read a batch of data from any store, preprocess it, and write the result to a savepoint that you use to bootstrap the state of a streaming application. It is also possible to fix inconsistent state entries. Finally, the State Processor API opens up many ways to evolve a stateful application that was previously blocked by parameter and design choices that could not be changed without losing all the state of the application after it was started. For example, you can now arbitrarily modify the data types of states, adjust the maximum parallelism of operators, split or merge operator state, re-assign operator UIDs, and so on.

To get started with the state processor api, include the following library in your application.
`[code: <dependency> ... (150 chars)]`
  Copied to clipboard!

## Mapping Application State to DataSets

The State Processor API maps the state of a streaming application to one or more data sets that can be processed separately. In order to be able to use the API, you need to understand how this mapping works.

But let us first have a look at what a stateful Flink job looks like. A Flink job is composed of operators; typically one or more source operators, a few operators for the actual processing, and one or more sink operators. Each operator runs in parallel in one or more tasks and can work with different types of state. An operator can have zero, one, or more "operator states" which are organized as lists that are scoped to the operator's tasks. If the operator is applied on a keyed stream, it can also have zero, one, or more "keyed states" which are scoped to a key that is extracted from each processed record. You can think of keyed state as a distributed key-value map.

The following figure shows the application "MyApp" which consists of three operators called "Src", "Proc", and "Snk". Src has one operator state (os1), Proc has one operator state (os2) and two keyed states (ks1, ks2) and Snk is stateless.

A savepoint or checkpoint of MyApp consists of the data of all states, organized in a way that the states of each task can be restored. When processing the data of a savepoint (or checkpoint) with a batch job, we need a mental model that maps the data of the individual tasks' states into data sets or tables. In fact, we can think of a savepoint as a database. Every operator (identified by its UID) represents a namespace. Each operator state of an operator is mapped to a dedicated table in the namespace with a single column that holds the state's data of all tasks. All keyed states of an operator are mapped to a single table consisting of a column for the key, and one column for each keyed state. The following figure shows how a savepoint of MyApp is mapped to a database.

The figure shows how the values of Src's operator state are mapped to a table with one column and five rows, one row for each of the list entries across all parallel tasks of Src. Operator state os2 of the operator "Proc" is similarly mapped to an individual table. The keyed states ks1 and ks2 are combined to a single table with three columns, one for the key, one for ks1 and one for ks2. The keyed table holds one row for each distinct key of both keyed states. Since the operator "Snk" does not have any state, its namespace is empty.

## Identifying operators

The State Processor API allows you to identify operators using UIDs or UID hashes via OperatorIdentifier#forUid/forUidHash. Hashes should only be used when the use of UIDs is not possible, for example when the application that created the savepoint did not specify them or when the UID is unknown.

## DataStream API

### Reading State

Reading state begins by specifying the path to a valid savepoint or checkpoint along with the StateBackend that should be used to restore the data. The compatibility guarantees for restoring state are identical to those when restoring a DataStream application.

`[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (184 chars)]`

#### Operator State

Operator state is any non-keyed state in Flink. This includes, but is not limited to, any use of CheckpointedFunction or BroadcastState within an application. When reading operator state, users specify the operator uid, the state name, and the type information.

##### Operator List State
Operator state stored in a CheckpointedFunction using getListState can be read using SavepointReader#readListState. The state name and type information should match those used to define the ListStateDescriptor that declared this state in the DataStream application.
`[code: DataStream<Integer> listState  = savepoint.readListState<>( ... (134 chars)]`

##### Operator Union List State
Operator state stored in a CheckpointedFunction using getUnionListState can be read using SavepointReader#readUnionState. The state name and type information should match those used to define the ListStateDescriptor that declared this state in the DataStream application. The framework will return a single copy of the state, equivalent to restoring a DataStream with parallelism 1.
`[code: DataStream<Integer> listState  = savepoint.readUnionState<>( ... (136 chars)]`

##### Broadcast State
BroadcastState can be read using SavepointReader#readBroadcastState. The state name and type information should match those used to define the MapStateDescriptor that declared this state in the DataStream application. The framework will return a single copy of the state, equivalent to restoring a DataStream with parallelism 1.
`[code: DataStream<Tuple2<Integer, Integer>> broadcastState = savepoint.readBr ... (180 chars)]`

##### Using Custom Serializers
Each of the operator state readers support using custom TypeSerializers if one was used to define the StateDescriptor that wrote out the state.
`[code: DataStream<Integer> listState = savepoint.readListState<>( ... (164 chars)]`

#### Keyed State

Keyed state, also known as partitioned state, is any state that is partitioned relative to a key. When reading a keyed state, users specify the operator id and a KeyedStateReaderFunction<KeyType, OutputType>.

The KeyedStateReaderFunction allows users to read arbitrary columns and complex state types such as ListState, MapState, and AggregatingState. This means if an operator contains a stateful process function such as:
`[code: public class StatefulFunctionWithTime extends KeyedProcessFunction<Int ... (761 chars)]`

Then it can read by defining an output type and corresponding KeyedStateReaderFunction.
`[code: DataStream<KeyedState> keyedState = savepoint.readKeyedState(OperatorI ... (1134 chars)]`

Along with reading registered state values, each key has access to a Context with metadata such as registered event time and processing time timers.

Note: When using a KeyedStateReaderFunction, all state descriptors must be registered eagerly inside of open. Any attempt to call a RuntimeContext#get*State will result in a RuntimeException.

#### Window State

The state processor api supports reading state from a window operator. When reading a window state, users specify the operator id, window assigner, and aggregation type. Additionally, a WindowReaderFunction can be specified to enrich each read with additional information similar to a WindowFunction or ProcessWindowFunction.

Suppose a DataStream application that counts the number of clicks per user per minute.
`[code: class Click { ... (705 chars)]`

This state can be read using the code below.
`[code:  ... (1039 chars)]`

Additionally, trigger state - from CountTriggers or custom triggers - can be read using the method Context#triggerState inside the WindowReaderFunction.

### Writing New Savepoints

Savepoint's may also be written, which allows such use cases as bootstrapping state based on historical data. Each savepoint is made up of one or more StateBootstrapTransformation's (explained below), each of which defines the state for an individual operator.

  When using the SavepointWriter, your application must be executed under BATCH execution.

`[code: int maxParallelism = 128; ... (275 chars)]`

The UIDs associated with each operator must match one to one with the UIDs assigned to the operators in your DataStream application; these are how Flink knows what state maps to which operator.

#### Operator State
Simple operator state, using CheckpointedFunction, can be created using the StateBootstrapFunction.
`[code: public class SimpleBootstrapFunction extends StateBootstrapFunction<In ... (847 chars)]`

#### Broadcast State
BroadcastState can be written using a BroadcastStateBootstrapFunction. Similar to broadcast state in the DataStream API, the full state must fit in memory.
`[code: public class CurrencyRate { ... (820 chars)]`

#### Keyed State
Keyed state for ProcessFunction's and other RichFunction types can be written using a KeyedStateBootstrapFunction.
`[code: public class Account { ... (924 chars)]`

The KeyedStateBootstrapFunction supports setting event time and processing time timers. The timers will not fire inside the bootstrap function and only become active once restored within a DataStream application. If a processing time timer is set but the state is not restored until after that time has passed, the timer will fire immediately upon start.

Attention If your bootstrap function creates timers, the state can only be restored using one of the process type functions.

#### Window State
The state processor api supports writing state for the window operator. When writing window state, users specify the operator id, window assigner, evictor, optional trigger, and aggregation type. It is important the configurations on the bootstrap transformation match the configurations on the DataStream window.
`[code: public class Account { ... (503 chars)]`

### Modifying Savepoints

Besides creating a savepoint from scratch, you can base one off an existing savepoint such as when bootstrapping a single new operator for an existing job.
`[code: SavepointWriter ... (172 chars)]`

#### Changing UID (hashes)
SavepointWriter#changeOperatorIdenfifier can be used to modify the UIDs or UID hash of an operator.

If a UID was not explicitly set (and was thus auto-generated and is effectively unknown), you can assign a UID provided that you know the UID hash (e.g., by parsing the logs):
`[code: savepointWriter ... (175 chars)]`

You can also replace one UID with another:
`[code: savepointWriter ... (146 chars)]`

## Table API

### Getting started

Before you interrogate state using the table API, make sure to review our Flink SQL guidelines.

IMPORTANT NOTE: State Table API only supports keyed state.

### Metadata

The following SQL table function allows users to read the metadata of savepoints and checkpoints in the following way:
`[code: LOAD MODULE state; ... (90 chars)]`

The new table function creates a table with the following fixed schema:
[table: 10 rows]

### Keyed State

Keyed state, also known as partitioned state, is any state that is partitioned relative to a key.

The SQL connector allows users to read arbitrary columns as ValueState and complex state types such as ListState, MapState. This means if an operator contains a stateful process function such as:
`[code: eventStream ... (1431 chars)]`

Then it can read by querying a table created using the following SQL statement:
`[code: CREATE TABLE state_table ( ... (410 chars)]`

### Connector options

#### General options
[table: 6 rows]

#### Connector options for column '#'
[table: 7 rows]

### Default Data Type Mapping

The state SQL connector infers the data type for primitive types when fields.#.value-class and fields.#.key-class are not defined. The following table shows the Flink SQL type -> Java type default mapping. If the mapping is not calculated properly then it can be overridden with the two mentioned config parameters on a per-column basis.
[table: 17 rows]

SHORTCUT: When a complex java class is defined in a column with STRING SQL type then the class instance toString method result will be the column value. This can be useful when a quick explanatory query is required.
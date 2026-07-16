---
title: 'Migrating a DB-backed microservice to event-driven Flink — constraints, tradeoffs & decision guide'
tags: [org, flink, migration, event-driven, cdc, outbox, cqrs, statefun, kafka-streams, architecture, microservices, decision-guide]
author: adrianb
sessions: []
created: '2026-07-08T16:27:51.530Z'
updated: '2026-07-08T16:27:51.530Z'
importanceScore: 0.8
---

## Executive Summary

**WHY this matters:** "Migrate a DB-backed microservice to event-driven Flink" is a recurring architecture ask that is easy to get wrong by treating it as one migration. This is a reusable decision framework — grounded in the workspace's own POCs (`poc-for-presentation-on-flink-variants-for-cdc-and-outbox` for CDC/outbox, `presentation-knative-camel-k` for the scale-to-zero contrast, the StateFun variant + `poc-flink-statefun-market-and-trading-demo` for the actor model) — for deciding **whether** to migrate, **how far**, and **which abstraction**. The headline: it's three separable problems, the risk lives in ingestion + serving reads, and the usual right answer is a hybrid/strangler migration, not wholesale.

## The reframing: three separable boundaries

1. **Ingestion boundary** — how data leaves the DB and enters the stream (CDC vs outbox).
2. **State boundary** — does the DB stay system-of-record, or does Flink managed state become it? (Promoting Flink state to source-of-truth = event sourcing = a deliberate, much bigger, separate decision.)
3. **Serving boundary** — how synchronous reads/queries are answered once the write path is a stream. **The one people forget, and usually the killer.**

**Single biggest decision:** is the service fundamentally request/response CRUD, or fundamentally processing? Flink is a stream processor, not a database. `GET /order/{id}` + transactional invariants → wholesale Flink is usually wrong. "consume events, enrich/aggregate/join/react, emit" → natural fit.

## Gate question: do you even need Flink? (the ladder)

For ONE microservice Flink is often too heavy. Climb the ladder, don't skip to Flink because it's interesting:
- **Outbox + plain consumer/worker** — event-driven decoupling with no stream processor at all.
- **Kafka Streams** — stateful stream processing as a *library in your service* (no cluster, scales like a normal deployment, no savepoint dance). Often the right first step.
- **Flink** — only when you outgrow the above: cross-key/cross-stream state, exactly-once across complex topologies, large keyed state, operational isolation from the app. Its tax (savepoints, rescaling, state backends, CDC server-IDs, always-on cost) only pays off past a real threshold.

## Constraints / what to consider

**Ingestion — dual-write problem.** Cannot atomically write DB + publish an event without CDC or transactional outbox.
- CDC (binlog): non-invasive, no app changes, but couples to DB internals (binlog format, Debezium quirks, server-ID allocation — every parallel reader + restart needs a non-overlapping ID because the binlog lease hasn't expired). DB schema changes ripple into the stream.
- Outbox: transactionally consistent with writes (same tx), explicit contract, but requires app changes + a relay.

**Backfill / bootstrapping.** Not just "stream new changes" — must snapshot existing rows then cut over with no gaps/dups. CDC incremental-snapshot handles the read side; if Flink state becomes source-of-truth, also need State Processor API / StateFun State Bootstrapping.

**Serving reads (the CQRS tax).** Flink is bad at point lookups + ad-hoc queries. You get CQRS whether you wanted it or not: stream = write/processing path, sink to a materialized view (KV / OLAP / compacted topic) for reads. Flink Queryable State is limited/semi-abandoned — don't build on it.

**Consistency model shift.** ACID multi-row tx → exactly-once is per-key/per-record, NOT multi-entity transactional; cross-aggregate invariants become sagas/orchestration. Per-key ordering preserved (keyed streams; StateFun single-writer-per-key), global ordering not. Read-your-writes gone by default (eventual consistency).

**Exactly-once is end-to-end or nothing.** Needs replayable source (Kafka/binlog) + transactional sink; the sink adds latency (commits on checkpoint). (Sharp edge seen: Kafka `InitProducerId` restart-loop when transaction-state config is wrong.)

**Operational model inversion (deepest cultural change).** Restart = replay from checkpoint (not a fresh pod). Deploy = savepoint → cancel → upgrade → restore. Rescale = restore from savepoint with new parallelism (keyed state repartitions), not CPU autoscale. Always-on — no scale-to-zero (the exact Knative-vs-Flink tradeoff). State lives in RocksDB + checkpoints — you now own size, TTL, backend tuning.

## Strong points

- Exactly-once stateful processing with no database (state + messages roll back together).
- High throughput, bounded latency, backpressure.
- First-class time & state (windows, timers, CEP, multi-stream joins) — things a CRUD service fakes with cron + polling tables.
- Decoupling; new consumers replay history.
- **StateFun** gives the actor/entity model — closest streaming analog to "a microservice per aggregate" (location transparency, per-key consistency). Natural target when the thing migrated is an entity with behavior.

## Weak points

- Not a serving DB (forces CQRS + read store).
- Loss of multi-entity ACID (invariants → sagas; harder to reason about/test).
- Eventual consistency (no read-your-writes; lag is permanent).
- Operational complexity (savepoints, backends, rescaling, exactly-once sink wiring, CDC server-IDs); harder local dev/testing.
- Always-on cost.
- **Ecosystem/version fragility:** StateFun 3.2.0 is dormant and hard-locked to Flink 1.14; Flink 1.14↔2.x broke `YieldingOperatorFactory`, Sink v2, the CDC package namespace (`org.apache.flink.cdc.*` needs 1.18+ vs `com.ververica.cdc.*` for ≤1.14), even a `setDeliverGuarantee` typo. A StateFun-based entity model pins you to a 2022-era unmaintained runtime — weigh vs Kafka Streams or plain keyed DataStream.
- Skill curve (state-as-stream, checkpoint semantics).

## Decision heuristic

- **Good fit:** high-volume event/stream processing, enrichment, aggregation, derived/materialized views, async multi-step workflows, CDC fan-out, analytics/CEP, entities that mostly "react to events and update state."
- **Bad fit (keep the microservice):** synchronous CRUD APIs, strong cross-entity transactional invariants, low-volume request/response, heavy ad-hoc query needs.

## Reference target topology (hybrid, not wholesale)

```
[service + DB]  --CDC or outbox-->  [Kafka]  -->  [Flink job]  --+--> [materialized view store] --> read API
 (system of record, STAYS)                        (processing,   |     (KV / OLAP / compacted topic)
                                                   exactly-once)  +--> [downstream event topics]
```

Load-bearing insight: the DB does NOT disappear day one. It stays system-of-record; Flink is a derived layer. Two proven shapes: (1) DB stays SoR, Flink is a derived streaming layer via CDC/outbox; (2) CQRS — Flink owns write-processing + emits materialized views, thin read API serves them.

## Phased strangler path (never big-bang)

1. **Tap, don't touch** — CDC/outbox → Kafka; service unchanged.
2. **Shadow** — run Flink job on that stream, produce outputs, diff against live service; no traffic moved.
3. **Serve reads from the view** — sink to read store, move read traffic (CQRS split); writes still to service.
4. **Move a write path** — migrate one command/workflow into the stream, keep the rest.
5. **Repeat** per capability until DB is (maybe) just backfill/audit.

Each step independently reversible.

## Three gotchas that actually sink these migrations

1. **Reads** — people plan the write path and forget `GET /thing/{id}`. Decide the read store FIRST; it dictates half the design.
2. **Cross-entity transactions** — every DB tx spanning two aggregates becomes a saga. Count them before committing; many → streaming will hurt.
3. **Backfill + cutover** — "stream new changes" is easy; snapshotting existing rows + gap-free/dup-free cutover is the real project. Budget for it.

## Pick the right Flink abstraction

Maps to the 5+1 POC variants: SQL/Table API for declarative transforms; DataStream for full control; **StateFun for stateful entities (the microservice-per-aggregate case)** — with the dormant-runtime caveat firmly in mind.

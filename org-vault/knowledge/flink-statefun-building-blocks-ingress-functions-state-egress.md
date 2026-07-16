---
title: 'Flink StateFun building blocks — ingress, functions, state, egress'
tags: [org, flink, flink-statefun, stateful-functions, application-model, exactly-once]
author: adrianb
sessions: []
created: '2026-07-08T09:10:49.718Z'
updated: '2026-07-08T09:19:19.385Z'
importanceScore: 0.7
---

## Executive Summary

## Executive summary

A StateFun application is an event-driven loop: **ingress** → **stateful functions** (with **persisted state**, messaging each other arbitrarily, not a static DAG) → **egress**. Exactly-once holds for *both* state and messages: on failure the whole world (state + in-flight messages) rolls back to simulate failure-free execution, via Flink's snapshotting — **no database required**.

**Why it matters:** this is the application model — functions are actor-like (dynamic, cyclic messaging) but with persisted state and exactly-once, which plain actors don't give you.

![Stateful functions message each other arbitrarily](/home/adrianb/.total-recall/downloads/flink-statefun-docs-stable/images/flink/flink-statefun-docs-release-3.2/fig/concepts/statefun-app-functions.svg)

## Event ingress

Component that ingests records to trigger the first functions — Kafka topic, message queue, HTTP request, anything that gets data in. Diagram: `fig/concepts/statefun-app-ingress.svg`.

## Stateful functions

- Building blocks of the service; **message each other arbitrarily** — potentially cyclic, round-trip. This breaks from traditional stream processing's static dataflow DAG.
- Actor-like in dynamic messaging, but with key differences (below).

### Persisted states

Every function has **locally embedded state** (persisted states). Inside a function you always work with state in local variables. Flink provides fault-tolerant local state. Diagram: `fig/concepts/statefun-app-state.svg`.

### Fault tolerance

Exactly-once for state **and** messaging. On failure, persisted states + messages roll back to simulate failure-free execution. No DB — leverages Flink's snapshotting. Diagram: `fig/concepts/statefun-app-fault-tolerance.svg`.

## Event egress

Output to external systems via event egresses — pre-built integrations on top of the Flink connector ecosystem (functions may also do arbitrary RPC side-effects). Diagram: `fig/concepts/statefun-app-egress.svg`.

See [[flink-statefun-architecture-logical-co-location-physical-separation]], [[flink-statefun-logical-functions-addressing-lifecycle]], [[flink-statefun-modules-module-yaml-format-component-kinds]]. Full text in mirror — [[flink-statefun-docs-local-mirror-v3-2-0]].
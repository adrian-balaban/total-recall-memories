---
title: Flink StateFun logical functions — addressing & lifecycle
tags: [org, flink, flink-statefun, stateful-functions, addressing, virtual-instances]
author: adrianb
sessions: []
created: '2026-07-08T09:10:59.889Z'
updated: '2026-07-08T09:19:26.910Z'
importanceScore: 0.7
---

## Executive Summary

## Executive summary

StateFun functions are **logical/virtual**: unbounded instances on finite resources. An instance uses no CPU/threads/memory when not invoked and has no state. Model applications as granularly as makes sense — don't design around resource limits. An **Address = FunctionType + ID** (type ≈ class, ID ≈ primary key); all reads/writes during an invocation are scoped to that address. **Single writer per key** at a time. Instances are never created/destroyed — they always exist; clearing all persisted state of a type == destroying it.

**Why it matters:** this is the addressing/scale model — it explains why you can have millions of per-entity functions (one per SKU, per user, per order) without paying for idle ones.

![Address = FunctionType + ID](/home/adrianb/.total-recall/downloads/flink-statefun-docs-stable/images/flink/flink-statefun-docs-release-3.2/fig/concepts/address.svg)

## Function address

- Function instances are virtual; runtime location is not exposed.
- `Address` = `FunctionType` (what kind — like a class) + `ID` (primary key — which instance).
- On invocation, all actions (incl. persisted-state reads/writes) are scoped to the current address.
- Example: an `Inventory` function type tracking stock per SKU; "shirt" and "pant" are two IDs → two logical instances. As many instances as item types.

## Function lifecycle

- Logical functions are **neither created nor destroyed** — always exist for the app's lifetime.
- At startup, each parallel worker creates **one physical object per function type**, used to run all logical instances of that type handled by that worker.
- First message to an address behaves as if the instance always existed with empty state.
- **Clearing all persisted states of a type == destroying it.** No state + not running ⇒ no CPU/threads/memory.
- An instance with data occupies only the storage for that data; state stored by Flink runtime in the configured state backend.

See [[flink-statefun-architecture-logical-co-location-physical-separation]], [[flink-statefun-building-blocks-ingress-functions-state-egress]]. Full text in mirror — [[flink-statefun-docs-local-mirror-v3-2-0]].
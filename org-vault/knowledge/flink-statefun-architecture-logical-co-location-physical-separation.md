---
title: 'Flink StateFun architecture — logical co-location, physical separation'
tags: [org, flink, flink-statefun, stateful-functions, architecture]
author: adrianb
sessions: []
created: '2026-07-08T09:10:04.627Z'
updated: '2026-07-08T09:19:08.717Z'
importanceScore: 0.75
---

## Executive Summary

## Executive summary

Apache Flink Stateful Functions (StateFun) v3.2.0 rethinks stateful stream processing: it **logically co-locates** state and compute (so it keeps Flink's exactly-once consistency) but lets you **physically separate** them (so functions scale/deploy independently like stateless services). The Flink runtime owns state + messaging + invocation routing; functions are just invoked with message+state bundled in the request.

**Why it matters:** this is the core mental model everything else hangs on — it's why StateFun can run functions on AWS Lambda / Knative while still giving exactly-once stateful semantics with no database.

![High-level architecture overview](/home/adrianb/.total-recall/downloads/flink-statefun-docs-stable/images/flink/flink-statefun-docs-release-3.2/fig/concepts/arch_overview.svg)

![Architecture components — Flink master/workers + remote functions](/home/adrianb/.total-recall/downloads/flink-statefun-docs-stable/images/flink/flink-statefun-docs-release-3.2/fig/concepts/arch_components.svg)

## Components

- A deployment = a set of **Apache Flink processes** (1 master/JobManager + N TaskManager workers), plus optionally remote function deployments.
- TaskManagers ingest events (Kafka, Kinesis, …), route them to target functions by key, invoke functions, route resulting messages to next functions, and write egress.
- Needs **ZooKeeper or Kubernetes** (master failover) + **bulk storage** (S3/HDFS/NAS/GCS/Azure Blob) for checkpoints. **No database; no persistent volumes** on Flink processes.

## The core principle: logical co-location + physical separation

- *Logical co-location:* messaging, state read/write, and function invocation are managed together (like Flink's DataStream API). State is sharded by key; messages routed to state by key; **single writer per key at a time**.
- *Physical separation:* functions can run **remotely** — invocation request carries message + state + timer access — so functions are managed like stateless apps.

## Three function deployment styles (per-module; can mix)

1. **Remote functions** — HTTP/gRPC invocation through a routing service (K8s Service, AWS Lambda gateway). Max independence (state & compute scale separately), some network overhead. Diagram: `fig/concepts/arch_funs_remote.svg` (in mirror).
2. **Co-located functions** — one function sidecar process per TaskManager pod, talking over pod-local network. Multi-language, no LB hop, but can't scale state/compute independently. Like Flink Table API / Beam portability. Diagram: `fig/concepts/arch_funs_colocated.svg`.
3. **Embedded functions** — run inside the Flink JVM, invoked directly. Most performant; JVM languages only; updating functions means updating the cluster. ("Stored procedures, but principled.") Diagram: `fig/concepts/arch_funs_embedded.svg`.

See [[flink-statefun-building-blocks-ingress-functions-state-egress]], [[flink-statefun-logical-functions-addressing-lifecycle]], [[flink-statefun-deployment-k8s-docker-image-configmaps]]. Full detail/code in the local mirror — see [[flink-statefun-docs-local-mirror-v3-2-0]].
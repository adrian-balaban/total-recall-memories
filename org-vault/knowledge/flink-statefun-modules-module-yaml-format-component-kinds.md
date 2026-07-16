---
title: Flink StateFun modules — module.yaml format & component kinds
tags: [org, flink, flink-statefun, stateful-functions, module-yaml, kafka, kinesis]
author: adrianb
sessions: []
created: '2026-07-08T09:11:12.405Z'
updated: '2026-07-08T09:19:35.351Z'
importanceScore: 0.7
---

## Executive Summary

## Executive summary

A StateFun **application module** is a YAML file describing the components of an app: function endpoints, ingress, and egress definitions. The file is **multi-document YAML** (`---`-separated); each document is one component = a `kind` typename + a `spec` object. Modules are attached to the runtime container (mounted as a ConfigMap in k8s).

**Why it matters:** this is the wiring format — the one thing you must edit to define an app (endpoints + IO). Recognizing the `kind` strings tells you immediately what a module does.

## Module.yaml example (HTTP endpoint + Kafka ingress + Kafka egress)

```yaml
kind: io.statefun.endpoints.v2/http
spec:
  functions: com.example/*
  urlPathTemplate: https://bar.foo.com/{function.name}
---
kind: io.statefun.kafka.v1/ingress
spec:
  id: com.example/my-ingress
  address: kafka-broker:9092
  consumerGroupId: my-consumer-group
  topics:
    - topic: message-topic
      valueType: io.statefun.types/string
      targets:
        - com.example/greeter
---
kind: io.statefun.kafka.v1/egress
spec:
  id: com.example/my-egress
  address: kafka-broker:9092
  deliverySemantic:
    type: exactly-once
    transactionTimeout: 15min
```

- Each component: `kind: <typename>` + `spec: { ... }`. Multiple docs separated by `---`.
- Endpoint `functions: com.example/*` binds a glob of function types to a URL template.

## Kind / component landscape

- **Endpoints**: `io.statefun.endpoints.v2/http` (remote HTTP functions; also gRPC). HTTP function endpoint module = where the runtime reaches remote functions.
- **Ingress / egress**: Kafka (`io.statefun.kafka.v1/ingress|egress`), AWS Kinesis, Flink connectors (reuse the Flink connector ecosystem).
- **Embedded modules**: functions bundled into the runtime image (JVM).
- **Function type/value types**: e.g. `io.statefun.types/string`.

## SDKs (how you write the function bodies)

JavaScript, Python, Java, Golang, Flink DataStream (JVM), plus an SDK appendix. Remote SDKs (JS/Python/Java/Golang) receive invocation requests (message + state + timers) over HTTP/gRPC and return side-effects; embedded runs in-JVM.

See [[flink-statefun-deployment-k8s-docker-image-configmaps]] (module.yaml is mounted as a ConfigMap) and [[flink-statefun-architecture-logical-co-location-physical-separation]]. Full per-IO docs (Kafka/Kinesis/flink-connectors options) in mirror — [[flink-statefun-docs-local-mirror-v3-2-0]].
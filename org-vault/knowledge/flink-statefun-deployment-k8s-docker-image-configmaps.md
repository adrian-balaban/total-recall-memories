---
title: 'Flink StateFun deployment — k8s, docker image, ConfigMaps'
tags: [org, flink, flink-statefun, stateful-functions, deployment, kubernetes, docker, rocksdb]
author: adrianb
sessions: []
created: '2026-07-08T09:11:28.256Z'
updated: '2026-07-08T09:19:45.705Z'
importanceScore: 0.72
---

## Executive Summary

## Executive summary

StateFun runtime = Apache Flink, so it inherits Flink's deploy/ops model. Recommended deploy: the official Docker image **`apache/flink-statefun:3.2.0`** (user code packages no Flink components) running as a **standalone Flink cluster on Kubernetes**. You attach app config via two ConfigMaps mounted into JM/TM: **`application-module`** (the `module.yaml`) and **`flink-config`** (the `flink-conf.yaml` + log4j props). State backend **RocksDB**, checkpoints to bulk storage (e.g. `s3://...`), **exactly-once**, fixed-delay restart.

**Why it matters:** this is the concrete ops recipe — image + the ConfigMaps + JM/TM deployments + kubectl sequence to stand up or tear down a StateFun cluster.

## Key flink-conf.yaml essentials

```yaml
jobmanager.rpc.address: statefun-master
taskmanager.numberOfTaskSlots: 1
classloader.parent-first-patterns.additional: org.apache.flink.statefun;org.apache.kafka;com.google.protobuf
state.checkpoints.dir: s3://my-checkpoint-bucket
state.backend: rocksdb
state.backend.rocksdb.timer-service.factory: ROCKSDB
state.backend.incremental: true
execution.checkpointing.interval: 10sec
execution.checkpointing.mode: EXACTLY_ONCE
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
restart-strategy.fixed-delay.delay: 1sec
jobmanager.memory.process.size: 1g
taskmanager.memory.process.size: 1g
parallelism.default: 3
```

## Kubernetes cluster components

- **`application-module.yaml`** (ConfigMap) — `module.yaml` with function + io module config. Mounted at `/opt/statefun/modules/application-module`.
- **`flink-config.yaml`** (ConfigMap) — `flink-conf.yaml` + `log4j-console.properties`. Mounted at `/opt/flink/conf`.
- **`jobmanager-service.yaml`** (Service, ClusterIP) — ports rpc 6123, blob 6124, webui 8081. Optional `jobmanager-rest-service.yaml` (NodePort 30081) for public REST.
- **`jobmanager-application.yaml`** (Deployment, replicas 1) — image `apache/flink-statefun:3.2.0`, env `ROLE=master`, `MASTER_HOST=statefun-jobmanager`, liveness tcpSocket 6123, mounts both ConfigMaps.
- **`taskmanager-job-deployment.yaml`** (Deployment, replicas 3) — env `ROLE=worker`, liveness tcpSocket 6122, mounts flink-config + application-module.

## kubectl create / delete sequence

```sh
kubectl create -f application-module.yaml
kubectl create -f flink-config.yaml
kubectl create -f jobmanager-service.yaml
kubectl create -f jobmanager-job.yaml        # runtime JM
kubectl create -f taskmanager-job-deployment.yaml   # runtime TM
# teardown: delete in reverse (TM, JM, service, config, module)
```

See [[flink-statefun-modules-module-yaml-format-component-kinds]] (the module.yaml mounted as application-module) and [[flink-statefun-architecture-logical-co-location-physical-separation]]. Full ConfigMap YAML + metrics/state-bootstrap in mirror — [[flink-statefun-docs-local-mirror-v3-2-0]].
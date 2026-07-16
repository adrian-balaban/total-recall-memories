---
title: Apache Flink 2.3 docs — Deployment (native kubernetes/yarn/config/memory setup)
tags: [org, flink, flink-2.3, docs, deployment, native-kubernetes, yarn, config, memory, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:53:42.257Z'
updated: '2026-07-08T04:53:42.257Z'
importanceScore: 1
---

## Executive Summary

Executive summary: Apache Flink 2.3 deployment reference covering native Kubernetes integration, YARN deployment, the config.yaml configuration system, and process memory setup. **WHY this matters:** these are the operational deployment backbones for running Flink 2.3 in production — native K8s and YARN are the two resource orchestrators Flink actively integrates with (vs. passive standalone-on-K8s), config.yaml is the YAML-1.2 replacement for flink-conf.yaml (mandatory since Flink 2.0), and memory setup is the most-misconfigured area causing startup failures. Captured near-verbatim from nightlies.apache.org/flink/flink-docs-release-2.3/docs (English only) as an org reference; code blocks condensed to `[code: <first-line> ... (N chars)]`, tables to `[table: N rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/native_kubernetes/

# Native Kubernetes

Native Kubernetes integration lets Flink deploy directly on a running K8s cluster and dynamically allocate/de-allocate TaskManagers (it talks to K8s directly). Apache Flink also provides a **Flink Kubernetes Operator** supporting both standalone and native deployment mode, simplifying deployment/config/lifecycle.

## Getting Started
Requirements: Kubernetes >= 1.9; KubeConfig with list/create/delete pods+services (~/.kube/config; verify via `kubectl auth can-i <list|create|edit|delete> pods`); enabled Kubernetes DNS; default service account with RBAC to create/delete pods.

### Starting a Flink Session on Kubernetes
[code: # (1) Start Kubernetes session ... (400 chars)] — launches Flink cluster in Session Mode. Web UI/REST exposed as ClusterIP by default.

## Deployment Modes
Production recommendation: Application Mode for better isolation.

### Application Mode
User code must be bundled with the Flink image (runs main() on cluster). Bundling via (a) modify base Docker image, or (b) User Artifact Management (upload/download artifacts not available locally).
- Modify Docker image: `[code: FROM flink ... (109 chars)]`, then `[code: $ ./bin/flink run \ ... (220 chars)]`.
- User Artifact Management: `kubernetes.artifacts.local-upload-enabled` + `kubernetes.artifacts.local-upload-target` (valid remote target, permissions configured). Additional artifacts via `user.artifacts.artifact-list` (mix local+remote). Already-remote artifacts (DFS/HTTP(S)) just fetched on JM pod. Existing artifacts NOT overwritten during local upload. JAR fetching supports filesystems or HTTP(S) in Application Mode. Downloaded to `user.artifacts.base-dir/kubernetes.namespace/kubernetes.cluster-id`. `kubernetes.cluster-id` = cluster name (unique; auto-generated random if unset). `kubernetes.container.image.ref` = image. Interact after deploy: `[code: # List running job on the cluster ... (272 chars)]`. Override config via `-Dkey=value` to bin/flink.

### Session Mode
Two modes: detached (default, kubernetes-session.sh deploys then terminates); attached (`-Dexecution.attached=true`, stays alive, `stop`/`help` commands). Re-attach: `[code: $ ./bin/kubernetes-session.sh \ ... (114 chars)]`. Stop: `[code: $ echo 'stop' | ./bin/kubernetes-session.sh \ ... (128 chars)]`.

## Flink on Kubernetes Reference
### Configuring Flink on Kubernetes
Flink uses **Fabric8 Kubernetes client** to talk to APIServer (create/delete Deployment/Pod/ConfigMap/Service, watch Pods/ConfigMaps). Expert Fabric8 options via system properties/env vars. E.g. `[code: containerized.master.env.KUBERNETES_MAX_CONCURRENT_REQUESTS: 200 ... (133 chars)]`.

### Accessing Flink's Web UI
Via `kubernetes.rest-service.exposed.type`:
- ClusterIP: cluster-internal only; need `kubectl port-forward service/<ServiceName> 8081` then localhost:8081. `[code: $ kubectl port-forward service/<ServiceName> 8081 ... (49 chars)]`.
- NodePort: `<NodeIP>:<NodePort>`.
- LoadBalancer: cloud LB; may initially return NodePort in client log; `kubectl get services/<cluster-id>-rest` → EXTERNAL-IP → `http://<EXTERNAL-IP>:8081`. Warning: LoadBalancer may make cluster PUBLICLY accessible (arbitrary code execution risk).

### Logging
Exposes `conf/log4j-console.properties` + `conf/logback-console.xml` as ConfigMap. Logs to console + `/opt/flink/log` per pod; STDOUT/STDERR only to console. Access: `kubectl logs <pod-name>`, or `kubectl exec -it <pod-name> bash`. TaskManagers auto de-allocate when idle (harder to access logs) — increase `resourcemanager.taskmanager-timeout`. Dynamic log level: edit ConfigMap `[code: $ kubectl edit cm flink-config-my-first-flink-cluster ... (53 chars)]`.

### Using Plugins
Copy to correct location in JM/TM pod; built-in plugins usable without mounting volume/custom image. E.g. enable S3 plugin: `[code: $ ./bin/kubernetes-session.sh ... (204 chars)]`.

### Custom Docker Image
Via `kubernetes.container.image.ref`. Community provides a rich base image.

### Using Secrets
Two ways: as files from pod (`-Dkubernetes.secrets=mysecret:/path/to/s` → files username/password), or as env vars (`-Dkubernetes.env.secretKeyRef=\` → SECRET_USERNAME/SECRET_PASSWORD).

### Mounting Persistent Volume Claims (PVCs)
Two config options for mounting PVCs to JM/TM pods: `[table: 3 rows]`. Mount single PVC: `[code: $ ./bin/kubernetes-session.sh \ ... (160 chars)]`. Multiple via commas: `[code: ... (185 chars)]`. Read-only: `[code: ... (210 chars)]`. Example PVC for checkpoints: create PVC `[code: apiVersion: v1 ... (199 chars)]`, start cluster `[code: $ ./bin/flink run \ ... (353 chars)]`.
Prerequisites: PVCs must exist in same namespace before deployment; appropriate access modes — RWO (single pod, standalone JM), RWX (multiple pods, HA setups/JM+TM write same storage), ROX (read-only multi-pod, reference data). HA warning: use RWX for HA; RWO causes mount failures/I/O errors. `kubernetes.persistent-volume-claim-read-only` applies globally to all PVCs — for per-PVC modes use Pod Templates.

### High-Availability on Kubernetes
Use existing HA services. Set `kubernetes.jobmanager.replicas` > 1 for standby JobManagers (faster recovery). HA must be enabled when starting standby JMs.

### Manual Resource Cleanup
Flink uses K8s OwnerReferences; all created resources (ConfigMap/Service/Pod) have OwnerReference set to `deployment/<cluster-id>`; deleting deployment cascades. `[code: $ kubectl delete deployment/<cluster-id> ... (40 chars)]`.

### Supported Kubernetes Versions
All >= 1.9.

### Namespaces
Via `kubernetes.namespace`.

### RBAC
Configure RBAC roles + service accounts for JM to access API server. Default service account may lack create/delete pods permission. Either update default SA or specify another: `[code: $ kubectl create clusterrolebinding flink-role-binding-default --clust ... (114 chars)]`, or create flink-service-account + role binding `[code: $ kubectl create serviceaccount flink-service-account ... (180 chars)]` and use `-Dkubernetes.service-account=flink-service-account`.

### Pod Template
Define JM/TM pods via template files (`kubernetes.pod-template-file.default`); main container name must be `flink-main-container`. Field resolution categories: Defined by Flink (user can't config), Defined by user (precedence: explicit config option > pod template > default), Merged with Flink (Flink values win on same-name). Overwrite tables: Pod Metadata `[table: 6 rows]`, Pod Spec `[table: 7 rows]`, Main Container Spec `[table: 8 rows]`. Example `pod-template.yaml`: `[code: apiVersion: v1 ... (1381 chars)]`.

### User jars & Classpath
Session: JAR in startup command. Application: JAR in startup command + all JARs in usrlib folder.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/yarn/

# Apache Hadoop YARN

Flink services submitted to YARN ResourceManager → containers on NodeManagers; JM/TM deployed into containers. Flink dynamically allocates/de-allocates TMs based on required slots.

## Getting Started
Assumes functional YARN >= 2.10.2 (EMR/DataProc/Cloudera convenient). Check `yarn top` (no errors). Download/unpack Flink. IMPORTANT: set `HADOOP_CLASSPATH` (`export HADOOP_CLASSPATH=\`hadoop classpath\``), verify via `echo $HADOOP_CLASSPATH`.

### Starting a Flink Session on YARN
[code: ... (629 chars)] — launch session + submit example job.

## Deployment Modes
Production recommendation: Application Mode.

### Application Mode
main() runs on JM in YARN; cluster shuts down when app finishes. Stop via `yarn application -kill <ApplicationId>` or cancel job. `[code: ./bin/flink run -t yarn-application ./examples/streaming/TopSpeedWindo ... (78 chars)]`. Interact: `[code: # List running job on the cluster ... (222 chars)]`. Cancelling job stops cluster. Lightweight submission via `yarn.provided.lib.dirs` + pre-upload app jar: `[code: ./bin/flink run -t yarn-application \ ... (145 chars)]` (Flink jars + app jar picked from remote locations, not shipped by client).

### Session Mode
Two modes: attached (default, client keeps running tracking cluster; client termination signals shutdown); detached (`-d`/`--detached`, client returns; need another invocation/YARN tools to stop). Creates hidden `/tmp/.yarn-properties-<username>` for cluster discovery. Manually specify target: `[code: ./bin/flink run -t yarn-session \ ... (124 chars)]`. Re-attach: `[code: ./bin/yarn-session.sh -id application_XXXX_YY ... (45 chars)]`. Pass config via `-Dkey=value`; shortcut args via `./bin/yarn-session.sh -h`.

## Flink on YARN Reference
### Configuring Flink on YARN
Framework-managed params (may be overwritten at runtime): `jobmanager.rpc.address` (set to JM container address), `io.tmp.dirs` (YARN temp dirs if unset), `high-availability.cluster-id` (auto-generated HA ID). Pass Hadoop config via `HADOOP_CONF_DIR` env var (default loads from classpath via HADOOP_CLASSPATH).

### Resource Allocation Behavior
JM requests extra TMs if existing resources insufficient (esp. Session Mode as jobs submitted); unused TMs freed after timeout. JM/TM memory configs respected; reported VCores = slots per TM by default; `yarn.containers.vcores` overrides (requires CPU scheduling enabled). Failed containers (incl. JM) replaced by YARN; max JM restarts via `yarn.application-attempts` (default 1); app fails once attempts exhausted.

### High-Availability on YARN
HA via YARN + HA service (persists JM metadata, leader election). YARN restarts failed JMs; max restarts = Flink `yarn.application-attempts` (default 2) limited by YARN `yarn.resourcemanager.am.max-attempts` (default 2). Flink manages `high-availability.cluster-id` (defaults to YARN application id) — do NOT overwrite (distinguishes HA clusters in backend like ZooKeeper; overwriting causes cross-cluster interference).

#### Container Shutdown Behaviour
- YARN 2.3.0–2.4.0: all containers restarted if AM fails.
- YARN 2.4.0–2.6.0: TM containers kept alive across AM failures (faster restart).
- YARN >= 2.6.0: attempt failure validity interval = Flink Pekko timeout (avoids long jobs depleting attempts).
- Hadoop YARN 2.4.0 major bug (fixed 2.5.0) preventing container restarts from restarted AM/JM (FLINK-4142); recommend >= Hadoop 2.5.0 for HA on YARN.

### Supported Hadoop versions
Compiled against Hadoop 2.10.2; all >= 2.10.2 supported incl. Hadoop 3.x. Provide Hadoop deps via HADOOP_CLASSPATH, or lib/ folder, or pre-bundled Hadoop fat jars (shaded; community NOT testing YARN integration against these).

### Running Flink on YARN behind Firewalls
Configure REST endpoint port range via `rest.bind-port` (single port "50010", range "50000-50025", or combination) to submit jobs crossing firewall.

### User jars & Classpath
Session: JAR in startup command. Application: JAR in startup command + usrlib folder JARs; included in system classpath by default, controlled by `yarn.classpath.include-user-jar`. DISABLED → user classpath. Position: ORDER (default, lexicographic), FIRST (beginning), LAST (end).

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/

# Configuration

All config in `conf/` directory (config.yaml). Parsed/evaluated at process startup — changes require restart. Out-of-box uses default Java; override via `JAVA_HOME` or `env.java.home` (must be flattened one-line key-value format). Different config dir via `FLINK_CONF_DIR` (per-job configs for non-session resource providers; NOT supported in Docker/standalone K8s — use `FLINK_PROPERTIES` env var there). Session clusters: config only affects execution params, not underlying cluster.

# Flink Configuration File
Since Flink 2.0, ONLY `config.yaml` (YAML 1.2 syntax); `flink-conf.yaml` no longer supported. More flexible/powerful than old simple key-value pairs.

### Usage
Config Key: nested `[code: restart-strategy: ... (135 chars)]` or flatten `[code: restart-strategy.type: failure-rate ... (194 chars)]`.
Config Value: YAML 1.2 core schema; types `[table: 12 rows]`; all types configurable as strings via single/double quotes.

### Migrate from flink-conf.yaml to config.yaml
Behavior changes:
- Null value: flink-conf blank only; config.yaml blank or `null`/`Null`/`NULL`/`~`.
- Comment: flink-conf everything after first `#`; config.yaml `#` considered comment only with ≥1 space before it OR at line start.
- Special char escaping: flink-conf only in Lists/Maps (list elems with `;`, map elems with `,`/`:`); config.yaml per YAML 1.2 spec.
- Duplicated keys: flink-conf allowed (last wins); config.yaml NOT allowed (error on load).
- Invalid config: flink-conf ignored; config.yaml error on load.

Migration Tool: place old flink-conf.yaml in conf/, run `[code: bin/migrate-config-file.sh ... (26 chars)]` in $FLINK_HOME → outputs config.yaml. All values recognized as String (legacy parser limitation); some quoted; Flink converts to actual types via ConfigOption during parsing.

# Basic Setup
Default config supports single-node session cluster without changes.

Hostnames / Ports (only standalone app/session; Yarn/active K8s auto-discover):
- `rest.address`, `rest.port`: client→Flink (JM hostname or K8s service in front of REST).
- `jobmanager.rpc.address` (default "localhost"), `jobmanager.rpc.port` (default 6123): TM→JM/ResourceManager. Ignored in HA (leader election).

Memory Sizes (defaults too low for complex apps):
- `jobmanager.memory.process.size`: total JM process size.
- `taskmanager.memory.process.size`: total TM process size.
Total includes everything; Flink subtracts JVM overhead (metaspace etc.), divides rest among components (heap/off-heap; TM also network/managed). Format e.g. `1536m`/`2g`.

Parallelism:
- `taskmanager.numberOfTaskSlots` (default 1): slots per TM; multiple slots amortize constant overheads. Smaller TMs (1 slot each) = best isolation; fewer larger TMs = better utilization, weaker isolation.
- `parallelism.default` (default 1).

Checkpointing (defaults if app doesn't configure): `state.backend.type` (hashmap/rocksdb/forst); `execution.checkpointing.dir` (path URI e.g. s3://.../checkpoints); `execution.checkpointing.savepoint-dir`; `execution.checkpointing.interval` (>0 to enable).

Web UI: `web.submit.enable` (default true; session still accepts REST even if disabled); `web.cancel.enable` (default true); `web.upload.dir`; `web.exception-history-size`.

Other: `io.tmp.dirs` (local data; default java.io.tmpdir; rotate if list; holds RocksDB files/spilled results/cached jars — NOT relied on for persistence but deletion triggers heavyweight recovery; set to non-purged dir; Yarn/K8s auto-config to local working dirs).

# Common Setup Options
### Hosts and Ports `[table: 18 rows]` (JM host/port only relevant standalone non-HA; HA uses HA-Service e.g. ZooKeeper; K8s/Yarn use framework discovery).

### Fault Tolerance `[table: 2 rows]` — defines cluster default restart strategy (only if no job-specific via ExecutionConfig). Fixed Delay `[table: 3 rows]`; Exponential Delay `[table: 7 rows]`; Failure Rate `[table: 4 rows]`.

### Retryable Cleanup `[table: 2 rows]` — cleanup after globally-terminal state, retried on failure. Fixed-Delay `[table: 3 rows]`; Exponential-Delay `[table: 4 rows]`.

### Checkpoints and State Backends — only for continuous streaming (batch uses different internal data structures). State Backends `[table: 2 rows]`; Checkpoints `[table: 11 rows]`.

### High Availability — JM process recovery; external service stores recovery metadata + elects/locks leader (avoid split-brain). `[table: 4 rows]`; JobResultStore `[table: 2 rows]`; ApplicationResultStore `[table: 3 rows]`; ZooKeeper `[table: 3 rows]`.

### Memory Configuration `[table: 27 rows]` — usually only set `taskmanager.memory.process.size` or `taskmanager.memory.flink.size` + adjust `taskmanager.memory.managed.fraction` (heap/managed ratio); rest for perf tuning/debugging.

### Miscellaneous `[table: 5 rows]`.

# Security
### SSL — network via SSL; see SSL Setup Docs. `[table: 23 rows]`.
### Auth with External Systems — Delegation token framework (pluggable, protocol-agnostic) `[table: 5 rows]`; ZooKeeper Auth `[table: 4 rows]`; Kerberos `[table: 7 rows]`.

# Resource Orchestration Frameworks
Not always necessary (e.g. deploy Flink on K8s without Flink knowing — no K8s config needed). Options needed only when Flink actively requests/releases resources. YARN `[table: 36 rows]`; Kubernetes `[table: 58 rows]`.

# State Backends
### RocksDB `[table: 9 rows]` (common options; advanced in Advanced RocksDB section). ### ForSt `[table: 11 rows]`.

# Metrics `[table: 27 rows]`. ### RocksDB Native Metrics (property-based by column family; statistics-based at DB level; may degrade perf) `[table: 40 rows]`. ### ForSt Native Metrics `[table: 40 rows]`.

# Traces `[table: 10 rows]`.

# History Server — keeps completed job info; enable job archiving via `jobmanager.archive.fs.dir`. `[table: 15 rows]`.

# Experimental

# Client `[table: 5 rows]`.

# User Artifact Management — upload/fetch local artifacts in Application Mode. Upload to DFS is K8s-specific (`kubernetes.artifacts.*`); fetch on deployed app cluster from DFS/HTTP(S) (supported in Standalone Application Mode + Native K8s Application Mode). `[table: 5 rows]`.

# Execution `[table: 10 rows]`, `[table: 8 rows]`. ### Pipeline `[table: 23 rows]`. ### Checkpointing `[table: 34 rows]`. ### Recovery `[table: 6 rows]`.

# Debugging & Expert Tuning
### Class Loading (dynamic code loading for session; hides classpath deps to reduce conflicts) `[table: 6 rows]`. ### Advanced debugging `[table: 2 rows]`. ### Advanced Checkpointing `[table: 4 rows]`. ### State Latency Tracking `[table: 5 rows]`. ### State Size Tracking `[table: 5 rows]`. ### Advanced RocksDB `[table: 5 rows]`. ### Advanced ForSt `[table: 10 rows]`. ### State Changelog Options `[table: 6 rows]`; FileSystem-based Changelog (when `state.changelog.storage=filesystem`) `[table: 15 rows]`. ### RocksDB Configurable Options (fine-grained ColumnFamily control; advanced perf tuning; also via `RocksDBStateBackend.setRocksDBOptions(RocksDBOptionsFactory)`) `[table: 28 rows]`. ### ForSt Configurable Options `[table: 27 rows]`. ### Advanced Fault Tolerance `[table: 11 rows]`. ### Advanced Cluster `[table: 9 rows]`. ### Advanced JobManager `[table: 3 rows]`. ### Advanced Scheduling `[table: 38 rows]`. ### Advanced HA `[table: 2 rows]`; ZooKeeper `[table: 11 rows]`; Kubernetes `[table: 4 rows]`. ### Advanced SSL `[table: 6 rows]`. ### Advanced REST/Client `[table: 24 rows]`. ### Advanced Web UI `[table: 14 rows]`. ### Full JobManager Options: JM `[table: 28 rows]`, Blob Server (JM component for large object distribution/caching) `[table: 11 rows]`, ResourceManager `[table: 16 rows]`. ### Full TaskManagerOptions `[table: 30 rows]` (network memory tuning via `taskmanager.network.memory.buffer-debloat.*`); Data Transport Network Stack `[table: 23 rows]`; RPC/Pekko (Pekko for RPC NOT data transport) `[table: 24 rows]`.

# JVM and Logging Options `[table: 20 rows]`.

# Forwarding Environment Variables (Yarn): `containerized.master.env.<VAR>` (JM), `containerized.taskmanager.env.<VAR>` (TM).

# Deprecated Options: Optimizer `[table: 4 rows]`; Runtime Algorithms `[table: 5 rows]`; File Sinks `[table: 3 rows]`.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_setup/

# Set up Flink's Process Memory

Flink tightly controls JVM memory usage of its components. Memory config applicable since 1.10 (TM) / 1.11 (JM); upgrade from earlier → check migration guide.

## Configure Total Memory
Total process memory = total Flink memory (JVM Heap + Off-heap Direct/Native) + JVM overhead. Simplest: configure one of `[table: 3 rows]` (total Flink memory OR total process memory); rest adjusted automatically.
- Total Flink memory: better for standalone (declare memory given to Flink itself; splits into heap + off-heap).
- Total process memory: declare total JVM process size; for containerized (K8s/Yarn) = requested container size.
Alternatively configure internal components directly (TM/JobManager). One of the three ways MUST be used (except local execution) or startup fails. Required explicit subsets (no defaults) `[table: 4 rows]`. WARNING: configuring both total process AND total Flink memory NOT recommended (deployment failures from conflicts); configuring other components also requires caution.

## JVM Parameters
Flink explicitly adds JVM args at startup based on configured/derived sizes `[table: 4 rows]`. (*) heap usable depends on GC (some GCs reserve heap). (**) native non-direct user-code memory counted as off-heap. (***) JVM Direct memory limit added for JM only if `jobmanager.memory.enable-jvm-direct-memory-limit` set.

## Capped Fractionated Components
Components that can be a fraction of another memory size, constrained by min-max range:
- JVM Overhead: fraction of total process memory.
- Network memory: fraction of total Flink memory (TM only).
Size must be within max/min or startup fails; max/min have defaults or explicit set. If same max=min → fixes size. If not explicitly configured → fraction of total, capped by min/max.
Examples:
- total=1000MB, JVM Overhead min=64/max=128/fraction=0.1 → 100MB (within 64-128).
- total=1000MB, min=128/max=256/fraction=0.1 → 128MB (fraction-derived 100 < min 128, so min wins).
- Fraction ignored if total + other components defined: JVM Overhead = rest of total (must be within min/max or fail). E.g. total=1000MB, task heap=100MB, min=64/max=256, fraction=0.1 → not 100MB (fraction) but rest of total within 64-256 or fail.
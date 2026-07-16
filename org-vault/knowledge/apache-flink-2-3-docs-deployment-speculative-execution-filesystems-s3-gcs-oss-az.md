---
title: Apache Flink 2.3 docs — Deployment (speculative execution/filesystems S3-GCS-OSS-Azure-plugins/HA + ZooKeeper HA)
tags: [org, flink, flink-2.3, docs, deployment, speculative-execution, filesystems, s3, gcs, oss, azure, plugins, high-availability, zookeeper, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:59:32.874Z'
updated: '2026-07-08T04:59:32.874Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — Deployment (speculative execution / filesystems S3-GCS-OSS-Azure-plugins / HA overview + ZooKeeper HA)

**Executive summary — WHY this exists:** Reference capture of Flink 2.3 deployment sub-pages covering (1) speculative execution for batch jobs (slow-task detection + blocklist + duplicate attempts), (2) the pluggable filesystem abstraction (local, S3 with three implementations incl. new Native experimental, GCS, Aliyun OSS, Azure WASB/ABFS, plugin classloader isolation, common connection-limiting/priority config), and (3) JobManager High Availability (leader election, ZooKeeper + Kubernetes HA services, data lifecycle, ApplicationResultStore/JobResultStore, ZooKeeper configuration & retry/suspend-tolerance). Stored verbatim-condensed so it serves as a faithful lookup for filesystem plugin setup, S3 credential strategies, and HA config keys without re-fetching the live site.

---

## Speculative Execution (speculative_execution)

Mechanism to mitigate batch-job slowness caused by problematic nodes (hardware issues, accidental I/O busy, high CPU load). Such nodes make hosted tasks much slower, affecting overall batch execution time. Speculative execution starts new attempts of the slow task on nodes NOT detected as problematic; new attempts process the same input and produce the same data; the old attempt keeps running; first finished attempt is admitted (output seen/consumed downstream), remaining attempts canceled.

Mechanism: slow task detector detects slow tasks → their nodes identified as problematic and blocked via the **blocklist** mechanism → scheduler creates new attempts deployed on non-blocked nodes.

### Usage

**Enable:** `execution.batch.speculative.enabled: true`. Only Adaptive Batch Scheduler supports it (default batch scheduler unless another explicitly configured).

**Tuning — scheduler options:**
- `execution.batch.speculative.max-concurrent-executions`
- `execution.batch.speculative.block-slow-node-duration`

**Tuning — slow task detector:**
- `slow-task-detector.check-interval`
- `slow-task-detector.execution-time.baseline-lower-bound`
- `slow-task-detector.execution-time.baseline-multiplier`
- `slow-task-detector.execution-time.baseline-ratio`

Detector (execution-time based) periodically counts finished executions; when finished-execution ratio reaches `baseline-ratio`, baseline = execution-time median × `baseline-multiplier`; running tasks exceeding baseline are flagged slow. Execution time is **weighted with input data volume** so executions with large data-volume differences but close computing power aren't flagged slow under data skew (avoids unnecessary speculative attempts). Note: weighting doesn't apply if the node is a Source or Hybrid Shuffle mode is used (input data volume can't be judged).

### Enabling sources

Custom `Source` using custom `SourceEvent` → its `SplitEnumerator` must implement `SupportsHandleExecutionAttemptSourceEvent` (SplitEnumerator must be aware which attempt sent the event, else exceptions on JM receiving source events → job failures).

`[code: public interface SupportsHandleExecutionAttemptSourceEvent { ... (149 chars)]`

No extra change for `SourceFunction` sources, `InputFormat` sources, new sources; all Apache Flink source connectors work with speculative execution.

### Enabling sinks

Speculative execution **disabled by default for a sink** unless it implements `SupportsConcurrentExecutionAttempts` (compatibility reasons).

`[code: public interface SupportsConcurrentExecutionAttempts {} ... (55 chars)]`

Works for `Sink`, `SinkFunction`, `OutputFormat`. If ANY operator in a task doesn't support speculative execution, the entire task is marked not supporting it (so a non-supporting Sink disables speculative execution for its whole task). For `Sink`, Flink disables speculative execution for `Committer` (incl. operators extended by `WithPreCommitTopology` and `WithPostCommitTopology`) — concurrent committing can cause unexpected problems; committer unlikely to be batch bottleneck.

### Checking effectiveness

Web UI shows speculative attempts on the SubTasks tab of vertices on the job page; blocked taskmanagers shown on Flink cluster Overview and Task Managers pages. Effectiveness also checkable via metrics.

---

## File Systems (filesystems/overview)

Flink uses file systems to consume/persistently store data (results + fault tolerance/recovery). File system for a given file is determined by its **URI scheme** (e.g. `file:///home/user/text.txt` local; `hdfs://namenode:50010/data/user/text.txt` HDFS). File system instances instantiated once per process, then cached/pooled (avoid config overhead per stream + enforce constraints like connection/stream limits).

### Local File System

Built-in support for the local machine's FS incl. NFS/SAN mounts. Usable by default, no config. `file://` scheme.

### Pluggable File Systems

Supported:
- **Amazon S3** — two alternative implementations: `flink-s3-fs-presto` and `flink-s3-fs-hadoop` (both self-contained, no dependency footprint). (Plus the new Native S3 — see S3 page.)
- **Aliyun OSS** — `flink-oss-fs-hadoop`, `oss://` scheme, Hadoop-based, self-contained.
- **Azure Data Lake Store Gen2** — `flink-azure-fs-hadoop`, `abfs(s)://` schemes, Hadoop-based, self-contained.
- **Azure Blob Storage** — `flink-azure-fs-hadoop`, `wasb(s)://` schemes, Hadoop-based, self-contained.
- **Google Cloud Storage** — `gcs-connector`, `gs://` scheme, Hadoop-based, self-contained.

Use as plugins: copy the corresponding JAR from `opt/` to a subdir of `plugins/` before starting Flink, e.g. `mkdir ./plugins/s3-fs-hadoop` + copy JAR.

Attention: plugin mechanism introduced in Flink 1.9 (dedicated classloaders per plugin, moving away from class shading). Old mechanism (JAR into `lib/`) still works for provided FS / own impls, BUT since 1.10 **s3 plugins must be loaded via plugin mechanism** (old way no longer works — plugins not shaded/relocated since 1.10). lib-based loading won't be supported in future.

### Adding a new pluggable FS implementation

`FileSystem` represented via `org.apache.flink.core.fs.FileSystem`. To add:
1. Add the FS implementation (subclass of `org.apache.flink.core.fs.FileSystem`).
2. Add a factory that instantiates it and declares the scheme (subclass of `org.apache.flink.core.fs.FileSystemFactory`).
3. Add a service entry: file `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` containing the factory class name (Java Service Loader).
4. Optionally override `getPriority()` (default 0) — highest priority wins when multiple factories exist for the same scheme (e.g. migration). For new experimental factories, recommended to override `getPriority()` to return `-1` (production-safe defaults for migrations).

During plugin discovery, factory loaded by dedicated classloader to avoid class conflicts; same classloader used for instantiation + operations. Avoid `Thread.currentThread().getContextClassLoader()` in implementations.

### Hadoop File System (HDFS) and other Hadoop impls

For schemes where Flink finds no directly supported FS, falls back to Hadoop. All Hadoop FS available when `flink-runtime` + Hadoop libs on classpath. Supports all FS implementing `org.apache.hadoop.fs.FileSystem` + all HCFS-compatible:
- HDFS (tested), Alluxio (tested), XtreemFS (tested), FTP via Hftp (not tested), HAR (not tested)…

Hadoop config must have entry for the required FS impl in `core-site.xml`. Recommend Flink built-in FS unless required otherwise (e.g. YARN resource storage via `fs.defaultFS` in Hadoop `core-site.xml`).

**Alluxio:** add entry to `core-site.xml`:
`[code: <property> ... (96 chars)]`

---

## File Systems Common Configurations (filesystems/common)

### Default File System

`fs.default-scheme: <default-fs>` — used when paths don't specify scheme/authority. e.g. `fs.default-scheme: hdfs://localhost:9000/` → `/user/hugo/in.txt` interpreted as `hdfs://localhost:9000/user/hugo/in.txt`.

### Connection limiting

Limit total concurrent connections a FS can open (useful when FS can't handle many concurrent reads/writes/connections — e.g. small HDFS clusters with few RPC handlers overwhelmed by a large Flink job building connections during a checkpoint). FS identified by scheme.

`[code: fs.<scheme>.limit.total: (number, 0/-1 mean no limit) ... (289 chars)]`

Limit input/output connections separately (`fs.<scheme>.limit.input`, `fs.<scheme>.limit.output`) and total (`fs.<scheme>.limit.total`); opening more blocks until streams close. If opening takes longer than `fs.<scheme>.limit.timeout`, opening fails. Inactivity timeout `fs.<scheme>.limit.stream-timeout` forcibly closes streams not reading/writing for that duration (prevents inactive streams hogging the pool). Limit enforcement **per TaskManager/file system**; pools independent per scheme+authority (e.g. `hdfs://myhdfs:50010/` and `hdfs://anotherhdfs:4399/` have separate pools).

### File System Factory Priority

When multiple `FileSystemFactory` impls available for the same URI scheme (e.g. migration), Flink resolves via priority — highest wins. Each factory declares default priority via `getPriority()` (default 0); override via config:

`[code: fs.<scheme>.priority.<factoryClassName>: <integer> ... (50 chars)]`

Higher = higher priority. If unset, factory's declared priority used. Equal priorities → winner depends on classloading order (non-deterministic; Flink logs a warning). Set explicit priorities for deterministic behavior. Example (Hadoop over Presto S3, default priority 0):

`[code: fs.s3.priority.org.apache.flink.fs.s3.hadoop.S3FileSystemFactory: 1 ... (67 chars)]`

---

## Amazon S3 (filesystems/s3)

S3 = cloud object storage; use with Flink for reading/writing + streaming state backends. S3 objects used like regular files: `s3://<your-bucket>/<endpoint>` (endpoint = single file or dir).

`[code: // Read from S3 bucket ... (691 chars)]`

Can use S3 anywhere Flink expects a FileSystem URI (HA setup, `EmbeddedRocksDBStateBackend`) unless otherwise stated.

### S3 FileSystem Implementations — three independent

`[table: 4 rows]` (Native experimental, Presto, Hadoop)

Previously: Presto (recommended for checkpointing throughput) vs Hadoop (only one with `RecoverableWriter`, required by FileSink). **Native S3** unifies both in one plugin; benchmarks show significant checkpoint throughput improvement over Presto. All three self-contained (no Hadoop classpath needed).

### Common Configuration — credentials (three independent alternatives)

**IAM (recommended):** IAM roles securely give Flink instances credentials to access S3 buckets; manage access within AWS, no access keys distributed.

**Delegation Tokens:** time-bounded, auto-negotiated. JM uses long-lived creds (access+secret key) to call AWS STS → short-lived session tokens distributed to TMs. Each impl has its own delegation-token provider with a dedicated config prefix; set access-key, secret-key, region under the corresponding prefix:

`[code: # For Native S3 implementation ... (715 chars)]`

All three (access-key, secret-key, region) must be set for tokens to be issued. `DynamicTemporaryAWSCredentialsProvider` auto-included in the credentials provider chain for each impl; TMs consume distributed tokens with no extra config.

**Access Keys:** `s3.access-key` + `s3.secret-key` in Flink config. IAM roles preferred (avoid managing/distributing static credentials).

`[code: s3.access-key: your-access-key ... (61 chars)]`

### Non-S3 endpoint & path-style access

S3-compliant object stores: `s3.endpoint: your-endpoint-hostname`. Path-style access (for stores without virtual-host addressing): `s3.path-style-access: true` (legacy `s3.path.style.access` still supported as fallback).

### Implementation Details

**Native S3 (Experimental in Flink 2.3):** pure-Java on AWS SDK v2, **removes Hadoop dependency**, registered under `s3://` and `s3a://`. Drop-in replacement for Presto/Hadoop; supports checkpointing, FileSink (via RecoverableWriter), SSE (SSE-S3, SSE-KMS), cross-account access via IAM role assumption, entropy injection, bulk copy via `S3TransferManager`. Functionally complete, strong benchmark performance.

Setup: `mkdir -p ./plugins/s3-fs-native` + copy `opt/flink-s3-fs-native` JAR. Extra config beyond common options:

`[code: # Server-side encryption ... (820 chars)]`

When `fs.s3.aws.credentials.provider` unset, Native builds chain: delegation tokens → static creds (if access/secret set) → AWS SDK v2 `DefaultCredentialsProvider` (env vars, instance profiles…). Set only for a custom chain.

**Presto S3:** based on Presto project, registered `s3://` + `s3p://`. Production-proven for checkpointing to S3. Does NOT support FileSink (`createRecoverableWriter` throws `UnsupportedOperationException`). No manual config on EMR. Setup: `mkdir -p ./plugins/s3-fs-presto`. Common options + Presto-specific keys via Presto FS config.

**Hadoop S3:** based on Hadoop, registered `s3://` + `s3a://`. Only **stable** implementation supporting FileSink (via RecoverableWriter). Setup: `mkdir -p ./plugins/s3-fs-hadoop`. Common options + Hadoop `s3a` keys (auto-translated, e.g. `fs.s3a.connection.maximum` → `s3.connection.maximum`).

### Using Multiple S3 Implementations

All three register as handlers for `s3://`; each supports alternative schemes:

`[table: 4 rows]`

Safe to load multiple S3 plugin JARs simultaneously — priority mechanism ensures only one factory handles each scheme. **Native S3 has lowest priority (-1 vs default 0)** → when another impl present, it takes precedence for all overlapping schemes (`s3://`, `s3a://`). Override via `fs.<scheme>.priority.<factoryClassName>`. Use multiple simultaneously via different URI schemes (e.g. `s3a://` for Hadoop sink, `s3p://` for Presto checkpointing). Native introduces no new scheme; to use it, place only `flink-s3-fs-native` in plugins, OR raise its priority via config while others present.

### Advanced Features

**Entropy Injection** (all S3 FS): improves AWS S3 bucket scalability by adding random chars near the beginning of the key. If activated, a configured substring in the path is replaced with random chars (e.g. `s3://my-bucket/_entropy_/checkpoints/dashboard-job/` → `s3://my-bucket/gf36ikvg/checkpoints/dashboard-job/`). Only happens when file creation passes the "inject entropy" write option; otherwise entropy key substring removed entirely. Flink currently passes the option **only to checkpoint data files** (metadata + external URI keep predictable URIs).

`[code: s3.entropy.key: _entropy_ ... (56 chars)]` — string replaced; `s3.entropy.length` — number of random alphanumeric chars.

**s5cmd** (Presto + Hadoop only): both can use `s5cmd` for faster upload/download — benchmarks show >2× CPU efficient (half the CPU for same files, or twice as fast with same CPU). Requires s5cmd binary present/accessible to TMs (e.g. embedded in docker image). Configure: `s3.s5cmd.path: /path/to/the/s5cmd`.

`[code: # Extra arguments passed to s5cmd call ... (343 chars)]`

`s3.s5cmd.batch.max-size` + `.max-files` control s5cmd resource usage. Recommend first verify Flink works without s5cmd, then enable. Credentials: access keys passed to s5cmd; s5cmd also has its own independent credential mechanism.

Limitations: `flink-s3-fs-hadoop` + `flink-s3-fs-presto` use s5cmd **only during recovery** (downloading state files from S3 with RocksDB). `flink-s3-fs-native` uses `S3TransferManager` when `s3.bulk-copy.enabled` (default true) for bulk copy + `s3.async.enabled` (default true) for async read/write — similar perf benefits.

---

## Google Cloud Storage (filesystems/gcs)

`gs://<your-bucket>/<endpoint>` (single file or dir). Use anywhere Flink expects a FileSystem URI (HA setup, `EmbeddedRocksDBStateBackend`, FileSystemCheckpointStorage with streaming state backends).

`[code: // Read from GCS bucket ... (691 chars)]`

**Plugin:** `flink-gs-fs-hadoop` — self-contained, no Hadoop classpath. Registers FS wrapper for `gs://`. Uses Google's `gcs-connector` Hadoop library + Google's `google-cloud-storage` library for `RecoverableWriter` support. Usable with the FileSystem connector. Setup: `mkdir ./plugins/gs-fs-hadoop` + copy `opt/` JAR.

**Configuration:** underlying Hadoop FS configurable via gcs-connector Hadoop keys in Flink config (e.g. `fs.gs.http.connect-timeout` → set `gs.http.connect-timeout: xyz`; Flink translates back). Can also set gcs-connector options directly in Hadoop `core-site.xml` if Hadoop config dir made known via `env.hadoop.conf.dir` or `HADOOP_CONF_DIR`.

`[table: 11 rows]` (flink-gs-fs-hadoop options)

**Authentication:** most GCS ops require auth. Either:
- Set `GOOGLE_APPLICATION_CREDENTIALS` env var to JSON credentials file path (where JMs/TMs run) — **recommended**.
- Set `google.cloud.auth.service.account.json.keyfile` in `core-site.xml` to JSON credentials path (Hadoop config dir known to Flink).

`[code: <configuration> ... (182 chars)]`

Service-account auth enabled by default; can be disabled in `core-site.xml`:

`[code: <configuration> ... (142 chars)]`

Not recommended: gcs-connector authentication-credentials options other than `google.cloud.auth.service.account.json.keyfile` — those credentials won't be used by the `google-cloud-storage` library (RecoverableWriter support) → Flink recoverable-write operations would fail.

---

## Aliyun OSS (filesystems/oss)

`oss://<your-bucket>/<object-name>`. Popular among China cloud users.

`[code: // Read from OSS bucket ... (718 chars)]`

**Shaded Hadoop OSS FS:** `flink-oss-fs-hadoop` — copy `opt/` JAR to a `plugins/` subdir before starting. `mkdir ./plugins/oss-fs-hadoop`. Registers FS wrapper for `oss://`.

**Config:** use same config keys in Flink config as in Hadoop `core-site.xml` (see Hadoop OSS docs). Required:

`[code: fs.oss.endpoint: Aliyun OSS endpoint to connect to ... (140 chars)]`

Alternative `CredentialsProvider` configurable, e.g.:

`[code: # Read Credentials from OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET ... (162 chars)]`

---

## Azure Blob Storage (filesystems/azure)

Microsoft-managed cloud storage. Flink supports `wasb://` and `abfs://`. Azure recommends `abfs://` for ADLS Gen2 (wasb:// works via backward compat); abfs:// for ADLS Gen2 only.

`[code: // WASB unencrypted access ... (441 chars)]`

`[code: // Read from Azure Blob storage ... (918 chars)]`

**Shaded Hadoop Azure Blob FS:** `flink-azure-fs-hadoop` — copy `opt/` JAR to `plugins/` before starting. `mkdir ./plugins/azure-fs-hadoop`. Registers FS wrapper for `wasb://` + `wasbs://` (SSL).

**Credentials — WASB:** Hadoop WASB Azure FS supports credential config via Hadoop config. Flink forwards all configs with `fs.azure` key prefix to Hadoop config. Azure blob storage key in Flink config:

`[code: fs.azure.account.key.<account_name>.blob.core.windows.net: <azure_stor ... (78 chars)]`

Alternatively, read key from `AZURE_STORAGE_KEY` env var:

`[code: fs.azure.account.keyprovider.<account_name>.blob.core.windows.net: org ... (125 chars)]`

**Credentials — ABFS:** several auth ways (see Hadoop ABFS docs). Azure recommends **managed identity** to access ADLS Gen2 via abfs. Flink clusters in services supporting Managed Identities can use them.

Accessing ABFS via storage keys (discouraged):

`[code: fs.azure.account.key.<account_name>.dfs.core.windows.net: <azure_stora ... (77 chars)]`

---

## Plugins (filesystems/plugins)

Plugins facilitate strict separation of code via restricted classloaders. Plugins can't access classes from other plugins or Flink unless whitelisted. Strict isolation allows conflicting library versions without relocating/shading. Currently **file systems + metric reporters** pluggable; future: connectors, formats, user code.

### Isolation and plugin structure

Plugins reside in own folders, can consist of several jars; folder names arbitrary.

`[code: flink-dist ... (181 chars)]`

Each plugin loaded through its own classloader, fully isolated (e.g. `flink-s3-fs-hadoop` + `flink-azure-fs-hadoop` can depend on conflicting lib versions; no shading needed). Plugins may access certain whitelisted packages from Flink's `lib/` — all necessary SPIs loaded through the system classloader so no two versions of `org.apache.flink.core.fs.FileSystem` exist at any time (singleton required as Flink runtime entry point into the plugin). Service classes discovered via `java.util.ServiceLoader` — retain service definitions in `META-INF/services` during shading. More Flink core classes still accessible as SPI system is fleshed out. Common logger frameworks whitelisted (uniform logging across Flink core, plugins, user code).

### File Systems

All FS pluggable — should be used as plugins. Copy `opt/` JAR to `plugins/` subdir before starting.

`[code: mkdir ./plugins/s3-fs-hadoop ... (90 chars)]`

S3 FS (`flink-s3-fs-presto`, `flink-s3-fs-hadoop`) **only usable as plugins** (relocations removed); placing in `lib/` → system failures. Strict isolation means FS no longer access credential providers in `lib/` — add needed providers to the respective plugin folder.

### Metric Reporters

All Flink metric reporters usable as plugins (see metrics docs).

---

## High Availability overview (ha/overview)

JobManager HA hardens a Flink cluster against JM failures — ensures the cluster always re-executes submitted applications running at failure time. After recovery, jobs may resume (from latest checkpoint) or be abandoned (→ FAILED + cleaned up) depending on execution path in the application's `main()`. Jobs before/after a failure matched by name; identical names further matched by submission order. To avoid mismatches (esp. non-deterministic submission order), assign each job a unique name via `execute(jobName)`.

### JobManager High Availability

JM coordinates every Flink deployment (scheduling + resource management). Default: single JM per cluster = single point of failure (SPOF) — JM crash → no new submissions + running programs fail. HA eliminates the SPOF. Configurable for every cluster deployment.

**How to make a cluster HA:** single leading JM at any time + multiple standby JMs to take over on leader failure. Example: three JM instances. HA services encapsulate required services:
- **Leader election** — single leader out of n candidates
- **Service discovery** — retrieve current leader's address
- **State persistence** — persist state needed for successor to resume (JobGraphs, user code jars, completed checkpoints)

### High Availability Services — two implementations

- **ZooKeeper:** usable with every Flink cluster deployment; requires a running ZooKeeper quorum.
- **Kubernetes:** only works running on Kubernetes.

### HA data lifecycle

Flink persists metadata for applications to recover them. HA data kept until the application reaches a terminal state (finished/cancelled/failed) → then all HA data (incl. metadata in HA services) deleted. Similar lifecycle for per-job HA data.

### ApplicationResultStore

Archives final result of an application reaching a terminal state. Stored on a file system (`application-result-store.storage-path`). Entries marked dirty while the application wasn't cleaned up properly (artifacts in the application's subfolder in `high-availability.storageDir`). Dirty entries subject to cleanup — cleaned by Flink now or picked up during recovery; deleted once cleanup succeeds. See HA configuration options for behavior tuning.

### JobResultStore

Archives final result of a job reaching a globally-terminal state. Stored on a file system (`job-result-store.storage-path`). Entries marked dirty while the job wasn't cleaned up properly (artifacts in the job's subfolder in `high-availability.storageDir`). Dirty entries subject to cleanup; deleted once cleanup succeeds and the corresponding application has created a dirty entry.

---

## ZooKeeper HA Services (ha/zookeeper_ha)

Flink's ZK HA services use ZooKeeper for HA. Flink leverages ZK for distributed coordination between all running JMs. ZK is separate from Flink — provides highly reliable distributed coordination via leader election + lightweight consistent state storage. Flink includes bootstrap scripts for a simple ZK installation.

### Configuration (required keys)

- **`high-availability.type: zookeeper`** (required).

`[code: high-availability.type: zookeeper ... (33 chars)]`

- **`high-availability.storageDir`** (required) — JM metadata persisted in the file system; only a pointer stored in ZK.

`[code: high-availability.storageDir: hdfs:///flink/recovery ... (52 chars)]`

Stores all metadata needed to recover from a JM failure.

- **`high-availability.zookeeper.quorum`** (required) — replicated group of ZK servers.

`[code: high-availability.zookeeper.quorum: address1:2181[,...],addressX:2181 ... (69 chars)]`

- **`high-availability.zookeeper.path.root`** (recommended) — root ZK node under which all cluster nodes are placed.

`[code: high-availability.zookeeper.path.root: /flink ... (45 chars)]`

- **`high-availability.cluster-id`** (recommended) — ZK node under which all required coordination data for a cluster is placed.

`[code: high-availability.cluster-id: /default_ns # important: customize per c ... (76 chars)]`

**Important:** don't set `cluster-id` manually when running on YARN, native Kubernetes or another cluster manager — auto-generated there. For multiple Flink HA clusters on bare metal, manually configure separate cluster-ids per cluster.

**Example configuration:**

`[code: high-availability.type: zookeeper ... (261 chars)]`

### ZooKeeper Security (Kerberos)

If ZK runs in secure mode with Kerberos, override configs as necessary:

`[code: # default is "zookeeper". If the ZooKeeper quorum is configured ... (330 chars)]`

See security section of the Flink configuration page + Kerberos-based security internals docs.

### Advanced Configuration

**ZK Client Retry:** on connection fail/interrupt, Flink auto-retries with bounded exponential backoff (doubles wait each retry, caps max to avoid overwhelming ZK while ensuring reasonably fast recovery).
- `high-availability.zookeeper.client.retry-wait` (default 5s) — initial wait, doubles each retry.
- `high-availability.zookeeper.client.max-retry-wait` (default 60s) — max wait, caps backoff.
- `high-availability.zookeeper.client.max-retry-attempts` (default 3) — max attempts before giving up.

**Tolerating Suspended ZK Connections:** default — Flink's ZK client treats suspended connections as an error (invalidates all leaderships → triggers failover). May be too disruptive (e.g. unstable network). `high-availability.zookeeper.client.tolerate-suspended-connections` → tolerate suspended connections, only treat lost connections as error. More resilient against temporary problems but increases risk of ZK timing problems. See Curator's error handling.

### Bootstrap ZooKeeper

Helper scripts ship with Flink. Config template in `conf/zoo.cfg`; configure hosts via `server.X=addressX:peerPort:leaderPort` (X = unique ID per server).

`[code: server.X=addressX:peerPort:leaderPort ... (81 chars)]`

`bin/start-zookeeper-quorum.sh` starts a ZK server on each configured host (via a Flink wrapper reading `conf/zoo.cfg` + setting required config). In production, manage your own ZK installation.

---

*Sources: nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/{speculative_execution, filesystems/overview, filesystems/common, filesystems/s3, filesystems/gcs, filesystems/oss, filesystems/azure, filesystems/plugins, ha/overview, ha/zookeeper_ha}/ — English pages only. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`; prose kept near-verbatim.*
---
title: Apache Flink 2.3 docs — Deployment (network memory tuning/adaptive batch/cli/elastic scaling/fine-grained resource)
tags: [org, flink, flink-2.3, docs, deployment, network-memory, adaptive-batch, cli, elastic-scaling, fine-grained-resource, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:57:39.218Z'
updated: '2026-07-08T04:57:39.218Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — Deployment (network memory tuning / adaptive batch / CLI / elastic scaling / fine-grained resource)

**Executive summary — WHY this exists:** Reference capture of the Flink 2.3 deployment sub-pages covering (1) the network buffer stack + the buffer-debloating auto-tuner, (2) AdaptiveBatchScheduler runtime plan adaptation (auto-parallelism, data balancing, broadcast join, skewed join), (3) the `bin/flink` CLI lifecycle actions and deployment `--target`s, (4) elastic/reactive scaling and rescale history, and (5) fine-grained (per-slot-sharing-group) resource management. Stored verbatim-condensed so it can serve as a faithful lookup for tuning keys, scheduler behaviour, CLI syntax and resource-profile APIs without re-fetching the live site.

---

## Network memory tuning (network_mem_tuning)

Each record is compounded with others into a network buffer — the smallest unit of subtask-to-subtask communication. Flink keeps network buffer queues (in-flight data) on input and output sides. More in-flight data ⇒ higher/more resilient throughput BUT longer checkpoint times: in aligned checkpoints barriers travel along network buffers so more in-flight data = longer barrier propagation; in unaligned checkpoints all captured in-flight data is persisted into the checkpoint so more in-flight = bigger checkpoint.

### Buffer Debloating (since Flink 1.14)

Previously in-flight amount was set via buffer count + buffer size (hard to choose, varies per deployment). Debloating auto-adjusts in-flight data to reasonable values: it computes the max possible throughput of a subtask (if always busy) and sets in-flight data so its consumption time equals a configured value.

- Enable: `taskmanager.network.memory.buffer-debloat.enabled=true`
- Target consume time: `taskmanager.network.memory.buffer-debloat.target=<duration>` (default is good for most cases)
- Uses past throughput to predict remaining consume time. Wrong predictions fail two ways: not enough buffered data (no full throughput) OR too much buffered in-flight data (hurts aligned barrier propagation / unaligned checkpoint size).

Tuning for varying load (sudden spikes, periodic windowed aggregations/joins):
- `taskmanager.network.memory.buffer-debloat.period` — min time between recalculation. Shorter = faster reaction, higher CPU overhead.
- `taskmanager.network.memory.buffer-debloat.samples` — number of samples averaged; fewer = faster reaction, higher chance of miscalculation on spikes/drops.
- `taskmanager.network.memory.buffer-debloat.threshold-percentages` — optimization to prevent frequent small changes.

Monitor metrics: `estimatedTimeToConsumeBuffersMs` (total time to consume all input channels), `debloatedBufferSize` (current buffer size).

### Limitations of debloating

- **Multiple inputs / unions:** throughput calc + debloating are per-subtask. Multiple/unioned inputs of differing throughput → low-throughput input gets too much in-flight, high-throughput input gets too-small buffers. Watch such subtasks when testing.
- **Buffer size & number unchanged:** debloating only caps the maximal used buffer size; it does NOT reduce memory usage. To actually reduce memory, manually reduce buffer amount/size. To go below what debloating allows, manually configure buffer count.
- **High parallelism (above ~200):** default config may mis-perform. If reduced throughput / long checkpoint times appear, increase `taskmanager.network.memory.floating-buffers-per-gate` from default to at least parallelism.

### Network buffer lifecycle

Local buffer pools: one per output stream, one per input gate. Target pool size formula:
`#channels * taskmanager.network.memory.buffers-per-channel + taskmanager.network.memory.floating-buffers-per-gate`
Buffer size configurable via `taskmanager.memory.segment-size`.

**Input buffers:** target not always reached; a threshold splits required (below) vs optional (above) buffers. Not obtaining required → task failure; missing optional → no failure but performance reduction. Default threshold: `Integer.MAX_VALUE` for streaming, `1000` for batch. Don't change unless well understood. Key: `taskmanager.network.memory.read-buffer.required-per-gate.max`. Smaller threshold ⇒ fewer "insufficient network buffers" exceptions but silent perf reduction.

**Output buffers:** one buffer type shared among all subpartitions; per-subpartition buffer count limited by `taskmanager.network.memory.max-buffers-per-channel`. Configured exclusive/floating amounts are only recommended — Flink can progress with one exclusive buffer per output subpartition and zero floating.

**Overdraft buffers:** each output subtask may request up to `taskmanager.network.memory.max-overdraft-buffers-per-gate` (default 5) extra, used only when backpressured AND needing >1 buffer to finish current work (e.g. serializing very large records; FlatMap-like operators producing many records per input; window triggers). Without overdraft buffers the subtask thread blocks on backpressure — prevents unaligned checkpoints from completing. Strictly optional; 0 is acceptable. **Takes effect only for Pipelined Shuffle.**

### Number of in-flight buffers / buffer size

Defaults (exclusive + floating) usually sufficient for max throughput. To set minimum in-flight, set exclusive buffers to 0 and decrease segment size.

Buffer too small / flushed too frequently (`execution.buffer-timeout`) ⇒ decreased throughput (per-buffer overhead >> per-record overhead). Don't increase buffer size/timeout unless a real network bottleneck is observed (downstream idling, upstream backpressured, output queue full, downstream input queue empty).

Buffer too large ⇒ high memory usage; huge checkpoint data (unaligned); long checkpoint time (aligned); inefficient memory use with small `execution.buffer-timeout`.

Buffer count via `taskmanager.network.memory.buffers-per-channel` + `taskmanager.network.memory.floating-buffers-per-gate`. Tune manually:
- Adjust count to expected throughput (bytes/s). Assigning credits + sending buffers takes ~two roundtrip messages; latency depends on network. With buffer roundtrip (~1ms healthy LAN), buffer size, expected throughput: `number_of_buffers = expected_throughput * buffer_roundtrip / buffer_size`. Example: 320MB/s, 1ms, default 32KB segment → 10 actively-used buffers.
- **Floating buffers** (default 8) handle data skew; ideally floating + exclusive (default 2) saturate throughput.
- **Exclusive buffers** provide fluent throughput (one in transit, one being filled). With high throughput they define in-flight data amount. Under backpressure in low-throughput setups, reduce exclusive count.

### Summary

Enable debloating first (may need tuning). If it fails, disable it and manually configure segment size + buffer count: use defaults for max throughput; reduce segment size and/or exclusive count to speed checkpointing and cut network memory.

---

## Adaptive Batch Execution (adaptive_batch)

Traditional batch: execution plan determined before submission; static optimizer needs accurate data characteristics/distribution which can't be predicted pre-run. Statistics often incomplete/inaccurate in production. **AdaptiveBatchScheduler** adjusts the plan at runtime, incrementally building the JobGraph. Optimization strategies:
- Auto-decide operator parallelisms
- Automatic data-distribution load balancing
- Adaptive Broadcast Join
- Adaptive Skewed Join Optimization

### Auto-decide operator parallelisms

Scheduler decides parallelism for operators that have none, based on consumed dataset size. Benefits: relieves users from parallelism tuning; fits datasets of varying daily volume; SQL batch operators get individually-tuned parallelisms.

Usage:
- Toggle `execution.batch.adaptive.auto-parallelism.enabled` (on by default). Related options:
  - `.min-parallelism` — lower bound
  - `.max-parallelism` — upper bound (falls back to `parallelism.default` / `setParallelism()`)
  - `.avg-data-volume-per-task` — expected avg data per task (note: under skew or hitting max-parallelism some tasks may far exceed this)
  - `.default-source-parallelism` — default source parallelism / upper bound for source (falls back to max-parallelism → `parallelism.default`)
- Don't set operator parallelism via `setParallelism()` for operators you want auto-decided.

**Dynamic source parallelism inference:** new `Source` may implement `DynamicParallelismInference` interface. Context provides upper bound, expected avg data/task, dynamic filtering info. Scheduler invokes before scheduling source vertices; avoid time-consuming operations. If not implemented, `execution.batch.adaptive.auto-parallelism.default-source-parallelism` is used. Only decides parallelism for sources without an explicit one.

`[code: public interface DynamicParallelismInference { ... (91 chars)]`

**Performance tuning:** use Sort Shuffle and set `taskmanager.network.memory.buffers-per-channel=0` (decouples network memory from parallelism, avoids "Insufficient number of network buffers" on large jobs). Set `.max-parallelism` to the worst-case parallelism you expect; larger values hurt (more subpartitions degrade hash shuffle + network transmission via small packets).

### Automatic Balancing of Data Distribution

Scheduler tries to evenly distribute data so each downstream subtask consumes roughly the same. No manual config. Works for point-wise (Rescale) and all-to-all (Hash, Rebalance, Custom) edges.

Limitations: requires auto-parallelism enabled (operators must not have manually-set parallelism); does NOT address single-key hotspots (a single key's data can't be split across subtasks for correctness) — partly handled by Skewed Join.

### Adaptive Broadcast Join

Broadcast join: if one table is small enough to fit one node's memory, broadcast it to all nodes; join in memory; cuts shuffling/sorting of the large table. Static optimizers often misjudge (incomplete source stats; intermediate-data size unknown until runtime; misjudging a large table as small → OOM creating in-memory hash table → task restart). Adaptive Broadcast Join dynamically converts Join → Broadcast Join at runtime based on actual input.

Broadcast possibility depends on join type:
`[table: 7 rows]` (broadcast-allowed join-type matrix)

Usage: Adaptive Batch Scheduler enables both compile-time static Broadcast Join and runtime dynamic adaptive Broadcast Join by default. Control timing via `table.optimizer.adaptive-broadcast-join.strategy` (e.g. `RUNTIME_ONLY`). Adjust `table.optimizer.join.broadcast-threshold` (raise for large TM memory, lower for limited memory).

Limitations: no optimization of Join operators inside MultiInput operators; incompatible with Batch Job Recovery Progress (after recovery progress enabled, adaptive broadcast join won't take effect).

### Adaptive Skewed Join Optimization

Frequent keys in joins → big variation in per-task data → degraded performance. Because both join inputs are related, the same keyGroup must go to the same downstream subtask, so plain load balancing isn't enough. Skewed Join dynamically splits skewed & splittable partitions based on runtime stats, alleviating tail latency.

Split possibility depends on join type:
`[table: 7 rows]`

Usage: enabled by default. Strategy via `table.optimizer.skewed-join-optimization.strategy`:
- `none` — disable
- `auto` — allow, but skip if it would need an extra Shuffle for correctness (avoids overhead)
- `forced` — allow even when extra Shuffle introduced

Adjust: `table.optimizer.skewed-join-optimization.skewed-threshold` (min data triggering optimization; when max subtask data exceeds this, Flink reduces max/median ratio below skew factor), `.skewed-factor` (target max/median ratio).

Limitations: affects Join parallelism → requires auto-parallelism enabled; no MultiInput Join operators; incompatible with Batch Job Recovery Progress.

### Adaptive Batch Execution overall limitations

- **AdaptiveBatchScheduler only** — it's the default batch scheduler, so no config needed unless `jobmanager.scheduler: default` is set.
- **BLOCKING or HYBRID jobs only** — shuffle mode must be `ALL_EXCHANGES_BLOCKING` / `ALL_EXCHANGES_HYBRID_FULL` / `ALL_EXCHANGES_HYBRID_SELECTIVE`.
- **FileInputFormat sources unsupported** — incl. `readFile(...)` and `createInput(FileInputFormat, ...)`. Use new sources (FileSystem DataStream/SQL Connector).
- **Inconsistent broadcast results metrics on WebUI** — with auto-parallelism, broadcast byte/record counts sent upstream ≠ received downstream (see FLIP-187).

---

## Command-Line Interface (cli)

`bin/flink` runs JAR-packaged programs and controls execution; part of any Flink setup; connects to the JobManager in the Flink config.

### Job lifecycle

Prerequisite: a running Flink deployment (Kubernetes / YARN / other).

**Submit:** `./bin/flink run <jar> [--detached]`. `--detached` returns after submission (output includes new JobID). `-D<key>=<val>` passes config (e.g. `-Dpipeline.max-parallelism=120`); very useful for application-mode clusters. Session-cluster submission only supports execution config params.

**Monitor:** `./bin/flink list` (shows running + scheduled jobs).

**Savepoint:** `./bin/flink savepoint <JobID> [savepointDir]` — folder optional if `execution.checkpointing.savepoint-dir` set; can choose binary format. For big state, client may time out waiting → use `-detached` (returns on trigger id; query status via REST). **Dispose:** `./bin/flink savepoint --dispose <path>` — for custom state (custom reducing/RocksDB) pass the program JAR via `-j` to avoid `ClassNotFoundException`. Disposal removes data AND cleans up savepoint metadata.

**Checkpoint:** `./bin/flink checkpoint <JobID>` — manually trigger a checkpoint. `--full` triggers a full checkpoint while the job periodically does incremental ones.

**Stop gracefully (with final savepoint):** `./bin/flink stop <JobID>` — stop flows source→sink; sources send last checkpoint barrier → savepoint → `cancel()`. `--savepointPath` required if `savepoint-dir` unset. `--drain` emits MAX_WATERMARK before last barrier (fires event-time timers, flushes windows); job keeps running until sources shut down → can produce post-savepoint records. **Use `--drain` only for permanent termination** — draining before resume can give incorrect results on resume. `-detached` for detached savepoint; optionally choose binary format.

**Cancel ungracefully:** `./bin/flink cancel <JobID>` — Running → Cancelled, computations stopped. `--withSavepoint` (deprecated, use `stop`).

**Start from savepoint:** `./bin/flink run --fromSavepoint <path> [jar]` — `--allowNonRestoredState` skips state that can't be restored (useful if you dropped an operator that was in the savepoint). Choose claim mode (controls who owns the savepoint files).

### CLI actions overview

`[table: 8 rows]` (run, list, savepoint, checkpoint, stop, cancel, etc.)

`bin/flink --help` / `bin/flink <action> --help` for full params.

### Advanced CLI

**REST API:** CLI commands are a subset of REST endpoints; `curl` for more.

**Deployment targets** (`--target`, overwrites `execution.target`):
- YARN: `yarn-session` (existing cluster), `yarn-application` (spin up Application Mode)
- Kubernetes: `kubernetes-session`, `kubernetes-application`
- Standalone: `local` (MiniCluster Session), `remote` (existing cluster)
Jobs must use `run` (Session + Application Mode).

**PyFlink jobs:** no JAR path / main class needed. Flink runs `python` — verify `python --version` is 3.9+.
- Run: `./bin/flink run --python <file.py>`
- With source/resource files: `--pyFiles` adds to PYTHONPATH; `--jarfile` uploads Java UDF/connector JARs; `--pyModule` (`-pym`) specifies entry module (preferred over `-py` for YARN app mode where absolute/relative paths are unknown).
- YARN application mode: `-t yarn-application` with `-pyarch`/`-pyfs` paths relative to `shipfiles`; archive files distributed via blob server (**2 GB limit** — larger files go to a distributed FS and referenced by path).
- Native Kubernetes: requires docker image with PyFlink.

`[table: 8 rows]` (Python-related options for `run`)

Dependencies can also be specified via config or Python API inside code.

---

## Elastic Scaling (elastic_scaling)

Historically parallelism was static (batch couldn't rescale; streaming needed stop+savepoint+restart). New schedulers adjust parallelism at runtime: **Adaptive Scheduler** (streaming) and **Adaptive Batch Scheduler** (batch).

### Adaptive Scheduler (streaming)

Adjusts parallelism based on available slots: reduces if not enough slots (insufficient resources at submission or TM outages); scales back up (to configured parallelism) when slots appear. In **Reactive Mode** configured parallelism is ignored (treated as ∞) → uses all resources. Benefit over default scheduler: handles TM losses gracefully (just scales down). Built on **Declarative Resource Management** — JobMaster declares desired resources (max=∞ for reactive) to ResourceManager.

When JobMaster gets more resources at runtime it auto-rescales from the latest savepoint (no external orchestration). Since Flink 1.18, **Externalized Declarative Resource Management** lets you re-declare resource requirements of a running job — otherwise adaptive scheduler can't handle rescaling due to input-rate / workload-performance changes.

**Externalized Declarative Resource Management (MVP):** addresses (a) Adaptive Scheduler on Session Cluster with multiple competing jobs needing finer-grained resource distribution, (b) Adaptive Scheduler on Application Cluster + Active RM (Native Kubernetes) wanting greedy TM spawning + reactive-mode rescaling. New REST endpoint re-declares resource requirements with per-vertex parallelism boundaries:

`[code: PUT /jobs/<job-id>/resource-requirements ... (312 chars)]`

A "re-scaling endpoint" building block for autoscaling. Can be tried manually via the Flink UI up/down-scale buttons in the task list.

**Usage:** set `jobmanager.scheduler: adaptive` at cluster level. Behaviour configured by all options prefixed `jobmanager.adaptive-scheduler`. On session cluster, no slot-distribution guarantees between jobs when resources are insufficient (external declarative RM partially mitigates; recommended: use application cluster).

**Limitations:** streaming jobs only (batch uses Adaptive Batch Scheduler); no partial failover (restarts entire job, not regions — impacts only embarrassingly-parallel jobs' recovery time); scaling events trigger job+task restarts (increase Task attempts).

### Reactive Mode

Special Adaptive Scheduler mode, single job per cluster (enforced by Application Mode). Job always uses all cluster resources: add TM → scale up; remove → scale down; Flink sets parallelism to highest possible. Restarts on rescale from latest completed checkpoint (no savepoint overhead; reprocessed data depends on checkpointing interval; restore time depends on state size). Enables external autoscaling: external service monitors metrics (consumer lag, CPU util, throughput, latency), adds/removes TMs (e.g. K8s replica count, AWS autoscaling group) above/below thresholds; Flink keeps the job running with available resources.

**Getting started (single machine):**

`[code:  ... (412 chars)]`

- `./bin/standalone-job.sh start` — Application Mode
- `-Dscheduler-mode=reactive` — enable
- `-Dexecution.checkpointing.interval="10s"` — checkpointing + restart strategy
- last arg = job main class

Scale up: start another TM (`./bin/taskmanager.sh start`). Scale down: remove a TM.

**Configuration:** `scheduler-mode=reactive`. Operator parallelism is determined by scheduler — not configurable/ignored if set. Only influence via operator `maxParallelism` (bounded by 2^15 = 32768); if unset, default-parallelism rules apply (possibly lower than max). High max parallelism may hurt perf (more internal structures).

Defaults in reactive mode:
- `jobmanager.adaptive-scheduler.resource-wait-timeout` defaults `-1` (wait forever for resources). Set to stop after a time without enough TMs.
- `jobmanager.adaptive-scheduler.resource-stabilization-timeout` defaults `0` (start as soon as sufficient). If TMs connect slowly one-by-one → restart per TM; increase to wait for stabilization.
- `jobmanager.adaptive-scheduler.min-parallelism-increase` — min aggregate parallelism increase before scale-up (default 1; e.g. source=2 + sink=2 → aggregate 4, any increase triggers restart).
- `jobmanager.adaptive-scheduler.scaling-interval.max` — disabled by default; if set, rescale scheduled after this even if min-parallelism-increase unsatisfied.
- `jobmanager.adaptive-scheduler.scaling-interval.min` — min time between 2 scaling ops (default 30s).

**Recommendations:**
- Configure periodic checkpointing for stateful jobs — reactive mode restores from latest completed checkpoint; without periodic checkpointing state is lost. Checkpointing also configures a restart strategy; if no restart strategy configured, reactive mode FAILS the job instead of scaling.
- Downscaling may be slow if TM not properly shut down (SIGKILL instead of SIGTERM) — Flink waits for JM↔TM heartbeat timeout (~50s) before redeploying lower-parallelism. Lower `heartbeat.timeout` if infra permits (keep `heartbeat.interval` < timeout; too-low timeout → failures on network congestion / long GC).

**Limitations:** standalone application deployment only; active resource providers (native Kubernetes, YARN) NOT supported; standalone session clusters NOT supported; single-job applications only. Supported deploy options: Standalone Application Mode, Docker Application Mode, Standalone Kubernetes Application Cluster. Adaptive Scheduler limitations also apply.

### Rescale History (since Flink 2.3, FLIP-495 + FLIP-487)

Before 2.3 users couldn't inspect AdaptiveScheduler rescaling internals (resource changes, parallelism adjustments, internal state-transition times) — needed for tuning latency/stability. Enable by setting a positive integer (number of recent records retained):
- `web.adaptive-scheduler.rescale-history.size: 4` (default 0; ≤0 disables)

**Web UI Rescales page** (same level as Checkpoints, similar style):
- **Overview** — recent rescale records across terminal states + job stats (total rescales since startup, failure/success counts); detailed rescale info.
- **History** — abbreviated recent records (up to configured size). Per-rescale details:
  - Basic info: Rescale UUID (32 hex chars), Attempt ID (rescale attempts on same requirements), Requirements ID, Trigger Cause, Terminal State, Terminated Reason, Start/End Time, Duration (until completion or now if ongoing).
  - Per Job Vertex: ID, Name, Slot Sharing Group ID, Previous/Acquired/Sufficient/Desired Parallelism.
  - Per Slot Sharing Group: Slot Sharing Group ID/Name, Previous/Acquired/Desired/Sufficient Slot, Request/Acquired Profile.
  - Internal Scheduler State History (AdaptiveScheduler states per FLIP-160): State, Enter/Leave Time, Duration, Exception.
- **Summary** — total rescales, failure/success counts, duration stats by status (Min/Max/Avg/P50…).
- **Configuration** — relevant AdaptiveScheduler parameter values for the current job.

See FLIP-495 and FLIP-487 for more details.

---

## Fine-Grained Resource Management (finegrained_resource)

MVP feature, **DataStream API only**. Flink auto-derives sensible default resource requirements; fine-grained lets users specify exact resource profiles per slot-sharing group for tuning.

### Applicable scenarios

- Tasks with significantly different parallelisms.
- Pipeline resource too much to fit a single slot/TM.
- Batch jobs where different-stage tasks need significantly different resources.

### How it works

TM resources split into slots (basic unit of scheduling + resource requirement). With fine-grained, slot requests carry specific resource profiles; Flink cuts an exactly-matched slot out of TM free resources (e.g. request 0.25 Core + 1GB → Slot 1). Previously (coarse-grained) requirements only had slot counts; TM had fixed identical slots. Unspecified-profile requests get an auto profile = TM total resource / `taskmanager.numberOfTaskSlots` (e.g. 1 Core/4GB, 2 slots → 0.5 Core/2GB each). After allocation, remaining free resources can be further partitioned. See Resource Allocation Strategy for details.

### Usage

Requirements are defined on **slot sharing groups** (hint that operators CAN share a slot). To specify:
1. Define the slot sharing group + the operators it contains.
2. Specify the group's resource.

Define group+operators two ways:
- `slotSharingGroup(String name)` — name only, attach to operator.
- `slotSharingGroup(SlotSharingGroup ssg)` — construct instance with name + optional resource profile.

Specify resource profile:
- If set via `slotSharingGroup(SlotSharingGroup ssg)`, profile in the constructor.
- If set via name only, construct `SlotSharingGroup` with same name + profile, register via `StreamExecutionEnvironment#registerSlotSharingGroup(SlotSharingGroup ssg)`.

`[code: Java example final StreamExecutionEnvironment env = ... (595 chars)]`
`[code: Python example env = StreamExecutionEnvironment.get_execution_environment() ... (611 chars)]`

Each slot sharing group attaches to ONE specified resource; conflicts fail compilation.

`SlotSharingGroup` resource components:
- **CPU Cores** — required, positive.
- **Task Heap Memory** — required, positive.
- **Task Off-Heap Memory** — can be 0.
- **Managed Memory** — can be 0.
- **External Resources** — can be empty.

`[code: Java build with specific resource ... (615 chars)]`
`[code: Python build with specific resource ... (604 chars)]`

With a profile you MUST set CPU cores + Task Heap Memory positive; others optional.

### Limitations

- No Elastic Scaling support (only slot requests without specified resource).
- Limited Web UI integration (shows slot number, not details).
- Limited batch integration — requires all edges BLOCKING: set `fine-grained.shuffle-mode.all-blocking=true` (may hurt performance; FLINK-20865).
- Hybrid resource requirements not recommended — specifying only some groups leaves others fulfillable by any-resource slots → inconsistent across executions/failover.
- Slot allocation may be suboptimal — multi-dimensional packing is NP-hard; default strategy may cause resource fragments or allocation failure.

### Notice

- Setting slot sharing group may change performance — putting chain-able operators in different groups breaks operator chains.
- Slot sharing group does NOT restrict scheduling — it's a hint; grouped operators may deploy in separate slots (then slot resources derive from the group requirement).

### Deep dive — resource efficiency

Coarse-grained (identical slots) works well when: all tasks same parallelism (each slot = whole pipeline, roughly equal resources); consumption varies over time (peak shaving / valley filling across tasks). Coarse-grained fails when: tasks have different parallelisms (source/sink/lookup constrained by external partitions/IO → slots with fewer tasks need fewer resources); pipeline too big for one slot/TM (split into multiple SSGs with different requirements); batch jobs execute tasks at different times (instantaneous requirement changes). Identical slots must satisfy the highest requirement → wasteful for others, especially with expensive external resources (GPU). Fine-grained uses slots of different resources to improve utilization.

### Resource Allocation Strategy

TM launches with total resources, no predefined slots. Request arrives → Flink traverses registered TMs, selects first with enough free resources, cuts a new slot. Freed slot returns resources to TM available. If no TM has enough → allocate new TM on Native Kubernetes / YARN (identical TMs per config).

Consequences:
- **Resource fragments** possible (e.g. two 3GB-heap requests with 4GB-heap TM → 2 TMs, 1GB wasted each). Future may allocate heterogeneous TMs.
- Group resource components must be ≤ TM total resources, else job fails with an exception.

---

*Sources: nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/{memory/network_mem_tuning, adaptive_batch, cli, elastic_scaling, finegrained_resource}/ — English pages only. Code blocks condensed to `[code: ...]`, tables to `[table: N rows]`; prose kept near-verbatim.*
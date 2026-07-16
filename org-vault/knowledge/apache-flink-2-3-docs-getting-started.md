---
title: Apache Flink 2.3 docs — Getting Started
tags: [org, flink, flink-2.3, docs, getting-started, reference]
author: adrianb
sessions: []
created: '2026-07-07T19:09:42.770Z'
updated: '2026-07-07T19:09:42.770Z'
importanceScore: 1
---

## Executive Summary

# Apache Flink 2.3 docs — Getting Started

Captured from https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/getting-started/ (5 pages, 2026-07-07). Code blocks are condensed to first-line summaries; prose/headings kept intact.

## First Steps (local_installation)
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/getting-started/local_installation/

Welcome to Apache Flink! Get a Flink cluster running to start exploring Flink's capabilities.

### Prerequisites
Choose one of:
- Docker: No Java needed, includes SQL Client
- Local Installation: Requires Java 11, 17, or 21
- PyFlink: Requires Java and Python 3.9+

### Option A: Docker Installation
Fastest start, no Java required.
- Step 1: Get docker-compose.yml — defines jobmanager (image flink:latest, port 8081), taskmanager (scale 1, taskmanager.numberOfTaskSlots: 2), and sql-client services, all using FLINK_PROPERTIES env to set jobmanager.rpc.address.
- Step 2: Start cluster: `docker compose up -d`
- Step 3: Verify at Flink Web UI http://localhost:8081
- Using SQL Client: `docker compose run sql-client`; exit with `exit;`
- Stopping: `docker compose down`

### Option B: Local Installation
- Step 1: Verify Java (`java -version`) — need Java 11/17/21. Download latest binary release, extract: `tar -xzf flink-*.tgz`
- Step 2: Start cluster: `./bin/start-cluster.sh`
- Step 3: Verify at http://localhost:8081 (one TaskManager with available slots)
- Using SQL Client: `./bin/sql-client.sh`; exit with `exit;`
- Stopping: `./bin/stop-cluster.sh`

### Option C: PyFlink Installation
For Python dev with Table API or DataStream API; runs in local mode, no cluster needed.
- Step 1: Verify Java and Python (`java -version`, Python 3.9+)
- Step 2: `python -m pip install apache-flink==2.3.0` (recommend venv)
- Step 3: Verify: `python -c "import pyflink; print(pyflink.__version__)"`

## Flink SQL Tutorial (quickstart-sql)
Source: .../docs/getting-started/quickstart-sql/

Flink SQL = develop streaming apps with standard SQL, ANSI-SQL 2011 compliant. SQL Client is interactive: submit SQL and see results. Start with `docker compose run sql-client` (Docker) or `./bin/sql-client.sh` (local). Exit with `exit;`.

- Hello World: `SELECT 'Hello World';` — `HELP` lists supported statements; `SHOW FUNCTIONS;` lists built-in functions; `SELECT CURRENT_TIMESTAMP;` prints system time.
- Source Tables: Flink does not manage data at rest; queries operate continuously over external tables (Kafka, databases, filesystems). Tables defined via SQL DDL (create/alter/drop) using connectors and formats. Example: CREATE TABLE with DataGen connector generating 10 rows/sec.
- Continuous Queries: SQL built for continuous pipelines — consumes rows as they arrive, produces updates. A continuous query never terminates, produces a dynamic table. Stateful queries (e.g. per-department counts) maintain state; Flink fault tolerance keeps results correct across failures.
- Sink Tables: Use INSERT INTO to write results to a sink table (e.g. print connector writes to TaskManager logs). `INSERT INTO department_counts SELECT ...`. Submitted as detached background job. In production use JDBC/Kafka/Filesystem connectors.
- Next: SQL Reference, SQL Client, Built-in Functions, Connectors, Dynamic Tables, Time Attributes.

## Table API Tutorial (table_api)
Source: .../docs/getting-started/table_api/

Table API = unified relational API for batch & stream processing, same semantics/results on unbounded streams or bounded batch. Used for analytics, pipelining, ETL. Tutorial builds a spend report aggregating transaction amounts by account and hour: Transactions (generated) → Flink (Table API aggregation) → Console.

Prerequisites: Java 11/17/21 + Maven (Java) or Python 3.9–3.12 + PyFlink (Java). Maven archetype: `mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-archetype-streaming-java ...` creates `spendreport` project with SpendReport.java. For Python: `pip install apache-flink`, edit spend_report.py. (IDE tip: IntelliJ "include dependencies with Provided scope".)

- Execution Environment: TableEnvironment sets job properties, batch vs streaming, creates sources. `EnvironmentSettings.inStreamingMode()` / `.in_streaming_mode()`.
- Creating Tables: TableDescriptor defines tables programmatically (DataGen connector generating accountId 1–5, amount 1–1000, transactionTime with watermark).
- The Query: `tEnv.from("transactions")` then apply Table API operations in a `report` function.
- Implementing the Report: relational apps in pure SQL or Table API (fluent DSL). Use built-in functions floor and sum to round timestamp to hour and aggregate spend per account/hour.
- Testing: Same semantics across batch and streaming — develop/test in batch mode on static data (SpendReportTest), deploy as streaming. Switch via EnvironmentSettings.inBatchMode().
- User Defined Functions: extend with UDFs (e.g. custom floor). Java: implement UDF class; Python: `from pyflink.table.udf import udf`.
- Process Table Functions (Java only): PTFs transform each row with access to state and timers (currently Java-only; Python uses UDTFs).
- Adding Windows: time-based grouping = window. Tumble window has fixed non-overlapping buckets. Window functions are intrinsics (runtime optimizations). 10-second tumbling windows via window function on timestamp column.
- Running: run SpendReport in IDE (Java) or `python spend_report.py` (Python, local mini cluster).

## DataStream API Tutorial (datastream)
Source: .../docs/getting-started/datastream/

DataStream API = lowest-level stream processing API, fine-grained control over state, time, custom processing. Ideal for advanced event-driven apps. Tutorial builds fraud detection: Transactions (source) → Flink (KeyedProcessFunction) → Alerts (sink).

Prerequisites: Java 11/17/21 + Maven, or Python 3.9–3.12 + PyFlink. Maven archetype creates `frauddetection` project with FraudDetectionJob.java + flink-streaming-java + flink-walkthrough-common deps.

- Execution Environment: `StreamExecutionEnvironment.getExecutionEnvironment()`.
- Creating a Source: ingest from Kafka/RabbitMQ/Pulsar. Java: TransactionSource wrapping DataGeneratorSource via `env.fromSource(source, noWatermarks(), name)`. Python: sample data via `from_collection`.
- Partitioning & Detecting: `keyBy` ensures same-key records go to same parallel task (keyed context). `process()` applies the FraudDetector function per partitioned element.
- Outputting Results: sink writes to Kafka/Cassandra/Kinesis. Java: AlertSink logs at INFO. Python: `alerts.print()`.
- Fraud Detector: KeyedProcessFunction with processElement per event. Logic: alert when a small transaction (<1.00) is immediately followed by a large one (>500).
- State: a member-variable flag would not be fault-tolerant and would mix keys across accounts in the same operator instance. Use ValueState (keyed state, scoped to current key, fault-tolerant). Created via ValueStateDescriptor in open(). Methods: update/value/clear; value returns null/None when empty.
- Implementation: check flag state per account; if set and current transaction large → alert; clear flag unconditionally; if current small → set flag.
- Running: run FraudDetectionJob in IDE (Java) or `python fraud_detection.py` (Python mini cluster).

## Flink Operations Playground (flink-operations-playground)
Source: .../docs/getting-started/flink-operations-playground/

Focuses on *operating* Flink (not writing apps) — for platform/DevOps. Docker (20.10+) and docker compose (2.1+) required. Learn to: deploy/monitor via Web UI, observe failure recovery with exactly-once, upgrade jobs via savepoints, rescale, query metrics via REST API, use Flink CLI.

### Anatomy
Long-lived Flink Session Cluster + Kafka Cluster (Zookeeper + broker). Flink Cluster = JobManager (job submission, supervision, resource mgmt) + TaskManagers (worker processes executing Tasks). Playground also has a dedicated `client` container for submission/ops. Job "Click Event Count" consumes ClickEvents from `input` topic (timestamp + page), keyed by page, counted in 15s windows, written to `output`. 6 pages, 1000 events/page per 15s → output 1000 views per page/window.

### Starting
`git clone https://github.com/apache/flink-playgrounds.git`, build image, `docker compose up -d`, `docker compose ps` to verify. Stop with `docker compose down -v`.

### Entering
- WebUI: http://localhost:8081 — one TaskManager, job "Click Event Count". Shows JobGraph, Metrics, Checkpointing stats, TaskManager status.
- Logs: `docker compose logs -f jobmanager` / `-f taskmanager` (mainly checkpoint-completion messages).
- Flink CLI: `docker compose run --no-deps client flink --help`.
- REST API: exposed on localhost:8081 / jobmanager:8081. `curl localhost:8081/jobs`.
- Kafka Topics: inspect records on input/output topics.

### Time to Play (independent tasks, CLI or REST API)
- Listing Running Jobs: `flink list` / `curl localhost:8081/jobs` (JobID assigned at submission).
- Observing Failure & Recovery: tail output topic; `docker compose kill taskmanager` (JobManager cancels + resubmits, tasks SCHEDULED — no slots until new TM). `docker compose up -d taskmanager` → tasks recover from last checkpoint, process Kafka backlog, exactly-once (at-least-once Kafka producer may show some duplicates). Production relies on K8s/YARN to auto-restart.
- Upgrading & Rescaling: stop job with savepoint (graceful stop = consistent snapshot of complete app state). `flink stop <job-id>` (savepoint in /tmp/flink-savepoints-directory/). Restart: `flink run -s <savepoint-path> ...`. Rescale: `flink run -p 3 -s <savepoint-path> ...` then `docker compose scale taskmanager=2` for enough slots.
- Querying Metrics: JobManager exposes metrics via REST at `jobs/<job-id>/metrics?get=...` (e.g. lastCheckpointSize).

### Variants
Job args `--checkpointing` (fault tolerance; without it data is lost on failure) and `--event-time` (event-time semantics; disabled = wall-clock windowing). Optional `--backpressure` operator causes severe backpressure in even-numbered minutes (inspect outputQueueLength, outPoolUsage, WebUI backpressure).

### Next Steps
Deployment Overview, Configuration, HA, Monitoring, Metrics, State & Fault Tolerance, plus the SQL/Table API tutorials.
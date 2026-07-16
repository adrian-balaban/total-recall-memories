---
title: Apache Flink 2.3 docs — Deployment (Kubernetes HA / metric-trace-event reporters / security SSL-Kerberos-delegation tokens / balanced scheduling / external resources / history server)
tags: [org, flink, flink-2.3, docs, deployment, kubernetes-ha, metric-reporters, trace-reporters, event-reporters, security, ssl, kerberos, delegation-tokens, task-scheduling, external-resources, history-server, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:03:02.713Z'
updated: '2026-07-08T05:03:02.713Z'
importanceScore: 1
---

## Executive Summary

## Executive summary

Capture of 10 English Apache Flink 2.3 docs pages (deployment section, indices 266–275) covering high availability on Kubernetes, the three independent reporter subsystems (metrics / traces / events), security (TLS/SSL, Kerberos, delegation tokens), task-quantity-based balanced scheduling, the external-resource framework (GPU etc.), and the History Server. Stored as a verbatim-faithful reference so Flink 2.3 operational/deployment decisions can be made without re-fetching the site.

**WHY this matters:** These are the cross-cutting operational concerns that determine whether a Flink 2.3 cluster survives JM failure, exposes telemetry, authenticates to secure Hadoop/Kafka/ZK services, balances load across TMs, uses accelerators, and lets you inspect completed jobs after cluster teardown. They are config-key heavy and version-specific (2.3 defaults differ — e.g. cipher suites aligned to RFC 9325), so near-verbatim capture preserves exact option names and defaults. Code blocks condensed to `[code: <first-line> ... (N chars)]`, tables to `[table: N rows]`; prose kept verbatim.

Source pages (all under https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/):
1. ha/kubernetes_ha/  2. metric_reporters/  3. trace_reporters/  4. event_reporters/  5. security/security-ssl/  6. security/security-kerberos/  7. security/security-delegation-token/  8. tasks-scheduling/balanced_tasks_scheduling/  9. advanced/external_resources/  10. advanced/historyserver/

---

## Kubernetes HA Services

Flink's Kubernetes HA services use Kubernetes for high availability services. Can only be used when deploying to Kubernetes (standalone Flink on Kubernetes or native Kubernetes integration).

Prerequisites: Kubernetes >= 1.9; service account with permissions to create, edit, delete ConfigMaps.

Config keys required to start an HA cluster:
- high-availability.type (required): must be `kubernetes`.
`[code: high-availability.type: kubernetes ... (34 chars)]`
- high-availability.storageDir (required): JobManager metadata persisted in the file system; only a pointer to this state is stored in Kubernetes.
`[code: high-availability.storageDir: s3://flink/recovery ... (49 chars)]`
The storageDir stores all metadata needed to recover a JobManager failure.
- kubernetes.cluster-id (required): identifies the Flink cluster.
`[code: kubernetes.cluster-id: cluster1337 ... (34 chars)]`

Example config:
`[code: kubernetes.cluster-id: <cluster-id> ... (123 chars)]`

HA data clean up: To keep HA data while restarting the Flink cluster, simply delete the deployment (kubectl delete deployment <cluster-id>). All Flink cluster resources are deleted (JM Deployment, TM pods, services, Flink conf ConfigMap). HA-related ConfigMaps are retained because they do not set the owner reference. When restarting the cluster, all previously running jobs are recovered and restarted from the latest successful checkpoint.

---

## Metric Reporters

Flink allows reporting metrics to external systems. Metrics can be exposed by configuring one or several reporters in the Flink configuration file; reporters are instantiated on each job and task manager at start. All properties configured via `metrics.reporter.<reporter_name>.<property>`.

General parameters (9-row table). All reporter configurations must contain the factory.class property. Some reporters (Scheduled) allow a reporting interval.

Example specifying multiple reporters:
`[code: metrics.reporters: my_jmx_reporter,my_other_reporter ... (599 chars)]`

Important: The jar containing the reporter must be accessible when Flink is started. Reporters are loaded as plugins. All reporters on this page are available by default. Write your own by implementing `org.apache.flink.metrics.reporter.MetricReporter` (+ `Scheduled` for periodic; report() must not block significantly — run async if needed); `MetricReporterFactory` enables plugin loading.

Identifiers vs tags: Identifier-based reporters assemble a flat string with scope + metric name (e.g. job.MyJobName.numRestarts). Tag-based reporters use a logical scope + metric name (job.numRestarts) and report instances as key-value "tags"/"variables" (jobName=MyJobName).

Push vs Pull: Push-based usually implement Scheduled and periodically send summaries. Pull-based are queried from an external system.

Reporters:
- JMX (org.apache.flink.metrics.jmx.JMXReporter) — pull/tags. `port` optional (range like 9250-9260 advisable when multiple instances on one host; actual port shown in logs; if set Flink starts an extra JMX connector; metrics always on default local JMX interface). Metrics identified by a domain (begins org.apache.flink + generalized metric identifier, not affected by scope-formats, constant across jobs, e.g. org.apache.flink.job.task.numBytesOut) and key-property list (values for all variables regardless of scope formats, e.g. host=localhost,job_name=MyJob,task_name=MyTask). Domain identifies metric class; key-property list identifies instance(s).
`[code: metrics.reporter.jmx.factory.class: ... (115 chars)]`
- Graphite (org.apache.flink.metrics.graphite.GraphiteReporter) — push/identifier. Params: host, port, protocol (TCP/UDP).
`[code: metrics.reporter.grph.factory.class: ... (244 chars)]`
- InfluxDB (org.apache.flink.metrics.influxdb.InfluxdbReporter) — push/tags. Params (11-row table). Sends metrics via http with specified retention policy (or server default). All Flink metric variables exported as InfluxDB tags.
`[code: metrics.reporter.influxdb.factory.class: ... (581 chars)]`
- Prometheus (org.apache.flink.metrics.prometheus.PrometheusReporter) — pull/tags. `port` optional default 9249 (range 9250-9260 advisable for multiple instances); `filterLabelValueCharacters` optional — if enabled all chars not matching [a-zA-Z0-9:_] removed (ensure label values meet Prometheus requirements before disabling). Flink metric types → Prometheus types (5-row table). All Flink metric variables exported as labels.
`[code: metrics.reporter.prom.factory.class: ... (98 chars)]`
- PrometheusPushGateway (org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter) — push/tags. Params (10-row table). Pushes to a Pushgateway scraped by Prometheus. Supports HTTP Basic Auth (enabled only when both username and password configured; use HTTPS when auth enabled).
`[code: metrics.reporter.promgateway.factory.class: ... (488 chars)]`
- StatsD (org.apache.flink.metrics.statsd.StatsDReporter) — push/identifier. Params: host, port.
`[code: metrics.reporter.stsd.factory.class: ... (204 chars)]`
- Datadog (org.apache.flink.metrics.datadog.DatadogHttpReporter) — push/tags. Variables (<host>,<job_name>,<tm_id>,<subtask_index>,<task_name>,<operator_name>) sent as tags (host:localhost, job_name:myjobname). For legacy reasons uses both identifier and tags — avoid redundancy with useLogicalIdentifier. Histograms exposed as a series of gauges following Datadog naming (<metric_name>.<aggregation>); min reported by default, sum not available; aggregations not computed for a specific reporting interval. Params: apikey; proxyHost (opt); proxyPort (opt, default 8080); dataCenter (opt EU/US, default US); maxMetricsPerRequest (opt, default 2000); useLogicalIdentifier (opt, default false).
`[code: metrics.reporter.dghttp.factory.class: ... (412 chars)]`
- OpenTelemetry (org.apache.flink.metrics.otel.OpenTelemetryMetricReporterFactory) — params (9-row table). Example configs incl. batching (500 metrics per export request).
`[code: metrics.reporter.otel.factory.class: ... (210 chars)]`
- Slf4j (org.apache.flink.metrics.slf4j.Slf4jReporter) — push/identifier.
`[code: metrics.reporter.slf4j.factory.class: ... (133 chars)]`

---

## Trace Reporters

Flink allows reporting traces to external systems. Traces exposed by configuring one or several reporters in the Flink config; instantiated on each job and task manager at start. All properties via `traces.reporter.<reporter_name>.<property>`. General params (8-row table). All configs must contain factory.class. Example multiple reporters:
`[code: traces.reporters: otel,my_other_otel ... (488 chars)]`

Important: jar accessible at start; reporters loaded as plugins; all on this page available by default. Write your own by implementing `org.apache.flink.traces.reporter.TraceReporter` and `TraceReporterFactory` (methods must not block significantly — run async).

Reporters:
- OpenTelemetry (org.apache.flink.traces.otel.OpenTelemetryTraceReporterFactory) — params (9-row table). Two example configs.
`[code: traces.reporter.otel.factory.class: ... (205 chars)]`
- Slf4j (org.apache.flink.traces.slf4j.Slf4jTraceReporter).
`[code: traces.reporter.slf4j.factory.class: ... (92 chars)]`

---

## Event Reporters

Flink allows reporting events (structured logging) to external systems. Events exposed by configuring one or several reporters; instantiated on each job and task manager at start. All properties via `events.reporter.<reporter_name>.<property>`. General params (8-row table). All configs must contain factory.class. Example multiple reporters:
`[code: events.reporters: otel,my_other_otel ... (488 chars)]`

Important: jar accessible at start; reporters loaded as plugins; all on this page available by default. Write your own by implementing `org.apache.flink.events.reporter.EventReporter` and `EventReporterFactory` (methods must not block significantly — run async).

Reporters:
- OpenTelemetry (org.apache.flink.events.otel.OpenTelemetryEventReporterFactory) — params (9-row table). Two example configs.
`[code: events.reporter.otel.factory.class: ... (205 chars)]`
- Slf4j (org.apache.flink.events.slf4j.Slf4jEventReporter).
`[code: events.reporter.slf4j.factory.class: ... (92 chars)]`

---

## SSL Setup

Enables TLS/SSL authentication and encryption for network communication with and between Flink processes. NOTE: TLS/SSL not enabled by default.

Internal and External Connectivity: Internal = all connections between Flink processes (Flink custom protocols; users never connect directly). External/REST = connections from outside to Flink processes (web UI, REST to start/control jobs, CLI↔JobManager/Dispatcher). Security for internal and external can be enabled/configured separately.

Internal Connectivity includes: control messages (RPC between JM/TM/Dispatcher/ResourceManager); the data plane (TM↔TM data exchange during shuffles/broadcasts/redistribution); the Blob Service (distribution of libraries/artifacts). All internal connections SSL authenticated+encrypted, mutual authentication (mTLS) — both server and client present certificates; certificate acts as shared secret when a dedicated CA exclusively signs an internal cert. Internal cert not needed by any other party — add to container images or attach to YARN deployment.
- Easiest: generate dedicated public/private key pair + self-signed cert; keystore and truststore identical, containing only that key pair/cert.
- With firm-wide Internal CA (cannot self-sign): still have dedicated key pair/cert signed by CA; TrustStore must also contain CA's public cert to accept the deployment's cert during SSL handshake (JDK TrustStore requirement). CRITICAL: specify the deployment certificate fingerprint (security.ssl.internal.cert.fingerprint) when not self-signed, to pin that cert as the only trusted one and prevent the TrustStore from trusting all certs signed by that CA.
- Note: internal connections mutually authenticated with shared certificates → Flink can skip hostname verification (eases container-based setups).

External/REST Connectivity: exposed via HTTP/REST (web UI, CLI). Used for: communication with Dispatcher to submit jobs (session clusters); communication with JobMaster to inspect/modify running job/application. REST endpoints can require SSL; server accepts connections from any client by default (does not authenticate client). Simple mutual auth may be enabled by config, but recommended to deploy a "side car proxy": bind REST endpoint to loopback (or pod-local in K8s) and start a REST proxy that authenticates and forwards (Envoy Proxy or NGINX with MOD_AUTH). Rationale: proxies offer wide auth options / better integration into existing infra.

Configuring SSL (separately for internal/external):
- security.ssl.internal.enabled: Enable SSL for all internal connections.
- security.ssl.rest.enabled: Enable SSL for REST/external connections.
- Note: security.ssl.enabled (backwards compat) enables SSL for both internal and REST.
With security.ssl.internal.enabled=true, can set false to disable SSL per connection type: taskmanager.data.ssl.enabled (TM data), blob.service.ssl.enabled (Blob JM→TM), pekko.ssl.enabled (Pekko-based RPC JM/TM/ResourceManager).

Keystores and Truststores: keystore = public cert (public key) + private key; truststore = trusted certs/authorities. Truststore must trust keystore's cert.
- Internal: mutually authenticated → keystore and truststore typically refer to dedicated cert (shared secret); cert can use wildcard hostnames/addresses; with self-signed, same file can be keystore and truststore.
`[code: security.ssl.internal.keystore: /path/to/file.keystore ... (284 chars)]`
When not self-signed (signed by CA), use certificate pinning:
`[code: security.ssl.internal.cert.fingerprint: 00:00:00:00: ... (99 chars)]`
- REST (external): by default keystore used by server endpoint, truststore used by REST clients (incl. CLI) to accept server's cert. Self-signed REST keystore → truststore must trust that cert directly. CA-signed → roots of hierarchy in trust store. If mutual auth enabled, keystore and truststore used by both server and clients as with internal.
`[code: security.ssl.rest.keystore: /path/to/file.keystore ... (312 chars)]`

Cipher suites: For strong security use modern robust cipher suites; IETF RFC 9325 (supersedes RFC 7525). Recent JDK updates (11.0.30+, 17.0.18+, etc.) disabled older TLS_RSA_* suites lacking forward secrecy. Flink's default security.ssl.algorithms is now:
TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
Customize via security.ssl.algorithms; if unsupported on your setup, Flink processes will not connect to each other.

Complete List of SSL Options: 44-row table.

Creating and Deploying Keystores/Truststores: generated with keytool; need appropriate Java Keystore and Truststore accessible from each node. Standalone: copy to each node or shared mount. Container: add to container images. YARN: cluster deployment phase can auto-distribute. For externally-facing REST endpoint, common name/SANs should match node's hostname and IP.

Example SSL Setup Standalone and Kubernetes:
- Internal Connectivity: keytool -genkeypair to create key pair in keystore; single key/cert used same way by server and client (mutual auth); acts as shared secret, used directly as keystore and truststore.
`[code: $ keytool -genkeypair \ ... (205 chars)]`
`[code: security.ssl.internal.enabled: true ... (369 chars)]`
- REST Endpoint: may receive connections from external processes (incl. curl); CA-signed cert may make sense; but REST doesn't authenticate clients → typically secured via proxy anyway.
  - Simple self-signed: myhost.company.org / ip:10.0.2.15 is the JM node/service.
`[code: $ keytool -genkeypair -alias flink.rest -keystore rest.keystore -dname ... (463 chars)]`
`[code: security.ssl.rest.enabled: true ... (338 chars)]`
  - Self-signed CA: create truststore with self-signed CA, then keystore for REST endpoint signed by that CA (flink.company.org / ip:10.0.2.15 = JM hostname).
`[code: $ keytool -genkeypair -alias ca -keystore ca.keystore -dname "CN=Sampl ... (383 chars)]`
`[code: $ keytool -genkeypair -alias flink.rest -keystore rest.signed.keystore ... (746 chars)]`
`[code: security.ssl.rest.enabled: true ... (341 chars)]`
  - curl tips: convert keystore to PEM via `openssl pkcs12 -passin pass:rest_keystore_password -in rest.keystore ...`; query `curl --cacert rest.pem flink_url`; if mutual SSL `curl --cacert rest.pem --cert rest.pem flink_url`.

Tips for YARN Deployment: internal security same as above; REST endpoint cert valid for all hosts JM may deploy to (wildcard DNS or multiple DNS names); easiest deploy of keystores/truststores via YARN client ship files (-yt): `flink run -m yarn-cluster -yt deploy-keys/ flinkapp.jar`; YARN web dashboard via YARN proxy Tracking URL — configure YARN proxy to accept Flink SSL certs (add custom CA cert into Java's default truststore on YARN Proxy node).

---

## Kerberos Authentication Setup and Configuration

Describes Flink security across deployment mechanisms (Standalone, native K8s, YARN), filesystems, connectors, state backends.

Objective — primary goals of Flink Kerberos security infrastructure: enable secure data access for jobs via connectors (e.g. Kafka); authenticate to ZooKeeper (if SASL); authenticate to Hadoop components (e.g. HDFS, HBase). Streaming jobs run long (days/weeks/months) and must authenticate throughout; Kerberos keytab does not expire in that timeframe (unlike credential cache or Hadoop delegation token). Supported credential forms: Keytab file (preferred); Credential cache (e.g. from kinit); Hadoop delegation tokens (user-provided tokens not renewed, may be overwritten by Flink). All jobs share the credential configured for a given cluster — use a different keytab per job by launching a separate Flink cluster. Numerous Flink clusters may run side-by-side in K8s/YARN.

How Flink Security works: first-/third-party connectors (Kafka, HDFS, Cassandra, Flume, Kinesis etc.) need arbitrary auth (Kerberos, SSL/TLS, user/pass). Flink provides first-class support for Kerberos only. Supported for Kerberos: Kafka (0.9+), HDFS, HBase, ZooKeeper. Can enable Kerberos independently per service/connector (e.g. enable Hadoop security without ZK Kerberos). Shared element is configuration of Kerberos credentials, used explicitly by each component. Internal architecture based on security modules (implementing org.apache.flink.runtime.security.modules.SecurityModule) installed at startup:
- Hadoop Security Module: uses Hadoop UserGroupInformation (UGI) to establish process-wide login user context, used for all Hadoop interactions (HDFS, HBase, YARN). If Hadoop security enabled (core-site.xml), login user has configured Kerberos credential; otherwise conveys only OS-account user identity. Login order of precedence: hadoop.security.authentication=kerberos → if security.kerberos.login.keytab + .principal configured, keytab login; else if security.kerberos.login.use-ticket-cache configured, credential cache login; else OS account identity.
- JAAS Security Module: provides dynamic JAAS config making configured Kerberos credential available to ZK, Kafka, etc. User may also provide static JAAS config file (static entries override dynamic).
- ZooKeeper Security Module: configures process-wide ZK security settings — ZK service name (default zookeeper) and JAAS login context name (default Client).

Deployment Modes:
- Standalone: add security config to Flink config file (all cluster nodes); ensure keytab at security.kerberos.login.keytab path on all nodes; deploy normally.
- Native Kubernetes and YARN: add security config to client config; ensure keytab at security.kerberos.login.keytab on client node; deploy normally. In YARN and native K8s mode the keytab is automatically copied from client to Flink containers. Kerberos config file required (fetched from cluster env or uploaded by Flink — configure security.kerberos.krb5-conf.path and Flink copies it to containers/pods). See YARN security docs.
  - Using user credential cache (kinit): credential cache (commonly FILE form) must be available on all cluster nodes where Kerberos auth performed; generated mainly with kinit; important difference from keytab — keytab can be generated to never expire, credential cache has expiry date (keeping it up-to-date is user responsibility). Steps: add security config to client; login via kinit; optionally make credential cache available on all nodes; deploy normally.

Further Details:
- TGT Renewal: each Kerberos-using component independently responsible for renewing the TGT. All components renew automatically when keytab provided; user's responsibility when credential cache used.
- Using delegation tokens: Flink 1.17 added DT support (experimental). When talking to Hadoop-based services, Flink can obtain delegation tokens so non-local processes can authenticate. Support: HDFS and other Hadoop FS; HBase. For Hadoop FS (HDFS/WebHDFS), Flink obtains tokens for: Hadoop default filesystem; filesystems in security.kerberos.access.hadoopFileSystems; YARN staging directory. HBase token obtained if HBase in app classpath and hbase.security.authentication=kerberos. Flink supports custom DT providers via Java Services (java.util.ServiceLoader) — implementations of org.apache.flink.runtime.security.token.DelegationTokenProvider listed in jar's META-INF/services.

---

## Delegation Tokens

Explains/demystifies delegation tokens (DTs) as used by Flink.

What Are Delegation Tokens and Why Use Them? DTs are authentication tokens used by some services to replace long-lived credentials. Many Hadoop-ecosystem services support DTs with advantages over long-lived credentials:
- No need to distribute long-lived credentials: in a distributed app distributing long-lived credentials is tricky and an attack surface. DTs allow a single place (e.g. the JobManager) to require long-lived credentials and distribute DTs to other parts (e.g. TaskManagers) so they can authenticate to services.
- A single token per service is used for authentication: with Kerberos, each client→server connection requires a KDC trip + service ticket; service tickets balloon (client processes × service processes, e.g. TMs × HDFS DataNodes) — unnecessary KDC load, may hit KDC admin usage limits.
- Delegation tokens are only used for authentication: unlike long-lived credentials, a DT can only authenticate to the specific service it was issued for — cannot create new DTs or DTs for a different service. DTs are not long-lived credentials; used to replace Kerberos or other auth (nothing ties them to Kerberos aside from implementation details).

Lifecycle of Delegation Tokens: DTs are service-specific — no centralized location to create a DT. First step: authenticate to the service (in Hadoop ecosystem, generally Kerberos). Requires long-lived credentials somewhere (user provides, most commonly via kinit → credential cache containing TGT, used to request service tickets). Other ways to obtain TGTs exist but a TGT bootstraps the process. With a TGT, the target service's client library authenticates and requests creation of a DT; the token is sent to other processes and used to authenticate to that service's daemons. First drawback: need service-specific logic to create and use them. Flink implements a (somewhat) pluggable internal DT creation API — new services added by implementing a DelegationTokenProvider called by the delegation token manager. Once created, DT semantics are service-specific but generally follow Kerberos token semantics: "renewable period" (≈ TGT lifetime) = DT validity length before renewal; "max lifetime" (≈ TGT renewable life) = time until DT can be renewed. Once max lifetime reached, a new DT must be created by contacting the service (restarts the process).

Delegation Token Renewal and Renewers (most confusing part; much designed with Hadoop YARN in mind): DTs need periodic renewal until final expiry. Example HDFS default: DTs valid up to 7 days, renewed every 24 hours; if 24h pass without renewal, token unusable; cannot be renewed after 7 days. Who renews? For a long time: YARN. On YARN app submission, DTs submitted with it; YARN distributes to containers (via UGI API conventions) and keeps them renewed while app runs (tokens also used by YARN for log collection/aggregation). Caveats:
- Who renews the tokens? Handled mostly transparently by Hadoop libs for YARN. Some services have a token "renewer" (name of service principal allowed to renew the DT); on YARN that's the YARN service principal — client app must know it. For other resource managers, renewer mostly doesn't matter (no service doing renewal).
- Which tokens are renewed? DTs service-specific, need service-specific libraries for creation+renewal. For YARN to renew, YARN needs: client libs for all services the app uses; info on how to connect; permissions to connect. In reality YARN mostly has access to a single HDFS cluster — that's the extent of its DT renewal. Other tokens sent to YARN are distributed to containers but not renewed (expire before max lifetime unless other code renews). Also not all client libs implement renewal — e.g. HBase token renew() is a no-op; only way to "renew" an HBase token is to create a new one.
- What happens when tokens expire for good? DTs have a maximum life regardless of renewal; after that, need to create new tokens → need ability to connect to the service without a DT (some auth aside from DTs). Important for long-running unsupervised apps.

Delegation Token Renewal (Flink's approach — a compromise targeting the lowest common denominator like HBase that doesn't support actual renewal): Flink DT "renewal" enabled by giving the application long-lived credentials (e.g. keytab). A keytab ≡ Kerberos password in a plain-text file — sensitive (anyone with it can authenticate as that user while credentials valid in KDC). With the keytab, Flink can indefinitely maintain a valid Kerberos TGT. With long-lived credentials available, Flink creates new DTs for configured services as old ones expire — so Flink doesn't renew tokens (per above); it creates new tokens at every renewal interval and distributes them to TMs. Advantage: supports services like HBase AND removes dependency on an external renewal service (like YARN) — Flink's renewal works with non-DT-aware resource managers (e.g. Kubernetes) as long as the app has long-lived credentials.

Delegation Tokens and Proxy Users: "Proxy users" = Hadoop-speak for impersonation (user A impersonates user B). Flink does NOT allow impersonation when submitting applications (Spark supports impersonation but not token renewal; Flink mainly designed for streaming so little gain).

Externally Generated Delegation Tokens: Flink uses UGI API to manage Hadoop credentials → inherits loading DTs automatically from a file. Hadoop classes load the token cache at HADOOP_TOKEN_FILE_LOCATION env var when defined. Mostly used by services that start workloads on behalf of users; regular users rarely use it. Flink itself can obtain DTs — if UGI contains a DT for a service and Flink is configured to obtain tokens for that service, the token is first loaded then overwritten by Flink's loading mechanism.

Limitations of Delegation Token Support:
- Not all DTs expose their renewal period (service config not generally exposed to clients) — some DT providers cannot provide a renewal period, requiring the service's config to be synchronized with another service that does (HDFS generally provides this; good idea for all DT-using services to use the same renewal-period config as HDFS).
- Flink doesn't parse user application code → doesn't know which DTs will be needed; tries to get as many DTs as possible based on config. If an HBase token provider is enabled but the app doesn't use HBase, a DT is still generated — user must explicitly disable the provider.
- Challenging to create DTs "on demand" — Flink obtains/distributes tokens upfront and re-obtains/re-distributes periodically. Advantage: user code need not worry about DTs (Flink handles transparently with proper config).
- External FS plugins authenticating to the same service (e.g. s3-hadoop and s3-presto both auth to S3, different service names but tokens for same service stored at the same place) — with same credentials no issues (tokens overwrite each other single-threaded, single user); with different user credentials the token used for data processing can belong to either user (non-deterministic).

---

## Balanced Tasks Scheduling

Background and principle of task-quantity-based balanced tasks scheduling for streaming jobs.

Background: When parallelism of all vertices within a Flink streaming job is inconsistent, the default deploy strategy sometimes leads some TaskManagers to have more tasks while others have fewer → excessive resource utilization at high-task TMs, becoming a bottleneck. Example: job with JV-A (parallelism 6) and JV-B (parallelism 3), same slot sharing group. Default strategy: TMs with most tasks host 4, lowest only 2 → 4-task TM becomes bottleneck. Flink provides task-quantity-based balanced tasks scheduling — within the job's resource view, aims to make the number of tasks scheduled to each TM as close as possible, improving resource-usage skew. Note: inconsistent parallelism does not imply this strategy must be used (not always the case in practice).

Principle — completes task→TM assignment in two phases: tasks-to-slots assignment; slots-to-TaskManagers assignment.
- tasks-to-slots assignment phase: example job with 5 vertices parallelism 1,4,4,2,3, all default slot sharing group. Strategy: first directly assigns tasks of vertices with highest parallelism to the i-th slot (JV-Bi→sloti, JV-Ci→sloti); next, tasks of sub-maximal-parallelism vertices assigned round-robin across slots within the current slot sharing group until all allocated. Result: range (max−min tasks per slot) = 1 (better than default's range of 3) → more balanced distribution across slots.
- slots-to-TaskManagers assignment phase: example JV-A (6) + JV-B (3) same SSG. After phase 1: Slot0/1/2 each 2 tasks, remaining slots 1 each. Strategy: submits all slot requests, waits until all required slot resources ready; sorts slot requests descending by tasks contained; sequentially assigns each slot request to the TM with the smallest current task loading; continues until all allocated. Final: each TM exactly 3 tasks (task-count diff 0 vs default's diff 2). Use this strategy if seeing the described bottlenecks; do NOT use if not seeing them (may cause performance degradation).

Usage: enable via `taskmanager.load-balance.mode: tasks`. Note: during failover, released resources + processed resource requests + delayed updates in resource view may lead to non-optimally-balanced allocation; improve by appropriately increasing slot.request.max-interval (e.g. +50ms each adjustment) — makes slot requests and available resource view more stable during scheduling, allowing balanced allocation; but extends overall task scheduling phase and raises risk of slot.request.timeout. If non-best balancing persists after sufficiently increasing the value, report at FLINK-38715 (include scheduling configs + observed phenomena). See FLIP-370 for more details.

---

## External Resource Framework

Beyond CPU and memory, workloads may need other resources (e.g. GPUs for deep learning). Flink provides an external resource framework to request various resource types from underlying resource managers (e.g. Kubernetes) and supply info needed to use these resources to operators. Different resource types supported via built-in plugins (currently only GPU) or custom plugins.

What it does (two things): sets corresponding fields of resource requests (for requesting from underlying system) w.r.t. config; provides operators with info needed to use the resources. On K8s/Yarn the framework ensures the allocated pod/container contains the desired external resources (K8s supports GPU/FPGA via Device Plugin since v1.10; Yarn supports GPU/FPGA since 2.10/3.1). In Standalone the user must ensure resources are available. Framework provides info to operators generated by configured external resource drivers.

Enable for your workload: prepare the external resource plugin (put in plugins/ folder); set configurations; get external resource info from RuntimeContext and use in operators.

Prepare plugins: put plugin into plugins/ folder. Apache Flink provides first-party GPU plugin; can implement custom.

Configurations: add resource names for all external resource types to the external resource list (key `external-resources`) with ";" delimiter (e.g. "external-resources: gpu;fpga"). Only <resource_name> defined here takes effect. For each external resource, configure:
- Amount (external.<resource_name>.amount): quantity requested from external system.
- Config key in Yarn (external-resource.<resource_name>.yarn.config-key): optional — if configured, framework adds this key to resource profile of container requests for Yarn, value = external-resource.<resource_name>.amount.
- Config key in Kubernetes (external-resource.<resource_name>.kubernetes.config-key): optional — if configured, framework adds resources.limits.<config-key> and resources.requests.<config-key> to main container spec of TM, value = amount.
- Driver Factory (external-resource.<resource_name>.driver-factory.class): optional — factory class name; if configured, factory instantiates drivers; if not configured the requested resource still exists in TM (if relevant options configured) but operator gets no info from RuntimeContext.
- Driver Parameters (external-resource.<resource_name>.param.<param>): optional — naming pattern for custom config options passed into the driver factory.
Example (two external resources):
`[code: external-resources: gpu;fpga # Define two external resources, "gpu" an ... (828 chars)]`

Use the resources: operators get ExternalResourceInfo set from RuntimeContext (wraps info needed; retrieve via getProperty; available properties + how to access depends on plugin). Get via RuntimeContext or FunctionContext getExternalResourceInfos(String resourceName) — resourceName same as configured in external resource list.
`[code: public class ExternalResourceMapFunction extends RichMapFunction<Strin ... (585 chars)]`
Each ExternalResourceInfo contains one or more properties (keys = resource dimensions); get valid keys via ExternalResourceInfo#getKeys. Note: info returned by RuntimeContext#getExternalResourceInfos is available to all operators.

Implement a plugin for custom resource type: implement org.apache.flink.api.common.externalresource.ExternalResourceDriver; implement ExternalResourceDriverFactory (instantiates driver); add service entry — file META-INF/services/org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory containing factory class name. Example FPGA: implement FPGADriver + FPGADriverFactory.
`[code: public class FPGADriver implements ExternalResourceDriver { ... (710 chars)]`
Create jar including FPGADriver, FPGADriverFactory, META-INF/services/ and all deps; make dir in plugins/ (arbitrary name e.g. "fpga") and put jar there. Note: external resources shared by all operators on same machine; isolation may come in a future release.

Existing supported plugins: GPUs.
- Plugin for GPU resources: first-party; leverages a discovery script to discover GPU device indexes, accessible via property "index"; default discovery script for NVIDIA; custom script supported. Example matrix-vector multiplication provided. Note: all operators get the same set of resource info (same GPU devices accessible to all operators in same TM; no operator-level isolation).
  - Pre-requisites: Standalone — NVIDIA driver installed + GPU accessible on all nodes. Yarn — configure Yarn cluster for GPU scheduling (Hadoop 2.10+/3.1+). Kubernetes — NVIDIA GPU device plugin installed (K8s 1.10+; K8s supports NVIDIA + AMD GPU; Flink provides discovery script only for NVIDIA, custom script for AMD).
  - Enable GPU: configure the GPU resource; get GPU info (index property) in operators.
  - Configurations: external-resources (append resource name e.g. gpu); external-resource.<resource_name>.amount (GPUs per TM); external-resource.<resource_name>.yarn.config-key (Yarn: yarn.io/gpu; Yarn only supports NVIDIA); external-resource.<resource_name>.kubernetes.config-key (K8s: <vendor>.com/gpu; "nvidia" and "amd" supported; AMD needs custom discovery script); external-resource.<resource_name>.driver-factory.class = org.apache.flink.externalresource.gpu.GPUDriverFactory. GPU-specific configs (3-row table). Example:
`[code: external-resources: gpu ... (669 chars)]`
  - Discovery script: GPUDriver leverages a discovery script to discover GPU resources and generate info.
    - Default Script: plugins/external-resource-gpu/nvidia-gpu-discovery.sh; gets indexes of visible GPUs via nvidia-smi; returns required amount (external-resource.<resource_name>.amount) of GPU indexes in a list, exits non-zero if amount cannot be satisfied. For standalone with co-located TMs, supports coordination mode — uses a coordination file to synchronize allocation state, ensuring each GPU used by only one TM process. Args: --enable-coordination-mode (enable; disabled by default); --coordination-file filePath (default /var/tmp/flink-gpu-coordination). Note: coordination mode only ensures a GPU is not shared by multiple TMs of the same Flink cluster — another cluster (different coordination file) or non-Flink app can still use the same GPU.
    - Custom Script: provide for custom requirements (e.g. AMD); ensure path accessible + configured (external-resource.<resource_name>.param.discovery-script.path). Contract: GPUDriver passes amount as first arg, user args (external-resource.<resource_name>.param.discovery-script.args) appended after; script returns list of available GPU indexes split by comma (whitespace-only indexes ignored); script may exit non-zero to signal discovery not properly performed (no gpu info provided to operators).

---

## History Server

Flink has a history server to query statistics of completed jobs and applications after the corresponding Flink cluster has been shut down. Exposes a REST API accepting HTTP requests, responding with JSON.

Overview: HistoryServer queries status/statistics of completed jobs/applications archived by a JobManager. After configuring HistoryServer and JobManager, start/stop via startup script.
`[code: # Start or stop the HistoryServer ... (84 chars)]`
By default binds to localhost, port 8082. Currently only runnable as a standalone process.

Configuration: keys jobmanager.archive.fs.dir and historyserver.archive.fs.refresh-interval need adjustment for archiving and displaying archived jobs/applications.
- JobManager: archiving of completed jobs/applications happens on JM, uploads archived info to a filesystem directory configured via jobmanager.archive.fs.dir.
`[code: # Directory to upload completed job and application information ... (107 chars)]`
  For directory structure details see FLIP-549: Support Application Management.
- HistoryServer: configured to monitor a comma-separated list of directories via historyserver.archive.fs.dir; directories polled regularly for new archives; polling interval via historyserver.archive.fs.refresh-interval.
`[code: # Monitor the following directories for completed jobs and application ... (195 chars)]`
Archives downloaded and cached in local filesystem via historyserver.web.tmpdir. See configuration page for complete list.

Log Integration: Flink does not provide built-in methods for archiving logs of completed jobs. If you have log archiving/browsing services, configure HistoryServer to integrate (historyserver.log.jobmanager.url-pattern and historyserver.log.taskmanager.url-pattern) — link directly from HistoryServer WebUI to logs of relevant JM/TMs.
`[code: # HistoryServer will replace <jobid> with the relevant job id ... (316 chars)]`

Available Requests: all of form http://hostname:8082/jobs (paths listed below). Angle-bracket values are variables (e.g. /jobs/<jobid>/exceptions → /jobs/7684be6004e4e955c2a558a9bc463f65/exceptions). Response format consistent with REST API docs.
Application-related: /applications/overview; /applications/<applicationid>; /applications/<applicationid>/jobmanager/config; /applications/<applicationid>/exceptions.
Job-related: /config; /jobs/overview; /jobs/<jobid>; /jobs/<jobid>/vertices; /jobs/<jobid>/config; /jobs/<jobid>/exceptions; /jobs/<jobid>/accumulators; /jobs/<jobid>/vertices/<vertexid>; /jobs/<jobid>/vertices/<vertexid>/subtasktimes; /jobs/<jobid>/vertices/<vertexid>/taskmanagers; /jobs/<jobid>/vertices/<vertexid>/accumulators; /jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators; /jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>; /jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>; /jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators; /jobs/<jobid>/plan; /jobs/<jobid>/jobmanager/config; /jobs/<jobid>/jobmanager/environment; /jobs/<jobid>/jobmanager/log-url; /jobs/<jobid>/taskmanagers/<taskmanagerid>/log-url.
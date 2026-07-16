---
title: Apache Flink 2.3 docs — Deployment advanced (logging / failure enrichers / job status listener)
tags: [org, flink, flink-2.3, docs, deployment, logging, log4j2, structured-logging, failure-enrichers, job-status-listener, data-lineage, plugins, reference]
author: adrianb
sessions: []
created: '2026-07-08T05:04:04.819Z'
updated: '2026-07-08T05:04:04.819Z'
importanceScore: 1
---

## Executive Summary

## Executive summary

Capture of the final 3 English Apache Flink 2.3 docs deployment pages (indices 276–278, all under /docs/deployment/advanced/): logging configuration, custom failure enrichers, and the job-status-changed listener (data lineage). Stored verbatim-faithful so operational logging setup, failure-tagging, and lineage-export decisions can be made without re-fetching.

**WHY this matters:** These complete the deployment section. Logging covers SLF4J/Log4j2 defaults and the structured-logging MDC fields (flink-job-id) plus Log4j1/logback classpath swaps; failure enrichers and the job-status listener are the two pluggable extension points for runtime failure metadata and data-lineage reporting (Datahub/OpenLineage) — all SPI/plugin-based with explicit factory + META-INF/services registration and config-key enablement. Code condensed to `[code: ...]`, tables to `[table: N rows]`; prose verbatim.

Source pages (https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/advanced/):
1. logging/  2. failure_enrichers/  3. job_status_listener/

---

## How to use logging

All Flink processes create a log text file with messages for various events; logs provide deep insights and detect problems (WARN/ERROR) and aid debugging. Log files accessible via Job-/TaskManager pages of the WebUI; resource provider (e.g. YARN) may provide additional means. Logging uses the SLF4J interface → use any SLF4J-supporting framework without modifying Flink source. By default Log4j 2 is the underlying framework.

Structured logging: Flink adds to MDC of most relevant log messages (experimental): Job ID — key flink-job-id, format string, length 32. Most useful in structured-logging environments for quick log filtering. MDC propagated by slf4j to the logging backend which usually adds it to log records automatically (e.g. log4j2 json layout).
- Log4j 2 JsonTemplateLayout: customizable, efficient, garbage-free JSON generating layout; encodes LogEvents per the provided JSON template. Required jar log4j-layout-template-json bundled in flink-dist. For example templates see Event Templates.
- Log4j 2 PatternLayout: can be configured explicitly — pattern might look like `[%-32X{flink-job-id}] %c{0} %m%n`.

Configuring Log4j 2: controlled using a mixture of property files and configuration.
- Log4j 2 property files (shipped in conf, used automatically if Log4j2 enabled): log4j-cli.properties (CLI e.g. flink run, sql-client); log4j-session.properties (CLI when starting K8s/Yarn session cluster — kubernetes-session.sh/yarn-session.sh); log4j-console.properties (Job-/TaskManagers if run in foreground e.g. Kubernetes); log4j.properties (Job-/TaskManagers by default). Log4j periodically scans this file for changes; by default every 30 seconds, controlled by monitorInterval in the Log4j properties files.
- Log4j 2 configuration: logging-related options (4-row table).
- Compatibility with Log4j 1: Flink ships with the Log4j API bridge, allowing existing apps working against Log4j1 classes to continue. For custom Log4j1 properties files or code relying on Log4j1, see official Log4j compatibility/migration guides.

Configuring Log4j1: ensure org.apache.logging.log4j:log4j-core, log4j-slf4j-impl and log4j-1.2-api are NOT on classpath; log4j:log4j, org.slf4j:slf4j-log4j12, org.apache.logging.log4j:log4j-to-slf4j and log4j-api ARE on classpath. IDE: replace such deps in pom + add exclusions on transitively-dependent deps. Flink distributions: remove log4j-core, log4j-slf4j-impl and log4j-1.2-api jars from lib; add log4j, slf4j-log4j12 and log4j-to-slf4j jars to lib; replace all log4j properties files in conf with Log4j1-compliant versions.

Configuring logback: ensure org.apache.logging.log4j:log4j-slf4j-impl NOT on classpath; ch.qos.logback:logback-core and logback-classic ARE on classpath. IDE: replace deps in pom + exclusions. Flink distributions: remove log4j-slf4j-impl jar from lib; add logback-core and logback-classic jars to lib. Shipped logback config files in conf (used automatically if logback enabled): logback-session.properties (CLI starting K8s/Yarn session cluster); logback-console.properties (Job-/TaskManagers in foreground e.g. Kubernetes); logback.xml (CLI and Job-/TaskManagers by default). Note: Logback 1.3+ requires SLF4J 2, currently not supported.

Best practices for developers: create an SLF4J logger via org.slf4j.LoggerFactory#getLogger with your class; store in a private static final field.
`[code: import org.slf4j.Logger; ... (229 chars)]`
Use SLF4J placeholder mechanism — avoids unnecessary string constructions when level set so high the message wouldn't log. Syntax:
`[code: LOG.info("This message contains {} placeholders. {}", 2, "Yippie"); ... (67 chars)]`
Placeholders can be used with exceptions to be logged:
`[code: catch(Exception exception){ ... (80 chars)]`

---

## Custom failure enrichers

Flink provides a pluggable interface to register custom logic and enrich failures with extra metadata labels (string key-value pairs). Enables users to implement failure-enrichment plugins to categorize job failures, expose custom metrics, or call external notification systems. FailureEnrichers triggered every time an exception is reported at runtime by the JobManager; each may asynchronously return labels associated with the failure, exposed via the JobManager REST API (e.g. a 'type:System' label implying the failure is categorized as a system error).

Implement a plugin for your custom enricher: implement FailureEnricher; implement FailureEnricherFactory; add service entry — file META-INF/services/org.apache.flink.core.failure.FailureEnricherFactory containing the factory class name. Create a jar including FailureEnricher, FailureEnricherFactory, META-INF/services/ and all external deps; make a directory in plugins/ (arbitrary name e.g. "failure-enrichment") and put the jar there. Note: every FailureEnricher should define a set of output keys that may be associated with values; this set must be unique otherwise all enrichers with overlapping keys will be ignored.

FailureEnricherFactory example:
`[code: package org.apache.flink.test.plugin.jar.failure; ... (254 chars)]`
FailureEnricher example:
`[code: package org.apache.flink.test.plugin.jar.failure; ... (579 chars)]`

Configuration: JobManager loads FailureEnricher plugins at startup. To ensure loading, all class names must be defined in jobmanager.failure-enrichers. If empty, NO enrichers started. Example:
`[code:     jobmanager.failure-enrichers = org.apache.flink.test.plugin.jar.fa ... (90 chars)]`

Validation: check JobManager logs for:
`[code:     Found failure enricher org.apache.flink.test.plugin.jar.failure.Cu ... (221 chars)]`
Query JobManager REST API looking for the failureLabels field:
`[code:     "failureLabels": { ... (61 chars)]`

---

## Job status changed listener

Flink provides a pluggable interface to register custom logic for handling job status changes in which lineage info about source/sink is provided. Enables users to implement a Flink lineage reporter to send lineage info to third-party data lineage systems (e.g. Datahub and OpenLineage). Listeners triggered every time a status change happens for the application; data lineage info included in the JobCreatedEvent.

Implement a plugin: implement JobStatusChangedListener; implement JobStatusChangedListenerFactory; add service entry — file META-INF/services/org.apache.flink.core.execution.JobStatusChangedListenerFactory containing the factory class name. Create a jar including JobStatusChangedListener, JobStatusChangedListenerFactory, META-INF/services/ and all external deps; make a directory in plugins/ (arbitrary name e.g. "job-status-changed-listener") and put the jar there.

JobStatusChangedListenerFactory example:
`[code: package org.apache.flink.test.execution; ... (300 chars)]`
JobStatusChangedListener example:
`[code: package org.apache.flink.test.execution; ... (250 chars)]`

Configuration: Flink components load JobStatusChangedListener plugins at startup. To ensure loading, all class names must be defined in execution.job-status-changed-listeners. If empty, NO enrichers started. Example:
`[code:     execution.job-status-changed-listeners = org.apache.flink.test.exe ... (115 chars)]`
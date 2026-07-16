---
title: 'Apache Flink 2.3 docs — Dev Configuration (Overview, Maven, Gradle, Connectors, Testing, Advanced)'
tags: [org, flink, flink-2.3, docs, dev, configuration, maven, gradle, connectors, packaging, fat-jar, scala, hadoop, table-planner, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:31:13.407Z'
updated: '2026-07-08T04:31:13.407Z'
importanceScore: 1
---

## Executive Summary

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/configuration/overview/
# Project Configuration
Every Flink application depends on Flink libraries: minimally the Flink APIs, plus connector libraries (Kafka, Cassandra) and 3rd-party deps for custom functions.
## Getting started
- Maven: archetype:generate (interactive groupId/artifactId/package) or quickstart script `curl https://flink.apache.org/q/quickstart.sh | bash -s 2.3.0`.
- Gradle: empty project (create src/main/java, src/main/resources manually) via build.gradle + settings.gradle, or quickstart `bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- 2.3.0`.
## Which dependencies
- Flink APIs (DataStream API and Table API & SQL — usable separately or mixed; include in build script).
- Connectors and formats (integrate with external systems).
- Testing utilities.
- + 3rd-party deps for custom functions.
## Running and packaging
Run by executing main class → need flink-clients in classpath. Table API programs also need flink-table-runtime and flink-table-planner-loader. Rule of thumb: package application code + all required deps (connectors, formats, 3rd-party) into ONE fat/uber JAR. EXCEPTION: Java APIs and aforementioned runtime modules are provided by Flink itself — do NOT include in job uber JAR. Job JAR submitable to running cluster or added to Flink application container image.

Source: .../dev/configuration/maven/
# How to use Maven
Requirements: Maven 3.8.6, Java 11. Import into IDE: IntelliJ out-of-the-box; Eclipse via m2e plugin. Notes: default JVM heap too small for Flink → increase (-Xmx800m in Eclipse Run Configurations VM args; IntelliJ Help|Edit Custom VM Options). IntelliJ: tick "Include dependencies with Provided scope" in run config (or workaround: create a test calling main()).
## Building
`mvn clean package` → target/<artifact-id>-<version>.jar. If different main class than DataStreamJob, change mainClass in pom.xml.
## Adding dependencies
Add in pom.xml dependencies block (e.g. Kafka connector); `mvn install`. Templates auto-include app deps into JAR on `mvn clean package`; otherwise add Maven Shade Plugin.
IMPORTANT: core API deps scope = provided (needed to compile, NOT packaged into app JAR). Not provided → JAR excessively large (contains Flink core) OR core deps clash with your own versions (normally avoided via inverted classloading). To correctly package deps into app JAR, Flink API deps set to compile scope.
## Packaging
- Only Flink deps, no 3rd-party (e.g. filesystem connector + JSON format) → no uber/fat JAR or shading needed.
- External deps not in Flink distribution → add to distribution classpath OR shade into uber/fat JAR. Submit: `bin/flink run -c org.example.MyJob myFatJar.jar`.
## Shade plugin template
Maven shade plugin includes by default all deps in "runtime" and "compile" scope.

Source: .../dev/configuration/gradle/
# How to use Gradle
Requirements: Gradle 7.x, Java 8 (deprecated) or Java 11. Import: IntelliJ via Gradle plugin; Eclipse via Buildship (specify Gradle >= 3.0 — shadow plugin requires it). Same JVM heap / IntelliJ "Provided scope" notes as Maven.
## Building
`gradle clean shadowJar` → build/libs/<project-name>-<version>-all.jar. Different main class than StreamingJob → change mainClassName in build.gradle.
## Adding dependencies
dependencies block in build.gradle (e.g. Kafka connector). Same provided-scope rule: core deps = provided (compile against, not packaged); not provided → large JAR or version clashes (inverted classloading). To package deps into app JAR → compile scope.
## Packaging
- Only Flink deps, no 3rd-party → no uber/fat JAR; `gradle clean installDist` (or `./gradlew clean installDist`).
- External deps → add to classpath OR shade into uber/fat JAR; `gradle clean installShadowDist` → single fat JAR in /build/install/yourProject/lib (or `./gradlew clean installShadowDist`). Submit: `bin/flink run -c org.example.MyJob myFatJar.jar`.

Source: .../dev/configuration/connector/
# Connectors and Formats
Flink apps read/write external systems via connectors; multiple formats encode/decode data. Overview available for DataStream and Table API/SQL.
## Available artifacts (two per connector on Maven Central)
- flink-connector-<NAME>: thin JAR, connector code only, excludes 3rd-party deps.
- flink-sql-connector-<NAME>: uber JAR, ready to use with all connector 3rd-party deps.
Same for formats. Some connectors may lack flink-sql-connector artifact (no 3rd-party deps needed). Uber/fat JARs mainly for SQL client but usable in any DataStream/Table app.
## Using artifacts (three options)
- Shade thin JAR + transitive deps in job JAR.
- Shade uber JAR in job JAR.
- Copy uber JAR directly into /lib folder of Flink distribution.
Shading → more control over dependency version. Shading thin JAR → even more control over transitive deps (change versions without changing connector version, binary compatibility permitting). Embedding uber JAR in /lib → control connector versions for all jobs in one place.

Source: .../dev/configuration/testing/
# Dependencies for Testing
## DataStream API Testing
Add flink-test-utils (Maven dependency / Gradle testCompile "org.apache.flink:flink-test-utils:2.3.0"). Provides MiniCluster — lightweight configurable Flink cluster runnable in a JUnit test that directly executes jobs.
## Table API Testing
Add flink-table-test-utils (in addition to flink-test-utils) for local IDE testing of Table API & SQL. Automatically brings in query planner + runtime (required to plan + execute queries). Note: flink-table-test-utils introduced in Flink 1.15, considered experimental.

Source: .../dev/configuration/advanced/
# Advanced Configuration Topics
## Anatomy of the Flink distribution
Core classes/deps (coordination, networking, checkpointing, failover, APIs, operators/windowing, resource management) packaged in flink-dist.jar in /lib (part of basic Flink container images) — like Java's core library. Core deps kept small, contain NO connectors or libraries (CEP, SQL, ML) to avoid excessive default classpath. /lib also contains commonly-used modules (Table execution modules + set of connectors/formats) loaded by default (removable by deleting from /lib). /opt contains optional deps (enable by moving JARs to /lib). See Classloading in Flink.
## Scala Versions
Different Scala versions NOT binary compatible. Flink deps (transitively) depending on Scala suffixed with Scala version (e.g. flink-table-api-scala-bridge_2.12). Java APIs only → any Scala version. Scala APIs → pick matching Scala version. Scala versions after 2.12.8 NOT binary compatible with previous 2.12.x → Flink can't upgrade 2.12.x builds beyond 2.12.8. Build locally for later versions with -Djapicmp.skip (skip binary compat checks).
## Anatomy of Table Dependencies
Flink distribution /lib contains by default JARs to execute Flink SQL Jobs:
- flink-table-api-java-uber-2.3.0.jar (all Java APIs)
- flink-table-runtime-2.3.0.jar (table runtime)
- flink-table-planner-loader-2.3.0.jar (query planner)
Previously all in flink-table.jar; since Flink 1.15 split into three → allows swapping flink-table-planner-loader with flink-table-planner_2.12. Table Scala API artifacts NOT included by default (download/include in /lib manually — recommended — or package in uber/fat JAR).
### Table Planner and Table Planner Loader (since Flink 1.15, two planners)
- flink-table-planner_2.12-2.3.0.jar in /opt (query planner).
- flink-table-planner-loader-2.3.0.jar loaded by default in /lib (query planner hidden behind isolated classpath — cannot address io.apache.flink.table.planner directly).
Same code, packaged differently. First: must use same Scala version. Second: no Scala considerations (hidden). Default = loader. Swap to access planner internals (copy flink-table-planner_2.12.jar to /lib) → constrained to distribution's Scala version. WARNING: two planners CANNOT co-exist in classpath — loading both in /lib fails Table Jobs. Future: flink-table-planner_2.12 artifact will stop shipping; migrate jobs/connectors/formats to API modules (no planner internals).
## Hadoop Dependencies
General rule: do NOT add Hadoop deps directly to application. Use Flink with Hadoop → Flink setup includes Hadoop deps (Hadoop = dependency of Flink system, not user code). Flink uses HADOOP_CLASSPATH env var: `export HADOOP_CLASSPATH=\`hadoop classpath\``. Two reasons:
- Some Hadoop interactions happen in Flink core before user app starts (HDFS for checkpoints, Kerberos auth, YARN deploy).
- Inverted classloading hides transitive deps from core (incl. Hadoop's) → apps use different versions without conflicts.
Need Hadoop deps during IDE dev/testing (e.g. HDFS access) → configure like dependency scope (test or provided).
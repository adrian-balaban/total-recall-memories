---
title: IBM mainframe (z/OS) data export options for cross-platform migration — architect's options with pros/cons (2026)
tags: [org, mainframe, ibm-zos, db2, vsam, ims, cdb, gdg, cics, iidr, q-replication, cdc, db2-unload, dsntiaul, dsn1copy, idcams, repro, dfsurgu0, datastage, kafka, ibm-mq, aws-m2, aws-transform, blu-age, rocket, bmc-ami, model9, precisely-connect, azure, gcp, ebcdic, comp-3, packed-decimal, zoned-decimal, copybook, cobrix, jrecord, legstar, ztract, reconciliation, asntdiff, data-migration, '2026']
author: adrianb
sessions: []
created: '2026-07-29T15:10:14.370Z'
updated: '2026-07-29T15:10:14.370Z'
importanceScore: 0.9
---

## Executive Summary

## Prompt used
as an IT Architect investigate from net all about IBM mainframes export data options for migrating an IBM application application data to another platform

## Executive Summary

Target-agnostic architect's guide to **exporting data off an IBM mainframe (z/OS) for migration to another platform**. Complements the narrower [[org/architecture/mainframe-ibm-to-vault-core-json-migration-2026-cu-reconciliere]] (which targets VaultCore/JSON + banking reconciliation). Synthesized from a deep-research fan-out (5 search angles → ~15 fetched primary sources → adversarial verification, stopped early by the user; claims below come from IBM Db2 z/OS 12.0.0 & IMS 15.5.0 official docs, AWS Prescriptive Guidance, BMC docs, and the Cobrix/JRecord/Legstar/ztract repos).

**The architect's core decision is two orthogonal axes:**
1. **Extract mechanism** — native batch unload vs CDC replication vs off-host pull. Pick by latency tolerance, MIPS budget, and cutover style (big-bang vs phased).
2. **Format/encoding bridge** — EBCDIC/COMP/COMP-3/zoned-decimal → ASCII/JSON/Parquet/Avro. This is where migration corruption lives; it is *separate* from extraction and must be planned explicitly (the unload tools are format-preserving, not format-converting).

**Bottom line for an architect:** No single tool covers all stores. A real strategy is a portfolio: DB2 UNLOAD or Q Replication for Db2; IDCAMS REPRO for VSAM; DFSURGU0 for IMS; a CDC layer (Precisely Connect / IBM IIDR-Q Replication) for near-zero-downtime; and a copybook-parsing layer (Cobrix for Spark/Parquet, JRecord/Legstar for Java/JSON/Avro) for the EBCDIC→open-format bridge. Reconciliation (asntdiff / line-by-line historical replay) is a hard requirement, not optional. The biggest 2026 constraint: **AWS Mainframe Modernization Managed Runtime (M2) closed to new customers 2025-11-07** → new AWS engagements route to AWS Transform (formerly Blu Age) self-managed refactoring or Rocket replatforming.

---

## 1. Native z/OS data stores that must be exported

| Store | Role | Native extraction primitive |
|---|---|---|
| **Db2 for z/OS** | Relational (the workhorse) | UNLOAD utility, DSNTIAUL, DSN1COPY, Q Replication CDC |
| **VSAM** (KSDS/ESDS/RRDS) | Indexed/entry-sequenced/relative-record files; heavy CICS use | IDCAMS REPRO |
| **IMS DB/DC** (incl. HALDB) | Hierarchical DB | DFSURGU0 (HD Reorg Unload) |
| **Sequential / GDG** | Flat-file batches, generation data groups | DFSMS/DFP utilities, FTP/SFTP/Zowe pull |
| **CICS VSAM files** | Online transaction files (VSAM underneath) | Treat as VSAM (REPRO) + CICS-aware quiesce |

## 2. Export / extract tooling per store

### 2a. Db2 for z/OS
- **UNLOAD utility** (native, IBM primary): batch-extract to BSAM sequential data sets in external LOAD-compatible formats; non-destructive (logical copy, not image copy); parallel per-partition (except PBG table spaces); can read from image-copy data sets. **CCSID gotcha:** an ASCII/Unicode column cannot be unloaded as Unicode in an EBCDIC table (CCSID 1200/1208).
- **DSNTIAUL** (IBM sample program): native unload alternative to UNLOAD; LOAD-utility-compatible output; **32 KB LOB column limit; max 100 output data sets;** EBCDIC/Unicode mixed-encoding gotcha.
- **DSN1COPY** (stand-alone): page-level *physical* copy of Db2 VSAM data sets (not a logical row unload). OBIDXLAT cross-subsystem OBID translation (≤10,000 OBIDs, SYSXLAT DD map). RESET mandatory when target has a different recovery log (else abend 00C200C1 / down-level 00C2010D). PRINT can render EBCDIC/ASCII/UNICODE. **2026 gotcha:** compressed LOBs copied to a target without zEDC hardware → SQLCODE -904.
- **IBM InfoSphere Data Replication / Q Replication (IIDR)** — CDC: queue/MQ-based, near real-time. Needs hybrid batch fallback for tables without PKs. Three-phase "switching day" cutover with **asntdiff** reconciliation + optional reverse replication for rollback. **Explicit constraints:** generated-always columns, MQTs, triggers, sequences cannot be replicated — must be handled manually at cutover.

### 2b. VSAM (KSDS/ESDS/RRDS) & CICS VSAM
- **IDCAMS REPRO** (native): unloads VSAM to sequential (PS) data sets; key-range/RBA/RRN partial-extract controls. **Format-preserving only** — no EBCDIC/COMP/COMP-3 conversion, no JSON/Parquet; must pair with a copybook-parsing layer. **KSDS reload requires sorted ascending-key input** (real prep constraint when reshaping for a target).

### 2c. IMS DB/DC
- **DFSURGU0** (HD Reorganization Unload, IBM IMS 15.5.0): native IMS unload. `MIGRATE=YES` unloads non-HALDB DBs for migration (needs DD cards for secondary indexes); `MIGRATX=YES` produces secondary-index unload files in one pass for reload via DFSURGL0; `KEYRANGE` enables parallel unload jobs for migration performance. Targets HALDB processing or non-HALDB→HALDB conversion.

### 2d. Sequential / GDG & off-host pull
- **DFSMS/DFP** utilities for native dataset handling; **FTP/SFTP/Zowe pull + decode off-host** = a zero-MIPS extraction pattern (decode EBCDIC on a laptop/server instead of burning mainframe CPU) — relevant when MIPS cost is the constraint.

### 2e. Messaging/streaming connectors
- **Apache Kafka** and **IBM MQ** explicitly named by AWS Prescriptive Guidance for async, decoupled, scalable mainframe→cloud transfer (reliability/queuing/fault-tolerance built in). *Caveat (verification):* one claim that MQ is "recommended" was **refuted as overreach** — the source says "suitable for scenarios that require," and MQ is sometimes the legacy system being retired in phased migrations. Treat MQ/Kafka as *applicable*, not universally *recommended*.

## 3. Modern migration approaches (vendor landscape)

| Approach / platform | Pros | Cons |
|---|---|---|
| **AWS Mainframe Modernization — replatform (Rocket Enterprise Server, formerly Micro Focus)** | Recompile COBOL/PL/I with minimal code changes; can **retain Db2 for z/OS in original format during a phased migration**; fastest path to off-mainframe compute. | **M2 Managed Runtime closed to new customers 2025-11-07** → must self-manage on EC2/ECS; still EBCDIC/ASCII decision upfront. |
| **AWS — automated refactoring (AWS Transform, formerly Blu Age)** | Convert COBOL/PL-I to Java cloud-native; modern target architecture. | High transformation effort/risk; long project; hidden business rules surface late. |
| **AWS M2 Managed Runtime (Micro Focus)** | Managed replatform runtime. | **Closed to new customers (Nov 2025)** — the defining 2026 constraint. |
| **BMC AMI Cloud Data (formerly Model9 Manager)** | zIIP-engine-backed direct-to-S3 backup/archive/export; VTL alternative; M9CLI allows batch/JCL extraction without the mgmt server; MIPS reduction via zIIP. | **z/OS dataset/USS-file scope only — no DBMS-aware/CDC extraction; does NOT convert to open formats (JSON/CSV/Parquet)** — that needs the separate **BMC AMI Cloud Analytics (formerly Model9 Gravity)** product; operational metadata limited to Docker-based Postgres on EC2 (no RDS). |
| **BMC AMI Cloud Analytics (formerly Model9 Gravity)** | Transforms Db2 image copies, VSAM, sequential/partitioned datasets into **JSON/CSV/TXT** via Docker REST API — the open-format bridge for BMC. | Separate product/license; not CDC (batch/image-copy driven). |
| **Precisely Connect** | Explicit CDC for **Db2 z/OS, IMS, and VSAM → S3**; two-component (mainframe Capture/Publisher + AWS AMI Apply Engine); optional Iceberg/S3 Tables via EMR for Athena; strongly advocates CDC over disruptive bulk extracts. | **Omits CICS, sequential datasets, and GDG as named sources**; requires Direct Connect/VPN; vendor/partner-attested (AWS+Precisely blog). |
| **Rocket Software (MFA + DFCONV)** | MFA for dataset pull/push; **DFCONV for EBCDIC/ASCII conversion**; replatform best-practices incl. go/no-go validation gates & reconciliation. | Gotchas: VB file format loss, hex-literal encoding mismatches; replatform-only (not refactor). |
| **Microsoft Azure mainframe migration** | Hyperscaler alternative (named in landscape comparisons). | Lighter 2026 primary-source coverage in this research; verify current tooling independently. |
| **Google Cloud mainframe modernization** | Hyperscaler alternative. | Same — lighter primary coverage here. |
| **OpenLegacy** | API-first legacy exposure. | Not deeply sourced in this run — verify. |

## 4. Data-format conversion (EBCDIC/COMP/COMP-3/zoned-decimal → ASCII/JSON/Parquet/Avro)

This is the highest-defect-risk layer. Naive EBCDIC→ASCII translation corrupts: **BINARY (COMP/COMP-4), packed-decimal (COMP-3), and zoned-decimal sign-overpunch** (translation tables don't handle sign overpunch). Collating-sequence differences affect VSAM keys, SORTs, SQL cursors. AWS source: **staying in EBCDIC can cut data-migration time up to 50%** (skip the conversion pass) — viable when the target is another mainframe-aware runtime (Rocket/Micro Focus), not when target is open/cloud-native.

Copybook-parsing tools (the bridge):
| Tool | Stack / output | Pros | Cons |
|---|---|---|---|
| **Cobrix** (AbsaOSS) | Apache Spark data source; outputs **Parquet/CSV/JSON** | Battle-tested; handles COMP/COMP-3 packed/binary, EBCDIC default + multi-codepage, REDEFINES, OCCURS DEPENDING ON, RDW/BDW; standalone copybook parser (non-Spark usable); record formats F/V/VB/FB/D + hierarchical multisegment (IMS-friendly); Maven 2.10.7. | No Avro; Spark-oriented (standalone mode needs care). |
| **JRecord** | Java/JVM standalone; **CobolToJson** utility | Handles EBCDIC + COMP/COMP-3/zoned-decimal; direct JSON output; lightweight standalone. | **LGPL-3.0**; low activity (55 stars, v0.93.4). |
| **Legstar v3** | Java; bidirectional COBOL↔Java/**XML/JSON**; Apache-2.0; CongoCC parser, no runtime deps | Clean modern rewrite of legstar-core2 (Antlr3); no runtime deps. | README does **not** explicitly document EBCDIC/COMP/COMP-3/zoned-decimal handling — verify the underlying layer before relying on it. |
| **legstar.avro** | Java + Hadoop MapReduce; COBOL copybook → **Avro** schemas/readers | The Avro path in this family. | Low activity (55 commits, 0 PRs); **AGPL-3.0** (may constrain commercial pipelines); EBCDIC/COMP-3 lives in legstar-core2, not this repo. |
| **ztract** | Python CLI → Parquet/JSON/cloud | Demonstrates **zero-MIPS off-host extraction** (FTP/SFTP/Zowe pull, decode on laptop); confirms Cobrix standalone path. | **Pre-1.0, 0 adoption (0.1.0.dev1, 0 stars)** — reference implementation / proof-of-pattern only, not production-grade; VSAM/GDG not named (sequential formats only). |

## 5. CDC vs batch, big-bang vs phased, reconciliation

**CDC vs batch (AWS Prescriptive Guidance, primary):**
- **CDC** — minimizes overhead & latency for near-real-time; but adds mainframe CPU (MIPS) cost + tooling complexity; reliability depends on the replication tool. Blind spots: DDL changes, some utilities, reorg rematerialization, downtime gaps.
- **Batch** — predictable, simpler; high-latency, consistency-risk across stores during the extract window.
- **Messaging (Kafka/IBM MQ)** — async/decoupled/scalable; adds infra complexity + dependency on messaging-platform reliability.

**Cutover styles:**
- **Big-bang** — fix-on-fail; highest risk, used when phased isn't feasible.
- **Phased / incremental (recommended)** — Q Replication "switching day" three-phase model; reverse replication for rollback; co-existence period.

**Reconciliation / parity validation — hard requirement:**
- **asntdiff** (IBM Q Replication) for row-level DB2 parity.
- **Automated line-by-line output comparison over 6–12 months of historical job runs** (GoCloud/AWS partner method) for batch parity.
- Reconciliation is what makes a migration *verifiable*; without it you ship silent defects (the documented COMP-3 rounding defects in naive refactors surface only under reconciliation).

## 6. 2026-era gotchas & constraints

1. **AWS M2 Managed Runtime closed to new customers (2025-11-07)** → route to AWS Transform (Blu Age) self-managed or Rocket replatform. *The* defining 2026 constraint for AWS-bound projects.
2. **COMP-3 packed-decimal rounding** is a documented source of production defects in naive refactors — reconcile early.
3. **CCSID/EBCDIC-Unicode mixing** in Db2 UNLOAD/DSNTIAUL (CCSID 1200/1208) — plan encoding per-column.
4. **zEDC hardware dependency**: compressed LOBs copied to a non-zEDC target → SQLCODE -904 (DSN1COPY gotcha).
5. **BMC AMI Cloud Data ≠ format converter** — needs the separate Analytics product; common scoping mistake.
6. **Precisely Connect omits CICS, sequential, GDG** as named CDC sources — gap if those stores dominate.
7. **CDC blind spots**: DDL, some utilities, reorg rematerialization, generated-always columns, MQTs, triggers, sequences (Q Replication) — must be handled manually at cutover.
8. **VSAM KSDS reload needs sorted ascending-key input** — prep constraint.
9. **VB file format loss & hex-literal encoding mismatches** in Rocket DFCONV replatform transfers.
10. **Unverified "recommended" framing for IBM MQ/Kafka** — they are *applicable* async transports, not universally recommended (one claim refuted on this).
11. **Sign-overpunch on zoned-decimals** not handled by EBCDIC→ASCII translation tables — needs a copybook-aware parser.
12. **Inventory undercounting & silent downstream failures** — common pre-migration analysis failures (IN-COM DATA Systems).
13. **Azure/GCP/OpenLegacy** had lighter 2026 primary-source coverage in this run — verify current tooling independently before committing.
14. **AGPL/LGPL licensing** of some copybook parsers (legstar.avro AGPL-3.0, JRecord LGPL-3.0) may constrain commercial pipelines — prefer Apache-2.0 (Cobrix, Legstar v3) where license matters.
15. **Verification caveat:** the deep-research adversarial-verification phase was stopped early by the user; most claims above passed verification (1 refuted: MQ "recommended" overreach), but a few sources are vendor blogs (GoCloud, IN-COM, softwaremodernizationservices) — treat their quantified stats (e.g., "67% failure rate") as illustrative, not independently validated.

## Sources (primary unless noted)
- IBM Db2 for z/OS 12.0.0 — UNLOAD utility docs (2026-01-07)
- IBM Db2 for z/OS 12.0.0 — DSNTIAUL sample program docs (2026-01-07)
- IBM Db2 for z/OS 12.0.0 — DSN1COPY docs (2026-01-07)
- IBM IMS 15.5.0 — DFSURGU0 (HD Reorganization Unload) docs (2026-03-02)
- IBM Support — "Database migration checklist with Q Replication" (Doc 1105041, IIDR)
- AWS Prescriptive Guidance — "Mainframe data replication strategy for the AWS Cloud" (2025-02-17, ©2026)
- AWS Prescriptive Guidance — mainframe modernization (replatform vs refactor; M2 closure)
- AWS Prescriptive Guidance — BMC AMI Cloud Data backup/archive pattern
- AWS Big Data Blog — Precisely Connect CDC to S3 (2026-05-08, AWS+Precisely)
- AWS Migration & Modernization Blog — Rocket Software replatforming (2025-12-12)
- BMC docs — BMC AMI Cloud Data / AMI Cloud Analytics (Model9 rebrand)
- GitHub (primary): Cobrix (AbsaOSS), JRecord, Legstar v3, legstar.avro, ztract
- Tech Agilist blog — VSAM REPRO/IDCAMS (secondary)
- GoCloud blog (AWS Advanced Tier Partner, 2026-03-04), IN-COM DATA Systems (2026-06-23) — vendor blogs (secondary)

## Related memories
- [[org/architecture/mainframe-ibm-to-vault-core-json-migration-2026-cu-reconciliere]] — the narrower VaultCore/JSON + banking-reconciliation case (this memory is the target-agnostic superset of the export-options portion).
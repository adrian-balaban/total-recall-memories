---
title: 'GCP GCS → AWS S3 transfer 2 TB (2026): soluții bulk + streaming cu verificabilitate'
tags: [org, gcp, aws, s3, gcs, data-transfer, data-migration, flink, kinesis, firehose, pubsub, verification, checksum, crc32c, datasync, rclone, '2026']
author: adrianb
sessions: []
created: '2026-07-29T07:59:11.661Z'
updated: '2026-07-29T07:59:11.661Z'
importanceScore: 0.9
---

## Used Prompt

Esti un arhitect IT; ai de rezolvat urmatoarea problema: 1. data de 2 TB in GCP pe bucket 2. trimitere in streaming sau alta solutie catre S3 AWS 3. verificabilitatea transferului datelor este esentiala 4. simplitatea este esentiala 5. pentru streaming considera si solutii de streaming precum a. Flink, b. Flink plus https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kinesis/ , c. https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pubsub/, d. https://aws.amazon.com/kinesis/data-streams/ ;6.  citeste pe net pentru solutii la zi si constrangeri la zi ; 7. propune solutii cu plusuri si minusuri la fiecare ; 8. scrie informatiile gasite intr-o memorie org

## Executive Summary

# GCP GCS → AWS S3 transfer 2 TB (2026): soluții bulk + streaming cu verificabilitate

## Executive summary (DE CE / CE CONTEAZĂ)
Migrarea a 2 TB dintr-un bucket GCP (GCS) către AWS S3 în 2026. Constrângeri dure: **(1) verificabilitatea transferului** (fiecare byte dovedit intact) și **(2) simplitatea**. User-ul a cerut explicit și variante de streaming: Flink, Flink+Kinesis, Flink+Pub/Sub, Kinesis Data Streams.

**Insight-ul arhitectural decisiv:** cele 2 TB există *la loc* în bucket — acesta este prin natură un problemă de **BULK**, nu de streaming. Streaming-ul are sens doar pentru ingestia *continuă* de obiecte noi/modificate, NU pentru mutarea celor 2 TB istorici. Alegerea depinde de scenariu:
- **ONE-TIME bulk** (doar migrarea celor 2 TB) → soluție batch.
- **BOTH** (2 TB backfill + flux continuu de obiecte noi) → batch pentru istoric + streaming pentru incremental.
- **CONTINUOUS only** (cele 2 TB deja mutate sau out-of-scope) → doar streaming.

Cercetare orchestrată cu workflow multi-agent (5 unghiuri paralele pe web + sinteză), verificat contra documentației curente Flink 2.3 / AWS / GCP din iulie 2026.

## Propunerile (ranked, best first)

### 1. AWS DataSync Enhanced mode (agentless GCS→S3) — BULK ONE-TIME ★recomandat implicit
- **Cum:** mod Enhanced (lansat 2025-05-29, agentless pentru destinații S3) citește GCS via XML API (S3-compatible) cu HMAC key + GCP service account (Storage Object Viewer) și scrie în S3. Listează/transferează/verifică în paralel. Re-run = incremental (doar obiecte noi/modificate). Shard în task-uri concurente doar dacă ai nevoie de >5 Gbps.
- **Plusuri:** cea mai puternică verificabilitate out-of-the-box (in-flight integrity + VerifyMode=ONLY_FILES_TRANSFERRED care recitește sursa și compară checksum per obiect la final; POINT_IN_TIME_CONSISTENT în Basic mode); cea mai simplă operare (fără VM/agent, console + CloudWatch, istoric 30 zile); incremental re-runs; CRC32C interoperabil cu GCS; până la 120 execuții concurente/account-region.
- **Minusuri:** 5 Gbps/task (nu ajustabil) → 2 TB în ~55 min best-case, real mai mult, trebuie sharduit; ~$31 fee (0.015/GB + 0.55/task) peste egress GCP; **object tags NU se copiază** (limitare GCS XML API — lasă debifat sau task-ul eșuează); setup HMAC în GCP + IAM role în AWS.
- **Verificare:** nativă, cea mai puternică din opțiunile batch. Verificabilitate + simplitate maxime → de aici se pornește.
- **Pentru:** migrare bulk one-time 2 TB cu verificabilitate + simplitate ca prioritate și ~$31 fee acceptabil.

### 2. Hibrid: bulk backfill (DataSync) + GCS Pub/Sub OBJECT_FINALIZE → Flink FileSink → S3 — BOTH
- **Cum:** Faza A (backfill 2 TB cu DataSync). Faza B (incremental): notificare GCS Pub/Sub pe OBJECT_FINALIZE (o comandă gcloud), pull subscription, job Flink cu PubSubSource trage evenimente, un ProcessFunction fetch-uiește bytes din GCS după objectId+generation, FileSink scrie în S3 cu native S3 FS (exactly-once via S3 MPU commit la checkpoint). Cutover: pornește stream-ul înainte de backfill; dedup pe objectId+objectGeneration (S3 overwrite pe same key).
- **Plusuri:** split conceptual corect (batch pentru 2 TB, streaming doar pentru obiecte noi — niciodată nu streamezi 2 TB event-by-event prin Pub/Sub); sink TRUE exactly-once (FileSink + flink-s3-fs-native, anunțat 2026-06-26); fiecare obiect S3 are CRC32C server-validat comparabil direct cu GCS crc32c; native S3 FS folosește AWS SDK v2 (legacy depind de SDK v1 EOL 2025-12-31), ~2x checkpoint mai rapid; cutover idempotent.
- **Minusuri:** **DEALBREAKER mid-2026: conectorul Flink Pub/Sub NU există pentru Flink 2.3** ("There is no connector yet available for Flink version 2.3") → trebuie pin la Flink 1.20 (legacy S3 FS plugin, pierzi beneficiile native) sau aștepți FLIP-27 rewrite (PR #32 / FLINK-20625, unmerged); sursa e AT-LEAST-ONCE (ack la checkpoint; dacă checkpoint interval > Pub/Sub ack deadline → duplicate) — exactly-once doar teoretic, trebuie dedup downstream; Flink fetch-uiește separat bytes din GCS (notificarea e doar metadata) → cost/latență GCS + a doua suprafață de eșec; două workflow-uri de operat; native S3 FS experimental în 2.3.
- **Verificare:** cea mai puternică din streaming paths (exactly-once sink, CRC32C per obiect, idempotency key natural objectId+generation).
- **Pentru:** scenariul BOTH cu cea mai tare semantică de sink (exactly-once) și toleranță la pin Flink 1.20 sau așteptare.

### 3. Hibrid: bulk backfill (DataSync) + GCP-side producer → Kinesis Data Streams → Firehose → S3 — BOTH, fără Flink
- **Cum:** Faza A backfill DataSync. Faza B: producer în GCP (Cloud Run / Compute Engine / Dataflow) folosește AWS SDK să cheme Kinesis PutRecordBatch pe endpoint public TLS, auth via Workload Identity Federation (OIDC token GCP → sts:AssumeRoleWithWebIdentity, fără AWS keys statice). KDS alimentează Firehose care bufferează (1-128 MiB / 0-900s) și scrie S3, opțional dynamic partitioning + JSON→Parquet. Se poate sări KDS și scrie direct în Firehose (cea mai lean variantă).
- **Plusuri:** cea mai simplă topologie streaming cu Kinesis (fără endpoint AWS public de securizat, fără Lambda/API Gateway — producerul cheamă afară); PutRecordBatch returnează per-record SequenceNumber + ShardId sincron → cea mai tare chitanță in-flight; KPL agreghează record-uri mici (taie cost PUT ~80-90%); Firehose gestionează batching/partitioning/retry (până 24h DirectPut)/format conversion fără cod Flink; fără capcana versiunii conector Flink.
- **Minusuri:** sink efectiv AT-LEAST-ONCE (Firehose nu are per-object content checksum și nici manifest first-class pentru S3 — "manifest" e doar Redshift-COPY error-path); cod producer custom de scris/operat în GCP (~200 linii cu KPL + WIF); limite Kinesis (1 MiB record default, 1 MiB/sec + 1000 rec/sec per shard provisioned → ~23 shards pentru 2 TB/day); 2 servicii managed extra (KDS + Firehose) → cost (~$250/lună KDS provisioned + ~$1,780/lună Firehose plain delivery la 2 TB/day, peste egress GCP); producer ajunge la Kinesis pe internet public (fără path privat decât cu Cloud Interconnect).
- **Verificare:** stratificată, post-hoc pe S3 (SequenceNumber PutRecordBatch sincron ca in-flight proof; CloudWatch DeliveryToS3.Success; S3 checksum-uri). Lipsa manifest Firehose = gap de verificare vs Propunerea 2.
- **Pentru:** scenariul BOTH centrul pe Kinesis (buffer durabil, batching/format conversion managed, replay fără re-read GCS), evitând Flink — ex. operezi deja Kinesis sau vrei consumatori multipli downstream.

### 4. rclone sync (GCS → S3), self-hosted — BULK, alternativă free
- **Cum:** rclone pe un VM robust (GCE în regiunea sursă minimizează routing egress, sau EC2) citește GCS (service-account sau HMAC) și scrie S3. Defaults 2 TB: `--transfers 64 --checkers 64 --s3-upload-concurrency 8 --s3-chunk-size 16M --checksum --fast-list`. Re-run = incremental.
- **Plusuri:** tooling gratuit (doar egress GCP + S3 requests + câțiva $ VM-hours); verificare checksum excelentă (`--checksum` compară size + MD5 fără API transactions extra când ambele părți S3-compatible); scalabil orizontal la PB (AWS blog: 2.7 PB IBM COS→S3 în ~2 săptămâni ~$2K compute); `rclone check` / `rclone check --download` pentru audit byte-for-byte.
- **Minusuri:** tu operezi (VM, flag-uri, monitorizare, eșecuri); listarea milioanelor de obiecte poate dura ore (`--fast-list`, ~1 KB RAM/obiect); fără raport end-of-transfer full reconciliation ca DataSync VerifyMode (rulează `rclone check` separat); **capcana MD5/multipart**: obiectele S3 multipart nu au MD5 ETag → `--checksum` poate cădea pe size-only; pentru obiecte GCS-multipart preferă tooling CRC32C (gcloud storage hash); tuning pentru throughput.
- **Verificare:** bună și gratuită; `rclone check` separat pentru source-vs-dest audit.
- **Pentru:** migrare bulk one-time 2 TB cu cost zero tooling, comfort ops cu rclone, sau nevoie de scalare orizontală peste un singur task managed.

### 5. Flink FileSink direct-to-S3 (streaming-only) — CONTINUOUS INGESTION
- **Cum:** job Flink streaming citește din GCP (GCS via FileSystem connector, sau Pub/Sub) și scrie direct S3 cu FileSink + native S3 FS (flink-s3-fs-native, Flink 2.3 experimental) sau flink-s3-fs-hadoop. Part files: in-progress → pending → finished; pending devin finished doar la checkpoint reușit via RecoverableWriter pe S3 MPU. Checkpointing obligatoriu. **NU mută cele 2 TB la loc** — necesită backfill bulk separat.
- **Plusuri:** TRUE exactly-once end-to-end (când checkpointing on + plugin RecoverableWriter — flink-s3-fs-hadoop sau native, NU presto); mai puține piese decât path Kinesis (doar Flink + S3); fiecare obiect S3 are checksum (CRC64NVME default din Dec 2024, sau CRC32C/SHA-256 opt-in) validat la upload + verificabil la download; native S3 FS 2026 (~2x checkpoint, drop-in JAR); fără costuri per-shard/per-GB Kinesis/Firehose; compaction (din Flink 1.15).
- **Minusuri:** **TOOL GREȘIT pentru cele 2 TB la loc** (mai lent, mai scump, mai greu operat decât batch); checkpointing obligatoriu STREAMING (altfel part files rămân in-progress/pending forever); RecoverableWriter depinde de plugin (flink-s3-fs-presto NU suportă — misconfiguration comun); lifecycle MPU abort prea agresiv poate sparge restore; Flink gestionează layout/bucketing/rolling/compaction; dacă sursa e Pub/Sub se aplică dealbreaker-ul Flink 2.3.
- **Verificare:** cea mai puternică din streaming paths (exactly-once, CRC32C per obiect, linearize pentru multipart).
- **Pentru:** ingestie continuă genuină de date noi din GCP (nu cele 2 TB) cu semantică exactly-once, cuplată cu un tool bulk separat.

## Recomandare (actionabilă)
1. **ONE-TIME 2 TB BULK (fără feed ongoing):** Propunerea 1 — AWS DataSync Enhanced. Cel mai bun fit pentru ambele constrângeri: cea mai simplă (agentless, console, fără VM) ȘI cea mai puternică verificabilitate nativă (in-flight + VerifyMode=ONLY_FILES_TRANSFERRED + CRC32C interop GCS). Cost ~$31 fee + egress GCP (~$157 Standard / ~$235 Premium; AWS ingress free). Optează egress la GCP Standard Tier pentru bulk latency-tolerant → salvezi ~$78. **Pornim de aici.**
2. **BOTH (backfill 2 TB + streaming ongoing):** Propunerea 2 (DataSync backfill + GCS Pub/Sub OBJECT_FINALIZE → Flink FileSink → S3). Split corect: niciodată nu streamezi 2 TB prin Pub/Sub. CAVEAT CRITIC: conector Flink Pub/Sub nu există în Flink 2.3 mid-2026 → pin Flink 1.20 sau așteaptă FLIP-27; sursa at-least-once → dedup pe objectId+objectGeneration. Dacă blocker-ul Flink 2.3 e inacceptabil sau operezi deja Kinesis → Propunerea 3 (producer GCP → KDS → Firehose → S3, fără Flink; cea mai tare chitanță in-flight sincronă, dar verificare S3 post-hoc fără manifest Firehose).
3. **CONTINUOUS only (2 TB out-of-scope/already moved):** Propunerea 5 (Flink FileSink direct-S3) pentru exactly-once sink. NU folosi Flink pentru cele 2 TB la loc.

**Opțiuni dominate (NU alege):** Google Cloud Storage Transfer Service (STS) — DISQUALIFIED (din May 2026 data_sink acceptă doar gcsDataSink/posixDataSink; S3 e source-only, nu poate push GCS→S3). AWS S3 Transfer Acceleration — MISMATCHED (accelerează upload-uri ÎN S3 de la clienți internet distanți, adaugă $0.04-0.08/GB; pentru datacenter-to-datacenter GCS→S3 adaugă cost fără beneficiu). Flink+Kinesis connector ca SINK (KinesisStreamsSink) — dominat de FileSink (e explicit AT-LEAST-ONCE, duplicate la checkpoint restore, +1 serviciu managed); folosește-l doar dacă vrei Kinesis ca buffer/replay bus. Flink+Pub/Sub moștenește dealbreaker-ul Flink 2.3.

**Bottom line pentru decident:** Pentru cele 2 TB la loc răspunsul e batch, nu streaming — DataSync Enhanced (Propunerea 1) e default. Adaugă streaming (Propunerea 2 sau 3) doar dacă există cerință genuină de feed ongoing, și acceptă că jumătatea streaming adună complexitate reală + caveat Flink-connector 2026.

## Constrângeri / gotchas critice 2026 (verifică înainte de commit)
- **Google STS nu poate scrie în S3/S3-compatible** (source-only) — confirmat contra release notes May 2026 + TransferSpec REST. Nu presupune capabilitate simetrică GCS↔S3.
- **Flink 2.3 NU are conector Pub/Sub** mid-2026 ("There is no connector yet available for Flink version 2.3"); legacy (Flink 1.17-1.20) e at-least-once. FLIP-27 rewrite (FLINK-20625 / PR #32) unmerged. Pin Flink 1.20 sau așteaptă.
- **Flink KinesisStreamsSink e AT-LEAST-ONCE** (duplicate la checkpoint restore; PutRecords nu garantează ordine per-record). KinesisStreamsSource (reader) IS exactly-once — asimetria source/sink e caveat-ul arhitectural cheie. FileSink + native S3 FS e singurul sink Flink exactly-once către S3.
- **Firehose NU are manifest per-obiect user-facing pentru S3** — "manifest" e doar Redshift-COPY error-path (errors/). Pentru S3 verifică via CloudWatch (DeliveryToS3.Success) + S3 checksum-uri (HeadObject ChecksumMode=ENABLED) + opțional Lambda verificare pe S3 event. Tratează livrarea Firehose ca "probabil livrat" până trece checksum post-hoc.
- **CRC32C interop:** GCS și S3 folosesc același polinom Castagnoli → GCS crc32c == S3 x-amz-checksum-crc32c FULL_OBJECT byte-for-byte pentru single-part. Pentru multipart TREBUIE ceri explicit FULL_OBJECT — COMPOSITE CRC32C (default) e checksum-of-checksums și NU va egala GCS. SDK-urile AWS recente default-ează CRC64NVME (signed trailing checksums) pe care endpoint-ul XML S3-compatible GCP îl respinge → override la CRC32C sau setează request_checksum_calculation=when_required când țintești GCP.
- **Cost:** AWS S3 ingress $0/GB; tot bill-ul e egress GCP. ~$235 Premium (default) vs ~$157 Standard (opt-in, internet public, fără SLA Google backbone — ok pentru batch) pentru 2 TB. NAT Gateway ($0.045/GB), cross-AZ, PrivateLink, S3 Transfer Acceleration, MRAP adaugă per-GB — evită pentru ingress internet plain.
- **Rețea:** fără peering privat nativ GCP↔AWS default. AWS Interconnect – multicloud (GA 14 Apr 2026, GCP primul partner) dă Layer-3 privat managed (MACsec, minute provisioning, 1-100 Gbps) dar doar 5 perechi regiuni launch (us-east-1, us-west-1/los-angeles, us-west-2, eu-west-2, eu-central-1) și bate Standard Tier egress doar la >20 TB/month susținut după port fees — nu merită pentru o mutare one-shot 2 TB.
- **DataSync Enhanced limits:** 5 Gbps/task (neajustabil), 120 execuții concurente/account-region, 100 tasks/account-region (ajustabil), 20 GB manifest limit, object tags NU se copiază (limitare GCS XML API — lasă debifat sau eșuează), obiecte GCS archive incur retrieval fees.
- **Kinesis limits (Propunerea 3):** 1 MiB record default (ridicabil la 10 MiB dar burst-only, AWS recomandă <2% trafic), 1 MiB/sec + 1000 rec/sec per shard provisioned, PutRecordBatch 500 records/4 MiB, Firehose 1,000 KiB record, dynamic partitioning 500 active partitions/stream (ridicabil 2,500), buffer 60-900s. On-demand KDS scalează la 10 GB/s write în us-east/us-west/eu-ireland (200 MB/s altundeva).
- **Verifică** disponibilitatea DataSync Enhanced mode + detaliile exacte GCS-HMAC contra docurilor AWS curente pentru regiunea ta înainte de commit.

## Surse cheie
- DataSync Enhanced GCS→S3: docs.aws.amazon.com/datasync (tutorial GCS, configure-data-verification-options, limits, pricing, blog migrating-gcs-to-s3)
- Flink 2.3 Kinesis connector: nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kinesis/
- Flink 2.3 Pub/Sub connector: nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pubsub/ + guarantees + FLINK-20625 + PR #32
- Flink native S3 FS (2026-06-26): flink.apache.org/2026/06/26/announcing-native-s3-fs/ + PR #27187
- Flink FileSink: nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/filesystem/
- Kinesis Data Streams: aws.amazon.com/kinesis/data-streams/ + service-sizes-and-limits + pricing
- Firehose: docs.aws.amazon.com/firehose (buffering, retry, limits, dynamic-partitioning, monitoring-with-cloudwatch-logs)
- GCS Pub/Sub notifications: docs.cloud.google.com/storage/docs/pubsub-notifications + reporting-changes
- S3 checksums: docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity*.html + sdkref feature-dataintegrity + 2026-04 five-additional-checksum-algorithms
- GCS data validation: docs.cloud.google.com/storage/docs/data-validation + gcloud storage hash
- rclone: rclone.org/s3/ + commands/rclone_sync + rclone check
- AWS Interconnect multicloud GA: aws.amazon.com/about-aws/whats-new/2026/04/aws-announces-ga-AWS-interconnect-multicloud/
- Cost: aws.amazon.com/s3/pricing, cloud.google.com/network-tiers/pricing, egresscost.com

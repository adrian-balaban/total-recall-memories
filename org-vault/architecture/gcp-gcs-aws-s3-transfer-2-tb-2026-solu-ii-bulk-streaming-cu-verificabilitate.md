---
title: 'GCP GCS → AWS S3 transfer 2 TB (2026): soluții bulk + streaming cu verificabilitate'
tags: [org, gcp, aws, s3, gcs, data-transfer, data-migration, push, vault-core, thought-machine, data-loader, workload-identity-federation, flink, kinesis, firehose, pubsub, verification, checksum, crc32c, rclone, datasync-descalificat, '2026']
author: adrianb
sessions: []
created: '2026-07-29T07:59:11.661Z'
updated: '2026-07-29T16:07:55.261Z'
importanceScore: 0.9
---

## Executive Summary

---
title: 'GCP GCS → AWS S3 transfer 2 TB (2026): soluții Push bulk + streaming cu verificabilitate, pentru inputul Vault Core Loader'
tags: [org, gcp, aws, s3, gcs, data-transfer, data-migration, push, vault-core, thought-machine, data-loader, workload-identity-federation, flink, kinesis, firehose, pubsub, verification, checksum, crc32c, rclone, datasync-descalificat, '2026']
author: adrianb
sessions: []
created: '2026-07-29T07:59:11.661Z'
updated: '2026-07-29'
importanceScore: 0.9
---

## Used Prompt

Esti un arhitect IT; ai de rezolvat urmatoarea problema: 1. data de 2 TB in GCP pe bucket 2. trimitere in streaming sau alta solutie catre S3 AWS 3. verificabilitatea transferului datelor este esentiala 4. simplitatea este esentiala 5. pentru streaming considera si solutii de streaming precum a. Flink, b. Flink plus https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kinesis/ , c. https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pubsub/, d. https://aws.amazon.com/kinesis/data-streams/ ; 6. citeste pe net pentru solutii la zi si constrangeri la zi ; 7. propune solutii cu plusuri si minusuri la fiecare ; 8. scrie informatiile gasite intr-o memorie org

**Addendum (revizuire 2026-07-29 — 3 constrângeri noi care reframează soluția):**
1. Soluția trebuie să fie **Push** (sursa/GCP împinge datele spre destinație), **nu Pull** (destinația nu trage din sursă).
2. **AWS DataSync face Pull** (citește GCS din AWS) → descalificat de constrângerea Push.
3. Destinația finală este **Vault Core (Thought Machine)**, iar pentru aceasta **interesează doar partea de Loader** (Data Loader / Migration API) — S3 e inputul Loader-ului, scope-ul se limitează la livrarea verificabilă către Loader, nu la procesarea internă Vault Core.

## Executive Summary

# GCP GCS → AWS S3 transfer 2 TB (2026): soluții Push bulk + streaming cu verificabilitate, pentru inputul Vault Core Loader

## Executive summary (DE CE / CE CONTEAZĂ)
Migrarea a 2 TB dintr-un bucket GCP (GCS) către AWS S3 în 2026, ca **input pentru Vault Core Data Loader** (Thought Machine). Constrângeri dure: **(1) Push** (sursa împinge, destinația NU trage), **(2) verificabilitatea transferului** (fiecare byte dovedit intact, într-o formă pe care Loader-ul o poate reconcilia), **(3) simplitatea**, și **(4) scope = doar partea de Loader** a Vault Core. User-ul a cerut explicit și variante de streaming: Flink, Flink+Kinesis, Flink+Pub/Sub, Kinesis Data Streams.

**Insight-ul arhitectural decisiv (reframat Push + Loader):**
- Cele 2 TB există *la loc* în bucket → prin natură problemă de **BULK**, nu de streaming. Streaming-ul are sens doar pentru ingestia *continuă* de obiecte noi/modificate, NU pentru mutarea celor 2 TB istorici.
- **Push din GCP este forțat**: GCP **nu are niciun serviciu nativ care să push-uie în S3** (Google STS e source-only pentru S3 — vezi gotchas). Deci orice soluție Push = **un producer/compute care rulează în GCP** și scrie în S3 (sau în Kinesis/Firehose→S3) cu AWS SDK + Workload Identity Federation. **Locația execuției decide Push/Pull**: rclone/Flink/producer trebuie să ruleze în GCP; aceleași tool-uri pe EC2/AWS devin Pull.
- **Vault Core Loader**: per memoria `mainframe-ibm-to-vault-core-json-migration-2026-cu-reconciliere`, Data Loader consumă nominal **Kafka (Migration API, JSON over Kafka)**, nu S3 direct. **Assumție (de confirmat cu Thought Machine sub NDA): în deployment-ul tău Loader-ul citește S3 direct** (interpretarea adoptată aici). Dacă nu, S3 e staging și verificarea reală se mută pe frontiera S3→Kafka→Loader (vezi Verificare + gotchas).

Alegerea depinde de scenariu:
- **ONE-TIME bulk** (doar migrarea celor 2 TB) → soluție batch Push.
- **BOTH** (2 TB backfill + flux continuu) → batch Push pentru istoric + streaming Push pentru incremental.
- **CONTINUOUS only** (cele 2 TB deja mutate/out-of-scope) → doar streaming Push.

Cercetare orchestrată cu workflow multi-agent (5 unghiuri paralele pe web + sinteză), verificat contra documentației curente Flink 2.3 / AWS / GCP din iulie 2026; revizuit pentru constrângerea Push + scope Loader.

## Propunerile (ranked, best first — Push-first)

### 1. rclone sync pe GCE (GCS → S3) — BULK ONE-TIME Push ★recomandat implicit
- **Cum:** rclone pe un VM **GCE în regiunea sursă GCP** (NU EC2 — altfel devine Pull) citește GCS (service-account sau HMAC) și **push-uie în S3** cu AWS SDK-ul său. Defaults 2 TB: `--transfers 64 --checkers 64 --s3-upload-concurrency 8 --s3-chunk-size 16M --checksum --fast-list`. Re-run = incremental idempotent (doar obiecte noi/modificate) — se aliniază natural cu modelul de **load dormant pe tranșe** al Vault Core.
- **Plusuri:** **Push** (execuție în GCP); tooling gratuit (doar egress GCP + S3 requests + câțiva $ VM-hours); verificare checksum excelentă (`--checksum` compară size + MD5 fără API transactions extra când ambele părți S3-compatible); scalabil orizontal la PB (AWS blog: 2.7 PB IBM COS→S3 în ~2 săptămâni ~$2K compute); `rclone check` / `rclone check --download` pentru audit byte-for-byte; re-run incremental idempotent = potrivește cu tranșe/idempotență Loader.
- **Minusuri:** tu operezi (VM, flag-uri, monitorizare, eșecuri); listarea milioanelor de obiecte poate dura ore (`--fast-list`, ~1 KB RAM/obiect); fără raport end-of-transfer full reconciliation ca manifest Loader-consumabil (rulează `rclone check` separat și generezi manifestul tu); **capcana MD5/multipart**: obiectele S3 multipart nu au MD5 ETag → `--checksum` poate cădea pe size-only; pentru obiecte GCS-multipart preferă CRC32C (gcloud storage hash); tuning pentru throughput.
- **Push/Pull:** **Push** (GCE).
- **Verificare:** bună și gratuită; `rclone check` separat pentru source-vs-dest audit + manifest generat separat pentru Loader (objectId+generation, checksum, quiet-point/LRSN marker).
- **Pentru:** migrare bulk one-time 2 TB Push cu cost zero tooling, comfort ops cu rclone, gândit ca input pentru Loader pe tranșe.

### 2. Producer GCP (Dataflow / Cloud Run / GCE) → S3 direct cu AWS SDK — BULK ONE-TIME Push, control programatic
- **Cum:** producer în GCP folosește AWS SDK v2 să scrie direct în S3 (PutObject / multipart MPU), auth via **Workload Identity Federation** (OIDC token GCP → `sts:AssumeRoleWithWebIdentity`, fără AWS keys statice). La upload cere explicit **checksum CRC32C FULL_OBJECT** (compatibil GCS). Emite un **manifest Loader-consumabil** per tranșe: `(objectId, objectGeneration, size, crc32c, quiet-point/LRSN marker)`. Re-run idempotent pe `objectId+objectGeneration` (S3 overwrite pe same key).
- **Plusuri:** **Push**; control programatic complet asupra manifestului/tranșelor (cuplare strânsă cu Data Loader — ordine dependențe, tranșe dormant, activare); CRC32C server-validat S3 comparabil direct cu GCS crc32c byte-for-byte (single-part); fără servicii managed extra (doar compute GCP + egress); WIF = zero AWS keys statice (securitate banking-grade).
- **Minusuri:** cod custom de scris/operat (~200 linii: AWS SDK v2 + WIF + MPU + manifest); tu gestionezi retry/parallelism/checkpointing (ceea ce rclone/DataSync fac managed); tuning throughput; **trebuie override la CRC32C** (SDK-urile AWS recente default-ează CRC64NVME pe care endpoint-ul XML GCP îl respinge — vezi gotchas).
- **Push/Pull:** **Push** (GCP).
- **Verificare:** cea mai bună pentru scop Loader — manifest nativ Loader-consumabil + CRC32C per obiect + idempotency key natural `objectId+generation`; cross-check direct vs. ResourceMigratedEvents.
- **Pentru:** bulk one-time 2 TB Push când vrei manifest custom / orchestrare tranșe cuplată cu Loader, sau ai deja un producer GCP de extins.

### 3. Hibrid: bulk backfill Push (rclone-GCE) + GCS Pub/Sub OBJECT_FINALIZE → Flink (în GCP) FileSink → S3 — BOTH, Push
- **Cum:** Faza A (backfill 2 TB Push cu rclone pe GCE — Propunerea 1). Faza B (incremental Push): notificare GCS Pub/Sub pe OBJECT_FINALIZE, pull subscription, job **Flink în GCP** cu PubSubSource trage evenimente, un ProcessFunction fetch-uiește bytes din GCS după objectId+generation, FileSink push-uie în S3 cu native S3 FS (exactly-once via S3 MPU commit la checkpoint). Cutover: pornește stream-ul înainte de backfill; dedup pe objectId+objectGeneration (S3 overwrite pe same key).
- **Plusuri:** **Push** (Flink în GCP scrie în S3); split conceptual corect (batch pentru 2 TB, streaming doar pentru obiecte noi — niciodată nu streamezi 2 TB event-by-event prin Pub/Sub); sink TRUE exactly-once (FileSink + flink-s3-fs-native, anunțat 2026-06-26); fiecare obiect S3 are CRC32C server-validat comparabil direct cu GCS crc32c; native S3 FS folosește AWS SDK v2, ~2x checkpoint mai rapid; cutover idempotent.
- **Minusuri:** **DEALBREAKER mid-2026: conectorul Flink Pub/Sub NU există pentru Flink 2.3** → pin la Flink 1.20 (legacy S3 FS plugin, pierzi beneficiile native) sau așteaptă FLIP-27 rewrite (PR #32 / FLINK-20625, unmerged); sursa AT-LEAST-ONCE (ack la checkpoint; dacă checkpoint interval > Pub/Sub ack deadline → duplicate) — exactly-once doar teoretic, dedup downstream; Flink fetch-uiește separat bytes din GCS (notificarea e doar metadata) → cost/latență GCS + a doua suprafață de eșec; două workflow-uri de operat; native S3 FS experimental în 2.3.
- **Push/Pull:** **Push** (Flink în GCP → S3; sursa Pub/Sub e GCP-internă).
- **Verificare:** cea mai puternică din streaming paths Push (exactly-once sink, CRC32C per obiect, idempotency key natural objectId+generation).
- **Pentru:** scenariul BOTH Push cu cea mai tare semantică de sink (exactly-once) și toleranță la pin Flink 1.20 sau așteptare.

### 4. Hibrid: bulk backfill Push (rclone-GCE) + producer GCP → Kinesis Data Streams → Firehose → S3 — BOTH, Push, fără Flink
- **Cum:** Faza A backfill Push cu rclone-GCE. Faza B: producer în GCP (Cloud Run / GCE / Dataflow) folosește AWS SDK să cheme Kinesis PutRecordBatch pe endpoint public TLS, auth via Workload Identity Federation. KDS alimentează Firehose care bufferează (1-128 MiB / 0-900s) și push-uie în S3, opțional dynamic partitioning + JSON→Parquet. Se poate sări KDS și scrie direct în Firehose (cea mai lean variantă Push).
- **Plusuri:** **Push** (producer GCP cheamă afară); cea mai simplă topologie streaming cu Kinesis (fără endpoint AWS public de securizat, fără Lambda/API Gateway); PutRecordBatch returnează per-record SequenceNumber + ShardId sincron → cea mai tare chitanță in-flight; KPL agreghează record-uri mici (taie cost PUT ~80-90%); Firehose gestionează batching/partitioning/retry (până 24h DirectPut)/format conversion fără cod Flink; fără capcana versiunii conector Flink.
- **Minusuri:** sink efectiv AT-LEAST-ONCE (Firehose nu are per-object content checksum și nici manifest first-class pentru S3 — "manifest" e doar Redshift-COPY error-path) → **gap de verificare vs. Propunerea 2/3, important pentru reconciliere Loader**; cod producer custom în GCP; limite Kinesis (1 MiB record default, 1 MiB/sec + 1000 rec/sec per shard provisioned → ~23 shards pentru 2 TB/day); 2 servicii managed extra (KDS + Firehose) → cost (~$250/lună KDS provisioned + ~$1,780/lună Firehose plain delivery la 2 TB/day, peste egress GCP); producer ajunge la Kinesis pe internet public (fără path privat decât cu Cloud Interconnect).
- **Push/Pull:** **Push** (producer GCP → Kinesis/Firehose → S3).
- **Verificare:** stratificată, post-hoc pe S3 (SequenceNumber PutRecordBatch sincron ca in-flight proof; CloudWatch DeliveryToS3.Success; S3 checksum-uri). Lipsa manifest Firehose = gap vs. Propunerile 2/3 — trebuie manifest Loader-consumabil construit separat.
- **Pentru:** scenariul BOTH Push centrat pe Kinesis (buffer durabil, batching/format conversion managed, replay fără re-read GCS), evitând Flink — ex. operezi deja Kinesis sau vrei consumatori multipli downstream.

### 5. Flink (în GCP) FileSink direct-to-S3 (streaming-only) — CONTINUOUS INGESTION Push
- **Cum:** job **Flink streaming în GCP** citește din GCP (GCS via FileSystem connector, sau Pub/Sub) și push-uie direct în S3 cu FileSink + native S3 FS (flink-s3-fs-native, Flink 2.3 experimental) sau flink-s3-fs-hadoop. Part files: in-progress → pending → finished; pending devin finished doar la checkpoint reușit via RecoverableWriter pe S3 MPU. Checkpointing obligatoriu. **NU mută cele 2 TB la loc** — necesită backfill bulk Push separat (Propunerea 1 sau 2).
- **Plusuri:** **Push** (Flink în GCP → S3); TRUE exactly-once end-to-end (când checkpointing on + plugin RecoverableWriter — flink-s3-fs-hadoop sau native, NU presto); mai puține piese decât path Kinesis (doar Flink + S3); fiecare obiect S3 are checksum (CRC64NVME default din Dec 2024, sau CRC32C/SHA-256 opt-in) validat la upload + verificabil la download; native S3 FS 2026 (~2x checkpoint, drop-in JAR); fără costuri per-shard/per-GB Kinesis/Firehose; compaction (din Flink 1.15).
- **Minusuri:** **TOOL GREȘIT pentru cele 2 TB la loc** (mai lent, mai scump, mai greu operat decât batch Push); checkpointing obligatoriu STREAMING (altfel part files rămân in-progress/pending forever); RecoverableWriter depinde de plugin (flink-s3-fs-presto NU suportă — misconfiguration comun); lifecycle MPU abort prea agresiv poate sparge restore; Flink gestionează layout/bucketing/rolling/compaction; dacă sursa e Pub/Sub se aplică dealbreaker-ul Flink 2.3.
- **Push/Pull:** **Push** (Flink în GCP → S3).
- **Verificare:** cea mai puternică din streaming paths Push (exactly-once, CRC32C per obiect, linearize pentru multipart).
- **Pentru:** ingestie continuă genuină Push de date noi din GCP (nu cele 2 TB) cu semantică exactly-once, cuplată cu un tool bulk Push separat.

## Recomandare (actionabilă, Push + Loader)
1. **ONE-TIME 2 TB BULK Push (fără feed ongoing):** Propunerea 1 — **rclone pe GCE → S3**. Cel mai bun fit pentru constrângerile Push + simplitate + verificabilitate + cost zero tooling, cu re-run incremental idempotent ce se aliniază cu load-ul pe tranșe al Loader-ului. Cost: doar egress GCP (~$157 Standard / ~$235 Premium; AWS ingress free) + câțiva $ VM-hours. Optează egress la GCP Standard Tier pentru bulk latency-tolerant → salvezi ~$78. Generează manifest Loader-consumabil separat (`rclone check` + script). **Pornim de aici.** Dacă vrei manifest custom / orchestrare tranșe cuplată strâns cu Data Loader → Propunerea 2 (producer GCP → S3 cu CRC32C opt-in + manifest nativ).
2. **BOTH (backfill 2 TB + streaming ongoing):** Propunerea 3 (rclone-GCE backfill + GCS Pub/Sub OBJECT_FINALIZE → Flink în GCP → FileSink → S3). Split corect Push: niciodată nu streamezi 2 TB prin Pub/Sub. CAVEAT CRITIC: conector Flink Pub/Sub nu există în Flink 2.3 mid-2026 → pin Flink 1.20 sau așteaptă FLIP-27; sursa at-least-once → dedup pe objectId+objectGeneration. Dacă blocker-ul Flink 2.3 e inacceptabil sau operezi deja Kinesis → Propunerea 4 (producer GCP → KDS → Firehose → S3, fără Flink; cea mai tare chitanță in-flight sincronă, dar verificare S3 post-hoc fără manifest Firehose → construiește manifest Loader separat).
3. **CONTINUOUS only (2 TB out-of-scope/already moved):** Propunerea 5 (Flink în GCP → FileSink direct-S3) pentru exactly-once sink Push. NU folosi Flink pentru cele 2 TB la loc.

**Opțiuni dominate / descalificate (NU alege):**
- **AWS DataSync Enhanced** — **DESCALIFICAT: Pull** (serviciul AWS citește GCS; încalcă constrângerea Push). Fusese anterior #1; retras.
- **rclone pe EC2/AWS** — **Pull** (destinația trage din GCS). Varianta Push este rclone pe GCE (Propunerea 1).
- **Google Cloud Storage Transfer Service (STS)** — DISQUALIFIED (din May 2026 data_sink acceptă doar gcsDataSink/posixDataSink; S3 e source-only, **nu poate push GCS→S3**; și altfel ar fi tot Pull/destinație-GCP).
- **AWS S3 Transfer Acceleration** — MISMATCHED (accelerează upload-uri ÎN S3 de la clienți internet distanți, adaugă $0.04-0.08/GB; pentru datacenter-to-datacenter GCS→S3 adaugă cost fără beneficiu).
- **Flink+Kinesis connector ca SINK (KinesisStreamsSink)** — dominat de FileSink (e explicit AT-LEAST-ONCE, duplicate la checkpoint restore, +1 serviciu managed); folosește-l doar dacă vrei Kinesis ca buffer/replay bus.
- **Flink+Pub/Sub** moștenește dealbreaker-ul Flink 2.3.

**Bottom line pentru decident:** Sub constrângerea Push, GCP nu are push nativ în S3 → execuția trebuie să ruleze în GCP. Pentru cele 2 TB la loc răspunsul rămâne **batch Push, nu streaming** — **rclone pe GCE** (Propunerea 1) e default, cu **producer GCP→S3** (Propunerea 2) dacă vrei manifest/tranșe cuplate cu Loader. Adaugă streaming Push (Propunerea 3 sau 4) doar dacă există cerință genuină de feed ongoing. Verificabilitatea se judecă la frontura Loader-ului: manifest Loader-consumabil + cross-check vs. ResourceMigratedEvents + control/hash totals în integer minor units.

## Constrângeri / gotchas critice 2026 (verifică înainte de commit)
- **Push-forțat din GCP:** GCP nu are niciun serviciu nativ care să push-uie în S3. Orice soluție Push necesită **compute în GCP** (GCE / Cloud Run / Dataflow / Flink-cluster în GCP) care scrie în S3 cu AWS SDK. **Locația execuției decide Push/Pull** — aceleași tool-uri pe EC2/AWS devin Pull.
- **Workload Identity Federation pentru auth cross-cloud:** producerul GCP ia credențiale AWS via OIDC token GCP → `sts:AssumeRoleWithWebIdentity` (fără AWS keys statice — banking-grade). Config: GCP Workload Identity Pool + AWS IAM role cu trust policy pe GCP provider.
- **Vault Core Loader — confirmă inputul:** per `mainframe-ibm-to-vault-core-json-migration-2026-cu-reconciliere`, Data Loader consumă nominal **Kafka (Migration API, JSON over Kafka)**, nu S3 direct. **Assumția aici: Loader-ul tău citește S3.** Dacă nu, S3 e staging și un producer separat publică din S3 pe Kafka (Migration API); atunci verificarea reală e pe frontura S3→Kafka→Loader (ResourceMigratedEvents), iar GCS→S3 e doar hop bulk. Confirmă cu Thought Machine (NDA / Enablement Portal portal.thoughtmachine.net).
- **Format JSON pentru Loader:** BigDecimal/DecimalType/NUMERIC(p,s) **niciodată double** (veridicitate la cent). Livrare **per-tranche**, idempotent pe `objectId+objectGeneration`, cu **markere quiet-point/LRSN aliniate cu snapshot-ul bulk** ca Loader-ul să poată activa tranșe dormant.
- **Google STS nu poate scrie în S3/S3-compatible** (source-only) — confirmat contra release notes May 2026 + TransferSpec REST. Nu presupune capabilitate simetrică GCS↔S3.
- **Flink 2.3 NU are conector Pub/Sub** mid-2026 ("There is no connector yet available for Flink version 2.3"); legacy (Flink 1.17-1.20) e at-least-once. FLIP-27 rewrite (FLINK-20625 / PR #32) unmerged. Pin Flink 1.20 sau așteaptă.
- **Flink KinesisStreamsSink e AT-LEAST-ONCE** (duplicate la checkpoint restore; PutRecords nu garantează ordine per-record). KinesisStreamsSource (reader) IS exactly-once — asimetria source/sink e caveat-ul arhitectural cheie. FileSink + native S3 FS e singurul sink Flink exactly-once către S3.
- **Firehose NU are manifest per-obiect user-facing pentru S3** — "manifest" e doar Redshift-COPY error-path (errors/). Pentru S3 verifică via CloudWatch (DeliveryToS3.Success) + S3 checksum-uri (HeadObject ChecksumMode=ENABLED) + opțional Lambda verificare pe S3 event. Pentru reconciliere Loader construiește manifest separat.
- **CRC32C interop + override SDK:** GCS și S3 folosesc același polinom Castagnoli → GCS crc32c == S3 x-amz-checksum-crc32c FULL_OBJECT byte-for-byte pentru single-part. Pentru multipart TREBUIE ceri explicit FULL_OBJECT — COMPOSITE CRC32C (default) e checksum-of-checksums și NU va egala GCS. SDK-urile AWS recente default-ează CRC64NVME (signed trailing checksums) pe care endpoint-ul XML S3-compatible GCP îl respinge → **override la CRC32C** sau setează `request_checksum_calculation=when_required` când țintești GCP. La upload Push (Propunerile 2/3/5) cere CRC32C FULL_OBJECT explicit.
- **Cost:** AWS S3 ingress $0/GB; tot bill-ul e egress GCP. ~$235 Premium (default) vs ~$157 Standard (opt-in, internet public, fără SLA Google backbone — ok pentru batch) pentru 2 TB. NAT Gateway ($0.045/GB), cross-AZ, PrivateLink, S3 Transfer Acceleration, MRAP adaugă per-GB — evită pentru ingress internet plain.
- **Rețea:** fără peering privat nativ GCP↔AWS default. AWS Interconnect – multicloud (GA 14 Apr 2026, GCP primul partner) dă Layer-3 privat managed dar doar 5 perechi regiuni launch și bate Standard Tier egress doar la >20 TB/month susținut — nu merită pentru o mutare one-shot 2 TB. Producer Push ajunge la S3/Kinesis pe internet public (TLS) — OK pentru bulk; pentru sensitiv confirmă path-ul cu sec/comp.
- **rclone limits (Propunerea 1):** listarea milioanelor de obiecte poate dura ore (`--fast-list`, ~1 KB RAM/obiect); obiectele S3 multipart nu au MD5 ETag → `--checksum` poate cădea pe size-only (preferă CRC32C / gcloud storage hash); tuning throughput (`--transfers`, `--s3-chunk-size`).
- **Verifică** disponibilitatea detaliilor exacte GCS-HMAC/service-account + override CRC32C contra docurilor AWS/GCP curente pentru regiunea ta înainte de commit.

## Verificare (reframeată pe frontura Loader-ului)
Verificabilitatea se judecă la ce poate consuma **Vault Core Data Loader** + canalul său de reconciliere, nu doar la checksum byte-level GCS↔S3:
1. **Manifest Loader-consumabil** per tranșe: `(objectId, objectGeneration, size, crc32c, quiet-point/LRSN marker)` — generat de Propunerile 1 (separat) sau 2 (nativ).
2. **Cross-check vs. ResourceMigratedEvents** (Streaming API Kafka al Vault Core) — evenimentele de load/activare trebuie să se reconcileze cu manifestul livrat.
3. **Control totals + hash totals** pe JSON-ul livrat (row count, sum amount în **integer minor units**, hash key fields) — perecheate cu un calcul identic pe sursa mainframe (DFSORT STATS/DISPLAY) sau pe GCS.
4. **CRC32C FULL_OBJECT** per obiect S3 comparabil direct cu GCS crc32c (override SDK — vezi gotchas).
5. **Idempotency key** natural `objectId+objectGeneration` (S3 overwrite pe same key) — re-run incremental fără dubluri.

## Surse cheie
- DataSync Enhanced GCS→S3 (context, descalificat ca Pull): docs.aws.amazon.com/datasync (tutorial GCS, configure-data-verification-options, limits, pricing, blog migrating-gcs-to-s3)
- rclone GCS→S3 (Propunerea 1): rclone.org/s3/ + commands/rclone_sync + rclone check
- AWS SDK v2 + WIF + S3 checksums (Propunerea 2): docs.aws.amazon.com/AmazonS3/latest/userguide — checking-object-integrity.html + sdkref feature-dataintegrity + 2026-04 five-additional-checksum-algorithms + IAM Workload Identity Federation
- Flink 2.3 Kinesis connector: nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kinesis/
- Flink 2.3 Pub/Sub connector: nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/pubsub/ + guarantees + FLINK-20625 + PR #32
- Flink native S3 FS (2026-06-26): flink.apache.org/2026/06/26/announcing-native-s3-fs/ + PR #27187
- Flink FileSink: nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/filesystem/
- Kinesis Data Streams: aws.amazon.com/kinesis/data-streams/ + service-sizes-and-limits + pricing
- Firehose: docs.aws.amazon.com/firehose (buffering, retry, limits, dynamic-partitioning, monitoring-with-cloudwatch-logs)
- GCS Pub/Sub notifications: docs.cloud.google.com/storage/docs/pubsub-notifications + reporting-changes
- GCS data validation: docs.cloud.google.com/storage/docs/data-validation + gcloud storage hash
- AWS Interconnect multicloud GA: aws.amazon.com/about-aws/whats-new/2026/04 — aws-announces-ga-AWS-interconnect-multicloud/
- Cost: aws.amazon.com/s3/pricing, cloud.google.com/network-tiers/pricing, egresscost.com
- Vault Core Data Loader / Migration API / Streaming API (cross-link): memorie org `mainframe-ibm-to-vault-core-json-migration-2026-cu-reconciliere` (portal.thoughtmachine.net Enablement Portal, NDA-gated)
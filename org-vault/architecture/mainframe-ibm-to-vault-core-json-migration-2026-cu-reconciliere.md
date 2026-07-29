---
title: Migrare mainframe IBM → Vault Core (Thought Machine) în JSON — soluții cu plusuri/minusuri și reconciliere banking-grade (2026)
tags: [org, mainframe, ibm-zos, db2, vsam, ims, cobol, copybook, vault-core, thought-machine, json, data-migration, cdc, reconciliation, banking, cobrix, precisely, iidr, qlik, kafka, '2026']
author: adrianb
sessions: []
created: '2026-07-29T08:15:15.171Z'
updated: '2026-07-29T08:15:15.171Z'
importanceScore: 0.92
---

## Executive Summary

## Prompt utilizat
1. esti un arhitect IT; 2. ai de gasit solutii pentru migrare date din mainframe IBM in aplicatie care importa datele in format json ; 3. aplicatia target este VaultCore de la ThoughtWorks ; 4. cauta pe net solutii de export date pentru mainframe IBM ; ; 5. cauta pe net solutiile de import date pentru VaultCore de la ThoughtWorks ; 6. creeaza o memorie org cu solutiiile gasite , iar la fiecare solutie gasita adauga plusurile si minusurile 7. absolut necesar este procesul de verificabilitate ( reconciliere) a datelor pentru care gasesti solutii ;

---

# Migrare mainframe IBM → Vault Core (JSON) — arhitectură cu reconciliere absolută

**Sumar executiv (DE CE, nu doar CE):** Sarcina a pornit cu o eroare de identificare a vendor-ului: "VaultCore de la ThoughtWorks" nu există — platforma este **Vault Core**, construită de **Thought Machine** (UK fintech); ThoughtWorks este o consultanță separată care uneori face implementare. Toate soluțiile de mai jos privesc Vault Core de la Thought Machine. Migrarea mainframe→JSON→Vault Core este o problemă banking-grade în care **reconcilierea este poarta de cutover (zero varianță neexplicată), nu un audit retrospectiv**; veridicitatea la cent este absolut necesară (BigDecimal/DecimalType/NUMERIC(p,s), niciodată double). Schema JSON exactă a Vault Core este **NDA-gated** (portal.thoughtmachine.net), deci orice design concret necesită acces Enablement Portal + un accelerator partener (Ikigai/Accenture/KPMG/GFT).

## Clarificare aplicație target

**CE ESTE VAULT CORE:** Platformă core banking cloud-native, cloud-agnostic (fără cod legacy), SaaS sau bank-hosted pe AWS/Azure/GCP/IBM/private. Microservicii pe Kubernetes + Istio, Kafka ca event/streaming backbone. Real-time, single, multi-tenant, event-sourced/immutable ledger (fără batch). Două produse flagship: Vault Core (ledger + product engine) și Vault Payments. 289M+ conturi migrate sau angajate.

**CUM INGESTEAZĂ DATE (JSON over Kafka):**
1. **Migration API (Kafka)** — ingeste resurse (clienți, conturi, produse) ca payload-uri JSON structurate "Resource Batch / Resource / Dependency Group".
2. **Data Loader** — serviciu care wrappuiește Migration API; orchestrează automat ordinea de load printr-un sistem de dependențe configurabil, astfel încât o bancă poate pune la coadă TOATE datele migrate deodată. Încarcă datele în stare **DORMANT**, menținute via delta updates, și activează conturi în **tranche** flexibile (decuplează load de go-live).
3. **Posting Migration API (Kafka)** — pentru mișcări financiare/solduri; payload-uri "Posting Instruction Batch" JSON.
4. **Core API (REST)** — pentru integrare BAU single-record (canale, CRM, operator UI), NU migrare bulk.
**Canal de reconciliere:** Streaming API (Kafka) emite **ResourceMigratedEvents** și evenimente de posting/accounting la fiecare load/activare — acesta este mecanismul event-driven de reconciliere.

**CE E NECERT / TREBUIE CONFIRMAT CU THOUGHT MACHINE (sub NDA):** Schema JSON field-level exactă, nume topicuri Kafka/partiționare, auth (probabil mTLS/OAuth), idempotency keys, throttling, DLQ, și decizia replay-vs-snapshot pentru posting-uri istorice NU sunt documentate public — sunt în spatele Enablement Portal (portal.thoughtmachine.net). Acceleratorii partener (KPMG, Ikigai, Accenture, GFT) sunt practic necesari; DIY pur e neobișnuit.

## Soluții propuse (cu plusuri și minusuri)

### Propunerea 1 — Composite cutover fazat: bulk backfill + CDC tail + activare dormant/tranche în Vault Core (arhitectura de referință)
**Abordare:** EXPORT snapshot bulk via Db2 HPU/UNLOAD (DB2) + DFSORT/z/OSMF sau BMC AMI Cloud Analytics (VSAM/secvențial) + extract de log IMS; apoi CDC tail log-based via Precisely Connect (cea mai largă acoperire: DB2+IMS+VSAM+secvențial) sau IBM IIDR (doar DB2) pentru a capta schimbările în fereastra dormant. TRANSFORM: decodere copybook-driven (Cobrix pe Spark pentru VSAM/secvențial; Precisely Apply Engine sau z/OS Connect pentru DB2/IMS) emit JSON cu COMP-3→DecimalType/BigDecimal (niciodată double). IMPORT: Data Loader încarcă resurse dormant în tranșe; Posting Migration API pentru solduri istorice; delta CDC ține datele dormant curente până la activarea tranșei.
**Plusuri:**
- Acoperă matricea sursă eterogenă completă (DB2 + VSAM + IMS + secvențial) combinând tool-uri bulk + CDC.
- Decuplează load de go-live (dormant + activare tranșe) — impact minim pe client, migrare fazată low-risk în loc de big-bang.
- Blast radius partiționat: o tranșe eșuată afectează o cohortă, nu toată cartea; rollback per-tranșe.
- Rata descrescătoare de break-uri de reconciliere per cohortă = metrică de maturitate; "fabrica de migrare" se îmbunătățește la fiecare tranșe.
- Satisface regulatorii (PRA SS3/19, ECB SREP, OCC) cu pachete de dovezi per-tranșe și parallel-running.
- Se potrivește cu orice arhitectură de referință banking 2026 documentată (ThoughtWorks seams, Google Dual Run, Mphasis zero-variance, Coretus Ledger Parity Kernel).
**Minusuri:**
- Cea mai complexă de engineerit și operat: tool-uri multiple (extractor bulk + motor CDC + transform + API-uri Vault Core + motor de reconciliere) trebuie integrate.
- Handoff-ul bulk→CDC e cel mai delicat — watermark overlap trebuie dimensionat ca nici o schimbare să nu se piardă sau se numere de două ori.
- Timeline total mai lung (parallel run 6-12 luni e realitatea banking, NU cele 3 luni pe care mulți le bugetează).
- Referințele cross-cohort (un client cu produse în două wave-uri) creează provocări de staleness — designul cohortelor e "cea mai consecventă decizie".
**Reconciliere:** Poarta engineered, nu audit retrospectiv. Per-wave, per-domeniu (client, cont, produs, GL): (1) control totals + hash totals între extract mainframe și starea încărcată în Vault Core; (2) field-level diff pe populație completă pentru câmpuri financiare; (3) cross-check fiecare ResourceMigratedEvent/posting event din Streaming API vs. snapshot mainframe + delta CDC din perioada dormant; (4) toleranță zero-varianță pentru solduri — orice diferență neexplicată nenulă oprește wave-ul; (5) poarta go/no-go e un feature flag (nu flag day), flip doar după N ferestre consecutive cu varianță zero. Markere quiet-point/LRSN aliniază snapshot-ul bulk cu CDC tail. Tooling partener (Ikigai Datadog dashboards, Accenture field-level write-back) drive repair/rollback.
**Pentru cine:** Bănci tier-one retail/commercial cu surse mainframe eterogene (DB2 + VSAM + IMS + secvențial) și mandat regulator pentru cutover fazat, evidence-based. Default-ul best-practice banking 2026.

### Propunerea 2 — Precisely Connect: CDC + ETL single-product la Kafka/JSON (cea mai largă acoperire single-vendor)
**Abordare:** EXPORT + TRANSFORM într-o singură familie de produse: Precisely Connect (fostul Syncsort/SQData) Capture/Publisher pe mainframe citește log-uri DB2 (IFI), log-uri IMS, VSAM (CICS VSAM Recovery Log pentru online, sau backup-file compare pentru batch), și fișiere secvențiale/copybook. Apply Engine pe target auto-generează JSON din descrierile sursă (copybook COBOL, DDL relațional, IMS DBD), convertind EBCDIC (cp1047)→UTF-8 (cp1208), mapând numericele la integer/decimal (unquoted), caracterele la string-uri quoted, emițând CDC before/after images cu operații I/U/D și timestamp-uri. Direct la S3/Iceberg sau la Kafka (Amazon MSK) cu JSON sau Avro. IMPORT: Vault Core Migration API + Data Loader consumă stream-ul JSON Kafka; Posting Migration API pentru mișcări financiare.
**Plusuri:**
- O singură familie de produse acoperă DB2 z/OS, IMS, VSAM și fișiere secvențiale/copybook — cea mai largă acoperire de surse mainframe printre tool-uri CDC, evitând integrarea multi-vendor.
- Config-driven: generare JSON din copybook/DDL fără să scrii cod de decodare; EBCDIC→UTF-8 și packed-decimal automat.
- CDC log-based + batch în același produs — suportă și initial load și sync ongoing în timpul cutover.
- Metadata de reconciliere bogată: row operation, row timestamp (YYYYMMDDHHMMSSffffff), transaction ID, transaction username, before/after images, dataset/schema/database/server/DBMS names.
- Arhitecturi de referință AWS și Azure documentate; exemple clienți (global payments provider → S3 Tables/Iceberg); AMI pe AWS Marketplace.
**Minusuri:**
- Licență comercială cu componentă de captură rezidentă pe mainframe — cost și ownership operațional pe z/OS.
- Arhitectură multi-component (Capture + Publisher + Listener + Replicator + Controller Daemon) — mai multe părți mobile decât Qlik.
- Mapping numeric intern opac: handling-ul exact al scale-ului COMP-3→JSON e engine-internal și TREBUIE validat (confirmă că scale e cariat prin, nu trunchat sau float-cast).
- Vendor lock-in pentru coloana vertebrală CDC; Precisely a trecut prin schimbări de ownership (Syncsort → Precisely → private equity).
- Recunoaștere de brand mai mică decât IBM/Qlik în RFP-uri greenfield mainframe-modernization.
**Reconciliere:** CDC before/after images și operation codes permit replay și verificare a fiecărei schimbări. Fiecare mesaj data poartă operation type, change sequence, timestamp, transaction ID — primitive puternice. Pentru siguranță la nivel de cent: validează că scale-ul packed-decimal e cariat (ex. S9(13)V9(2) COMP-3 → decimal cu scale 2), construiește reconciliere control-total (sumă câmpuri amount per batch din mainframe vs. sumă în target JSON, în integer minor units), și cross-check evenimente CDC vs. Vault Core ResourceMigratedEvents. Full Load + CDC mode permite reconcilierea snapshot-ului inițial apoi tracking la quiet point.
**Pentru cine:** Shop-uri mainframe eterogene (DB2 + VSAM + IMS + secvențial) care vor un singur produs comercial pentru bulk și CDC ongoing, cu cod custom de decodare minim, și care acceptă să valideze fidelitatea numerică internă a motorului.

### Propunerea 3 — Cobrix (Spark) + Db2 HPU + Data Loader: cale lakehouse open-source bulk (cea mai bună veridicitate, cost-sensitivă)
**Abordare:** EXPORT: Db2 HPU/UNLOAD pentru snapshot bulk DB2 (NOTĂ: HPU pe z/OS NU emite JSON — output e DSNTIAUL/DELIMITED/EXTERNAL/XML, deci pas de transform e obligatoriu); DFSORT TRAN=ETOA + z/OSMF REST Files API (sau SFTP/Connect:Direct) pentru VSAM și fișiere secvențiale FB/VB. TRANSFORM: Cobrix (AbsaOSS, Apache 2.0, v2.10.6 iunie 2026) pe Spark — `spark.read.format('cobol').option('copybook','x.cpy').load(path)` — parsează copybook, decodează EBCDIC peste 30+ code pages, gestionează REDEFINES, OCCURS DEPENDING ON, structuri nested, RDW/BDW variable-length, și emite JSON via `df.toJSON`. Spark DecimalType păstrează precision/scale nativ. IMPORT: Data Loader încarcă resurse dormant; Posting Migration API pentru solduri.
**Plusuri:**
- Cea mai transparentă și auditabilă decodare: Cobrix e open-source cu matrice de suport COMP/COMP-3/REDEFINES/ODO documentată — poți inspecta exact cum devin byte-ii valori.
- Spark DecimalType păstrează precision/scale nativ — ține coloanele money ca DecimalType(p,s) matching copybook V-implied scale, niciodată cast la double.
- Mecanismul `_corrupt_fields` (2.9.8+) scoate la suprafață record-urile undecodable în loc să le dropeze silențios — critic pentru vizibilitatea pierderii de date.
- Bidirecțional de la 2.9.0 (EBCDIC writer) → test de reconciliere round-trip: decode apoi re-encode și compară byte-ii.
- Fără conectivitate mainframe la decodare — doar copybook + fișier; fără vendor lock-in pentru data layer.
- Scalează liniar pe Spark (EMR Serverless, Databricks, Fabric); benchmark-uri acoperă fișiere 40GB cu 30M-640M record-uri.
**Minusuri:**
- Doar snapshot/batch — fără CDC; trebuie împerecheat cu un tool CDC (Precisely/IIDR/Qlik) pentru sync de cutover.
- Necesită cluster/platformă Spark — mai greu decât o librărie embedded pentru volume mici/tranzacționale.
- Prin design exclude cazuri patologice (nested ODO peste REDEFINES, RENAMES cu REDEFINES) — verifică pe copybook-urile tale.
- JSON via `df.toJSON` e reprezentare text Spark; consumatorii downstream trebuie să trateze coloanele decimal ca decimal, nu double, altfel reintroduci float loss.
- Tu deții contractul copybook→pipeline; fără gate de schema-evolution built-in (adaugă kobold-governance ca check CI).
**Reconciliere:** Excelentă pentru bulk. Perechează un control total row-count + checksum din mainframe (DFSORT STATS/DISPLAY: record count, hash key fields, sum amount fields în cents) cu același calculat peste DataFrame-ul decodat Cobrix, și asertă egalitate după load. `_corrupt_fields` face pierderea de date vizibilă. EBCDIC writer permite test round-trip (decode → re-encode → compară byte-ii) probând zero loss. Pentru cutover, împerechează cu tool CDC și reconciliază la CDC quiet point după RBA/LRSN snapshot-ului bulk. Cross-check vs. ResourceMigratedEvents.
**Pentru cine:** Bănci cost-sensitivă sau cu capabilitate Spark/data-engineering care fac migrare bulk back-book one-time, unde decodarea transparentă copybook-driven și precizia DecimalType sunt non-negociabile. Împerechează cu tool CDC pentru cutover.

### Propunerea 4 — IBM IIDR CDC + Data Gate for Confluent (shop-uri DB2-dominante, IBM-stack)
**Abordare:** EXPORT: IBM InfoSphere Data Replication (IIDR) CDC pentru Db2 for z/OS citește log-urile de recovery DB2 (nu datele din tabele) pentru a capta INSERT/UPDATE/DELETE cu impact sursă minim (near-real-time, 2-5s latență). Din IDR 11.4 started-task log reader e strategic (stored-procedure log reader deprecated feb 2026). Pentru calea strategică 2026, **IBM Data Gate for Confluent** (anunțat mai 2026, post-aciziția IBM a Confluent) streamează DB2 z/OS direct la Confluent via connector Kafka Connect nativ cu până la 96% zIIP offload, făcând și snapshot inițial și CDC continuu. TRANSFORM: KCOP custom processors (ex. KcopJsonFormatIntegrated) scriu JSON la Kafka fără schema registry; Avro de asemenea. IMPORT: Vault Core Migration API + Data Loader consumă stream-ul JSON Kafka.
**Plusuri:**
- CDC cu impact cel mai mic: citește log-uri, nu tabele; near-real-time 2-5s latență — ideal pentru migrare zero-downtime.
- Acoperă sync ongoing în cutover, nu doar initial load.
- Schema evolution (ADD/ALTER/DROP COLUMN) gestionată automat din build 5603.
- Până la ~96% procesare zIIP-eligible pe calea Data Gate for Confluent — cost minim general-processor.
- IBM numit Leader în Gartner MQ 2025 Data Integration; cele mai puternice referințe banking (Garanti BBVA streamează Db2 z/OS → Exadata/Kafka).
- Data Gate suportă și snapshot inițial și CDC continuu într-un produs — acoperă tot ciclul de migrare.
**Minusuri:**
- DOAR DB2-for-z/OS — NU acoperă direct VSAM, IMS DB sau fișiere secvențiale; necesită tool companion (DFSORT/Cobrix/Precisely) pentru sursele non-DB2.
- Necesită DATA CAPTURE CHANGES pe fiecare tabelă în scope + SYSIBM.SYSTABLES.
- Tuning operațional non-trivial: WLM NUMTCB~40, log-cache sizing, limită ~1000-table subscription, ~70 subscriptions/instance below-bar.
- Log retention trebuie dimensionat (tipic 5-10 zile) altfel CDC rate schimbări; DDL structural necesită REORG + Update Source Table Definition.
- IBM Event Streams e deprecated; shop-uri pe Event Streams trebuie migrate la Confluent — disruptare 2026. Data Gate for Confluent e nou (mai 2026) cu referințe producție limitate la lansare.
**Reconciliere:** CDC emite transaction IDs, change sequence numbers, timestamp-uri în fiecare mesaj; reconcilierea în cutover folosește un marker 'consistent point'/quiet-point ca target-ul să fie validat vs. sursa până la un log RBA/LRSN specific. Row-count + key-range checksums la quiet point confirmă paritatea înainte de switch aplicație. Cross-check vs. ResourceMigratedEvents. Împerechează cu bulk unload (Db2 HPU) pentru snapshot inițial și reconciliază la CDC quiet point după RBA/LRSN unload-ului.
**Pentru cine:** Bănci IBM-stack unde DB2 for z/OS e sursa dominantă și sursele non-DB2 (VSAM/IMS/secvențial) sunt un subset minor, gestionat separat. Împerechează cu Cobrix sau Precisely pentru datele non-DB2.

### Propunerea 5 — Qlik Replicate CDC la Kafka/JSON (GUI-driven, claim-uri largi de surse)
**Abordare:** EXPORT: Qlik Replicate (fostul Attunity) log-based CDC din Db2 for z/OS via componenta R4Z (Replicate for z/OS) furnizată de Qlik, folosind UDTF și sesiuni IFI. Suportă Full Load (REFRESH) + INSERT/UPDATE/DELETE CDC. Qlik listează de asemenea IMS/DB, VSAM, RMS ca surse mainframe suportate (validează per tip sursă — DB2 z/OS e calea cea mai matură; IMS/VSAM pot folosi captură file-based nu log-based). TRANSFORM: output JSON sau Avro nativ la Kafka (și Confluent, Azure Event Hubs, AWS Kinesis) — fără transform separat pentru a ajunge la JSON. Mesaje metadata (structură tabelă, proprietăți coloană, schema Avro) și mesaje data (operation type, change sequence, timestamp, transaction ID, change mask, before/after data). Override-uri CCSID-to-character-set pentru EBCDIC. IMPORT: Vault Core Migration API + Data Loader consumă stream-ul JSON Kafka.
**Plusuri:**
- Un singur produs acoperă DB2 z/OS CDC ȘI listează IMS/DB, VSAM, RMS ca surse — cel mai larg claim de acoperire mainframe printre tool-uri CDC (validează per sursă).
- Output JSON sau Avro nativ la Kafka — fără pas de transform separat pentru JSON.
- Ordine tranzacțională, before/after images, change sequence și transaction ID în fiecare mesaj — primitive puternice de reconciliere.
- Qlik numit Leader în Gartner MQ 2025 Data Integration (al 10-lea an consecutiv); GUI-driven și larg implementat.
- Suportă Full Load + CDC într-un task — acoperă initial load și sync ongoing.
**Minusuri:**
- R4Z e componentă licențiată Qlik instalată pe z/OS — cost și amprentă mainframe adițională.
- Claim-ul de acoperire cea mai largă (IMS/VSAM) necesită validare per tip sursă — DB2 z/OS e calea matură; IMS/VSAM pot folosi captură file-based nu log-based.
- Schema envelopei de mesaj proprietară ('atMSG' magic, Qlik envelope) — consumatorii au nevoie de Avro SDK sau cunoștințe parsing JSON.
- Artefacte legacy 'Attunity' persistă în path-uri/installere (ex. makeconv.exe) — oarecare suprafață de tech-debt.
- Licență comercială — nu open-source; preț scalează cu surse/target-uri/throughput.
**Reconciliere:** Fiecare mesaj data poartă operation type, change sequence, timestamp, transaction ID; Full Load + CDC mode permite reconcilierea snapshot-ului inițial apoi tracking la quiet point. Before/after images permit validare row-level diff vs. sursă. Cross-check vs. ResourceMigratedEvents. Pentru siguranță la cent, validează că output-ul numeric JSON cară scale decimal complet (nu float-cast) și construiește reconciliere control-total în integer minor units.
**Pentru cine:** Shop-uri care vor un tool CDC GUI-driven, larg implementat, cu output JSON-to-Kafka nativ, unde DB2 z/OS e sursa primară și acoperirea IMS/VSAM e o preocupare secundară, validată per caz.

### Propunerea 6 — z/OS Connect + IBM Open Enterprise SDK for Kafka (streaming on-platform, nivel aplicație)
**Abordare:** EXPORT + TRANSFORM on-platform: z/OS Connect Enterprise Edition (docs actualizate feb 2026) oferă mapping COBOL→JSON schema (PIC X→string, COMP/COMP-3→number cu multipleOf derivat din decimal implied, OCCURS DEPENDING ON→array variable-length). IBM Open Enterprise SDK for Apache Kafka (ian 2026: adăugat API-uri PL/I producer/consumer alături de COBOL și C/C++) permite aplicațiilor mainframe să publice nativ evenimente Kafka, cu un utilitar de transformare care mapează copybook COBOL la JSON. Datele nu părăsesc z/OS până la publicare. IMPORT: Vault Core Migration API + Data Loader consumă stream-ul JSON Kafka.
**Plusuri:**
- Datele nu părăsesc z/OS până la publicare — latență mică, fără server de extracție separat, expunere de securitate minimă.
- IBM-supported, current (docs feb 2026), cu mapping COBOL↔JSON autoritativ documentat incluzând OCCURS DEPENDING ON ca array variabil (un caz pe care multe tool-uri open îl greșesc).
- Open Enterprise SDK acoperă acum COBOL, C/C++, și PL/I (ian 2026) — limbajele mainframe principale.
- Permite emisie incrementală, nivel-aplicație, de evenimente JSON la Kafka fără bulk unload.
**Minusuri:**
- Cale de modificare a aplicației: necesită schimbarea/wrapping programelor COBOL/PL/I pentru a apela Kafka SDK sau z/OS Connect API — NU e un extract non-invasiv; e modernizare de aplicație, nu un utilitar DBA.
- NU e tool de migrare bulk — cel mai bun pentru emisie ongoing/incrementală, nu initial load de fișiere terabyte-scale.
- Mapping COBOL→JSON al z/OS Connect e API-oriented (request/response), nu unload batch high-throughput; PIC-uri decimal reale (explicit) NU sunt suportate — doar câmpurile implied-decimal (V) se mapează curat.
- IBM Event Streams e deprecated; target Kafka strategic e acum Confluent (IBM-owned, martie 2026).
- Complexitate cea mai mare: necesită dezvoltare COBOL/PL/I și integrare z/OS Connect sau Kafka SDK.
**Reconciliere:** Nivel aplicație: fiecare eveniment publicat poartă un transaction/sequence ID; consumatorii ack offsets. Pentru reconciliere migrare, emite un eveniment marker 'snapshot complete' și compară consumed record count cu un count mainframe-side (ex. DFSORT COUNT/DISPLAY). Pentru garanții la cent, verifică că JSON consumer interpretează multipleOf ca constrângere decimală și nu coercează la double. Cel mai bun pentru record-uri request/response unde programul sursă validează deja datele; pentru reconciliere bulk preferă Cobrix/Precisely.
**Pentru cine:** Shop-uri care deja service-enable programelor COBOL/PL/I via z/OS Connect și vor să emită evenimente JSON la Kafka nativ din z/OS, pentru extracție incrementală/ongoing. NU recomandat ca cale primară de migrare bulk.

## Secțiunea de reconciliere (banking-grade, absolut necesară)

Reconcilierea NU e un checkpoint final — e un control continuu țesut prin tot ciclul de migrare, și poarta engineered care autorizează fiecare decizie de cutover. Regula operațională: **ZERO VARIAȚĂ NEEXPLICATĂ** pentru solduri financiare — orice diferență nenulă neexplicată e un potențial defect de integritate și oprește wave-ul. Nici o tehnică singulară nu dovedește totul — trebuie stratificată.

**LAYER 0 — CANONICALIZARE (precondiția care face valide toate celelalte straturi):** Înainte de orice comparație, transformă ambele părți într-o formă canonică comună. E componenta de cea mai mare expertiză și risc — "discrepanțele" din săptămâna 1 sunt aproape mereu bug-uri de canonicalizare, nu bug-uri de date.
- COMP-3 packed decimal e BINARY, nu text — niciodată prin tabelă EBCDIC→ASCII. Unpack semantic la DECIMAL(p,s)/BigDecimal cu precision/scale explicit din COBOL PIC S9(m)V9(n) (map la DECIMAL(m+n, n); în JSON tip 'number' cu minimum/maximum/multipleOf=10^-n).
- Sign nibbles: acceptă C (pozitiv) și D (negativ); respinge F ca non-standard cu excepția convenției COBOL vs PL/I — mismatch-uri de sign-nibble cauzează eșecuri reale (IBM APARs PH61848, PI68592).
- Zoned-decimal sign overpunch în ultimul byte e mutilat de conversie naivă (EBCDIC '123D' → ASCII '123M', nu '123t') — decode, nu translate.
- Conversie text EBCDIC→ASCII cu awareness de signed-overpunch și collation-order.
- Date normalizate la un singur format ISO cu timezone explicit (DB2 TIMESTAMP WITH TIMEZONE).
- Semantică NULL/zero/spații: definește dacă NULL, 0, și spații sunt echivalente per câmp — PostgreSQL respinge nibble-uri packed-decimal invalide (>9) din poluare istorică, necesitând path de carantină.
- Sortează rândurile pe business keys ca diff-urile să fie deterministice.
Un singur strat de canonicalizare servește control totals, field diffs, și hash totals. Versionează-l și ține-l consistent prin fiecare stadiu ETL altfel comparațiile downstream sunt fără sens.

**LAYER 1 — CONTROL TOTALS / HASH TOTALS (gate agregat ieftin):** Compute per partiție (branch/produs/dată/fișier) pe ambele părți: record counts, sumă câmpuri monetare LA CENT cu BigDecimal scale explicit, și hash totals (SHA-256 peste o reprezentare canonică a rândului). Rulează pe populația completă la fiecare stadiu ETL. Prinde divergență grosieră instant. Gate necesar primul, NICIODATĂ suficient singur — două rânduri greșite se pot anula la același total.

**LAYER 2/3 — FIELD-LEVEL / ROW-LEVEL DIFF (baseline banking-grade):** Extrage ambele părți la formă canonică, sortează pe business keys, outer-join pe key, marchează rânduri missing/extra/mismatched. Pentru mismatched, comparație per-câmp cu reguli de toleranță: EXACT pentru money și keys, WINDOWED pentru timestamp-uri (±1s), MAPPED pentru enumerări recodate. POPULAȚIE COMPLETĂ e default-ul banking pentru solduri și câmpuri regulatorii; samplingul statistic se folosește doar pentru date reference/lookup low-risk sau ca smoke test zilnic rapid, NICIODATĂ ca dovadă singulară pentru câmpuri financiare.

**LAYER 4 — PARALLEL RUN / SHADOW RUN (reconciliere comportamentală):** Rulează mainframe (system of record) și Vault Core în paralel 6-12 luni (perioada realistă banking mainframe, NU 3 luni — 42% din proiecte depășesc timeline). Alimentează ambele cu aceleași tranzacții, compară output-uri zilnic: interest, fee logic, GL/trial balance, output-uri regulatorii (Basel III RWA, GAAP/IFRS). Shadow run (30-60 zile per cohortă): tranzacțiile rulează pe legacy core ca SoR dar sunt replayed pe Vault Core; output-uri comparate și divergențe rezolvate înainte de flip. Cea mai persuasivă dovadă pentru un regulator. Prinde divergență comportamentală (discrepanțe de aritmetică decimală între COBOL și limbaje moderne) pe care diff-urile statice nu pot. Niciodată nu împărți un client pe ambele core-uri.

**RECONCILIERE MONETARĂ PRECISION-SAFE LA CENT (stratul care face 'la cent' literal adevărat):**
- Aritmetică decimală cu scale explicit (BigDecimal în Java, NUMERIC(p,s) în SQL, DecimalType în Spark), NICIODATĂ floating point (IEEE-754 double/float).
- Framework IEEE MSMP 2026: 8-10 zecimale pentru computație, 4-8 pentru ledger posting, 2 pentru prezentare client, cu patru invarianți de corectitudine (deterministic posting, ledger conservation, presentation consistency, fractional carry-forward).
- Prevenirște trei moduri de eșec: rounding timpuriu (round doar rezultatul final, niciodată intermediarele), boundary truncation (câmpuri intermediare au nevoie de ≥6 zecimale mai mult decât rezultatul final), aggregare nedeterministică (definește o ordine fixă de sumare).
- Pentru divergență de timing interest-posting (sursa calculează per-zi cu rounding per-zi; target calculează balance*rate/365*days într-o operație — matematic echivalent, computațional diferit): track diferențele cumulative de rounding într-un rounding adjustment register și postează netul la un interest suspense account (GlobalBank BAL-CALC fix: $1,247.33 discrepanță → $0.00).
- Pentru migrare între sisteme cu convenții diferite precision/rounding: postează o intrare one-time true-up adjustment (Cornerstone: $847.63 true-up). Folosește Banker's Rounding (round half to even) și un rounding adjustment/suspense account să absoarbă rezidualul ca books să balanseze exact.

**DUAL-WRITE / TRANSACTIONAL OUTBOX (reconciliere continuă near-real-time în cutover):** În cutover, scrie fiecare tranzacție la ambele legacy și target via transactional outbox: rândul legacy și o intrare outbox commit atomic într-o tranzacție, un proces async fan-uiește evenimentul la target, livrarea e idempotent via ON CONFLICT DO UPDATE key-ed pe idempotency key. Comparare rolling checksum peste rânduri recent modificate detectează drift; drift rate susținut >0.01% declanșează alertă. Cutover e eveniment gated cu criterii go/no-go (lag sub percentile budget, reconciliation error rate aproape zero, shadow reads clean); rollback e configuration flag pentru că legacy rămâne autoritativ până la flip.

**IMMUTABLE AUDIT LOG / EVENT LOG (dovadă tamper-evidentă de transfer + match):** Event store append-only unde fiecare transfer și match e un eveniment numit, timestamped (record extracted, transformed, loaded, matched, break raised, break resolved). Corecțiile sunt reversing entries, niciodată in-place updates. Hash-chain intrări prin log și semnează exporturi pentru regulatori. În arhitecturi event-sourced (Vault Core e event-sourced), event log-ul ESTE audit trail. Log-ul dovedește: (1) fiecare record sursă a fost extras, (2) fiecare record target a fost încărcat, (3) fiecare a fost match-at cu confidence score și rațional, (4) fiecare break a fost investigat și rezolvat cu decizie documentată. Design audit trail ÎNAINTE de migrare, nu după.

**IDEMPOTENT ȘI RESUMABLE (loop fix-and-rerun):** Idempotency keys pe fiecare record transferat și fiecare run de reconciliere. Watermarks și per-partiție checkpoints (high-water-mark pe business date / transaction sequence / batch ID). Log-sequence positioning (DB2 log RBA/LSN / outbox offset) pentru delta capture CDC după backfill inițial. Delta-only re-diff: după un fix, re-extrage doar rânduri modificate de la ultimul checkpoint verde. Cutover gates exigă 'reconciliation error rate aproape zero' înainte de avansare.

**CANAL DE RECONCILIERE VAULT CORE-SPECIFIC:** Streaming API emite ResourceMigratedEvents și evenimente posting/accounting la fiecare load/activare — ACESTA e canalul de reconciliere. Pattern: (1) menține un snapshot frozen mainframe + delta capture (CDC) în fereastra dormant; (2) consumă fiecare ResourceMigratedEvent/posting event din Streaming API; (3) cross-check accepted state al fiecărui eveniment vs. record mainframe corespondent la nivel cont și sold; (4) surface mismatch-uri la Datadog/field-level write-back (Ikigai streamează la Datadog pentru dashboard-uri/alerte live; Accenture face field-level write-back vs. quality thresholds built-in Vault Core); (5) drive repair/rollback via tooling partener (Ikigai: repair account parameters/balance positions, sau zero-out și close și re-load). Reconciliază evenimente posting/accounting la poziția de sold nivel-cont (opening balance + sumă posting-uri migrate == mainframe closing balance) și la nivel tranzacție individual unde e disponibil. Folosește time-travel/time-series testing pentru backtest posting-uri migrate vs. outcome-uri istorice cunoscute pe mainframe înainte de cutover.

**RECONCILIERE CA POARTĂ DE CUTOVER (pattern-ul Mphasis zero-variance, concret):** O SQL recon view (recon.parallel_run_diff) diferențiază output COBOL vs Java/Vault Core cu ABS(source_value - target_value) < 0.01 pentru MATCH/MISMATCH; când view-ul returnează zero rânduri MISMATCH peste o fereastră batch completă, un AWS AppConfig feature flag (modernization.cutover.enabled) flip — poarta e feature flag, nu flag day. Threshold-uri go/no-go pre-agreed cu rollback triggers time-boxed și decision makers numiți. Dovada de reconciliere trebuie să demonstreze completențe și acuratețe la niveluri operațional semnificative: counts, control totals, balances, customer positions, product attributes, output-uri reporting downstream. Unde rămân excepții, trebuie deținute explicit, time-bounded, și aprobate la nivelul potrivit.

## Recomandare pe scenarii (veridicitate + simplitate ponderate, reconciliere absolută)

**SCENARIUL 1 — migrare bulk back-book one-time (fără sync ongoing):** Propunerea 3 (Cobrix). Export via Db2 HPU (DB2) + DFSORT/z/OSMF (VSAM/secvențial), transform via Cobrix pe Spark (copybook-driven, DecimalType păstrează precision/scale nativ, `_corrupt_fields` surface data loss, EBCDIC writer permite round-trip byte comparison), import via Data Loader (dormant + tranche activation). Reconciliază cu control totals + field-level diff populație completă + round-trip byte test, cross-check vs. ResourceMigratedEvents. Cea mai transparentă, auditabilă decodare la cost cel mai scăzut, cu cea mai puternică garanție de precizie. Adaugă kobold-governance ca gate CI pentru copybook drift. Cea mai bună veridicitate-per-dolar.

**SCENARIUL 2 — bulk + CDC ongoing (cutover zero/low-downtime, surse eterogene):** Propunerea 2 (Precisely Connect). Singura familie single-product care acoperă DB2 + IMS + VSAM + secvențial cu bulk și CDC log-based, emițând JSON/Avro la Kafka cu before/after images și transaction IDs. Import via Migration API + Data Loader. Reconciliază la CDC quiet point cu control totals + field-level diff, cross-check vs. ResourceMigratedEvents. CRITIC: validează că Apply Engine cară scale COMP-3 ca decimal (nu float-cast) înainte de commit — construiește control-total reconciliation în integer minor units ca acceptance test. Calea single-vendor cea mai simplă care acoperă matricea eterogenă cu CDC.

**SCENARIUL 3 — cutover fazat, core retail tier-one (default best-practice banking):** Propunerea 1 (composite fazat). Bulk backfill (Cobrix pentru VSAM/secvențial + Db2 HPU pentru DB2) + CDC tail (Precisely Connect pentru acoperire largă, sau IBM IIDR dacă DB2-dominant) + activare dormant/tranche + Posting Migration API pentru solduri. Reconcilierea e poarta engineered zero-varianță per wave: control totals → field-level diff → parallel run → cross-check ResourceMigratedEvents → feature-flag flip doar după N ferestre consecutive cu varianță zero neexplicată. Bugetează 6-12 luni parallel run, nu 3. Cea mai complexă dar singura care credibil gestionează un core retail tier-one cu surse mainframe eterogene și mandat regulator. Se potrivește cu orice arhitectură de referință banking 2026 documentată.

**REGULI CROSS-CUTTING (toate scenariile):**
- BigDecimal/DecimalType/NUMERIC(p,s) pentru toate câmpurile monetare, NICIODATĂ double/float. Mapează scale COMP-3 explicit din decimal V-implied.
- Construiește stratul de canonicalizare și audit log immutable ÎNAINTE de migrare, nu după.
- Tratează primele break-uri de reconciliere ca cele mai valoroase bug-uri — sunt de obicei erori de canonicalizare, nu erori de date.
- Cere zero discrepanțe neexplicate înainte de a decomisiona mainframe-ul.
- Angajează un accelerator partener Thought Machine (Ikigai, Accenture, KPMG, GFT) — schema NDA-gated și reconcilierea async event-based necesită practic tooling partener.
- Adaugă un gate de governance copybook-drift (kobold-governance în CI) ca un resize/offset shift silențios să nu corupă stream-ul fără eroare.

**BOTTOM LINE:** Pentru mainframe eterogen (DB2 + VSAM + IMS + secvențial) migrând la Vault Core cu reconciliere ca cerință absolută: Propunerea 1 (composite fazat) pentru bănci tier-one, Propunerea 2 (Precisely Connect) pentru cea mai simplă cale single-vendor CDC cu toate tipurile de surse, sau Propunerea 3 (Cobrix) pentru migrare bulk one-time cu decodare open-source transparentă și precision-safe. În toate cazurile, reconcilierea e poarta de cutover zero-varianță, nu un audit retrospectiv.

## Note critice 2026 (gotchas, limite, ce trebuie confirmat cu Thought Machine)

1. **IDENTITATE TARGET:** Platforma e "Vault Core" de Thought Machine — NU "ThoughtWorks VaultCore." ThoughtWorks e consultanță separată. Confirmă vendor-ul corect și acces Enablement Portal (portal.thoughtmachine.net) înainte de orice design de integrare.
2. **VAULT CORE NDA-GATED (confirmă sub NDA înainte de commit):** Schema JSON field-level exactă pentru Resource Batch/Resource/Dependency Group și Posting Instruction Batch. Nume topicuri Kafka, partiționare, auth (probabil mTLS/OAuth), idempotency keys, throttling, DLQ. Decizia replay-vs-snapshot pentru posting-uri istorice (per-bank, nu rezolvată de docs publice). Dacă Data Loader orchestrează dependency graph-ul tău specific. Semantică concurrency/at-least-once (Ikigai referențiază at-least-once cu DLQ-uri, dar contractul exact de dedup/idempotency consumer-side trebuie confirmat). Fără detalii publice pe gRPC/GraphQL; REST Core API + Kafka migration/streaming e norma 2026.
3. **DB2 HPU PE z/OS NU EMITE JSON:** Gap critic 2026. Output e DSNTIAUL/DELIMITED/VARIABLE/USER/EXTERNAL/INTERNAL/XML. JSON există doar pe distributed (LUW) Optim HPU. Pas de transform downstream la JSON e OBLIGATORIU.
4. **GAP-URI DE DECODARE COPYBOOK PE TOOL:**
   - AWS open-source mainframe-data-utilities: OCCURS DEPENDING ON NU e suportat (pe backlog); REDEFINES doar pentru group items; îmbunătățiri COMP/COMP-3 pending. NU te baza pe ele pentru copybook-uri hard cu ODO — folosește Cobrix sau Precisely.
   - copybook-rs: doar fixed-width — fără RDW/BDW variable-block framing; exclude nested ODO și ODO-over-REDEFINES.
   - Cobrix: exclude nested ODO peste REDEFINES, RENAMES cu REDEFINES — verifică pe copybook-urile tale.
   - z/OS Connect: PIC-uri decimal reale (explicit) nesuportate — doar implied-decimal (V) se mapează curat.
   - DFSORT TRAN=ETOA: traducere 1:1 byte table doar — NU decodează semantică copybook (COMP-3, COMP, REDEFINES, OCCURS sunt opace); EBCDIC multi-byte (DBCS, CCSID 937/1388) necesită tabele custom.
5. **IBM EVENT STREAMS E DEPRECATED:** Shop-uri pe Event Streams trebuie migrate la Confluent (IBM a achiziționat Confluent, închis martie 2026). IBM Data Gate for Confluent (anunțat mai 2026) e noua cale strategică Db2-z/OS→Kafka CDC — dar e produs nou cu referințe producție limitate la lansare. Validează maturitatea.
6. **AWS MAINFRAME MODERNIZATION MANAGED RUNTIME ÎNCHIS NOI CLIENȚI (noi 2025):** Userii noi folosesc Self-Managed Experience sau AWS Transform for mainframe (rebrandit martie 2026, code transformation acum gratuit).
7. **IBM IDR CONSTRAINTS OPERAȚIONALE:** Necesită DATA CAPTURE CHANGES pe fiecare tabelă în scope + SYSIBM.SYSTABLES; log retention dimensionat (5-10 zile) altfel CDC rate schimbări; limită ~1000-table subscription, ~70 subscriptions/instance below-bar; stored-procedure log reader deprecated feb 2026 — deployments noi trebuie started-task log reader; DDL structural necesită REORG + Update Source Table Definition.
8. **QLIK IMS/VSAM CLAIM-URI NECESITĂ VALIDARE:** DB2 z/OS e calea log-CDC matură; IMS/VSAM pot folosi captură file-based nu log-based. Validează per caz.
9. **PRECISELY FIDELITATE NUMERICĂ E ENGINE-INTERNAL:** Mapping exact COMP-3→JSON scale e opac — TREBUIE validezi că scale packed-decimal e cariat (ex. S9(13)V9(2) COMP-3 → decimal scale 2) și că Apply Engine nu trunchiază sau float-cast. Construiește control-total reconciliation în integer minor units ca acceptance test.
10. **DURATĂ PARALLEL RUN:** Bugetează 6-12 luni pentru parallel run banking mainframe, NU 3 luni — 42% din proiecte depășesc timeline. Big-bang cutover e anti-pattern pentru core-uri retail tier-one (TSB 2018 e eșecul canonic: £48.65M amendă FCA/PRA, 1.9M clienți blocați).
11. **NICIODATĂ NU ÎMPĂRȚI UN CLIENT PE DOUĂ CORE-URI:** Un core trebuie să fie system of record pentru un client la un moment dat. Designul cohortelor e "cea mai consecventă decizie" — slice pe risk profile (nu pe count), ordonează simplest-first.
12. **COPYBOOK DRIFT E KILLER-UL SILENT:** Fără gate governance (kobold-governance în CI, sau Avro Schema Registry pe z/OS pentru streaming), un resize/offset shift silențios corupe stream-ul fără eroare. Cea mai comună cauză de pierdere silențioasă la nivel de cent în timp. Tratează un diff gate care flag-uiește câmpuri Resized/Retyped ca obligatoriu, forțând regenerarea binding-ului și re-validarea scale înainte ca noul layout să meargă live.
13. **ROLLBACK E REAL DOAR DACĂ E REPETAT:** Rollback netestat e un slide, nu un control. Cere multiple mock cutovers full-scale care măsoară durata vs plan, pass rate reconciliere per domeniu, count intervenții manuale, și eficacitate incident-response. Rollback triggers time-boxed și legate de outcome-uri reconciliere, cu decision makers numiți.
14. **DECOMMISSIONING E WORKSTREAM SEPARAT:** Majoritatea case studies se termină la 'cutover reușit', nu 'mainframe oprit'. Retenție regulatorie, interface readers downstream, și pipeline-uri reporting care încă citesc date mainframe sunt adesea uitate. Planifică decommissioning explicit.
15. **IBM DB2 NATIVE JSON PE z/OS E LIMITAT:** JSON_TABLE e UDF limitat 2-coloană (nu SQL-standard JSON_TABLE ca pe Db2 LUW); funcții JSON publishing (JSON_OBJECT, JSON_ARRAY) încă absente pe z/OS (idea DB24ZOS-I-1113 'Planned for future release'). BSON storage adaugă un strat binar; tooling în afara Db2 vede BLOB. NU e o soluție generală mainframe-file — ajută doar când sursa e deja DB2 relațional.
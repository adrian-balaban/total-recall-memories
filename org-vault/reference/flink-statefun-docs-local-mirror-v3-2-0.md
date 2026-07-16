---
title: Flink StateFun docs — local mirror (v3.2.0)
tags: [org, flink, flink-statefun, stateful-functions, reference, local-mirror, docs]
author: adrianb
sessions: []
created: '2026-07-08T09:11:40.156Z'
updated: '2026-07-08T09:11:40.156Z'
importanceScore: 0.6
---

## Executive Summary

## Executive summary

Full local mirror of the **Apache Flink Stateful Functions v3.2.0** docs, on disk for when you need complete text, code samples, or diagrams beyond the distilled org memories. Crawled with the `site-to-markdown-images` skill (image-preserving site→markdown crawler); pages cross-link to each other locally and image links are relative, so it browses offline.

## Location

- Root: `/home/adrianb/.total-recall/downloads/flink-statefun-docs-stable/`
- Pages root: `…/flink-statefun-docs-stable/flink/flink-statefun-docs-release-3.2/` (mirrors URL path `nightlies.apache.org/flink/flink-statefun-docs-release-3.2/`)
- Manifest (URL→file map, image counts, links_localized, skipped/errors): `…/flink-statefun-docs-stable/manifest.json`
- Images: `…/flink-statefun-docs-stable/images/flink/flink-statefun-docs-release-3.2/`

## Contents (24 .md + 15 images, 1.5 MB)

- Concepts: `docs/concepts/{application-building-blocks,logical,distributed_architecture}/index.md`
- SDKs: `docs/sdk/{js,python,java,golang,flink-datastream,appendix}/index.md`
- Modules: `docs/modules/{overview,http-endpoint,embedded}/index.md` + `docs/modules/io/{overview,apache-kafka,aws-kinesis,flink-connectors}/index.md`
- Deployment: `docs/deployment/{overview,configurations,metrics,state-bootstrap}/index.md`
- API: `api/java/index.md` · versions: `versions/index.md` · root: `index.md`
- Concept diagrams: `images/…/fig/concepts/{arch_overview,arch_components,arch_funs_remote,arch_funs_colocated,arch_funs_embedded,statefun-app-ingress,statefun-app-functions,statefun-app-state,statefun-app-fault-tolerance,statefun-app-egress,address}.svg` + `fig/dispatch.png`.

## Crawl provenance

- Source: `https://nightlies.apache.org/flink/flink-statefun-docs-release-3.2/` (the `…/stable/` URL is an alias landing page; real tree is `release-3.2`).
- Flags: `--unlimited --restrict-to-subpath --ignore-robots --delay 2.0` (Apache robots.txt blocks generic bots; bypass was an explicit choice for public docs mirroring — the skill respects robots by default).
- Result: 26 pages fetched (24 distinct .md — `/versions` and `/versions/` redirect-collide to one file), 15 unique images, 618 page-to-page links localized to local .md.

## When to read the mirror vs the memories

The distilled org memories cover the mental model. Read the mirror pages for: full SDK code (Python/Java/JS/Golang function definitions, state registration, timers), per-IO connector options (Kafka/Kinesis/flink-connectors consumer/producer configs), full ConfigMap YAML, metrics list, and state-bootstrap procedures. `grep` the mirror or follow the cross-links from the knowledge memories.

Cross-linked from: [[flink-statefun-architecture-logical-co-location-physical-separation]], [[flink-statefun-building-blocks-ingress-functions-state-egress]], [[flink-statefun-logical-functions-addressing-lifecycle]], [[flink-statefun-modules-module-yaml-format-component-kinds]], [[flink-statefun-deployment-k8s-docker-image-configmaps]].
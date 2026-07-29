---
title: Fake architecture org-sync hook live test delete me 114b
tags: [org, fake, test, delete-me]
author: adrianb
sessions: []
created: '2026-07-29T20:28:51.984Z'
updated: '2026-07-29T20:28:51.984Z'
importanceScore: 0.1
---

## Executive Summary

Disposable fake org memory to verify the org-sync PostToolUse hook fires on a correctly-routed `store_memory` (no explicit key, `org` tag, `category:architecture`) and auto-pushes to `origin/knowledge`. No real content — safe to delete.

**Why:** end-to-end check that the matcher fix (v1.1.3) works live; the redundant-`git add` allowFail fix (v1.1.4) only bites on delete, so a store should push even on the stale installed 1.1.3.

**How to apply:** delete this memory after confirming it landed on `origin/knowledge`.
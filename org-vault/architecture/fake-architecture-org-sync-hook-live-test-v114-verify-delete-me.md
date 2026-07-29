---
title: Fake architecture org-sync hook live test v114 verify delete me
tags: [org, fake, test, delete-me]
author: adrianb
sessions: []
created: '2026-07-29T20:36:45.471Z'
updated: '2026-07-29T20:36:45.471Z'
importanceScore: 0.1
---

## Executive Summary

Disposable fake org memory for the v1.1.4 live hook test — verify the PostToolUse org-sync hook fires on a correctly-routed `store_memory` (no explicit key, `org` tag, `category:architecture`) and auto-pushes to `origin/knowledge`. No real content — safe to delete.

**Why:** end-to-end confirmation that the matcher fix (v1.1.3) + the redundant-`git add` allowFail fix (v1.1.4) both work live with 1.1.4 installed.

**How to apply:** delete this memory after confirming it landed on `origin/knowledge`.
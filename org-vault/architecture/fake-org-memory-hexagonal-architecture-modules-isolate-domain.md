---
title: 'Fake org memory: hexagonal architecture modules isolate domain'
tags: [org, architecture, fake-test, hexagonal-architecture]
author: adrianb
sessions: []
created: '2026-07-30T10:46:41.007Z'
updated: '2026-07-30T10:46:41.007Z'
importanceScore: 0.5
---

## Executive Summary

## Executive summary

FAKE TEST MEMORY — created to verify the org-sync PostToolUse hook fires on store_memory and pushes org memories to the org vault git remote.

**Claim (illustrative only, not a real decision):** Hexagonal architecture (ports-and-adapters) keeps the domain core isolated from infrastructure by defining inbound/outbound ports; adapters implement those ports so swapping PostgreSQL for a REST mock touches only the adapter, not the domain. This is a standard architecture pattern worth recording in the org vault.

**Why:** separates policy from mechanism, enables dependency-inversion testability.
**How to apply:** define ports as interfaces in the domain package; implement adapters in a separate module.

Tags: architecture, fake-test, hexagonal-architecture, org
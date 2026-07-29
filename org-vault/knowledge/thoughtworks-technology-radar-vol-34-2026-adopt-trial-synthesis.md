---
title: Thoughtworks Technology Radar Vol 34 (2026) — Adopt & Trial synthesis
tags: [org, book-summary, thoughtworks, technology-radar, vol34, ai-agents, context-engineering, mutation-testing, llm, '2026']
author: adrianb
sessions: []
created: '2026-07-29T20:23:37.910Z'
updated: '2026-07-29T20:23:37.910Z'
importanceScore: 0.7
---

## Executive Summary

Synthesis of **Thoughtworks Technology Radar Vol. 34 (2026)**, restricted to items in the **Adopt** and **Trial** rings across all four quadrants (Techniques, Platforms, Tools, Languages & Frameworks). WHY it matters: Vol 34 is dominated by the shift of AI-agent engineering from experimentation to disciplined practice — context engineering, feedback loops, sandboxing, mutation testing, and evaluation are now table-stakes rather than novelties. This is the reference set used to triage which practices/tools are appropriate to fold into the total-recall plugin (see the BACKLOG.md additions cross-referenced from this memory). Source PDF: `~/.total-recall/downloads/www.thoughtworks.com-radar/tr_technology_radar_vol_34_en_1.pdf` (58 pp).

## Techniques

**Adopt**
1. **Context engineering** — treat the context window as a design surface; construct the AI's information environment deliberately. Combats "context rot" via progressive disclosure, prompt caching, dynamic retrieval (load only needed MCP servers), context graphs (policies/precedents as queryable data), and stateful compression/sub-agents.
2. **Curated shared instructions for software teams** — treat AI guidance (CLAUDE.md, AGENTS.md, .cursorrules) as a collaborative engineering asset anchored into service templates / reference apps, not per-developer prompts.
3. **DORA metrics** — change lead time, deploy frequency, MTTR, change failure rate, + new fifth metric **rework rate** (unplanned rework fixing "done" work). More important in the AI era: measure delivery flow/stability, not lines of AI-generated code.
4. **Passkeys** — FIDO2 credentials replacing passwords; hardware-backed, origin-bound, structurally phishing-resistant. Default for new auth.
5. **Structured output from LLMs** — constrain models to JSON / typed classes; sensible default for programmatic consumption. Use Instructor / Pydantic AI / Outlines for a stable cross-provider abstraction with validation + retries.
6. **Zero trust architecture** — "never trust, always verify," identity-based, least-privilege; non-negotiable default for building/operating agents (SPIFFE identities, OIDC impersonation, short-lived tokens).

**Trial**
7. **Agent Skills** — open standard for modularizing context; agents load skills only when needed (reduces tokens, mitigates instruction bloat). Caution: supply-chain risk of unreviewed third-party skills.
8. **Browser-based component testing** — Playwright-based component tests now viable (fast, less flaky) vs jsdom emulation.
9. **Feedback sensors for coding agents** — wire deterministic quality gates (compilers, linters, tests) into agentic workflows so failures trigger self-correction; run during the session, pre-commit.
10. **Mapping code smells to refactoring techniques** — instruct agents to map specific smells → defined refactoring approaches via Skills/slash commands/AGENTS.md + linting.
11. **Mutation testing** — most honest signal of a test suite's real fault-detection power; injects deliberate bugs to catch "perpetually green" AI-generated tests. Tools: Stryker, Pitest, cargo-mutants.
12. **Progressive context disclosure** — a technique within context engineering: lightweight discovery phase, load detail only when relevant; keeps the window lean, prevents context rot. Core RAG/Skills pattern.
13. **Sandboxed execution for coding agents** — run agents in isolated envs (restricted FS/network/resources). Dev Containers, Shuru microVMs, Sprites, Bubblewrap, sandbox-exec. Sensible default.
14. **Semantic layer** — shared business-logic layer between data stores and consumers (BI, agents, APIs); centralizes metric definitions. dbt MetricFlow, Cube, Snowflake Semantic Views.
15. **Server-driven UI** — generic client container + server-provided structure/data; bypasses app-store review cycles (Airbnb/Lyft patterns).

## Platforms

**Adopt** — none.
**Trial** — AG-UI Protocol, Apache APISIX, Amazon Bedrock AgentCore, **Graphiti** (open-source temporal knowledge-graph engine from Zep — bi-temporal, production-ready), Langfuse, Port, Replit, SigNoz.

## Tools

**Adopt** — Axe-core, Claude Code, Cursor, Kafbat UI, mise.

**Trial**
- **cargo-mutants** — zero-config Rust mutation testing (no source-tree changes); primary cost is per-mutant incremental build → target modules locally / run full suites async in CI.
- **Claude Code plugin marketplace** — Git-based distribution of shared commands/prompts/skills/MCP servers; internal team marketplaces on GitHub avoid version drift from copy-paste.
- **Dev Containers** — reproducible containerized dev envs (devcontainer.json); now used as sandboxes for coding agents; supply-chain benefits from declarative toolchain.
- **Figma Make** — AI prototyping from design-system components.
- **OpenAI Codex** — standalone agentic coding tool (app + CLI) for autonomous task delegation; needs testing + human review.
- **Typst** — markup typesetting, modern LaTeX successor; fast compile, scripting, loads JSON/CSV → good for automated document generation at scale.

## Languages & Frameworks

**Adopt** — Apache Iceberg, Declarative Automation Bundles, React JS, React Native, Svelte, Typer.

**Trial**
- **Agent Development Kit (ADK)** — agentic framework.
- **DeepEval** — LLM eval framework beyond word-matching (accuracy/relevance/consistency, hallucination detection, custom metrics); now covers agentic/multi-turn workflows incl. **evaluation of MCP-server interactions** (tool correctness, step efficiency, task completion) + conversation simulation.
- **Docling** — open-source Python/TS lib converting unstructured docs (PDFs, scans) → clean JSON/Markdown via CV layout understanding; strong for RAG pipelines; self-hostable alternative to Textract/Document AI.
- **LangExtract** — Python lib using LLMs to extract structured info from unstructured text with **precise source grounding** (each extracted entity linked to its location); JSONL export + HTML review. Complements Pydantic AI (LangExtract for long-form source material, Pydantic AI for short predictable inputs).
- **LangGraph** — moved OUT of Adopt to Trial: stateful-graph + global-shared-state approach is not always best; leaner code-execution-based agents (à la Pydantic AI) often win. Still powerful, no longer the default.
- **LiteLLM** — evolved from provider abstraction into a full AI gateway (retries/failover, load balancing, cost tracking, governance). Caveat: `drop_params` silently discards unsupported params; provider-specific features reintroduce coupling.
- **Modern.js** — React meta-framework (ByteDance) for Module Federation micro-frontends (nextjs-mf is EOL).

## Notable Caution (context for the above)
- **MCP by default** — don't reach for MCP reflexively; many use cases are served just as well by pointing an agent at a local CLI/script (this is partly why Agent Skills rose).
- **Ignoring durability in agent workflows** — durable execution is being integrated into frameworks like LangGraph.
- **Agent instruction bloat / Codebase cognitive debt** — the failure modes progressive disclosure and Skills are meant to prevent.

## Relevance to total-recall (cross-ref)
The plugin already embodies **progressive context disclosure** (SessionStart injects a lightweight index; the agent pulls full memories via `get_memories_by_keys` only when needed) and is itself distributed via a **Claude Code plugin marketplace** (git-subdir). It already uses **mutation testing** (Stryker; see BACKLOG.md mutation-survivor entry). Candidates evaluated for BACKLOG.md from this Radar: **context graph / temporal relations** (Graphiti-style, pairs with the existing supersede/`supersededAt` chain), **DeepEval** MCP-tool evaluation (pairs with the deferred Langfuse item), and **LangExtract**-style source-grounded extraction for the PreCompact learning-capture hook. See BACKLOG.md "Technology Radar Vol 34" entries.
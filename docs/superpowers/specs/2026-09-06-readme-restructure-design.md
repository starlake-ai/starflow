# README Restructure

Date: 2026-09-06. Approved by Hayssam in session; implemented directly (single-file docs
change, the section-by-section design below is the plan).

## Goal

Make the repository README more appealing: lead with proof instead of claims, adopt the
Starflow-first branding (product Starflow, umbrella Starlake, binary `starlake`), and surface
the community now that the team is on Discord.

## Design (approach "Show, don't list")

1. **Hero**: logo, `Starflow` as the product name, tagline "Declarative data pipelines by
   Starlake". Badge row: build (test-only workflow, pull_request-scoped), release, license,
   GitHub stars, Discord (static shield linking https://discord.com/invite/6tNa7yCNqw). Nav
   row gains Discord.
2. **Hook** (replaces the old opening paragraph): "Your warehouse, described, not scripted",
   then the boilerplate-to-YAML promise in two sentences.
3. **"A pipeline in 30 seconds"**: one self-explanatory load YAML (write strategy + semantic
   type), the three CLI commands, and the generated-DAG image as payoff, captioned that no
   DAG was written.
4. **"Why teams pick Starflow"**: five outcome bullets (config-not-code; any source to any
   warehouse to any orchestrator with generated DAGs; quality and lineage built in; privacy
   controls; AI-assistant-ready via Starlake Skills).
5. **Quick start**: unchanged installers; one-line note that the CLI keeps the historical
   `starlake` name.
6. **How it works**: existing 4-step walkthrough, YAML trimmed.
7. **Platform matrix, IDE & AI**: kept, tightened.
8. **Community & docs footer**: Discord prominent, docs, contributing, license.

No new assets: reuses starlake-draw.png, intent.png, transform-viz.svg, transform-dags.png,
data-star.png. Discord badge is a static shields.io badge (no server-id dependency).

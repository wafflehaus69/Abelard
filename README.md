# Abelard

Personal multi-agent system. Abelard is the primary judgment-tier agent;
daemons under `daemons/` are specialized capability layers.

## Layout

- `doctrine/` — Abelard's identity, values, methodology, and theses
- `daemons/research_daemon/` — financial data fetcher (Finnhub + SEC EDGAR)
- `daemons/news_watch_daemon/` — narrative-tracking news ingestion
- `scripts/` — operational scripts (doctrine deployment, etc.)

## Status

This repository is the operational infrastructure for a personal advisory
practice in development. It is not a product, not open-sourced, and not
accepting contributions.

## Doctrine deployment

The `doctrine/` directory is the version-controlled source of truth for
Abelard's identity, methodology, and operational doctrine. OpenClaw's
runtime expects these files in its workspace directory at
`~/.openclaw/workspace/`.

Edits happen in the monorepo. Deploy to OpenClaw's runtime via:

- `scripts/deploy_doctrine.sh` (Linux/macOS — for Mac mini deployment)
- `scripts/deploy_doctrine.ps1` (Windows — for Orban deployment to WSL)

Override the deploy target via `OPENCLAW_WORKSPACE` environment variable.

**Never edit doctrine directly in OpenClaw's workspace.** Those edits
would be lost on the next deploy. Edit in the monorepo, commit, deploy.

## Pending cleanup

<!-- TODO: remove C:\Users\mdiba\Code\OpenClaw\ parent directory after the
Step 3 smoke test (live Finnhub scrape) succeeds and we're confident the
monorepo is the working location. The directory was left behind
intentionally during the Step 2 monorepo restructure as a safety net. -->

## graphify

graphify is an on-demand **structural-analysis** tool, not a navigation tool.

For precise navigation — callers, consumers, reads/writes, paths — use **grep / the Grep tool**. It is faster, exact, and cheaper.

Use graphify ONLY for:
- **(a) cold orientation** of an unfamiliar subsystem, and
- **(b) centrality / community / structural** questions,

by parsing `graphify-out/graph.json` directly (NOT `graphify query`, which emits no edges and matches by substring).

Treat `graph.json` as a snapshot inventory, never as ground truth — **disk is canonical**.

The graph rebuilds automatically (AST-only, no LLM) via the git post-commit / post-checkout hooks; graphify resolves on-demand through the venv recorded in `graphify-out/.graphify_python`.

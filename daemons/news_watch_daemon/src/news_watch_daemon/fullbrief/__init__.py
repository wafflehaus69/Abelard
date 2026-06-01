"""Full Brief composition layer.

Per Abelard's Full Brief spec v1.0 + 2026-05-29 amendments (Adjustments
1-5). Composes Pass C theme-event synthesis + Pass E ATTENTION sweep +
convergence analysis + frequency diagnostic into one structured
deliverable.

Stage 1 leaf modules (this scaffolding):
  - cost.py: 4-category token cost estimator with rates_as_of provenance
  - convergence.py: strict-headline ASCII-substring convergence matcher
  - frequency_diagnostic.py: dynamic near-miss assembler (no fixed cap)

Stage 2+ (future): orchestrator (scrape -> Pass C -> Pass E -> convergence
-> frequency_diagnostic -> envelope), render layer (3-shape rendering),
CLI registration (full-brief subcommand).
"""

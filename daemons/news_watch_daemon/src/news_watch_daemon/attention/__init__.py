"""Pass E — ATTENTION-driven synthesis (parallel to Pass C theme synthesis).

Pass C synthesis is a confirmation engine: themes filter input, synthesis
produces interrupt-shaped briefs about material events within those themes.
It has a structural blind spot — anything Mando hasn't named as a theme is
invisible.

Pass E adds a parallel synthesis path that compensates. The trigger is
statistical, not semantic: a single-word frequency counter runs over the
24h headline window; terms crossing a signal-over-noise threshold spawn
ATTENTION briefs describing what the attention LOOKS LIKE (sources, entities,
shape) without evaluating thesis relevance or materiality. Abelard handles
the theme-and-thesis intersection at his layer.

Architectural symmetry with Pass C:
  counter → threshold → cluster → ATTENTION prompt → LLM → brief → dispatch
maps to
  tagger  → trigger   → cluster → SYNTHESIS prompt → LLM → brief → dispatch

Shared infrastructure: SQLite headlines table, Anthropic SDK call path,
prompt-cache pattern, AlertSink dispatch, archive directory tree.

Pass C remains the primary synthesis engine. ATTENTION briefs are
deliberately theme-blind to surface unknown-unknowns.
"""

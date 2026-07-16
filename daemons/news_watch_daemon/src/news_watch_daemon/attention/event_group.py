"""Convergence grouping — collapse same-event attention crossings.

A single dominant story is often described by several DISTINCT phrases that
each independently cross the attention floor: "attacks iran", "hormuz tensions",
"tensions rise" are three crossings but ONE event. Left alone, each fires its
own Pass E synthesis call — N LLM calls describing the same story, burning cash
for no added signal.

The bigram-collapse in `adjacency.py` only merges ADJACENT word pairs within a
single term; it cannot see that two different terms share the same underlying
headlines. This module closes that gap with a scripts-only (no LLM) pre-pass:
group crossing terms whose cluster headline-id sets are near-identical, then the
orchestrator synthesizes ONCE per group instead of once per term.

Metric — Jaccard similarity of the two terms' headline-id sets:

    jaccard(A, B) = |A ∩ B| / |A ∪ B|

Jaccard (not the overlap coefficient) is deliberate: it requires BOTH high
overlap AND comparable cluster sizes, so it merges "same event, multiple
phrasings" (near-identical clusters) but does NOT merge a broad signal that
merely contains a narrow one. Empirically calibrated on the live corpus
(2026-07-15): same-event pairs scored 0.60–1.00, the next-highest distinct pair
scored 0.16 — a wide gap, so `MIN_JACCARD = 0.5` sits safely between them.

Grouping is transitive (union-find): A~B and B~C put all three in one group.
"""

from __future__ import annotations

from .threshold import CrossingTerm


# Merge two crossings when their clusters' Jaccard similarity is at least this.
# Tunable; 0.5 sits in the empirical gap between same-event (>=0.6) and distinct
# (<=0.16) pairs measured on the live corpus.
MIN_JACCARD = 0.5


def group_convergent_crossings(
    crossings: list[CrossingTerm],
    cluster_id_sets: dict[str, set[str]],
    *,
    min_jaccard: float = MIN_JACCARD,
) -> list[list[CrossingTerm]]:
    """Group crossing terms that describe the SAME event.

    Args:
        crossings: the threshold-crossing terms (order preserved as tiebreak).
        cluster_id_sets: term -> set of headline_ids in that term's cluster.
            A term absent here (or with an empty set) never merges — it forms
            its own singleton group.
        min_jaccard: merge two terms when Jaccard(sets) >= this value.

    Returns:
        A list of groups. Within each group the REPRESENTATIVE (highest
        window_count, ties broken alphabetically) is first. Groups are ordered
        by representative window_count desc — same discipline as
        `evaluate_threshold`, so a no-convergence run reproduces the original
        per-term order exactly.
    """
    n = len(crossings)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]   # path halving
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        parent[find(x)] = find(y)

    for i in range(n):
        si = cluster_id_sets.get(crossings[i].term) or set()
        if not si:
            continue
        for j in range(i + 1, n):
            sj = cluster_id_sets.get(crossings[j].term) or set()
            if not sj:
                continue
            inter = len(si & sj)
            if inter == 0:
                continue
            if inter / len(si | sj) >= min_jaccard:
                union(i, j)

    groups_map: dict[int, list[CrossingTerm]] = {}
    for i, crossing in enumerate(crossings):
        groups_map.setdefault(find(i), []).append(crossing)

    groups = list(groups_map.values())
    for members in groups:
        members.sort(key=lambda c: (-c.window_count, c.term))   # representative first
    groups.sort(key=lambda g: (-g[0].window_count, g[0].term))  # stable overall order
    return groups


__all__ = ["MIN_JACCARD", "group_convergent_crossings"]

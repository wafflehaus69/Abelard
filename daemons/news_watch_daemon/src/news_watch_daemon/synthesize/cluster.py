"""Headline clustering — pure script, deterministic, no LLM.

One cluster represents one logical event. Multiple wires of the same
Reuters story, near-duplicate publisher rewrites of the same event,
and identical-URL syndications all collapse into a single cluster.
The synthesis call then writes about events (clusters), not about
individual headlines.

Three signals union-find headlines into clusters (any one triggers
a merge — the OR composition was Mando's hypothesis at §15):

  1. Same URL: identical URLs are the same article (and same event).
  2. Token Jaccard ≥ threshold: shared content tokens above a cutoff.
     Stopwords stripped; tokens lowercased; min-length 2 to drop
     "a"/"I"/"o" noise.
  3. Same publisher + ±N min (OPT-IN, default off): a single publisher
     emitting two pieces in a short window. The Step 3 benchmark
     against Pass B's 100-headline smoke corpus showed this signal
     has weak precision in practice — Reuters and CNBC publish many
     unrelated stories within a few minutes of each other, and a
     single bad link via union-find transitively absorbs whole sets
     of unrelated headlines into one cluster. Disabled by default.
     Callers can opt in via time_window_s=N when they have a corpus
     where this signal helps (e.g. wire-rewrite-heavy feeds).

Threshold tuning (against the Pass B 100-headline smoke corpus, see
Step 3 commit message for the table): jaccard_threshold=0.4 produces
clean groupings of wire-rewrite variants (e.g. four Trump/Iran/China
headlines collapse to one cluster) while keeping unrelated stories
separate. 0.3 over-merges thematically-similar but distinct events;
0.5 splits clear wire variants apart.

Complexity: O(n²) for n headlines per scrape window. With n ≤ ~500
per cycle the wall-clock cost is sub-millisecond and not worth
optimizing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


DEFAULT_JACCARD_THRESHOLD = 0.4
DEFAULT_TIME_WINDOW_S: int | None = None  # publisher+time signal OFF by default; see module docstring


# Minimal English stopword list. Deliberately narrow — over-stripping
# erodes Jaccard precision more than it helps.
_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "of", "to", "and", "is", "in", "for", "on",
    "with", "as", "at", "by", "from", "or", "but", "be", "are",
    "was", "were", "this", "that", "it", "its", "has", "have",
    "had", "will", "would", "could", "should", "after", "before",
    "into", "out", "up", "down", "over", "under", "than", "then",
    "so", "if", "not", "no", "nor", "yes", "you", "your", "we",
    "our", "they", "their", "he", "she", "his", "her", "us",
    "i", "me", "my", "all", "any", "some", "more", "most",
    "such", "which", "who", "whom", "what", "when", "where",
    "why", "how", "amid", "while", "via", "per", "near", "off",
    "between", "through", "during", "across", "about",
})

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


@dataclass(frozen=True)
class ClusterInput:
    """Lightweight view of a headline for clustering.

    Detached from FetchedItem on purpose — clustering operates on
    persisted-headline rows from the SQLite DB (post-dedup), not on
    in-flight source-plugin outputs.
    """

    headline_id: str
    headline: str
    url: str | None
    publisher: str | None
    published_at_unix: int


@dataclass(frozen=True)
class Cluster:
    """A group of headlines that represent the same event.

    Ordered newest-first by `published_at_unix` for stable iteration.
    The leader (first item) is the most recent headline in the cluster
    — synthesis prompts typically cite the leader and treat others as
    corroboration.
    """

    headline_ids: tuple[str, ...]
    members: tuple[ClusterInput, ...]

    @property
    def size(self) -> int:
        return len(self.members)

    @property
    def leader(self) -> ClusterInput:
        return self.members[0]


def _tokens(s: str) -> frozenset[str]:
    """Tokenize for Jaccard: lowercase, alnum, ≥2 chars, drop stopwords."""
    if not s:
        return frozenset()
    return frozenset(
        t for t in (m.group(0).lower() for m in _TOKEN_RE.finditer(s))
        if len(t) >= 2 and t not in _STOPWORDS
    )


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _should_merge(
    i: ClusterInput,
    j: ClusterInput,
    ti: frozenset[str],
    tj: frozenset[str],
    *,
    jaccard_threshold: float,
    time_window_s: int | None,
) -> bool:
    # (1) Same URL.
    if i.url and j.url and i.url == j.url:
        return True
    # (2) Jaccard ≥ threshold.
    if _jaccard(ti, tj) >= jaccard_threshold:
        return True
    # (3) Same publisher within ±time_window_s. OPT-IN: only fires
    # when caller explicitly passes a non-None window. See module
    # docstring for the precision-weakness rationale.
    if (
        time_window_s is not None
        and i.publisher and j.publisher
        and i.publisher == j.publisher
        and abs(i.published_at_unix - j.published_at_unix) <= time_window_s
    ):
        return True
    return False


def cluster_headlines(
    items: Iterable[ClusterInput],
    *,
    jaccard_threshold: float = DEFAULT_JACCARD_THRESHOLD,
    time_window_s: int | None = DEFAULT_TIME_WINDOW_S,
) -> list[Cluster]:
    """Cluster headlines via union-find over the three OR-signals.

    Returns clusters ordered by leader timestamp newest-first. Members
    within a cluster are also newest-first.
    """
    item_list = list(items)
    n = len(item_list)
    if n == 0:
        return []

    parents = list(range(n))

    def find(x: int) -> int:
        # Iterative path-compression.
        root = x
        while parents[root] != root:
            root = parents[root]
        while parents[x] != root:
            parents[x], x = root, parents[x]
        return root

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parents[ra] = rb

    tokens = [_tokens(it.headline) for it in item_list]
    for i in range(n):
        for j in range(i + 1, n):
            if _should_merge(
                item_list[i], item_list[j], tokens[i], tokens[j],
                jaccard_threshold=jaccard_threshold,
                time_window_s=time_window_s,
            ):
                union(i, j)

    # Group by root, then sort members and clusters newest-first.
    groups: dict[int, list[ClusterInput]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(item_list[i])

    clusters: list[Cluster] = []
    for members in groups.values():
        members.sort(key=lambda m: m.published_at_unix, reverse=True)
        clusters.append(Cluster(
            headline_ids=tuple(m.headline_id for m in members),
            members=tuple(members),
        ))
    clusters.sort(key=lambda c: c.leader.published_at_unix, reverse=True)
    return clusters


__all__ = [
    "Cluster",
    "ClusterInput",
    "DEFAULT_JACCARD_THRESHOLD",
    "DEFAULT_TIME_WINDOW_S",
    "cluster_headlines",
]

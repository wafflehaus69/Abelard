"""Adjacency map with bigram collapse for the ATTENTION attention list.

The single-word counter (`counter.py`) is bag-of-words: it loses token
order, so multi-word entities fragment into competing unigrams — "supreme"
(18) and "court" (22) surface as two separate near-misses when the real
signal is "supreme court" (one story). This module reworks tokenization to
preserve order, counts adjacent bigrams, and collapses a promoted bigram's
two constituent unigrams into the single pair — producing ONE final
attention list ordered by mentions.

Three-tier token handling (Order: ATTENTION adjacency map, 2026-06-30):

  A. HARD-DROP — removed from the sequence entirely, never a unigram and
     never a bigram member: tokenizer contraction fragments ("re" from
     "we're", "ve", "ll", negation stems like "doesn"/"isn"/"wasn") and
     source-name-as-class tokens (wire/aggregator + telegram-channel
     handles).

  A'. GRAMMATICAL STOPWORDS (stopwords.yaml) — RETAINED in the sequence as
     adjacency-breakers but forbidden as bigram members. They hold their
     position so that flanking content words are NOT treated as adjacent
     ("in iran" and "bank of england" produce no pair), and they never
     surface as unigrams (they are stopwords). This is the guard that keeps
     a grammatical filler from promoting a junk pair and swallowing a clean
     content unigram — "iran" survives standalone rather than collapsing
     into "in iran" or the skip-gram "trouble iran". Two adjacent content
     words with no stopword between them ("supreme court") still pair.

  B. SOFT-STOPWORDS (bigram-only) — suppressed as standalone unigrams but
     RETAINED in the sequence AND allowed as bigram members so they can
     still form a meaningful pair: "world cup", "new york", "media post".
     The generic words that surface as noisy standalone near-misses
     (are/new/use/since/people/world/years/low/strong/post/media/
     information/service) live here.

  C. CONTENT UNIGRAMS — kept as standalone terms and as bigram members.

Promotion threshold: a bigram is promoted when it co-occurs in MORE than
`ADJACENCY_MIN` (5) distinct headlines per window (i.e. count >= 6). On
promotion, BOTH constituent unigrams are dropped from the final list and
replaced by the pair (per the standing clarification: "the adjacent list
findings that produce a word pair should replace both single words if they
appear together").

Per-headline-distinct counting matches counter.py: a headline that repeats
a term or pair counts it once. The module is pure — it takes iterables of
headline strings (window + prior) and returns a ranked list — so it is
trivially unit-testable and corpus-runnable without DB plumbing.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable


# Bigram promotes when it co-occurs in MORE than this many distinct
# headlines (count > 5 → count >= 6). Tunable; matches the spec's ">5".
ADJACENCY_MIN = 5

# Same alphabetic, 2+ char, word-boundary token as counter.py — keeps the
# two tokenizers in lockstep so unigram counts reconcile across modules.
_TOKEN_RE = re.compile(r"\b[a-zA-Z]{2,}\b")

# Tier A — tokenizer fragments. The `\b[a-zA-Z]{2,}\b` regex splits
# contractions on the apostrophe: "we're" -> ["we", "re"], "doesn't" ->
# ["doesn", "t"] (the 1-char "t" is dropped by the 2-char floor). These
# stems carry no signal and, left in, "re" pollutes the near-miss table
# (the artifact the order calls out by name).
_TOKENIZER_FRAGMENTS = frozenset({
    "re", "ve", "ll",                       # 're, 've, 'll
    "don", "doesn", "didn", "isn", "wasn",  # negation stems
    "aren", "weren", "hasn", "haven", "hadn",
    "won", "wouldn", "couldn", "shouldn",
    "mustn", "needn", "mightn", "shan", "ain",
    "let",                                  # let's -> let + s
})

# Tier A — source-name-as-class. Wire / aggregator publisher tokens and the
# daemon's telegram-channel handles that leak into headline text (Google
# News appends " - Reuters"; telegram source tags surface the handle).
# stopwords.yaml already drops reuters/bloomberg/cnbc/ft; this adds the
# rest of the class so a publisher name can never anchor a bigram.
_SOURCE_NAME_TOKENS = frozenset({
    "reuters", "bloomberg", "cnbc", "ft", "wsj", "ap", "afp", "axios",
    "politico", "nikkei", "scmp", "tass", "ria", "npr", "bbc", "cnn",
    "guardian", "marketwatch", "barrons", "forbes", "yahoo",
    "finnhub", "telegram", "cig", "trading",
})

# Tier B — soft-stopwords. Generic words that surface as noisy standalone
# near-misses but carry signal inside a pair. Suppressed from the unigram
# list; retained in the sequence so "world cup" / "new york" still form.
# million/billion/trillion added 2026-07-07 (footgun #2): bare magnitude
# denominators topped the attention list ("million 16/2") while carrying no
# standalone signal. Tier B (not stopwords.yaml) so "X million barrels"-style
# pairs still form — a yaml entry would forbid the term as a bigram member.
_SOFT_STOPWORDS = frozenset({
    "are", "new", "use", "since", "people", "world", "years",
    "low", "strong", "post", "media", "information", "service",
    "million", "billion", "trillion",
})

# Tier A — the intrinsic hard-drop set (fragments + source names). Folded
# out of the token sequence unconditionally by tokenize_ordered; unlike the
# grammatical stopwords, these do not even hold an adjacency-breaking slot.
_TIER_A = _TOKENIZER_FRAGMENTS | _SOURCE_NAME_TOKENS


@dataclass(frozen=True)
class AttentionTerm:
    """One entry in the collapsed attention list.

    `kind` is "bigram" for a promoted pair (text is "word1 word2"), or
    "unigram" for a surviving single content word. `prior_count` and
    `delta_ratio` mirror the frequency-diagnostic near-miss table so this
    list drops straight into that surface.
    """

    text: str
    kind: str          # "bigram" | "unigram"
    window_count: int
    prior_count: int
    delta_ratio: float


def tokenize_ordered(text: str | None) -> list[str]:
    """Extract ORDERED lowercase tokens, dropping only Tier-A members.

    Order is preserved (returns a list, not a set) so adjacency survives.
    Only Tier-A tokens (contraction fragments + source names) are removed;
    grammatical stopwords and Tier-B soft words are RETAINED so they hold
    their adjacency-breaking / pair-forming positions in the sequence.
    """
    if not text:
        return []
    out: list[str] = []
    for m in _TOKEN_RE.finditer(text):
        token = m.group(0).lower()
        if token not in _TIER_A:
            out.append(token)
    return out


def _count_window(
    headlines: Iterable[str | None],
    grammatical_stops: frozenset[str],
) -> tuple[Counter[str], Counter[tuple[str, str]]]:
    """Per-headline-distinct unigram (Tier-C only) and bigram counters.

    Unigrams exclude grammatical stopwords and Tier-B soft words. A bigram
    is formed from an adjacent pair only when NEITHER member is a
    grammatical stopword — soft members are allowed, so "world cup" forms
    but "in iran" and "of the" do not. The grammatical stopword still
    occupies its slot, so it breaks adjacency between the words flanking it.
    """
    unigrams: Counter[str] = Counter()
    bigrams: Counter[tuple[str, str]] = Counter()
    for text in headlines:
        toks = tokenize_ordered(text)
        # Unigrams: distinct, Tier-C only (grammatical + soft excluded).
        for tok in {
            t for t in toks
            if t not in grammatical_stops and t not in _SOFT_STOPWORDS
        }:
            unigrams[tok] += 1
        # Bigrams: distinct adjacent pairs with no grammatical-stopword member.
        pairs = {
            (toks[i], toks[i + 1])
            for i in range(len(toks) - 1)
            if toks[i] not in grammatical_stops
            and toks[i + 1] not in grammatical_stops
        }
        for pair in pairs:
            bigrams[pair] += 1
    return unigrams, bigrams


def _delta_ratio(window_n: int, prior_n: int) -> float:
    """window/prior with prior=0 treated as 1 (matches frequency_diagnostic)."""
    return window_n / max(prior_n, 1)


def build_attention_list(
    window_headlines: Iterable[str | None],
    prior_headlines: Iterable[str | None],
    stopwords: frozenset[str],
    *,
    adjacency_min: int = ADJACENCY_MIN,
) -> list[AttentionTerm]:
    """Build the single collapsed attention list for one window.

    Bigrams co-occurring in > `adjacency_min` distinct window-headlines are
    promoted; each promoted pair drops both its constituent unigrams from
    the list and replaces them. Remaining Tier-C unigrams pass through.
    The result is ordered by window_count desc, ties broken by delta_ratio
    desc then text — same discipline as the near-miss table.

    `stopwords` is the frozenset loaded from stopwords.yaml; it is used as
    the grammatical adjacency-breaker set (retained in-sequence, forbidden
    as bigram members, excluded from unigrams).
    """
    win_uni, win_big = _count_window(window_headlines, stopwords)
    prior_uni, prior_big = _count_window(prior_headlines, stopwords)

    # Promote bigrams over the threshold (strictly greater than adjacency_min).
    promoted_pairs = {
        pair: n for pair, n in win_big.items() if n > adjacency_min
    }

    # Collapse set: every unigram that is a constituent of a promoted pair
    # is removed from the standalone list and represented by the pair.
    collapsed: set[str] = set()
    for a, b in promoted_pairs:
        collapsed.add(a)
        collapsed.add(b)

    terms: list[AttentionTerm] = []

    # Promoted bigrams.
    for (a, b), n in promoted_pairs.items():
        prior_n = prior_big.get((a, b), 0)
        terms.append(AttentionTerm(
            text=f"{a} {b}",
            kind="bigram",
            window_count=n,
            prior_count=prior_n,
            delta_ratio=_delta_ratio(n, prior_n),
        ))

    # Surviving Tier-C unigrams (those not swallowed by a promoted pair).
    for tok, n in win_uni.items():
        if tok in collapsed:
            continue
        prior_n = prior_uni.get(tok, 0)
        terms.append(AttentionTerm(
            text=tok,
            kind="unigram",
            window_count=n,
            prior_count=prior_n,
            delta_ratio=_delta_ratio(n, prior_n),
        ))

    terms.sort(key=lambda t: (-t.window_count, -t.delta_ratio, t.text))
    return terms


__all__ = [
    "ADJACENCY_MIN",
    "AttentionTerm",
    "build_attention_list",
    "tokenize_ordered",
]

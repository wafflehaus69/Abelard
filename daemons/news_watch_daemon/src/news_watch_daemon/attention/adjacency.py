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

# Singular/plural fold (2026-07-20). The counter treats "prediction market" and
# "prediction markets" as two distinct bigrams: they clear the threshold
# separately, and — measured 22 vs 16 headlines with ZERO overlap — convergence
# grouping (Jaccard >= 0.5) cannot merge them, so each fired its own attention
# synthesis and both surfaced as crossings. This helper feeds a term-identity
# pass in event_group.py that merges such variants at grouping time (NOT in the
# counter: the term text is a downstream search key for cluster_for_term, and a
# singular key would not match plural headline text).
#
# CONSERVATIVE by construction — it must never corrupt a singular noun that
# ends in 's'. Words <= 4 chars are left alone (gas, news, odds, lens), as are
# the singular-in-'s' families below. So it folds markets->market,
# sanctions->sanction, tariffs->tariff, yields->yield, prices->price, but
# leaves consensus / analysis / series / politics / status / campus intact.
_PLURAL_KEEP_SUFFIXES = ("ss", "us", "is", "ics", "ies", "as", "os")


def _singularize(word: str) -> str:
    """Fold a regular English plural to its singular. Leaves short words and
    singular-in-'s' families (consensus/analysis/series/gas) untouched — see
    _PLURAL_KEEP_SUFFIXES."""
    if len(word) <= 4:
        return word
    if word.endswith(_PLURAL_KEEP_SUFFIXES):
        return word
    if word.endswith("s"):
        return word[:-1]
    return word


def normalize_term(term: str) -> str:
    """Singular-fold every word of a (possibly multi-word) attention term, so
    "prediction markets" and "prediction market" normalize to one key. Used by
    convergence grouping to merge singular/plural variants of the same term."""
    return " ".join(_singularize(w) for w in term.split())

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
    # NW-SRC-3 Fix 2 (Tier A backstop): single-token outlet names that leaked
    # as attention orphans off the NW-SRC-2 Google News feeds. `barron` is the
    # real culprit behind the "$0.072 barron" orphan — "Barron's" tokenizes to
    # `barron` (the apostrophe splits and the 1-char trailing "s" is dropped by
    # the 2-char floor), so the existing `barrons` entry above never matched.
    # `jazeera` (from "Al Jazeera"), `coindesk`, `cryptorank`, `magnates` (from
    # "Finance Magnates") are outlet-ONLY tokens, never content words.
    # DELIBERATELY NOT ADDED (polysemy guard, per order §4): the multi-word
    # outlet names whose constituents are real subjects — "New York" (Times),
    # "finance" (Magnates), "crypto" (Briefing), "ledger" (Insights). Those are
    # single-token here would delete genuine signal; Fix 1's position-aware,
    # source-matched suffix strip removes them in byline position instead.
    "barron", "jazeera", "coindesk", "cryptorank", "magnates",
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
    # NW-SRC-3 Fix 2 (Tier B): generic frequency-gate words that crossed the
    # threshold as bare unigrams on the enlarged NW-SRC-2 corpus but carry no
    # standalone signal. Bigram-only: suppressed as unigrams, RETAINED as pair
    # members — so "defense production", "ground troops", "data center",
    # "year end" still surface; bare production/troops/end/etc do not. Kept out
    # of stopwords.yaml on purpose: a grammatical stop there would forbid the
    # bigram membership too and kill those real pairs.
    "get", "plans", "bring", "buy", "face", "raises", "recent", "companies",
    "shares", "live", "final", "ahead", "fund", "end", "production", "troops",
    # near-miss fillers that sat just under the gate — pre-empt next-cycle crossings
    "now", "one", "two", "latest", "across", "major", "push",
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
    kind: str          # "trigram" | "bigram" | "unigram"
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
) -> tuple[Counter[str], Counter[tuple[str, str]], Counter[tuple[str, str, str]]]:
    """Per-headline-distinct unigram / bigram / TRIGRAM counters (Tier-C).

    Unigrams exclude grammatical stopwords and Tier-B soft words. A bigram or
    trigram is formed from adjacent tokens only when NO member is a grammatical
    stopword — soft members are allowed, so "world cup" and "defense industrial
    base" form but "in iran" and "of the day" do not. The grammatical stopword
    still occupies its slot, so it breaks adjacency between the words flanking it.
    Trigrams extend the signal window to 3 words (Mando 2026-07-17).
    """
    unigrams: Counter[str] = Counter()
    bigrams: Counter[tuple[str, str]] = Counter()
    trigrams: Counter[tuple[str, str, str]] = Counter()
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
        # Trigrams: distinct adjacent triples, no grammatical-stopword member.
        triples = {
            (toks[i], toks[i + 1], toks[i + 2])
            for i in range(len(toks) - 2)
            if toks[i] not in grammatical_stops
            and toks[i + 1] not in grammatical_stops
            and toks[i + 2] not in grammatical_stops
        }
        for tr in triples:
            trigrams[tr] += 1
    return unigrams, bigrams, trigrams


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
    win_uni, win_big, win_tri = _count_window(window_headlines, stopwords)
    prior_uni, prior_big, prior_tri = _count_window(prior_headlines, stopwords)

    # 1) Promote TRIGRAMS over the threshold (strictly greater than adjacency_min).
    promoted_tri = {tr: n for tr, n in win_tri.items() if n > adjacency_min}

    # Collapse from a promoted trigram "a b c":
    #   - its 3 constituent unigrams ALWAYS (they are fragments of the phrase);
    #   - a constituent bigram ONLY when that bigram appears exclusively within
    #     the trigram (win_big <= trigram count). A bigram that recurs
    #     INDEPENDENTLY (e.g. "data center" @66 vs "data center moratorium" @15)
    #     is a BROADER signal and must survive as its own term, so it is NOT
    #     collapsed.
    collapsed_uni: set[str] = set()
    collapsed_big: set[tuple[str, str]] = set()
    for (a, b, c), n in promoted_tri.items():
        collapsed_uni.update((a, b, c))
        for pair in ((a, b), (b, c)):
            if win_big.get(pair, 0) <= n:
                collapsed_big.add(pair)

    # 2) Promote BIGRAMS over the threshold, except those fully subsumed by a
    #    promoted trigram. Each surviving promoted bigram collapses its unigrams.
    promoted_pairs = {
        pair: n for pair, n in win_big.items()
        if n > adjacency_min and pair not in collapsed_big
    }
    for a, b in promoted_pairs:
        collapsed_uni.add(a)
        collapsed_uni.add(b)

    terms: list[AttentionTerm] = []

    # Promoted trigrams (widest signal first in the tiebreak sense).
    for (a, b, c), n in promoted_tri.items():
        prior_n = prior_tri.get((a, b, c), 0)
        terms.append(AttentionTerm(
            text=f"{a} {b} {c}", kind="trigram",
            window_count=n, prior_count=prior_n,
            delta_ratio=_delta_ratio(n, prior_n),
        ))

    # Promoted bigrams.
    for (a, b), n in promoted_pairs.items():
        prior_n = prior_big.get((a, b), 0)
        terms.append(AttentionTerm(
            text=f"{a} {b}", kind="bigram",
            window_count=n, prior_count=prior_n,
            delta_ratio=_delta_ratio(n, prior_n),
        ))

    # Surviving Tier-C unigrams (not swallowed by a promoted bigram/trigram).
    for tok, n in win_uni.items():
        if tok in collapsed_uni:
            continue
        prior_n = prior_uni.get(tok, 0)
        terms.append(AttentionTerm(
            text=tok, kind="unigram",
            window_count=n, prior_count=prior_n,
            delta_ratio=_delta_ratio(n, prior_n),
        ))

    terms.sort(key=lambda t: (-t.window_count, -t.delta_ratio, t.text))
    return terms


__all__ = [
    "ADJACENCY_MIN",
    "AttentionTerm",
    "build_attention_list",
    "normalize_term",
    "tokenize_ordered",
]

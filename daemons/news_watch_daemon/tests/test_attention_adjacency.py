"""Unit tests for the ATTENTION adjacency map (bigram collapse).

Covers the three-tier token handling, the >ADJACENCY_MIN promotion rule,
full-replacement collapse, per-headline-distinct counting, and the
grammatical-stopword-breaks-adjacency guard that protects clean content
unigrams from being swallowed by a filler pair.
"""

from __future__ import annotations

from news_watch_daemon.attention.adjacency import (
    ADJACENCY_MIN,
    AttentionTerm,
    build_attention_list,
    tokenize_ordered,
)

# Minimal grammatical stopword set for tests (a real load is larger).
_SW = frozenset({"the", "of", "in", "on", "a", "and", "to"})


def _terms_by_text(terms: list[AttentionTerm]) -> dict[str, AttentionTerm]:
    return {t.text: t for t in terms}


# ---------- tokenize_ordered ----------

def test_tokenize_ordered_preserves_order_and_duplicates():
    toks = tokenize_ordered("Iran strikes Iran again")
    assert toks == ["iran", "strikes", "iran", "again"]


def test_tokenize_ordered_drops_contraction_fragments():
    # "we're" -> we + re ; "doesn't" -> doesn + t(dropped) ; "re"/"doesn" are Tier-A
    toks = tokenize_ordered("we're sure it doesn't matter")
    assert "re" not in toks
    assert "doesn" not in toks
    assert "sure" in toks and "matter" in toks


def test_tokenize_ordered_drops_source_names():
    toks = tokenize_ordered("Big deal - Reuters via telegram")
    assert "reuters" not in toks
    assert "telegram" not in toks
    assert "big" in toks and "deal" in toks


def test_tokenize_ordered_retains_grammatical_stopwords_in_sequence():
    # Grammatical stopwords hold their slot (adjacency-breaker); Tier-A only is dropped.
    toks = tokenize_ordered("Bank of England holds rates")
    assert toks == ["bank", "of", "england", "holds", "rates"]


# ---------- promotion threshold ----------

def test_bigram_promotes_strictly_above_min():
    # exactly ADJACENCY_MIN copies -> NOT promoted (rule is > min).
    at_min = ["supreme court ruling"] * ADJACENCY_MIN
    terms = _terms_by_text(build_attention_list(at_min, [], _SW))
    assert "supreme court" not in terms
    # "supreme" and "court" survive as unigrams instead.
    assert "supreme" in terms and "court" in terms

    # one more copy crosses the threshold.
    above = ["supreme court ruling"] * (ADJACENCY_MIN + 1)
    terms2 = _terms_by_text(build_attention_list(above, [], _SW))
    assert "supreme court" in terms2
    assert terms2["supreme court"].kind == "bigram"


# ---------- collapse ----------

def test_promoted_bigram_replaces_both_unigrams():
    # Vary the tail so only "supreme court" recurs >5x (verbatim-repeat would
    # chain-collapse the whole headline — ingest dedups identical headlines).
    win = [f"supreme court decision {w}" for w in
           ("alpha", "beta", "gamma", "delta", "epsilon", "zeta")]
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "supreme court" in terms
    assert terms["supreme court"].kind == "bigram"
    # both constituents removed from the standalone list
    assert "supreme" not in terms
    assert "court" not in terms


def test_soft_stopword_suppressed_as_unigram_but_forms_bigram():
    # "world" is a soft-stopword: never a standalone unigram, but "world cup" forms.
    win = ["world cup final"] * 6
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "world" not in terms          # Tier-B: suppressed standalone
    assert "world cup" in terms          # but survives inside the pair
    assert terms["world cup"].kind == "bigram"


# ---------- grammatical-stopword adjacency guard ----------

def test_grammatical_filler_does_not_pair_with_content():
    # "in iran" must NOT promote (would swallow the clean 'iran' signal):
    # 'in' is hard-dropped, so 'iran' stands alone.
    win = ["trouble in Iran"] * 6
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "iran" in terms
    assert terms["iran"].kind == "unigram"
    assert "in iran" not in terms       # stopword member -> no promotion
    assert "trouble iran" not in terms  # and no skip-gram across the stopword


def test_no_skipgram_across_stopword():
    # 'of' holds its slot -> bank/england are NOT adjacent, no pair forms;
    # both survive as clean unigrams instead of collapsing to "bank england".
    win = ["Bank of England"] * 6
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "bank england" not in terms
    assert "bank" in terms and "england" in terms


# ---------- per-headline distinct ----------

def test_per_headline_distinct_counting():
    # The pair appears TWICE in each headline but must count once per headline.
    win = ["supreme court backs supreme court"] * 6
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    # 6 headlines, pair distinct-per-headline -> window_count 6, not 12.
    assert terms["supreme court"].window_count == 6


# ---------- delta ratio & ordering ----------

def test_delta_ratio_prior_zero_uses_one():
    # Vary the partner so no pair promotes and 'novel' survives as a unigram.
    win = [f"novel {w}" for w in ("aa", "bb", "cc", "dd", "ee", "ff", "gg")]
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    t = terms["novel"]
    assert t.window_count == 7
    assert t.prior_count == 0
    assert t.delta_ratio == float(t.window_count)  # window / max(prior,1) == 7


def test_ordered_by_window_count_desc():
    win = (["iran strikes"] * 10) + (["oil prices climb"] * 6)
    terms = build_attention_list(win, [], _SW)
    counts = [t.window_count for t in terms]
    assert counts == sorted(counts, reverse=True)


def test_prior_counts_feed_delta_ratio():
    win = ["ecb rate decision"] * 8
    prior = ["ecb rate decision"] * 2
    terms = _terms_by_text(build_attention_list(win, prior, _SW))
    # "ecb rate" or "rate decision" bigrams promoted; check a surviving/known term prior wired
    # 'ecb' collapses into 'ecb rate' (adjacent). Verify a promoted bigram carried prior.
    promoted = [t for t in terms.values() if t.kind == "bigram"]
    assert promoted, "expected at least one promoted bigram"
    assert all(p.prior_count == 2 for p in promoted)

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
    _singularize,
    build_attention_list,
    normalize_term,
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
    at_min = ["supreme court"] * ADJACENCY_MIN
    terms = _terms_by_text(build_attention_list(at_min, [], _SW))
    assert "supreme court" not in terms
    # "supreme" and "court" survive as unigrams instead.
    assert "supreme" in terms and "court" in terms

    # one more copy crosses the threshold.
    above = ["supreme court"] * (ADJACENCY_MIN + 1)
    terms2 = _terms_by_text(build_attention_list(above, [], _SW))
    assert "supreme court" in terms2
    assert terms2["supreme court"].kind == "bigram"


# ---------- collapse ----------

def test_promoted_bigram_replaces_both_unigrams():
    # Vary the tail so only "supreme court" recurs >5x (verbatim-repeat would
    # chain-collapse the whole headline — ingest dedups identical headlines).
    win = [f"supreme court {w}" for w in
           ("alpha", "beta", "gamma", "delta", "epsilon", "zeta")]
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "supreme court" in terms
    assert terms["supreme court"].kind == "bigram"
    # both constituents removed from the standalone list
    assert "supreme" not in terms
    assert "court" not in terms


def test_soft_stopword_suppressed_as_unigram_but_forms_bigram():
    # "world" is a soft-stopword: never a standalone unigram, but "world cup" forms.
    win = ["world cup"] * 6
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
    # 'and' (grammatical stop) breaks adjacency so no trigram forms — this stays
    # a clean bigram-counting test.
    win = ["supreme court and supreme court"] * 6
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
    # "ecb rate decision" promotes as a trigram (constituent bigrams collapse
    # into it); its prior count (2) must wire through to the delta ratio.
    promoted = [t for t in terms.values() if t.kind == "trigram"]
    assert promoted, "expected at least one promoted trigram"
    assert all(p.prior_count == 2 for p in promoted)


# ---------- Tier-B magnitude denominators (footgun #2, 2026-07-07) ----------

def test_bare_million_billion_trillion_suppressed_as_unigrams():
    # Each flanked by unique words so no pair promotes; as Tier-B soft words
    # they must not surface as standalone attention terms.
    win = [
        "alpha million beta", "gamma billion delta", "epsilon trillion zeta",
        "eta million theta", "iota billion kappa", "lambda trillion mu",
        "nu million xi", "omicron billion pi",
    ]
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "million" not in terms
    assert "billion" not in terms
    assert "trillion" not in terms


def test_million_still_forms_inside_a_bigram():
    # "million barrels" context must survive — Tier-B allows bigram membership.
    win = ["million barrels"] * 6
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "million barrels" in terms          # the pair forms
    assert terms["million barrels"].kind == "bigram"
    assert "million" not in terms              # but bare "million" stays suppressed


# ---------- trigrams (up-to-3-word signal detection, 2026-07-17) ----------

def test_trigram_promotes_and_collapses_its_constituents():
    win = ["defense industrial base"] * (ADJACENCY_MIN + 1)
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert "defense industrial base" in terms
    assert terms["defense industrial base"].kind == "trigram"
    # constituent unigrams + exclusive bigrams collapse into the trigram
    assert "defense" not in terms and "industrial" not in terms and "base" not in terms
    assert "defense industrial" not in terms and "industrial base" not in terms


# ---------- singular/plural fold on the count key (2026-07-20) ----------

def test_singularize_folds_regular_plurals():
    assert _singularize("markets") == "market"
    assert _singularize("sanctions") == "sanction"
    assert _singularize("tariffs") == "tariff"
    assert _singularize("yields") == "yield"
    assert _singularize("prices") == "price"      # -es plural, singular ends in e
    assert _singularize("chips") == "chip"


def test_singularize_leaves_singular_s_nouns_untouched():
    # The families the keep-suffixes and length guard protect from corruption.
    for w in ("gas", "news", "odds", "lens",           # <= 4 chars
              "consensus", "status", "campus", "virus", # us
              "analysis", "basis", "crisis",            # is
              "politics", "physics",                    # ics
              "series", "species",                      # ies
              "atlas", "kudos"):                        # as / os
        assert _singularize(w) == w, w


def test_normalize_term_folds_multiword_plurals():
    # The grouping key that merges singular/plural crossings. Every word is
    # folded, so both spellings normalize to the same string.
    assert normalize_term("prediction markets") == "prediction market"
    assert normalize_term("prediction market") == "prediction market"
    assert normalize_term("chip sanctions") == "chip sanction"
    # singular-in-'s' words are preserved (no false merge)
    assert normalize_term("gas crisis") == "gas crisis"


def test_broad_bigram_survives_a_promoted_trigram():
    # "data center" is a BROAD signal (many stories); "data center moratorium" is
    # a narrower promoted trigram. The broad bigram must NOT be collapsed away.
    win = (["data center moratorium"] * 6) + [f"data center {w}" for w in
           ("expansion", "outage", "boom", "permit", "grid", "water", "zoning")]
    terms = _terms_by_text(build_attention_list(win, [], _SW))
    assert terms["data center moratorium"].kind == "trigram"   # narrow phrase
    assert terms["data center"].kind == "bigram"               # broad signal survives
    assert terms["data center"].window_count == 13             # full independent count kept

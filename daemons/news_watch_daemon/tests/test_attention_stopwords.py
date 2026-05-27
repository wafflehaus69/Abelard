"""Stopword loader tests — hermetic, tmp-dir scoped."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from news_watch_daemon.attention.stopwords import StopwordsError, load_stopwords


def _write_yaml(tmp_path: Path, payload, name: str = "sw.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def test_load_valid_yaml_returns_frozenset(tmp_path):
    p = _write_yaml(tmp_path, {"english": ["the", "a", "an"], "news_domain": ["said"]})
    sw = load_stopwords(p)
    assert isinstance(sw, frozenset)
    assert sw == {"the", "a", "an", "said"}


def test_load_lowercases_entries(tmp_path):
    """Stopwords stored uppercase in YAML are lowercased before insertion —
    case-insensitive matching per Pass E Q1."""
    p = _write_yaml(tmp_path, {"english": ["THE", "A", "An"]})
    sw = load_stopwords(p)
    assert sw == {"the", "a", "an"}


def test_load_strips_whitespace(tmp_path):
    p = _write_yaml(tmp_path, {"english": ["  the  ", "of\n"]})
    sw = load_stopwords(p)
    assert sw == {"the", "of"}


def test_load_missing_file_fails_loud(tmp_path):
    with pytest.raises(StopwordsError, match="not found"):
        load_stopwords(tmp_path / "missing.yaml")


def test_load_invalid_yaml_fails_loud(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: yaml: :", encoding="utf-8")
    with pytest.raises(StopwordsError, match="invalid YAML"):
        load_stopwords(bad)


def test_load_root_not_mapping_fails_loud(tmp_path):
    bad = tmp_path / "list.yaml"
    bad.write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(StopwordsError, match="must be a mapping"):
        load_stopwords(bad)


def test_load_empty_file_returns_empty(tmp_path):
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")
    assert load_stopwords(empty) == frozenset()


def test_load_section_not_a_list_rejected(tmp_path):
    p = _write_yaml(tmp_path, {"english": "not a list"})
    with pytest.raises(StopwordsError, match="english must be a list"):
        load_stopwords(p)


def test_load_non_string_entry_rejected(tmp_path):
    p = _write_yaml(tmp_path, {"english": ["the", 123]})
    with pytest.raises(StopwordsError, match="non-empty strings"):
        load_stopwords(p)


def test_load_empty_string_entry_rejected(tmp_path):
    p = _write_yaml(tmp_path, {"english": ["the", ""]})
    with pytest.raises(StopwordsError, match="non-empty strings"):
        load_stopwords(p)


def test_load_unknown_section_ignored(tmp_path):
    """Forward-compat: unknown top-level keys are ignored, not errors."""
    p = _write_yaml(tmp_path, {
        "english": ["the"],
        "future_section_not_yet_modeled": ["foo", "bar"],
    })
    sw = load_stopwords(p)
    assert sw == {"the"}


def test_load_bundled_seed_file_loads_cleanly():
    """The shipped config/stopwords.yaml must load without error and contain
    a sensible number of entries (~145 per Pass E build greenlight)."""
    path = Path(__file__).resolve().parent.parent / "config" / "stopwords.yaml"
    sw = load_stopwords(path)
    # Sanity range: enough to be useful, not so many that we're filtering signal
    assert 100 <= len(sw) <= 250
    # A handful of expected entries from each category
    assert {"the", "a", "an", "is", "of"}.issubset(sw)            # english
    assert {"said", "reported", "today", "first"}.issubset(sw)    # news_domain
    # And NOT "new" — Mando removed it from the approved draft
    assert "new" not in sw

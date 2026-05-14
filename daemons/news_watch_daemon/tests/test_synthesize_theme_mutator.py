"""Theme YAML mutator tests — tier mapping, dedup, atomicity, rollback."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml as pyyaml

from news_watch_daemon.synthesize.theme_mutator import (
    ThemeMutationError,
    apply_proposal_to_theme,
)
from news_watch_daemon.theme_config import load_theme


# ---------- fixture builder ----------


_BASE_YAML = """\
theme_id: example
display_name: Example Theme
status: active
created_at: 2026-05-01

brief: |
  Multi-line brief block that must be preserved across the
  round-trip. Has \"quotes\" and: punctuation.

  Second paragraph keeps the blank line.

keywords:
  primary:
    - alpha
    - beta
  secondary:
    - gamma
  exclusions: []

tracked_entities:
  tickers:
    - AAPL
  companies: []
  countries: []
  commodities: []
  people: []

alerts:
  velocity_baseline_headlines_per_day: 5.0
"""


@pytest.fixture
def themes_dir(tmp_path: Path) -> Path:
    (tmp_path / "example.yaml").write_text(_BASE_YAML, encoding="utf-8")
    return tmp_path


# ---------- tier mapping ----------


def test_apply_primary_tier(themes_dir):
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="delta",
        suggested_tier="primary",
    )
    theme = load_theme(themes_dir / "example.yaml")
    assert "delta" in theme.keywords.primary


def test_apply_secondary_tier(themes_dir):
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="epsilon",
        suggested_tier="secondary",
    )
    theme = load_theme(themes_dir / "example.yaml")
    assert "epsilon" in theme.keywords.secondary


def test_apply_exclusion_tier_maps_to_exclusions_plural(themes_dir):
    """suggested_tier='exclusion' (singular, per Brief schema) →
    keywords.exclusions (plural, per ThemeConfig schema)."""
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="zeta",
        suggested_tier="exclusion",
    )
    theme = load_theme(themes_dir / "example.yaml")
    assert "zeta" in theme.keywords.exclusions


def test_invalid_tier_rejected(themes_dir):
    with pytest.raises(ThemeMutationError, match="unknown suggested_tier"):
        apply_proposal_to_theme(
            themes_dir,
            theme_id="example",
            proposed_keyword="x",
            suggested_tier="bogus",
        )


# ---------- duplicate detection ----------


def test_duplicate_in_primary_rejected(themes_dir):
    """The keyword is already in primary — refuse to add it again
    (to any tier)."""
    with pytest.raises(ThemeMutationError, match="already exists"):
        apply_proposal_to_theme(
            themes_dir,
            theme_id="example",
            proposed_keyword="alpha",
            suggested_tier="secondary",
        )


def test_duplicate_in_secondary_rejected(themes_dir):
    with pytest.raises(ThemeMutationError, match="already exists"):
        apply_proposal_to_theme(
            themes_dir,
            theme_id="example",
            proposed_keyword="gamma",
            suggested_tier="primary",
        )


def test_duplicate_across_tier_rejected(themes_dir):
    """Even if the user wants to MOVE a keyword to a different tier,
    the mutator rejects — moves require manual edits, not drift
    approvals."""
    with pytest.raises(ThemeMutationError, match="already exists"):
        apply_proposal_to_theme(
            themes_dir,
            theme_id="example",
            proposed_keyword="gamma",
            suggested_tier="exclusion",
        )


# ---------- file existence ----------


def test_missing_theme_file_rejected(tmp_path):
    with pytest.raises(ThemeMutationError, match="not found"):
        apply_proposal_to_theme(
            tmp_path,
            theme_id="missing",
            proposed_keyword="x",
            suggested_tier="primary",
        )


# ---------- formatting preservation ----------


def test_brief_block_preserved(themes_dir):
    """The multi-line `brief: |` block survives the round-trip."""
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="delta",
        suggested_tier="primary",
    )
    content = (themes_dir / "example.yaml").read_text(encoding="utf-8")
    assert "Multi-line brief block" in content
    assert "Second paragraph keeps the blank line." in content


def test_empty_exclusions_list_gets_first_entry(themes_dir):
    """Tier list was `[]` in source — mutator adds the entry without
    breaking the YAML structure."""
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="newly-excluded",
        suggested_tier="exclusion",
    )
    theme = load_theme(themes_dir / "example.yaml")
    assert theme.keywords.exclusions == ["newly-excluded"]


def test_mutation_validates_post_write(themes_dir):
    """After mutation, the file must still load as ThemeConfig.
    Tested by load_theme inside apply_proposal_to_theme."""
    apply_proposal_to_theme(
        themes_dir,
        theme_id="example",
        proposed_keyword="legit",
        suggested_tier="secondary",
    )
    # If this raises, the post-write validation failed silently.
    theme = load_theme(themes_dir / "example.yaml")
    assert theme.theme_id == "example"
    assert "legit" in theme.keywords.secondary


# ---------- rollback on validation failure ----------


def test_rollback_on_validation_failure(tmp_path, monkeypatch):
    """If the post-mutation re-validation fails, restore the original
    file bytes so Mando never sees a broken YAML."""
    yaml_path = tmp_path / "example.yaml"
    yaml_path.write_text(_BASE_YAML, encoding="utf-8")
    original_bytes = yaml_path.read_bytes()

    # Force load_theme to fail after mutation.
    from news_watch_daemon.synthesize import theme_mutator
    from news_watch_daemon.theme_config import ThemeLoadError

    def _broken_load(path):
        raise ThemeLoadError("simulated post-mutation validation failure")

    monkeypatch.setattr(theme_mutator, "load_theme", _broken_load)

    with pytest.raises(ThemeMutationError, match="reverted"):
        apply_proposal_to_theme(
            tmp_path,
            theme_id="example",
            proposed_keyword="x",
            suggested_tier="primary",
        )
    # File contents should be restored to the original.
    assert yaml_path.read_bytes() == original_bytes

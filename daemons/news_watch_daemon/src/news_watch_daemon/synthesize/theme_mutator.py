"""Theme-YAML mutator — apply an approved drift proposal in place.

Pass C Step 11. When Mando runs `proposals approve <id>`, the CLI
calls `apply_proposal_to_theme()` to add the proposed keyword to the
right tier list in the theme YAML.

Uses `ruamel.yaml` (round-trip-safe) so the file's comments, blank
lines, multi-line `brief: |` block, and field ordering are all
preserved. PyYAML would mangle these.

Safety net:
  1. Refuse to apply if the proposal_id's theme_id doesn't exist as
     `<themes_dir>/<theme_id>.yaml`.
  2. Refuse to apply if the proposed_keyword is ALREADY in any of
     primary/secondary/exclusions for the theme (drift orchestrator's
     filter should catch this too, but defense in depth).
  3. After mutation, re-validate the file by reloading via
     `theme_config.load_theme()`. If validation fails, roll back the
     file write (restore the pre-mutation bytes) and surface a clear
     error. Mando never gets a half-broken theme YAML.
  4. Atomic write via tmpfile + os.replace.

Tier → list-field mapping:
  - "primary"    → keywords.primary
  - "secondary"  → keywords.secondary
  - "exclusion"  → keywords.exclusions   (note: schema plural)
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Literal

from ruamel.yaml import YAML

from ..theme_config import ThemeLoadError, load_theme


_LOG = logging.getLogger("news_watch_daemon.synthesize.theme_mutator")


class ThemeMutationError(RuntimeError):
    """Raised when a theme YAML cannot be safely mutated."""


_TIER_TO_FIELD: dict[str, Literal["primary", "secondary", "exclusions"]] = {
    "primary": "primary",
    "secondary": "secondary",
    "exclusion": "exclusions",
}


def _build_yaml() -> YAML:
    """Configure ruamel.yaml for round-trip-safe edits."""
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096  # don't reflow long brief: blocks
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def apply_proposal_to_theme(
    themes_dir: Path,
    *,
    theme_id: str,
    proposed_keyword: str,
    suggested_tier: str,
) -> Path:
    """Append `proposed_keyword` to the correct tier list in the theme YAML.

    Args:
        themes_dir: Directory containing `<theme_id>.yaml` files.
        theme_id: Target theme. Must correspond to an existing file.
        proposed_keyword: The exact string to append.
        suggested_tier: "primary" | "secondary" | "exclusion".

    Returns:
        Path to the mutated YAML file (for reporting).

    Raises:
        ThemeMutationError: invalid tier, missing file, duplicate
            keyword in any tier, or post-mutation re-validation fail.
    """
    if suggested_tier not in _TIER_TO_FIELD:
        raise ThemeMutationError(
            f"unknown suggested_tier {suggested_tier!r}; "
            f"must be one of {sorted(_TIER_TO_FIELD)}"
        )
    target_field = _TIER_TO_FIELD[suggested_tier]

    theme_path = themes_dir / f"{theme_id}.yaml"
    if not theme_path.is_file():
        raise ThemeMutationError(
            f"theme YAML not found: {theme_path}"
        )

    # Capture original bytes for rollback on validation failure.
    original_bytes = theme_path.read_bytes()

    yaml = _build_yaml()
    with theme_path.open("r", encoding="utf-8") as fp:
        data = yaml.load(fp)

    if not isinstance(data, dict):
        raise ThemeMutationError(
            f"theme YAML root must be a mapping at {theme_path}; "
            f"got {type(data).__name__}"
        )

    keywords = data.get("keywords")
    if not isinstance(keywords, dict):
        raise ThemeMutationError(
            f"theme YAML missing 'keywords' mapping at {theme_path}"
        )

    # Cross-check against all three tiers to avoid silent re-insertion.
    for check_field in ("primary", "secondary", "exclusions"):
        existing = keywords.get(check_field) or []
        if proposed_keyword in existing:
            raise ThemeMutationError(
                f"keyword {proposed_keyword!r} already exists in "
                f"keywords.{check_field} for theme {theme_id!r}"
            )

    # Append to target list (create the list if it's missing/empty).
    target_list = keywords.get(target_field)
    if target_list is None:
        keywords[target_field] = [proposed_keyword]
    else:
        target_list.append(proposed_keyword)

    # Atomic write via tmpfile + os.replace.
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{theme_id}-", suffix=".yaml.tmp", dir=str(themes_dir),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            yaml.dump(data, fp)
        os.replace(tmp_name, theme_path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise

    # Re-validate the mutated file. Roll back on failure.
    try:
        load_theme(theme_path)
    except ThemeLoadError as exc:
        # Restore original bytes so Mando doesn't end up with a broken YAML.
        theme_path.write_bytes(original_bytes)
        raise ThemeMutationError(
            f"mutation produced an invalid theme YAML at {theme_path}; "
            f"reverted. Validation error: {exc}"
        ) from exc

    _LOG.info(
        "applied proposal to %s: keyword=%r tier=%s",
        theme_path, proposed_keyword, suggested_tier,
    )
    return theme_path


__all__ = [
    "ThemeMutationError",
    "apply_proposal_to_theme",
]

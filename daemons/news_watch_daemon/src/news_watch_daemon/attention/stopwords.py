"""Stopword loader for attention/counter.py.

Loads a YAML file with `english:` and `news_domain:` lists (other
top-level keys are ignored — extensibility hook). Returns a single
frozenset of lowercase stopwords for fast O(1) membership checks at
the counter's hot path.

Fail-loud on missing file, malformed YAML, wrong root shape, non-string
entries. Backwards-compat behavior: empty list under either section is
fine (e.g. ship news_domain empty, expand later); both sections
entirely absent is also fine (returns empty stopword set — counter
will see lots of noise but won't crash).
"""

from __future__ import annotations

from pathlib import Path

import yaml


class StopwordsError(RuntimeError):
    """Raised when stopwords.yaml cannot be loaded or validated."""


def load_stopwords(path: Path) -> frozenset[str]:
    """Load and validate a stopword YAML file. Returns a frozenset of
    lowercase entries from all known sections.

    Known sections:
      - english:     list[str]
      - news_domain: list[str]

    Unknown top-level keys are ignored (forward-compat — adding a new
    section won't break an old daemon). All entries lowercased before
    insertion into the returned set; case-insensitive matching is the
    Pass E Q1 design decision.
    """
    if not isinstance(path, Path):
        path = Path(path)
    if not path.is_file():
        raise StopwordsError(f"stopwords file not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise StopwordsError(f"invalid YAML in {path}: {exc}") from exc
    if raw is None:
        # Empty file = empty stopword set. Permissive — operator may
        # be in the middle of an edit; counter just sees more noise.
        return frozenset()
    if not isinstance(raw, dict):
        raise StopwordsError(
            f"stopwords root must be a mapping in {path}; got {type(raw).__name__}"
        )
    collected: set[str] = set()
    for section in ("english", "news_domain"):
        entries = raw.get(section)
        if entries is None:
            continue
        if not isinstance(entries, list):
            raise StopwordsError(
                f"{section} must be a list in {path}; got {type(entries).__name__}"
            )
        for item in entries:
            if not isinstance(item, str) or not item.strip():
                raise StopwordsError(
                    f"{section} entries must be non-empty strings in {path}; got {item!r}"
                )
            collected.add(item.strip().lower())
    return frozenset(collected)


__all__ = ["StopwordsError", "load_stopwords"]

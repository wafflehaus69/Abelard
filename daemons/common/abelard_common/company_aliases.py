"""Company-name → ticker resolution for prose name-matching.

Whole-word, case-insensitive matching of company names (e.g. "Nvidia" → NVDA),
universe-gated so a name only resolves when its ticker is a real symbol. A
resolved ticker folds into the same per-post mention set as the symbol/cashtag
paths, so a name and its ticker in one post count once.

Logic only — the ``name<TAB or ws>TICKER`` map data file lives with each
consuming daemon and is passed in by path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class NameResolver:
    """Resolves company names in prose to tickers (whole-word, case-insensitive).

    A name only resolves if its ticker is in the universe — consistency with
    the symbol/bare validation. Folding the resolved ticker into the same
    per-post set means "Nvidia" and "NVDA" in one post count once.
    """

    pattern: "re.Pattern[str]"
    mapping: dict[str, str]

    def tickers_in(self, text: str, universe: frozenset[str] | set[str]) -> set[str]:
        out: set[str] = set()
        if not text:
            return out
        for match in self.pattern.finditer(text):
            ticker = self.mapping.get(match.group(0).lower())
            if ticker and ticker in universe:
                out.add(ticker)
        return out


def load_name_map(path: Path) -> dict[str, str]:
    """Load the ``name<TAB or ws>TICKER`` map. Name lowercased, ticker uppercased.

    The ticker is the final whitespace-delimited token, so multi-word names
    (``home depot HD``) parse correctly. Blank lines and #comments ignored.
    """
    mapping: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.rsplit(None, 1)  # split on the last whitespace run
        if len(parts) != 2:
            continue
        name, ticker = parts[0].strip().lower(), parts[1].strip().upper()
        if name and ticker:
            mapping[name] = ticker
    return mapping


def build_name_resolver(mapping: dict[str, str]) -> NameResolver | None:
    """Compile a whole-word, case-insensitive resolver from a name map."""
    if not mapping:
        return None
    # Longest names first so multi-word names win over any shorter prefix.
    names = sorted(mapping.keys(), key=len, reverse=True)
    pattern = re.compile(
        r"\b(?:" + "|".join(re.escape(n) for n in names) + r")\b",
        re.IGNORECASE,
    )
    return NameResolver(pattern=pattern, mapping=mapping)

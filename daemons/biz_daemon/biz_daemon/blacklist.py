"""Curated /biz/-slang blacklist loader.

The blacklist rejects bare-token ticker candidates that collide with /biz/
slang (FUD, DD, ATH, ...). An explicit `$CASHTAG` bypasses it — see
`extractor`. The list is config-file backed so it can grow after live scrapes
without a code change.
"""

from __future__ import annotations

from pathlib import Path

from .config import BizDaemonError


class BlacklistError(BizDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="blacklist")


def load_blacklist(path: Path) -> frozenset[str]:
    """Load the uppercase slang denylist. Blank lines and #comments ignored.

    Tokens are uppercased on load so denylist enforcement is case-insensitive
    against the (also-uppercased) bare candidates in the extractor.
    """
    if not path.exists():
        raise BlacklistError(f"blacklist file not found: {path}")

    tokens: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        token = line.strip()
        if not token or token.startswith("#"):
            continue
        tokens.add(token.upper())
    return frozenset(tokens)


_CLI_SECTION = "# --- added via CLI ---"


def _normalize_tokens(tokens: list[str]) -> list[str]:
    """Uppercase, strip, drop blanks, dedupe preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        u = tok.strip().upper()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def add_tokens(path: Path, tokens: list[str]) -> tuple[list[str], list[str]]:
    """Append new denylist tokens. Uppercases, dedupes vs the file, appends.

    Returns (added, skipped) where skipped were already present. The file is
    re-read fresh on the next scrape, so the change takes effect immediately.
    """
    normalized = _normalize_tokens(tokens)
    existing = load_blacklist(path) if path.exists() else frozenset()
    added = [t for t in normalized if t not in existing]
    skipped = [t for t in normalized if t in existing]

    if added:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        chunk = ""
        if text and not text.endswith("\n"):
            chunk += "\n"
        if _CLI_SECTION not in text:
            chunk += f"\n{_CLI_SECTION}\n"
        chunk += "".join(f"{t}\n" for t in added)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(chunk)
    return added, skipped


def remove_tokens(path: Path, tokens: list[str]) -> list[str]:
    """Remove denylist token lines from the file. Returns the tokens removed."""
    if not path.exists():
        return []
    targets = set(_normalize_tokens(tokens))
    removed: list[str] = []
    kept: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and stripped.upper() in targets:
            removed.append(stripped.upper())
            continue
        kept.append(line)
    path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    # dedupe preserving order
    seen: set[str] = set()
    return [t for t in removed if not (t in seen or seen.add(t))]


def load_common_words(path: Path) -> frozenset[str]:
    """Load the lowercased common-English-word set for the wordlist filter."""
    if not path.exists():
        raise BlacklistError(f"common-words file not found: {path}")

    words: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        word = line.strip()
        if not word or word.startswith("#"):
            continue
        words.add(word.lower())
    return frozenset(words)

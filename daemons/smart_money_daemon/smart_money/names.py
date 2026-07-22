"""Person-name canonicalization. House PTR filer names leak honorifics
(Honorable, Dr, Mrs, Hon, MD, FACS) and punctuation/whitespace drift, which
splits one politician into several person rows. Canonical key groups them;
display cleanup picks a tidy label. Generational suffixes (Jr, Sr, II-IV) are
REAL name parts and are preserved — they distinguish different people."""
import re

TITLE_TOKENS = {
    "honorable", "hon", "dr", "mr", "mrs", "ms", "mx", "rep", "sen",
    "senator", "representative", "the", "md", "facs", "phd", "esq",
}


def canonical_key(name: str) -> str:
    """Lowercase token key with honorifics and punctuation removed. Used only
    for grouping, never displayed."""
    n = re.sub(r"[.,]", " ", name)
    toks = [t for t in n.split() if t and t.lower() not in TITLE_TOKENS]
    return " ".join(toks).lower()


def display_name(name: str) -> str:
    """Human-facing cleanup: drop honorific tokens, collapse whitespace, keep
    the 'Last, First' comma if present."""
    had_comma = "," in name
    parts = re.split(r"\s*,\s*", name)
    cleaned = []
    for part in parts:
        toks = [
            t for t in part.split()
            if t and re.sub(r"[.]", "", t).lower() not in TITLE_TOKENS
        ]
        if toks:
            cleaned.append(" ".join(toks))
    if had_comma and len(cleaned) >= 2:
        return cleaned[0] + ", " + " ".join(cleaned[1:])
    return " ".join(cleaned) if cleaned else name.strip()

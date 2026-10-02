"""Registered verdict constants (MR-14 ruling 5). The runner refuses any query that matches one.

Rule: a definition of WHAT IS COUNTED may live in a query (top-10 holders, the 0.001 SOL floor,
the +/-24 h window, the entry lags). Anything that decides PASS OR FAIL may not: it is applied
locally, to numbers the query returns. This module is the list of the second kind, as patterns.

It is a tripwire, not a proof. It catches a registered constant used in a comparison (either
side of the operator, or in BETWEEN), the registered products, and the one registered
column-against-column verdict. It cannot recognise a verdict nobody registered, so every new
threshold ruled is added here in the same commit that records the ruling.

Comments and quoted literals are removed before matching: dates, intervals and addresses are
quoted, and none of them is a verdict.

There is no override flag. A query that trips it is rewritten to return the number.
"""
from __future__ import annotations

import re

_OP = r"(?:[<>]=?|<>|!=)"              # a comparison, not '='. 'g = 15' selects a lag; '>= 15' judges
_L = r"(?<![\w.])"                     # the constant starts here: 5 but not 15, x5 or .5
_R = r"(?![\w.])"                      # and ends here: 5 but not 50, 5.5 or 5e6

# (name, the constant as a regex alternation, where it was ruled)
CONSTANTS: list[tuple[str, str, str]] = [
    ("S6 holder concentration 40% (sensitivity 30 / 50); S9b round-trip share 30%",
     r"(?:0?\.[345]0*|[345]0(?:\.0+)?)", "M0_TASKING s4; VALIDATION_1_5_S6, S9_S10"),
    ("S7 linked supply 15%", r"(?:0?\.150*|15(?:\.0+)?)", "M0_TASKING s4; VALIDATION_1_5_S7_S8"),
    ("S8 bundle / syndicate size 5", r"5(?:\.0+)?", "M0_TASKING s4, s6 H2; MR-14 ruling 5"),
    ("Depth floor F 20%", r"(?:0?\.20*|20(?:\.0+)?)", "MR-4; H3_AMENDMENT_v1.1; MR-14 order 5"),
    ("Funder fan-out 400 (sensitivity 32 / 8,192)", r"(?:400|32|8192)(?:\.0+)?", "MR-12 ruling 4"),
    ("H5 winner 3x, and the 2x reach share; any group-size cut of 2 or 3", r"[23](?:\.0+|e0)?", "MR-13 (H5); the burned-week breach"),
]
# Shapes that are a verdict whatever the operator
OTHER: list[tuple[str, str, str]] = [
    ("S9a 95th percentile", _L + r"0?\.950*" + _R, "M0_TASKING s4; MR-8"),
    ("H5 winner 3x / 2x written as a product",
     _L + r"[23](?:\.0+|e0)?\s*\*\s*[A-Za-z_(]|[A-Za-z_)]\s*\*\s*[23](?:\.0+|e0)?" + _R, "MR-13 (H5)"),
    ("Price at 7 days judged against the entry price (H5 survival; 'above entry')",
     r"px_7d\s*" + _OP + r"\s*(?:[A-Za-z_]+\.)?px_g\d+|px_g\d+\s*" + _OP + r"\s*(?:[A-Za-z_]+\.)?px_7d", "MR-13 (H5)"),
]


def _forms(const: str) -> str:
    c = _L + const + _R
    return "|".join([
        _OP + r"\s*" + c,                                   # x >= 5
        c + r"\s*" + _OP,                                   # 5 <= x
        r"\bBETWEEN\s+" + c,                                # BETWEEN 5 AND y
        r"\bBETWEEN\s+[^\s()]+\s+AND\s+" + c,               # BETWEEN x AND 5
    ])


REGISTERED: list[tuple[str, str, str]] = [(n, _forms(c), src) for n, c, src in CONSTANTS] + OTHER
_COMPILED = [(name, re.compile(pat, re.IGNORECASE), src) for name, pat, src in REGISTERED]


def strip(sql: str) -> str:
    """Remove -- comments and blank out quoted literals, keeping line numbers."""
    out = []
    for line in sql.split("\n"):
        line = re.sub(r"'[^']*'", "''", line)       # literals first, so a '--' inside one is not a comment
        out.append(re.sub(r"--.*$", "", line))
    return "\n".join(out)


def hits(sql: str) -> list[tuple[str, int, str]]:
    """(registered name, line number, matched text) for every match outside comments and literals."""
    out = []
    for no, line in enumerate(strip(sql).split("\n"), 1):
        for name, rx, _src in _COMPILED:
            for m in rx.finditer(line):
                out.append((name, no, m.group(0).strip()))
    return out


if __name__ == "__main__":
    import pathlib
    import sys
    bad = 0
    for f in sys.argv[1:]:
        h = hits(pathlib.Path(f).read_text(encoding="utf-8"))
        bad += bool(h)
        print(f"{'REFUSED' if h else 'ok     '} {f}")
        for name, no, text in h:
            print(f"    line {no}: {text!r}  <- {name}")
    sys.exit(3 if bad else 0)

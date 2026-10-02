"""Registered verdict constants (MR-14 ruling 5). No query that matches one is submitted to Dune.

Rule: a definition of WHAT IS COUNTED may live in a query (top-10 holders, the 0.001 SOL floor,
the +/-24 h window, the entry lags). Anything that decides PASS OR FAIL may not: it is applied
locally, to numbers the query returns. This module is the list of the second kind, as patterns.

It is a tripwire, not a proof. It catches a registered constant next to a comparison (either
side, through parentheses, CAST or a unary plus, across line breaks), in BETWEEN, or multiplied
into one side of a comparison; and the registered column-against-column verdict in its direct,
ratio and difference forms. It cannot recognise a verdict nobody registered, and it cannot see
through arithmetic that hides the constant (4 * supply < 10 * top10). So: every new threshold
is added here in the commit that records its ruling, and a query is still read by a person.

Comments and quoted literals are removed before matching: dates, intervals and addresses are
quoted, and none of them is a verdict. Block comments are refused outright, because a stray
apostrophe inside one can hide the code that follows it.

There is no override flag. A query that trips it is rewritten to return the number.
The check runs in two places: recon/dune_run_sql.py (for the message) and recon/dune_roundtrip.dune,
the one function every request to Dune passes through, saved queries and views included.
"""
from __future__ import annotations

import re

_OP = r"(?:[<>]=?|<>|!=)"              # a comparison, not '='. 'g = 15' selects a lag; '>= 15' judges
_L = r"(?<![\w.])"                     # the constant starts here: 5 but not 15, x5 or .5
_EXP = r"(?:[eE]\+?0+)?"               # 5e0 is 5
_R = r"(?![\w.])"                      # and ends here: 5 but not 50, 5.5 or 5e6
_WRAP = r"(?:\s|\(|\+|CAST\s*\()*"     # what may stand between an operator and its constant

# (name, the constant as a regex alternation, where it was ruled)
CONSTANTS: list[tuple[str, str, str]] = [
    ("S6 holder concentration 40% (sensitivity 30 / 50); S9b round-trip share 30%",
     r"(?:0?\.[345]0*|[345]0(?:\.0+)?)", "M0_TASKING s4; VALIDATION_1_5_S6, S9_S10"),
    ("S7 linked supply 15%", r"(?:0?\.150*|15(?:\.0+)?)", "M0_TASKING s4; VALIDATION_1_5_S7_S8"),
    ("S8 bundle / syndicate size 5", r"5(?:\.0+)?", "M0_TASKING s4, s6 H2; MR-14 ruling 5"),
    ("S9a 95th percentile, percent scale", r"95(?:\.0+)?", "M0_TASKING s4; MR-8"),
    ("Depth floor F 20%", r"(?:0?\.20*|20(?:\.0+)?)", "MR-4; H3_AMENDMENT_v1.1; MR-14 order 5"),
    ("Funder fan-out 400 (sensitivity 32 / 8,192)", r"(?:400|32|8192)(?:\.0+)?", "MR-12 ruling 4"),
    ("H5 winner 3x, and the 2x reach share; any group-size cut of 2 or 3", r"[23](?:\.0+)?", "MR-13 (H5); the burned-week breach"),
]
_PX = r"(?:\w+\.)?px_7d"
_PG = r"(?:\w+\.)?px_g\d+"
# Shapes that are a verdict whatever the constant
OTHER: list[tuple[str, str, str]] = [
    ("S9a 95th percentile", _L + r"0?\.950*" + _EXP + _R, "M0_TASKING s4; MR-8"),
    ("Price at 7 days judged against the entry price (H5 survival; 'above entry')",
     "|".join([
         _PX + r"\s*" + _OP + _WRAP + r"(?:COALESCE\s*\(\s*)?" + _PG,          # px_7d > px_g240, > (px_g240), > COALESCE(px_g240, ..
         _PG + r"\s*" + _OP + _WRAP + r"(?:COALESCE\s*\(\s*)?" + _PX,
         _PX + r"\s*[/-]\s*" + _PG + r"\s*\)?\s*" + _OP,                      # px_7d / px_g240 > 1 ; px_7d - px_g240 > 0
         _PG + r"\s*[/-]\s*" + _PX + r"\s*\)?\s*" + _OP,
     ]), "MR-13 (H5)"),
    ("Count cut written one below its registered value (> 4 for the bundle five, > 399 for fan-out 400)",
     r"(?:>|<=)" + _WRAP + _L + r"(?:4|399)(?:\.0+)?" + _EXP + _R + r"|" + _L + r"(?:4|399)(?:\.0+)?" + _EXP + _R + r"\s*(?:<(?!=|>)|>=)",
     "MR-14 ruling 5; MR-12 ruling 4"),
    ("Block comment (refused: it can hide the code after it)", r"/\*", "MR-14 order 3"),
]


def _forms(const: str) -> str:
    c = _L + r"(?:" + const + r")" + _EXP + _R
    return "|".join([
        _OP + _WRAP + c,                                    # x >= 5 ; >= (5) ; >= CAST(5 AS ..) ; >= +5
        c + r"\s*\)?\s*" + _OP,                             # 5 <= x
        r"\bBETWEEN" + _WRAP + c,                           # BETWEEN 5 AND y
        r"\bBETWEEN\b[^;]{0,200}?\bAND" + _WRAP + c,        # BETWEEN x AND 5, whatever x is
        c + r"\s*\*",                                       # 0.4 * supply ; 3 * px
        r"\*\s*" + c,                                       # supply * 0.4 ; px_g15 * 3
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
    """(registered name, line number, matched text) for every match outside comments and literals.
    Patterns run over the whole text, so a comparison wrapped across a line break is still seen."""
    text = strip(sql)
    out = []
    for name, rx, _src in _COMPILED:
        for m in rx.finditer(text):
            out.append((name, text.count("\n", 0, m.start()) + 1, " ".join(m.group(0).split())))
    return sorted(out, key=lambda h: (h[1], h[0]))


def refuse(sql: str, what: str = "query") -> None:
    """Raise SystemExit(3) with the hit list if the text carries a registered verdict constant."""
    h = hits(sql)
    if h:
        lines = "\n".join(f"   line {no}: {text!r}  <- {name}" for name, no, text in h)
        raise SystemExit(f"REFUSED: {what} carries {len(h)} registered verdict constant(s); "
                         f"the cut is applied locally, not in Dune\n{lines}")


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

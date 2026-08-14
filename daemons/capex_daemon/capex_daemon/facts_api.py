"""Leg A — the SEC XBRL aggregation API (companyfacts).

Cheap, keyless, deep history, and structurally blind in three ways (E6):
dimension-qualified facts and custom-namespace facts are permanently absent,
and freshly filed periods lag by days. Leg A is the history spine; Leg B
(``ixbrl``) is the correctness and freshness path.

Values here are absolute integers — companyfacts exposes no ``scale``
attribute — so the unit basis is ``api-absolute``. That is the one G1 hazard
this leg does NOT carry and the filing path does (E5).
"""
import json

SCALE_BASIS_API = "api-absolute"


class ApiFact:
    __slots__ = ("concept", "taxonomy", "unit", "value", "period_start",
                 "period_end", "duration_days", "form", "filed", "frame")

    def __init__(self, concept, taxonomy, unit, value, period_start, period_end,
                 duration_days, form, filed, frame):
        self.concept = concept
        self.taxonomy = taxonomy
        self.unit = unit
        self.value = value
        self.period_start = period_start
        self.period_end = period_end
        self.duration_days = duration_days
        self.form = form
        self.filed = filed
        self.frame = frame

    @property
    def is_duration(self):
        return self.period_start is not None

    def __repr__(self):
        return "ApiFact({} {}..{} {} {})".format(
            self.concept, self.period_start, self.period_end, self.value, self.unit)


def _days(start, end):
    from datetime import date
    try:
        return (date.fromisoformat(end) - date.fromisoformat(start)).days
    except (TypeError, ValueError):
        return None


def load_companyfacts(path):
    """Parse a companyfacts JSON document into {concept: [ApiFact, ...]}.

    Only the ``us-gaap`` taxonomy is indexed by concept name here; ``dei``,
    ``srt``, ``ecd`` and ``ffd`` are carried under their own keys so a caller
    that wants filing-fee facts can still reach them.
    """
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return index_facts(doc)


def index_facts(doc):
    out = {}
    for taxonomy, concepts in (doc.get("facts") or {}).items():
        for concept, body in concepts.items():
            key = concept if taxonomy == "us-gaap" else "{}:{}".format(taxonomy, concept)
            rows = out.setdefault(key, [])
            for unit, arr in (body.get("units") or {}).items():
                for x in arr:
                    start = x.get("start")
                    end = x.get("end")
                    if end is None:
                        continue
                    rows.append(ApiFact(
                        concept, taxonomy, unit, x.get("val"), start, end,
                        _days(start, end) if start else None,
                        x.get("form"), x.get("filed"), x.get("frame")))
    return out


def entity_name(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh).get("entityName")


def dedupe_latest_filed(facts):
    """Collapse restatements: one fact per (start, end, unit), latest ``filed`` wins.

    The same period recurs across filings as a comparative; the newest filing
    carries the authoritative value and, often, the ``frame`` label the original
    lacked. Keying must be on the PERIOD, never on ``fy``/``fp`` — those describe
    the report a fact appeared in, not the period it covers (measured: META's
    FY2023 fact carries fy=2025).
    """
    best = {}
    for f in facts:
        key = (f.period_start, f.period_end, f.unit)
        cur = best.get(key)
        if cur is None or (f.filed or "") >= (cur.filed or ""):
            best[key] = f
    return sorted(best.values(), key=lambda f: (f.period_end, f.period_start or ""))

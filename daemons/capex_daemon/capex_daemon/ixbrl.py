"""Filing-level XBRL parsing — Leg B, the primitive the aggregation API cannot replace.

Two source shapes, one Fact record:

  * **Extracted instance** (``<name>_htm.xml``): plain XBRL. Values are absolute;
    there is no ``scale`` attribute. Basis recorded as ``instance-absolute``.
  * **Inline XBRL** (the primary ``.htm``): ``ix:nonFraction`` elements carrying
    ``scale`` and ``sign``. A displayed "329.1" with ``scale="9"`` means
    329,100,000,000. Ignoring ``scale`` is wrong by nine orders of magnitude, so
    it is read explicitly and the basis recorded as ``ixbrl-scale-attr`` (E5).

Dimensioned facts are first-class (E6): companyfacts returns only the
undimensioned fact for a concept/period, which is how Meta's $45.95B Louisiana
VIE exposure hides behind a $5.58B "other VIEs" figure. Both are stored here as
distinct rows, discriminated by ``dim_key``.

**Nested-fact collapse.** iXBRL permits one ``ix:nonFraction`` to wrap another so
a single displayed number carries several contexts. Microsoft's $329.1B
not-yet-commenced lease figure is wrapped twice — ``LeaseContractualTermAxis``
to ``FinanceLeaseMember`` and to ``OperatingLeaseMember`` — with the *identical*
value. There is no finance/operating split to recover; counting both overstates
by exactly 2x. Nested facts are collapsed to the outermost, and the collapsed
contexts are retained on the surviving row so the loss is visible, not silent.
Collapse is decided **structurally** (element ancestry), never by value equality,
so two genuinely distinct segment facts that happen to share a value both survive.
"""
import re
import xml.etree.ElementTree as ET

XBRLI = "http://www.xbrl.org/2003/instance"
XBRLDI = "http://xbrl.org/2006/xbrldi"
IX_NAMESPACES = (
    "http://www.xbrl.org/2013/inlineXBRL",
    "http://www.xbrl.org/2008/inlineXBRL",
)

SCALE_BASIS_INSTANCE = "instance-absolute"
SCALE_BASIS_IXBRL = "ixbrl-scale-attr"
SCALE_BASIS_UNDETERMINED = "undetermined"

_ENTITY_RE = re.compile(rb"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)[A-Za-z][A-Za-z0-9]*;")


class Fact:
    __slots__ = ("taxonomy", "concept", "value", "unit", "scale", "scale_basis",
                 "period_start", "period_end", "dims", "context_ref",
                 "collapsed_context_refs", "decimals")

    def __init__(self, taxonomy, concept, value, unit, scale, scale_basis,
                 period_start, period_end, dims, context_ref, decimals=None):
        self.taxonomy = taxonomy
        self.concept = concept
        self.value = value
        self.unit = unit
        self.scale = scale
        self.scale_basis = scale_basis
        self.period_start = period_start
        self.period_end = period_end
        self.dims = dims or {}
        self.context_ref = context_ref
        self.collapsed_context_refs = []
        self.decimals = decimals

    @property
    def dim_key(self):
        """Canonical serialization of the axis->member set. '' when undimensioned."""
        if not self.dims:
            return ""
        return ";".join("{}={}".format(a, m) for a, m in sorted(self.dims.items()))

    @property
    def is_dimensioned(self):
        return bool(self.dims)

    def __repr__(self):
        return "Fact({}:{} {}..{} {} {} dims={})".format(
            self.taxonomy, self.concept, self.period_start, self.period_end,
            self.value, self.unit, self.dim_key or "-")


def _local(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _split_qname(text, nsmap=None):
    """'us-gaap:Foo' -> ('us-gaap', 'Foo'). Bare names get taxonomy ''."""
    if not text:
        return "", ""
    text = text.strip()
    if ":" in text:
        pfx, local = text.split(":", 1)
        return pfx, local
    return "", text


def _sanitize(raw):
    """Neutralize bare HTML entities so XHTML parses as XML."""
    return _ENTITY_RE.sub(b"", raw)


def _read(source):
    """Accept a path, raw bytes, or an already-fetched document STRING.

    `edgar.fetch_document` returns text, so a caller wiring the fetcher straight
    into the parser hands this a `str` of XML. Treating every `str` as a path
    made that fail as `No such file or directory: '<?xml version="1.0"...'` — a
    message that names the content it could not find, which is confusing enough
    that it survived a full test suite and only surfaced on the first live run.
    A string that opens with an XML declaration or a tag is content, not a path;
    nothing else could be, since no path starts with `<`.
    """
    if isinstance(source, (bytes, bytearray)):
        return bytes(source)
    if isinstance(source, str) and source.lstrip()[:1] == "<":
        # Encode the STRIPPED text: an XML declaration is only valid at the very
        # start of the entity, so a stray leading newline from a fetch would
        # otherwise turn a detected document into a parse error.
        return source.lstrip().encode("utf-8")
    with open(source, "rb") as fh:
        return fh.read()


def _parse_contexts(root):
    """context id -> (period_start, period_end, {axis: member})."""
    out = {}
    for ctx in root.iter("{%s}context" % XBRLI):
        cid = ctx.get("id")
        period = ctx.find("{%s}period" % XBRLI)
        start = end = None
        if period is not None:
            s = period.find("{%s}startDate" % XBRLI)
            e = period.find("{%s}endDate" % XBRLI)
            i = period.find("{%s}instant" % XBRLI)
            if i is not None:
                start, end = None, (i.text or "").strip()
            else:
                start = (s.text or "").strip() if s is not None else None
                end = (e.text or "").strip() if e is not None else None
        dims = {}
        for m in ctx.iter("{%s}explicitMember" % XBRLDI):
            axis = m.get("dimension") or ""
            member = (m.text or "").strip()
            if axis:
                dims[_split_qname(axis)[1]] = _split_qname(member)[1]
        # Typed dimensions carry their member as element CONTENT, not as a QName
        # attribute. The SEC filing-fee taxonomy discriminates offering tranches
        # this way — ffd:OfferingAxis -> <dei:lineNo>1</dei:lineNo> — so a parser
        # reading only explicitMember collapses every tranche of a multi-tranche
        # note offering into one indistinguishable context.
        for m in ctx.iter("{%s}typedMember" % XBRLDI):
            axis = m.get("dimension") or ""
            if not axis:
                continue
            value = ""
            for child in m:
                value = (child.text or "").strip()
                if value:
                    break
            dims[_split_qname(axis)[1]] = value or "?"
        out[cid] = (start, end, dims)
    return out


def _parse_units(root):
    """unit id -> measure string, e.g. 'USD' or 'USD/shares'."""
    out = {}
    for u in root.iter("{%s}unit" % XBRLI):
        uid = u.get("id")
        measures = [_split_qname((m.text or "").strip())[1]
                    for m in u.iter("{%s}measure" % XBRLI)]
        divide = u.find("{%s}divide" % XBRLI)
        if divide is not None:
            num = [_split_qname((m.text or "").strip())[1]
                   for m in divide.iter("{%s}measure" % XBRLI)]
            out[uid] = "/".join(num) if num else "?"
        else:
            out[uid] = measures[0] if measures else "?"
    return out


def _to_number(text, scale, sign):
    if text is None:
        return None
    t = text.strip().replace(",", "").replace(" ", "").replace(" ", "")
    if t in ("", "-", "—"):
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    if scale:
        v *= 10 ** scale
    if sign == "-":
        v = -v
    return v


def parse_instance(source):
    """Parse an extracted XBRL instance (``*_htm.xml``). Values are absolute."""
    root = ET.fromstring(_sanitize(_read(source)))
    contexts = _parse_contexts(root)
    units = _parse_units(root)
    facts = []
    for el in root:
        tag = el.tag
        if tag.startswith("{%s}" % XBRLI) or tag.startswith("{%s}" % XBRLDI):
            continue
        cref = el.get("contextRef")
        if not cref or cref not in contexts:
            continue
        uref = el.get("unitRef")
        value = _to_number(el.text, None, None)
        if value is None:
            continue
        start, end, dims = contexts[cref]
        taxonomy = _taxonomy_from_uri(tag)
        facts.append(Fact(taxonomy, _local(tag), value, units.get(uref, uref or "?"),
                          None, SCALE_BASIS_INSTANCE, start, end, dims, cref,
                          el.get("decimals")))
    return facts


_TAXONOMY_HINTS = (
    ("us-gaap", "us-gaap"), ("/dei/", "dei"), ("/srt/", "srt"),
    ("xbrl.sec.gov/ffd", "ffd"), ("xbrl.sec.gov/ecd", "ecd"),
)


def _taxonomy_from_uri(tag):
    if "}" not in tag:
        return ""
    uri = tag[1:].split("}", 1)[0]
    for hint, name in _TAXONOMY_HINTS:
        if hint in uri:
            return name
    return uri.rsplit("/", 2)[-2] if "/" in uri else uri


def parse_ixbrl(source):
    """Parse an inline-XBRL document. Honors ``scale``/``sign``; collapses nesting."""
    root = ET.fromstring(_sanitize(_read(source)))
    contexts = _parse_contexts(root)
    units = _parse_units(root)

    nf_tags = {"{%s}nonFraction" % ns for ns in IX_NAMESPACES}
    parents = {}
    for parent in root.iter():
        for child in parent:
            parents[child] = parent

    nodes = [el for el in root.iter() if el.tag in nf_tags]
    nodeset = set(id(n) for n in nodes)

    def outermost_ancestor(el):
        """Nearest enclosing nonFraction, or None. Structural, never value-based."""
        cur = parents.get(el)
        while cur is not None:
            if id(cur) in nodeset:
                return cur
            cur = parents.get(cur)
        return None

    facts_by_el = {}
    nested_children = []
    for el in nodes:
        if outermost_ancestor(el) is not None:
            nested_children.append(el)
            continue
        f = _fact_from_ix(el, contexts, units)
        if f is not None:
            facts_by_el[id(el)] = f

    # Attach each nested child's context to its surviving outermost ancestor so
    # the collapse is recorded rather than silently dropped.
    for el in nested_children:
        anc = outermost_ancestor(el)
        while anc is not None and id(anc) not in facts_by_el:
            anc = outermost_ancestor(anc)
        if anc is not None:
            surviving = facts_by_el[id(anc)]
            cref = el.get("contextRef")
            if cref and cref not in surviving.collapsed_context_refs:
                surviving.collapsed_context_refs.append(cref)
    return list(facts_by_el.values())


def _fact_from_ix(el, contexts, units):
    cref = el.get("contextRef")
    if not cref or cref not in contexts:
        return None
    scale_raw = el.get("scale")
    scale = int(scale_raw) if scale_raw not in (None, "") else None
    basis = SCALE_BASIS_IXBRL if scale is not None else SCALE_BASIS_UNDETERMINED
    text = "".join(el.itertext())
    value = _to_number(text, scale, el.get("sign"))
    if value is None:
        return None
    taxonomy, concept = _split_qname(el.get("name"))
    uref = el.get("unitRef")
    start, end, dims = contexts[cref]
    return Fact(taxonomy, concept, value, units.get(uref, uref or "?"),
                scale, basis, start, end, dims, cref, el.get("decimals"))


def select(facts, concept=None, period_end=None, dimensioned=None, unit=None):
    """Filter helper for tests and callers. Exact matching only, no fuzziness."""
    out = facts
    if concept is not None:
        out = [f for f in out if f.concept == concept]
    if period_end is not None:
        out = [f for f in out if f.period_end == period_end]
    if unit is not None:
        out = [f for f in out if f.unit == unit]
    if dimensioned is not None:
        out = [f for f in out if f.is_dimensioned == dimensioned]
    return out

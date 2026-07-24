"""Human-readable rendering for FullBriefEnvelope.

Per Abelard's Full Brief spec Section 6 + Adjustments 1 and 3
(2026-05-29). The render layer is the single human-facing surface of
the entire daemon — every analytical layer below exists to feed it.

Design discipline (Mando's 2b-i forward-guidance):
  - SCANNABILITY BEATS COMPLETENESS. Mando reads this at 7am. Highest-
    signal items first (orphan crossings, dominant narrative); lower-
    signal items recede.
  - Orphan attention crossings render FIRST after the narrative, as a
    dedicated highlighted section. They appear AGAIN in the Pass E
    section's complete listing with an ORPHAN tag. Twice on purpose —
    once for review priority, once for completeness.
  - "THESES DOC: NOT WIRED" warning lives at the section header for
    theme_synthesis when the doc isn't wired, NOT buried in metadata.
  - Near-miss soft cap of 50 rows per Adjustment 1. When more terms
    qualify, render the top 50 with an explicit truncation footer.
    The JSON envelope always carries the full list.
  - threshold_note (Adjustment 2) renders in the FREQUENCY DIAGNOSTIC
    section header when populated (window_hours != 24).

Implementation note: structural rendering only — no logic computation.
The orchestrator at fullbrief/orchestrator.py produces a complete
FullBriefEnvelope; this module just composes the text.
"""

from __future__ import annotations


from .brief import (
    AttentionCrossing,
    AttentionSynthesisSection,
    ExecutiveSummary,
    FrequencyDiagnosticNearMissRow,
    FrequencyDiagnosticSection,
    FullBriefEnvelope,
    FullBriefEnvelopeHealth,
    PassFFootprint,
    ThemeEventDigest,
    ThemeSegmentsSection,
    ThemeSynthesisSection,
)


# Near-miss render cap per Adjustment 1. The JSON envelope holds the full
# list; the render truncates with a footer pointing back at the JSON.
NEAR_MISS_RENDER_CAP = 50

# Section divider widths (Section 6 sample).
_HEAVY_DIVIDER = "=" * 80
_LIGHT_DIVIDER = "-" * 80


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def render_full_brief(envelope: FullBriefEnvelope) -> str:
    """Render a FullBriefEnvelope as scannable human-readable text.

    Sections in order:
      1. Header (brief_id + window)
      2. Narrative (theme_synthesis.narrative or no_trigger text)
      3. Summary metrics line (dominant themes / material events / orphans)
      4. ORPHAN ATTENTION CROSSINGS (only if any orphans exist) — review first
      5. THEME-EVENT SYNTHESIS (Pass C section, with theses warning if not wired)
      6. ATTENTION SYNTHESIS (Pass E section — all crossings incl. orphans
         with ORPHAN tag)
      7. FREQUENCY DIAGNOSTIC — near-miss table, with threshold_note in header
         if populated, and truncation footer if > 50 rows
      8. PASS F FOOTPRINT
      9. ENVELOPE HEALTH
      10. Footer (artifact path + divider)
    """
    parts: list[str] = []
    parts.append(_render_header(envelope))
    parts.append("")
    parts.append(_render_narrative(envelope))
    parts.append("")
    parts.append(_render_summary_line(envelope.executive_summary))
    parts.append("")

    orphan_section = _render_orphan_section(envelope.attention_synthesis)
    if orphan_section:
        parts.append(orphan_section)
        parts.append("")

    theme_segments_section = _render_theme_segments(envelope.theme_segments)
    if theme_segments_section:
        parts.append(theme_segments_section)
        parts.append("")

    parts.append(_render_theme_synthesis(envelope.theme_synthesis))
    parts.append("")
    parts.append(_render_attention_synthesis(envelope.attention_synthesis))
    parts.append("")
    parts.append(_render_frequency_diagnostic(envelope.frequency_diagnostic))
    parts.append("")
    parts.append(_render_pass_f_footprint(envelope.pass_f_footprint))
    parts.append("")
    pass_failures_section = _render_pass_failures(envelope.pass_failures)
    if pass_failures_section:
        parts.append(pass_failures_section)
        parts.append("")
    parts.append(_render_envelope_health(envelope.envelope_health, envelope.cost))
    parts.append("")
    parts.append(_render_footer(envelope))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Header / Narrative / Summary
# ---------------------------------------------------------------------------


def _render_header(envelope: FullBriefEnvelope) -> str:
    w = envelope.window
    return (
        f"{_HEAVY_DIVIDER}\n"
        f"FULL BRIEF — {envelope.brief_id}\n"
        f"Window: {w.since} → {w.until} ({w.duration_hours}h)\n"
        f"{_HEAVY_DIVIDER}"
    )


def _render_narrative(envelope: FullBriefEnvelope) -> str:
    return f"NARRATIVE\n{envelope.executive_summary.narrative}"


def _render_summary_line(summary: ExecutiveSummary) -> str:
    """Compact one-or-two-line summary metrics block."""
    themes_part = (
        f"DOMINANT THEMES: {', '.join(summary.dominant_themes)}"
        if summary.dominant_themes
        else "DOMINANT THEMES: (none)"
    )
    counts_part = (
        f"MATERIAL EVENTS: {summary.material_event_count} above threshold "
        f"| ATTENTION CROSSINGS: {summary.attention_crossings_count} "
        f"({summary.orphan_crossings_count} orphan)"
    )
    materiality_part: str
    if summary.highest_materiality_score is not None:
        materiality_part = (
            f"HIGHEST MATERIALITY: {summary.highest_materiality_score:.2f}"
        )
    else:
        materiality_part = "HIGHEST MATERIALITY: (no events)"
    return f"{themes_part}\n{counts_part}\n{materiality_part}"


# ---------------------------------------------------------------------------
# Orphan section (highlighted top-of-brief — Mando's "review first")
# ---------------------------------------------------------------------------


def _render_orphan_section(attention: AttentionSynthesisSection) -> str:
    """Return the orphan-crossings-highlight section, or empty string if
    no orphans exist. When present, this renders BEFORE Pass C synthesis
    per Section 6 rendering rules — the highest-value diagnostic surfaces
    first."""
    orphans = [c for c in attention.crossings if c.convergence.status == "orphan"]
    if not orphans:
        return ""

    lines: list[str] = [
        _LIGHT_DIVIDER,
        "ORPHAN ATTENTION CROSSINGS — review first",
        _LIGHT_DIVIDER,
    ]
    for c in orphans:
        lines.append(
            f"  {c.term} ({c.freq_window}/{c.freq_prior}, {c.shape})"
        )
        # Full llm_read_summary (whitespace-flattened) — the orphans are the
        # "review first" items, so the operator wants the whole LLM read here,
        # not a one-line teaser (2026-07-08).
        summary = " ".join((c.llm_read_summary or "").split())
        if summary:
            lines.append(f"    {summary}")
    return "\n".join(lines)


def _first_line_excerpt(text: str, max_chars: int = 200) -> str:
    """Compact inline excerpt — strip newlines, cap length."""
    if not text:
        return ""
    flat = " ".join(text.split())
    if len(flat) > max_chars:
        return flat[:max_chars].rstrip() + "..."
    return flat


# ---------------------------------------------------------------------------
# Theme segments — guaranteed per-theme coverage (every tracked theme)
# ---------------------------------------------------------------------------


def _render_theme_segments(section: ThemeSegmentsSection) -> str:
    """Every-tracked-theme roll-up. Active segments first (by tag count),
    then quiet. A theme that is active but fell outside Pass C scope is
    flagged — that's the hot-but-dropped signal this section exists to
    surface. Returns empty string when the section is skipped/empty."""
    if section.status == "skipped" or not section.segments:
        return ""

    header = "THEME SEGMENTS — every tracked theme"
    if section.status == "failed":
        header = f"{header}  |  DEGRADED ({section.failure_reason or 'unknown'})"
    elif section.llm_degraded:
        header = f"{header}  |  summaries degraded to templates"

    lines: list[str] = [_LIGHT_DIVIDER, header, _LIGHT_DIVIDER]

    ordered = sorted(
        section.segments,
        key=lambda s: (s.status != "active", -s.tagged_headline_count, s.theme_id),
    )
    for seg in ordered:
        tag = "ACTIVE" if seg.status == "active" else "quiet "
        flag = ""
        if seg.status == "active" and not seg.in_pass_c_scope:
            flag = "  (hot — outside Pass C scope)"
        lines.append(
            f"  [{tag}] {seg.display_name} ({seg.theme_id}) — "
            f"{seg.tagged_headline_count} tagged{flag}"
        )
        summary = _first_line_excerpt(seg.summary, max_chars=320)
        if summary:
            lines.append(f"    {summary}")
        if seg.convergence_terms:
            lines.append(f"    attention: {', '.join(seg.convergence_terms)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pass C theme-event synthesis section
# ---------------------------------------------------------------------------


def _render_theme_synthesis(section: ThemeSynthesisSection) -> str:
    """Theme synthesis section with theses warning at the section header
    when not wired (Mando's "section header, not metadata footer")."""
    # Header — append theses warning inline when theses_doc_available=False.
    header = "THEME-EVENT SYNTHESIS (Pass C)"
    if section.theses_doc_warning:
        header = f"{header}  |  THESES DOC: NOT WIRED ({section.theses_doc_warning})"

    lines: list[str] = [_LIGHT_DIVIDER, header, _LIGHT_DIVIDER]

    if section.status == "no_trigger":
        # Per Q2 resolution: narrative carries the informational-not-error text.
        lines.append(
            "  Pass C trigger gate did not fire — no theme crossed materiality "
            "threshold this window. This is informational, not an error."
        )
        if section.no_trigger_reason:
            lines.append(f"  Gate reason: {section.no_trigger_reason}")
        return "\n".join(lines)

    if section.status == "failed":
        lines.append(f"  Pass C failed: {section.failure_reason or 'unknown'}")
        return "\n".join(lines)

    # status == "ok" — render events in materiality-descending order.
    if not section.events:
        lines.append("  (no events surfaced)")
    else:
        events_sorted = sorted(
            section.events,
            key=lambda ev: ev.materiality_score,
            reverse=True,
        )
        for ev in events_sorted:
            lines.append(_render_event_compact(ev))

    if section.direction_tally:
        tally = section.direction_tally
        lines.append("")
        lines.append(
            f"  Direction tally: "
            f"{tally.get('confirm', 0)} confirm | "
            f"{tally.get('ambiguous', 0)} ambiguous | "
            f"{tally.get('break', 0)} break"
        )

    if section.brief_id:
        lines.append(f"  Pass C brief: {section.brief_id}")

    return "\n".join(lines)


def _render_event_compact(ev: ThemeEventDigest) -> str:
    """One event in materiality-descending compact form.

      evt-1  0.88  confirm     us_iran_escalation
        {headline_summary}
        Sources: ...
    """
    direction = ev.direction or "—"
    themes = ", ".join(ev.themes) if ev.themes else "(no themes)"
    lines: list[str] = [
        f"  {ev.event_id}  {ev.materiality_score:.2f}  {direction:<10}  {themes}",
        f"    {ev.headline_summary}",
        f"    Sources: {ev.source_count}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pass E attention synthesis section (all crossings, orphan tag inline)
# ---------------------------------------------------------------------------


def _render_attention_synthesis(section: AttentionSynthesisSection) -> str:
    """Pass E section — full crossing list. Orphans appear here with
    ORPHAN tag in addition to the dedicated orphan section above.

    Mando: "Pass E crossings render with convergence-tagged inline,
    orphans separated above" — orphan section above + complete listing
    here. Twice-listing is deliberate per Section 6 rendering rules.
    """
    lines: list[str] = [
        _LIGHT_DIVIDER,
        "ATTENTION SYNTHESIS (Pass E)",
        _LIGHT_DIVIDER,
    ]

    if section.status == "failed":
        lines.append(f"  Pass E failed: {section.failure_reason or 'unknown'}")
        return "\n".join(lines)

    if not section.crossings:
        lines.append("  (no crossings this cycle)")
        return "\n".join(lines)

    for c in section.crossings:
        lines.append(_render_crossing_compact(c))

    return "\n".join(lines)


def _render_crossing_compact(c: AttentionCrossing) -> str:
    """One crossing in compact form:

      iranian      12/2   multi_source_convergence  → converges with evt-1, evt-2
      south        13/2   cross_topic_recurrence    → ORPHAN
    """
    freq_part = f"{c.freq_window}/{c.freq_prior}"
    base = f"  {c.term:<12} {freq_part:>6}   {c.shape:<28}"
    if c.convergence.status == "convergent":
        merges = ", ".join(c.convergence.converges_with)
        return f"{base} → converges with {merges}"
    if c.convergence.status == "orphan":
        return f"{base} → ORPHAN"
    return f"{base} → unknown"


# ---------------------------------------------------------------------------
# Frequency diagnostic — near-miss table with threshold_note + truncation
# ---------------------------------------------------------------------------


def _render_frequency_diagnostic(section: FrequencyDiagnosticSection) -> str:
    """Frequency diagnostic with near-miss table.

    Header carries threshold_note (Adjustment 2) when populated. Body
    renders up to NEAR_MISS_RENDER_CAP (50) rows; if more qualify, a
    truncation footer points consumers to the JSON envelope's full list
    (Adjustment 1)."""
    header = "FREQUENCY DIAGNOSTIC — near-miss terms (sustained-attention signal)"
    if section.threshold_note:
        header = f"{header}\n  {section.threshold_note}"

    lines: list[str] = [_LIGHT_DIVIDER, header, _LIGHT_DIVIDER]

    if not section.near_misses:
        lines.append("  No elevated terms in window (quiet cycle)")
        if section.diagnostic_note:
            lines.append("")
            lines.append(f"  Note: {section.diagnostic_note}")
        return "\n".join(lines)

    # Table header.
    lines.append(
        f"  {'Term':<16} {'Window':>6} {'Prior':>6} {'Δ-ratio':>8}   Reason"
    )
    lines.append(
        f"  {'-'*16} {'-'*6} {'-'*6} {'-'*8}   {'-'*20}"
    )

    total = len(section.near_misses)
    visible_rows = section.near_misses[:NEAR_MISS_RENDER_CAP]
    for nm in visible_rows:
        lines.append(_render_near_miss_row(nm))

    if total > NEAR_MISS_RENDER_CAP:
        lines.append("")
        lines.append(
            f"  Truncated at {NEAR_MISS_RENDER_CAP} of {total} qualifying "
            f"terms; full list in JSON envelope under "
            f"frequency_diagnostic.near_misses."
        )

    if section.diagnostic_note:
        lines.append("")
        lines.append(f"  Note: {section.diagnostic_note}")

    return "\n".join(lines)


def _render_near_miss_row(nm: FrequencyDiagnosticNearMissRow) -> str:
    return (
        f"  {nm.term:<16} {nm.freq_window:>6} {nm.freq_prior:>6} "
        f"{nm.delta_ratio:>8.2f}   {nm.reason_not_crossed}"
    )


# ---------------------------------------------------------------------------
# Pass F footprint
# ---------------------------------------------------------------------------


def _render_pass_f_footprint(footprint: PassFFootprint) -> str:
    lines: list[str] = [
        _LIGHT_DIVIDER,
        "PASS F FOOTPRINT",
        _LIGHT_DIVIDER,
        f"  Translated rows in window: {footprint.translated_rows_in_window}",
        f"  Cross-language event merges: {footprint.cross_language_event_merges}",
    ]
    enabled = footprint.attention_crossings_enabled_by_pass_f
    enabled_text = ", ".join(enabled) if enabled else "(none)"
    lines.append(f"  Attention crossings enabled by Pass F: {enabled_text}")
    # Defensive audit signal — only render when populated (the abnormal case).
    if footprint.url_match_warnings is not None:
        lines.append(
            f"  URL match warnings: {footprint.url_match_warnings} "
            f"(source_headline URLs did not match DB rows — audit signal)"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Envelope health + cost
# ---------------------------------------------------------------------------


def _render_pass_failures(failures: list) -> str:   # noqa: ANN001 — list[PassFailure]
    """Render the pass_failures section ONLY when non-empty.

    Operational degraded-path diagnostic — surfaces when any orchestration
    step failed-but-recovered (archive_failed Pass C, pass_f_footprint DB
    failure, Pass E attention error, etc.). The orchestrator's "always
    returns a valid envelope" invariant means the brief can assemble while
    the daemon is in degraded mode; this section is the visible signal.

    Returns empty string when pass_failures is empty — clean days produce
    no noise. Surfaced position is right before ENVELOPE HEALTH so the
    operator sees both the per-step OK/FAILED labels AND the recovered-
    failure reasons in the same scan zone.
    """
    if not failures:
        return ""
    lines: list[str] = [
        _LIGHT_DIVIDER,
        "PASS FAILURES (degraded paths — brief still assembled)",
        _LIGHT_DIVIDER,
    ]
    for pf in failures:
        recovered = "recovered" if pf.recovered else "NOT RECOVERED"
        lines.append(f"  {pf.step}: {pf.reason} [{recovered}]")
    return "\n".join(lines)


def _render_envelope_health(
    health: FullBriefEnvelopeHealth,
    cost: "Any",   # CostEnvelope — Any here to avoid extra import / circular
) -> str:
    lines: list[str] = [
        _LIGHT_DIVIDER,
        "ENVELOPE HEALTH",
        _LIGHT_DIVIDER,
        (
            f"  Scrape: {_health_label(health.scrape)} | "
            f"Pass C: {_health_label(health.pass_c)} | "
            f"Pass E: {_health_label(health.pass_e)} | "
            f"Convergence: {_health_label(health.convergence_analysis)} | "
            f"Frequency: {_health_label(health.frequency_diagnostic)}"
        ),
    ]
    if health.scrape.headlines_inserted is not None:
        sf = health.scrape.sources_failed
        sf_text = f" | Sources failed: {sf}" if sf is not None else ""
        lines.append(
            f"  Headlines this scrape: {health.scrape.headlines_inserted}{sf_text}"
        )
    if cost is not None:
        lines.append(
            f"  Cost: ${cost.total_usd:.3f} ({cost.model}, rates as of {cost.rates_as_of})"
        )
    return "\n".join(lines)


def _health_label(step) -> str:   # noqa: ANN001 — StepHealth, no-import-cycle dance
    """OK / FAILED / SKIPPED uppercase label."""
    return step.status.upper()


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------


def _render_footer(envelope: FullBriefEnvelope) -> str:
    artifact = f"Artifact: {envelope.brief_id}.json"
    return f"{artifact}\n{_HEAVY_DIVIDER}"


__all__ = [
    "NEAR_MISS_RENDER_CAP",
    "render_full_brief",
]

"""Scan spine: window alignment, per-source dispatch, failure ownership.

Division of responsibility, and it is the reason adapters stay simple:

  ADAPTERS are leaf modules, TOTAL over valid inputs. They return what they
  extracted. They do not decide whether a source is healthy, whether a
  watermark advances, or what a failure means.

  THE ORCHESTRATOR owns every failure case. One adapter raising must not take
  the scan down -- a scan that dies on source 3 of 14 loses eleven healthy
  sources to one broken parser.

WINDOW ALIGNMENT: `now_unix` is computed ONCE here and passed down. Nothing
downstream calls `time.time()`. Two sources fetched a second apart with two
different "now" values produce windows that disagree at the boundary, and the
resulting gap is invisible.

NO LLM IN THIS PHASE. Scripts-first: Phase 1 ingests and measures. Nothing here
classifies, scores, or ranks -- and no threshold exists yet, because
measure-before-mandate means the distribution comes first.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field

import json

from . import classify, config, ledger, risk, state
from abelard_common.dedupe import compute_dedupe_hash

from .identity import compute_opportunity_id
from .errors import ClassificationError
from .fetch import build_client
from . import models
from .models import RawItem
from .sources import ADAPTERS

# First-run lookback when a source has no watermark yet. 90 days: long enough
# to see a quarter of a slow grant program's cadence, short enough that a first
# scan is not an unbounded backfill of a 910-row directory.
DEFAULT_LOOKBACK_S = 90 * 24 * 3600


@dataclass
class SourceReport:
    """What one source did this scan -- the Phase 1 deliverable, per source."""

    source: str
    status: str
    item_count: int = 0
    detail: str = ""
    recon_field_fit: float = 0.0
    observed_field_fit: float = 0.0
    hits: dict[str, int] = field(default_factory=dict)
    watermark_advanced: bool = False
    elapsed_s: float = 0.0

    @property
    def divergence(self) -> float:
        """Observed minus predicted, in percentage points. Negative = shortfall."""
        return self.observed_field_fit - self.recon_field_fit


@dataclass
class ScanReport:
    scan_id: str
    now_unix: int
    sources: list[SourceReport] = field(default_factory=list)
    items: list[RawItem] = field(default_factory=list)
    # Title collisions, NOT necessarily cross-source: Phase 1 measurement
    # showed most are duplicate titles WITHIN one board (Questbook reposts).
    # Renamed from `cross_source_collisions` because the old name oversold
    # what the mechanism detects.
    title_collisions: dict[str, list[str]] = field(default_factory=dict)
    classes: dict[str, int] = field(default_factory=dict)
    disagreements: list[dict] = field(default_factory=list)
    cost: "classify.CostRecord | None" = None
    inserted: int = 0
    updated: int = 0
    promotions: int = 0
    veto_rates: dict = field(default_factory=dict)
    residual_vetoes: list = field(default_factory=list)
    risk_bands: list[tuple[str, int]] = field(default_factory=list)
    dead_zone: list[dict] = field(default_factory=list)

    @property
    def totals(self) -> dict[str, int]:
        return {
            "ok": sum(1 for s in self.sources if s.status == "ok"),
            "empty": sum(1 for s in self.sources if s.status == "empty"),
            "error": sum(1 for s in self.sources if s.status == "error"),
            "items": len(self.items),
        }


def _measure_field_fit(items: list[RawItem]) -> tuple[float, dict[str, int]]:
    """Percent of items carrying title + payout + category.

    Same three-field gate SC-R1 used, so predicted and observed are the same
    metric rather than two things sharing a name.
    """
    if not items:
        return 0.0, {"title": 0, "payout": 0, "category": 0}
    hits = {"title": 0, "payout": 0, "category": 0}
    complete = 0
    for item in items:
        marks = item.extraction_hits()
        for key, value in marks.items():
            hits[key] += int(value)
        complete += int(all(marks.values()))
    return round(100.0 * complete / len(items), 1), hits


def _classify_and_persist(
    conn, report: ScanReport, log: logging.Logger, *, use_llm: bool
) -> None:
    """Classify every ingested item and write the ledger.

    Ordering here is doctrine, not preference:
      1. mechanical pass over everything (free, deterministic)
      2. ONE batched LLM pass over the ambiguous middle only
      3. cost telemetry PERSISTED
      4. ledger rows persisted

    Step 3 precedes step 4 because a disk failure on the ledger must not lose
    the record of money already spent at the API. The tokens were burned
    either way.
    """
    ledger.apply_schema(conn)

    keys = [compute_opportunity_id(i.source, i.native_id) for i in report.items]
    mechanical = [classify.mechanical_classify(i) for i in report.items]

    # WHAT GOES TO THE LLM, and why it is not just "the ambiguous middle".
    #
    # Ambiguous items go, obviously. Mechanically-GREEN items ALSO go, and that
    # is the important part: GREEN is the only verdict where a mechanical miss
    # is unrecoverable. A novel fraud phrasing the lexicon does not match lands
    # GREEN and is never reviewed again -- the exact shape of the YesWeHack
    # failure, one layer down. Sending GREEN for veto means the LLM can only
    # move an item DOWN (resolve() never resolves upward), so this strictly
    # tightens classification and can never loosen it.
    #
    # Mechanical RED and YELLOW do NOT go. Both are already the safe direction,
    # resolve() would not let the LLM lift them anyway, and paying to confirm a
    # verdict that cannot change is waste.
    #
    # A first pass sent only the 2 genuinely-ambiguous items and produced a
    # near-empty disagreement set -- no calibration evidence at all, which the
    # order names as the phase's deliverable. That is what surfaced the flaw.
    to_llm = [
        (item, key)
        for item, key, verdict in zip(report.items, keys, mechanical)
        if verdict.legitimacy_class is None or verdict.legitimacy_class == classify.GREEN
    ]
    ambiguous_n = sum(1 for v in mechanical if v.legitimacy_class is None)
    log.info(
        "mechanical: %d GREEN, %d YELLOW, %d RED, %d ambiguous -> %d to LLM",
        sum(1 for v in mechanical if v.legitimacy_class == classify.GREEN),
        sum(1 for v in mechanical if v.legitimacy_class == classify.YELLOW),
        sum(1 for v in mechanical if v.legitimacy_class == classify.RED),
        ambiguous_n, len(to_llm),
    )
    ambiguous = to_llm

    llm_verdicts: dict[str, tuple[str, str]] = {}
    cost = classify.CostRecord()
    if ambiguous and use_llm:
        batch_size = config.CLASSIFY_BATCH_SIZE
        for start in range(0, len(ambiguous), batch_size):
            chunk = ambiguous[start : start + batch_size]
            chunk_items = [c[0] for c in chunk]
            chunk_keys = [c[1] for c in chunk]
            # RETRY ONCE ON TRANSPORT FAILURE. Measured 2026-08-19..21: 2 of 5
            # consecutive classify calls failed on first attempt, every one a
            # truncated stream --
            #   RemoteProtocolError('peer closed connection without sending
            #   complete message body (incomplete chunked read)')
            # -- on a ~12k-output-token generation. Each failure degraded a
            # whole scan to YELLOW-with-the-failure-as-reason, which is safe but
            # wastes the fetch and, during measurement work, silently produced
            # scans with no judgments in them.
            #
            # A retry is legitimate here ONLY because the failure is a dropped
            # transport, not a rejected request: no verdict was returned, so
            # nothing is being overwritten or double-counted. A
            # ClassificationError is NOT retried -- that means the model
            # answered and the answer was unusable, and asking again would just
            # buy a second unusable answer at full price.
            #
            # The existing loud-degrade path stays as the fallback: one retry,
            # then degrade exactly as before. This narrows a window; it does not
            # replace the guarantee.
            exc = None
            for attempt in (1, 2):
                try:
                    verdicts, chunk_cost = classify.classify_batch(
                        chunk_items, chunk_keys, logger=log
                    )
                    llm_verdicts.update(verdicts)
                    exc = None
                    break
                except ClassificationError as unusable:
                    exc = unusable
                    break                      # answered badly; retrying buys nothing
                except Exception as transport:  # noqa: BLE001
                    exc = transport
                    if attempt == 1:
                        cost.transport_retries += 1
                        log.warning(
                            "classification transport failure, retrying once: %r",
                            transport,
                        )
                        continue
            if exc is not None:
                # Deliberately broad. A transport failure from the API (an
                # `APITimeoutError` on a long generation, a 529, a dropped
                # connection) is not a ClassificationError, and on 2026-08-11
                # one of them killed an entire scan on its way out of this
                # call. The orchestrator owns failure cases: a failed batch
                # degrades to YELLOW-with-the-failure-as-reason and the scan
                # continues. The degradation direction is always safe --
                # `resolve()` has no path from a failure to GREEN.
                # Fail-loud, bounded blast radius: this chunk degrades to
                # YELLOW-with-the-failure-as-reason via resolve(). It does not
                # take the scan down and it never silently becomes GREEN.
                log.error("classification chunk failed: %s", exc)
                chunk_cost = classify.CostRecord(items_classified=len(chunk))
            cost.llm_calls += chunk_cost.llm_calls
            cost.input_tokens += chunk_cost.input_tokens
            cost.output_tokens += chunk_cost.output_tokens
            cost.cache_read_tokens += chunk_cost.cache_read_tokens
            cost.cache_creation_tokens += chunk_cost.cache_creation_tokens
            cost.items_classified += chunk_cost.items_classified

    # --- 3. COST BEFORE ARTIFACT ------------------------------------------
    ledger.record_cost(
        conn,
        scan_id=report.scan_id,
        model=cost.model,
        llm_calls=cost.llm_calls,
        input_tokens=cost.input_tokens,
        output_tokens=cost.output_tokens,
        cache_read_tokens=cost.cache_read_tokens,
        cache_creation_tokens=cost.cache_creation_tokens,
        cost_usd=cost.cost_usd,
        transport_retries=cost.transport_retries,
        items_classified=cost.items_classified,
    )
    report.cost = cost

    # --- 4. resolve, then the risk pass over YELLOW -----------------------
    resolved: list[tuple[RawItem, ledger.Classification]] = []
    for item, key, verdict in zip(report.items, keys, mechanical):
        decision = classify.resolve(verdict, llm_verdicts.get(key))

        # Risk scoring runs on YELLOW only, AFTER resolution. Scoring RED
        # would be pointless (no promotion path exists from RED, by design)
        # and scoring GREEN would be scoring something already clear.
        if decision.legitimacy_class == classify.YELLOW:
            assessment = risk.assess(
                item,
                reason_codes=decision.reason_codes,
                mechanical_reason=verdict.reason,
            )
            decision.risk_score = assessment.score
            decision.risk_factors = json.dumps(assessment.breakdown)[:900]
            decision.risk_weights_version = risk.RISK_WEIGHTS_VERSION
            decision.promotion_eligible = assessment.eligible
            decision.promotion_blocked_by = (
                ",".join(assessment.blocked_by) or None
            )
            if risk.should_promote(assessment):
                decision.pre_promotion_class = classify.YELLOW
                decision.promoted_from_yellow = True
                # A DISTINCT class, never plain GREEN. A reader querying
                # legitimacy_class='GREEN' must not silently pick up a
                # risk-scored promotion alongside native clearances.
                decision.legitimacy_class = ledger.GREEN_PROMOTED
                decision.classified_by = "risk-promotion"
                # Canonical scan clock, not a local time.time().
                decision.promoted_unix = report.now_unix
                decision.class_reason = (
                    f"PROMOTED YELLOW->GREEN_PROMOTED: risk {assessment.score} < "
                    f"{risk.PROMOTION_THRESHOLD}; eligible on "
                    f"{','.join(assessment.evaluated_codes)} "
                    f"({assessment.rationale}). Prior: {decision.class_reason}"
                )[:900]
        resolved.append((item, decision))

    report.promotions = sum(1 for _, d in resolved if d.promoted_from_yellow)
    report.inserted, report.updated = ledger.upsert_items(
        conn, resolved, scan_id=report.scan_id, now_unix=report.now_unix
    )
    ledger.record_collisions(
        conn, report.title_collisions,
        scan_id=report.scan_id, now_unix=report.now_unix,
    )

    report.classes = ledger.class_distribution(conn)
    report.risk_bands = ledger.risk_histogram(conn)
    report.veto_rates = ledger.veto_rates(conn)
    report.residual_vetoes = [
        {
            "source": r["source"], "title": r["title"],
            "reason": r["class_reason"],
        }
        for r in ledger.residual_vetoes(conn)
    ]
    report.dead_zone = [
        {
            "source": r["source"], "title": r["title"],
            "risk_score": r["risk_score"],
            "eligible": bool(r["promotion_eligible"]),
        }
        for r in ledger.dead_zone_occupants(conn)
    ]
    report.disagreements = [
        {
            "source": row["source"],
            "title": row["title"],
            "mechanical": row["mechanical_class"],
            "llm": row["llm_class"],
            "resolved": row["legitimacy_class"],
            "reason": row["class_reason"],
        }
        for row in ledger.disagreements(conn)
    ]


def run_scan(
    *,
    only: list[str] | None = None,
    db_path=None,
    logger: logging.Logger | None = None,
    classify_items: bool = False,
    use_llm: bool = True,
) -> ScanReport:
    """Execute one full scan across the roster."""
    log = logger or config.configure_logging()

    if config.fetching_halted():
        # Kill switch is checked BEFORE the client is built and before any
        # socket opens, so a halt is a halt rather than a best-effort request
        # to stop soon.
        log.warning("SCOUT HALT engaged -- no fetching performed")
        return ScanReport(scan_id="halted", now_unix=int(time.time()))

    now_unix = int(time.time())          # the one canonical timestamp
    scan_id = uuid.uuid4().hex[:16]

    config.ensure_state_home()
    conn = state.connect(db_path)
    state.start_scan(conn, scan_id, now_unix)

    client = build_client(log)
    report = ScanReport(scan_id=scan_id, now_unix=now_unix)

    roster = [s.name for s in config.WIRE_SOURCES]
    # Sherlock and YesWeHack are roster members but not in WIRE_SOURCES --
    # they entered under SC-1's white-hat carve-out after the recon roster was
    # frozen. Driving the loop off ADAPTERS keeps the two lists from silently
    # diverging; a registered adapter with no config entry still runs.
    for name in ADAPTERS:
        if name not in roster:
            roster.append(name)
    if only:
        roster = [name for name in roster if name in set(only)]

    for name in roster:
        adapter = ADAPTERS.get(name)
        if adapter is None:
            log.error("no adapter registered for %s", name)
            report.sources.append(SourceReport(name, "error", detail="no adapter"))
            continue

        source_cfg = config.SOURCES_BY_NAME.get(name)
        predicted = source_cfg.recon_field_fit if source_cfg else 0.0
        since_unix = state.since_unix_for_source(
            conn, name, DEFAULT_LOOKBACK_S, now_unix
        )

        started = time.monotonic()
        try:
            result = adapter.fetch(client, now_unix=now_unix, since_unix=since_unix)
        except Exception as exc:                      # orchestrator owns failure
            log.exception("adapter %s raised", name)
            state.record_source_result(
                conn, name, now_unix=now_unix, status="error", item_count=0,
                ingested_high_watermark_unix=None,
                detail=f"{type(exc).__name__}: {exc}",
            )
            report.sources.append(
                SourceReport(
                    name, "error",
                    detail=f"{type(exc).__name__}: {exc}",
                    recon_field_fit=predicted,
                    elapsed_s=round(time.monotonic() - started, 2),
                )
            )
            continue

        elapsed = round(time.monotonic() - started, 2)
        watermark = result.high_watermark_unix()
        state.record_source_result(
            conn, name,
            now_unix=now_unix,
            status=result.status,
            item_count=len(result.items),
            ingested_high_watermark_unix=watermark,
            detail=result.detail,
        )

        observed, hits = _measure_field_fit(result.items)
        report.sources.append(
            SourceReport(
                source=name,
                status=result.status,
                item_count=len(result.items),
                detail=result.detail,
                recon_field_fit=predicted,
                observed_field_fit=observed,
                hits=hits,
                watermark_advanced=bool(
                    result.status == "ok" and result.items and watermark
                ),
                elapsed_s=elapsed,
            )
        )
        report.items.extend(result.items)
        log.info(
            "%s: %s items=%d fit=%.1f%% (recon %.1f%%) %.2fs",
            name, result.status, len(result.items), observed, predicted, elapsed,
        )

    # Duplicate-title OBSERVATION. Invariant 1 forbids dropping, so a
    # collision is recorded and linked; nothing is discarded on its account.
    by_hash: dict[str, list[str]] = {}
    for item in report.items:
        by_hash.setdefault(compute_dedupe_hash(item.title), []).append(
            compute_opportunity_id(item.source, item.native_id)
        )
    report.title_collisions = {h: ids for h, ids in by_hash.items() if len(ids) > 1}

    # Asset class is DERIVED centrally from the currency symbol rather than in
    # each adapter -- one rule, fourteen sources, and it stays honestly labelled
    # as derived. Adapters that know more (Dework publishes a token quantity and
    # a quoted price) fill the finer fields themselves and are not overwritten.
    for item in report.items:
        if item.payout_asset_class is None:
            item.payout_asset_class = models.classify_asset(item.payout_currency)

    if classify_items:
        _classify_and_persist(conn, report, log, use_llm=use_llm)

    totals = report.totals
    state.finish_scan(
        conn, scan_id,
        finished_unix=int(time.time()),
        ok=totals["ok"], empty=totals["empty"],
        error=totals["error"], items=totals["items"],
    )
    conn.close()
    return report


__all__ = ["run_scan", "ScanReport", "SourceReport", "DEFAULT_LOOKBACK_S"]

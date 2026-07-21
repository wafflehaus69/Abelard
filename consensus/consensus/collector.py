"""M1.5 forward collector — the L2 layer (addendum v1.2 §2).

Single-pass design: each invocation (scheduled every ``hot_interval_minutes``
by the OS scheduler) enumerates target-category markets if due, runs the
global lane once, polls due markets up to a per-run budget, and emits an
orchestrator-facing JSON envelope. State lives in the tape DB, so invocations
are stateless processes — the OpenClaw daemon pattern.

Lane design is dictated by the 2026-07-12 measurements (reference memo):

  - The GLOBAL 4k window rolls in ~5 min at ordinary load and ~64 s at the
    Feb-28 burst peak → a global lane can never be the coverage guarantee.
  - Per-MARKET windows roll in >=13.7 min even at the worst observed burst
    concentration → a 2-minute hot-tier poll gives >5x margin. The MARKET
    lane is the guarantee; three tiers (hot/quiet/dormant) plus a per-run
    poll budget bound the request rate under any universe size (measured
    live: the three tags enumerate ~15k markets).
  - The GLOBAL lane is telemetry + stray detection + promotion trigger: any
    tracked market seen in global fills is promoted to hot and polled this
    same invocation, which closes the dormant-market-goes-vertical hole well
    inside its roll time.

Continuity model (review-hardened): a market walk is continuous with the
stored tape only when it reaches the market's PRE-WALK FRONTIER — the newest
fill timestamp recorded by a previous completed walk (``l2_markets.
newest_fill_ts``, which only market-lane polls advance). "We hit a fill we
already stored" is NOT continuity: the global lane stores a market's newest
fills moments before the market walk runs, and treating that as continuity
silently skips the middle of a burst (found in adversarial review).

Rule 1 applied to time: when a walk cannot reach its frontier — window rolled,
offset cap, or a mid-walk error — the unobserved interval is DECLARED as a
gap: logged loudly, recorded in ``l2_gaps``, never bridged. Errored walks
never advance continuity anchors (market frontier, global watermark) without
declaring what they may have missed.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from .errors import DataLayerError
from .fetching import DataLayer
from .models import Trade
from .tape import TapeStore, fill_key_base

_DATA = "polymarket_data"
_GAMMA = "polymarket_gamma"

# Keys sampled from each global poll's newest fills; the next poll walks until
# it re-sees one of them (continuity) or exhausts the window (declared gap).
_GLOBAL_WATERMARK_SAMPLE = 100
_GLOBAL_WATERMARK_KEY = "global_watermark_keys"
_LAST_ENUMERATION_KEY = "last_enumeration_ts"

# A trade cannot be timestamped in the future. A fill whose raw timestamp is
# above now by more than this (clock skew between data-api and here) is corrupt
# upstream data — e.g. a millisecond value where seconds are expected — and must
# never advance the frontier: the frontier is MAX-monotonic, so one far-future
# value would poison it permanently, truncating every later walk to page 0 and,
# via the dedup skip band, silently dropping real fills (re-audit 2026-07-19).
_MAX_CLOCK_SKEW_S = 300


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


class Collector:
    """One invocation's worth of collecting. Construct, call :meth:`run_once`,
    read the envelope."""

    def __init__(self, dl: DataLayer, tape: TapeStore) -> None:
        self.dl = dl
        self.tape = tape
        self.cfg = dl.loaded.config.collector
        self.log = dl.logger

    # -- fetch helpers ---------------------------------------------------------

    def _fetch_trades_page(self, *, market: str | None, offset: int) -> list[Any]:
        params: dict[str, Any] = {"limit": self.cfg.page_size, "offset": offset}
        if market is not None:
            params["market"] = market
        body = self.dl.fetch(
            source=_DATA,
            base_url=self.dl.endpoints.polymarket_data_api,
            endpoint="/trades",
            request_params=params,
            persist=False,  # raw records land in the tape instead (module docstring)
        )
        if not isinstance(body, list):
            raise DataLayerError(
                f"{_DATA}/trades: expected a JSON array, got {type(body).__name__}",
                source=_DATA,
            )
        if self.cfg.request_spacing_ms > 0:
            time.sleep(self.cfg.request_spacing_ms / 1000.0)
        return body

    # -- enumeration lane --------------------------------------------------------

    def enumeration_due(self, now_ts: int) -> bool:
        last = self.tape.get_meta(_LAST_ENUMERATION_KEY)
        if last is None:
            return True
        return now_ts - int(last) >= self.cfg.enumeration_interval_minutes * 60

    def run_enumeration(self, now_ts: int) -> dict[str, Any]:
        """Refresh the market roster from gamma tag pages + adjudicate strays.

        ``last_enumeration_ts`` is stamped only if at least one tag page
        succeeded — a fully-failed enumeration stays due, retries next
        invocation, and surfaces in the envelope instead of going quiet for a
        whole interval on a stale roster."""
        poll_id = self.tape.open_poll(invoked_ts=now_ts, lane="enumeration")
        added = refreshed = 0
        errors: list[str] = []
        any_page_ok = False
        # The open (closed=false) enumeration pages the FULL open set per tag, so
        # a tracked market of a fully-succeeded tag that is absent from seen_open
        # has resolved (or delisted) — the hook the resolution sweep needs.
        seen_open: set[str] = set()
        succeeded_tags: set[str] = set()
        for tag in self.cfg.tags:
            offset = 0
            tag_failed = False
            while True:
                try:
                    events = self.dl.fetch(
                        source=_GAMMA,
                        base_url=self.dl.endpoints.polymarket_gamma_api,
                        endpoint="/events",
                        request_params={
                            "tag_slug": tag, "closed": "false",
                            "limit": self.cfg.gamma_page_limit, "offset": offset,
                        },
                        persist=False,
                    )
                except DataLayerError as exc:
                    errors.append(exc.to_error())
                    tag_failed = True
                    break
                if not isinstance(events, list):
                    # A 200 with a non-list body is a malformed upstream, not
                    # end-of-pagination. Silent acceptance would leave the
                    # roster stale while the envelope says "ok" (review find).
                    errors.append(
                        f"data_layer.{_GAMMA}: /events tag={tag}: expected a JSON "
                        f"array, got {type(events).__name__}"
                    )
                    tag_failed = True
                    break
                any_page_ok = True
                if not events:
                    break
                for ev in events:
                    for m in (ev.get("markets") or []):
                        cid = m.get("conditionId")
                        if not cid:
                            continue
                        seen_open.add(cid)
                        inserted = self.tape.upsert_market(
                            cid,
                            slug=m.get("slug"),
                            question=m.get("question"),
                            tags=tag,
                            source="enumeration",
                            now_ts=now_ts,
                            closed=bool(m.get("closed")),
                            end_date=m.get("endDate"),
                        )
                        added += int(inserted)
                        refreshed += int(not inserted)
                if len(events) < self.cfg.gamma_page_limit:
                    break
                offset += self.cfg.gamma_page_limit
            # Only a tag that paged to its natural end without error can be
            # trusted for "absent => gone"; a fetch failure makes absence
            # ambiguous and must NOT drive a closed-sweep (would falsely retire
            # still-open markets).
            if not tag_failed:
                succeeded_tags.add(tag)

        strays_adopted = self._adjudicate_strays(now_ts, errors)
        resolved_stamped = self._sweep_resolved(now_ts, seen_open, succeeded_tags, errors)

        if any_page_ok:
            self.tape.set_meta(_LAST_ENUMERATION_KEY, str(now_ts))
        else:
            self.log.error(
                "enumeration failed for every tag page; roster left stale and "
                "enumeration stays due (errors: %d)", len(errors),
            )
        self.tape.close_poll(
            poll_id, new_records=added, error="; ".join(errors) or None
        )
        return {"markets_added": added, "markets_refreshed": refreshed,
                "strays_adopted": strays_adopted, "resolved_stamped": resolved_stamped,
                "stamped": any_page_ok, "errors": errors}

    def _adjudicate_strays(self, now_ts: int, errors: list[str]) -> int:
        """Adjudicate markets sighted in the global lane but not tracked.

        Looks the market up open-first-then-closed (gamma hides resolved
        markets by default — verified quirk), so a stray that closed between
        sighting and enumeration is still recognized and adopted with its
        drain window (capturing the fills we personally observed). Every
        decision is logged; a market gamma doesn't know at all stays
        unresolved for retry rather than being silently discarded."""
        strays_adopted = 0
        cap = self.cfg.stray_adjudication_max_per_run
        max_attempts = self.cfg.stray_max_attempts
        for cid, attempts in self.tape.strays_pending_adjudication(limit=cap):
            market_rec: dict[str, Any] | None = None
            closed_lookup = False
            lookup_failed = False
            for extra in ({}, {"closed": "true"}):
                try:
                    markets = self.dl.fetch(
                        source=_GAMMA,
                        base_url=self.dl.endpoints.polymarket_gamma_api,
                        endpoint="/markets",
                        request_params={"condition_ids": cid, **extra},
                        persist=False,
                    )
                except DataLayerError as exc:
                    errors.append(exc.to_error())
                    lookup_failed = True
                    break
                if isinstance(markets, list) and markets:
                    market_rec = markets[0]
                    closed_lookup = bool(extra)
                    break
            if lookup_failed:
                continue  # transient: retry next enumeration, no attempt counted
            if market_rec is None:
                # Unknown to gamma both open and closed. Count the attempt and
                # give up after ``max_attempts`` so a permanently-unknown stray
                # (usually a malformed cid from the global feed) stops being
                # re-fetched every enumeration — that unbounded accumulation was
                # the dominant pass-duration cost (2026-07-20). A real target
                # market is still adopted by the tag-page enumeration walk, so
                # abandoning an unknown cid loses no coverage.
                if attempts + 1 >= max_attempts:
                    self.tape.resolve_stray(cid)
                    self.log.warning(
                        "stray %s abandoned after %d gamma lookups (never known); "
                        "stop re-adjudicating", cid, attempts + 1,
                    )
                else:
                    self.tape.bump_stray_attempt(cid)
                    self.log.info(
                        "stray %s unknown to gamma (attempt %d/%d); will retry",
                        cid, attempts + 1, max_attempts,
                    )
                continue
            cat = (market_rec.get("category") or "").lower()
            if any(t in cat for t in self.cfg.tags) or cat in self.cfg.tags:
                self.tape.upsert_market(
                    cid, slug=market_rec.get("slug"),
                    question=market_rec.get("question"),
                    tags=f"stray:{cat}", source="stray", now_ts=now_ts,
                    closed=closed_lookup or bool(market_rec.get("closed")),
                    end_date=market_rec.get("endDate"),
                )
                strays_adopted += 1
                self.log.info("stray %s adopted (category=%r, closed=%s)",
                              cid, cat, closed_lookup)
            else:
                self.log.info("stray %s adjudicated out-of-scope (category=%r)", cid, cat)
            self.tape.resolve_stray(cid)
        return strays_adopted

    def _sweep_resolved(
        self, now_ts: int, seen_open: set[str], succeeded_tags: set[str],
        errors: list[str],
    ) -> int:
        """Catch tracked markets that RESOLVED by dropping out of the open
        (closed=false) enumeration — the majority of resolutions, since gamma's
        server-side filter omits any whole-event closure (adversarial scout
        2026-07-20). For markets of FULLY-SUCCEEDED tags that were not seen open
        this pass, confirm closure with an explicit ``closed=true`` gamma lookup
        (never infer from absence — Rule 1) and persist the resolution outcome so
        the Detector-A confirmation pass can read the winning side from the tape.

        Bounded per run, most-stale-first — mirrors stray adjudication. Markets
        with a resolution already recorded are skipped; once stamped, the drain
        sweep retires them. A market gamma never confirms closed (delisted /
        purged / unconfirmable) is NOT stamped; instead its sweep_attempts is
        counted and, past ``resolution_sweep_max_attempts``, it ages out of the
        pool (reset if it is ever seen open again) — so the pool provably drains
        and never re-looks-up the same market forever."""
        if not succeeded_tags:
            return 0
        cap = self.cfg.resolution_sweep_max_per_run
        max_attempts = self.cfg.resolution_sweep_max_attempts
        presumed = [
            m for m in self.tape.markets(active_only=True)
            if m.get("tags") in succeeded_tags
            and m["condition_id"] not in seen_open
            and not m.get("resolution")
            # Stop re-looking-up a market gamma never confirms closed (delisted /
            # purged / unconfirmable) after the cap, so the pool drains and can't
            # burn a chain call on it forever — mirrors the stray give-up. Reset
            # on re-list (upsert clears sweep_attempts). (review 2026-07-20)
            and (m.get("sweep_attempts") or 0) < max_attempts
        ]
        presumed.sort(key=lambda m: int(m.get("last_polled_ts") or 0))
        stamped = 0
        for m in presumed[:cap]:
            cid = m["condition_id"]
            try:
                rows = self.dl.fetch(
                    source=_GAMMA,
                    base_url=self.dl.endpoints.polymarket_gamma_api,
                    endpoint="/markets",
                    request_params={"condition_ids": cid, "closed": "true"},
                    persist=False,
                )
            except DataLayerError as exc:
                errors.append(exc.to_error())
                continue  # transient: retry next enumeration, no attempt counted
            rec = rows[0] if isinstance(rows, list) and rows else None
            if rec is None or not bool(rec.get("closed")):
                # Not confirmed closed (gamma empty, or a non-closed record):
                # never stamp on absence. Count the empty confirmation so an
                # unconfirmable market ages out of the sweep pool.
                self.tape.bump_sweep_attempt(cid)
                continue
            resolution = json.dumps(
                {
                    "outcomePrices": rec.get("outcomePrices"),
                    "outcomes": rec.get("outcomes"),
                    "umaResolutionStatus": rec.get("umaResolutionStatus"),
                    "swept_ts": now_ts,
                },
                default=str,
            )
            if self.tape.record_resolution(cid, resolution=resolution, now_ts=now_ts):
                stamped += 1
                self.log.info(
                    "resolution swept: market %s closed (uma=%s)",
                    cid, rec.get("umaResolutionStatus"),
                )
        if stamped:
            self.log.info("resolution sweep stamped %d newly-resolved market(s)", stamped)
        return stamped

    # -- market lane ---------------------------------------------------------------

    def _tier_for(
        self, mkt: dict[str, Any], *, new_fills: int, newest_fill_ts: int, now_ts: int
    ) -> tuple[str, int]:
        """Tier from the JUST-COMPLETED poll's facts (``newest_fill_ts`` is the
        post-walk value — using the stale row value misclassified a market
        whose first fills arrived in this very poll; review find)."""
        t = self.cfg.tiers
        hot_until = int(mkt.get("hot_until_ts") or 0)
        if new_fills >= t.hot_threshold_new_fills:
            hot_until = now_ts + t.hot_ttl_minutes * 60
        if now_ts < hot_until:
            return "hot", hot_until
        if newest_fill_ts and now_ts - newest_fill_ts <= t.quiet_if_fill_within_hours * 3600:
            return "quiet", hot_until
        return "dormant", hot_until

    def market_due(self, mkt: dict[str, Any], now_ts: int) -> bool:
        t = self.cfg.tiers
        interval_min = {
            "hot": t.hot_interval_minutes,
            "quiet": t.quiet_interval_minutes,
            "dormant": t.dormant_interval_minutes,
        }.get(mkt.get("tier") or "dormant", t.dormant_interval_minutes)
        return now_ts - int(mkt.get("last_polled_ts") or 0) >= interval_min * 60

    def poll_market(self, mkt: dict[str, Any], now_ts: int) -> dict[str, Any]:
        """Walk one market newest-first until it reaches the market's pre-walk
        FRONTIER (continuity), the end of history, or the offset cap. Declares
        a gap whenever continuity cannot be established — including on a
        mid-walk error, which must not create a false anchor."""
        cid = mkt["condition_id"]
        poll_id = self.tape.open_poll(invoked_ts=now_ts, lane="market", condition_id=cid)
        # The frontier is the l2_markets row value: only completed market-lane
        # walks advance it, so fills the global lane stored minutes ago do NOT
        # count as continuity (they are above the frontier, not at it).
        frontier = int(mkt.get("newest_fill_ts") or 0)
        had_prior = frontier > 0 or self.tape.has_fills(cid)

        # Dedup fast-path: below (frontier − margin) the tape is contiguous, so
        # those records are re-fetched every poll only to be IGNORE'd. Skip that
        # redundant work; the margin keeps re-inserting the near-frontier band so
        # a late-indexed fill is still captured. Disabled on bootstrap (no
        # frontier) and when the margin is 0. The frontier is clamped to
        # wall-clock first: a corrupt far-future timestamp on the tape must never
        # push the skip band up to (now), which would elide genuinely-new fills.
        margin_min = self.cfg.late_arrival_margin_minutes
        eff_frontier = min(frontier, now_ts)
        skip_below_ts = (eff_frontier - margin_min * 60) if (eff_frontier > 0 and margin_min > 0) else None

        pages = raw_total = new_total = dupes_total = skipped_total = unparsed_total = 0
        presumed_total = 0
        reached_frontier = False
        end_of_history = False
        oldest_seen: int | None = None
        newest_seen: int | None = None  # max ts THIS walk observed (frontier advance)
        error: str | None = None
        occurrence: dict[str, int] = {}  # spans the whole walk (page-boundary dupes)
        try:
            for page_idx in range(self.cfg.max_pages):
                raw = self._fetch_trades_page(market=cid, offset=page_idx * self.cfg.page_size)
                pages += 1
                counts = self.tape.store_page(
                    raw, lane="market", poll_id=poll_id, parsed_by=Trade.from_api,
                    occurrence=occurrence, skip_below_ts=skip_below_ts,
                )
                raw_total += counts["raw"]
                new_total += counts["new"]
                dupes_total += counts["dupes"]
                skipped_total += counts["skipped"]
                unparsed_total += counts["unparsed"]
                presumed_total += counts["presumed_stored"]
                page_oldest: int | None = None
                for r in raw:
                    ts = r.get("timestamp") if isinstance(r, dict) else None
                    if isinstance(ts, int):
                        # A corrupt NON-POSITIVE ts (e.g. a zeroed/null-coerced
                        # field) must not pull page_oldest below the frontier and
                        # falsely trip reached_frontier — that would stop the walk
                        # mid-burst and silently drop the deeper pages (re-audit
                        # 2026-07-19). Symmetric with the future-ts guard below;
                        # store_page still archives the record, only continuity
                        # ignores it.
                        if ts > 0:
                            page_oldest = ts if page_oldest is None else min(page_oldest, ts)
                        # A corrupt future ts must not advance the frontier
                        # (MAX-monotonic → permanent poison). page_oldest is a min
                        # so a high ts never lowers it; only newest_seen needs the
                        # upper bound.
                        if 0 < ts <= now_ts + _MAX_CLOCK_SKEW_S:
                            newest_seen = ts if newest_seen is None else max(newest_seen, ts)
                if page_oldest is not None:
                    oldest_seen = page_oldest if oldest_seen is None else min(oldest_seen, page_oldest)
                if frontier and page_oldest is not None and page_oldest <= frontier:
                    reached_frontier = True
                    break
                if counts["raw"] < self.cfg.page_size:
                    end_of_history = True
                    break
        except DataLayerError as exc:
            error = exc.to_error()

        gap = 0
        # pages == 0 means nothing was fetched or stored: no false anchor was
        # created, the next poll retries from the same frontier — an error is
        # recorded but there is no interval to declare.
        if had_prior and not reached_frontier and not end_of_history and pages > 0:
            # Walk ended (offset cap or mid-walk error) without bridging down
            # to the frontier: the interval (frontier, oldest_seen] was not
            # observed. Declare it — an errored walk that stored newer fills
            # must not become a false continuity anchor for the next poll.
            gap = 1
            reason = (
                "market walk errored before reaching frontier"
                if error is not None
                else "market window rolled past stored tape"
            )
            self.tape.declare_gap(
                lane="market", condition_id=cid,
                lo_ts=frontier or None, hi_ts=oldest_seen,
                declared_ts=now_ts, reason=reason,
            )
            self.log.error(
                "DECLARED GAP market=%s interval=(%s, %s]: %s",
                cid, frontier, oldest_seen, reason,
            )
        if not had_prior and not end_of_history and pages > 0:
            # Bootstrap that couldn't reach the beginning of history (offset
            # cap or error): pre-window history belongs to L1/L3.
            gap = 1
            reason = (
                "bootstrap interrupted by error; continuity below unknown"
                if error is not None
                else "bootstrap truncated at offset cap (pre-history in L1/L3)"
            )
            self.tape.declare_gap(
                lane="market", condition_id=cid,
                lo_ts=None, hi_ts=oldest_seen,
                declared_ts=now_ts, reason=reason,
            )
            self.log.warning(
                "bootstrap gap market=%s: history older than %s unreachable (%s)",
                cid, oldest_seen, reason,
            )

        # Advance the frontier ONLY to what THIS market-lane walk observed —
        # never a cross-lane MAX(timestamp). A pages==0 walk (errored first
        # fetch) saw nothing (newest_seen is None) and must not advance the
        # anchor: otherwise a fill the GLOBAL lane stored above the frontier
        # would seed the new frontier across an interval the market lane never
        # walked, no gap would be declared, and the next poll's dedup skip would
        # silently elide that un-walked hole (adversarial audit 2026-07-19). The
        # frontier only ever moves forward (MAX with the prior value in
        # update_market_poll_state), so a walk that saw only old fills can't
        # regress it.
        newest_after = max(frontier, newest_seen) if newest_seen is not None else frontier
        tier, hot_until = self._tier_for(
            mkt, new_fills=new_total, newest_fill_ts=newest_after, now_ts=now_ts
        )
        self.tape.update_market_poll_state(
            cid, tier=tier, hot_until_ts=hot_until, last_polled_ts=now_ts,
            newest_fill_ts=newest_after, last_new_fills=new_total,
        )
        self.tape.close_poll(
            poll_id, pages=pages, raw_records=raw_total, new_records=new_total,
            dupe_records=dupes_total, skipped_records=skipped_total,
            unparsed_records=unparsed_total, presumed_records=presumed_total,
            overlap_found=int(reached_frontier), gap_declared=gap, error=error,
        )
        return {"condition_id": cid, "pages": pages, "new": new_total,
                "unparsed": unparsed_total, "gap": bool(gap), "tier": tier,
                "presumed_stored": presumed_total, "error": error}

    # -- global lane -----------------------------------------------------------------

    def poll_global(self, now_ts: int, tracked_cids: set[str]) -> dict[str, Any]:
        """Best-effort breadth: walk global /trades until a previously-seen fill
        reappears. Persists fills of tracked markets, records strays, promotes
        tracked markets seen here to hot (the belt-and-suspenders trigger).

        The watermark advances only on a CLEAN walk (continuity established or
        window end reached with no error) — an errored walk keeping the old
        watermark means the next poll re-walks the same territory instead of
        silently skipping what the errored walk never saw (review find)."""
        poll_id = self.tape.open_poll(invoked_ts=now_ts, lane="global")
        prev_raw = self.tape.get_meta(_GLOBAL_WATERMARK_KEY)
        prev_keys: set[str] = set(json.loads(prev_raw)) if prev_raw else set()

        pages = raw_total = new_total = skipped_total = unparsed_total = 0
        overlap = not prev_keys  # first run: nothing to be continuous with
        first_run = not prev_keys
        newest_keys: list[str] = []
        seen_cids: dict[str, int] = {}
        error: str | None = None
        oldest_seen: int | None = None
        occurrence: dict[str, int] = {}
        try:
            for page_idx in range(self.cfg.max_pages):
                raw = self._fetch_trades_page(market=None, offset=page_idx * self.cfg.page_size)
                pages += 1
                if page_idx == 0:
                    newest_keys = [fill_key_base(r) for r in raw[:_GLOBAL_WATERMARK_SAMPLE]
                                   if isinstance(r, dict)]
                counts = self.tape.store_page(
                    raw, lane="global", poll_id=poll_id, parsed_by=Trade.from_api,
                    restrict_condition_ids=tracked_cids, occurrence=occurrence,
                )
                raw_total += counts["raw"]
                new_total += counts["new"]
                skipped_total += counts["skipped"]
                unparsed_total += counts["unparsed"]
                for r in raw:
                    if not isinstance(r, dict):
                        continue
                    cid = r.get("conditionId")
                    if cid:
                        seen_cids[cid] = seen_cids.get(cid, 0) + 1
                    ts = r.get("timestamp")
                    if isinstance(ts, int):
                        oldest_seen = ts if oldest_seen is None else min(oldest_seen, ts)
                if prev_keys and any(fill_key_base(r) in prev_keys for r in raw if isinstance(r, dict)):
                    overlap = True
                    break
                if counts["raw"] < self.cfg.page_size:
                    overlap = overlap or bool(prev_keys)  # reached end of window
                    break
        except DataLayerError as exc:
            error = exc.to_error()

        gap = 0
        if error is None and prev_keys and not overlap:
            gap = 1
            self.tape.declare_gap(
                lane="global", condition_id=None,
                lo_ts=None, hi_ts=oldest_seen,
                declared_ts=now_ts,
                reason="global window rolled between polls (informational; market lane is the guarantee)",
            )
            self.log.warning("global window rolled between polls (hi_ts=%s)", oldest_seen)

        promoted = 0
        strays = 0
        hot_until = now_ts + self.cfg.tiers.hot_ttl_minutes * 60
        for cid, n in seen_cids.items():
            if cid in tracked_cids:
                self.tape.promote_to_hot(cid, hot_until_ts=hot_until)
                promoted += 1
            else:
                self.tape.record_stray(cid, now_ts=now_ts, fills=n)
                strays += 1

        clean_walk = error is None and (overlap or gap)
        if newest_keys and clean_walk:
            self.tape.set_meta(_GLOBAL_WATERMARK_KEY, json.dumps(newest_keys))
        self.tape.close_poll(
            poll_id, pages=pages, raw_records=raw_total, new_records=new_total,
            skipped_records=skipped_total, unparsed_records=unparsed_total,
            overlap_found=int(overlap), gap_declared=gap, error=error,
        )
        return {"pages": pages, "raw": raw_total, "tracked_new": new_total,
                "promoted": promoted, "strays_seen": strays, "gap": bool(gap),
                "first_run": first_run, "unparsed": unparsed_total, "error": error}

    # -- one invocation ---------------------------------------------------------------

    def run_once(self) -> dict[str, Any]:
        started = _now_ts()
        errors: list[str] = []
        enum_result: dict[str, Any] | None = None

        if self.enumeration_due(started):
            enum_result = self.run_enumeration(started)
            errors.extend(enum_result.get("errors") or [])

        # Drain sweep: markets whose close was seen a full drain window ago
        # have been polled through resolution; retire them from rotation.
        deactivated = self.tape.deactivate_drained(
            now_ts=started, drain_seconds=self.cfg.drain_minutes * 60
        )
        if deactivated:
            self.log.info("deactivated %d drained closed market(s)", len(deactivated))

        markets = self.tape.markets(active_only=True)
        tracked = {m["condition_id"] for m in markets}

        glob = None
        if self.cfg.global_lane_enabled:
            glob = self.poll_global(started, tracked)
            if glob.get("error"):
                errors.append(glob["error"])

        # Re-read market rows: the global lane may have promoted tiers.
        markets = self.tape.markets(active_only=True)
        due = [m for m in markets if self.market_due(m, started)]
        # Priority = how overdue a market is RELATIVE TO ITS TIER INTERVAL, not
        # in absolute time. A hot market 30 min past its 2-min interval (15x
        # overdue) must beat a dormant market 6 h past its 360-min interval
        # (1x overdue). Sorting on absolute last-poll age instead lets a large
        # dormant backlog crowd hot markets out of the budget and starve the
        # coverage guarantee (observed on Basilic 2026-07-19: an ~8k dormant
        # backlog held every one of 563 hot markets > 90 min unpolled, median
        # 8 h). Ties within a tier fall back to oldest-first (same interval, so
        # larger age wins) — preserving the original fair-rotation intent.
        t = self.cfg.tiers
        interval_min = {
            "hot": t.hot_interval_minutes,
            "quiet": t.quiet_interval_minutes,
            "dormant": t.dormant_interval_minutes,
        }

        def _overdue_ratio(m: dict[str, Any]) -> float:
            iv = interval_min.get(m.get("tier") or "dormant",
                                  t.dormant_interval_minutes) * 60
            age = started - int(m.get("last_polled_ts") or 0)
            return age / iv if iv else float(age)

        due.sort(key=_overdue_ratio, reverse=True)
        budget = self.cfg.max_markets_per_run
        to_poll = due[:budget]
        polled: list[dict[str, Any]] = []
        gaps = 0
        for mkt in to_poll:
            # Fresh wall-clock per poll, not the pass-start ``started``: a pass
            # can run many minutes (budget + a large tape), and a stale now_ts
            # makes the frontier's future-ts skew bound reject legitimately-recent
            # fills late in the pass, under-advancing the frontier so the next
            # pass needlessly re-walks the tip band (re-audit 2026-07-19).
            r = self.poll_market(mkt, _now_ts())
            polled.append(r)
            gaps += int(r["gap"])
            if r.get("error"):
                errors.append(r["error"])
        if glob and glob.get("gap"):
            gaps += 1
        backlog = len(due) - len(to_poll)
        if backlog:
            # Not a data gap (the fills wait in each market's window), but the
            # scheduler must see rotation pressure explicitly — a silent cap
            # would read as full coverage (Rule 1 adjacent).
            self.log.warning(
                "poll budget %d < due %d: %d market(s) deferred to next invocation",
                budget, len(due), backlog,
            )

        unparsed_total = sum(p.get("unparsed", 0) for p in polled) + (
            (glob or {}).get("unparsed", 0)
        )
        if unparsed_total:
            self.log.warning("archived %d unparseable record(s) this pass", unparsed_total)

        finished = _now_ts()
        rate_hits = self.dl.rate_limits.count_429 if self.dl.rate_limits else 0
        status = "ok"
        if errors or gaps:
            status = "degraded"
        envelope = {
            "daemon": "consensus_collector",
            "schema": 1,
            "status": status,
            "started_ts": started,
            "finished_ts": finished,
            "duration_s": finished - started,
            "result": {
                "enumeration": enum_result,
                "global_lane": glob,
                "markets_tracked": len(markets),
                "markets_due": len(due),
                "markets_polled": len(polled),
                "due_backlog": backlog,
                "markets_deactivated": len(deactivated),
                "new_fills": sum(p["new"] for p in polled) + (glob or {}).get("tracked_new", 0),
                "dedup_skipped": sum(p.get("presumed_stored", 0) for p in polled),
                "unparsed_archived": unparsed_total,
                "gaps_declared": gaps,
                "tiers": {
                    t: sum(1 for m in markets if (m.get("tier") or "dormant") == t)
                    for t in ("hot", "quiet", "dormant")
                },
                "rate_limit_hits": rate_hits,
                "tape": self.tape.stats(),
            },
            "errors": errors,
        }
        return envelope

"""On-demand entry: assemble one scrape and return the output contract.

Sequence (per the build order):
  1. Stamp ONE canonical scrape_ts and thread it everywhere downstream.
  2. fourchan: find /smg/ threads, scrape, clean. (Loud-fail / no-thread here.)
  3. ticker_universe: load (cached) US symbol set.
  4. extractor: per-post ticker hits over all validated tickers.
  5. rank by mention count; flag attention = mentions >= N.
  6. sentiment: Haiku pass on ATTENTION-tier tickers only.
  7. assemble JSON.
  8. capture cost into the payload, THEN persist the snapshot.
  9. return JSON.

The full validated tail is always returned (attention:false, sentiment:null) —
low-mention names are visible, never dropped. `errors` is always present.

AlertSink wiring is intentionally omitted in v1: on-demand invocation makes
Abelard the consumer of the return value, so no autonomous alert is needed.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from abelard_common import company_aliases, fourchan_fetch, ticker_noise
from abelard_common.fourchan_fetch import NoSmgThreadError
from abelard_common.ticker_noise import load_blacklist, load_common_words

from . import sentiment, storage, ticker_universe
from .config import Config

_log = logging.getLogger("biz_daemon.orchestrator")

_MAX_SAMPLE_POST_IDS = 5


def run_scrape(
    cfg: Config,
    *,
    now: int | None = None,
    fetcher: fourchan_fetch.Fetcher | None = None,
    conn: Any = None,
    anthropic_client: Any = None,
) -> dict[str, Any]:
    """Run one scrape and return the §8 output contract as a dict."""
    scrape_ts = int(time.time()) if now is None else int(now)
    errors: list[str] = []

    owns_conn = conn is None
    if conn is None:
        conn = storage.connect(cfg.db_path)
    storage.init_db(conn)

    try:
        if fetcher is None:
            fetcher = fourchan_fetch.Fetcher(
                user_agent=cfg.user_agent, timeout=cfg.http_timeout_s
            )

        # 2. Scrape /smg/. No-thread and hard-fetch failures surface loudly.
        try:
            threads = fourchan_fetch.scrape_smg(fetcher)
        except NoSmgThreadError as exc:
            return _finalize_error(
                conn, cfg, scrape_ts, [exc.to_error()], owns_conn=owns_conn
            )
        except fourchan_fetch.FourchanError as exc:
            return _finalize_error(
                conn, cfg, scrape_ts, [exc.to_error()], owns_conn=owns_conn
            )

        all_posts = [p for t in threads for p in t.posts]
        posts_by_no = {int(p["no"]): p.get("com", "") for p in all_posts}

        # 3. Universe (cached, threaded scrape_ts as the clock).
        universe_result = ticker_universe.load_universe(
            conn,
            api_key=cfg.finnhub_api_key,
            fallback_path=cfg.symbol_fallback_path,
            ttl_s=cfg.universe_ttl_s,
            now=scrape_ts,
        )
        if universe_result.warning:
            errors.append(universe_result.warning)

        # 4. Extract (four-layer bare-token filter + S&P 500 name resolution).
        blacklist = load_blacklist(cfg.blacklist_path)
        common_words = load_common_words(cfg.common_words_path)
        name_resolver = None
        try:
            name_resolver = company_aliases.build_name_resolver(
                company_aliases.load_name_map(cfg.sp500_names_path)
            )
        except Exception as exc:  # missing/broken map: degrade, don't crash
            _log.warning("name map unavailable, skipping name resolution: %s", exc)
            errors.append(f"ticker_universe: name map unavailable ({exc})")
        table = ticker_noise.extract(
            all_posts,
            universe=universe_result.symbols,
            blacklist=blacklist,
            common_words=common_words,
            allowlist=cfg.word_ticker_allowlist,
            name_resolver=name_resolver,
        )

        # 5. Rank. The ATTENTION flag (●) is keyed to N; sentiment eligibility
        #    is decoupled and runs at a lower floor.
        attention_tickers = {
            t for t, hits in table.items() if hits.mention_count >= cfg.attention_n
        }
        sentiment_tickers = {
            t
            for t, hits in table.items()
            if hits.mention_count >= cfg.sentiment_min_mentions
        }

        # 6. Sentiment on the sentiment-eligible set (mentions >= floor).
        cost = sentiment.Cost()
        reads: dict[str, dict[str, Any]] = {}
        if sentiment_tickers:
            client = anthropic_client
            if client is None:
                try:
                    client = sentiment.build_anthropic_client(cfg.anthropic_api_key)
                except sentiment.SentimentError as exc:
                    msg = f"sentiment: {exc}"
                    errors.append(msg)
                    for t in sentiment_tickers:
                        reads[t] = {"error": str(exc)}
                    client = None
            if client is not None:
                outcome = sentiment.run_sentiment(
                    attention_tickers=sentiment_tickers,
                    table=table,
                    posts_by_no=posts_by_no,
                    client=client,
                    model=cfg.haiku_model_id,
                    read_bull_pct=cfg.read_bull_pct,
                    read_bear_pct=cfg.read_bear_pct,
                )
                reads = outcome.reads
                cost = outcome.cost
                errors.extend(outcome.errors)

        # 7. Assemble.
        payload = _assemble(
            scrape_ts=scrape_ts,
            threads=threads,
            table=table,
            attention_tickers=attention_tickers,
            sentiment_tickers=sentiment_tickers,
            reads=reads,
            cost=cost,
            errors=errors,
        )

        # 8. Capture cost into the payload (done above), THEN persist.
        _persist(conn, scrape_ts, payload, cost, now=scrape_ts, errors=payload["errors"])
        return payload
    finally:
        if owns_conn:
            conn.close()


def _assemble(
    *,
    scrape_ts: int,
    threads: list[fourchan_fetch.Thread],
    table: dict[str, ticker_noise.TickerHits],
    attention_tickers: set[str],
    sentiment_tickers: set[str],
    reads: dict[str, dict[str, Any]],
    cost: sentiment.Cost,
    errors: list[str],
) -> dict[str, Any]:
    ranked = sorted(
        table.values(), key=lambda h: (-h.mention_count, h.ticker)
    )
    tickers_out: list[dict[str, Any]] = []
    for hits in ranked:
        attention = hits.ticker in attention_tickers
        # sentiment is decoupled from attention: any ticker at/above the
        # sentiment floor carries a read, even when attention=false.
        sentiment_block = (
            reads.get(hits.ticker) if hits.ticker in sentiment_tickers else None
        )
        tickers_out.append(
            {
                "ticker": hits.ticker,
                "mentions": hits.mention_count,
                "attention": attention,
                "sentiment": sentiment_block,
                "sample_post_ids": sorted(hits.post_ids)[:_MAX_SAMPLE_POST_IDS],
            }
        )

    return {
        "scrape_ts": scrape_ts,
        "threads": [
            {"no": t.no, "subject": t.subject, "post_count": t.post_count}
            for t in threads
        ],
        "tickers": tickers_out,
        "cost": {
            "haiku_calls": cost.haiku_calls,
            "input_tokens": cost.input_tokens,
            "output_tokens": cost.output_tokens,
        },
        "errors": list(errors),
    }


def _persist(
    conn: Any,
    scrape_ts: int,
    payload: dict[str, Any],
    cost: sentiment.Cost,
    *,
    now: int,
    errors: list[str],
) -> None:
    """Persist the snapshot with the FULL cost record. Never raises outward."""
    try:
        storage.persist_snapshot(
            conn,
            scrape_ts=scrape_ts,
            payload=payload,
            cost=cost.as_dict(),
            now=now,
        )
    except Exception as exc:  # a disk failure must not lose the returned object
        _log.error("snapshot persist failed: %s", exc)
        errors.append(f"storage: snapshot persist failed: {exc}")


def _finalize_error(
    conn: Any,
    cfg: Config,
    scrape_ts: int,
    errors: list[str],
    *,
    owns_conn: bool,
) -> dict[str, Any]:
    """Build and persist the contract for a total/empty-scrape failure state."""
    cost = sentiment.Cost()
    payload = _assemble(
        scrape_ts=scrape_ts,
        threads=[],
        table={},
        attention_tickers=set(),
        sentiment_tickers=set(),
        reads={},
        cost=cost,
        errors=errors,
    )
    _persist(conn, scrape_ts, payload, cost, now=scrape_ts, errors=payload["errors"])
    return payload

"""CONSENSUS command-line interface (owner-facing).

Unlike the OpenClaw daemons — whose CLIs emit a JSON envelope for *Abelard* to
parse — this CLI is run by the owner, so it prints human-readable output by
default and offers ``--json`` for a structured summary. Logs go to stderr;
stdout carries only the report.

M1 exposes the ``data`` group: ``smoke`` (the milestone acceptance) plus thin
inspection commands over each fetcher.

Exit code: 0 if everything succeeded, 1 on a config error or if any data source
failed (a "gap"). Gaps are shown, never hidden or filled with fabricated data.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Callable

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is a declared dep, but degrade gracefully.
    load_dotenv = None  # type: ignore[assignment]

from .config import LoadedConfig, configure_logging, load_config
from .errors import ConfigError, DataLayerError
from .fetching import DataLayer, build_data_layer
from .sources_kalshi import get_kalshi_markets
from .sources_polymarket import (
    get_market_meta,
    get_market_trades,
    get_wallet_activity,
    get_wallet_positions,
    get_wallet_trades,
    paginate_market_trades,
    paginate_wallet_trades,
)


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="consensus",
        description="Polymarket winners-circle signal system (read-only, advisory).",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml (overrides default).")
    parser.add_argument("--json", action="store_true", help="Emit a JSON summary on stdout.")
    groups = parser.add_subparsers(dest="group", required=True)

    data = groups.add_parser("data", help="Data-layer inspection and smoke test.")
    data_cmds = data.add_subparsers(dest="command", required=True)

    p = data_cmds.add_parser("smoke", help="Fetch one market, one wallet, one Kalshi page; print counts.")
    p.add_argument("--market", default=None, help="Market condition id (default: config smoke value).")
    p.add_argument("--wallet", default=None, help="Wallet proxy address (default: config smoke value).")
    p.add_argument("--kalshi-limit", type=int, default=None, help="Kalshi markets to fetch.")
    p.add_argument("--trade-limit", type=int, default=100, help="Trades per fetch. Default 100.")

    p = data_cmds.add_parser("trades", help="Fills for a market or a wallet.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--market", help="Market condition id.")
    g.add_argument("--wallet", help="Wallet proxy address.")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--all", action="store_true",
                   help="Paginate to exhaustion (full history; pages of --limit, "
                        "terminating on the raw upstream page length).")
    p.add_argument("--max-records", type=int, default=None,
                   help="With --all: stop after this many parsed records.")

    data_cmds.add_parser("cache-stats",
                         help="Raw-response cache observability: size, rows, per-source ranges.")

    p = data_cmds.add_parser("positions", help="Current holdings for a wallet.")
    p.add_argument("--wallet", required=True)

    p = data_cmds.add_parser("activity", help="Activity feed for a wallet.")
    p.add_argument("--wallet", required=True)
    p.add_argument("--limit", type=int, default=100)

    p = data_cmds.add_parser("market", help="Gamma metadata for a market.")
    p.add_argument("--market", required=True, help="Market condition id.")

    p = data_cmds.add_parser("kalshi", help="List Kalshi markets (public).")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--status", default=None, help="e.g. open, closed, settled.")

    p = data_cmds.add_parser(
        "subgraph",
        help="L1 archival tape: walk on-chain fill events for a market or asset (deep history).",
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--market", help="Condition id: resolves clobTokenIds via gamma, walks both outcome tokens.")
    g.add_argument("--asset", help="One ERC-1155 outcome-token id.")
    p.add_argument("--since", type=int, default=None, help="timestamp_gte (unix seconds UTC).")
    p.add_argument("--until", type=int, default=None, help="timestamp_lt (unix seconds UTC).")
    p.add_argument("--max-records", type=int, default=None)
    p.add_argument("--replay", action="store_true",
                   help="Serve the walk from the response cache only (no network; loud on miss).")

    collect = groups.add_parser(
        "collect",
        help="M1.5 forward collector (L2). `run` is orchestrator-facing and always emits a JSON envelope.",
    )
    collect_cmds = collect.add_subparsers(dest="command", required=True)
    collect_cmds.add_parser(
        "run",
        help="One collection pass: enumerate if due, global lane, poll due markets. Prints the envelope.",
    )
    collect_cmds.add_parser(
        "status",
        help="Owner-facing tape summary: fills, tiers, declared gaps, strays.",
    )

    m0c = groups.add_parser(
        "m0c",
        help="M0-C consensus historical replay (zero-lookahead backtest + parameter sweep + GO/NO-GO).",
    )
    m0c_cmds = m0c.add_subparsers(dest="command", required=True)
    m0c_cmds.add_parser("universe", help="Enumerate target-category markets resolved in the replay window.")
    p = m0c_cmds.add_parser("sweep", help="Run the parameter sweep and emit the GO/NO-GO report.")
    p.add_argument("--limit-markets", type=int, default=None,
                   help="Only the N highest-volume resolved markets in the band (reported).")
    p.add_argument("--min-volume", type=float, default=0.0, help="Volume-band floor (USDC).")
    p.add_argument("--max-volume", type=float, default=None,
                   help="Volume-band cap (USDC) — skip mega-markets whose full history is infeasible to walk.")
    p.add_argument("--replay", action="store_true", help="Serve all fetches from cache (offline).")
    p.add_argument("--resume", action="store_true",
                   help="Resume a partial live pull: serve already-cached L1 pages from cache "
                        "(frozen tape — cached==fresh) and only fetch the un-walked tail. "
                        "Survives a network drop mid-pull.")

    m0f = groups.add_parser(
        "m0f",
        help="M0-F Feb-28 footprint backtest (historical study on L1; no live scanning, no alerting).",
    )
    m0f_cmds = m0f.add_subparsers(dest="command", required=True)
    m0f_cmds.add_parser("universe", help="Enumerate the Iran-cluster market universe -> artifact.")
    p = m0f_cmds.add_parser("pull", help="Walk every universe market's L1 fills into the cache.")
    p.add_argument("--limit-markets", type=int, default=None,
                   help="Only the N highest-volume universe markets (explicitly reported).")
    p = m0f_cmds.add_parser("score", help="Seven-factor detection replay at the as-of ladder.")
    p.add_argument("--as-of", type=int, default=None,
                   help="Single as-of ts (default: the configured ladder).")
    p.add_argument("--replay", action="store_true",
                   help="Serve every fetch from the response cache (offline, deterministic).")
    p.add_argument("--limit-markets", type=int, default=None,
                   help="Score only the N highest-volume universe markets (must match the pull).")

    m5 = groups.add_parser(
        "m5",
        help="M5 funded->bet latency (v1.4): the funding-provenance factor + its FP curve.",
    )
    m5_cmds = m5.add_subparsers(dest="command", required=True)
    p = m5_cmds.add_parser(
        "latency-scan",
        help="Funded->bet latency for every Feb-28 M0-F candidate + the false-positive curve.",
    )
    p.add_argument("--limit-wallets", type=int, default=None,
                   help="Cap the batch (labeled wallets always kept). Omit for the full ~900.")
    p.add_argument("--replay", action="store_true",
                   help="Serve every chain fetch from the cache (offline; loud on miss).")

    return parser


# ---------------------------------------------------------------------------
# smoke
# ---------------------------------------------------------------------------


def _safe(label: str, fn: Callable[[], Any]) -> dict[str, Any]:
    """Run one fetch step, capturing a DataLayerError as a recorded gap rather
    than aborting the whole smoke run (Rule 1: show the gap, move on)."""
    try:
        return {"label": label, "status": "ok", "value": fn(), "error": None}
    except DataLayerError as exc:
        return {"label": label, "status": "error", "value": None, "error": exc.to_error()}


def _step_value_json(label: str, value: Any) -> Any:
    """JSON-safe projection of a smoke step's fetched value. ``count`` is always
    the true total; ``sample`` is explicitly a capped sample, not the full set."""
    if value is None:
        return None
    if label == "market_meta":
        return {
            "question": value.question, "category": value.category, "slug": value.slug,
            "outcomes": value.outcomes, "outcome_prices": value.outcome_prices,
            "volume": value.volume, "liquidity": value.liquidity,
        }
    if label in ("market_trades", "wallet_trades"):
        return {"count": len(value), "sample": [_trade_dict(t) for t in value[:10]]}
    if label == "wallet_positions":
        return {"count": len(value), "sample": [_position_dict(p) for p in value[:10]]}
    if label == "kalshi_markets":
        return {
            "count": len(value),
            "sample": [
                {"ticker": m.ticker, "title": m.title, "status": m.status}
                for m in value[:10]
            ],
        }
    return None


def cmd_smoke(dl: DataLayer, loaded: LoadedConfig, args: argparse.Namespace) -> dict[str, Any]:
    smoke = loaded.config.data_layer.smoke
    market = args.market or smoke.market_condition_id
    wallet = args.wallet or smoke.wallet_proxy
    kalshi_limit = args.kalshi_limit or smoke.kalshi_markets_limit
    trade_limit = args.trade_limit

    rows_before = dl.cache.count()

    raw_steps = [
        _safe("market_meta", lambda: get_market_meta(dl, market)),
        _safe("market_trades", lambda: get_market_trades(dl, market, limit=trade_limit)),
        _safe("wallet_trades", lambda: get_wallet_trades(dl, wallet, limit=trade_limit)),
        _safe("wallet_positions", lambda: get_wallet_positions(dl, wallet)),
        _safe("kalshi_markets", lambda: get_kalshi_markets(dl, limit=kalshi_limit)),
    ]

    # Project live objects into JSON-safe steps: `summary` for humans, `value`
    # structured for --json consumers (never a Python repr).
    steps = [
        {
            "label": s["label"],
            "status": s["status"],
            "error": s["error"],
            "summary": _describe_step(s),
            "value": _step_value_json(s["label"], s["value"]),
        }
        for s in raw_steps
    ]

    rows_after = dl.cache.count()
    ok = sum(1 for s in steps if s["status"] == "ok")
    return {
        "kind": "data.smoke",
        "market": market,
        "wallet": wallet,
        "cache_path": str(dl.cache.path),
        "cache_rows_before": rows_before,
        "cache_rows_after": rows_after,
        "steps": steps,
        "ok_count": ok,
        "total": len(steps),
        "all_ok": ok == len(steps),
    }


def _describe_step(step: dict[str, Any]) -> str:
    """One-line human summary of a smoke step's value."""
    if step["status"] != "ok":
        return f"GAP - {step['error']}"
    val = step["value"]
    label = step["label"]
    if label == "market_meta":
        if val is None:
            return "NO DATA (gamma knows no such market)"
        outc = len(val.outcomes)
        q = (val.question or "")[:60]
        return f'OK  "{q}"  category={val.category!r} outcomes={outc}'
    if label in ("market_trades", "wallet_trades"):
        n = len(val)
        if not n:
            return "OK  0 fills (empty)"
        t = val[0]
        return f"OK  {n} fills   sample: {t.side} {t.size:g} @ {t.price:g}"
    if label == "wallet_positions":
        n = len(val)
        if not n:
            return "OK  0 positions (empty)"
        return f"OK  {n} positions   sample: {(val[0].title or '')[:40]!r}"
    if label == "kalshi_markets":
        n = len(val)
        if not n:
            return "OK  0 markets (empty)"
        return f"OK  {n} markets   sample: {val[0].ticker}"
    return f"OK  {val!r}"


def _render_smoke_human(summary: dict[str, Any]) -> str:
    lines = [
        "CONSENSUS data smoke",
        f"  market : {summary['market']}",
        f"  wallet : {summary['wallet']}",
        f"  cache  : {summary['cache_path']}",
        f"           rows {summary['cache_rows_before']} -> {summary['cache_rows_after']} "
        f"(+{summary['cache_rows_after'] - summary['cache_rows_before']})",
        "",
    ]
    for step in summary["steps"]:
        lines.append(f"  {step['label']:<17} {step['summary']}")
    lines.append("")
    verdict = "ALL OK" if summary["all_ok"] else "GAPS PRESENT"
    lines.append(f"RESULT: {summary['ok_count']}/{summary['total']} sources OK - {verdict}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# direct inspection commands
# ---------------------------------------------------------------------------


def cmd_trades(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    if args.all:
        # Full-history walk (Gate 0 / M0-F workhorse). Pages of --limit,
        # terminating on the RAW upstream page length.
        page_size = max(args.limit, 1)
        if args.market:
            trades = paginate_market_trades(
                dl, args.market, page_size=page_size, max_records=args.max_records
            )
            target = {"market": args.market}
        else:
            trades = paginate_wallet_trades(
                dl, args.wallet, page_size=page_size, max_records=args.max_records
            )
            target = {"wallet": args.wallet}
        ts = [t.timestamp for t in trades]
        # The full tape is already persisted in the raw cache; the report echoes
        # an EXPLICIT sample (count is the true total — no silent cap).
        sample_n = 50
        return {
            "kind": "data.trades",
            **target,
            "paginated": True,
            "count": len(trades),
            "earliest_ts": min(ts) if ts else None,
            "latest_ts": max(ts) if ts else None,
            "rate_limit_hits": dl.rate_limits.count_429 if dl.rate_limits else None,
            "sample_size": min(sample_n, len(trades)),
            "trades": [_trade_dict(t) for t in trades[:sample_n]],
        }
    if args.market:
        trades = get_market_trades(dl, args.market, limit=args.limit, offset=args.offset)
        target = {"market": args.market}
    else:
        trades = get_wallet_trades(dl, args.wallet, limit=args.limit, offset=args.offset)
        target = {"wallet": args.wallet}
    return {
        "kind": "data.trades",
        **target,
        "count": len(trades),
        "trades": [_trade_dict(t) for t in trades],
    }


def cmd_cache_stats(dl: DataLayer) -> dict[str, Any]:
    return {"kind": "data.cache_stats", **dl.cache.stats()}


def cmd_subgraph(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    import json as _json

    from .sources_polymarket import get_market_meta
    from .sources_subgraph import paginate_order_filled

    if args.market:
        meta = get_market_meta(dl, args.market)
        if meta is None:
            return {"kind": "data.subgraph", "market": args.market, "found": False,
                    "error": "market unknown to gamma (open and closed lookups empty)"}
        try:
            token_ids = [str(t) for t in _json.loads(meta.clob_token_ids or "[]")]
        except (ValueError, TypeError):
            token_ids = []
        if not token_ids:
            return {"kind": "data.subgraph", "market": args.market, "found": True,
                    "error": "market has no clobTokenIds; cannot map to subgraph assets"}
    else:
        token_ids = [args.asset]

    events, provenance = paginate_order_filled(
        dl, asset_ids=token_ids, ts_gte=args.since, ts_lt=args.until,
        max_records=args.max_records,
    )
    ts = [e.timestamp for e in events]
    sample_n = 20
    return {
        "kind": "data.subgraph",
        "market": args.market,
        "asset_ids": token_ids,
        "count": len(events),
        "earliest_ts": min(ts) if ts else None,
        "latest_ts": max(ts) if ts else None,
        "provenance": provenance,
        "sample_size": min(sample_n, len(events)),
        "events": [
            {"id": e.event_id, "ts": e.timestamp, "maker": e.maker, "taker": e.taker,
             "maker_asset": e.maker_asset_id[-10:], "taker_asset": e.taker_asset_id[-10:],
             "maker_amt": e.maker_amount_filled, "taker_amt": e.taker_amount_filled}
            for e in events[:sample_n]
        ],
    }


def cmd_positions(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    positions = get_wallet_positions(dl, args.wallet)
    return {
        "kind": "data.positions",
        "wallet": args.wallet,
        "count": len(positions),
        "positions": [_position_dict(p) for p in positions],
    }


def cmd_activity(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    acts = get_wallet_activity(dl, args.wallet, limit=args.limit)
    return {
        "kind": "data.activity",
        "wallet": args.wallet,
        "count": len(acts),
        "activity": [
            {"type": a.type, "ts": a.timestamp, "size": a.size, "usdc": a.usdc_size,
             "price": a.price, "side": a.side, "title": a.title}
            for a in acts
        ],
    }


def cmd_market(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    meta = get_market_meta(dl, args.market)
    return {
        "kind": "data.market",
        "market": args.market,
        "found": meta is not None,
        "meta": None if meta is None else {
            "question": meta.question, "category": meta.category, "slug": meta.slug,
            "outcomes": meta.outcomes, "outcome_prices": meta.outcome_prices,
            "volume": meta.volume, "liquidity": meta.liquidity,
            "active": meta.active, "closed": meta.closed, "end_date": meta.end_date,
        },
    }


def cmd_kalshi(dl: DataLayer, args: argparse.Namespace) -> dict[str, Any]:
    markets = get_kalshi_markets(dl, limit=args.limit, status=args.status)
    return {
        "kind": "data.kalshi",
        "count": len(markets),
        "markets": [
            {"ticker": m.ticker, "title": m.title, "status": m.status,
             "yes_bid": m.yes_bid, "yes_ask": m.yes_ask, "close_time": m.close_time}
            for m in markets
        ],
    }


def _trade_dict(t: Any) -> dict[str, Any]:
    return {"wallet": t.proxy_wallet, "side": t.side, "size": t.size, "price": t.price,
            "ts": t.timestamp, "outcome": t.outcome, "tx": t.transaction_hash}


def _position_dict(p: Any) -> dict[str, Any]:
    return {"condition_id": p.condition_id, "size": p.size, "avg_price": p.avg_price,
            "cur_price": p.cur_price, "realized_pnl": p.realized_pnl, "title": p.title}


def _render_generic_human(summary: dict[str, Any]) -> str:
    kind = summary.get("kind", "?")
    if kind == "data.cache_stats":
        size = summary.get("size_bytes")
        size_mb = f"{size / 1_048_576:.2f} MiB" if isinstance(size, int) else "unknown"
        lines = [
            "CONSENSUS cache stats",
            f"  path : {summary.get('path')}",
            f"  size : {size_mb}",
            f"  rows : {summary.get('total_rows')}",
        ]
        for s in summary.get("sources", []):
            lines.append(
                f"  {s['source']:<18} {s['rows']:>7} rows   "
                f"{s['oldest_fetch_ts']} .. {s['newest_fetch_ts']}"
            )
        return "\n".join(lines)
    count = summary.get("count")
    head = f"{kind}: {count} record(s)" if count is not None else kind
    lines = [head]
    if summary.get("paginated"):
        lines.append(
            f"  full-history walk: earliest_ts={summary.get('earliest_ts')} "
            f"latest_ts={summary.get('latest_ts')} "
            f"rate_limit_hits={summary.get('rate_limit_hits')}"
        )
    for key in ("trades", "positions", "activity", "markets"):
        for row in summary.get(key, [])[:50]:
            lines.append("  " + json.dumps(row, default=str, ensure_ascii=False))
    if summary.get("kind") == "data.market":
        lines.append("  " + json.dumps(summary.get("meta"), default=str, ensure_ascii=False))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# collect group (M1.5)
# ---------------------------------------------------------------------------


def _acquire_collector_lock(tape_path: Any, *, stale_minutes: int) -> Any | None:
    """Best-effort single-instance lock (Task Scheduler can overlap a slow pass).
    Returns the lock path on success, None if a fresh lock is held elsewhere.
    A stale lock (crashed run) expires after ``stale_minutes``. The per-run
    poll budget bounds a pass well under the staleness window, so a live pass
    can't be mistaken for a crashed one."""
    import os
    import time as _time
    from pathlib import Path

    lock = Path(str(tape_path) + ".lock")
    if lock.exists():
        try:
            held = json.loads(lock.read_text(encoding="utf-8"))
            if _time.time() - float(held.get("ts", 0)) < stale_minutes * 60:
                return None
        except (OSError, ValueError):
            pass  # unreadable/corrupt lock -> treat as stale
    tmp = lock.with_suffix(".lock.tmp")
    tmp.write_text(json.dumps({"pid": os.getpid(), "ts": _time.time()}), encoding="utf-8")
    os.replace(tmp, lock)
    return lock


def _release_collector_lock(lock: Any) -> None:
    """Unlink only if we still own it — a stale takeover must not cascade into
    deleting the taker's lock."""
    import os

    try:
        held = json.loads(lock.read_text(encoding="utf-8"))
        if int(held.get("pid", -1)) == os.getpid():
            lock.unlink()
    except (OSError, ValueError):
        pass


def cmd_collect_run(dl: DataLayer, loaded: LoadedConfig) -> tuple[dict[str, Any], int]:
    """One collector pass. Returns (envelope, exit_code); exit 0 covers ok AND
    degraded (the envelope carries the detail — orchestrator reads it), 1 only
    for a fatal failure to run at all."""
    from .collector import Collector
    from .tape import TapeStore

    lock = _acquire_collector_lock(
        loaded.tape_path, stale_minutes=loaded.config.collector.lock_stale_minutes
    )
    if lock is None:
        return ({"daemon": "consensus_collector", "schema": 1,
                 "status": "skipped_lock",
                 "detail": "previous invocation still running (fresh lock)"}, 0)
    tape = TapeStore(loaded.tape_path)
    try:
        envelope = Collector(dl, tape).run_once()
    finally:
        tape.close()
        _release_collector_lock(lock)

    if loaded.envelope_log is not None:
        try:
            loaded.envelope_log.parent.mkdir(parents=True, exist_ok=True)
            with open(loaded.envelope_log, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(envelope, ensure_ascii=False, default=str) + "\n")
        except OSError as exc:
            # errors present must imply a degraded status — orchestrators
            # branch on status, not on scanning the errors array.
            envelope.setdefault("errors", []).append(f"envelope_log write failed: {exc}")
            if envelope.get("status") == "ok":
                envelope["status"] = "degraded"
    return envelope, 0


def cmd_collect_status(loaded: LoadedConfig) -> dict[str, Any]:
    from .tape import TapeStore

    tape = TapeStore(loaded.tape_path)
    try:
        stats = tape.stats()
        markets = tape.markets(active_only=True)
        tiers = {t: sum(1 for m in markets if (m.get("tier") or "quiet") == t)
                 for t in ("hot", "quiet", "dormant")}
        recent_gaps = tape._conn.execute(
            "SELECT lane, condition_id, lo_ts, hi_ts, declared_ts, reason"
            " FROM l2_gaps ORDER BY id DESC LIMIT 10"
        ).fetchall()
    finally:
        tape.close()
    return {
        "kind": "collect.status",
        "tape": stats,
        "tracked_markets": len(markets),
        "tiers": tiers,
        "recent_gaps": [
            {"lane": g[0], "market": g[1], "lo_ts": g[2], "hi_ts": g[3],
             "declared_ts": g[4], "reason": g[5]}
            for g in recent_gaps
        ],
    }


def _render_collect_status_human(s: dict[str, Any]) -> str:
    t = s["tape"]
    size = t.get("size_bytes")
    size_mb = f"{size / 1_048_576:.2f} MiB" if isinstance(size, int) else "unknown"
    lines = [
        "CONSENSUS collector status (L2 tape)",
        f"  tape   : {t['path']}  ({size_mb})",
        f"  fills  : {t['fills']} ({t['fills_unparsed']} unparsed, kept raw)",
        f"  span   : {t['oldest_fill_ts']} .. {t['newest_fill_ts']} (unix UTC)",
        f"  markets: {s['tracked_markets']} tracked  "
        f"(hot {s['tiers']['hot']} / quiet {s['tiers']['quiet']} / dormant {s['tiers']['dormant']})",
        f"  polls  : {t['polls']}   declared gaps: {t['gaps_declared']}   "
        f"unresolved strays: {t['unresolved_strays']}",
    ]
    for g in s["recent_gaps"]:
        lines.append(f"  GAP [{g['lane']}] market={g['market']} ({g['lo_ts']}, {g['hi_ts']}]: {g['reason']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# dispatch + main
# ---------------------------------------------------------------------------


def _dispatch(dl: DataLayer, loaded: LoadedConfig, args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "smoke":
        return cmd_smoke(dl, loaded, args)
    if args.command == "trades":
        return cmd_trades(dl, args)
    if args.command == "positions":
        return cmd_positions(dl, args)
    if args.command == "activity":
        return cmd_activity(dl, args)
    if args.command == "market":
        return cmd_market(dl, args)
    if args.command == "kalshi":
        return cmd_kalshi(dl, args)
    if args.command == "cache-stats":
        return cmd_cache_stats(dl)
    if args.command == "subgraph":
        if args.replay:
            dl.replay = True  # serve from the response cache; loud on miss
        return cmd_subgraph(dl, args)
    raise ValueError(f"unknown command: {args.command}")


def _emit(summary: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        json.dump(summary, sys.stdout, indent=2, ensure_ascii=False, default=str)
        sys.stdout.write("\n")
    elif summary.get("kind") == "data.smoke":
        sys.stdout.write(_render_smoke_human(summary) + "\n")
    else:
        sys.stdout.write(_render_generic_human(summary) + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Force UTF-8 on the console so non-ASCII in market questions/titles is
    # written faithfully rather than mojibaked to cp1252 on Windows (Rule 1:
    # don't corrupt real data on the way out). No-op when streams are captured.
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass

    logging.basicConfig(level="WARNING", stream=sys.stderr, format="%(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("consensus.cli")

    if load_dotenv is not None:
        load_dotenv()  # load .env if present, for ETHERSCAN_API_KEY / LOG_LEVEL

    collect_run = args.group == "collect" and getattr(args, "command", None) == "run"

    def _fatal_envelope(message: str) -> int:
        # collect run is orchestrator-facing: even a fatal init failure must
        # come out as a JSON envelope on stdout, never a bare traceback/log.
        json.dump({"daemon": "consensus_collector", "schema": 1,
                   "status": "error", "errors": [message]}, sys.stdout,
                  ensure_ascii=False, default=str)
        sys.stdout.write("\n")
        sys.stdout.flush()
        return 1

    try:
        loaded = load_config(args.config)
        configure_logging(loaded)
    except ConfigError as exc:
        if collect_run:
            return _fatal_envelope(f"config error: {exc.to_error()}")
        log.error("configuration error: %s", exc.to_error())
        return 1

    try:
        dl = build_data_layer(loaded)
    except Exception as exc:  # noqa: BLE001 - e.g. CacheError on unwritable path
        if collect_run:
            return _fatal_envelope(f"init error: {type(exc).__name__}: {exc}")
        log.error("init error: %s", exc)
        return 1
    try:
        if args.group == "m0c":
            from .m0c import run_sweep, run_universe as m0c_universe

            if args.command == "universe":
                summary = m0c_universe(dl, loaded)
            elif args.command == "sweep":
                if args.replay:
                    dl.replay = True
                if args.resume:
                    dl.prefer_cache = True   # frozen L1: cached==fresh, resume the pull
                summary = run_sweep(dl, loaded, limit_markets=args.limit_markets,
                                    min_volume=args.min_volume, max_volume=args.max_volume)
            else:
                raise ValueError(f"unknown m0c command: {args.command}")
            _emit(summary, as_json=True)
            return 0
        if args.group == "m5":
            from .m5 import run_latency_scan
            if args.command == "latency-scan":
                if args.replay:
                    dl.replay = True
                summary = run_latency_scan(dl, loaded, limit_wallets=args.limit_wallets)
                _emit(summary, as_json=True)
                return 0
            raise ValueError(f"unknown m5 command: {args.command}")
        if args.group == "m0f":
            from .m0f import run_pull, run_score, run_universe

            if args.command == "universe":
                summary = run_universe(dl, loaded)
            elif args.command == "pull":
                summary = run_pull(dl, loaded, limit_markets=args.limit_markets)
            elif args.command == "score":
                if args.replay:
                    dl.replay = True
                summary = run_score(dl, loaded, as_of_override=args.as_of,
                                    limit_markets=args.limit_markets)
            else:
                raise ValueError(f"unknown m0f command: {args.command}")
            _emit(summary, as_json=True)  # backtest artifacts are structured
            return 0
        if args.group == "collect":
            if args.command == "run":
                try:
                    envelope, rc = cmd_collect_run(dl, loaded)
                except Exception as exc:  # noqa: BLE001 - fatal path must stay machine-readable
                    envelope, rc = {
                        "daemon": "consensus_collector", "schema": 1,
                        "status": "error", "errors": [dl._scrub(f"{type(exc).__name__}: {exc}")],
                    }, 1
                # Orchestrator-facing: the envelope IS the output, always JSON.
                json.dump(envelope, sys.stdout, ensure_ascii=False, default=str)
                sys.stdout.write("\n")
                sys.stdout.flush()
                return rc
            if args.command == "status":
                summary = cmd_collect_status(loaded)
                if args.json:
                    _emit(summary, as_json=True)
                else:
                    sys.stdout.write(_render_collect_status_human(summary) + "\n")
                return 0
            raise ValueError(f"unknown collect command: {args.command}")
        summary = _dispatch(dl, loaded, args)
    except DataLayerError as exc:
        # A direct command (not smoke) hit a hard data failure. Report loudly.
        log.error("data error: %s", exc.to_error())
        _emit({"kind": "error", "error": exc.to_error()}, as_json=args.json)
        return 1
    finally:
        dl.cache.close()

    _emit(summary, as_json=args.json)

    # Exit non-zero if a smoke run had any gaps, so cron/scripts can detect it.
    if summary.get("kind") == "data.smoke" and not summary.get("all_ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

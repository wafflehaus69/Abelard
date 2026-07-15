"""abelard_queue consumer — Abelard's triage + dispatch tool.

Run as ``abelard-queue <command>`` (console script) or
``python -m abelard_queue.consumer <command>``.
This is ABELARD'S side of the GATE 2 alert path: daemons only enqueue
(via ``abelard_common.alert_queue``, the shared primitive); this tool
interprets and dispatches. No daemon imports this package. Extracted
from abelard_common per Mando's Ruling 1 at the GATE 2 diff review
(2026-07-14) — abelard_common stays a pure logic library.

Materiality is cheap-first, per Mando's cost discipline:

  1. **Explicit rules** (free, mechanical, always tried first):
     - cooldown-suppress: a push for the same (source, kind, topic_key)
       was already decided inside the cooldown window -> suppress.
     - synthesis-brief-push: a full synthesis brief already cleared the
       daemon's own materiality gate -> push.
     - convergence-push: an attention brief with shape
       multi_source_convergence -> push.
  2. **Haiku** (cheap LLM tier) for items no rule decides — e.g.
     narrow_source_spike attention briefs. Structured JSON verdict.
  3. Anything Haiku can't decide (or when Haiku is unavailable) stays
     ``pending`` and is SURFACED with a non-zero exit — never silently
     pushed, never silently dropped. Opus-tier synthesis is reserved
     for Abelard itself on material items; this tool never calls it.

Every push/suppress verdict is journaled by the queue layer in the
same transaction (calibration record).

Dispatch: HTTPS Bot API ``sendMessage``. Reads TELEGRAM_BOT_TOKEN +
TELEGRAM_ALERT_CHAT_ID from the environment — names only ever appear
here; the token value is never logged, and error text is scrubbed with
``_redact_token`` because requests exception messages can embed the
request URL (which contains the token). Non-2xx / ok!=true leaves the
item un-dispatched with the error recorded and exits non-zero. Claim
stamps before I/O make double-push impossible; a crash between send
and confirm leaves an "unconfirmed" item that is never auto-retried
(``status`` surfaces it; ``reset-claim`` is the manual operator path).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import requests

from abelard_common.alert_queue import AlertQueue, QueueItem

DEFAULT_DB_PATH = "~/.openclaw/abelard_queue/queue.db"
DB_PATH_ENV = "ABELARD_QUEUE_DB_PATH"

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
CHAT_ID_ENV = "TELEGRAM_ALERT_CHAT_ID"
TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_TIMEOUT_S = 30.0
MAX_MESSAGE_CHARS = 4000  # Bot API hard limit 4096; reserve trailer room

ANTHROPIC_KEY_ENV = "ANTHROPIC_API_KEY"
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
HAIKU_MODEL_ID = "claude-haiku-4-5"
HAIKU_MAX_TOKENS = 300
HAIKU_TIMEOUT_S = 60.0
HAIKU_EXCERPT_CHARS = 1200

DEFAULT_COOLDOWN_S = 6 * 3600
DISPATCH_CHANNEL = "telegram_bot"

_LOG = logging.getLogger("abelard_queue.consumer")

_HAIKU_SYSTEM_PROMPT = (
    "You are the materiality triage layer of a personal macro/markets "
    "alerting pipeline. You receive one alert already flagged by an "
    "upstream daemon. Decide whether it merits an immediate push "
    "notification to the operator's phone (push) or should be archived "
    "without a notification (suppress). Push only for developments a "
    "macro-focused retail trader would want to know about within the "
    "hour: escalations, reversals, confirmed policy shifts, broad "
    "cross-source convergence. Suppress incremental chatter, single-"
    "source speculation, and routine follow-ups. Respond with STRICT "
    'JSON, nothing else: {"decision": "push"|"suppress", "reason": '
    '"<one sentence>"}'
)


class ConsumerError(RuntimeError):
    """Configuration or transport-layer failure. Fail loud."""


def _redact_token(text: str, token: str) -> str:
    """Scrub the bot token from any string that may be logged or stored.
    requests exception messages embed the request URL (token included);
    this is the mandatory laundering step for all error text."""
    if token:
        return text.replace(token, "<redacted>")
    return text


# ---------- triage rules (explicit, free, first) ----------

@dataclass(frozen=True)
class RuleVerdict:
    decision: str        # 'push' | 'suppress'
    rule_name: str       # journaled as decided_by='rule:<name>'
    reason: str


def apply_rules(
    queue: AlertQueue,
    item: QueueItem,
    *,
    cooldown_s: int = DEFAULT_COOLDOWN_S,
) -> Optional[RuleVerdict]:
    """Explicit rule ladder. Returns None when no rule decides —
    the caller escalates to Haiku. Order matters: cooldown outranks
    the push rules so a convergence rerun inside the window stays
    suppressed."""
    if queue.recent_push_exists(
        source=item.source, kind=item.kind, topic_key=item.topic_key,
        within_s=cooldown_s,
    ):
        return RuleVerdict(
            decision="suppress",
            rule_name="cooldown-suppress",
            reason=(f"push already decided for ({item.source}, {item.kind}, "
                    f"{item.topic_key}) within {cooldown_s}s"),
        )
    if item.kind == "synthesis_brief":
        return RuleVerdict(
            decision="push",
            rule_name="synthesis-brief-push",
            reason="full synthesis brief already cleared the daemon materiality gate",
        )
    if item.kind == "attention_brief":
        shape = item.payload.get("attention_shape")
        if shape == "multi_source_convergence":
            return RuleVerdict(
                decision="push",
                rule_name="convergence-push",
                reason="multi-source convergence attention shape",
            )
    return None


# ---------- haiku escalation (cheap LLM tier) ----------

def _item_summary_for_haiku(item: QueueItem) -> str:
    narrative = str(item.payload.get("narrative", ""))[:HAIKU_EXCERPT_CHARS]
    lines = [
        f"source: {item.source}",
        f"kind: {item.kind}",
        f"topic: {item.topic_key}",
    ]
    shape = item.payload.get("attention_shape")
    if shape:
        lines.append(f"attention_shape: {shape}")
    source_mix = item.payload.get("source_mix")
    if isinstance(source_mix, dict) and source_mix:
        mix = ", ".join(f"{k}({v})" for k, v in sorted(source_mix.items()))
        lines.append(f"source_mix: {mix}")
    lines.append(f"narrative: {narrative}")
    return "\n".join(lines)


def haiku_verdict(
    item: QueueItem,
    *,
    api_key: str,
    post_fn: Callable[..., requests.Response] | None = None,
) -> RuleVerdict:
    """One Haiku call -> strict-JSON verdict. Any transport, parse, or
    schema failure raises ConsumerError — the item stays pending and
    the failure is surfaced; we never guess."""
    post = post_fn or requests.post
    body = {
        "model": HAIKU_MODEL_ID,
        "max_tokens": HAIKU_MAX_TOKENS,
        "system": _HAIKU_SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": _item_summary_for_haiku(item)},
        ],
    }
    try:
        resp = post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
                "content-type": "application/json",
            },
            json=body,
            timeout=HAIKU_TIMEOUT_S,
        )
    except requests.RequestException as exc:
        raise ConsumerError(f"haiku: transport failure: "
                            f"{_redact_token(str(exc), api_key)}") from exc
    if resp.status_code != 200:
        tail = _redact_token(resp.text[-300:], api_key)
        raise ConsumerError(f"haiku: http_{resp.status_code}: {tail}")
    try:
        text = resp.json()["content"][0]["text"]
        verdict = json.loads(text.strip())
        decision = verdict["decision"]
        reason = str(verdict["reason"]).strip()
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ConsumerError(f"haiku: malformed verdict: {exc}") from exc
    if decision not in ("push", "suppress") or not reason:
        raise ConsumerError(f"haiku: invalid verdict fields: "
                            f"decision={decision!r}")
    return RuleVerdict(
        decision=decision,
        rule_name=f"haiku:{HAIKU_MODEL_ID}",
        reason=reason,
    )


# ---------- telegram dispatch (the ONLY external channel) ----------

def format_message(item: QueueItem) -> str:
    """Render a queue item into a Bot API message. Same shape family as
    the daemon-side sink formatters so the operator reads one format."""
    header = f"[ABELARD] {item.kind} — {item.topic_key} ({item.source})"
    narrative = str(item.payload.get("narrative", "")).strip()
    if not narrative:
        narrative = json.dumps(
            {k: item.payload[k] for k in sorted(item.payload)[:8]},
            ensure_ascii=False, default=str,
        )
    trailer_lines = [f"[queue_id: {item.id}  dedupe: {item.dedupe_key}]"]
    if item.decided_by:
        trailer_lines.append(f"[decided_by: {item.decided_by}]")
    trailer = "\n\n" + "\n".join(trailer_lines)
    overhead = len(header) + len("\n\n") + len(trailer)
    max_narrative = MAX_MESSAGE_CHARS - overhead
    if len(narrative) > max_narrative:
        narrative = narrative[:max_narrative - len("\n[truncated]")] + "\n[truncated]"
    return header + "\n\n" + narrative + trailer


def send_telegram(
    text: str,
    *,
    bot_token: str,
    chat_id: str,
    post_fn: Callable[..., requests.Response] | None = None,
) -> tuple[bool, Optional[str]]:
    """One sendMessage POST. Returns (success, error). Never raises;
    every error string is token-redacted before it leaves this
    function."""
    post = post_fn or requests.post
    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    try:
        resp = post(
            url,
            data={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": "true",
            },
            timeout=TELEGRAM_TIMEOUT_S,
        )
    except requests.RequestException as exc:
        return False, f"transport: {_redact_token(str(exc), bot_token)}"
    if resp.status_code != 200:
        tail = _redact_token(resp.text[-300:], bot_token)
        return False, f"http_{resp.status_code}: {tail}"
    try:
        parsed = resp.json()
    except ValueError as exc:
        return False, f"telegram_api: malformed JSON: {exc}"
    if parsed.get("ok") is True:
        return True, None
    description = _redact_token(
        str(parsed.get("description", "unknown error")), bot_token,
    )
    return False, f"telegram_api: {description}"


# ---------- command handlers ----------

def run_triage(
    queue: AlertQueue,
    *,
    use_haiku: bool,
    cooldown_s: int,
    haiku_post_fn: Callable[..., requests.Response] | None = None,
) -> dict[str, Any]:
    """Interpret every pending item. Undecidable items stay pending and
    are reported — the caller exits non-zero when any remain."""
    decided: list[dict[str, Any]] = []
    undecided: list[dict[str, Any]] = []
    api_key = os.environ.get(ANTHROPIC_KEY_ENV, "").strip()
    for item in queue.items(status="pending"):
        verdict = apply_rules(queue, item, cooldown_s=cooldown_s)
        if verdict is None and use_haiku and api_key:
            try:
                verdict = haiku_verdict(item, api_key=api_key,
                                        post_fn=haiku_post_fn)
            except ConsumerError as exc:
                _LOG.error("triage: haiku failed for item %d: %s", item.id, exc)
                undecided.append({"id": item.id, "topic_key": item.topic_key,
                                  "why": str(exc)})
                continue
        if verdict is None:
            why = ("no rule matched and haiku disabled" if not use_haiku
                   else f"no rule matched and {ANTHROPIC_KEY_ENV} not set")
            undecided.append({"id": item.id, "topic_key": item.topic_key,
                              "why": why})
            continue
        decided_by = (verdict.rule_name if verdict.rule_name.startswith("haiku:")
                      else f"rule:{verdict.rule_name}")
        queue.mark_interpreted(
            item.id, decision=verdict.decision,
            decided_by=decided_by, reason=verdict.reason,
        )
        decided.append({"id": item.id, "topic_key": item.topic_key,
                        "decision": verdict.decision, "decided_by": decided_by,
                        "reason": verdict.reason})
    return {"decided": decided, "undecided": undecided}


def run_dispatch(
    queue: AlertQueue,
    *,
    post_fn: Callable[..., requests.Response] | None = None,
) -> dict[str, Any]:
    """Deliver every dispatchable item. Missing credentials abort BEFORE
    any item is claimed. Failures leave items un-dispatched with the
    error recorded; unconfirmed (crash-window) items are surfaced and
    never retried here."""
    bot_token = os.environ.get(BOT_TOKEN_ENV, "").strip()
    chat_id = os.environ.get(CHAT_ID_ENV, "").strip()
    if not bot_token or not chat_id:
        raise ConsumerError(
            f"dispatch requires {BOT_TOKEN_ENV} + {CHAT_ID_ENV} in the "
            f"environment; at least one is empty. No items were claimed."
        )
    sent: list[int] = []
    failed: list[dict[str, Any]] = []
    for item in queue.dispatchable():
        if not queue.claim_for_dispatch(item.id):
            continue  # raced or unconfirmed — never double-send
        text = format_message(item)
        ok, error = send_telegram(text, bot_token=bot_token,
                                  chat_id=chat_id, post_fn=post_fn)
        if ok:
            queue.mark_dispatched(item.id, channel=DISPATCH_CHANNEL)
            sent.append(item.id)
        else:
            assert error is not None
            queue.record_dispatch_failure(item.id, error=error)
            _LOG.error("dispatch: item %d failed: %s", item.id, error)
            failed.append({"id": item.id, "error": error})
    unconfirmed = [{"id": i.id, "claimed_at_unix": i.claimed_at_unix}
                   for i in queue.unconfirmed()]
    return {"sent": sent, "failed": failed, "unconfirmed": unconfirmed}


def run_status(queue: AlertQueue) -> dict[str, Any]:
    return {
        "counts": queue.counts(),
        "dispatchable": [i.id for i in queue.dispatchable()],
        "unconfirmed": [{"id": i.id, "claimed_at_unix": i.claimed_at_unix}
                        for i in queue.unconfirmed()],
    }


def run_journal(queue: AlertQueue, *, limit: int) -> dict[str, Any]:
    return {
        "journal": [
            {"ts_unix": e.ts_unix, "item_id": e.item_id,
             "decision": e.decision, "decided_by": e.decided_by,
             "reason": e.reason}
            for e in queue.journal(limit=limit)
        ]
    }


# ---------- CLI ----------

def _resolve_db_path(cli_value: Optional[str]) -> Path:
    raw = cli_value or os.environ.get(DB_PATH_ENV, "").strip() or DEFAULT_DB_PATH
    return Path(raw).expanduser()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="abelard-queue",
        description="Abelard's alert-queue triage + Telegram dispatch tool.",
    )
    parser.add_argument("--db", help=f"queue DB path (default: ${DB_PATH_ENV} "
                                     f"or {DEFAULT_DB_PATH})")
    sub = parser.add_subparsers(dest="command", required=True)

    p_triage = sub.add_parser("triage", help="interpret pending items "
                                             "(rules first, then haiku)")
    p_triage.add_argument("--no-haiku", action="store_true",
                          help="rules only; undecided items stay pending")
    p_triage.add_argument("--cooldown-s", type=int, default=DEFAULT_COOLDOWN_S)

    sub.add_parser("dispatch", help="send pushed items via Telegram")

    p_run = sub.add_parser("run", help="triage then dispatch")
    p_run.add_argument("--no-haiku", action="store_true")
    p_run.add_argument("--cooldown-s", type=int, default=DEFAULT_COOLDOWN_S)

    sub.add_parser("status", help="counts + unconfirmed items")

    p_journal = sub.add_parser("journal", help="recent decisions")
    p_journal.add_argument("--limit", type=int, default=50)

    p_reset = sub.add_parser(
        "reset-claim",
        help="MANUAL: clear the claim on an unconfirmed item after "
             "verifying it did not deliver",
    )
    p_reset.add_argument("item_id", type=int)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    db_path = _resolve_db_path(args.db)

    exit_code = 0
    with AlertQueue(db_path) as queue:
        try:
            if args.command == "triage":
                data = run_triage(queue, use_haiku=not args.no_haiku,
                                  cooldown_s=args.cooldown_s)
                if data["undecided"]:
                    exit_code = 2
            elif args.command == "dispatch":
                data = run_dispatch(queue)
                if data["failed"] or data["unconfirmed"]:
                    exit_code = 2
            elif args.command == "run":
                triage = run_triage(queue, use_haiku=not args.no_haiku,
                                    cooldown_s=args.cooldown_s)
                dispatch = run_dispatch(queue)
                data = {"triage": triage, "dispatch": dispatch}
                if (triage["undecided"] or dispatch["failed"]
                        or dispatch["unconfirmed"]):
                    exit_code = 2
            elif args.command == "status":
                data = run_status(queue)
                if data["unconfirmed"]:
                    exit_code = 2
            elif args.command == "journal":
                data = run_journal(queue, limit=args.limit)
            elif args.command == "reset-claim":
                item = queue.reset_claim(args.item_id)
                data = {"reset": item.id, "status": item.status}
            else:  # pragma: no cover — argparse enforces the choices
                raise ConsumerError(f"unknown command {args.command!r}")
        except ConsumerError as exc:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2))
            return 2

    print(json.dumps({"status": "ok" if exit_code == 0 else "attention",
                      "data": data}, indent=2, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

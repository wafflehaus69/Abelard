"""Human-readable table rendering of the scrape output object.

PURE PRESENTATION. This module consumes the exact §8 output payload that the
orchestrator assembles and renders it as a ranked text table. It introduces no
new data, makes no network or LLM call, and does not touch the JSON contract —
`--table` and `--json` render the same object two ways.

Errors surface loudly: an error state prints the header plus a flagged ERRORS
block, never a silently blank table.
"""

from __future__ import annotations

import time
from typing import Any

ATTN_TRUE = "●"   # ● filled — attention tier
ATTN_FALSE = "·"  # · dot — count-only tail
EM_DASH = "—"     # — tail sentiment placeholder

# Haiku 4.5 list price, $ per 1M tokens (input / output).
_HAIKU_IN_PER_MTOK = 1.00
_HAIKU_OUT_PER_MTOK = 5.00


def _fmt_ts(ts: Any) -> str:
    if not ts:
        return "scrape_ts: n/a"
    lt = time.localtime(int(ts))
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", lt).strip()


def _sentiment_cell(sentiment: Any, attention: bool) -> str:
    if not attention or sentiment is None:
        return EM_DASH
    if "error" in sentiment:
        return f"ERROR: {sentiment['error']}"
    pb = sentiment.get("pct_bullish")
    pbe = sentiment.get("pct_bearish")
    pct = f"{pb}/{pbe}" if pb is not None else f"{EM_DASH}/{EM_DASH}"
    return (
        f"{sentiment.get('read', '?')} {pct} "
        f"({sentiment.get('directional', 0)} dir, {sentiment.get('neutral', 0)} neu)"
    )


def _est_cost(cost: dict[str, Any]) -> float:
    in_tok = int(cost.get("input_tokens", 0) or 0)
    out_tok = int(cost.get("output_tokens", 0) or 0)
    return in_tok / 1_000_000 * _HAIKU_IN_PER_MTOK + out_tok / 1_000_000 * _HAIKU_OUT_PER_MTOK


def render_table(payload: dict[str, Any]) -> str:
    """Render the output payload as a ranked text table. Returns a string."""
    threads = payload.get("threads", []) or []
    tickers = payload.get("tickers", []) or []
    errors = payload.get("errors", []) or []
    cost = payload.get("cost", {}) or {}

    total_posts = sum(int(t.get("post_count", 0) or 0) for t in threads)

    lines: list[str] = []
    lines.append(f"BizDaemon /smg/ scrape {EM_DASH} {_fmt_ts(payload.get('scrape_ts'))}")
    lines.append(
        f"threads: {len(threads)}    posts: {total_posts}    tickers: {len(tickers)}"
    )
    lines.append("")

    header = ("TICKER", "MENTIONS", "ATTN", "SENTIMENT")
    rows = [
        (
            str(t.get("ticker", "")),
            str(t.get("mentions", 0)),
            ATTN_TRUE if t.get("attention") else ATTN_FALSE,
            _sentiment_cell(t.get("sentiment"), bool(t.get("attention"))),
            bool(t.get("attention")),
        )
        for t in tickers
    ]

    w_tic = max([len(header[0])] + [len(r[0]) for r in rows])
    w_men = max([len(header[1])] + [len(r[1]) for r in rows])
    w_attn = max(len(header[2]), 4)

    def fmt(tic: str, men: str, attn: str, sent: str) -> str:
        return f"{tic:<{w_tic}}  {men:>{w_men}}  {attn:^{w_attn}}  {sent}"

    head_line = fmt(*header)
    rule = "-" * len(head_line)
    lines.append(head_line)
    lines.append(rule)

    if rows:
        seen_attention = False
        emitted_separator = False
        for tic, men, attn, sent, is_attn in rows:
            # one visual separator at the attention -> tail boundary
            if is_attn:
                seen_attention = True
            elif seen_attention and not emitted_separator:
                lines.append(ATTN_FALSE * len(head_line))
                emitted_separator = True
            lines.append(fmt(tic, men, attn, sent))
    else:
        lines.append("(no tickers)")

    lines.append("")
    lines.append(
        f"haiku_calls: {cost.get('haiku_calls', 0)}    "
        f"est_cost: ${_est_cost(cost):.4f}    "
        f"errors: {len(errors)}"
    )

    if errors:
        lines.append("")
        lines.append("ERRORS:")
        for err in errors:
            lines.append(f"  ! {err}")

    return "\n".join(lines)

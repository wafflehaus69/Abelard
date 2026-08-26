"""CD-DASH2 P3 — one-time backlog triage digest. Report-only.

Groups every pending queue item by daemon and age and renders enough of each
payload to be judged, so the backlog can be reviewed and dismissed or marked
before the scheduled digest starts its life. Writes a file. Sends nothing.
Mutates nothing.

**It does not run triage, deliberately.** `abelard-queue triage` writes a
decision onto each row, and a decision written before Mando has read the item is
his review pre-empted by a machine. Every pending item here carries no decision
at all — measured, not assumed: 134 of 134 have `decision IS NULL`.

Where an item's own payload carries a field the consumer's DOCUMENTED rules key
on, that field is surfaced as a fact, flagged as *"the rule that would fire"*
rather than as a verdict. The distinction matters: `multi_source_convergence` is
the shape that produced the only three pushes this queue has ever made, so an
item carrying it is worth looking at first — but noting that is reading the
payload, not deciding materiality.
"""
import argparse
import json
import os
import sqlite3
import sys
import time

DEFAULT_DB = "~/.openclaw/abelard_queue/queue.db"
DEFAULT_OUT = "~/.openclaw/abelard_queue/digests/TRIAGE-BACKLOG.md"

# The one explicit rule whose trigger is visible in the payload. Named here so
# the digest cites the consumer's own rule rather than inventing a heuristic.
CONVERGENCE_SHAPE = "multi_source_convergence"


def _age_bucket(days):
    if days >= 30:
        return "30d+"
    if days >= 14:
        return "14-30d"
    if days >= 7:
        return "7-14d"
    if days >= 2:
        return "2-7d"
    return "under 2d"


BUCKET_ORDER = ("30d+", "14-30d", "7-14d", "2-7d", "under 2d")


def _money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if abs(v) >= 1e9:
        return "${:,.2f}B".format(v / 1e9)
    if abs(v) >= 1e6:
        return "${:,.1f}M".format(v / 1e6)
    return "${:,.0f}".format(v)


def _describe(source, kind, payload):
    """One reviewable line per item, from whatever that kind actually carries."""
    if kind == "positioning_event":
        amt = (payload.get("amount") or {})
        val = _money(amt.get("value"))
        flags = payload.get("flags") or {}
        marks = [k for k, v in flags.items() if v]
        return "**{}** {} {} {}{}{}".format(
            payload.get("ticker") or "?",
            payload.get("side") or "?",
            val or "size n/a",
            payload.get("person") or payload.get("entity") or "?",
            "  ·  " + payload.get("tx_date", "") if payload.get("tx_date") else "",
            "  ·  " + ", ".join(marks) if marks else "")
    if kind == "attention_brief":
        ents = payload.get("entities_observed") or []
        return "shape=`{}` cluster={} · {}".format(
            payload.get("attention_shape") or "?",
            payload.get("cluster_size") or "?",
            ", ".join(str(e) for e in ents[:6]) or "(no entities)")
    return json.dumps(payload)[:160]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    now = int(time.time())
    con = sqlite3.connect("file:{}?mode=ro".format(
        os.path.expanduser(args.db).replace("?", "%3F")), uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM queue_items WHERE status='pending' ORDER BY created_at_unix"
    ).fetchall()
    decided = con.execute(
        "SELECT COUNT(*) FROM queue_items WHERE status='pending' AND decision IS NOT NULL"
    ).fetchone()[0]
    hist = con.execute(
        "SELECT decision, decided_by, COUNT(*) c FROM queue_items "
        "WHERE decision IS NOT NULL GROUP BY 1,2"
    ).fetchall()
    con.close()

    by_source = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)

    L = ["# Alert queue — backlog triage digest",
         "",
         "Report-only. **Nothing was sent, nothing was triaged, no row was "
         "modified to produce this file.**",
         "",
         "{} pending items across {} daemons. **{} of them carry a materiality "
         "read; {} do not.** The queue has made {} decisions in its entire "
         "history, all of them on items already dispatched:".format(
             len(rows), len(by_source), decided, len(rows) - decided,
             sum(h["c"] for h in hist)),
         ""]
    for h in hist:
        L.append("- `{}` by `{}` × {}".format(h["decision"], h["decided_by"], h["c"]))
    L += ["",
          "So this backlog has never been interpreted. It is not a set of "
          "suppressed items that leaked; it is a set of items nothing ever "
          "looked at.",
          "",
          "**How to read the annotations.** Where an item carries "
          "`attention_shape={}`, that is the trigger for the consumer's "
          "`rule:convergence-push` — the rule behind all three pushes this queue "
          "has ever made. It is flagged below as *the rule that would fire*, "
          "which is a fact about the payload, not a verdict about the item.".format(
              CONVERGENCE_SHAPE),
          ""]

    for source in sorted(by_source):
        items = by_source[source]
        oldest = (now - min(i["created_at_unix"] for i in items)) / 86400.0
        L += ["---", "",
              "## {} — {} pending, oldest {:.1f}d".format(source, len(items), oldest),
              ""]
        buckets = {}
        for r in items:
            age = (now - r["created_at_unix"]) / 86400.0
            buckets.setdefault(_age_bucket(age), []).append((age, r))
        for b in BUCKET_ORDER:
            if b not in buckets:
                continue
            group = sorted(buckets[b], key=lambda t: -t[0])
            L += ["### {} — {} items".format(b, len(group)), ""]
            for age, r in group:
                try:
                    payload = json.loads(r["payload_json"])
                except Exception:
                    payload = {}
                flag = ""
                if payload.get("attention_shape") == CONVERGENCE_SHAPE:
                    flag = "  ⟵ **would fire `rule:convergence-push`**"
                L.append("- `{:>5.1f}d` [{}] {}{}".format(
                    age, r["topic_key"], _describe(source, r["kind"], payload), flag))
            L.append("")

    L += ["---", "",
          "## What Mando decides",
          "",
          "Mark each item, or each group, **send** or **dismiss**. Nothing here "
          "moves until that is said explicitly — dispatch has no schedule and "
          "will not acquire one.",
          "",
          "Dismissal is the expected majority verdict. Much of this is 30-40 days "
          "stale, and a positioning event or an attention spike from six weeks ago "
          "is history rather than a signal, however material it looked when it "
          "was enqueued.",
          ""]

    out = os.path.expanduser(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    print(json.dumps({"out": out, "pending": len(rows), "with_decision": decided,
                      "by_source": {k: len(v) for k, v in sorted(by_source.items())}},
                     indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

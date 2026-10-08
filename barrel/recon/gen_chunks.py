"""Chunk list for the paid build (runbook section 1): one calendar month of stratum-P graduations
per chunk, with every query pre-generated and statically reviewed. No network, no credits.

  python barrel/recon/gen_chunks.py     writes recon/sql/chunks/<YYYY-MM>_<query>.sql and
                                        recon/chunks_manifest.json (dates, projection, review)

Queries per chunk, the core (scheduled from day 1)
  events  the event-derived columns of pt_features            gen_pt_features_tier_a.build
  b1a     aligned-set members grouped by funder (build form)  gen_heavy_b1.members_grouped
  b1b     holder concentration and supply at entry            gen_heavy_b1.c06
  b2      mint and freeze authority history, token program    gen_heavy_b2_b3.auth
  fan     funder fan-out over the chunk's calendar month      gen_fanout.chunk

Queries per chunk, Plus path only (MR-18: written before purchase, each proven at one-day scope
before any chunk of it runs, and gated; in build order)
  fs      fee-share: raw sharing-config events                gen_feeshare.fs
  fsm     fee-share: member row of each decoded recipient     gen_feeshare.fsm
  b3      early-buyer groups, H2                              gen_heavy_b2_b3.cluster
  org2    organic-v2 taker counts by cell, H5                 gen_org2.build

The review is static: it reads the generated text. It cannot prove a query is cheap; the
one-day run does that. It does catch the failures that have cost credits in this project:
a large table without literal partition bounds, a large table referenced more often than
designed, a wallet-bearing query without the owner filter, and a bound that does not match
the chunk.
"""
import calendar
import datetime as dt
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import gen_heavy_b1 as b1
import gen_fanout
import gen_feeshare as fsq
import gen_heavy_b2_b3 as b23
import gen_org2
import gen_pt_features_tier_a as ta
import seeded_selections as ss
import verdict_constants

ROOT = pathlib.Path(__file__).resolve().parents[1]
WINDOW_START = dt.date(2025, 3, 20)
WINDOW_END = ss.WINDOW_END            # 2026-09-20: last graduation day with its follow-up on Dune at the universe draw
BOOST = ss.BOOST

# Designed number of references to each large table, per query. More than this is a defect.
LARGE = {
    "tokens_solana.sol_transfers": {"b1a": 2, "b3": 2, "fan": 1, "fsm": 1, "org2": 1},
    "tokens_solana.transfers": {"b1a": 1, "b1b": 1, "fsm": 1},
    "pumpdotfun_solana.pump_amm_evt_buyevent": {"events": 1, "b3": 1, "org2": 2},
    "pumpdotfun_solana.pump_amm_evt_sellevent": {"events": 1, "b3": 1, "org2": 2},
    "pumpdotfun_solana.pump_evt_tradeevent": {"b1a": 1, "org2": 1},   # textual references; in b1a the CTE is read twice
    "solana.instruction_calls": {"fs": 1},
}
# Measured units (HEAVY_TIER_UNITS.md, MATERIALIZATION_UNITS.md): credits = a + b * days of large table scanned,
# times the post-BOOST ratio for the share of the chunk that is post-BOOST.
# b1a was measured with a third SOL-transfer reference (the fan-out it no longer carries), so its unit is high.
# fan: about 1 credit per day scanned (the calibration histogram: 11.5 for 12 days, all senders).
UNIT = {"events": (20.0, 0.83, 9, 1.3), "b1a": (16.9, 4.6, 5, 1.34), "b1b": (0.0, 0.46, 12, 1.23), "fan": (0.0, 1.0, 0, 1.0),
        "b2": (0.5, 0.0, 0, 1.0),
        # PRICED, NOT MEASURED: no run of these four texts exists. Each unit is put together from other queries'
        # measurements and is replaced by its one-day proof (docs/PLUS_QUERIES.md).
        "fs": (0.0, 11.6, 5, 1.0),        # the readiness extraction's 11.6 a day as a ceiling; it scans days + 5; that day WAS post-BOOST
        "fsm": (0.0, 1.5, 15, 1.25),      # ledger 0.46 a day once, plus one SOL-transfer pass at about 1 a day
        "b3": (33.2, 0.96, 8, 1.51),      # 0.96 per SOL-transfer scan-day: funding (days + 8) plus the calendar month folded into the fixed part
        "org2": (0.0, 1.35, 4, 1.4)}      # midpoint of 501-1,293 over the window
# MR-18 (RULINGS_2026-10-08.md): what has to be true before a chunk of each Plus-only query may run.
GATE = {
    "fs": "MR-18: one-day proof first; chunks from day 11 if the day-10 check passes, else after the day-15 check",
    "fsm": "MR-18: same gate as fs; runs on its chunk's decoded fs export (--pairs-from)",
    "b3": "MR-18: one-day proof first; no chunk before the day-15 check clears; after fee-share",
    "org2": "MR-18: one-day proof first; no chunk before the day-15 check clears; after b3; needs the chunk's fee-share codes or --no-pairs",
}
CORE = ("events", "b1a", "b1b", "b2", "fan")


def chunks() -> list[tuple[str, dt.date, dt.date]]:
    out, d = [], WINDOW_START
    while d <= WINDOW_END:
        last = min(dt.date(d.year, d.month, calendar.monthrange(d.year, d.month)[1]), WINDOW_END)
        out.append((f"{d.year}-{d.month:02d}", d, last))
        d = last + dt.timedelta(days=1)
    return out


def project(kind: str, d0: dt.date, d1: dt.date) -> float:
    a, b, extra, post = UNIT[kind]
    days = (d1 - d0).days + 1
    post_days = max(0, (d1 - max(d0, BOOST)).days + 1)
    factor = 1 + (post - 1) * post_days / days
    return round((a + b * (days + extra)) * factor, 1)


def review(kind: str, sql: str, d0: dt.date, d1: dt.date) -> list[str]:
    problems = []
    for table, allowed in LARGE.items():
        n = len(re.findall(re.escape(table) + r"\b", sql))
        if n > allowed.get(kind, 0):
            problems.append(f"{table}: {n} references, designed {allowed.get(kind, 0)}")
        for m in re.finditer(re.escape(table) + r"\b", sql):
            tail = sql[m.end():m.end() + 900]
            if not re.search(r"(block_date|evt_block_date|call_block_date)\s+(BETWEEN|=)\s+DATE '|block_time >= TIMESTAMP '", tail):
                problems.append(f"{table}: no literal partition bound after reference at {m.start()}")
    if kind in ("fan", "b3"):       # b3 carries the same ruled measure for every sender (docs/PLUS_QUERIES.md)
        t0, t1, days = gen_fanout.month_window(d0)
        if kind == "fan" and "__FUNDERS__" not in sql:
            problems.append("fan-out query without the funder placeholder")
        if (f"TIMESTAMP '{t0} 00:00:00'" not in sql or f"TIMESTAMP '{t1} 00:00:00'" not in sql
                or f"{days} AS window_days" not in sql):
            problems.append("fan-out window is not the chunk's calendar month")
    if kind in ("b1a", "b3", "fsm", "org2") and "__NOT_OWNER(" not in sql:
        problems.append("wallet-bearing query without the owner filter")
    if kind == "fs" and "__NOT_OWNER_HEX(" not in sql:       # the recipients are inside the payload, not in a column
        problems.append("payload-bearing query without the hex owner filter")
    if kind == "fsm" and "__FS_PAIRS__" not in sql:
        problems.append("fee-share member query without the pairs placeholder")
    if kind == "org2" and ("__FS_CODES__" not in sql or "__FS_SUPPLIED__" not in sql):
        problems.append("organic query without the fee-share code placeholders")
    if kind != "fan" and (f"DATE '{d0.isoformat()}'" not in sql or f"DATE '{d1.isoformat()}'" not in sql):
        problems.append("chunk bounds not found in the query text")
    problems += [f"verdict constant: line {no}: {text}" for _n, no, text in verdict_constants.hits(sql)]
    if re.search(r"\b(CREATE|INSERT|DELETE|UPDATE)\b", sql):
        problems.append("statement other than SELECT")
    if re.search(r"block_date[^\n]*\bOR\b|\bOR\b[^\n]*block_date", sql):
        problems.append("OR on a partition column")
    # each new generator also brings a stricter reading of its own text: exact reference counts, its exact
    # placeholders, one consumer per scanning block
    if kind in fsq.DESIGN:
        problems += fsq.check(sql, fsq.DESIGN[kind][0], (d0.isoformat(), d1.isoformat()), fsq.DESIGN[kind][1], True)
    if kind == "org2":
        problems += gen_org2.self_check(sql, d0.isoformat(), d1.isoformat())
    return list(dict.fromkeys(problems))


def main() -> None:
    out = ROOT / "recon" / "sql" / "chunks"
    out.mkdir(parents=True, exist_ok=True)
    builders = {"events": ta.build, "b1a": b1.members_grouped, "b1b": b1.c06, "b2": b23.auth, "fan": gen_fanout.chunk,
                "fs": fsq.fs, "fsm": fsq.fsm, "b3": b23.cluster, "org2": gen_org2.build}
    manifest, bad = [], 0
    for name, d0, d1 in chunks():
        row = {"chunk": name, "from": d0.isoformat(), "to": d1.isoformat(), "days": (d1 - d0).days + 1,
               "era": "post_boost" if d0 >= BOOST else "pre_boost" if d1 < BOOST else "both",
               "contains": [t for t, c in (("calibration slice (to 2025-07-10)", d0 <= ss.CALIBRATION_END),
                                           ("burned week 2026-08-10", d0 <= ss.burned_week() <= d1)) if c],
               "queries": {}}
        for kind, fn in builders.items():
            sql = fn(d0.isoformat(), d1.isoformat())
            (out / f"{name}_{kind}.sql").write_text(sql, encoding="utf-8")
            problems = review(kind, sql, d0, d1)
            bad += len(problems)
            row["queries"][kind] = {"file": f"recon/sql/chunks/{name}_{kind}.sql", "projected_credits": project(kind, d0, d1),
                                    "review": problems or "ok"}
            if kind in GATE:     # written and reviewed, never run: the gate and the kind of number the projection is
                row["queries"][kind].update(gate=GATE[kind], projection="priced, not measured")
        manifest.append(row)
    w = ss.burned_week().isoformat()
    (ROOT / "recon" / "sql" / "events_burned_day.sql").write_text(ta.build(w, w), encoding="utf-8")   # MR-15 B1
    for r in manifest:   # the fan-out query scans its whole calendar month, whatever the chunk's length
        d0 = dt.date.fromisoformat(r["from"])
        r["queries"]["fan"]["projected_credits"] = float(gen_fanout.month_window(d0)[2])
    (ROOT / "recon" / "chunks_manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    tot = {k: round(sum(r["queries"][k]["projected_credits"] for r in manifest)) for k in builders}
    print(f"{len(manifest)} chunks; core: {len(manifest) * len(CORE)} scheduled files; Plus-only, gated by MR-18: "
          f"{len(manifest) * len(GATE)} files; review problems: {bad}")
    print("projected credits, core (measured units):", {k: tot[k] for k in CORE}, "| total", sum(tot[k] for k in CORE))
    print("projected credits, Plus-only (PRICED, NOT MEASURED):", {k: tot[k] for k in GATE}, "| total", sum(tot[k] for k in GATE))
    for r in manifest:
        q = r["queries"]
        print(f"  {r['chunk']}  {r['from']} .. {r['to']}  {r['days']:2d}d  {r['era']:10s} "
              f"ev {q['events']['projected_credits']:6.1f}  b1a {q['b1a']['projected_credits']:6.1f}  b1b {q['b1b']['projected_credits']:5.1f}  "
              f"| fs {q['fs']['projected_credits']:6.1f}  fsm {q['fsm']['projected_credits']:5.1f}  b3 {q['b3']['projected_credits']:6.1f}  "
              f"org2 {q['org2']['projected_credits']:5.1f}  {'; '.join(r['contains'])}")


if __name__ == "__main__":
    main()

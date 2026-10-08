"""Fee-share recipients (S7b, c07b): the two chunk queries, their one-day proving files and the probes.

A recipient is an entry {address, share_bps} of a token's sharing config, an account of the pump fee
program. The program has no decoded tables on Dune, so the history is rebuilt from the raw instruction
table. Two queries per chunk with a local decode between them, the way the aligned-set query is followed
by the fan-out query. The aligned-set query (gen_heavy_b1.py) is not touched: its head() text is taken
by import, so the universe, the creation bound and the ledger windows are its own by construction.

  fs   (query 1)  one row per sharing-config EVENT of a token of the chunk, the payload as hex.
                  Events, not instructions: the instruction that makes a config has no arguments, and
                  its recipients exist only in the event; the three events carry the mint and the whole
                  new list alike. Kept dumb: no Borsh decode in Dune, so a layout surprise in some era is
                  fixed locally at zero credits (recon/decode_feeshare.py) and not by a second scan.
                  Window per token: from its creation bound (3 days before its graduation day) to
                  graduation + 240 minutes. The as-of rule (R7) needs nothing later than the last entry
                  lag. A token with no event keeps one row with the event columns empty.
  fsm  (query 2)  the aligned-set MEMBER row for each decoded (token, recipient) pair, supplied at run
                  time (--pairs-from), under the aligned-set query's own windows and predicates: balance
                  at each entry lag, net flow after entry, first acquisition, funder (last sender by
                  slot, then address, up to the slot of the first action; rent is not funding, MR-16),
                  and how the creator is tied to it. A recipient that is the creator or creator-paid is
                  already in that set (is_creator, is_funded say so); every other one can be added to
                  it locally, which is why the columns and their names are that query's.

What is NOT in either query: which lag "entry" is, what c07b counts, set membership, whether a
recipient is the creator's actor, every classification and every threshold. All local.

Wallets. fs returns no wallet column, but a payload names recipients and the admin: rows are private.
A payload that holds an owner wallet comes back WITHOUT the payload (data_hex empty, owner_in_payload
true). The row must still exist: dropping it would leave the config before it in force, silently
wrong. fsm filters the candidates with the owner filter before anything is joined to them; senders
are not filtered, exactly as in the aligned-set query (a row whose funder is an owner wallet is
dropped by the runner after the fetch).

Effective scans (a WITH block is executed once per reference; measured, 85 credits against 14.65):
  fs   raw instruction table 1 (ev is read once, by j). Small decoded tables: comp and pc twice.
  fsm  token ledger 1 (bal is read by mm, mm by snd, and nothing else reads either);
       SOL transfers 1 (tr is read by snd). The candidate list `pairs` is read twice (by mm and by
       tr's semi-join): it is the VALUES list and scans nothing. Small decoded tables: base 3 times
       (comp and pc six), as in the holder-concentration query.
The aligned-set query joins the SOL transfers to its member block and reads that block a second time
for its output, which reads the ledger twice. Here the transfers are narrowed by the candidate list
itself (a semi-join against the VALUES list, bounds in the same WHERE clause) and the member block is
the kept side of a join to what is left, so each is read once. The predicates are the same; they sit
in FILTER clauses because one pass serves both funding and the creator's payment. That shape has not
run at any scope: its one-day file is the proof.

Unverified until the column probe has run: the names tx_success, tx_index, outer_instruction_index
and inner_instruction_index on the raw instruction table. A wrong name fails at analysis, for nothing.

  python barrel/recon/gen_feeshare.py     writes under recon/sql/, in the order they are meant to run:
    fs_columns_probe.sql            the raw instruction table's column names, zero rows
    fs_scan_events_pbday.sql        one partition of events, counted: the unit of fs, and the reconciliation
    fs_scan_both_pbday.sql          the same partition with the instructions, as rows (only if needed)
    fs_disc_by_month_probe.sql      one day per chunk: which chunks have anything to extract (E38)
    feeshare_fs_{day,week,pbday}.sql    query 1 at one-day and one-week scope
    feeshare_fsm_{day,week,pbday}.sql   query 2, fed with the pairs decoded from query 1's rows
The chunk files (recon/sql/chunks/<chunk>_fs.sql, _fsm.sql) are written by gen_chunks.py from fs and fsm.
"""
import datetime as dt
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_feeshare as dfs
import gen_heavy_b1 as b1
import verdict_constants

ROOT = pathlib.Path(__file__).resolve().parents[1]
WALLET_LINE = "-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here."
PROVING_DAY = "2026-09-01"     # the one day every fee-program count held so far was taken on (post-BOOST)

_LAY = dfs.layouts()
PFEE = _LAY["program"]
TAG = dfs.EVENT_IX_TAG.hex().upper()
# 16-byte prefix of each event's inner call: wrapper tag, then the event discriminator. Both come from the
# pinned IDL through decode_feeshare.layouts(), which recomputes every discriminator from its name.
EVENT_PREFIX = {name: TAG + disc.hex().upper() for disc, (name, _fields) in _LAY["events"].items()}
IX_DISC = {name: disc.hex().upper() for disc, (name, _args, _accounts) in _LAY["ix"].items()}
# The control of the per-month probe: a program with calls on every day of the window, from its own pinned IDL.
PUMP = json.loads((pathlib.Path(__file__).with_name("idl") / "pump.json").read_text(encoding="utf-8"))["address"]

if TAG != "E445A52E51CB9A1D":
    raise ValueError("the event wrapper tag is not the one counted on chain (10,193 inner calls on 2026-09-01)")
if IX_DISC[dfs.RESET_IX] != "0A02B65F107F81BA":
    raise ValueError("the reset instruction's discriminator is not the one counted on chain (3 inner calls on 2026-09-01)")
for _name, _args, _accounts in _LAY["ix"].values():
    # the proving query returns the fifth account of an instruction as its mint
    if _accounts is not None and _accounts[4] != "mint":
        raise ValueError(f"{_name}: account 4 of the pinned IDL is not the mint")


def _quoted(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _events_in() -> str:
    return _quoted(EVENT_PREFIX[n] for n in dfs.IN_FORCE)


def fs(gd0: str, gd1: str) -> str:
    h, p = b1.head(gd0, gd1)
    # comp, pc, u, cr, base: the aligned-set query's universe and creation bound, verbatim. Its ledger
    # block is cut off: this query reads no token transfer.
    if h.count("bal AS (") != 1:
        raise ValueError("gen_heavy_b1.head() no longer has exactly one 'bal AS (' to cut at")
    uni = h[:h.index("bal AS (")].rstrip()
    if not uni.endswith("),"):
        raise ValueError("the text before the ledger block does not end with the closing of `base` and a comma")
    # a config cannot precede the token (creation bound, 3 days before the first graduation day); a pool can
    # be created up to a day after the chunk's last completion and the last entry lag is 240 minutes after that
    ev1 = (dt.date.fromisoformat(gd1) + dt.timedelta(days=2)).isoformat()
    return f"""-- Fee-share recipients (S7b, c07b), query 1 of 2: the fee program's sharing-config events, raw, graduations {gd0} .. {gd1}.
{WALLET_LINE}
-- One row per event that wrote a token's recipient list (config made, shares changed, config reset), from the
-- token's creation bound to graduation + 240 minutes. The as-of rule (R7) reads the config in force at entry
-- and the last entry lag is 240 minutes, so nothing later is fetched. A token with no such event keeps one
-- row with the event columns empty: "no event" is then a returned answer, not a token that went missing.
-- Nothing is decoded here: the payload goes back as hex and recon/decode_feeshare.py reads it with the pinned IDL.
-- A payload holding an owner wallet comes back without it (data_hex empty, owner_in_payload true). The row has
-- to exist: without it the config before it would be read as still in force.
{uni}
bm AS (   -- the mint as its 32 bytes, to meet the event's own copy of them; the conversion runs on this small side
  SELECT mint, from_base58(mint) AS mint_bin, grad_time, t0 FROM base),
ev AS (   -- ONE pass over the raw instruction table. An event is an inner call of the fee program to itself:
          -- 8 bytes of wrapper tag, 8 of event discriminator, then the timestamp (8) and the mint (32)
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success,
         to_hex(substr(data, 9, 8)) AS evt, substr(data, 25, 32) AS mint_bin, to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date BETWEEN DATE '{p['cr0']}' AND DATE '{ev1}'
    AND executing_account = '{PFEE}'
    -- a failed transaction changed nothing. NULL-safe, as the rent rule is: only a transaction KNOWN to have
    -- failed is left out; a row whose flag is empty comes back with tx_success empty and is not used locally
    AND coalesce(tx_success, true)
    AND to_hex(substr(data, 1, 16)) IN ({_events_in()})),
j AS (
  SELECT b.mint, b.grad_time, b.t0, e.block_slot, e.block_time, e.tx_index, e.outer_instruction_index,
         e.inner_instruction_index, e.tx_id, e.tx_success, e.evt, e.data_hex,
         __NOT_OWNER_HEX(e.data_hex)__ AS no_owner
  FROM bm b
  LEFT JOIN ev e ON e.mint_bin = b.mint_bin
    -- per-token window: the same for a token wherever it falls in the chunk
    AND e.block_time >= date_trunc('day', b.grad_time) - INTERVAL '3' DAY
    AND e.block_time <= b.grad_time + INTERVAL '240' MINUTE)
SELECT mint, grad_time, t0, block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index,
       tx_id, tx_success, evt,
       CASE WHEN no_owner THEN data_hex END AS data_hex,
       NOT no_owner AS owner_in_payload
FROM j"""


def fsm(gd0: str, gd1: str) -> str:
    h, p = b1.head(gd0, gd1)     # comp, pc, u, cr, base, bal: verbatim, so every window is the aligned-set query's
    f8 = [f"f{k}" for k in range(8)]
    # what a candidate row carries from the ledger through the one pass over SOL transfers
    carry = ["is_creator", "grad_time", "t0", "first_in", "first_in_tx", "b15", "b60", "b240", "net_after", *f8, "got_mint"]
    # the aligned-set query's funding predicates (its block inb0), unchanged: through the slot of the first
    # action, never inside the first-acquisition transaction (NULL-safe), from 4 days before the graduation day
    fund = ("t.block_slot <= m.slot_act AND (m.first_in_tx IS NULL OR t.tx_id IS NULL OR t.tx_id <> m.first_in_tx)\n"
            "             AND t.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY")
    # and its creator-payment predicate (role F of its block hit): the creator paid this wallet within 24 hours
    # either side of the token's creation
    crpay = "t.sender = m.creator AND t.block_time BETWEEN m.t0 - INTERVAL '24' HOUR AND m.t0 + INTERVAL '24' HOUR"
    return f"""-- Fee-share recipients (S7b), query 2 of 2: the aligned-set member row of each decoded recipient, graduations {gd0} .. {gd1}.
{WALLET_LINE}
-- The (token, recipient) candidates are the decoded pairs of this chunk, supplied at run time (--pairs-from).
-- One row per candidate, in the columns, windows and predicates of the aligned-set member rows, so that a
-- recipient that is not in that set yet can be added to it locally. is_creator and is_funded mark the ones
-- that are in it already. Whether a candidate matters (held the token, did not receive the mint) is decided
-- locally from first_in and got_mint: every candidate comes back, so a missing row is a fault and not an answer.
{h},
pairs AS (
  SELECT DISTINCT mint, w FROM (VALUES __FS_PAIRS__) AS t(mint, w)
  WHERE mint IS NOT NULL AND w IS NOT NULL AND __NOT_OWNER(w)__),
mm AS (   -- one row per candidate: its token's windows and its own ledger aggregates (empty when it never touched the token)
  SELECT p.mint, p.w, bs.creator, bs.t0, bs.grad_time, (p.w = bs.creator) AS is_creator,
         b.first_in, b.first_in_tx, b.b15, b.b60, b.b240, b.net_after, {", ".join("b." + c for c in f8)}, b.got_mint,
         CASE WHEN p.w = bs.creator THEN bs.s0 ELSE b.first_in_slot END AS slot_act
  FROM pairs p JOIN base bs ON bs.mint = p.mint
  LEFT JOIN bal b ON b.mint = p.mint AND b.w = p.w),
tr AS (   -- ONE pass over SOL transfers: everything of at least 0.001 SOL a candidate received in the scanned window
  SELECT s.to_owner AS w, s.from_owner AS sender, s.block_time, s.block_slot, s.tx_id, CAST(s.amount AS double) AS amt
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '{p['st0']} 00:00:00' AND s.block_time < TIMESTAMP '{p['fu1']} 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.to_owner IN (SELECT w FROM pairs)
    AND s.from_owner <> s.to_owner),
snd AS (  -- one row per (candidate, sender); a candidate that received nothing keeps one row with the sender empty.
          -- Both uses of a transfer are read from this one pass: funding, and the creator's payment.
  SELECT m.mint, m.w, t.sender,
         {", ".join(f"arbitrary(m.{c}) AS {c}" for c in carry)},
         min(t.block_time) FILTER (WHERE {fund}) AS t_first,
         max(t.block_slot) FILTER (WHERE {fund}) AS slot_last,
         max_by(t.amt, t.block_slot) FILTER (WHERE {fund}) / 1e9 AS last_sol,
         sum(t.amt) FILTER (WHERE {crpay}) / 1e9 AS cr_sol,
         array_agg(DISTINCT t.tx_id) FILTER (WHERE {crpay}) AS cr_txs
  FROM mm m LEFT JOIN tr t ON t.w = m.w
  GROUP BY 1, 2, 3),
one AS (  -- funder = the last sender before the first action, by slot, then by address; a sender with no funding
          -- transfer sorts after every one that has. The creator's payment sits on one sender row and is spread
          -- over the candidate's rows, so nothing above is read a second time.
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY slot_last DESC NULLS LAST, sender DESC) AS rn,
           count(slot_last) OVER (PARTITION BY mint, w) AS n_fund,
           max(cr_sol) OVER (PARTITION BY mint, w) AS cr_sol_m,
           arbitrary(cr_txs) OVER (PARTITION BY mint, w) AS cr_txs_m
    FROM snd)
  WHERE rn = 1)
SELECT o.mint, o.w, o.is_creator, (o.cr_sol_m IS NOT NULL) AS is_funded,
       -- typed as the aligned-set query types it (MR-16): the creator's SOL inside the wallet's own first-acquisition
       -- transaction is a token delivery, in any other transaction it is funding, and both can hold
       CASE WHEN o.cr_sol_m IS NULL THEN NULL
            WHEN coalesce(contains(o.cr_txs_m, o.first_in_tx), false)
                 THEN CASE WHEN cardinality(o.cr_txs_m) > 1 THEN 'both' ELSE 'token_delivery_by_creator' END
            ELSE 'sol_funding' END AS link,
       CAST(NULL AS bigint) AS bundle_n,     -- creation-slot traders are not looked at here
       o.cr_sol_m AS cr_sol, o.grad_time, o.t0, o.first_in, o.b15, o.b60, o.b240, o.net_after,
       {", ".join("o." + c for c in f8)}, o.got_mint,
       CASE WHEN o.slot_last IS NOT NULL THEN o.sender END AS funder,
       o.t_first AS funded_ts, o.last_sol AS funded_sol, NULLIF(o.n_fund, 0) AS n_senders,
       date_diff('second', o.t_first, o.first_in) AS fund_to_first_buy_s
FROM one o"""


def columns_probe(day: str = PROVING_DAY) -> str:
    return f"""-- Column names of the raw instruction table, zero rows. The fee-share queries read tx_success, tx_index,
-- outer_instruction_index and inner_instruction_index, and none of the four has been read from this table here.
-- No wallet in the output: the names are in the result's metadata (column_names), not in rows.
SELECT * FROM solana.instruction_calls WHERE block_date = DATE '{day}' LIMIT 0"""


def scan_events(day: str = PROVING_DAY) -> str:
    return f"""-- Fee-share proving run 1: the sharing-config events of ONE partition ({day}), every token, counted.
{WALLET_LINE}
-- It reads what query 1 reads (the same columns, the whole payload) and nothing wider, so its cost is the unit
-- of query 1 per scanned day. No universe join. One row per (event kind, payload length):
--   n against the instruction counts already held for this day (config made 3,288; shares changed 2,516 + 751;
--   reset 3), which were taken without a success filter, as n is here;
--   how many rows have each column filled that query 1 orders and filters by;
--   one payload per layout, the earliest, for the local decoder: the events have never been fetched.
-- A payload holding an owner wallet is never the sample.
SELECT evt, data_len, count(*) AS n, count(DISTINCT tx_id) AS n_tx, count(DISTINCT mint_bin) AS n_mints,
       count(tx_success) AS tx_success_filled, count_if(tx_success) AS n_success,
       count(is_inner) AS is_inner_filled, count_if(is_inner) AS n_inner,
       count(tx_index) AS tx_index_filled, count(outer_instruction_index) AS outer_ix_filled,
       count(inner_instruction_index) AS inner_ix_filled,
       min(block_time) AS t_min, max(block_time) AS t_max,
       min_by(data_hex, block_slot) FILTER (WHERE __NOT_OWNER_HEX(data_hex)__) AS sample_hex
FROM (
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
         to_hex(substr(data, 9, 8)) AS evt, length(data) AS data_len, substr(data, 25, 32) AS mint_bin,
         to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date = DATE '{day}'
    AND executing_account = '{PFEE}'
    AND to_hex(substr(data, 1, 16)) IN ({_events_in()}))
GROUP BY 1, 2"""


def scan_both(day: str = PROVING_DAY) -> str:
    kinds = [EVENT_PREFIX[n] for n in dfs.IN_FORCE] + [IX_DISC[n] for n in ("create_fee_sharing_config", *dfs.UPDATE_IX, dfs.RESET_IX)]
    k = f"CASE WHEN to_hex(substr(data, 1, 8)) = '{TAG}' THEN to_hex(substr(data, 1, 16)) ELSE to_hex(substr(data, 1, 8)) END"
    return f"""-- Fee-share proving run 2: sharing-config INSTRUCTIONS and events of ONE partition ({day}), every token, as rows.
{WALLET_LINE}
-- For the case where the events of run 1 do not reconcile with the instruction counts, and to price the
-- account column that run 1 leaves out. It gives a second encoding to hold the first against: the arguments
-- of an instruction that changes the shares must equal the list in its event (decode_feeshare.crosscheck),
-- and the 20 rows held from the readiness run must come back unchanged.
-- kind is the 8-byte discriminator of an instruction or the 16-byte prefix of an event; one list selects both.
-- Run it with --no-rows first: about 13,000 rows, and fetching rows is billed by size.
WITH c AS (
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
         {k} AS kind,
         cardinality(account_arguments) AS n_accounts,
         -- the mint is the fifth account of the three instructions the pinned IDL has (held on 20 of 20 rows);
         -- the reset instruction is not in the IDL, so for it this column is only its fifth account
         CASE WHEN to_hex(substr(data, 1, 8)) = '{TAG}' THEN NULL ELSE element_at(account_arguments, 5) END AS ix_mint,
         to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date = DATE '{day}'
    AND executing_account = '{PFEE}'
    AND {k}
        IN ({_quoted(kinds)}))
SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
       kind, n_accounts, ix_mint,
       CASE WHEN __NOT_OWNER_HEX(c.data_hex)__ THEN c.data_hex END AS data_hex,
       NOT __NOT_OWNER_HEX(c.data_hex)__ AS owner_in_payload
FROM c"""


def probe_day(d1: dt.date) -> dt.date:
    """The day of a chunk the per-month probe looks at: the last day query 1 scans for it."""
    return d1 + dt.timedelta(days=2)


def disc_by_month(chunks: list[tuple[str, dt.date, dt.date]], control: bool = True) -> str:
    """One day per chunk: fee-program calls by kind, with the fill of the columns query 1 uses.

    The day is the LAST day query 1 scans for the chunk (two days after its last graduation day), which is
    also four days into the scan of the next chunk. So every chunk but the first has a count near each end of
    its own scan; a chunk with no sharing-config event at either end has nothing to extract, on the one
    assumption that the mechanism, once on chain, stayed.

    `control` adds the pump program's calls of the same day as one more kind, so that a day with no
    fee-program row is shown to be a day the table had rows on (E38). It is the dearer half of the probe on
    the days before the fee program existed; control=False gives the counts without it."""
    programs = _quoted([PFEE, PUMP] if control else [PFEE])
    first = f"WHEN executing_account = '{PUMP}' THEN 'control: pump program, all calls'\n            " if control else ""

    def one(i: int, name: str, day: str) -> str:
        names = (" AS chunk", " AS day", " AS kind", " AS n", " AS n_inner", " AS tx_success_filled", " AS n_success", " AS tx_index_filled",
                 " AS outer_ix_filled", " AS inner_ix_filled", " AS len_min", " AS len_max") if i == 0 else ("",) * 12
        return f"""SELECT '{name}'{names[0]}, '{day}'{names[1]},
       CASE {first}WHEN to_hex(substr(data, 1, 8)) = '{TAG}' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END{names[2]},
       count(*){names[3]}, count_if(is_inner){names[4]}, count(tx_success){names[5]}, count_if(tx_success){names[6]},
       count(tx_index){names[7]}, count(outer_instruction_index){names[8]}, count(inner_instruction_index){names[9]},
       min(length(data)){names[10]}, max(length(data)){names[11]}
FROM solana.instruction_calls
WHERE block_date = DATE '{day}'
  AND executing_account IN ({programs})
GROUP BY 3"""
    body = "\nUNION ALL\n".join(one(i, name, probe_day(d1).isoformat()) for i, (name, _d0, d1) in enumerate(chunks))
    said = ("""
-- Each day also counts the pump program's calls: a day with no fee-program row is then a day the table had
-- rows on, and the four index and flag columns get a fill count in every era, including the ones before the
-- fee program existed.""" if control else "")
    return f"""-- Fee-share, evidence per era (E38): the fee program's calls by kind on one day per chunk, {len(chunks)} single-day counts.
-- Counts only; no wallet in the output.
-- It decides which chunks have anything to extract: until it has run, an empty result of query 1 is not a
-- result. An instruction is named by its 8-byte discriminator, an event by its 16-byte prefix (wrapper tag
-- {TAG}, then the event discriminator), so a kind the pinned IDL lacks in some era shows up under
-- its own hex. The three events query 1 takes are, in order (made, changed, reset):
--   {" ".join(EVENT_PREFIX[n] for n in dfs.IN_FORCE)}
-- The day is the last one query 1 scans for the chunk (its last graduation day + 2).{said}
{body}"""


# ------------------------------------------------------------------ the self-check

# Every large table of the build, the three this module reads first. gen_chunks.LARGE does not have the raw
# instruction table yet; until it does, gen_chunks.review() does not look at it.
LARGE = ("solana.instruction_calls", "tokens_solana.transfers", "tokens_solana.sol_transfers",
         "pumpdotfun_solana.pump_amm_evt_buyevent", "pumpdotfun_solana.pump_amm_evt_sellevent",
         "pumpdotfun_solana.pump_evt_tradeevent")
_BOUND = r"(block_date|evt_block_date|call_block_date)\s+(BETWEEN|=)\s+DATE '|block_time >= TIMESTAMP '"   # gen_chunks.review's own
_PLACEHOLDER = r"__[A-Z][A-Z_]*(?:\([^)]*\))?__"                                                           # the runner's own
DESIGN = {"fs": ({"solana.instruction_calls": 1}, ("__NOT_OWNER_HEX(e.data_hex)__",)),
          "fsm": ({"tokens_solana.transfers": 1, "tokens_solana.sol_transfers": 1}, ("__FS_PAIRS__", "__NOT_OWNER(w)__"))}


def check(sql: str, tables: dict[str, int], dates: tuple[str, ...], placeholders: tuple[str, ...] = (),
          wallets: bool = True) -> list[str]:
    """What can be checked without running anything, on the raw text, comments included. The same rules as
    gen_chunks.review(), with the reference count held to exactly the design, plus the placeholders, the
    wallet line and balanced brackets."""
    problems = []
    for table in LARGE:
        refs = list(re.finditer(re.escape(table) + r"\b", sql))
        if len(refs) != tables.get(table, 0):
            problems.append(f"{table}: {len(refs)} references, designed {tables.get(table, 0)}")
        for m in refs:
            if not re.search(_BOUND, sql[m.end():m.end() + 900]):
                problems.append(f"{table}: no literal partition bound after reference at {m.start()}")
    problems += [f"date not in the text: {d}" for d in dates if f"DATE '{d}'" not in sql]
    problems += [f"verdict constant: line {no}: {text}" for _n, no, text in verdict_constants.hits(sql)]
    if re.search(r"\b(CREATE|INSERT|DELETE|UPDATE)\b", sql):
        problems.append("statement other than SELECT")
    if re.search(r"block_date[^\n]*\bOR\b|\bOR\b[^\n]*block_date", sql):
        problems.append("OR on a partition column")
    if sorted(set(re.findall(_PLACEHOLDER, sql))) != sorted(placeholders):
        problems.append(f"placeholders in the text: {sorted(set(re.findall(_PLACEHOLDER, sql)))}, designed {sorted(placeholders)}")
    if (sql.split("\n")[1] == WALLET_LINE) != wallets or ("--private-rows" in sql) != wallets:
        problems.append("line 2 does not say what the rows hold")
    code = verdict_constants.strip(sql)
    if code.count("(") != code.count(")") or "{" in sql or "}" in sql:
        problems.append("brackets do not balance, or a format field was left in the text")
    if not re.match(r"\s*(WITH|SELECT)\b", code):
        problems.append("does not begin with WITH or SELECT")
    return problems


def proving_files(chunks: list[tuple[str, dt.date, dt.date]]) -> dict[str, tuple[str, list[str]]]:
    """file name -> (text, problems found by check()). Nothing is written here."""
    raw = {"solana.instruction_calls": 1}
    hexarg = ("__NOT_OWNER_HEX(data_hex)__",)
    out = {"fs_columns_probe.sql": (columns_probe(), raw, (PROVING_DAY,), (), False),
           "fs_scan_events_pbday.sql": (scan_events(), raw, (PROVING_DAY,), hexarg, True),
           "fs_scan_both_pbday.sql": (scan_both(), raw, (PROVING_DAY,), ("__NOT_OWNER_HEX(c.data_hex)__",), True),
           "fs_disc_by_month_probe.sql": (disc_by_month(chunks), {"solana.instruction_calls": len(chunks)},
                                          tuple(probe_day(d1).isoformat() for _n, _d0, d1 in chunks), (), False)}
    for tag, (a, b) in {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"),
                        "pbday": (PROVING_DAY, PROVING_DAY)}.items():
        for kind, fn in (("fs", fs), ("fsm", fsm)):
            out[f"feeshare_{kind}_{tag}.sql"] = (fn(a, b), DESIGN[kind][0], (a, b), DESIGN[kind][1], True)
    return {name: (sql, check(sql, *rest)) for name, (sql, *rest) in out.items()}


if __name__ == "__main__":
    import gen_chunks      # here and not at the top: gen_chunks imports the builders of this module
    out = ROOT / "recon" / "sql"
    files = proving_files(gen_chunks.chunks())
    bad = {name: problems for name, (_sql, problems) in files.items() if problems}
    if bad:     # a text that fails the static check is not written
        raise SystemExit("\n".join(f"{name}: {p}" for name, problems in bad.items() for p in problems))
    for name, (sql, _problems) in files.items():
        (out / name).write_text(sql, encoding="utf-8")
    print("written, static check and threshold check passed: " + ", ".join(files))

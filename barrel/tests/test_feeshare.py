"""Fee-share recipients (S7b, c07b): the local decode (recon/decode_feeshare.py) and the two query texts
(recon/gen_feeshare.py). No network, no credits.

Every key here is synthetic (32 equal bytes). Payloads are built by this file's own writer from the pinned
IDL's field order, with discriminators recomputed from the event names, so the decoder is held against
something it did not produce.
Run: python -m pytest barrel/tests/test_feeshare.py"""
import ast
import datetime as dt
import hashlib
import pathlib
import re
import struct
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "recon"))
import decode_feeshare as dfs  # noqa: E402
import gen_feeshare as g  # noqa: E402
import gen_heavy_b1 as b1  # noqa: E402
import verdict_constants as vc  # noqa: E402

SQL = ROOT / "recon" / "sql"
TAG = bytes.fromhex("E445A52E51CB9A1D")          # the first 8 bytes of 10,193 inner calls counted on 2026-09-01
GRAD = dt.datetime(2026, 9, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
MINT, OTHER_MINT, OWNER = 1, 2, 250
# day, week, pbday, then the chunk bounds whose day counts (12, 30, 31, 31, 20) include registered constants
BOUNDS = [("2025-06-09", "2025-06-09"), ("2025-06-09", "2025-06-15"), ("2026-09-01", "2026-09-01"),
          ("2025-03-20", "2025-03-31"), ("2025-06-01", "2025-06-30"), ("2026-07-01", "2026-07-31"),
          ("2026-08-01", "2026-08-31"), ("2026-09-01", "2026-09-20")]


# ------------------------------------------------------------ a writer of its own

def pk(i: int) -> bytes:
    return bytes([i]) * 32


def addr(i: int) -> str:
    return dfs.b58encode(pk(i))


def disc(kind: str, name: str) -> bytes:
    return hashlib.sha256(f"{kind}:{name}".encode()).digest()[:8]


def shares(n: int, first: int = 10) -> list[tuple[int, int]]:
    """n shareholders (keys first, first+1, ..) whose shares sum to 10,000 bps."""
    return [(first + k, 10_000 - 100 * (n - 1) if k == 0 else 100) for k in range(n)]


def vec(sh) -> bytes:
    return struct.pack("<I", len(sh)) + b"".join(pk(i) + struct.pack("<H", bps) for i, bps in sh)


def as_list(sh) -> list[dict]:
    return [{"w": addr(i), "bps": bps} for i, bps in sh]


def create(sh, mint=MINT, pool=None, status=1) -> bytes:
    return (TAG + disc("event", "CreateFeeSharingConfigEvent") + struct.pack("<q", 1) + pk(mint) + pk(200)
            + (b"\x00" if pool is None else b"\x01" + pk(pool)) + pk(201) + pk(202) + vec(sh) + bytes([status]))


def update(sh, mint=MINT) -> bytes:
    return TAG + disc("event", "UpdateFeeSharesEvent") + struct.pack("<q", 1) + pk(mint) + pk(201) + pk(202) + vec(sh) + b"\x01"


def reset(old, new, mint=MINT) -> bytes:
    return (TAG + disc("event", "ResetFeeSharingConfigEvent") + struct.pack("<q", 1) + pk(mint) + pk(201) + pk(202) + vec(old)
            + pk(203) + vec(new) + b"\x01\x02")


def when(minutes: float = 0) -> str:
    """A time `minutes` after graduation, as Dune writes it."""
    return (GRAD + dt.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S.000 UTC")


def row(payload, minutes, *, mint=MINT, tx=0, outer=0, inner=1, slot=None, **kw) -> dict:
    return {"mint": addr(mint), "grad_time": when(), "t0": when(-600),
            "block_slot": 1_000_000 + int(minutes * 150) if slot is None else slot, "block_time": when(minutes),
            "tx_index": tx, "outer_instruction_index": outer, "inner_instruction_index": inner, "tx_id": f"tx-{minutes}-{tx}",
            "tx_success": True, "evt": None if payload is None else payload[8:16].hex().upper(),
            "data_hex": None if payload is None else payload.hex().upper(), "owner_in_payload": False, **kw}


def token_row(mint=MINT) -> dict:
    """The row the first query keeps for a token with no event in its window."""
    return {"mint": addr(mint), "grad_time": when(), "t0": when(-600), **{c: None for c in (
        "block_slot", "block_time", "tx_index", "outer_instruction_index", "inner_instruction_index", "tx_id", "tx_success",
        "evt", "data_hex", "owner_in_payload")}}


# ------------------------------------------------------------------- the decoder

def test_the_wrapper_tag_is_the_hash_prefix_reversed():
    h = hashlib.sha256(b"anchor:event").digest()[:8]
    assert dfs.EVENT_IX_TAG == h[::-1] == TAG and dfs.EVENT_IX_TAG != h
    assert dfs.decode_event((h + create(shares(1))[8:]).hex())["why"] == "no event tag"     # the unreversed prefix is not an event


def test_the_three_events_and_the_instructions_come_from_the_pinned_idl():
    lay = dfs.layouts()
    assert {n for n, _ in lay["events"].values()} == set(dfs.IN_FORCE)
    assert all(d == disc("event", n) for d, (n, _f) in lay["events"].items())
    assert {n for n, _a, _acc in lay["ix"].values()} == {"create_fee_sharing_config", *dfs.UPDATE_IX, dfs.RESET_IX}
    assert all(d == disc("global", n) for d, (n, _a, _acc) in lay["ix"].items())
    assert [a for n, a, _acc in lay["ix"].values() if n == "create_fee_sharing_config"] == [[]]   # no arguments: only the event names recipients


@pytest.mark.parametrize("n", range(1, 11))
def test_each_event_decodes_with_one_to_ten_shareholders(n):
    sh, old = shares(n), shares(2, first=100)
    for payload, name, pool in ((create(sh), "CreateFeeSharingConfigEvent", None), (create(sh, pool=77), "CreateFeeSharingConfigEvent", addr(77)),
                                (update(sh), "UpdateFeeSharesEvent", None), (reset(old, sh), "ResetFeeSharingConfigEvent", None)):
        d = dfs.decode_event(payload.hex().upper())
        assert (d["event"], d["code"], d["absent"], d["trailing_bytes"]) == (name, None, [], 0)
        assert d["shareholders"] == as_list(sh) and d["fields"]["mint"] == addr(MINT)
        if name.startswith("Create"):
            assert d["fields"]["pool"] == pool and d["fields"]["status"] == "Active"
        if name.startswith("Reset"):
            assert dfs._shareholders(d["fields"]["old_shareholders"]) == as_list(old)      # the list in force is the NEW one


@pytest.mark.parametrize("payload", [create(shares(3)), create(shares(2), pool=77), update(shares(10)), reset(shares(2), shares(3))])
def test_a_truncated_payload_is_recorded_and_never_used(payload):
    for cut in range(len(payload)):
        d = dfs.decode_event(payload[:cut].hex())
        assert d["code"] == dfs.U_UNDECODED, cut
        if cut >= 16:
            assert d["why"] == "bytes missing" and d["absent"], cut
    last = dfs.decode_event(payload[:-1].hex())                 # only the last one-byte field is missing: what was read is kept as read
    assert last["absent"] and last["shareholders"] is not None and last["code"] == dfs.U_UNDECODED


def test_an_over_long_payload_is_recorded_and_never_used():
    d = dfs.decode_event((update(shares(2)) + b"\x00\x07").hex())
    assert (d["code"], d["why"], d["trailing_bytes"]) == (dfs.U_UNDECODED, "bytes left over", 2)
    assert d["shareholders"] == as_list(shares(2))              # recorded, and the caller sees the code first


def test_bytes_the_layout_does_not_allow_and_shares_that_do_not_sum():
    bad_option = create(shares(1)).replace(pk(200) + b"\x00", pk(200) + b"\x02", 1)
    assert dfs.decode_event(bad_option.hex())["why"] == "option tag 2"
    assert dfs.decode_event(create(shares(1), status=7).hex())["why"] == "ConfigStatus variant 7"
    assert dfs.decode_event(update([(10, 6000), (11, 3000)]).hex())["code"] == dfs.U_BPS
    assert dfs.decode_event(update([]).hex())["code"] == dfs.U_BPS          # an empty list does not sum to 10,000 either
    assert dfs.decode_event("not hex")["code"] == dfs.decode_event(None)["code"] == dfs.U_UNDECODED


def test_an_unknown_discriminator_is_counted_never_dropped():
    odd = TAG + disc("event", "DonationFeePdaCreated") + struct.pack("<q", 1) + pk(MINT) + pk(9)
    d = dfs.decode_event(odd.hex())
    assert d["code"] == dfs.U_UNKNOWN_EVENT and d["disc"] == disc("event", "DonationFeePdaCreated").hex().upper() and d["event"] is None
    hist = dfs.history([row(create(shares(1)), -10), row(odd, 5)])
    c = dfs.counts(hist)
    assert c["kinds"] == {"CreateFeeSharingConfigEvent": 1, f"not decoded ({d['disc']})": 1}
    assert c["codes"] == {"usable": 1, dfs.U_UNKNOWN_EVENT: 1}
    assert dfs.state_at(hist[addr(MINT)]["events"], int(GRAD.timestamp()) + 900)["state"] == dfs.UNKNOWN


# ------------------------------------------------------------ order, as-of, state

def test_a_config_made_and_changed_in_one_transaction_is_ordered_by_instruction_index():
    made, changed = create(shares(1, first=10)), update(shares(2, first=20))
    for a, b in (({"outer": 2, "inner": 1}, {"outer": 3, "inner": 1}), ({"outer": 2, "inner": 1}, {"outer": 2, "inner": 3})):
        rows = [row(changed, -10, tx=7, **b), row(made, -10, tx=7, **a)]             # given in the wrong order, same slot and second
        ev = dfs.history(rows)[addr(MINT)]["events"]
        assert [e["event"] for e in ev] == ["CreateFeeSharingConfigEvent", "UpdateFeeSharesEvent"] and not any(e["code"] for e in ev)
        assert dfs.token_states(rows)[addr(MINT)][15]["recipients"] == as_list(shares(2, first=20))


def test_as_of_at_the_three_lags_with_a_change_between_two_of_them():
    a, bc, d = shares(1, first=10), shares(2, first=20), shares(3, first=30)
    rows = [row(create(a), -10), row(update(bc), 30), row(reset(bc, d), 100), token_row(OTHER_MINT)]
    st = dfs.token_states(rows)
    got = {lag: (r["state"], r["c07b"], r["recipients"]) for lag, r in st[addr(MINT)].items()}
    assert got == {15: (dfs.MEASURED, 1, as_list(a)), 60: (dfs.MEASURED, 2, as_list(bc)), 240: (dfs.MEASURED, 3, as_list(d))}
    assert {lag: (r["state"], r["c07b"], r["recipients"], r["code"]) for lag, r in st[addr(OTHER_MINT)].items()} == \
        {lag: (dfs.NOT_APPLICABLE, 0, [], None) for lag in dfs.LAGS}


def test_the_comparison_is_block_time_at_or_before_the_entry_time():
    at, after = dfs.token_states([row(create(shares(1)), 15)]), dfs.token_states([row(create(shares(1)), 15 + 1 / 60)])
    assert at[addr(MINT)][15]["state"] == dfs.MEASURED                       # block_time <= T, as the token ledger compares
    assert after[addr(MINT)][15]["state"] == dfs.NOT_APPLICABLE and after[addr(MINT)][60]["state"] == dfs.MEASURED


def test_a_withheld_payload_makes_the_state_unknown_from_that_event_on():
    rows = [row(create(shares(1, first=10)), -10), row(None, 30, owner_in_payload=True, evt="15BAC4B85BE4E1CB"),
            row(update(shares(2, first=20)), 100)]
    st = dfs.token_states(rows)[addr(MINT)]
    assert (st[15]["state"], st[15]["c07b"]) == (dfs.MEASURED, 1)
    for lag in (60, 240):                                                    # a later clean event does not bring it back
        assert (st[lag]["state"], st[lag]["code"], st[lag]["recipients"], st[lag]["c07b"], st[lag]["in_force"]) == \
            (dfs.UNKNOWN, dfs.U_WITHHELD, None, None, None)
    assert "OWNER" not in dfs.U_WITHHELD.upper()                             # the code goes to a public column
    # the token is asked about for the lag at which it was measured, and for nothing after
    assert dfs.pairs({addr(MINT): st}) == ([{"mint": addr(MINT), "w": addr(10)}], 0)


def test_any_unusable_event_holds_from_then_on_and_in_force_keeps_the_other_reading():
    rows = [row(create(shares(1, first=10)), -10), row(update(shares(2, first=20))[:-1], 30), row(update(shares(1, first=40)), 100)]
    st = dfs.token_states(rows)[addr(MINT)]
    assert (st[60]["state"], st[60]["code"], st[60]["in_force"]) == (dfs.UNKNOWN, dfs.U_UNDECODED, None)
    assert (st[240]["state"], st[240]["code"], st[240]["recipients"]) == (dfs.UNKNOWN, dfs.U_UNDECODED, None)
    assert st[240]["in_force"] == as_list(shares(1, first=40))               # the last event alone is clean: the other reading
    assert {p["w"] for p in dfs.pairs({addr(MINT): st})[0]} == {addr(10), addr(40)}


def test_rows_of_failed_transactions_are_left_out_and_an_empty_flag_is_unknown():
    rows = [row(create(shares(1, first=10)), -10), row(update(shares(1, first=20)), 5, tx_success=False)]
    hist = dfs.history(rows)[addr(MINT)]
    assert hist["n_failed"] == 1 and len(hist["events"]) == 1
    assert dfs.token_states(rows)[addr(MINT)][15]["recipients"] == as_list(shares(1, first=10))
    st = dfs.token_states([row(create(shares(1)), -10, tx_success=None)])[addr(MINT)][15]
    assert (st["state"], st["code"]) == (dfs.UNKNOWN, dfs.U_SUCCESS)


def test_order_that_the_filled_columns_do_not_decide_is_unknown_and_a_repeated_row_counts_once():
    one, two = row(create(shares(1)), -10, tx=None), row(update(shares(1, first=20)), -10, tx=None)
    two["tx_id"] = "another transaction in the same slot"
    st = dfs.token_states([one, two])[addr(MINT)][15]
    assert (st["state"], st["code"]) == (dfs.UNKNOWN, dfs.U_ORDER)
    same = row(create(shares(1)), -10)
    hist = dfs.history([same, dict(same)])[addr(MINT)]
    assert hist["n_repeated"] == 1 and len(hist["events"]) == 1 and hist["events"][0]["code"] is None


def test_a_payload_whose_mint_is_not_the_rows_is_not_used():
    st = dfs.token_states([row(create(shares(1), mint=OTHER_MINT), -10)])[addr(MINT)][15]
    assert (st["state"], st["code"]) == (dfs.UNKNOWN, dfs.U_UNDECODED)


def test_an_event_row_without_a_time_is_refused_not_placed():
    with pytest.raises(ValueError):
        dfs.history([row(create(shares(1)), -10, block_time=None)])


def test_the_pairs_are_the_union_over_the_lags_without_owner_wallets():
    rows = [row(create([(10, 10_000)]), -10), row(update([(20, 5000), (OWNER, 5000)]), 30), row(update([(10, 2500), (30, 7500)]), 100),
            row(create([(10, 10_000)], mint=OTHER_MINT), -10, mint=OTHER_MINT)]
    got, removed = dfs.pairs(dfs.token_states(rows), owner={addr(OWNER)})
    want = sorted([(addr(MINT), addr(10)), (addr(MINT), addr(20)), (addr(MINT), addr(30)), (addr(OTHER_MINT), addr(10))])
    assert got == [{"mint": m, "w": w} for m, w in want] and removed == 1      # each pair once; the number removed, never the address
    assert dfs.pairs(dfs.token_states([token_row()])) == ([], 0)


# ------------------------------------------------ an instruction against its event

def ix_update(sh, name="update_fee_shares") -> bytes:
    return disc("global", name) + vec(sh)


@pytest.mark.parametrize("name", dfs.UPDATE_IX)
@pytest.mark.parametrize("n", [1, 2, 10])
def test_an_updates_arguments_equal_its_event(name, n):
    a, e = dfs.decode_update_args(ix_update(shares(n), name).hex()), dfs.decode_event(update(shares(n)).hex())
    assert a["ix"] == name and a["code"] is None and a["trailing_bytes"] == 0 and a["shareholders"] == e["shareholders"] == as_list(shares(n))
    assert len(ix_update(shares(n), name)) == 8 + 4 + 34 * n                 # 46 bytes for one shareholder, as on the held rows


def test_the_args_decoder_records_what_it_cannot_read():
    assert dfs.decode_update_args((ix_update(shares(2)) + b"\x00").hex())["why"] == "bytes left over"
    assert dfs.decode_update_args(ix_update(shares(2))[:-3].hex())["why"] == "bytes missing"
    assert dfs.decode_update_args(disc("global", "create_fee_sharing_config").hex())["why"] == "not an update instruction"


def test_crosscheck_pairs_each_instruction_with_the_event_under_it():
    def r(kind, payload, tx, outer, inner):
        return {"tx_id": tx, "block_slot": 5, "tx_index": 1, "outer_instruction_index": outer, "inner_instruction_index": inner,
                "kind": kind.hex().upper(), "data_hex": None if payload is None else payload.hex().upper()}
    ev_c, ev_u = TAG + disc("event", "CreateFeeSharingConfigEvent"), TAG + disc("event", "UpdateFeeSharesEvent")
    ix_c, ix_u, ix_v2 = (disc("global", n) for n in ("create_fee_sharing_config", "update_fee_shares", "update_fee_shares_v2"))
    rows = [r(ix_c, ix_c, "A", 2, None), r(ev_c, create(shares(1)), "A", 2, 0),                        # made, then changed, in one transaction
            r(ix_v2, ix_update(shares(2), "update_fee_shares_v2"), "A", 3, None), r(ev_u, update(shares(2)), "A", 3, 0),
            r(ix_u, ix_update(shares(1, first=10)), "B", 0, None), r(ev_u, update(shares(1, first=11)), "B", 0, 0),   # a different recipient
            r(ix_u, ix_update(shares(1)), "C", 0, None),                                               # no event came back
            r(ix_u, None, "D", 0, None), r(ev_u, None, "D", 0, 0),                                     # payloads withheld
            r(disc("global", "get_fees"), b"\x00", "E", 0, None)]
    assert dfs.crosscheck(rows) == {"create_fee_sharing_config: event found": 1, "update_fee_shares_v2: arguments equal the event": 1,
                                    "update_fee_shares: arguments DIFFER from the event": 1, "update_fee_shares without its event": 1,
                                    "payload withheld or empty": 2, f"kind not known ({disc('global', 'get_fees').hex().upper()})": 1}


# ------------------------------------------------------------------ the module

def test_the_decoder_reads_nothing_but_the_idl():
    """It must not import the module it ports base58 from: that import reads the credentials file."""
    tree = ast.parse((ROOT / "recon" / "decode_feeshare.py").read_text(encoding="utf-8"))
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert imported <= {"__future__", "collections", "datetime", "functools", "hashlib", "json", "pathlib", "typing"}


def test_base58_is_the_shared_definition_ported_not_rewritten():
    def defs(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        fns = {n.name: ast.dump(n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ("b58encode", "b58decode")}
        consts = {t.id: ast.dump(n.value) for n in tree.body if isinstance(n, ast.Assign) for t in n.targets
                  if isinstance(t, ast.Name) and t.id in ("_B58", "_B58_IDX")}
        return fns, consts
    assert defs(ROOT / "recon" / "decode_feeshare.py") == defs(ROOT / "recon" / "decode_pumpswap_events.py")
    assert dfs.b58decode(addr(0)) == pk(0) and dfs.b58decode(addr(255)) == pk(255) and addr(0) == "1" * 32


# -------------------------------------------------------------- the query texts

def days(d: str, n: int) -> str:
    return (dt.date.fromisoformat(d) + dt.timedelta(days=n)).isoformat()


def out_columns(sql: str) -> list[str]:
    """Output names of the final SELECT: the alias, or the column's own name."""
    body = vc.strip(sql)
    body = body[body.rindex("\nSELECT ") + len("\nSELECT "):body.rindex("\nFROM ")]
    items, depth, cur = [], 0, ""
    for ch in body:
        depth += (ch == "(") - (ch == ")")
        if ch == "," and depth == 0:
            items.append(cur)
            cur = ""
        else:
            cur += ch
    items.append(cur)
    return [re.split(r"\s+AS\s+", " ".join(i.split()))[-1].split(".")[-1] for i in items]


@pytest.mark.parametrize("a,b", BOUNDS)
def test_both_queries_pass_the_static_check_and_the_threshold_check(a, b):
    for kind, fn in (("fs", g.fs), ("fsm", g.fsm)):
        sql = fn(a, b)
        tables, placeholders = g.DESIGN[kind]
        assert vc.hits(sql) == [], kind
        # exactly one reference to each large table it reads and none to any other, a literal bound within 900
        # characters of each, the chunk's dates as DATE literals, its placeholders and no other, the wallet line
        assert g.check(sql, tables, (a, b), placeholders) == [], kind
        assert sql.split("\n")[1] == "-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here."


@pytest.mark.parametrize("a,b", BOUNDS)
def test_the_literal_bounds_are_the_per_token_windows_at_their_widest(a, b):
    fs, fsm = g.fs(a, b), g.fsm(a, b)
    assert f"FROM solana.instruction_calls\n  WHERE block_date BETWEEN DATE '{days(a, -3)}' AND DATE '{days(b, 2)}'" in fs
    assert f"WHERE t.block_date BETWEEN DATE '{days(a, -3)}' AND DATE '{days(b, 10)}'" in fsm
    assert (f"FROM tokens_solana.sol_transfers s\n  WHERE s.block_time >= TIMESTAMP '{days(a, -4)} 00:00:00' "
            f"AND s.block_time < TIMESTAMP '{days(b, 11)} 00:00:00'") in fsm
    # and inside the scan every window is the token's own
    assert "e.block_time >= date_trunc('day', b.grad_time) - INTERVAL '3' DAY" in fs
    assert "e.block_time <= b.grad_time + INTERVAL '240' MINUTE" in fs


def test_the_universe_is_the_aligned_set_querys_own_text():
    a, b = "2026-08-01", "2026-08-31"
    head = b1.head(a, b)[0]
    assert head in g.fsm(a, b)                                               # comp, pc, u, cr, base, bal: verbatim
    universe = head[:head.index("bal AS (")].rstrip()
    assert universe in g.fs(a, b) and "bal AS (" not in g.fs(a, b) and universe.endswith("INTERVAL '3' DAY),")
    assert b1.members(a, b).startswith("-- Heavy tier B1a") and head in b1.members(a, b)     # the same head the member form is built on


def test_query_one_takes_the_three_events_and_nothing_wide():
    sql = g.fs("2026-09-01", "2026-09-01")
    want = [(TAG + disc("event", n)).hex().upper() for n in ("CreateFeeSharingConfigEvent", "UpdateFeeSharesEvent", "ResetFeeSharingConfigEvent")]
    assert "to_hex(substr(data, 1, 16)) IN (" + ", ".join(f"'{p}'" for p in want) + ")" in sql
    assert "substr(data, 25, 32) AS mint_bin" in sql and "from_base58(mint) AS mint_bin" in sql     # tag 8, discriminator 8, timestamp 8, mint
    for wide in ("account_arguments", "tx_signer", "is_inner", "log_messages"):
        assert wide not in sql
    assert "coalesce(tx_success, true)" in sql
    assert out_columns(sql) == ["mint", "grad_time", "t0", "block_slot", "block_time", "tx_index", "outer_instruction_index",
                                "inner_instruction_index", "tx_id", "tx_success", "evt", "data_hex", "owner_in_payload"]
    # an owner wallet inside a payload: the row comes back, the payload does not
    assert "CASE WHEN no_owner THEN data_hex END AS data_hex" in sql and "__NOT_OWNER_HEX(e.data_hex)__ AS no_owner" in sql
    assert "FROM bm b\n  LEFT JOIN ev e" in sql                               # a token with no event keeps a row


def test_query_two_returns_member_rows_in_the_aligned_set_querys_names():
    sql = g.fsm("2026-09-01", "2026-09-01")
    cols = out_columns(sql)
    assert cols == ["mint", "w", "is_creator", "is_funded", "link", "bundle_n", "cr_sol", "grad_time", "t0", "first_in", "b15", "b60", "b240",
                    "net_after", *[f"f{k}" for k in range(8)], "got_mint", "funder", "funded_ts", "funded_sol", "n_senders", "fund_to_first_buy_s"]
    theirs = set(out_columns(b1.members("2026-09-01", "2026-09-01")))
    assert set(cols) - theirs == {*[f"f{k}" for k in range(8)], "got_mint"}   # what the member form keeps inside and the build form sums
    assert "FROM (VALUES __FS_PAIRS__) AS t(mint, w)\n  WHERE mint IS NOT NULL AND w IS NOT NULL AND __NOT_OWNER(w)__" in sql
    # the aligned-set query's own funding predicates and link typing, unchanged but for the alias
    theirs_sql = b1.members("2026-09-01", "2026-09-01")
    assert "AND (m.first_in_tx IS NULL OR s.tx_id IS NULL OR s.tx_id <> m.first_in_tx)" in theirs_sql
    assert "(m.first_in_tx IS NULL OR t.tx_id IS NULL OR t.tx_id <> m.first_in_tx)" in sql and "t.block_slot <= m.slot_act" in sql
    assert "t.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY" in sql and "CAST(s.amount AS double) >= 1e6" in sql
    assert "THEN CASE WHEN cardinality(s.cr_txs) > 1 THEN 'both' ELSE 'token_delivery_by_creator' END" in theirs_sql
    assert "THEN CASE WHEN cardinality(o.cr_txs_m) > 1 THEN 'both' ELSE 'token_delivery_by_creator' END" in sql
    assert "ORDER BY slot_last DESC NULLS LAST, sender DESC" in sql
    # each block that reads a large table is read by one block only
    code = vc.strip(sql)
    for block in ("bal", "mm", "tr", "snd", "one"):
        assert len(re.findall(rf"\b(?:FROM|JOIN) {block}\b", code)) == 1, block


def test_gen_chunks_review_passes_once_the_kinds_are_registered(monkeypatch):
    import gen_chunks
    for kind, fn in (("fs", g.fs), ("fsm", g.fsm)):
        for table, n in g.DESIGN[kind][0].items():
            monkeypatch.setitem(gen_chunks.LARGE, table, {**gen_chunks.LARGE.get(table, {}), kind: n})
    for _name, d0, d1 in gen_chunks.chunks():
        for kind, fn in (("fs", g.fs), ("fsm", g.fsm)):
            sql = fn(d0.isoformat(), d1.isoformat())
            assert gen_chunks.review(kind, sql, d0, d1) == [], (kind, d0)
            assert g.check(sql, g.DESIGN[kind][0], (d0.isoformat(), d1.isoformat()), g.DESIGN[kind][1]) == [], (kind, d0)
            # a list is substituted wherever its placeholder stands: it stands once, and never in a comment
            assert all(sql.count(p) == 1 and p in vc.strip(sql) for p in g.DESIGN[kind][1]), (kind, d0)


def test_the_proving_files_on_disk_are_what_the_generator_writes():
    import gen_chunks
    files = g.proving_files(gen_chunks.chunks())
    assert sorted(files) == sorted(["fs_columns_probe.sql", "fs_scan_events_pbday.sql", "fs_scan_both_pbday.sql", "fs_disc_by_month_probe.sql",
                                    *[f"feeshare_{k}_{t}.sql" for k in ("fs", "fsm") for t in ("day", "week", "pbday")]])
    for name, (sql, problems) in files.items():
        assert problems == [] and vc.hits(sql) == [], name
        assert (SQL / name).read_text(encoding="utf-8") == sql, name


def test_the_probes_are_one_partition_each():
    import gen_chunks
    chunks = gen_chunks.chunks()
    by_month = g.disc_by_month(chunks)
    assert len(chunks) == 19 and by_month.count("\nUNION ALL\n") == 18 and by_month.count("FROM solana.instruction_calls") == 19
    for name, _d0, d1 in chunks:        # the last day query 1 scans for the chunk
        assert f"SELECT '{name}'" in by_month and f"WHERE block_date = DATE '{days(d1.isoformat(), 2)}'" in by_month
    assert f"DATE '{days(chunks[0][2].isoformat(), 2)}'" in g.fs(chunks[0][1].isoformat(), chunks[0][2].isoformat())
    assert g.PUMP in by_month and g.PUMP not in g.disc_by_month(chunks, control=False)      # the control is one argument
    assert g.check(g.disc_by_month(chunks, control=False), {"solana.instruction_calls": 19}, (), wallets=False) == []
    for sql in (g.scan_events(), g.scan_both()):
        assert sql.count("solana.instruction_calls") == 1 and "WHERE block_date = DATE '2026-09-01'" in sql
    assert "account_arguments" not in g.scan_events() and "element_at(account_arguments, 5)" in g.scan_both()
    # the column probe is the one form of a bare star the request function lets through (dune_roundtrip.dune)
    probe = vc.strip(g.columns_probe()).strip()
    assert re.match(r"(?is)^SELECT\s+\*\s+FROM\b", probe) and re.search(r"(?i)\bLIMIT\s+0\s*$", probe)

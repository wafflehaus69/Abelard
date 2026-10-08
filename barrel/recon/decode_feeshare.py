"""Fee-share recipients (S7b, c07b): the local half. Pure functions, no network, no credits.

The first fee-share query (recon/gen_feeshare.py, kind `fs`) returns the fee program's sharing-config
events raw: one row per event, the payload as hex. Nothing is decoded in Dune, so a layout the pinned
IDL does not describe is fixed here at no cost instead of by scanning again. This module

  * decodes one payload with the pinned IDL (recon/idl/pump_fees.json): the event wrapper tag, the
    event discriminator, then the Borsh body with its vec, Option and enum;
  * orders a token's events by (block_slot, tx_index, outer_instruction_index, inner_instruction_index);
  * gives the recipient list in force at graduation + 15, 60 and 240 minutes (R7: as of T, from the
    history, never today's state), with the comparison the token ledger uses, block_time <= T;
  * writes the (mint, wallet) pairs the second query (kind `fsm`) is asked about.

A recipient is an entry {address, share_bps} of the shareholders list in a token's sharing config.
Three events write that list and each carries the WHOLE new list, so the last one at or before T is
the config in force at T:
    CreateFeeSharingConfigEvent   initial_shareholders
    UpdateFeeSharesEvent          new_shareholders
    ResetFeeSharingConfigEvent    new_shareholders

Per token and lag the state is one of
    measured        the list in force is known; c07b is the number of distinct addresses in it
    not_applicable  no sharing-config event at or before T (c07b 0)
    unknown         an event at or before T could not be used; `code` is the reason, for u_codes
Nothing is guessed. Bytes missing from a payload or left over after it are recorded and the event is
not used; a discriminator the IDL does not have is counted, never dropped; a list whose shares do not
sum to 10,000 bps is not used. Unknown holds FROM THAT EVENT ON: a later clean event does not make the
token measured again. For a withheld payload that is the design; for the other reasons it is the
reading taken here, not a ruling, and `in_force` on each record carries the other one (the last event
alone decides) so that a ruling either way changes this file and nothing that was paid for.

The reason codes are neutral on purpose: u_codes is a public column, and a code must not say that an
operator's own wallet was involved (the same rule that named adj_applied).

This module never reads barrel/private. The caller passes the owner wallets to pairs().

Base58 is ported from recon/decode_pumpswap_events.py and not imported: importing that module reads
the credentials file (it builds its RPC address at import). tests/test_feeshare.py holds the two
definitions to the same source.
"""
from __future__ import annotations

import collections
import datetime as dt
import functools
import hashlib
import json
import pathlib
from typing import Any, Iterable

IDL_PATH = pathlib.Path(__file__).with_name("idl") / "pump_fees.json"

# Anchor's emit_cpi! puts this tag in front of every event it sends as an inner call of the program to
# itself. Anchor defines the tag as the NUMBER 0x1d9acb512ea545e4 (sha256("anchor:event")[:8] read as
# one big-endian integer) and writes it little-endian, so the bytes on the wire are the hash prefix
# REVERSED: E4 45 A5 2E 51 CB 9A 1D. This project's own count agrees: 10,193 inner calls of the fee
# program began with exactly those bytes on 2026-09-01 (run_s7b_pfee_by_discriminator), and none with
# the unreversed prefix. decode_pumpswap_events.EVENT_IX_TAG is the unreversed prefix.
EVENT_IX_TAG = hashlib.sha256(b"anchor:event").digest()[:8][::-1]

# event -> the field that holds the list in force after it
IN_FORCE = {"CreateFeeSharingConfigEvent": "initial_shareholders",
            "UpdateFeeSharesEvent": "new_shareholders",
            "ResetFeeSharingConfigEvent": "new_shareholders"}
UPDATE_IX = ("update_fee_shares", "update_fee_shares_v2")
# In the pinned IDL only as its event; the instruction was identified by its discriminator
# (0A02B65F107F81BA, 3 inner calls on 2026-09-01). Its accounts and arguments are not known.
RESET_IX = "reset_fee_sharing_config"
LAGS = (15, 60, 240)                     # entry lags in minutes after graduation, as everywhere
BPS_TOTAL = 10_000                       # a structural check of the decode, not a threshold

MEASURED, NOT_APPLICABLE, UNKNOWN = "measured", "not_applicable", "unknown"
U_WITHHELD = "S7B_PAYLOAD_WITHHELD"      # the query returned the row without its payload
U_UNDECODED = "S7B_EVENT_UNDECODED"      # no wrapper tag, bytes missing, bytes left over, a byte the layout does not allow
U_UNKNOWN_EVENT = "S7B_EVENT_UNKNOWN"    # a discriminator that is not one of the three sharing-config events
U_BPS = "S7B_BPS_SUM"                    # the decoded shares do not sum to 10,000
U_SUCCESS = "S7B_TX_SUCCESS_EMPTY"       # the row does not say whether its transaction succeeded
U_ORDER = "S7B_ORDER_UNDECIDED"          # two events of one token that the filled columns cannot put in order


class DecodeError(RuntimeError):
    """The pinned IDL is not what this module was written against. Never raised for a payload."""


class _Short(Exception):
    """The payload ended inside a field."""


class _Bad(Exception):
    """A byte the layout does not allow (an Option tag that is not 0 or 1, an enum variant out of range)."""


# ------------------------------------------------------------------------ base58

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58)}


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        if ch not in _B58_IDX:
            raise DecodeError(f"invalid base58 character {ch!r}")
        n = n * 58 + _B58_IDX[ch]
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + body


def b58encode(b: bytes) -> str:
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out


# ------------------------------------------------------------------------ layout

def _disc(kind: str, name: str) -> bytes:
    return hashlib.sha256(f"{kind}:{name}".encode()).digest()[:8]


@functools.lru_cache(maxsize=None)
def layouts(path: str = str(IDL_PATH)) -> dict[str, Any]:
    """The parts of the pinned IDL this module reads. Every discriminator is recomputed from its name
    (Anchor: sha256("event:<Name>")[:8], sha256("global:<name>")[:8]) and has to equal the IDL's, so a
    constant is never taken on trust from either side."""
    idl = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    types = {t["name"]: t["type"] for t in idl["types"]}
    events = {}
    for ev in idl["events"]:
        if ev["name"] in IN_FORCE:
            if bytes(ev["discriminator"]) != _disc("event", ev["name"]):
                raise DecodeError(f"{ev['name']}: the IDL's discriminator is not sha256('event:{ev['name']}')[:8]")
            fields = types[ev["name"]]["fields"]
            # the first query joins on bytes 25-56 of the payload: tag 8, discriminator 8, timestamp 8, then the mint
            if [(f["name"], f["type"]) for f in fields[:2]] != [("timestamp", "i64"), ("mint", "pubkey")]:
                raise DecodeError(f"{ev['name']}: does not begin with timestamp i64, mint pubkey")
            events[bytes(ev["discriminator"])] = (ev["name"], fields)
    if len(events) != len(IN_FORCE):
        raise DecodeError(f"expected {sorted(IN_FORCE)} in the IDL, found {sorted(n for n, _ in events.values())}")
    ix = {}
    for i in idl["instructions"]:
        if i["name"] in UPDATE_IX + ("create_fee_sharing_config",):
            if bytes(i["discriminator"]) != _disc("global", i["name"]):
                raise DecodeError(f"{i['name']}: the IDL's discriminator is not sha256('global:{i['name']}')[:8]")
            ix[bytes(i["discriminator"])] = (i["name"], i["args"], [a["name"] for a in i["accounts"]])
    if len(ix) != len(UPDATE_IX) + 1:
        raise DecodeError("the sharing-config instructions are not all in the IDL")
    ix[_disc("global", RESET_IX)] = (RESET_IX, None, None)
    return {"program": idl["address"], "types": types, "events": events, "ix": ix}


# ------------------------------------------------------------------------- borsh

_INT = {"u8": 1, "u16": 2, "u32": 4, "u64": 8, "i64": 8, "u128": 16, "i128": 16}


def _take(buf: bytes, pos: int, n: int) -> tuple[bytes, int]:
    if pos + n > len(buf):
        raise _Short()
    return buf[pos: pos + n], pos + n


def _read(buf: bytes, pos: int, typ, types: dict) -> tuple[Any, int]:
    """One Borsh value of IDL type `typ` at `pos`: (value, position after it)."""
    if isinstance(typ, str):
        if typ == "pubkey":
            raw, pos = _take(buf, pos, 32)
            return b58encode(raw), pos
        if typ == "bool":
            raw, pos = _take(buf, pos, 1)
            return raw[0] != 0, pos
        if typ == "string":
            n, pos = _read(buf, pos, "u32", types)
            raw, pos = _take(buf, pos, n)
            return raw.decode("utf-8", errors="replace"), pos
        if typ in _INT:
            raw, pos = _take(buf, pos, _INT[typ])
            return int.from_bytes(raw, "little", signed=typ.startswith("i")), pos
        raise DecodeError(f"unsupported IDL type {typ!r}")
    if "vec" in typ:
        n, pos = _read(buf, pos, "u32", types)
        if n > len(buf) - pos:      # every element is at least one byte: a count the payload cannot hold is a short payload
            raise _Short()
        out = []
        for _ in range(n):
            v, pos = _read(buf, pos, typ["vec"], types)
            out.append(v)
        return out, pos
    if "option" in typ:
        tag, pos = _take(buf, pos, 1)
        if tag[0] == 0:
            return None, pos
        if tag[0] != 1:
            raise _Bad(f"option tag {tag[0]}")
        return _read(buf, pos, typ["option"], types)
    if "defined" in typ:
        t = types[typ["defined"]["name"]]
        if t["kind"] == "struct":
            out = {}
            for f in t["fields"]:
                out[f["name"]], pos = _read(buf, pos, f["type"], types)
            return out, pos
        if t["kind"] == "enum" and not any(v.get("fields") for v in t["variants"]):
            i, pos = _take(buf, pos, 1)
            if i[0] >= len(t["variants"]):
                raise _Bad(f"{typ['defined']['name']} variant {i[0]}")
            return t["variants"][i[0]]["name"], pos
    raise DecodeError(f"unsupported IDL type {typ!r}")


def _shareholders(v: list[dict] | None) -> list[dict[str, Any]] | None:
    return None if v is None else [{"w": s["address"], "bps": s["share_bps"]} for s in v]


def _bps_ok(sh: list[dict[str, Any]]) -> bool:
    return sum(s["bps"] for s in sh) == BPS_TOTAL


def decode_event(data_hex: str) -> dict[str, Any]:
    """One event payload (the whole inner-call data, hex). Never raises for what is in the payload:
    `code` is None when the event can be used, otherwise the reason, with `why` in words."""
    out = {"event": None, "disc": None, "fields": {}, "absent": [], "trailing_bytes": 0,
           "shareholders": None, "code": None, "why": None}
    try:
        raw = bytes.fromhex(data_hex)
    except (TypeError, ValueError):
        return {**out, "code": U_UNDECODED, "why": "not hex"}
    if raw[:8] != EVENT_IX_TAG:
        return {**out, "code": U_UNDECODED, "why": "no event tag"}
    if len(raw) < 16:
        return {**out, "code": U_UNDECODED, "why": "bytes missing", "absent": ["discriminator"]}
    lay = layouts()
    out["disc"] = raw[8:16].hex().upper()
    if raw[8:16] not in lay["events"]:
        return {**out, "code": U_UNKNOWN_EVENT, "why": "discriminator not in the pinned IDL's sharing-config events"}
    out["event"], fields = lay["events"][raw[8:16]]
    body, pos = raw[16:], 0
    for i, f in enumerate(fields):
        try:
            out["fields"][f["name"]], pos = _read(body, pos, f["type"], lay["types"])
        except _Short:
            # this field and every one after it is ABSENT, never zero; what was read before it is kept as read
            return {**out, "absent": [x["name"] for x in fields[i:]], "code": U_UNDECODED, "why": "bytes missing",
                    "shareholders": _shareholders(out["fields"].get(IN_FORCE[out["event"]]))}
        except _Bad as ex:
            return {**out, "absent": [x["name"] for x in fields[i:]], "code": U_UNDECODED, "why": str(ex)}
    out["trailing_bytes"] = len(body) - pos
    out["shareholders"] = _shareholders(out["fields"][IN_FORCE[out["event"]]])
    if out["trailing_bytes"]:
        # the chain's layout is longer than the pinned IDL's: where the extra bytes belong is not known
        return {**out, "code": U_UNDECODED, "why": "bytes left over"}
    if not _bps_ok(out["shareholders"]):
        return {**out, "code": U_BPS, "why": f"shares sum to {sum(s['bps'] for s in out['shareholders'])}"}
    return out


def decode_update_args(data_hex: str) -> dict[str, Any]:
    """The arguments of an update_fee_shares / update_fee_shares_v2 INSTRUCTION: 8 bytes of
    discriminator, a u32 count, then 34 bytes per shareholder. Used only to check an event against
    the instruction that emitted it (the proving run); the history is read from events."""
    out = {"ix": None, "shareholders": None, "trailing_bytes": 0, "code": None, "why": None}
    try:
        raw = bytes.fromhex(data_hex)
    except (TypeError, ValueError):
        return {**out, "code": U_UNDECODED, "why": "not hex"}
    lay = layouts()
    name, args, _accounts = lay["ix"].get(raw[:8], (None, None, None))
    if name not in UPDATE_IX:
        return {**out, "ix": name, "code": U_UNKNOWN_EVENT, "why": "not an update instruction"}
    out["ix"] = name
    try:
        v, pos = _read(raw[8:], 0, args[0]["type"], lay["types"])
    except _Short:
        return {**out, "code": U_UNDECODED, "why": "bytes missing"}
    out["shareholders"], out["trailing_bytes"] = _shareholders(v), len(raw) - 8 - pos
    if out["trailing_bytes"]:
        return {**out, "code": U_UNDECODED, "why": "bytes left over"}
    if not _bps_ok(out["shareholders"]):
        return {**out, "code": U_BPS, "why": f"shares sum to {sum(s['bps'] for s in out['shareholders'])}"}
    return out


# ----------------------------------------------------------------------- history

def _secs(s: str | None) -> int | None:
    """Dune writes a timestamp as '2026-09-01 15:04:00.000 UTC'. Whole seconds, as block_time is."""
    if s is None:
        return None
    return int(dt.datetime.strptime(str(s)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
               .replace(tzinfo=dt.timezone.utc).timestamp())


_ORDER = ("block_slot", "tx_index", "outer_instruction_index", "inner_instruction_index")


def order_key(row: dict[str, Any]) -> tuple[int, int, int, int]:
    """Position on chain. An empty index sorts first, as an outer instruction does before its own inner
    calls; whether an empty index left two events undecided is checked separately (_undecided)."""
    return tuple(-1 if row.get(c) is None else int(row[c]) for c in _ORDER)


def _undecided(a: dict, b: dict) -> bool:
    """True when the filled columns do not say which of two events came first: the first column on
    which they could differ is empty on one of them, or they sit at the very same position."""
    for c in _ORDER:
        if a["pos"][c] is None or b["pos"][c] is None:
            return True
        if a["pos"][c] != b["pos"][c]:
            return False
    return True


def history(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Rows of the first query -> per token: graduation time, creation time and its events in chain
    order, each decoded and carrying `code` (None = usable). A token whose only row has no event (the
    query keeps one such row) has an empty list. Rows of a failed transaction are left out and counted;
    a row repeated exactly is taken once and counted."""
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        tok = out.setdefault(r["mint"], {"grad_time": _secs(r["grad_time"]), "t0": _secs(r.get("t0")), "events": [],
                                         "n_failed": 0, "n_repeated": 0, "_seen": set()})
        if r.get("block_slot") is None and r.get("evt") is None:
            continue                                        # the token's own row: no event in its window
        if r.get("tx_success", True) is False:
            tok["n_failed"] += 1
            continue
        ident = (order_key(r), r.get("tx_id"), r.get("data_hex"), bool(r.get("owner_in_payload")))
        if ident in tok["_seen"]:
            tok["n_repeated"] += 1
            continue
        tok["_seen"].add(ident)
        if r.get("owner_in_payload"):
            d = {"event": None, "shareholders": None, "code": U_WITHHELD, "why": "payload withheld by the query"}
        elif r.get("data_hex") is None:
            d = {"event": None, "shareholders": None, "code": U_UNDECODED, "why": "no payload"}
        else:
            d = decode_event(r["data_hex"])
            if not d["code"] and d["fields"].get("mint") != r["mint"]:
                d = {**d, "code": U_UNDECODED, "why": "the payload's mint is not the row's"}
        if not d["code"] and "tx_success" in r and r["tx_success"] is None:
            d = {**d, "code": U_SUCCESS, "why": "success flag empty"}
        if r.get("block_time") is None:
            raise ValueError("an event row without block_time cannot be placed against an entry time")
        tok["events"].append({"pos": {c: r.get(c) for c in _ORDER}, "key": order_key(r), "t": _secs(r["block_time"]),
                              "tx_id": r.get("tx_id"), "evt": r.get("evt"), "event": d.get("event"),
                              "shareholders": d.get("shareholders"), "code": d["code"], "why": d["why"]})
    for tok in out.values():
        del tok["_seen"]
        tok["events"].sort(key=lambda e: e["key"])
        for a, b in zip(tok["events"], tok["events"][1:]):
            if _undecided(a, b) and not b["code"]:
                b["code"], b["why"] = U_ORDER, "order against the event before it is not decided by the filled columns"
    return out


def state_at(events: list[dict[str, Any]], t: int) -> dict[str, Any]:
    """The recipient list in force at time `t` (seconds), from one token's events in chain order.

    recipients / c07b  the list and its distinct-address count when the state is measured; None when
                       unknown (an unknown must never read as a number); [] and 0 when not applicable
    in_force           the other reading, kept so a ruling does not need a new scan: the list of the
                       LAST event at or before t when that event alone is usable, whatever came before
                       it. None once a payload was withheld (the design leaves no other reading for
                       that), or when the last event is not usable."""
    seen = [e for e in events if e["t"] <= t]
    if not seen:
        return {"state": NOT_APPLICABLE, "code": None, "recipients": [], "c07b": 0, "in_force": [], "n_events": 0}
    last = seen[-1]
    withheld = any(e["code"] == U_WITHHELD for e in seen)
    in_force = None if withheld or last["code"] else last["shareholders"]
    bad = next((e for e in seen if e["code"]), None)
    if bad:
        return {"state": UNKNOWN, "code": bad["code"], "recipients": None, "c07b": None, "in_force": in_force,
                "n_events": len(seen)}
    return {"state": MEASURED, "code": None, "recipients": last["shareholders"],
            "c07b": len({s["w"] for s in last["shareholders"]}), "in_force": in_force, "n_events": len(seen)}


def token_states(rows: Iterable[dict[str, Any]], lags: tuple[int, ...] = LAGS) -> dict[str, dict[int, dict[str, Any]]]:
    """mint -> {lag in minutes -> state_at(graduation + lag)} for every token in the rows."""
    return {mint: {g: state_at(tok["events"], tok["grad_time"] + 60 * g) for g in lags}
            for mint, tok in history(rows).items()}


def pairs(states: dict[str, dict[int, dict[str, Any]]], owner: Iterable[str] = ()) -> tuple[list[dict[str, str]], int]:
    """The candidates for the second query: every address in force at any of the lags, as
    [{"mint", "w"}], sorted, each pair once. Built from `in_force`, the wider of the two readings, so
    the member query never has to be run again for a pair a later ruling lets in.

    `owner` is the caller's set of owner wallets (this module does not read it). The first query has
    already withheld every payload that holds one; this is the second lock. Returns the pairs and how
    many were removed for it: the number, never the address."""
    own, keep, removed = set(owner), set(), set()
    for mint, by_lag in states.items():
        for rec in by_lag.values():
            for s in rec["in_force"] or []:
                (removed if s["w"] in own else keep).add((mint, s["w"]))
    return [{"mint": m, "w": w} for m, w in sorted(keep)], len(removed)


def counts(hist: dict[str, dict[str, Any]]) -> dict[str, collections.Counter]:
    """What the rows held, for the run report: events by kind (a discriminator the IDL does not have is
    counted under its own hex), events by reason code, and the rows left out."""
    kinds, codes, rows = collections.Counter(), collections.Counter(), collections.Counter()
    for tok in hist.values():
        rows["tokens"] += 1
        rows["tokens_without_event"] += not tok["events"]
        rows["rows_of_failed_transactions"] += tok["n_failed"]
        rows["rows_repeated"] += tok["n_repeated"]
        for e in tok["events"]:
            kinds[e["event"] or f"not decoded ({e['evt']})"] += 1
            codes[e["code"] or "usable"] += 1
    return {"kinds": kinds, "codes": codes, "rows": rows}


# ------------------------------------------------- an event against its instruction

def crosscheck(rows: Iterable[dict[str, Any]]) -> collections.Counter:
    """Rows of the proving query that returns instructions AND events of one day (column `kind`: the
    8-byte discriminator of an instruction, or the 16-byte prefix of an event). Each event is the
    inner call that follows its instruction under the same outer instruction, so within one
    (transaction, outer instruction) the rows are walked in order and each update instruction is
    paired with the next update event. Counts what agreed: an update's arguments must be its event's
    list, entry for entry. Rows whose payload was withheld are counted and not compared."""
    lay = layouts()
    tag = EVENT_IX_TAG.hex().upper()
    ev_kind = {tag + d.hex().upper(): n for d, (n, _f) in lay["events"].items()}
    ix_kind = {d.hex().upper(): n for d, (n, _a, _acc) in lay["ix"].items()}
    emits = {"create_fee_sharing_config": "CreateFeeSharingConfigEvent", RESET_IX: "ResetFeeSharingConfigEvent",
             **{n: "UpdateFeeSharesEvent" for n in UPDATE_IX}}
    groups: dict[tuple, list[dict]] = collections.defaultdict(list)
    for r in rows:
        groups[(r.get("tx_id"), r.get("block_slot"), r.get("tx_index"), r.get("outer_instruction_index"))].append(r)
    out: collections.Counter = collections.Counter()
    for g in groups.values():
        pending: list[tuple[str, dict]] = []
        for r in sorted(g, key=order_key):
            kind, held = r.get("kind"), r.get("data_hex") is None
            if kind in ix_kind:
                if held:
                    out["payload withheld or empty"] += 1
                else:
                    pending.append((ix_kind[kind], r))
            elif kind in ev_kind:
                i = next((k for k, (n, _r) in enumerate(pending) if emits[n] == ev_kind[kind]), None)
                name, ix_row = pending.pop(i) if i is not None else (None, None)
                if held:
                    out["payload withheld or empty"] += 1
                elif name is None:
                    out[f"{ev_kind[kind]} without its instruction"] += 1
                elif name not in UPDATE_IX:
                    out[f"{name}: event found"] += 1
                else:
                    a, e = decode_update_args(ix_row["data_hex"]), decode_event(r["data_hex"])
                    same = not a["code"] and not e["code"] and a["shareholders"] == e["shareholders"]
                    out[f"{name}: arguments equal the event" if same else f"{name}: arguments DIFFER from the event"] += 1
            else:
                out[f"kind not known ({kind})"] += 1
        for name, _r in pending:
            out[f"{name} without its event"] += 1
    return out

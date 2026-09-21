"""BARREL M0 — A5 per-era fee schedule, read from PumpSwap's admin history.

v1.1 §A5 wants the fee schedule modelled per era. The first plan was to decode
every historical swap event out of BigQuery — petabyte-scale, and blocked on a
billing project. This is cheaper and more direct: every change to the fee
schedule is itself an admin transaction that emits a dated event
(`UpdateFeeConfigEvent`, `UpdateCreatorFeeConfigEvent`, ...). Read those and the
era boundaries are OBSERVED, not inferred from where swap fees happen to shift.

Path, all keyless:
  1. GlobalConfig account (from any swap's `global_config` account) -> `admin`.
  2. Page the admin's full signature history.
  3. Decode every PumpSwap event in the successful transactions.

    python barrel/recon/fee_schedule_history.py

Completeness caveat, checked rather than assumed: if the admin key ever changed
(`UpdateAdminEvent`), changes signed by the PREVIOUS admin are not in this key's
history. The script reports every admin handover it sees and fails loud if the
earliest history does not reach the config's creation.
"""

from __future__ import annotations

import collections
import datetime as dt
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as m  # noqa: E402  — shared rpc, base58, borsh, event extraction

GLOBAL_CONFIG = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw"
ADMIN = "FFWtrEQ4B4PKQoVuHYzZq8FabGkVatYzDpEVHsK5rrhF"   # GlobalConfig.admin, read 2026-09-21

# Events that change what a trader pays, or who receives it.
FEE_EVENTS = {
    "CreateConfigEvent", "UpdateFeeConfigEvent", "UpdateCreatorFeeConfigEvent",
    "UpdateAdminEvent", "ReservedFeeRecipientsEvent",
}


def decode_value(buf: bytes, pos: int, typ):
    """Decode one IDL value at pos -> (value, new_pos), or None if it does not fit."""
    if isinstance(typ, dict) and "array" in typ:
        inner, n = typ["array"]
        vals = []
        for _ in range(n):
            r = decode_value(buf, pos, inner)
            if r is None:
                return None
            v, pos = r
            vals.append(v)
        return vals, pos
    if isinstance(typ, dict) and "option" in typ:
        if pos + 1 > len(buf):
            return None
        if buf[pos] == 0:
            return None, pos + 1
        return decode_value(buf, pos + 1, typ["option"])
    if typ == "string":
        if pos + 4 > len(buf):
            return None
        n = int.from_bytes(buf[pos:pos + 4], "little")
        if pos + 4 + n > len(buf):
            return None
        return buf[pos + 4:pos + 4 + n].decode("utf-8", "replace"), pos + 4 + n
    if typ not in m._FIXED:
        raise m.DecodeError(f"unsupported IDL type {typ!r}")
    size = m._FIXED[typ]
    if pos + size > len(buf):
        return None
    raw = buf[pos:pos + size]
    if typ == "pubkey":
        return m.b58encode(raw), pos + size
    if typ == "bool":
        return raw[0] != 0, pos + size
    return int.from_bytes(raw, "little", signed=typ.startswith("i")), pos + size


def all_event_layouts(idl: dict) -> dict[bytes, tuple[str, list]]:
    types = {t["name"]: t for t in idl["types"]}
    return {bytes(e["discriminator"]): (e["name"], types[e["name"]]["type"]["fields"])
            for e in idl["events"] if e["name"] in types}


def decode_event(payload: bytes, layouts) -> dict | None:
    layout = layouts.get(payload[:8])
    if layout is None:
        return None
    name, fields = layout
    body, pos, out, absent = payload[8:], 0, {}, []
    for i, f in enumerate(fields):
        r = decode_value(body, pos, f["type"])
        if r is None:
            absent = [x["name"] for x in fields[i:]]
            break
        out[f["name"]], pos = r
    return {"event": name, "fields": out, "absent": absent, "trailing": len(body) - pos}


def utc(ts) -> str:
    return dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M") if ts else "?"


def main() -> None:
    idl = json.loads(m.IDL_PATH.read_text(encoding="utf-8"))
    layouts = all_event_layouts(idl)

    sigs, before = [], None
    while True:
        params = {"limit": 1000}
        if before:
            params["before"] = before
        page = m.rpc("getSignaturesForAddress", [ADMIN, params])
        if not page:
            break
        sigs += page
        before = page[-1]["signature"]
        if len(page) < 1000:
            break
        time.sleep(0.5)
    ok = [s for s in sigs if not s.get("err")]
    times = [s["blockTime"] for s in sigs if s.get("blockTime")]
    print(f"admin {ADMIN}")
    print(f"signatures: {len(sigs)} total, {len(ok)} successful, "
          f"span {utc(min(times))} -> {utc(max(times))}\n")

    records, kinds, rpc_fail = [], collections.Counter(), 0
    for s in reversed(ok):                       # oldest first
        try:
            tx = m.rpc("getTransaction",
                       [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}],
                       tries=5)
        except m.RpcError:
            rpc_fail += 1
            continue
        for payload in m.event_payloads(tx):
            ev = decode_event(payload, layouts)
            if ev is None:
                continue
            kinds[ev["event"]] += 1
            ev["slot"], ev["blockTime"], ev["signature"] = s.get("slot"), s.get("blockTime"), s["signature"]
            records.append(ev)
        time.sleep(0.3)

    print(f"admin transactions not fetched (rpc): {rpc_fail}"
          + ("   <-- history INCOMPLETE; re-run before using" if rpc_fail else ""))
    print(f"events by type: {dict(kinds.most_common())}\n")

    handovers = [r for r in records if r["event"] == "UpdateAdminEvent"]
    if handovers:
        print("ADMIN HANDOVERS — fee changes signed by an earlier admin are NOT in this history:")
        for r in handovers:
            print(f"   {utc(r['blockTime'])}  {r['fields']}")
        print()
    if not any(r["event"] == "CreateConfigEvent" for r in records):
        print("WARNING: no CreateConfigEvent in this admin's history — the config was created "
              "by another key, so the EARLIEST fee regime is not observed here.\n")

    print("== fee schedule timeline (events that change trader cost) ==")
    for r in records:
        if r["event"] not in FEE_EVENTS:
            continue
        f = dict(r["fields"])
        for k in list(f):
            if isinstance(f[k], list):
                f[k] = f"[{len(f[k])} pubkeys]"
        f.pop("timestamp", None)
        f.pop("admin", None)
        print(f"  {utc(r['blockTime'])}  {r['event']:28} {f}"
              + (f"   (absent: {r['absent']})" if r["absent"] else ""))

    out = pathlib.Path(__file__).with_name("fee_schedule_history.json")
    out.write_text(json.dumps({
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "admin": ADMIN, "global_config": GLOBAL_CONFIG,
        "signatures_total": len(sigs), "signatures_ok": len(ok), "rpc_failures": rpc_fail,
        "events": [{k: v for k, v in r.items()} for r in records],
    }, indent=1, default=str), encoding="utf-8")
    print(f"\nwritten: {out.name}")


if __name__ == "__main__":
    main()

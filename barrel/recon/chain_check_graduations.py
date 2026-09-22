"""1.2 validation against the chain: for sample graduations from Dune, confirm on
mainnet that (a) the bonding curve account is marked complete (pump IDL layout),
(b) the pool account exists and is owned by the PumpSwap program. Keyless, read-only.
No metadata fields are fetched (§1 / A7)."""
import base64, json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as m
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
idl = json.load(open(pathlib.Path(__file__).with_name("idl") / "pump.json"))
bc_disc = bytes([a for a in idl["accounts"] if a["name"] == "BondingCurve"][0]["discriminator"])
bc_fields = {t["name"]: t for t in idl["types"]}["BondingCurve"]["type"]["fields"]
SAMPLES = {  # from run of graduations_quote_confirm_day.sql (2026-09-01)
  "SOL": [("GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8","GSPSxykn9ocxrT7WubpjVXdnVKwET2ZRx4FWUezUKRjm","GCg7VGVMq38g7T8vxZ7qMzVkTPeWiKpwTPWvTz9L8tck"),
          ("FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump","9SRxKrUvHdwnsprahJL68ctLHpRzbhj8fDZL1vzRBPqT","GThiQvNXQm4C4p2azLXQWNtyVM6wBgsauyug27TeGvei"),
          ("BNr9ReNanVCCW6VX8J8CcRkPp4AyeEeYXMiYs6T4TdPm","4evWdVPpSnq9iRwfPBwrxwJRQm3Dq8ELgAHesFrFDHsy","7BHMubkbX71SoYdNM4HRy7MrbvSi6oJvMcH1Epwu8Csu")],
  "USDC":[("5xczFwu1UcM2H66dn9VGTPHEyXEHYyC97DeWcrev19xC","4Hn4LjrChtqaSabmHoQoaPvyghzP9D8AjKCbyGYT76VB","DGDZ7bpMogjUDeHN28ZYzF3jP6p9UzEgFhK7rxGSAS1x"),
          ("3V2ksJNCwTN1LtCg8HSQDcABo6UCMiXwDmut5EZJpump","GCz52z9HLv4C6R1GzNmhiGMkzweGscNKSf24adgwMKN6","6biFetzg82T2Q5ptWeco4FAXakMfkQKPxgC8mnAsvd8o"),
          ("ATNSDN7n7YYpQGyvziV9ZLdfd5LMfMutVNaisoZVpump","4oddvedGRrqRb7jyrXmpGVu3k6WofEUQNKhKt31RYR29","9CVSTSSaA9iuKoT4ZujNGdKSM9KSnjrGGMuh5RaJDEs")],
}
def acct(pk):
    v = (m.rpc("getAccountInfo", [pk, {"encoding": "base64"}], tries=6) or {}).get("value")
    return v
def decode_bc(raw):
    if raw[:8] != bc_disc: return {"disc_ok": False}
    out, pos = {"disc_ok": True}, 8
    for f in bc_fields:
        t = f["type"]
        if t not in m._FIXED: break
        sz = m._FIXED[t]; b = raw[pos:pos+sz]
        if len(b) < sz:          # older account layout: fields appended later are absent, not zero
            out["absent_from"] = f["name"]; break
        out[f["name"]] = (b[0] != 0) if t == "bool" else (m.b58encode(b) if t == "pubkey" else int.from_bytes(b, "little"))
        pos += sz
    return out
results = []; allok = True
for kind, rows in SAMPLES.items():
    for mint, curve, pool in rows:
        c = acct(curve); time.sleep(0.6); p = acct(pool); time.sleep(0.6)
        bc = decode_bc(base64.b64decode(c["data"][0])) if c else {"missing": True}
        pool_ok = bool(p) and p["owner"] == m.PUMPSWAP
        ok = bool(c) and c["owner"] == PUMP and bc.get("disc_ok") and bc.get("complete") is True and pool_ok
        allok &= ok
        results.append({"kind": kind, "mint": mint, "curve_owner_is_pump": bool(c) and c["owner"] == PUMP,
                        "curve_complete": bc.get("complete"), "real_token_reserves": bc.get("real_token_reserves"),
                        "pool_owner_is_pumpswap": pool_ok, "PASS": ok})
        print(f"{kind:4} {mint[:12]}…  curve owner pump={results[-1]['curve_owner_is_pump']} complete={bc.get('complete')} real_token_reserves={bc.get('real_token_reserves')}  pool owner PumpSwap={pool_ok}  -> {'PASS' if ok else 'FAIL'}")
print("\nALL PASS" if allok else "\nFAILURES PRESENT")
pathlib.Path(__file__).with_name("out").joinpath("chain_check_graduations.json").write_text(json.dumps(results, indent=1), encoding="utf-8")

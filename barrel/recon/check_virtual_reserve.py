"""MR-14 order 2: is the PumpSwap virtual quote reserve one constant on post-BOOST pools?

Reads `virtual_quote_reserves` from the Pool accounts listed in recon/out/run_vqr_pool_sample_*.json
(30 post-BOOST stratum-P pools, one every second day of the era) plus a pre-BOOST control set,
over RPC. No Dune credits. Writes recon/out/vqr_check.json and prints the verdict the order asks
for: one constant, or not.

Pool layout from the pinned IDL (recon/idl/pump_amm.json): 8-byte discriminator, bump u8,
index u16, six pubkeys, lp_supply u64, coin_creator pubkey, is_mayhem_mode bool,
is_cashback_coin bool, virtual_quote_reserves i128 at byte 245. An account shorter than 261
bytes predates the field: recorded as absent, never as zero.
"""
import base64
import glob
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as d

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "recon" / "out"
POOL_DISC = bytes([241, 154, 109, 4, 17, 177, 109, 188])
OFF = 8 + 1 + 2 + 32 * 6 + 8 + 32 + 1 + 1          # 245
PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"


def read_pool(addr: str) -> dict:
    r = d.rpc("getAccountInfo", [addr, {"encoding": "base64"}])
    v = (r or {}).get("value")
    if not v:
        return {"pool": addr, "status": "account not found"}
    raw = base64.b64decode(v["data"][0])
    rec = {"pool": addr, "owner_is_pumpswap": v.get("owner") == PUMPSWAP, "len": len(raw),
           "disc_ok": raw[:8] == POOL_DISC}
    if len(raw) < OFF + 16:
        rec["status"] = "field absent (short layout)"
        return rec
    rec["virtual_quote_reserves"] = int.from_bytes(raw[OFF:OFF + 16], "little", signed=True)
    rec["quote_vault"] = d.b58encode(raw[8 + 3 + 32 * 5:8 + 3 + 32 * 6])
    rec["status"] = "ok"
    return rec


def main() -> None:
    src = sorted(glob.glob(str(OUT / "run_vqr_pool_sample_*.json")))[-1]
    rows = json.load(open(src))["rows"]
    out = []
    for r in sorted(rows, key=lambda x: x["grad_day"]):
        rec = read_pool(r["pool"])
        rec.update(grad_day=r["grad_day"], era=r["era"])
        if rec.get("status") == "ok":   # real quote balance now, for context: how much of the price is virtual
            bal = d.rpc("getTokenAccountBalance", [rec.pop("quote_vault")])
            rec["real_quote_now"] = int(((bal or {}).get("value") or {}).get("amount") or 0)
        out.append(rec)
        time.sleep(0.15)
    post = [x for x in out if x["era"] == "post_boost"]
    vals = sorted({x.get("virtual_quote_reserves") for x in post if x.get("status") == "ok"})
    summary = {
        "source": pathlib.Path(src).name, "n_post": len(post),
        "n_post_read": sum(x.get("status") == "ok" for x in post),
        "post_distinct_values": len(vals), "post_min": vals[0] if vals else None, "post_max": vals[-1] if vals else None,
        "post_spread_lamports": (vals[-1] - vals[0]) if vals else None,
        "post_spread_relative": ((vals[-1] - vals[0]) / vals[-1]) if vals and vals[-1] else None,
        "pre_values": sorted({x.get("virtual_quote_reserves") for x in out if x["era"] == "pre_boost" and x.get("status") == "ok"}),
        "pre_absent": sum(1 for x in out if x["era"] == "pre_boost" and x.get("status") != "ok"),
    }
    (OUT / "vqr_check.json").write_text(json.dumps({"summary": summary, "pools": out}, indent=1), encoding="utf-8")
    print(json.dumps(summary, indent=1))
    for x in out:
        print(f"  {x['grad_day']} {x['era']:10s} {x['pool'][:8]}…  {x.get('status'):28s} vqr={x.get('virtual_quote_reserves')}  real_now={x.get('real_quote_now')}")


if __name__ == "__main__":
    main()

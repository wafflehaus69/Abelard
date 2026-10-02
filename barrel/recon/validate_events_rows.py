"""MR-15 B1: check an exported chunk of the event query, and re-count the price paths locally.

  python barrel/recon/validate_events_rows.py <exported rows .json>

1. Derived virtual reserve against the chain. 35 pools are taken from the rows by a seeded hash,
   their Pool accounts read over RPC (free), and the derived `vqr` compared with the account's
   `virtual_quote_reserves`. The standard is 35 of 35.
2. Null counts of the price columns, and how many pools got no reserve (no buy of 0.001 SOL).
3. Price paths after entry, counted here and not in Dune: every cut below (above entry, twice
   entry, the H5 winner definition) is a verdict and stays local.

Writes recon/out/events_validation_<name>.json (counts and pool addresses only; pools are public).
"""
import base64
import hashlib
import json
import pathlib
import statistics
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as d

ROOT = pathlib.Path(__file__).resolve().parents[1]
OFF = 245                      # virtual_quote_reserves i128 in the Pool account (check_virtual_reserve.py)
SEED = "20261001"
LAGS = (15, 60, 240)
H5_PEAK, REACH = 3.0, 2.0      # MR-13: winner = 24h peak >= 3x entry AND 7d price >= entry; 2x reach share


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p / 100 * len(v)))] if v else None


def chain_vqr(pool: str):
    r = d.rpc("getAccountInfo", [pool, {"encoding": "base64"}])
    v = (r or {}).get("value")
    if not v:
        return None, None
    raw = base64.b64decode(v["data"][0])
    if len(raw) < OFF + 16:
        return None, None
    return int.from_bytes(raw[OFF:OFF + 16], "little", signed=True), raw[OFF - 2] != 0


def main(path: str) -> None:
    rows = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    out = {"file": pathlib.Path(path).name, "tokens": len(rows)}

    # 1. derived reserve against pool accounts
    sample = sorted(rows, key=lambda r: hashlib.sha256(f"{SEED}|{r['pool']}".encode()).hexdigest())[:35]
    # every pool whose derived value is neither about 0 nor about 17.58 SOL is checked too, outside the 35
    odd = [r for r in rows if r.get("vqr") is not None and not abs(r["vqr"]) < 1e6 and not 17.5e9 < r["vqr"] < 17.7e9 and r not in sample]
    checks = []
    for r in sample:
        truth, mayhem = chain_vqr(r["pool"])
        derived = r.get("vqr")
        ok = truth is not None and derived is not None and abs(derived - truth) <= max(1000.0, 1e-6 * truth)
        checks.append({"pool": r["pool"], "account_vqr": truth, "mayhem": mayhem, "derived_vqr": derived,
                       "diff": None if truth is None or derived is None else derived - truth, "match": ok})
        time.sleep(0.12)
    odd_checks = []
    for r in odd:
        truth, mayhem = chain_vqr(r["pool"])
        odd_checks.append({"pool": r["pool"], "account_vqr": truth, "mayhem": mayhem, "derived_vqr": r["vqr"],
                           "diff": None if truth is None else r["vqr"] - truth,
                           "match": truth is not None and abs(r["vqr"] - truth) <= max(1000.0, 1e-6 * truth)})
    out["reserve_check_other_values"] = {"pools": len(odd_checks), "match": sum(c["match"] for c in odd_checks), "detail": odd_checks}
    out["reserve_check"] = {"pools": len(checks), "match": sum(c["match"] for c in checks),
                            "derived_null": sum(c["derived_vqr"] is None for c in checks),
                            "account_unreadable": sum(c["account_vqr"] is None for c in checks),
                            "max_abs_diff_lamports": max([abs(c["diff"]) for c in checks if c["diff"] is not None] or [None]),
                            "sample_mayhem": sum(bool(c["mayhem"]) for c in checks), "detail": checks}

    # 2. fill
    v = [r.get("vqr") for r in rows]
    out["reserve_fill"] = {"null": sum(x is None for x in v), "zero_or_near": sum(x is not None and abs(x) < 1e6 for x in v),
                           "about_17_58_sol": sum(x is not None and 17.5e9 < x < 17.7e9 for x in v),
                           "other": sum(x is not None and not abs(x) < 1e6 and not 17.5e9 < x < 17.7e9 for x in v)}
    cols = ["px_grad", "px_g15", "px_g60", "px_g240", "px_7d", "peak_24h_g240", "mdd_7d_g240", "cb_p50", "cs_p50", "creator", "token_program"]
    out["null_counts"] = {c: sum(r.get(c) is None for r in rows) for c in cols}
    out["cost_bps"] = {"buy_p50": pct([r["cb_p50"] for r in rows if r.get("cb_p50") is not None], 50),
                       "sell_p50": pct([r["cs_p50"] for r in rows if r.get("cs_p50") is not None], 50),
                       "buy_negative": sum(1 for r in rows if (r.get("cb_p50") or 0) < 0)}

    # 3. price paths, all cuts applied here
    paths = {}
    for g in LAGS:
        ok = [r for r in rows if r.get(f"px_g{g}") and r.get("px_7d") is not None]
        ret = [r["px_7d"] / r[f"px_g{g}"] for r in ok]
        pk7 = [r[f"peak_7d_g{g}"] / r[f"px_g{g}"] for r in ok if r.get(f"peak_7d_g{g}") is not None]
        pk24 = [r[f"peak_24h_g{g}"] / r[f"px_g{g}"] for r in ok if r.get(f"peak_24h_g{g}") is not None]
        paths[f"g{g}"] = {
            "tokens": len(ok),
            "ret7_p10_p25_p50_p75_p90": [round(pct(ret, p), 4) for p in (10, 25, 50, 75, 90)] if ret else None,
            "ret7_mean": round(statistics.mean(ret), 4) if ret else None,
            "share_above_entry": round(sum(x > 1 for x in ret) / len(ret), 4) if ret else None,
            "peak7_p50": round(pct(pk7, 50), 4) if pk7 else None,
            "share_reaching_2x": round(sum(x >= REACH for x in pk7) / len(pk7), 4) if pk7 else None,
            "peak24_p50": round(pct(pk24, 50), 4) if pk24 else None,
            "mdd7_p50": round(pct([r[f"mdd_7d_g{g}"] for r in ok if r.get(f"mdd_7d_g{g}") is not None], 50), 4),
            "entry_over_graduation_p50": round(pct([r[f"px_g{g}"] / r["px_grad"] for r in ok if r.get("px_grad")], 50), 4),
            "real_quote_reserve_sol_p50": round(pct([r[f"qres_g{g}"] / 1e9 for r in ok if r.get(f"qres_g{g}") is not None], 50), 3),
            "h5_winners": sum(1 for r in ok if r.get(f"peak_24h_g{g}") is not None
                              and r[f"peak_24h_g{g}"] >= H5_PEAK * r[f"px_g{g}"] and r["px_7d"] >= r[f"px_g{g}"]),
        }
    out["price_paths"] = paths
    by = {"reserve about 0 (mayhem pools post-BOOST; every pool pre-BOOST)": [r for r in rows if r.get("vqr") is not None and abs(r["vqr"]) < 1e6],
          "with virtual reserve": [r for r in rows if r.get("vqr") is not None and r["vqr"] > 1e9]}
    out["g240_by_pool_kind"] = {k: {"tokens": len(vv),
                                    **(lambda x: {"ret7_p50": round(pct(x, 50), 4) if x else None, "ret7_mean": round(statistics.mean(x), 4) if x else None})(
                                        [r["px_7d"] / r["px_g240"] for r in vv if r.get("px_g240") and r.get("px_7d") is not None])}
                                for k, vv in by.items()}
    name = pathlib.Path(path).stem
    (ROOT / "recon" / "out" / f"events_validation_{name}.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    brief = {k: v for k, v in out.items() if k != "reserve_check"}
    brief["reserve_check"] = {k: v for k, v in out["reserve_check"].items() if k != "detail"}
    print(json.dumps(brief, indent=1))


if __name__ == "__main__":
    main(sys.argv[1])

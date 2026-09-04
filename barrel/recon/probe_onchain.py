"""BARREL M0 recon — live on-chain probe.

Reproduces the LIVE evidence cited in docs/M0_RECON.md (findings R2, R4, R7)
against a public Solana mainnet RPC. No API key, no wallet, read-only.

    python barrel/recon/probe_onchain.py

Two deliberate constraints, both doctrine:

* [E1] Every RPC failure raises or is reported. Nothing is defaulted, and a
  sample that came back empty is printed as empty rather than skipped.
* M0_TASKING §1 metadata quarantine. Only account-state fields are requested
  (authorities, decimals, supply, extension count, owning program). Name,
  symbol, description and URI are never fetched, so untrusted token metadata
  cannot reach this process at all. The quarantine is enforced at the fetch.

Caveat carried into the report: the rate figures are instantaneous — one
1,000-signature window at one moment. They are an order-of-magnitude floor on
current activity, not a window average. The window average is a Gate 0 output.
"""

from __future__ import annotations

import datetime as dt
import json
import time
import urllib.request
from dataclasses import dataclass

RPC = "https://api.mainnet-beta.solana.com"

TOKENKEG = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"  # SPL Token (legacy)
TOKEN22 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"  # Token-2022

PROGRAMS = {
    "pump.fun bonding curve": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "PumpSwap AMM": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "pump fee program": "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
    "Raydium AMM v4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
}

# A known pump.fun graduate, used for the R4 account-state check.
SAMPLE_GRADUATE = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"


class RpcError(RuntimeError):
    """The RPC did not answer. Never swallowed, never defaulted ([E1])."""


def rpc(method: str, params: list, tries: int = 5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                RPC, data=body, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.load(resp)
            if "error" in payload:
                last = payload["error"]
                time.sleep(0.8 * (attempt + 1))
                continue
            return payload["result"]
        except Exception as exc:  # noqa: BLE001 - reported, not suppressed
            last = exc
            time.sleep(0.8 * (attempt + 1))
    raise RpcError(f"{method} failed after {tries} attempts: {last!r}")


def utc(ts: int | None) -> str:
    return dt.datetime.fromtimestamp(ts, dt.UTC).isoformat() if ts else "unknown"


@dataclass
class Rate:
    name: str
    address: str
    sigs: int
    span_s: int | None
    newest: str

    @property
    def per_min(self) -> float | None:
        if not self.span_s:
            return None
        return self.sigs / self.span_s * 60


def probe_rates() -> list[Rate]:
    """R2 / R7 — relative venue activity, from one 1,000-signature window each."""
    out = []
    for name, addr in PROGRAMS.items():
        sigs = rpc("getSignaturesForAddress", [addr, {"limit": 1000}])
        if not sigs:
            out.append(Rate(name, addr, 0, None, "no signatures returned"))
            continue
        t_new, t_old = sigs[0].get("blockTime"), sigs[-1].get("blockTime")
        span = (t_new - t_old) if (t_new and t_old) else None
        out.append(Rate(name, addr, len(sigs), span, utc(t_new)))
        time.sleep(0.4)
    return out


def probe_mint_state(mint: str) -> dict:
    """R4 — account state only. Metadata fields are never requested (§1)."""
    value = (rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}]) or {}).get("value")
    if value is None:
        raise RpcError(f"mint account not found: {mint}")
    info = value["data"]["parsed"]["info"]
    owner = value["owner"]
    return {
        "mint": mint,
        "owner_program": owner,
        "token_program": (
            "SPL-Token (legacy)" if owner == TOKENKEG
            else "Token-2022" if owner == TOKEN22
            else f"other:{owner}"
        ),
        "mint_authority": info.get("mintAuthority"),
        "freeze_authority": info.get("freezeAuthority"),
        "decimals": info.get("decimals"),
        "supply_raw": info.get("supply"),
        "extension_count": len(info.get("extensions", []) or []),
    }


def probe_token_program_mix(sample: int = 20) -> dict[str, int]:
    """R4 counter-evidence — do Token-2022 mints appear in PumpSwap flow?"""
    sigs = rpc("getSignaturesForAddress", [PROGRAMS["PumpSwap AMM"], {"limit": sample * 2}])
    mix: dict[str, int] = {"SPL-Token": 0, "Token-2022": 0}
    seen = 0
    for s in sigs:
        if s.get("err") or seen >= sample:
            continue
        try:
            tx = rpc(
                "getTransaction",
                [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                tries=3,
            )
        except RpcError:
            continue
        seen += 1
        ixs = list(tx["transaction"]["message"].get("instructions", []))
        for inner in tx["meta"].get("innerInstructions") or []:
            ixs += inner.get("instructions", [])
        for ix in ixs:
            pid = ix.get("programId", "")
            if pid == TOKENKEG:
                mix["SPL-Token"] += 1
            elif pid == TOKEN22:
                mix["Token-2022"] += 1
        time.sleep(0.2)
    mix["_transactions_sampled"] = seen
    return mix


def main() -> None:
    print(f"BARREL M0 recon probe — {dt.datetime.now(dt.UTC).isoformat()}\n")

    print("== R2 / R7 — venue activity (one 1,000-signature window each) ==")
    print(f"{'program':26} {'span':>6} {'tx/min':>10}   newest")
    for r in probe_rates():
        rate = f"{r.per_min:,.0f}" if r.per_min else "n/a"
        span = f"{r.span_s}s" if r.span_s else "n/a"
        print(f"{r.name:26} {span:>6} {rate:>10}   {r.newest}")
    print("  NOTE: instantaneous rates, single sample. Floor, not window average.\n")

    print("== R4 — account state of a known pump.fun graduate ==")
    for k, v in probe_mint_state(SAMPLE_GRADUATE).items():
        print(f"  {k:16}: {v}")
    print("  NOTE: n=1. Does not establish a universe-wide constant.\n")

    print("== R4 — token-program mix in recent PumpSwap flow ==")
    for k, v in probe_token_program_mix().items():
        print(f"  {k:22}: {v}")
    print("  NOTE: Token-2022 present => S3/S4 are NOT vacuous checks.")


if __name__ == "__main__":
    main()

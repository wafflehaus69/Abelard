"""Owner-wallet exclusion (readiness 1.2 / H3 spec §0). Reads barrel/private/owner_wallets.txt
(gitignored; one base58 address per line, '#' comments). Never logs or prints an address.
Provides a SQL fragment for exclusion and a checker for exported rows."""
import pathlib, re
PRIV = pathlib.Path(__file__).resolve().parents[1] / "private" / "owner_wallets.txt"
_B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
def owner_wallets() -> list[str]:
    if not PRIV.exists():
        raise FileNotFoundError("barrel/private/owner_wallets.txt missing; owner exclusion cannot be applied")
    ws = [l.strip() for l in PRIV.read_text(encoding="utf-8").splitlines() if l.strip() and not l.strip().startswith("#")]
    bad = [w for w in ws if not _B58.match(w)]
    if bad:
        raise ValueError(f"{len(bad)} line(s) in owner_wallets.txt are not base58 addresses")
    return ws
def sql_not_owner(col: str) -> str:
    """Predicate excluding owner wallets. The addresses are inlined into the query text sent to
    Dune (unavoidable) but never into files under version control or into logs."""
    return f"{col} NOT IN ({', '.join(repr(w) for w in owner_wallets())})"
_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def hex_key(addr: str) -> str:
    """A base58 address as the 64 upper-case hex characters of its 32 bytes: how it appears inside a raw
    instruction payload that a query returns with to_hex()."""
    n = 0
    for ch in addr:
        n = n * 58 + _ALPHABET.index(ch)
    raw = n.to_bytes(32, "big")     # raises if the address is longer than 32 bytes: not a key
    return raw.hex().upper()
def hex_keys() -> list[str]:
    return [hex_key(w) for w in owner_wallets()]
def sql_not_owner_hex(col: str) -> str:
    """True when no owner key occurs anywhere in the hex varchar `col` (a payload from to_hex(), which is
    upper case). Layout-independent: the key is looked for at any offset. Same handling as sql_not_owner:
    the keys reach the query text sent to Dune and nothing under version control."""
    keys = hex_keys()
    return "(" + " AND ".join(f"strpos({col}, '{k}') = 0" for k in keys) + ")" if keys else "TRUE"
def assert_no_owner_rows(rows, *cols) -> None:
    ws = set(owner_wallets())
    hits = sum(1 for r in rows or [] for c in cols if r.get(c) in ws)
    if hits:
        raise AssertionError(f"{hits} exported row(s) reference an owner wallet; exclusion failed")
if __name__ == "__main__":
    print(f"owner wallets loaded: {len(owner_wallets())} (addresses not shown)")

"""Credential loader. Reads barrel/private/dune.env (gitignored). Never prints a value."""
import os, pathlib
PRIV = pathlib.Path(__file__).resolve().parents[1] / "private" / "dune.env"
def _read(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v and PRIV.exists():
        for line in PRIV.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith(name + "="):
                v = line.split("=", 1)[1].strip().strip('"').strip("'")
    return v
def dune_key() -> str:
    v = _read("DUNE_API_KEY")
    if not v: raise SystemExit("DUNE_API_KEY not set (barrel/private/dune.env). Nothing was run.")
    return v
def helius_rpc_url() -> str | None:
    v = _read("HELIUS_API_KEY")
    return f"https://mainnet.helius-rpc.com/?api-key={v}" if v else None

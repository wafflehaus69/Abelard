"""Key-leak guard. Refuses if any tracked or staged file contains a credential.
Run before every commit: python barrel/recon/leak_guard.py  (exit 1 = leak).
Patterns: the live Dune key (read from barrel/private/dune.env, never printed),
any 32-char base62 token next to 'X-Dune-Api-Key' or 'DUNE_API_KEY=', and any
GCP/Google credential markers. Extend PATTERNS when a new secret class appears."""
import pathlib, re, subprocess, sys
ROOT = pathlib.Path(__file__).resolve().parents[2]
PRIV = ROOT / "barrel" / "private" / "dune.env"
live_values = []
if PRIV.exists():
    for line in PRIV.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            v = line.split("=", 1)[1].strip()
            if len(v) >= 16: live_values.append(v)
PATTERNS = [
    re.compile(r"X-Dune-Api-Key:\s*[A-Za-z0-9]{20,}"),
    re.compile(r"DUNE_API_KEY=\s*[A-Za-z0-9]{20,}"),
    re.compile(r'"private_key":\s*"-----BEGIN'),
    re.compile(r"AIza[0-9A-Za-z\-_]{35}"),           # Google API key shape
    re.compile(r"api-key=[0-9a-fA-F\-]{20,}"),         # Helius-style key in a URL
    re.compile(r"HELIUS_API_KEY=\s*[0-9a-fA-F\-]{20,}"),
]
files = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "barrel", "doctrine"],
                       capture_output=True, text=True, cwd=ROOT).stdout.split()
bad = []
for f in files:
    p = ROOT / f
    if not p.is_file() or p.suffix in {".png", ".jpg"}:
        continue
    try:
        txt = p.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        continue
    for lv in live_values:
        if lv in txt:
            bad.append((f, "live credential value from private/dune.env"))
    for pat in PATTERNS:
        if pat.search(txt):
            bad.append((f, pat.pattern[:30]))
if bad:
    for f, why in bad:
        print(f"LEAK  {f}  ({why})")
    sys.exit(1)
print(f"guard passed: {len(files)} files, no credential found")

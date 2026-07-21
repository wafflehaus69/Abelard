"""Patch twitter-cli's ClientTransaction init so the x-client-transaction-id lane works again.

twitter-cli 0.8.5 fetches bare https://x.com for the transaction-id step, but X now serves a
migration interstitial there with no `ondemand.s` reference -> the extraction regex returns None
-> HTTP 404 on EVERY search. The xclienttransaction library ships handle_x_migration() (fetches
/home and follows the interstitial to the real page) but twitter-cli wasn't calling it.

This is a local venv patch (reverts on `pipx reinstall/upgrade twitter-cli` — just re-run this).
Portable: locates client.py across pipx layouts + Python versions. Idempotent: skips if patched.
Self-verifying: each anchor must match exactly once or it aborts with no change (backs up first).
"""
import glob
import os
import shutil
import sys

cands = []
for root in ("~/.local/share/pipx/venvs/twitter-cli", "~/.local/pipx/venvs/twitter-cli"):
    cands += glob.glob(os.path.join(os.path.expanduser(root), "lib", "*", "site-packages", "twitter_cli", "client.py"))
if not cands:
    print("ERROR: twitter_cli/client.py not found under pipx venvs")
    sys.exit(2)
F = cands[0]
src = open(F, encoding="utf-8").read()

if "handle_x_migration(cffi_session)" in src or "PATCH(ct-migration)" in src:
    print(f"ALREADY PATCHED: {F}")
    sys.exit(0)

repls = [
    (
        "from x_client_transaction.utils import generate_headers as _gen_ct_headers, get_ondemand_file_url\n",
        "from x_client_transaction.utils import generate_headers as _gen_ct_headers, get_ondemand_file_url, handle_x_migration\n",
    ),
    (
        '            home_page = cffi_session.get(\n'
        '                "https://x.com", headers=ct_headers, timeout=10,\n'
        '            )\n'
        '            home_page_response = bs4.BeautifulSoup(home_page.content, "html.parser")\n',
        '            # PATCH(ct-migration): X serves a migration interstitial at bare https://x.com\n'
        '            # that lacks the ondemand.s ref; handle_x_migration fetches /home + follows it.\n'
        '            cffi_session.headers.update(ct_headers)\n'
        '            home_page_response = handle_x_migration(cffi_session)\n'
        '            home_page_html = str(home_page_response)\n',
    ),
    (
        "            _update_features_from_html(home_page.text)\n",
        "            _update_features_from_html(home_page_html)\n",
    ),
    (
        "            self._save_ct_cache(home_page.text, ondemand_file.text)\n",
        "            self._save_ct_cache(home_page_html, ondemand_file.text)\n",
    ),
]

for i, (old, new) in enumerate(repls, 1):
    n = src.count(old)
    if n != 1:
        print(f"ABORT at edit {i}: expected exactly 1 match, found {n}. Anchor:\n  {old[:70]!r}")
        sys.exit(1)

shutil.copy(F, F + ".bak_ctfix")
for old, new in repls:
    src = src.replace(old, new)
open(F, "w", encoding="utf-8").write(src)
print(f"PATCHED OK ({len(repls)} edits): {F}")
print(f"backup: {F}.bak_ctfix")

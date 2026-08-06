# Twitter/X lane — failure runbook (patch fix vs cookie fix)

**Owner:** ChatterDaemon (the patch + script live here), but the lane is **shared** — BizDaemon's planned Twitter enrichment uses the same CLI, same install, same account. Fixing it here fixes it everywhere on that host.
**Verified live:** 2026-07-27 on Orban (WSL, py3.12) and Basilic (macOS, py3.14).

---

## 0. There are TWO different failures. Diagnose before you fix.

They look similar from the daemon's side (Twitter lane returns nothing, source `ok=False`, scan `degraded`), but the causes and fixes are unrelated. **Do not refresh cookies for a 404 — it will not help.**

| Symptom | Cause | Fix |
|---|---|---|
| `search $SYM exited 1` on **every** ticker; CLI log shows `Failed to init ClientTransaction: 'NoneType' object has no attribute 'group'`; underlying HTTP **404** | **X migration interstitial** — twitter-cli 0.8.5 fetches bare `https://x.com`, which no longer carries the `ondemand.s` reference the transaction-id generator scrapes | **§1 — the patch.** Re-run `scripts/patch_twitter_cli.py` |
| **401 / 403**, "not authenticated", `twitter status` reports unauthenticated | **Session cookies expired/revoked** | **§2 — the cookie refresh** |
| Fails only on the **last** N tickers of a run, first ones fine; often `exit 1` mid-scan | **Rate limit** — X meters ~25 searches / rolling window / account | Not a bug. Lower the cap / wait out the window. §4 |
| `subprocess TimeoutExpired` on a few tickers | Slow response, not auth | Transient. Raise timeout or ignore. |

**First command to run, always:**
```bash
/home/wafflehouse/.local/bin/twitter status     # authenticated? -> distinguishes cookie vs patch
```
`twitter whoami` confirms *which* account is authenticated.

---

## 1. The PATCH fix (404 / ClientTransaction) — the one that bit us

### What's actually broken
twitter-cli 0.8.5 fetches **bare `https://x.com`** for its `x-client-transaction-id` step. X now serves a **migration interstitial** at that URL with no `ondemand.s` reference → `get_ondemand_file_url()`'s regex returns `None` → `.group(1)` raises → no transaction id → **HTTP 404 on every search**.

The `xclienttransaction` library already ships the correct helper — **`handle_x_migration()`** (fetches `/home` and follows the interstitial to the real page). twitter-cli simply wasn't calling it. That's the whole bug.

**Not fixable by upgrading:** twitter-cli **0.8.5** and xclienttransaction **1.0.3** are both the *latest* on PyPI, and upstream (`iSarabjitDhiman/XClientTransaction`, issue #38 "broken again") has no fix. So we patch locally.

### The fix
```bash
python3 daemons/chatter_daemon/scripts/patch_twitter_cli.py
```
- **Idempotent** — prints `ALREADY PATCHED` and exits 0 if applied.
- **Portable** — auto-locates `twitter_cli/client.py` across pipx layouts (`~/.local/share/pipx` on Orban, `~/.local/pipx` on Basilic) and Python versions.
- **Self-verifying** — each of its 4 anchors must match exactly once, or it aborts with **no change**.
- **Backs up** to `client.py.bak_ctfix`.

**It is a local venv patch → it REVERTS on any `pipx reinstall/upgrade twitter-cli`. Re-run it after.**
ChatterDaemon's `scripts/run.sh` now calls it at the top of **every run**, so the lane self-heals. Any other daemon using Twitter should do the same.

### Verify
```bash
/home/wafflehouse/.local/bin/twitter search '$NVDA' -t latest --since 2026-07-26 \
  --exclude links --min-likes 2 -n 2 --json | head -5
# want: {"ok": true, "data": [ ... ]}   and exit 0
```

### Gotcha — the 1-hour transaction cache can mask your test
twitter-cli caches the client-transaction data at **`~/.twitter-cli/transaction_cache.json` with a 1 h TTL**. A warm cache can make a broken patch *look* fine (or vice-versa). To genuinely exercise the patched code path:
```bash
rm -f ~/.twitter-cli/transaction_cache.json    # force a fresh CT init
```
then run the search above.

---

## 2. The COOKIE fix (401 / 403)

### How auth works — it is AMBIENT
twitter-cli reads the session cookies **from the environment itself**:

| Variable | What |
|---|---|
| `TWITTER_AUTH_TOKEN` | X session auth token |
| `TWITTER_CT0` | X CSRF token |

> ⚠️ **Naming correction:** `daemons/chatter_daemon/BASILIC_MANUAL.md` §4 says `TWITTER_CT`. That is a **typo** — the real variable is **`TWITTER_CT0`** (verified: `TWITTER_CT` is unset on Orban and the lane works). Fix the manual when convenient.

The daemon **never reads, handles, or logs these values** — it just inherits them into the child process (`env=None` on the subprocess call), and it never logs argv either. Keep it that way: the safest posture is that the cookies exist **only** in the `.env`, never in daemon code or config objects.

### When they need refreshing
Cookies expire (weeks, not days) or get revoked when you log out of X in the browser they came from. Symptom is **401/403**, or `twitter status` reporting unauthenticated — **not** the 404 in §1.

### The refresh procedure
1. In a browser **logged into the X account the daemon uses** (`twitter whoami` tells you which), open DevTools → **Application → Cookies → `https://x.com`**.
2. Copy the values of the `auth_token` and `ct0` cookies.
3. Put them in the daemon's `.env` (gitignored) as `TWITTER_AUTH_TOKEN=` / `TWITTER_CT0=`.
   - **Orban:** `~/Code/Abelard/daemons/chatter_daemon/.env`
   - **Basilic:** `~/Code/Abelard/daemons/chatter_daemon/.env` — hand-carry with `scp`, then `chmod 600`. (Same pattern used for the news_watch `.env`.)
4. Verify: `twitter status` → authenticated; `twitter whoami` → the expected account.
5. No restart needed — each scan spawns a fresh subprocess that reads the current env.

### Security rules (non-negotiable)
- **Never paste cookies into a chat, an issue, a commit message, or a log.** (Precedent: cookies were pasted into an assistant chat on 2026‑07‑21 and deliberately **not** persisted — treat any cookie that touches a chat window as compromised and rotate it by logging out/in.)
- **Never commit `.env`** — it's gitignored; keep it that way. `chmod 600`.
- Don't echo them in shell one-liners (they land in shell history). Edit the file directly.
- If a cookie leaks: log out of X in that browser session (invalidates the token), log back in, re-extract.

---

## 3. Host matrix

| | Orban (dev) | Basilic (prod) |
|---|---|---|
| Platform | WSL Ubuntu, Python 3.12 | macOS arm64, Python 3.14 |
| CLI binary | `/home/wafflehouse/.local/bin/twitter` | `/Users/wafflehaus/.local/bin/twitter` |
| pipx venv | `~/.local/share/pipx/venvs/twitter-cli` | `~/.local/pipx/venvs/twitter-cli` |
| Patched | ✅ 2026‑07‑21 (+ self-heals each run via `run.sh`) | ✅ 2026‑07‑21 |
| `.env` | `daemons/chatter_daemon/.env` | same path, `chmod 600`, hand-carried |

**Basilic note:** `which twitter` over a bare ssh returns *not found* — **that is normal**. The pipx bin dir isn't on the non-interactive PATH; the daemon invokes the binary by absolute path via `CHATTER_TWITTER_BINARY`. Don't "fix" the PATH; check the absolute path instead.

---

## 4. Rate limits (not a bug — don't fix it as one)

X meters **~25 authenticated searches per rolling window per account**, and the CLI is paced ~5 s between searches. Exceeding it looks like `exit 1` on the tail of a run.

- ChatterDaemon handles this with **priority-first + top-N cap** (`CHATTER_TWITTER_PRIORITY`, `CHATTER_TWITTER_MAX_TICKERS=25`): named tickers are searched first and always land; the rest are dropped and logged.
- **OR-batching does NOT work** — live-certified 2026‑07: `$A OR $B` returns a small, low-engagement shared page (~1 usable tweet/name vs 15–34 solo). Rejected. Don't retry it.
- **Shared quota:** every daemon on the host uses the same account. If BizDaemon adds Twitter enrichment, biz + chatter draw from **one** ~25 window — coordinate their caps or stagger schedules, or they'll collectively 404.

---

## 5. Quick triage flow

```
Twitter lane returned nothing
        │
        ├─ `twitter status` → NOT authenticated? ──────────► §2 cookie refresh
        │
        ├─ log shows "Failed to init ClientTransaction" / 404 on every ticker?
        │        └─ rm ~/.twitter-cli/transaction_cache.json
        │           python3 scripts/patch_twitter_cli.py   ──► §1 patch
        │
        ├─ first N tickers fine, later ones exit 1? ───────► §4 rate limit (lower cap / wait)
        │
        └─ scattered TimeoutExpired? ─────────────────────► transient; raise timeout
```

**Known-good reference:** a healthy full scan shows `twitter CLI ok: twitter, version 0.8.5` and ~22–23 of 24 tickers carrying tweets + a crowd summary. Zero tickers with tweets = one of the failures above.

---

## 6. If the patch itself stops working

The patcher aborts (no change) when its anchors don't match — that means twitter-cli's source changed, or X changed again.

1. Reproduce the extraction directly to see whether X's page or the tool is at fault:
   ```python
   from curl_cffi import requests
   from x_client_transaction.utils import generate_headers, handle_x_migration, get_ondemand_file_url
   s = requests.Session(impersonate="chrome"); s.headers.update(generate_headers())
   home = handle_x_migration(s)
   print(len(str(home)), get_ondemand_file_url(home))   # big page + a real URL = library is FINE
   ```
   (Run it with the pipx venv's python.) A healthy result here + a broken CLI = the bug is in twitter-cli's call path, same class as §1.
2. Check upstream for a real fix: PyPI `twitter-cli` / `xclienttransaction`, and `github.com/iSarabjitDhiman/XClientTransaction` issues.
3. Update `scripts/patch_twitter_cli.py`'s anchors, re-run, re-verify with the §1 search.

Full history of the original diagnosis is in the memory note `project_chatter_twitter_cli_fix`.

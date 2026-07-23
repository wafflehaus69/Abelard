# VESTIGIAL_INVENTORY — smart_money_daemon SM-D1 Phase B

Read-only inventory produced by a verified multi-agent sweep (4 dimension
finders + adversarial per-candidate removal-safety verification). Recommend,
do not execute. Phase C actions are gated on Mando's explicit approval.

## 1. VERIFIED REMOVABLE (adversarially confirmed unreferenced)

| Candidate | Evidence | Regenerable |
|---|---|---|
| `scorecard.midpoint(low, high)` (scorecard.py:68-69) | Dead function. Only occurrence of `midpoint(` in the whole tree is its own definition. Weight math moved to the clustering-summed `r.mid` (scorecard.py:232,281); the distinct `clustering._midpoint` does the real band math. No import/call/CLI/test/dynamic reference. | yes (code) |
| `analysis/.~lock.POLITICIAN_SCORECARD.csv#` | LibreOffice Calc lock file, untracked, NOT matched by any .gitignore rule (accidentally committable). Zero code references. | n/a |

Note on the lock file: it exists because POLITICIAN_SCORECARD.csv is currently
open in LibreOffice (user "Orban"). Deleting it is cosmetic and the app may
recreate it — close the spreadsheet first, and the open handle can block
scorecard regeneration meanwhile. Fix: delete + add `.~lock.*#` to .gitignore.

## 2. PARTIAL — one-off recon scripts (split verdict, needs a paired edit)

| Candidate | Verdict | Detail |
|---|---|---|
| `recon/extract_harvest.py` | removable alone | Self-described "One-off" script; zero references anywhere; its OUTPUT (senate_ptr_index_20260720.json) is consumed by collect_senate.sh via `python -m efd_ingest`, not by this script. |
| `recon/g2_parse_13f.py` | NOT removable in isolation | Has a live path reference in `recon/SOURCE_VERDICTS.md:37` ("parser: `recon/g2_parse_13f.py`"). SOURCE_VERDICTS.md is load-bearing (cited by efd_session, efd_ingest, data_quality, scan). Removing the script requires editing that doc line. |

Recommendation: if removing, delete extract_harvest.py cleanly; for
g2_parse_13f.py, either keep (it documents the G2 recon method) or remove AND
edit SOURCE_VERDICTS.md:37 in the same commit. Prefer KEEP — they are the
recon provenance for a verdicts doc that is still cited.

## 3. KEEP — verified still-referenced (do NOT remove)

- `analysis/archive/POLITICIAN_SCORECARD_sm1.md` — intentional pre-SM-2
  baseline; `.gitignore` tracks `archive/*.md` by hand; scorecard.py:423
  re-emits a pointer to it on every run. "_sm1" = SM-1 milestone, NOT the dead
  Senate Stock Watcher source.
- No Senate Stock Watcher remnant exists. The only matches are the recon
  verdict record in SOURCE_VERDICTS.md documenting its DEATH. Clean.
- No dead modules: all 22 non-`__init__` modules are imported or are CLI
  entrypoints. (`watermarks.get` is production-unread — only tests call it;
  the scan advances watermarks but never reads them to gate work. Flagged as a
  design note, NOT a removal — confirm whether incremental gating was intended.)

## 4. UNUSED DEPENDENCIES — none

All four pyproject deps resolve to live call-sites: requests (6 modules),
pandas (clustering/data_quality/scorecard/commonality), pdfplumber
(house_ingest only, but genuinely invoked), pyyaml (overlay). No removals.

## 5. abelard_common CONVERGENCE DEBTS — INVENTORY ONLY (do NOT hoist here)

Per the order, catalogued for a future hoist order; not touched now.

| Duplicate | Where | Converges to |
|---|---|---|
| HTTP client re-implementation x5-6 | prices.py, form4.py, thirteenf.py, form4_backfill.py, house_ingest.py, efd_session.py (partial) | abelard_common.http_client.HttpClient |
| Rate-pacing primitive | `_pace/_last_call` copied in prices/efd_ingest/house_ingest + inline sleep in form4/thirteenf/form4_backfill (6 PACE constants) | fourchan_fetch.Fetcher._throttle style min-interval |
| **Forced-UTF-8 decode obligation OMITTED** | every `.text`/`.json()` in prices/efd_session/form4/thirteenf/form4_backfill; efd_ingest PTR-HTML cache read/write without encoding=utf-8 | http_client/fourchan_fetch forced-utf-8 contract |
| Per-module error taxonomies | PriceError/EfdSessionError/IngestError/bare RuntimeError | abelard_common.errors.DaemonError + HttpClient exceptions |
| Duplicated EDGAR constants | UA_TMPL/ARCH byte-identical in form4.py & thirteenf.py | shared EDGAR source-adapter |
| .env parser x2 | db.py::_load_env_var vs efd_ingest.py::load_env (already de-facto shared) | collapse to one; no common-lib equivalent yet |
| pdfplumber extraction | house_ingest only | NO abelard_common PDF module exists — future hoist seed, not a current debt |

**Flagged latent-bug note (not just style):** the omitted forced-UTF-8 decode
is a real mojibake risk on Windows cp1252 for non-ASCII senator/issuer names
(the abelard_common comment calls out that cp1252 eats ticker `\b` boundaries).
Worth prioritizing when the hoist order runs.

## 6. REGENERABLE CACHES + LEFTOVERS (Phase A cross-ref; sizes measured)

| Item | Size | Location | Regenerable | Tracked? |
|---|---|---|---|---|
| data/raw/house (zips+PDFs) | 676 M | C: | yes | gitignored |
| data/raw/house_unparsed | 212 M | C: | yes | gitignored |
| `~/.openclaw/smart_money/_deploy_snapshot.db` | 240 M | WSL ext4 | yes (VACUUM copy) | n/a — **leftover, should have been deleted post-deploy** |
| data/raw/efd | 38 M | C: | yes | gitignored |
| `data/cache/_sync_snapshot.db` | 13 M | C: | yes | gitignored — **leftover** |
| analysis/*.log, logs_basilic/ | small | C: | yes | gitignored |
| data/raw/recon, price_errors | 4.5 M | C: | yes | gitignored |

## 7. POLICY GAP (low severity)

`analysis/COMMONALITY_COUNTERS.md` is git-tracked and regenerable but is NOT in
`.gitignore`'s "# Tracked by hand" comment (which names only SCORECARD.md,
DATA_QUALITY.md, registry.json, archive/*.md). Reconcile: add it to the comment
(if versioning is intended) or stop tracking it.

---

## PHASE C — PROPOSED RECLAMATION (each gated on Mando approval)

- **C1a** Delete leftover snapshots: `_deploy_snapshot.db` (240M) + `_sync_snapshot.db` (13M) = **253M**, zero risk (regenerable VACUUM copies).
- **C1b** Purge regenerable raw caches: data/raw/house (676M) + house_unparsed (212M) + efd (38M) = **~926M on C:**. Regenerable from EDGAR/House Clerk/browser-harvest. NOTE: house_unparsed is the raw material for a possible 2nd House parser — see RETENTION_POLICY; recommend KEEP house_unparsed until that order is built/declined.
- **C1c** Delete `.~lock` file + add `.~lock.*#` to .gitignore (close LibreOffice first).
- **C2** VACUUM the DB — **near-useless** (0.2M slack); skip unless C1 changes anything (it won't; DB is separate).
- **C3** One commit `chore remove vestigial artifacts and unused raw caches`: remove `scorecard.midpoint`, optionally extract_harvest.py, the .gitignore `.~lock` rule, and the COMMONALITY_COUNTERS.md policy reconciliation.
- **C4** VHDX compaction: gap is only ~1.9G. If Mando still wants it, the command (run from Windows PowerShell as admin, WSL shut down first) is:
  `wsl --shutdown; Optimize-VHD -Path "C:\Users\mdiba\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc\LocalState\ext4.vhdx" -Mode Full`
  (Optimize-VHD needs Hyper-V module; else `diskpart` → `select vdisk file=...` → `compact vdisk`). Do NOT run from inside WSL. Reclaims ~1.9G at most — low value.

Nothing above is executed. Awaiting per-item approval.

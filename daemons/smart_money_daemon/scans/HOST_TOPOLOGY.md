# HOST_TOPOLOGY — smart_money_daemon SM-D1 Phase A

Recon date 2026-07-22, from Orban. Numbers are measured (du/df/sqlite/ssh),
not estimated. Note: this repo `scans/` dir holds recon reports; it is distinct
from the runtime scan-envelope dir at `~/.openclaw/smart_money/scans/`.

## A1 — Orban footprint (measured)

### Disk, the headline
- **WSL ext4 root: 14G used / 1007G filesystem** (2%). Plenty of headroom
  inside WSL.
- **ext4.vhdx allocated: 15.9 GB** at
  `C:\Users\mdiba\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc\LocalState\ext4.vhdx`.
- **VHDX gap = ~1.9 GB** (15.9 allocated − 14 used). This is SMALL.
  **Finding: the VHDX is NOT the disk problem.** Per the order's own guidance,
  a WSL-to-elsewhere migration is not justified by vhdx slack — there is barely
  any. Whatever disk pressure exists is real data on Windows C:, not vhdx bloat.

### Daemon dir (on Windows C:, `Code\Abelard\...`, NOT OneDrive-synced): 1.2 GB
| Path | Size | Note |
|---|---|---|
| data/raw/house | 676 M | House PTR zips + extracted PDFs |
| data/raw/house_unparsed | 212 M | unparsed-layout PDFs (counted, kept) |
| .venv | 216 M | rebuildable via pip |
| data/raw/efd | 38 M | eFD raw HTML/index |
| data/raw/recon | 4.2 M | SM-0 recon captures |
| data/raw/price_errors | 320 K | fail-loud dumps |
| data/cache/_sync_snapshot.db | 13 M | **LEFTOVER** sync snapshot |

### State home `~/.openclaw/smart_money/` (WSL ext4): 491 M
| Path | Size | Note |
|---|---|---|
| smart_money_v0.db | 251 M | canonical DB — KEEP |
| _deploy_snapshot.db | 240 M | **LEFTOVER** deploy VACUUM snapshot — never cleaned on Orban |
| scans/ | 32 K | runtime envelopes |
| logs/ | 4 K | |

### DB internals
- 64064 pages x 4096 = 262 MB; **freelist = 49 pages = 0.2 MB slack.**
  **VACUUM would reclaim ~nothing** (C2 is a near-no-op on this DB).

### Other daemon venvs (context): biz 54M, chatter 226M, common 29M,
news_watch 109M, smart_money 216M.

### Host health note
WSL intermittently returns `HCS/0x800705aa insufficient system resources`
and a `networkingMode Nat catastrophic failure` fallback. Recovered each time
via `wsl --shutdown` + restart. Flagging as a stability observation.

### Reclamation opportunity (all regenerable, detail in VESTIGIAL_INVENTORY)
- Windows C: ~926 M of regenerable raw caches (house 676M + house_unparsed 212M
  + efd 38M) + 13 M leftover sync snapshot.
- WSL ext4: 240 M leftover deploy snapshot (delete frees inside-WSL; compaction
  returns it to Windows).
- DB VACUUM: negligible.

## A2 — Basilic state (canonical recon, no memory assumptions)

| Item | State |
|---|---|
| Reachable from Orban | YES, `ssh wafflehaus@basilic` (Tailscale MagicDNS); link intermittently wedged by Nord kill-switch |
| python3 | 3.14.6 |
| node | v24.18.0 |
| git | 2.55.0 |
| gh | 2.96.0 |
| gh auth | status returned no "Logged in" line — **treat as NOT confirmed logged in**; verify before any gh-dependent op |
| openclaw | present |
| Abelard clone | `~/Code/Abelard`, single clone, branch main |
| Abelard HEAD | **602e943 — 3 commits BEHIND origin/main (340a85b)** |
| ~/.openclaw state | abelard_queue, agents, chatter, identity, news_watch, smart_money, state |
| disk free | **381 GiB free** of 460 GiB (14% used) — very well provisioned |

### Basilic git caveats (reconcile before next pull / SM-F4 deploy)
- HEAD 602e943 is a clean ANCESTOR of origin/main — no fork, just 3 behind
  (missing 8cc57c3 consensus, 6ec741a commonality, 340a85b form4 persist).
- **Local uncommitted modification: `daemons/smart_money_daemon/scripts/run_scan.sh`**
  — almost certainly the `chmod +x` mode bit set during deploy. A plain
  `git pull` may complain; reconcile (checkout or commit the mode) first.
- Untracked other-daemon files present (news_watch/chatter tools) — not ours.

## Verdict for the migration question
Basilic has 381 GB free and is the canonical collection host. Orban's disk
pressure, if any, is NOT vhdx slack (only ~1.9 GB) — it is the regenerable
raw caches on C: (~940 MB) plus two leftover snapshots (253 MB). Reclaiming
those (Phase C, gated) addresses it without any host migration.

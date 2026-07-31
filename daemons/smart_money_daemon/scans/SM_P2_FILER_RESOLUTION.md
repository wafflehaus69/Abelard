# ORDER SM-P2 Phase 1 — 13F manager CIK resolution

Read-only EDGAR resolution. Identity = EDGAR CIK. No ingest, no registry writes. Where a name resolves to more than one CIK with 13F history, EVERY candidate is listed — Mando picks, the resolver never chooses silently.

`13F-HR` = holdings report. `13F-NT` = notice of NO Section 13(f) holdings — a legitimate reported state, rendered as such, never as an empty book. `/A` = amendment.


## Coatue Management (tier1, thesis `ai_tmt`)
_Laffont, TMT/AI_

  - CIK 0001135730 `COATUE MANAGEMENT LLC` — 13F filings: 99 [13F-HR x92, 13F-HR/A x6, 13F-NT x1] | periods 97 | 2001-03-02 .. 2026-05-15

## Whale Rock Capital Management (tier1, thesis `ai_tmt`)
_Sacerdote, TMT_

  - CIK 0001387322 `Whale Rock Capital Management LLC` — 13F filings: 83 [13F-HR x78, 13F-HR/A x5] | periods 82 | 2007-02-13 .. 2026-05-15

## Light Street Capital Management (tier1, thesis `ai_tmt`)
_Kacher, tech_

  - CIK 0001569049 `LIGHT STREET CAPITAL MANAGEMENT, LLC` — 13F filings: 54 [13F-HR x54] | periods 54 | 2013-02-13 .. 2026-05-15

## Lone Pine Capital (tier1, thesis `ai_tmt`)
_Tiger cub_

  - CIK 0001061165 `LONE PINE CAPITAL LLC` — 13F filings: 94 [13F-HR x86, 13F-HR/A x8] | periods 89 | 2005-02-14 .. 2026-05-15

## Appaloosa (tier1, thesis `value`)
_Tepper_

- **AMBIGUOUS — 2 candidates with 13F history. MANDO PICKS.**
  - CIK 0001006438 `?` — 13F filings: 74 [13F-HR x68, 13F-HR/A x6] | periods 72 | 1999-05-07 .. 2016-02-12
  - CIK 0001656456 `?` — 13F filings: 41 [13F-HR x41] | periods 41 | 2016-05-13 .. 2026-05-15

## Baker Bros Advisors (tier1, thesis `biotech`)
_adjacent to GUTS/ABCL surfacing_

  - CIK 0001263508 `BAKER BROS. ADVISORS LP` — 13F filings: 98 [13F-HR x98] | periods 92 | 2003-09-19 .. 2026-05-15

## Pershing Square Capital Management (tier1, thesis `activist`)
_Ackman, ~10 positions_

  - CIK 0001336528 `Pershing Square Capital Management, L.P.` — 13F filings: 102 [13F-HR x82, 13F-HR/A x15, 13FCONP x5] | periods 94 | 2006-02-14 .. 2026-05-15

## Soros Fund Management (tier2, thesis `macro`)
  - CIK 0001029160 `SOROS FUND MANAGEMENT LLC` — 13F filings: 125 [13F-HR x110, 13F-HR/A x15] | periods 117 | 1999-05-17 .. 2026-05-15

## Third Point (tier2, thesis `activist`)
  - CIK 0001040273 `Third Point LLC` — 13F filings: 117 [13F-HR x108, 13F-HR/A x9] | periods 112 | 1999-11-26 .. 2026-05-15

## Greenlight Capital (tier2, thesis `value`)
  - CIK 0001079114 `GREENLIGHT CAPITAL INC` — 13F filings: 105 [13F-HR x100, 13F-HR/A x5] | periods 105 | 1999-05-12 .. 2024-02-14

## Scion Asset Management (wildcard, thesis `contrarian`)
_expect small, put-heavy, possible 13F-NT and confidential-treatment gaps_

  - CIK 0001649339 `Scion Asset Management, LLC` — 13F filings: 33 [13F-HR x32, 13F-HR/A x1] | periods 33 | 2016-02-16 .. 2025-11-03

## RULINGS NEEDED before ingest

- **Appaloosa — AMBIGUOUS.** Candidates:
  - CIK 0001006438 — 74 filings, 1999-05-07 .. 2016-02-12
  - CIK 0001656456 — 41 filings, 2016-05-13 .. 2026-05-15  <- most recent
- **Greenlight Capital — STALE.** Newest 13F is 2024-02-14 while the target set runs to 2026-05-15. An 8-quarter window from today would be largely EMPTY; this needs a keep/drop ruling rather than a silently thin book.

## Deliberate exclusions (per order, NOT oversight)

- **Renaissance Technologies** — files RIEF/RIDA/RIDGE, not Medallion; ~3,213 positions
- **Citadel** — quant/multistrat, structurally illegible
- **Millennium** — quant/multistrat, structurally illegible
- **Two Sigma** — quant/multistrat, structurally illegible
- **DE Shaw** — quant/multistrat, structurally illegible
- **Balyasny** — quant/multistrat, structurally illegible

## STOP

Resolution only. No CIK ingested, no registry entry written. Awaiting Mando's picks before ORDER SM-P2 ingest (8 quarters each, gates G1-G4).

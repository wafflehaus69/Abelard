# Congressional annual-FD holdings — capture / data-quality addendum (SM-C1)

House annual Financial Disclosure (Schedule A: Assets) holdings ingest. Source: House
Clerk `disclosures-clerk.house.gov` yearly `{year}FD.zip` index + `financial-pdfs`
PDFs. Annual report types `O` (original) + `A` (amendment). Scripts-only, no LLM.
Positional column parser (`house_fd_ingest.parse_fd_assets`) reusing `house_ingest`'s
technique. Resume-safe by DocID (`congress_fd_seen`); PDFs disk-cached; two-run
idempotent (a re-run skips all seen docs, writes 0 rows).

## Phase A — measured capture (100-filing sample, 2024-2025), after A3 hardening
- **Ticker capture (ST/OP rows): 100%** (median 1.00, min 0.98) — gate >=95% MET.
- **Value-band capture: median 0.90 / mean 0.84** — gate >=90% met at median; the mean
  is dragged by fund-heavy filings (nested 401k / trust sub-funds), the ROUGH value
  layer, not stocks.
- **Parse status: ~86% ok, ~7% paper, ~7% unparsed_layout.**

### A3 fixes applied (descending coverage)
1. Header "Tx." made optional — one template family omits the `Tx. > $1,000?` column;
   requiring it had dropped ~19% of filings to unparsed. Recovered ~12/19.
2. Pure-structural held-through account rows (`Savings Plus ⇒`, no ticker/value/type)
   dropped — they are not holdings and only inflated the band-miss count.

### Documented residuals (below the bar, kept visible)
- **Paper (~7%)**: scanned PDFs, no text layer — OCR territory, out of scope; sampled to
  `data/raw/house_fd_unparsed/`, counted, never guessed.
- **unparsed_layout (~7%)**: remaining header variants; sampled + counted.
- **Nested-fund value bands**: held-through sub-fund bands sometimes null; the miss is
  KEPT per row (null band, countable in the UI), never silently corrupted. Concentrated
  in funds, not stocks.

## Phase B — full House run
- Filings processed: **341** (2026 + 2025 annual O/A).
- ok **298** (87%) · paper **21** (6%) · unparsed **22** (6%) · errors **0**.
- Holding rows landed: **11,183** across **294** filings; **2,726** distinct tickers.
- Two-run idempotence: all 341 docs recorded in `congress_fd_seen`; a re-run skips them.

## Coverage caveat
Not all ~435 House members appear: annual reports for the current filing year are still
being filed, some members file late or seek extensions, and paper/unparsed filings are
excluded. Senate is a separate chamber (SM-C1 Phase D, gated on eFD recon). House-only is
a legitimate v1 with the gap stated.

"""DATA_QUALITY.md generator per ORDER SM-1. Reads the cache DB, writes
analysis/DATA_QUALITY.md. Deterministic, no network."""
import argparse
import datetime as dt
import sys

import pandas as pd

from . import db as dbmod
from .mdfmt import md_table


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--out", default=dbmod.artifact_path("DATA_QUALITY.md"))
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)

    filings = pd.read_sql_query("SELECT * FROM ingested_filings", con)
    trades = pd.read_sql_query(
        "SELECT ct.*, p.name FROM congress_trades ct JOIN persons p USING(person_id)",
        con,
    )
    horizon = con.execute(
        "SELECT v FROM meta_kv WHERE k='senate_efd_horizon_year'"
    ).fetchone()
    horizon = horizon[0] if horizon else "WALK NOT COMPLETED"

    if filings.empty:
        print("FATAL no ingested filings", file=sys.stderr)
        return 2

    filings["year"] = filings.filed_date.str[:4]
    trades["year"] = trades.disclosure_date.str[:4]

    md = []
    md.append("# DATA_QUALITY — smart_money_daemon")
    md.append("")

    titles = {
        "senate": "Senate eFD ingest (Phase 1a)",
        "house": "House Clerk ingest (Phase 1b, pdfplumber text-layer extraction)",
    }
    horizon_keys = {
        "senate": "senate_efd_horizon_year",
        "house": "house_horizon_year",
    }
    for chamber in [c for c in ("senate", "house") if (filings.chamber == c).any()]:
        cf = filings[filings.chamber == chamber]
        ct = trades[trades.chamber == chamber]
        hrow = con.execute(
            "SELECT v FROM meta_kv WHERE k=?", (horizon_keys[chamber],)
        ).fetchone()
        md.append("## {}".format(titles[chamber]))
        md.append("")
        # F4 one-truth coverage statement per chamber. House was a year-walk to a
        # true horizon; Senate was a full browser index harvest (WAF blocked the
        # search endpoint), so state the verified coverage start, not a fake walk.
        cov_start = ct.tx_date.min() if not ct.empty else "n/a"
        if chamber == "house":
            md.append("- Coverage: year-walk to electronic horizon **{}**; "
                      "earliest trade {}.".format(
                          hrow[0] if hrow else "WALK NOT COMPLETED", cov_start))
        else:
            md.append("- Coverage: full browser index harvest of the eFD PTR "
                      "corpus (search endpoint WAF-blocked, see recon/"
                      "EFD_WAF_FINDING.md). Verified coverage from earliest "
                      "trade **{}**; no year-walk applies.".format(cov_start))
        md.append("- Filings seen: {} — status breakdown: {}".format(
            len(cf), cf.status.value_counts().to_dict()
        ))
        md.append("- Trade rows ingested: {}".format(len(ct)))
        md.append("- Amendments among filings: {}".format(
            int(cf.report_label.fillna("").str.contains("Amendment", case=False).sum())
        ))
        md.append("")
        md.append("### Per-year filings and rows")
        md.append("")
        per_year = (
            cf.groupby(["year", "status"]).size().unstack(fill_value=0).reset_index()
        )
        rows_per_year = ct.groupby("year").size().rename("trade_rows")
        per_year = per_year.merge(rows_per_year, on="year", how="left").fillna(0)
        md.append(md_table(per_year))
        md.append("")
        md.append("### Skipped filings per person per year (no OCR, never guessed)")
        md.append("")
        skipped = cf[cf.status.isin(["paper", "unparsed_layout", "fetch_failed"])]
        if skipped.empty:
            md.append("None.")
        else:
            pp = (
                skipped.groupby(["person_name", "year", "status"])
                .size()
                .rename("n")
                .reset_index()
            )
            md.append(md_table(pp.sort_values(["n"], ascending=False).head(40)))
            md.append("")
            md.append("({} skipped filings total; table shows top 40)".format(len(skipped)))
        md.append("")
        md.append("### Row-level quality")
        md.append("")
        md.append("- Rows with no ticker: {}".format(int(ct.ticker.isna().sum())))
        md.append("- Rows with negative disclosure lag: {}".format(
            int((ct.lag_days < 0).sum())
        ))
        md.append("- Open-ended top band rows (amt_high NULL): {}".format(
            int(ct.amt_high.isna().sum())
        ))
        md.append("- Side distribution: {}".format(ct.side.value_counts().to_dict()))
        md.append("")
        md.append("### Asset-type distribution (non-stock ingested, tagged, "
                  "filtered only in Phase 2)")
        md.append("")
        at = (
            ct.asset_type.value_counts().head(20)
            .rename_axis("asset_type").rename("rows").reset_index()
        )
        md.append(md_table(at))
        md.append("")
    md.append("## Survivorship (F3, missing-price tickers)")
    md.append("")
    ts = pd.read_sql_query("SELECT * FROM ticker_status", con)
    if ts.empty:
        md.append("Not yet classified — run smart_money.survivorship.")
    else:
        vc = ts.verdict.value_counts().to_dict()
        md.append("- delisted_presumed: {}".format(vc.get("delisted_presumed", 0)))
        md.append("- data_gap: {}".format(vc.get("data_gap", 0)))
        provisional = ts.heuristic.astype(str).str.contains("PROVISIONAL").any()
        last_probe = dt.datetime.utcfromtimestamp(
            int(ts.probed_at_unix.max())
        ).date().isoformat()
        md.append("- Status: {} (last probe {})".format(
            "PROVISIONAL recency-only, probe pending" if provisional
            else "probed via Yahoo v8", last_probe))
        md.append("- Heuristic: {}".format(ts.heuristic.dropna().iloc[0]
                                           if ts.heuristic.notna().any() else "n/a"))
        md.append("")
        md.append("**Bias statement:** excluded missing-series tickers are more "
                  "likely losers (delisting skews down), so measured returns are "
                  "inflated. Direction is known, magnitude is unmeasured. Returns "
                  "are NEVER imputed for a missing series.")
        md.append("")
        dl = ts[ts.verdict == "delisted_presumed"].sort_values("ticker")
        if not dl.empty:
            md.append("delisted_presumed tickers: {}".format(
                " ".join(dl.ticker.tolist())))
        md.append("")
    md.append("## Date integrity (filer typos, all chambers)")
    md.append("")
    today = dt.date.today().isoformat()
    def _bad(s):
        return not (isinstance(s, str) and "2004-01-01" <= s <= today)
    bad_tx = trades[trades.tx_date.map(_bad)]
    bad_disc = trades[trades.disclosure_date.map(_bad)]
    md.append("- Out-of-range tx_date rows (excluded from scorecard): {}".format(len(bad_tx)))
    md.append("- Out-of-range disclosure_date rows: {}".format(len(bad_disc)))
    neg = trades[trades.lag_days < 0]
    md.append("- Negative-lag rows (disclosure before trade, excluded both "
              "clocks): {} — by chamber {}".format(
                  len(neg), neg.chamber.value_counts().to_dict()))
    bad = pd.concat([bad_tx, bad_disc]).drop_duplicates(subset=["trade_id"])
    if not bad.empty:
        md.append("")
        show = bad[["name", "chamber", "ticker", "tx_date", "disclosure_date",
                    "filing_id"]].head(30)
        md.append(md_table(show))
    md.append("")
    md.append("## Notes")
    md.append("")
    md.append("- disclosure_date = filing date (eFD PTR date for senate, Clerk "
              "index FilingDate for house).")
    md.append("- Paper and unparsed-layout filings counted, never OCRed, never "
              "guessed, per order.")
    md.append("- Amendment PTRs ingested as filed — possible re-reports of the "
              "same transaction. Flagged for Mando, dedup policy not specced "
              "in SM-1.")
    md.append("- House extraction library: pdfplumber (chosen over pypdf for "
              "positional words enabling layout-versioned column parsing).")

    with open(args.out, "w") as f:
        f.write("\n".join(md) + "\n")
    print("[dq] wrote {}".format(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

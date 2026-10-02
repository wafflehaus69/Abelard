"""Price paths after entry, summarised inside the query (no per-token rows fetched).

Reuses the event-column query's own definitions (gen_pt_features_tier_a.build) up to its
`snap` and `ddp` blocks, then aggregates: one output row per entry lag. Descriptive only:
mid price to mid price, no fills, no costs, no gate. Used for the burned post-BOOST week
(MR-13) and comparable to HEAVY/TRIAL report section 3.7 for the June 2025 calibration week.

The last two columns count the H5 'winner' definition pre-registered in the runbook
(24h peak >= 3x entry at G240 AND 7-day price >= entry); a base rate, not a test.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import gen_pt_features_tier_a as ta
import seeded_selections as ss
import datetime as dt

ROOT = pathlib.Path(__file__).resolve().parents[1]


def build(gd0: str, gd1: str) -> str:
    full = ta.build(gd0, gd1)
    head = full[:full.index("d24 AS (")].rstrip().rstrip(",")
    lags = "\n  UNION ALL\n".join(f"""  SELECT {g} AS lag_min, count(*) AS tokens, count(s.px_g{g}) AS has_entry, count(s.px_7d) AS has_7d,
    approx_percentile(s.px_7d / s.px_g{g}, ARRAY[0.1, 0.25, 0.5, 0.75, 0.9, 0.99]) AS ret7_pcts,
    avg(s.px_7d / s.px_g{g}) AS ret7_mean, avg(CASE WHEN s.px_7d > s.px_g{g} THEN 1e0 ELSE 0e0 END) AS share_above_entry,
    approx_percentile(s.peak_7d_g{g} / s.px_g{g}, 0.5) AS peak7_p50, avg(CASE WHEN s.peak_7d_g{g} >= 2 * s.px_g{g} THEN 1e0 ELSE 0e0 END) AS share_2x,
    approx_percentile(s.peak_24h_g{g} / s.px_g{g}, 0.5) AS peak24_p50, approx_percentile(d.mdd_7d_g{g}, 0.5) AS mdd7_p50,
    approx_percentile(s.px_g{g} / s.px_grad, 0.5) AS mk_p50,
    approx_percentile(s.cb_p50, 0.5) AS buy_cost_bps_p50, approx_percentile(s.cs_p50, 0.5) AS sell_cost_bps_p50,
    approx_percentile(s.qres_g{g} / 1e9, 0.5) AS qres_sol_p50,
    count_if(s.peak_24h_g{g} >= 3 * s.px_g{g} AND s.px_7d >= s.px_g{g}) AS h5_winners,
    count_if(s.peak_24h_g{g} >= 3 * s.px_g{g}) AS peak24_3x
  FROM snap s LEFT JOIN ddp d ON d.mint = s.mint WHERE s.px_g{g} > 0""" for g in ta.GS)
    return f"-- Price-path summary, graduations {gd0} .. {gd1}. Descriptive; one row per entry lag.\n{head}\n{lags}"


if __name__ == "__main__":
    w = ss.burned_week()
    out = ROOT / "recon" / "sql"
    (out / "pricepath_burned_week.sql").write_text(build(w.isoformat(), (w + dt.timedelta(days=6)).isoformat()), encoding="utf-8")
    (out / "pricepath_burned_day.sql").write_text(build(w.isoformat(), w.isoformat()), encoding="utf-8")
    print("written: pricepath_burned_week.sql, pricepath_burned_day.sql for week", w)

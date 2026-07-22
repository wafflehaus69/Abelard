"""F1 cluster correction (ORDER SM-2). Pre-scoring pass that collapses an
accumulation episode into one scored event, so a politician who dollar-cost-
averages one conviction (e.g. McCormick into BITB) is not counted as N
independent correct calls.

Placed in the smart_money package (order text said analysis/clustering.py) so
scorecard.py can import it; noted as a path deviation in the SM-2 report.

Rule: per (person_id, ticker), sort purchases by tx_date and chain them into a
cluster while each next fill is within window_days of the PREVIOUS fill (a
rolling gap, so a continuous accumulation is one episode and a >window gap
starts a new one). Event fields:
  tx_date     = earliest tx_date in the cluster
  disclosure  = earliest disclosure_date in the cluster
  mid         = SUM of band midpoints (p90 cap is applied later, at event level)
  n_fills     = cluster size (a visible column, never a weight)
  lag_days    = event disclosure minus event tx
"""
import datetime as dt

import pandas as pd

WINDOW_DAYS = 30


def _midpoint(low, high):
    return (low + high) / 2.0 if high is not None and not pd.isna(high) else float(low)


def cluster_purchases(purchases: pd.DataFrame, window_days: int = WINDOW_DAYS):
    """Collapse (person, ticker) accumulation episodes into events.

    Input columns: person_id, name, chamber, ticker, amt_low, amt_high,
    tx_date, disclosure_date, lag_days. Returns one row per event."""
    events = []
    cols = ["person_id", "name", "chamber", "ticker"]
    for key, grp in purchases.groupby(cols, sort=False):
        grp = grp.sort_values("tx_date")
        cluster = []
        last = None
        for r in grp.itertuples():
            d = dt.date.fromisoformat(r.tx_date)
            if last is not None and (d - last).days > window_days:
                events.append(_emit(key, cluster))
                cluster = []
            cluster.append(r)
            last = d
        if cluster:
            events.append(_emit(key, cluster))
    return pd.DataFrame(events)


def _emit(key, cluster):
    person_id, name, chamber, ticker = key
    tx_dates = [c.tx_date for c in cluster]
    disc_dates = [c.disclosure_date for c in cluster]
    tx = min(tx_dates)
    disc = min(disc_dates)
    mid = sum(_midpoint(c.amt_low, c.amt_high) for c in cluster)
    lag = (dt.date.fromisoformat(disc) - dt.date.fromisoformat(tx)).days
    return {
        "person_id": person_id,
        "name": name,
        "chamber": chamber,
        "ticker": ticker,
        "tx_date": tx,
        "disclosure_date": disc,
        "mid": mid,
        "n_fills": len(cluster),
        "lag_days": lag,
    }

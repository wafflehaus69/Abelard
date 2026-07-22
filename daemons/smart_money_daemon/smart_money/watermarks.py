"""Per-source watermarks (SM-4 STEP 3). news_watch 72ecd7a lesson verbatim:
advance ONLY on ok-with-items, to the newest INGESTED item's disclosure
timestamp — never to now(). 0-item-ok and non-ok runs preserve the watermark.
Callers re-fetch an overlap window past the watermark; dedup by filing_id makes
the overlap free.
"""
import time

OVERLAP_DAYS = 3


def get(con, source):
    r = con.execute(
        "SELECT watermark_ts FROM watermarks WHERE source=?", (source,)
    ).fetchone()
    return r[0] if r else None


def advance(con, source, newest_disclosure_ts):
    """Advance the watermark to the newest ingested item's disclosure ts.
    Only call on an ok-with-items run. Never moves backward."""
    cur = get(con, source)
    if cur is not None and newest_disclosure_ts <= cur:
        return cur
    con.execute(
        "INSERT OR REPLACE INTO watermarks VALUES (?,?,?)",
        (source, newest_disclosure_ts, int(time.time())),
    )
    con.commit()
    return newest_disclosure_ts

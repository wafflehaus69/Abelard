"""ChatterDaemon — multi-source retail-chatter sensor for OpenClaw.

The multi-source generalization of BizDaemon: against a named watchlist it extracts
US-equity ticker mentions, counts distinct-post salience, and classifies stance
across StockTwits, Reddit, Google Trends, Finnhub company-news, and 4chan /smg/,
emitting structured JSON. A dumb sensor — it extracts, counts, classifies; Abelard
judges.
"""

__version__ = "0.1.0"

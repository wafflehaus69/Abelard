"""CONSENSUS — Polymarket winners-circle signal system.

Read-only ingestion of Polymarket **international** on-chain data and Kalshi
public market data. This package never places, signs, or stages an order on
international Polymarket (spec Rule 2). LLMs are never in the data path
(Rule 4). No value is ever fabricated or interpolated; gaps surface as loud
failures or empty results, never as invented data (Rule 1).

M1 (this milestone) is the data layer: typed, cached, rate-limited fetchers.
"""

__version__ = "0.1.0"

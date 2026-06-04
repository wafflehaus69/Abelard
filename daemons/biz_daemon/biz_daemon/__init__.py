"""Biz Daemon — retail-sentiment sensor over 4chan /biz/ /smg/ for OpenClaw.

A dumb extraction sensor: it finds the live Stock Market General thread,
pulls posts, validates US-equity ticker mentions against the Finnhub
universe, counts distinct-post mentions, and runs a single batched Haiku
classification pass over attention-tier tickers. It emits structured JSON.

It performs no materiality judgment, no trade signal, no theme
intersection. Abelard does the contrarian/confirm read at his layer.
Scripts execute; judgment is Abelard's.
"""

__version__ = "0.1.0"

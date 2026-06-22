"""Source adapters — one per chatter surface (StockTwits, /smg/, ...). Order 2+.

The `Source` protocol and its transport types live in `base.py`; concrete plugins
land in sibling modules and register with the orchestrator.
"""

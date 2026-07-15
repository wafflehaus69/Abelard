"""abelard_queue — Abelard's consumer of the GATE 2 durable alert queue.

The queue primitive (schema, enqueue, status machine, journal) lives in
``abelard_common.alert_queue`` so daemons can enqueue without depending
on this package. This package holds the OTHER side: Abelard's triage +
Telegram dispatch tool (``consumer.py``). Daemons never import it.
"""

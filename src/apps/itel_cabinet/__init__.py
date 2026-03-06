"""iTel Cabinet App.

Standalone app that consumes task events from Event Hub/Kafka,
enriches them with ClaimX data, writes to Delta tables, and
sends to the iTel Cabinet API — all in a single worker.

Workers:
- tracking_worker: Enriches, writes to Delta, and sends to iTel API
"""

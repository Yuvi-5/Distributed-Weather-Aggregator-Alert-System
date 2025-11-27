from prometheus_client import Counter, Gauge

ingested_messages = Counter(
    "ingested_messages_total", "Total MQTT observation messages ingested."
)
ingest_failures = Counter(
    "ingest_failures_total", "Total MQTT observation messages failed to ingest."
)
aggregate_runs = Counter(
    "aggregate_runs_total", "Total aggregate refresh runs executed."
)
alerts_emitted = Counter("alerts_emitted_total", "Total alerts emitted.")
ingest_queue_size = Gauge(
    "ingest_queue_size", "Current MQTT ingestion queue size (approx)."
)

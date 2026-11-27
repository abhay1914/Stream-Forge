"""Worker batch metrics and Prometheus instrumentation."""

from threading import RLock
from time import time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

_metrics_lock = RLock()

_latest_batch_metrics = {
    "found": 0,
    "processed": 0,
    "failed": 0,
    "processing_time_ms": 0.0,
    "average_latency_ms": 0.0,
}

EVENTS_FOUND = Counter(
    "streamforge_events_found",
    "Total number of events claimed by workers.",
)

EVENTS_PROCESSED = Counter(
    "streamforge_events_processed",
    "Total number of events successfully processed by workers.",
)

EVENTS_FAILED = Counter(
    "streamforge_events_failed",
    "Total number of events that failed worker processing.",
)

BATCH_PROCESSING_DURATION = Histogram(
    "streamforge_batch_processing_duration_seconds",
    "Time spent processing a worker batch in seconds.",
)

EVENT_AVERAGE_LATENCY = Histogram(
    "streamforge_event_average_latency_seconds",
    "Average per-event processing latency reported by each non-empty batch.",
)

BATCH_SIZE = Gauge(
    "streamforge_batch_size",
    "Number of events in the most recently processed batch.",
)

WORKER_LAST_RUN_TIMESTAMP = Gauge(
    "streamforge_worker_last_run_timestamp_seconds",
    "Unix timestamp of the most recent worker batch completion.",
)


def record_batch_metrics(summary):
    """Store the latest batch summary and update Prometheus metrics."""
    with _metrics_lock:
        _latest_batch_metrics.update(summary)

        found = int(summary.get("found", 0))
        processed = int(summary.get("processed", 0))
        failed = int(summary.get("failed", 0))

        EVENTS_FOUND.inc(found)
        EVENTS_PROCESSED.inc(processed)
        EVENTS_FAILED.inc(failed)

        BATCH_SIZE.set(found)
        BATCH_PROCESSING_DURATION.observe(
            max(float(summary.get("processing_time_ms", 0.0)), 0.0) / 1000
        )

        if found > 0:
            EVENT_AVERAGE_LATENCY.observe(
                max(float(summary.get("average_latency_ms", 0.0)), 0.0) / 1000
            )

        WORKER_LAST_RUN_TIMESTAMP.set(time())


def get_batch_metrics():
    """Return a copy of the latest batch summary."""
    with _metrics_lock:
        return _latest_batch_metrics.copy()


def get_prometheus_metrics():
    """Return metrics in Prometheus exposition format."""
    return generate_latest(), CONTENT_TYPE_LATEST

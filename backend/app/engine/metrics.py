
"""In-memory storage for the latest worker batch metrics."""

_latest_batch_metrics = {
    "found": 0,
    "processed": 0,
    "failed": 0,
    "processing_time_ms": 0.0,
    "average_latency_ms": 0.0,
}


def record_batch_metrics(summary):
    """Store the latest batch summary."""
    _latest_batch_metrics.update(summary)


def get_batch_metrics():
    """Return a copy of the latest batch summary."""
    return _latest_batch_metrics.copy()

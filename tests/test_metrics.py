from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY

from backend.app.engine.metrics import (
    get_batch_metrics,
    get_prometheus_metrics,
    record_batch_metrics,
)


def test_record_batch_metrics_updates_prometheus_metrics():
    summary = {
        "found": 3,
        "processed": 2,
        "failed": 1,
        "processing_time_ms": 125.0,
        "average_latency_ms": 12.5,
    }

    before_found = REGISTRY.get_sample_value(
        "streamforge_events_found_total"
    ) or 0.0
    before_processed = REGISTRY.get_sample_value(
        "streamforge_events_processed_total"
    ) or 0.0
    before_failed = REGISTRY.get_sample_value(
        "streamforge_events_failed_total"
    ) or 0.0
    before_batches = REGISTRY.get_sample_value(
        "streamforge_batch_processing_duration_seconds_count"
    ) or 0.0
    before_latency = REGISTRY.get_sample_value(
        "streamforge_event_average_latency_seconds_count"
    ) or 0.0

    record_batch_metrics(summary)

    payload, content_type = get_prometheus_metrics()
    exposition = payload.decode("utf-8")

    assert content_type == CONTENT_TYPE_LATEST
    assert get_batch_metrics() == summary

    assert REGISTRY.get_sample_value(
        "streamforge_events_found_total"
    ) == before_found + 3
    assert REGISTRY.get_sample_value(
        "streamforge_events_processed_total"
    ) == before_processed + 2
    assert REGISTRY.get_sample_value(
        "streamforge_events_failed_total"
    ) == before_failed + 1

    assert REGISTRY.get_sample_value(
        "streamforge_batch_processing_duration_seconds_count"
    ) == before_batches + 1
    assert REGISTRY.get_sample_value(
        "streamforge_event_average_latency_seconds_count"
    ) == before_latency + 1

    assert REGISTRY.get_sample_value("streamforge_batch_size") == 3
    assert REGISTRY.get_sample_value(
        "streamforge_worker_last_run_timestamp_seconds"
    ) > 0

    assert "# HELP streamforge_events_found" in exposition
    assert "# TYPE streamforge_batch_processing_duration_seconds histogram" in exposition

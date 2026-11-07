from datetime import datetime, timezone

import pytest
from backend.app.storage.state_store import RocksDBStateStore

from backend.app.streaming.event_time_windows import (
    EventTimeWindowAggregator,
)


def make_event(event_id, timestamp, temperature=8.0, truck_id="TRUCK-001"):
    return {
        "event_id": event_id,
        "truck_id": truck_id,
        "temperature": temperature,
        "timestamp": timestamp,
    }


@pytest.fixture
def aggregator():
    return EventTimeWindowAggregator(
        window_minutes=5,
        allowed_lateness_minutes=1,
    )


def test_event_is_accepted_without_finalizing_window(aggregator):
    result = aggregator.process(
        make_event("event-1", "2026-11-05T10:01:00+00:00", 8.0)
    )

    assert result["status"] == "accepted"
    assert result["finalized_windows"] == []


def test_events_are_aggregated_into_five_minute_window(aggregator):
    aggregator.process(
        make_event("event-1", "2026-11-05T10:01:00+00:00", 8.0)
    )

    aggregator.process(
        make_event("event-2", "2026-11-05T10:03:00+00:00", 12.0)
    )

    result = aggregator.process(
        make_event("event-3", "2026-11-05T10:06:01+00:00", 10.0)
    )

    assert result["status"] == "accepted"
    assert len(result["finalized_windows"]) == 1

    window = result["finalized_windows"][0]

    assert window["truck_id"] == "TRUCK-001"
    assert window["window_start"] == "2026-11-05T10:00:00+00:00"
    assert window["window_end"] == "2026-11-05T10:05:00+00:00"
    assert window["reading_count"] == 2
    assert window["average_temperature"] == 10.0
    assert window["minimum_temperature"] == 8.0
    assert window["maximum_temperature"] == 12.0


def test_event_at_five_minute_boundary_starts_new_window(aggregator):
    result = aggregator.process(
        make_event("event-boundary", "2026-11-05T10:05:00+00:00")
    )

    assert result["status"] == "accepted"
    assert result["finalized_windows"] == []


def test_duplicate_event_is_rejected(aggregator):
    event = make_event(
        "event-duplicate",
        "2026-11-05T10:01:00+00:00",
    )

    first = aggregator.process(event)
    second = aggregator.process(event)

    assert first["status"] == "accepted"
    assert second["status"] == "duplicate"


def test_event_older_than_watermark_is_late(aggregator):
    aggregator.process(
        make_event("event-new", "2026-11-05T10:06:01+00:00")
    )

    result = aggregator.process(
        make_event("event-late", "2026-11-05T10:04:59+00:00")
    )

    assert result["status"] == "late"
    assert result["event_id"] == "event-late"
    assert result["finalized_windows"] == []


def test_naive_timestamp_is_treated_as_utc(aggregator):
    result = aggregator.process(
        make_event("event-naive", "2026-11-05T10:01:00")
    )

    assert result["status"] == "accepted"


def test_different_trucks_have_independent_windows(aggregator):
    aggregator.process(
        make_event(
            "event-truck-1",
            "2026-11-05T10:01:00+00:00",
            truck_id="TRUCK-001",
        )
    )

    result = aggregator.process(
        make_event(
            "event-truck-2",
            "2026-11-05T10:06:01+00:00",
            truck_id="TRUCK-002",
        )
    )

    assert result["finalized_windows"] == []


def test_open_window_recovers_after_restart(tmp_path):
    db_path = tmp_path / "recovery"

    store = RocksDBStateStore(db_path)
    aggregator = EventTimeWindowAggregator(state_store=store)

    aggregator.process(
        make_event("recovery-1", "2026-11-05T10:01:00+00:00", 8.0)
    )
    aggregator.process(
        make_event("recovery-2", "2026-11-05T10:03:00+00:00", 12.0)
    )

    store.close()

    # Simulate application restart.
    store = RocksDBStateStore(db_path)
    recovered = EventTimeWindowAggregator(state_store=store)

    result = recovered.process(
        make_event("recovery-3", "2026-11-05T10:06:01+00:00", 10.0)
    )

    assert result["status"] == "accepted"
    assert len(result["finalized_windows"]) == 1

    window = result["finalized_windows"][0]
    assert window["reading_count"] == 2
    assert window["average_temperature"] == 10.0

    store.close()


def test_processed_event_ids_survive_restart(tmp_path):
    db_path = tmp_path / "deduplication"

    store = RocksDBStateStore(db_path)
    aggregator = EventTimeWindowAggregator(state_store=store)

    event = make_event(
        "persistent-duplicate",
        "2026-11-05T10:01:00+00:00",
    )

    assert aggregator.process(event)["status"] == "accepted"

    store.close()

    store = RocksDBStateStore(db_path)
    recovered = EventTimeWindowAggregator(state_store=store)

    result = recovered.process(event)

    assert result["status"] == "duplicate"

    store.close()


def test_watermark_survives_restart(tmp_path):
    db_path = tmp_path / "watermark"

    store = RocksDBStateStore(db_path)
    aggregator = EventTimeWindowAggregator(state_store=store)

    aggregator.process(
        make_event(
            "watermark-new",
            "2026-11-05T10:06:01+00:00",
        )
    )

    store.close()

    store = RocksDBStateStore(db_path)
    recovered = EventTimeWindowAggregator(state_store=store)

    result = recovered.process(
        make_event(
            "watermark-late",
            "2026-11-05T10:04:59+00:00",
        )
    )

    assert result["status"] == "late"

    store.close()

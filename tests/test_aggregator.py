
from datetime import datetime, timedelta, timezone

from backend.app.engine.aggregator import RollingTemperatureAggregator
from backend.app.schemas.events import TemperatureEvent


def make_event(event_id, temperature, timestamp, truck_id="TRUCK-001"):
    return TemperatureEvent(
        event_id=event_id,
        truck_id=truck_id,
        temperature=temperature,
        timestamp=timestamp,
    )


def test_calculates_average():
    aggregator = RollingTemperatureAggregator()
    now = datetime.now(timezone.utc)

    aggregator.process(make_event("e1", 20, now))
    result = aggregator.process(
        make_event("e2", 30, now + timedelta(seconds=10))
    )

    assert result["average_temperature"] == 25
    assert result["reading_count"] == 2


def test_rejects_duplicate_event():
    aggregator = RollingTemperatureAggregator()
    now = datetime.now(timezone.utc)
    event = make_event("e1", 20, now)

    assert aggregator.process(event)["accepted"] is True
    assert aggregator.process(event)["accepted"] is False


def test_excludes_expired_reading():
    aggregator = RollingTemperatureAggregator()
    now = datetime.now(timezone.utc)

    aggregator.process(make_event("e1", 20, now))
    result = aggregator.process(
        make_event("e2", 30, now + timedelta(minutes=6))
    )

    assert result["average_temperature"] == 30
    assert result["reading_count"] == 1


def test_keeps_trucks_separate():
    aggregator = RollingTemperatureAggregator()
    now = datetime.now(timezone.utc)

    aggregator.process(make_event("e1", 20, now, "TRUCK-001"))
    result = aggregator.process(
        make_event("e2", 40, now, "TRUCK-002")
    )

    assert result["average_temperature"] == 40
    assert result["reading_count"] == 1

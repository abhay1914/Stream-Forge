
from datetime import datetime,  timedelta, timezone

from backend.app.engine.worker import EventWorker
from backend.app.schemas.events import TemperatureEvent
from backend.app.storage.repository import EventRepository


def test_worker_processes_pending_event(tmp_path):
    repository = EventRepository(tmp_path / "worker_test.db")

    event = TemperatureEvent(
        event_id="worker-event-001",
        truck_id="TRUCK-001",
        temperature=28.5,
        timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert repository.add_event(event) is True

    worker = EventWorker(repository=repository)
    summary = worker.process_pending_events()

    assert summary["found"] == 1
    assert summary["processed"] == 1
    assert summary["failed"] == 0

    # A processed event should no longer be pending.
    assert repository.get_pending_events() == []


def test_worker_does_not_process_same_event_twice(tmp_path):
    repository = EventRepository(tmp_path / "worker_test.db")

    event = TemperatureEvent(
        event_id="worker-event-002",
        truck_id="TRUCK-002",
        temperature=30.0,
        timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
    )

    repository.add_event(event)
    worker = EventWorker(repository=repository)

    first_summary = worker.process_pending_events()
    second_summary = worker.process_pending_events()

    assert first_summary["processed"] == 1
    assert second_summary["found"] == 0
    assert second_summary["processed"] == 0



def test_claimed_event_cannot_be_claimed_again(tmp_path):
    repository = EventRepository(tmp_path / "claim_test.db")

    event = TemperatureEvent(
        event_id="claim-event-001",
        truck_id="TRUCK-CLAIM",
        temperature=29.5,
        timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
    )
    assert repository.add_event(event) is True

    first_claim = repository.claim_pending_events()
    second_claim = repository.claim_pending_events()

    assert len(first_claim) == 1
    assert first_claim[0]["event_id"] == "claim-event-001"
    assert first_claim[0]["status"] == "pending"
    assert second_claim == []


def test_claim_pending_events_respects_limit(tmp_path):
    repository = EventRepository(tmp_path / "claim_limit_test.db")

    for number in range(3):
        event = TemperatureEvent(
            event_id=f"claim-limit-{number}",
            truck_id="TRUCK-LIMIT",
            temperature=25.0 + number,
            timestamp=datetime(
                2026, 10, 20, 12, number, tzinfo=timezone.utc
            ),
        )
        assert repository.add_event(event) is True

    claimed = repository.claim_pending_events(limit=2)

    assert len(claimed) == 2
    assert len(repository.get_pending_events()) == 1


def test_worker_marks_failed_event_and_continues(tmp_path, monkeypatch):
    repository = EventRepository(tmp_path / "worker_failure_test.db")

    for event_id, temperature in [
        ("failure-event-001", 28.0),
        ("failure-event-002", 30.0),
    ]:
        event = TemperatureEvent(
            event_id=event_id,
            truck_id="TRUCK-FAILURE",
            temperature=temperature,
            timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
        )
        assert repository.add_event(event) is True

    worker = EventWorker(repository=repository)

    original_average = repository.get_rolling_average

    def fail_first_event(truck_id, event_timestamp, window_minutes=5):
        if truck_id == "TRUCK-FAILURE":
            raise RuntimeError("Simulated processing failure")
        return original_average(truck_id, event_timestamp, window_minutes)

    monkeypatch.setattr(repository, "get_rolling_average", fail_first_event)

    summary = worker.process_pending_events()

    assert summary["found"] == 2
    assert summary["failed"] == 2
    assert summary["processed"] == 0
    assert repository.get_pending_events() == []



def test_stale_processing_event_can_be_reclaimed(tmp_path):
    repository = EventRepository(tmp_path / "stale_event_test.db")

    event = TemperatureEvent(
        event_id="stale-event-001",
        truck_id="TRUCK-STALE",
        temperature=29.0,
        timestamp=datetime.now(timezone.utc),
    )
    assert repository.add_event(event) is True

    stale_time = (
        datetime.now(timezone.utc) - timedelta(minutes=11)
    ).isoformat()

    with repository._connect() as connection:
        connection.execute(
            """
            UPDATE temperature_events
            SET status = 'processing',
                processing_started_at = ?
            WHERE event_id = ?
            """,
            (stale_time, event.event_id),
        )

    claimed = repository.claim_pending_events(
        limit=100,
        recovery_timeout_minutes=10,
    )

    assert len(claimed) == 1
    assert claimed[0]["event_id"] == event.event_id


def test_recently_processing_event_is_not_reclaimed(tmp_path):
    repository = EventRepository(tmp_path / "recent_event_test.db")

    event = TemperatureEvent(
        event_id="recent-event-001",
        truck_id="TRUCK-RECENT",
        temperature=28.0,
        timestamp=datetime.now(timezone.utc),
    )
    assert repository.add_event(event) is True

    recent_time = datetime.now(timezone.utc).isoformat()

    with repository._connect() as connection:
        connection.execute(
            """
            UPDATE temperature_events
            SET status = 'processing',
                processing_started_at = ?
            WHERE event_id = ?
            """,
            (recent_time, event.event_id),
        )

    claimed = repository.claim_pending_events(
        limit=100,
        recovery_timeout_minutes=10,
    )

    assert claimed == []

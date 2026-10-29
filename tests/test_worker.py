
from datetime import datetime,  timedelta, timezone

from backend.app.engine.worker import EventWorker
from backend.app.schemas.events import TemperatureEvent
from backend.app.storage.repository import EventRepository
from concurrent.futures import ThreadPoolExecutor


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

    for event_id, truck_id in [
        ("failure-event-001", "TRUCK-FAILURE"),
        ("failure-event-002", "TRUCK-SUCCESS"),
    ]:
        event = TemperatureEvent(
            event_id=event_id,
            truck_id=truck_id,
            temperature=28.0,
            timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
        )
        assert repository.add_event(event) is True

    worker = EventWorker(repository=repository)
    original_average = repository.get_rolling_average

    def fail_only_first_truck(truck_id, event_timestamp, window_minutes=5):
        if truck_id == "TRUCK-FAILURE":
            raise RuntimeError("Simulated processing failure")

        return original_average(truck_id, event_timestamp, window_minutes)

    monkeypatch.setattr(
        repository, "get_rolling_average", fail_only_first_truck
    )

    summary = worker.process_pending_events()

    assert summary["found"] == 2
    assert summary["failed"] == 1
    assert summary["processed"] == 1

    with repository._connect() as connection:
        statuses = {
            row["event_id"]: row["status"]
            for row in connection.execute(
                "SELECT event_id, status FROM temperature_events"
            ).fetchall()
        }

    assert statuses["failure-event-001"] == "failed"
    assert statuses["failure-event-002"] == "processed"



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



def test_worker_stats_returns_event_status_counts(tmp_path):
    repository = EventRepository(tmp_path / "worker_stats_test.db")

    events = [
        ("stats-pending", "pending"),
        ("stats-processing", "processing"),
        ("stats-processed", "processed"),
        ("stats-failed", "failed"),
    ]

    for event_id, status in events:
        event = TemperatureEvent(
            event_id=event_id,
            truck_id="TRUCK-STATS",
            temperature=28.0,
            timestamp=datetime.now(timezone.utc),
        )
        assert repository.add_event(event) is True

        if status != "pending":
            assert repository.update_event_status(event_id, status) is True

    counts = repository.get_event_status_counts()

    assert counts == {
        "pending": 1,
        "processing": 1,
        "processed": 1,
        "failed": 1,
        "total": 4,
    }


def test_worker_returns_processing_latency_metrics(tmp_path):
    repository = EventRepository(tmp_path / "worker_latency_test.db")

    event = TemperatureEvent(
        event_id="latency-event-001",
        truck_id="TRUCK-LATENCY",
        temperature=28.5,
        timestamp=datetime.now(timezone.utc),
    )
    assert repository.add_event(event) is True

    worker = EventWorker(repository=repository)
    summary = worker.process_pending_events()

    assert summary["found"] == 1
    assert summary["processed"] == 1
    assert summary["failed"] == 0
    assert summary["processing_time_ms"] >= 0
    assert summary["average_latency_ms"] >= 0


def test_empty_batch_returns_zero_latency_metrics(tmp_path):
    repository = EventRepository(tmp_path / "empty_latency_test.db")
    worker = EventWorker(repository=repository)

    summary = worker.process_pending_events()

    assert summary["found"] == 0
    assert summary["processed"] == 0
    assert summary["failed"] == 0
    assert summary["processing_time_ms"] == 0.0
    assert summary["average_latency_ms"] == 0.0


def test_worker_continues_after_one_event_fails(tmp_path, monkeypatch):
    repository = EventRepository(tmp_path / "worker_continue_test.db")

    for event_id, truck_id in [
        ("continue-event-001", "TRUCK-FAIL"),
        ("continue-event-002", "TRUCK-OK"),
    ]:
        event = TemperatureEvent(
            event_id=event_id,
            truck_id=truck_id,
            temperature=28.0,
            timestamp=datetime.now(timezone.utc),
        )
        assert repository.add_event(event) is True

    worker = EventWorker(repository=repository)
    original_average = repository.get_rolling_average

    def fail_only_first_truck(truck_id, event_timestamp, window_minutes=5):
        if truck_id == "TRUCK-FAIL":
            raise RuntimeError("Simulated processing failure")

        return original_average(
            truck_id, event_timestamp, window_minutes
        )

    monkeypatch.setattr(
        repository, "get_rolling_average", fail_only_first_truck
    )

    summary = worker.process_pending_events()

    assert summary["found"] == 2
    assert summary["failed"] == 1
    assert summary["processed"] == 1



def test_concurrent_workers_do_not_claim_same_event(tmp_path):
    database_path = tmp_path / "concurrent_claim_test.db"
    repository = EventRepository(database_path)

    event = TemperatureEvent(
        event_id="concurrent-event-001",
        truck_id="TRUCK-CONCURRENT",
        temperature=28.0,
        timestamp=datetime.now(timezone.utc),
    )
    assert repository.add_event(event) is True

    def claim_events():
        worker_repository = EventRepository(database_path)
        return worker_repository.claim_pending_events(limit=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(claim_events)
        second_future = executor.submit(claim_events)

        first_claim = first_future.result()
        second_claim = second_future.result()

    claimed_ids = [
        event["event_id"]
        for batch in (first_claim, second_claim)
        for event in batch
    ]

    assert claimed_ids.count("concurrent-event-001") == 1
    assert len(first_claim) + len(second_claim) == 1



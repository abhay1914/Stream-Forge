
import pytest

from backend.app.storage.kafka_outbox import KafkaOutbox
from backend.app.storage.state_store import RocksDBStateStore


@pytest.fixture
def outbox(tmp_path):
    store = RocksDBStateStore(tmp_path / "outbox")
    yield KafkaOutbox(store)
    store.close()


def test_enqueue_message(outbox):
    result = outbox.enqueue(
        message_id="event-001:processed",
        topic="temperature-events-processed",
        key="TRUCK-001",
        value={"event_id": "event-001", "temperature": 25.5},
    )

    assert result is True
    assert outbox.pending_count() == 1


def test_pending_message_contains_kafka_payload(outbox):
    outbox.enqueue(
        message_id="event-002:processed",
        topic="temperature-events-processed",
        key="TRUCK-002",
        value={"event_id": "event-002", "temperature": 22.0},
    )

    messages = outbox.get_pending()

    assert len(messages) == 1
    assert messages[0]["topic"] == "temperature-events-processed"
    assert messages[0]["key"] == "TRUCK-002"
    assert messages[0]["value"]["temperature"] == 22.0


def test_duplicate_enqueue_is_ignored(outbox):
    message = {
        "event_id": "event-003",
        "temperature": 18.0,
    }

    first = outbox.enqueue(
        "event-003:processed",
        "temperature-events-processed",
        message,
    )

    second = outbox.enqueue(
        "event-003:processed",
        "temperature-events-processed",
        message,
    )

    assert first is True
    assert second is False
    assert outbox.pending_count() == 1


def test_pending_messages_survive_restart(tmp_path):
    db_path = tmp_path / "outbox-recovery"

    store = RocksDBStateStore(db_path)
    outbox = KafkaOutbox(store)

    outbox.enqueue(
        "event-004:processed",
        "temperature-events-processed",
        {"event_id": "event-004"},
    )

    store.close()

    reopened_store = RocksDBStateStore(db_path)
    recovered_outbox = KafkaOutbox(reopened_store)

    messages = recovered_outbox.get_pending()

    assert len(messages) == 1
    assert messages[0]["message_id"] == "event-004:processed"

    reopened_store.close()


def test_mark_delivered_removes_message(outbox):
    outbox.enqueue(
        "event-005:processed",
        "temperature-events-processed",
        {"event_id": "event-005"},
    )

    result = outbox.mark_delivered("event-005:processed")

    assert result is True
    assert outbox.pending_count() == 0


def test_unknown_delivery_acknowledgement_returns_false(outbox):
    result = outbox.mark_delivered("unknown-message")

    assert result is False


def test_empty_message_id_is_rejected(outbox):
    with pytest.raises(ValueError, match="message_id cannot be empty"):
        outbox.enqueue(
            "",
            "temperature-events-processed",
            {"event_id": "event-006"},
        )


def test_empty_topic_is_rejected(outbox):
    with pytest.raises(ValueError, match="topic cannot be empty"):
        outbox.enqueue(
            "event-007:processed",
            "",
            {"event_id": "event-007"},
        )


def test_enqueue_many_stores_multiple_messages_atomically(outbox):
    messages = [
        {
            "message_id": "event-008:processed",
            "topic": "temperature-events-processed",
            "key": "TRUCK-001",
            "value": {"event_id": "event-008"},
        },
        {
            "message_id": "event-008:window:1",
            "topic": "temperature-windows-5m",
            "key": "TRUCK-001",
            "value": {"reading_count": 5},
        },
    ]

    count = outbox.enqueue_many(messages)

    assert count == 2
    assert outbox.pending_count() == 2


def test_enqueue_many_prevents_duplicate_ids(outbox):
    message = {
        "message_id": "event-009:processed",
        "topic": "temperature-events-processed",
        "value": {"event_id": "event-009"},
    }

    assert outbox.enqueue_many([message, message]) == 1
    assert outbox.pending_count() == 1


def test_enqueue_many_preserves_pending_messages_after_restart(tmp_path):
    db_path = tmp_path / "batch-recovery"

    store = RocksDBStateStore(db_path)
    outbox = KafkaOutbox(store)

    outbox.enqueue_many([
        {
            "message_id": "event-010:processed",
            "topic": "temperature-events-processed",
            "value": {"event_id": "event-010"},
        },
        {
            "message_id": "event-010:window:1",
            "topic": "temperature-windows-5m",
            "value": {"reading_count": 3},
        },
    ])

    store.close()

    reopened_store = RocksDBStateStore(db_path)
    recovered = KafkaOutbox(reopened_store)

    assert recovered.pending_count() == 2

    reopened_store.close()
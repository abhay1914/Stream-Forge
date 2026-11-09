import hashlib
import json
import logging
import os
from pathlib import Path

import faust
from pydantic import ValidationError

from backend.app.storage.state_store import RocksDBStateStore
from backend.app.storage.kafka_outbox import KafkaOutbox
from backend.app.streaming.transforms import transform_temperature_event
from backend.app.streaming.event_time_windows import EventTimeWindowAggregator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("streamforge.faust")

STATE_DIR = Path(
    os.getenv(
        "STREAMFORGE_STATE_DIR",
        str(Path.cwd() / "data" / "streamforge-rocksdb"),
    )
)

state_store = None
window_aggregator = None
kafka_outbox = None


def initialize_state() -> None:
    """Initialize RocksDB, outbox and persistent window state."""

    global state_store, window_aggregator, kafka_outbox

    if state_store is not None:
        return

    logger.info("Opening RocksDB state store: %s", STATE_DIR)

    state_store = RocksDBStateStore(STATE_DIR)

    try:
        kafka_outbox = KafkaOutbox(state_store)

        window_aggregator = EventTimeWindowAggregator(
            window_minutes=5,
            allowed_lateness_minutes=1,
            state_store=state_store,
        )

    except Exception:
        state_store.close()
        state_store = None
        kafka_outbox = None
        window_aggregator = None
        raise

    logger.info(
        "Persistent state restored. Pending outbox messages: %s",
        kafka_outbox.pending_count(),
    )


async def close_state() -> None:
    """Close persistent state during graceful shutdown."""

    global state_store, window_aggregator, kafka_outbox

    if state_store is not None:
        logger.info("Closing RocksDB state store")

        state_store.close()

        state_store = None
        window_aggregator = None
        kafka_outbox = None


app = faust.App(
    "streamforge-week2-processor",
    broker="kafka://localhost:9092",
    key_serializer="raw",
    value_serializer="json",
    consumer_auto_offset_reset="latest",
)

app.on_before_shutdown.connect(close_state)


# Kafka topics

temperature_topic = app.topic(
    "temperature-events",
    key_type=str,
    value_serializer="json",
)

processed_topic = app.topic(
    "temperature-events-processed",
    key_type=str,
    value_serializer="json",
)

dead_letter_topic = app.topic(
    "temperature-events-dead-letter",
    value_serializer="json",
)

window_topic = app.topic(
    "temperature-windows-5m",
    key_type=str,
    value_serializer="json",
)

late_events_topic = app.topic(
    "temperature-events-late",
    key_type=str,
    value_serializer="json",
)


# Map persisted outbox topic names to Faust topics.

OUTPUT_TOPICS = {
    "temperature-events-processed": processed_topic,
    "temperature-events-dead-letter": dead_letter_topic,
    "temperature-windows-5m": window_topic,
    "temperature-events-late": late_events_topic,
}


def build_outbox_messages(
    transformed: dict,
    result: dict,
) -> list[dict]:
    """Build deterministic outgoing messages for an event."""

    event_id = transformed["event_id"]
    truck_id = transformed["truck_id"]

    messages = [
        {
            "message_id": f"{event_id}:processed",
            "topic": "temperature-events-processed",
            "key": truck_id,
            "value": transformed,
        }
    ]

    if result["status"] == "late":
        messages.append(
            {
                "message_id": f"{event_id}:late",
                "topic": "temperature-events-late",
                "key": truck_id,
                "value": result,
            }
        )

    else:
        for window_result in result["finalized_windows"]:
            window_id = (
                f"{window_result['truck_id']}:"
                f"{window_result['window_start']}"
            )

            messages.append(
                {
                    "message_id": f"window:{window_id}",
                    "topic": "temperature-windows-5m",
                    "key": window_result["truck_id"],
                    "value": window_result,
                }
            )

    return messages


async def publish_pending_messages() -> None:
    """Publish pending outbox messages and acknowledge successful sends."""

    if kafka_outbox is None:
        return

    for message in kafka_outbox.get_pending():
        topic_name = message["topic"]
        topic = OUTPUT_TOPICS.get(topic_name)

        if topic is None:
            logger.error(
                "Unknown outbox topic=%s message=%s",
                topic_name,
                message["message_id"],
            )
            break

        try:
            send_arguments = {
                "value": message["value"],
            }

            if message.get("key") is not None:
                send_arguments["key"] = message["key"]

            await topic.send(**send_arguments)

            kafka_outbox.mark_delivered(message["message_id"])

            logger.info(
                "Outbox message delivered id=%s topic=%s",
                message["message_id"],
                topic_name,
            )

        except Exception:
            logger.exception(
                "Outbox delivery failed id=%s topic=%s",
                message["message_id"],
                topic_name,
            )

            # Keep this message and later messages pending.
            break


@app.timer(interval=1.0)
async def outbox_delivery_timer():
    """Periodically recover and publish pending Kafka messages."""

    try:
        initialize_state()
        await publish_pending_messages()

    except Exception:
        logger.exception("Outbox delivery timer failed")


@app.agent(temperature_topic)
async def process_temperature_events(events):

    async for payload in events:

        try:
            transformed = transform_temperature_event(payload)

        except ValidationError as exc:
            logger.warning("Invalid temperature event: %s", exc)

            initialize_state()

            # Use a deterministic ID even if the payload has no event_id.
            serialized = json.dumps(
                payload,
                sort_keys=True,
                default=str,
            )

            digest = hashlib.sha256(
                serialized.encode("utf-8")
            ).hexdigest()

            dead_letter_id = f"dead-letter:{digest}"

            kafka_outbox.enqueue(
                message_id=dead_letter_id,
                topic="temperature-events-dead-letter",
                value={
                    "payload": payload,
                    "error": str(exc),
                },
            )

            continue

        except Exception:
            logger.exception("Unexpected event processing failure")
            raise

        if transformed is None:
            logger.info(
                "Filtered event with non-positive temperature: %s",
                payload.get("event_id"),
            )
            continue

        initialize_state()

        # Prepare outbox records and commit them in the same RocksDB
        # transaction as the aggregator state.
        result = window_aggregator.process(
            transformed,
            extra_puts_factory=lambda processing_result: (
                kafka_outbox.prepare_enqueue_many(
                    build_outbox_messages(
                        transformed,
                        processing_result,
                    )
                )
            ),
        )

        if result["status"] == "duplicate":
            logger.info(
                "Duplicate event ignored: %s",
                transformed["event_id"],
            )
            continue

        if result["status"] == "late":
            logger.warning(
                "Late event=%s truck=%s watermark=%s",
                transformed["event_id"],
                transformed["truck_id"],
                result["watermark"],
            )
            continue

        for window_result in result["finalized_windows"]:
            logger.info(
                "Finalized window truck=%s start=%s average=%s count=%s",
                window_result["truck_id"],
                window_result["window_start"],
                window_result["average_temperature"],
                window_result["reading_count"],
            )

        logger.info(
            "Persisted event=%s truck=%s temperature=%s",
            transformed["event_id"],
            transformed["truck_id"],
            transformed["temperature"],
        )


if __name__ == "__main__":
    app.main()

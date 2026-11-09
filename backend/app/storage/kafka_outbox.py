from typing import Any
from backend.app.storage.state_store import RocksDBStateStore

class KafkaOutbox:
    """Persist outgoing Kafka messages until delivery is confirmed."""

    PREFIX = "streamforge:outbox:"
    INDEX_KEY = PREFIX + "pending_ids"
    MESSAGE_PREFIX = PREFIX + "message:"

    def __init__(self, state_store: RocksDBStateStore) -> None:
        self.state_store = state_store

    def _message_key(self, message_id: str) -> str:
        return self.MESSAGE_PREFIX + message_id

    def prepare_enqueue_many(
        self,
        messages: list[dict[str, Any]],
    ) -> dict[str, Any]:

        pending_ids = self.state_store.get(self.INDEX_KEY) or []
        known_ids = set(pending_ids)
        updated_ids = list(pending_ids)
        puts = {}

        for message in messages:
            message_id = message.get("message_id")
            topic = message.get("topic")

            if not message_id:
                raise ValueError("message_id cannot be empty")

            if not topic:
                raise ValueError("topic cannot be empty")

            if message_id in known_ids:
                continue

            record = {
                "message_id": message_id,
                "topic": topic,
                "key": message.get("key"),
                "value": message.get("value"),
            }

            puts[self._message_key(message_id)] = record
            updated_ids.append(message_id)
            known_ids.add(message_id)

        if puts:
            puts[self.INDEX_KEY] = updated_ids

        return puts

    def prepare_enqueue(
        self,
        message_id: str,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
    ) -> dict[str, Any] | None:
        """Prepare a single message for a combined RocksDB batch."""
        puts = self.prepare_enqueue_many([
            {
                "message_id": message_id,
                "topic": topic,
                "key": key,
                "value": value,
            }
        ])

        return puts or None

    def enqueue_many(
        self,
        messages: list[dict[str, Any]],
    ) -> int:
        """Atomically persist multiple outgoing messages."""
        puts = self.prepare_enqueue_many(messages)

        if not puts:
            return 0

        message_count = len(puts) - 1
        self.state_store.write_batch(puts=puts)

        return message_count

    def enqueue(
        self,
        message_id: str,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
    ) -> bool:
        """Persist one message before attempting Kafka delivery."""
        puts = self.prepare_enqueue(
            message_id=message_id,
            topic=topic,
            value=value,
            key=key,
        )

        if puts is None:
            return False

        self.state_store.write_batch(puts=puts)
        return True

    def get_pending(self) -> list[dict[str, Any]]:
        """Return pending messages in enqueue order."""
        pending_ids = self.state_store.get(self.INDEX_KEY) or []

        messages = []

        for message_id in pending_ids:
            message = self.state_store.get(
                self._message_key(message_id)
            )

            if message is not None:
                messages.append(message)

        return messages

    def mark_delivered(self, message_id: str) -> bool:
        """Remove a message after successful Kafka delivery."""
        pending_ids = self.state_store.get(self.INDEX_KEY) or []

        if message_id not in pending_ids:
            return False

        updated_ids = [
            pending_id
            for pending_id in pending_ids
            if pending_id != message_id
        ]

        self.state_store.write_batch(
            puts={self.INDEX_KEY: updated_ids},
            deletes=[self._message_key(message_id)],
        )

        return True

    def pending_count(self) -> int:
        """Return the number of pending messages."""
        return len(self.get_pending())

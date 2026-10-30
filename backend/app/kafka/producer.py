
import json
from datetime import datetime, timezone

from kafka import KafkaProducer


class TemperatureProducer:
    """Publish truck temperature events to Kafka."""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    def send_event(self, event):
        """Send one temperature event, keyed by truck ID."""
        future = self.producer.send(
            "temperature-events",
            key=event["truck_id"],
            value=event,
        )

        metadata = future.get(timeout=10)

        print(
            f"Sent event | truck={event['truck_id']} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )

    def close(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer = TemperatureProducer()

    try:
        event = {
            "event_id": "event-001",
            "truck_id": "TRUCK-101",
            "temperature": 5.8,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        producer.send_event(event)

    finally:
        producer.close()

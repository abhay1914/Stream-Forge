
import json

from kafka import KafkaConsumer


class TemperatureConsumer:
    """Consume truck temperature events from Kafka."""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.consumer = KafkaConsumer(
            "temperature-events",
            bootstrap_servers=bootstrap_servers,
            group_id="streamforge-temperature-consumers",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            key_deserializer=lambda key: (
                key.decode("utf-8") if key else None
            ),
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

    def start(self):
        print("Listening for temperature events...")

        try:
            for message in self.consumer:
                event = message.value

                print("\nTemperature event received")
                print(f"Truck ID: {event['truck_id']}")
                print(f"Temperature: {event['temperature']} °C")
                print(f"Timestamp: {event['timestamp']}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")

        except KeyboardInterrupt:
            print("\nConsumer stopped.")

        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = TemperatureConsumer()
    consumer.start()

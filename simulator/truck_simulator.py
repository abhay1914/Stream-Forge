
import random
import time
import uuid
from datetime import datetime, timezone

from backend.app.kafka.producer import TemperatureProducer


TRUCK_IDS = ["TRUCK-101", "TRUCK-102", "TRUCK-103"]

def generate_temperature():
    """Generate a simulated truck temperature in Celsius."""
    return round(random.uniform(2.0, 10.0), 2)


def run_simulator():
    producer = TemperatureProducer()

    print("Starting truck temperature simulator...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            truck_id = random.choice(TRUCK_IDS)

            event = {
                "event_id": str(uuid.uuid4()),
                "truck_id": truck_id,
                "temperature": generate_temperature(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            producer.send_event(event)

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nTruck simulator stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    run_simulator()

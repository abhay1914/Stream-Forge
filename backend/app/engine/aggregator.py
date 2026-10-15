
from collections import defaultdict
from datetime import timedelta
from threading import RLock

from backend.app.schemas.events import TemperatureEvent


class RollingTemperatureAggregator:
    """Calculate rolling temperature averages for each truck."""

    def __init__(self, window_minutes: int = 5):
        if window_minutes <= 0:
            raise ValueError("window_minutes must be positive")

        self.window = timedelta(minutes=window_minutes)
        self._events = defaultdict(dict)
        self._seen_ids = set()
        self._lock = RLock()

    def process(self, event: TemperatureEvent) -> dict:
        with self._lock:
            if event.event_id in self._seen_ids:
                return {
                    "accepted": False,
                    "reason": "duplicate_event",
                    "truck_id": event.truck_id,
                }

            self._seen_ids.add(event.event_id)
            readings = self._events[event.truck_id]

            readings[event.event_id] = event

            cutoff = event.timestamp - self.window

            expired_ids = [
                event_id
                for event_id, reading in readings.items()
                if reading.timestamp < cutoff
            ]

            for event_id in expired_ids:
                del readings[event_id]

            # Sort by event time so out-of-order readings are handled.
            ordered = sorted(
                readings.values(),
                key=lambda reading: reading.timestamp,
            )

            average = sum(
                reading.temperature for reading in ordered
            ) / len(ordered)

            return {
                "accepted": True,
                "truck_id": event.truck_id,
                "event_id": event.event_id,
                "average_temperature": round(average, 2),
                "reading_count": len(ordered),
                "window_minutes": self.window.total_seconds() / 60,
            }

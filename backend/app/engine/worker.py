
from datetime import datetime

from backend.app.storage.repository import EventRepository


class EventWorker:
    """Process pending temperature events stored in SQLite."""

    def __init__(self, repository=None, window_minutes=5):
        self.repository = repository or EventRepository()
        self.window_minutes = window_minutes

    def process_pending_events(self, limit=100):
        """Atomically claim and process a batch of events."""
        events = self.repository.claim_pending_events(limit=limit)

        summary = {
            "found": len(events),
            "processed": 0,
            "failed": 0,
        }

        for event in events:
            event_id = event["event_id"]

            try:
                timestamp = datetime.fromisoformat(event["timestamp"])

                self.repository.get_rolling_average(
                    truck_id=event["truck_id"],
                    event_timestamp=timestamp,
                    window_minutes=self.window_minutes,
                )

                updated = self.repository.update_event_status(
                    event_id, "processed"
                )

                if updated:
                    summary["processed"] += 1
                else:
                    summary["failed"] += 1

            except Exception:
                self.repository.update_event_status(event_id, "failed")
                summary["failed"] += 1

        return summary


from datetime import datetime
from time import perf_counter
from backend.app.engine.metrics import record_batch_metrics
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
            "processing_time_ms": 0.0,
            "average_latency_ms": 0.0,
        }

        total_latency_seconds = 0.0

        for event in events:
            event_id = event["event_id"]
            started_at = perf_counter()

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

            finally:
                total_latency_seconds += perf_counter() - started_at

        summary["processing_time_ms"] = round(
            total_latency_seconds * 1000, 2
        )

        if summary["found"] > 0:
            summary["average_latency_ms"] = round(
                total_latency_seconds * 1000 / summary["found"], 2
            )

        record_batch_metrics(summary)
        return summary

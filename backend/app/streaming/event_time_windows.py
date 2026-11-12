from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.app.storage.state_store import RocksDBStateStore


class EventTimeWindowAggregator:
    """Aggregate temperature events with optional persistent state."""

    STATE_PREFIX = "streamforge:aggregator:"
    WINDOWS_KEY = STATE_PREFIX + "windows"
    WATERMARKS_KEY = STATE_PREFIX + "watermarks"
    SEEN_IDS_KEY = STATE_PREFIX + "seen_ids"

    def __init__(
        self,
        window_minutes: int = 5,
        allowed_lateness_minutes: int = 1,
        state_store: RocksDBStateStore | None = None,
    ):
        if window_minutes <= 0:
            raise ValueError("window_minutes must be positive")

        if allowed_lateness_minutes < 0:
            raise ValueError("allowed_lateness_minutes cannot be negative")

        self.window = timedelta(minutes=window_minutes)
        self.allowed_lateness = timedelta(
            minutes=allowed_lateness_minutes
        )
        self.state_store = state_store

        self._windows = defaultdict(dict)
        self._max_event_time = {}
        self._seen_ids = set()

        if self.state_store is not None:
            self._restore_state()

    @staticmethod
    def _normalize_timestamp(value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)

        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    def _get_window_start(self, timestamp):
        window_seconds = self.window.total_seconds()
        timestamp_seconds = timestamp.timestamp()

        start_seconds = (
            timestamp_seconds // window_seconds
        ) * window_seconds

        return datetime.fromtimestamp(
            start_seconds,
            tz=timezone.utc,
        )

    def _build_window_result(self, truck_id, window_start, events):
        temperatures = [
            event["temperature"] for event in events.values()
        ]

        return {
            "truck_id": truck_id,
            "window_start": window_start.isoformat(),
            "window_end": (
                window_start + self.window
            ).isoformat(),
            "window_minutes": self.window.total_seconds() / 60,
            "reading_count": len(temperatures),
            "average_temperature": round(
                sum(temperatures) / len(temperatures), 2
            ),
            "minimum_temperature": min(temperatures),
            "maximum_temperature": max(temperatures),
        }

    def _restore_state(self) -> None:
        """Restore unfinished windows, watermarks and deduplication IDs."""

        windows = self.state_store.get(self.WINDOWS_KEY) or {}
        watermarks = self.state_store.get(self.WATERMARKS_KEY) or {}
        seen_ids = self.state_store.get(self.SEEN_IDS_KEY) or []

        for truck_id, truck_windows in windows.items():
            for start, events in truck_windows.items():
                window_start = self._normalize_timestamp(start)
                self._windows[truck_id][window_start] = events

        self._max_event_time = {
            truck_id: self._normalize_timestamp(timestamp)
            for truck_id, timestamp in watermarks.items()
        }

        self._seen_ids = set(seen_ids)

    def _persist_state(
        self,
        extra_puts: dict[str, Any] | None = None,
    ) -> None:
        """Atomically persist aggregator state and additional records."""

        if self.state_store is None:
            return

        windows = {
            truck_id: {
                start.isoformat(): events
                for start, events in truck_windows.items()
            }
            for truck_id, truck_windows in self._windows.items()
            if truck_windows
        }

        watermarks = {
            truck_id: timestamp.isoformat()
            for truck_id, timestamp in self._max_event_time.items()
        }

        puts = {
            self.WINDOWS_KEY: windows,
            self.WATERMARKS_KEY: watermarks,
            self.SEEN_IDS_KEY: sorted(self._seen_ids),
        }

        if extra_puts:
            puts.update(extra_puts)

        self.state_store.write_batch(puts=puts)

    def process(
        self,
        event: dict,
        extra_puts_factory: Callable[[dict], dict[str, Any]] | None = None,
    ) -> dict:
        """Accept an event and atomically persist state and extra records."""

        event_id = event["event_id"]
        truck_id = event["truck_id"]

        if event_id in self._seen_ids:
            return {
                "status": "duplicate",
                "finalized_windows": [],
            }

        timestamp = self._normalize_timestamp(event["timestamp"])
        window_start = self._get_window_start(timestamp)
        window_end = window_start + self.window

        previous_max = self._max_event_time.get(truck_id)

        if previous_max is not None:
            watermark = previous_max - self.allowed_lateness

            if window_end <= watermark:
                late_result = {
                    "status": "late",
                    "event_id": event_id,
                    "truck_id": truck_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "watermark": watermark.isoformat(),
                    "finalized_windows": [],
                }

                # Persist late-event deduplication and outbox records
                # together so retries cannot emit the same late event again.
                previous_seen_ids = self._seen_ids.copy()
                self._seen_ids.add(event_id)

                try:
                    extra_puts = (
                        extra_puts_factory(late_result)
                        if extra_puts_factory
                        else None
                    )
                    self._persist_state(extra_puts=extra_puts)
                except Exception:
                    self._seen_ids = previous_seen_ids
                    raise

                return late_result

        # Keep previous state in case persistence fails.
        previous_seen_ids = self._seen_ids.copy()
        previous_max_times = self._max_event_time.copy()
        previous_windows = {
            key: {
                start: events.copy()
                for start, events in truck_windows.items()
            }
            for key, truck_windows in self._windows.items()
        }

        self._seen_ids.add(event_id)

        self._windows[truck_id][window_start] = (
            self._windows[truck_id].get(window_start, {})
        )
        self._windows[truck_id][window_start][event_id] = event

        if previous_max is None or timestamp > previous_max:
            self._max_event_time[truck_id] = timestamp

        watermark = (
            self._max_event_time[truck_id] - self.allowed_lateness
        )

        finalized = []

        for start in sorted(list(self._windows[truck_id])):
            if start + self.window <= watermark:
                events = self._windows[truck_id].pop(start)

                finalized.append(
                    self._build_window_result(
                        truck_id,
                        start,
                        events,
                    )
                )

        try:
            result = {
                "status": "accepted",
                "event_id": event_id,
                "truck_id": truck_id,
                "finalized_windows": finalized,
            }

            extra_puts = (
                extra_puts_factory(result)
                if extra_puts_factory
                else None
            )

            self._persist_state(extra_puts=extra_puts)

        except Exception:
            self._seen_ids = previous_seen_ids
            self._max_event_time = previous_max_times
            self._windows = defaultdict(dict, previous_windows)
            raise

        return result


import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "streamforge.db"


class EventRepository:
    """Persist temperature events in a SQLite database."""

    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self):
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=10,
        )
        connection.row_factory = sqlite3.Row
        return connection

    
    def initialize(self):
        """Create the event table and migrate older databases."""
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS temperature_events (
                    event_id TEXT PRIMARY KEY,
                    truck_id TEXT NOT NULL,
                    temperature REAL NOT NULL,
                    timestamp TEXT NOT NULL
                )
                """
            )

            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(temperature_events)"
                ).fetchall()
            }

            if "status" not in columns:
                connection.execute(
                    """
                    ALTER TABLE temperature_events
                    ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'
                    """
                )

            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(temperature_events)"
                    ).fetchall()
            }

            if "processing_started_at" not in columns:
                connection.execute(
                    """
                    ALTER TABLE temperature_events
                    ADD COLUMN processing_started_at TEXT
                    """
                )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_temperature_events_truck_timestamp
                ON temperature_events (truck_id, timestamp)
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_temperature_events_status_timestamp
                ON temperature_events (status, timestamp)
                """
            )

        
    def get_pending_events(self, limit=100):
        """Fetch pending events in timestamp order."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id, truck_id, temperature, timestamp, status
                FROM temperature_events
                WHERE status = 'pending'
                ORDER BY timestamp ASC, event_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(row) for row in rows]

    def update_event_status(self, event_id, status):
        """Update an event status if the requested transition is valid."""
        allowed_statuses = {"pending", "processing", "processed", "failed"}

        if status not in allowed_statuses:
            raise ValueError(f"Invalid event status: {status}")

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE temperature_events
                SET status = ?
                WHERE event_id = ?
                """,
                (status, event_id),
            )

        return cursor.rowcount == 1


    def claim_pending_events(self, limit=100, recovery_timeout_minutes=10):
        """Atomically claim pending or abandoned processing events."""
        if limit <= 0:
            return []

        now = datetime.now(timezone.utc)
        now_text = now.isoformat()
        stale_before = (
            now - timedelta(minutes=recovery_timeout_minutes)
        ).isoformat()

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")

            rows = connection.execute(
                """
                SELECT event_id, truck_id, temperature, timestamp, status
                FROM temperature_events
                WHERE status = 'pending'
                   OR (
                        status = 'processing'
                        AND (
                            processing_started_at IS NULL
                            OR processing_started_at <= ?
                        )
                   )
                ORDER BY timestamp ASC, event_id ASC
                LIMIT ?
                """,
                (stale_before, limit),
            ).fetchall()

            event_ids = [row["event_id"] for row in rows]

            if event_ids:
                placeholders = ",".join("?" for _ in event_ids)
                connection.execute(
                    f"""
                    UPDATE temperature_events
                    SET status = 'processing',
                        processing_started_at = ?
                    WHERE event_id IN ({placeholders})
                      AND (
                          status = 'pending'
                          OR (
                              status = 'processing'
                              AND (
                                  processing_started_at IS NULL
                                  OR processing_started_at <= ?
                              )
                          )
                      )
                    """,
                    [now_text, *event_ids, stale_before],
                )

            claimed_events = [dict(row) for row in rows]

        return claimed_events



    @staticmethod
    def _normalize_timestamp(timestamp):
        """Convert timestamps to UTC for consistent comparisons."""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        return timestamp.astimezone(timezone.utc)
    def add_event(self, event):
        """Store an event, returning False if its ID already exists."""
        timestamp = self._normalize_timestamp(event.timestamp)
        timestamp_text = timestamp.isoformat()

        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO temperature_events (
                        event_id,
                        truck_id,
                        temperature,
                        timestamp
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.truck_id,
                        event.temperature,
                        timestamp_text,
                    ),
                )
            return True

        except sqlite3.IntegrityError:
            # The primary key prevents duplicate event IDs.
            return False

    def get_rolling_average(
        self,
        truck_id,
        event_timestamp,
        window_minutes=5,
    ):
        """Calculate an average around the supplied event time."""
        event_timestamp = self._normalize_timestamp(event_timestamp)
        cutoff = event_timestamp - timedelta(minutes=window_minutes)

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    AVG(temperature) AS average_temperature,
                    COUNT(*) AS reading_count
                FROM temperature_events
                WHERE truck_id = ?
                  AND timestamp >= ?
                  AND timestamp <= ?
                """,
                (
                    truck_id,
                    cutoff.isoformat(),
                    event_timestamp.isoformat(),
                ),
            ).fetchone()

        return {
            "average_temperature": (
                round(row["average_temperature"], 2)
                if row["average_temperature"] is not None
                else None
            ),
            "reading_count": row["reading_count"],
        }

    def get_latest_timestamp(self, truck_id):
        """Return the newest stored event timestamp for a truck."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT MAX(timestamp) AS latest_timestamp
                FROM temperature_events
                WHERE truck_id = ?
                """,
                (truck_id,),
            ).fetchone()

        if row["latest_timestamp"] is None:
            return None

        return datetime.fromisoformat(row["latest_timestamp"])

    def get_event_count(self):
        """Return the number of stored events."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM temperature_events"
            ).fetchone()

        return row["total"]

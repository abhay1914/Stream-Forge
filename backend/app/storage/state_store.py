"""Durable key-value state storage backed by RocksDB."""
import json
from pathlib import Path
from typing import Any, Iterable

from rocksdict import Options, Rdict, WriteBatch, WriteOptions


class RocksDBStateStore:
    """Persistent JSON state store with synchronous batch writes."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        options = Options()
        options.create_if_missing(True)

        self._db = Rdict(str(self.db_path), options)
        self._write_options = WriteOptions()
        self._write_options.sync = True
        self._closed = False

    @staticmethod
    def _encode(value: Any) -> bytes:
        return json.dumps(
            value,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    @staticmethod
    def _decode(value: bytes | None) -> Any:
        if value is None:
            return None
        return json.loads(value.decode("utf-8"))

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("State store is closed")

    def get(self, key: str) -> Any:
        self._ensure_open()
        value = self._db.get(key.encode("utf-8"))
        return self._decode(value)

    def put(self, key: str, value: Any) -> None:
        self._ensure_open()
        self._db.put(
            key.encode("utf-8"),
            self._encode(value),
            self._write_options,
        )

    def delete(self, key: str) -> None:
        self._ensure_open()
        self._db.delete(key.encode("utf-8"), self._write_options)

    def write_batch(
        self,
        puts: dict[str, Any] | None = None,
        deletes: Iterable[str] | None = None,
    ) -> None:
        """Apply multiple puts and deletes atomically within RocksDB."""
        self._ensure_open()

        batch = WriteBatch()

        for key, value in (puts or {}).items():
            batch.put(key.encode("utf-8"), self._encode(value))

        for key in deletes or ():
            batch.delete(key.encode("utf-8"))

        if not batch.is_empty():
            self._db.write(batch, self._write_options)

    def close(self) -> None:
        if not self._closed:
            self._db.close()
            self._closed = True

    def __enter__(self) -> "RocksDBStateStore":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

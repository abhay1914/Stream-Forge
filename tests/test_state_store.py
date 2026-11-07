import pytest

from backend.app.storage.state_store import RocksDBStateStore


def test_put_and_get(tmp_path):
    store = RocksDBStateStore(tmp_path / "state")

    store.put("truck:001", {"temperature": 25.5})

    assert store.get("truck:001") == {"temperature": 25.5}

    store.close()


def test_state_survives_restart(tmp_path):
    db_path = tmp_path / "state"

    store = RocksDBStateStore(db_path)
    store.put("event:001", {"status": "processed"})
    store.close()

    reopened = RocksDBStateStore(db_path)

    assert reopened.get("event:001") == {"status": "processed"}

    reopened.close()


def test_batch_put_and_delete(tmp_path):
    store = RocksDBStateStore(tmp_path / "state")

    store.put("old:event", {"status": "old"})

    store.write_batch(
        puts={
            "truck:001": {"temperature": 22.5},
            "event:002": {"status": "processed"},
        },
        deletes=["old:event"],
    )

    assert store.get("truck:001") == {"temperature": 22.5}
    assert store.get("event:002") == {"status": "processed"}
    assert store.get("old:event") is None

    store.close()


def test_missing_key_returns_none(tmp_path):
    store = RocksDBStateStore(tmp_path / "state")

    assert store.get("missing:key") is None

    store.close()


def test_operations_after_close_raise_error(tmp_path):
    store = RocksDBStateStore(tmp_path / "state")
    store.close()

    with pytest.raises(RuntimeError, match="State store is closed"):
        store.get("truck:001")

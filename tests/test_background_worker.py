
import asyncio

from backend.app.main import background_worker


def test_background_worker_processes_repeatedly(monkeypatch):
    calls = []
    sleep_count = 0

    class FakeRepository:
        pass

    class FakeWorker:
        def __init__(self, repository):
            pass

        def process_pending_events(self, limit=100):
            calls.append(limit)
            return {"found": 0, "processed": 0, "failed": 0}

    async def fake_sleep(seconds):
        nonlocal sleep_count
        sleep_count += 1

        if sleep_count >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(
        "backend.app.main.EventRepository",
        FakeRepository,
    )
    monkeypatch.setattr(
        "backend.app.main.EventWorker",
        FakeWorker,
    )
    monkeypatch.setattr(
        "backend.app.main.asyncio.sleep",
        fake_sleep,
    )

    try:
        asyncio.run(background_worker())
    except asyncio.CancelledError:
        pass

    assert calls == [100, 100]

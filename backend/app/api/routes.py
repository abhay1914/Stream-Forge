
from fastapi import APIRouter

from backend.app.schemas.events import TemperatureEvent
from backend.app.storage.repository import EventRepository
from backend.app.engine.worker import EventWorker
from backend.app.engine.metrics import get_batch_metrics


router = APIRouter(
    prefix="/api/v1",
    tags=["Temperature Events"],
)

repository = EventRepository()
WINDOW_MINUTES = 5

@router.post("/events")
def submit_event(event: TemperatureEvent):
    accepted = repository.add_event(event)

    if not accepted:
        return {
            "accepted": False,
            "reason": "duplicate_event",
            "truck_id": event.truck_id,
        }

    result = repository.get_rolling_average(
        truck_id=event.truck_id,
        event_timestamp=event.timestamp,
        window_minutes=WINDOW_MINUTES,
    )

    return {
        "accepted": True,
        "truck_id": event.truck_id,
        "event_id": event.event_id,
        "average_temperature": result["average_temperature"],
        "reading_count": result["reading_count"],
        "window_minutes": WINDOW_MINUTES,
    }


@router.get("/trucks/{truck_id}/temperature")
def get_truck_temperature(truck_id: str):
    latest_timestamp = repository.get_latest_timestamp(truck_id)

    if latest_timestamp is None:
        return {
            "truck_id": truck_id,
            "message": "No temperature events found for this truck",
        }

    result = repository.get_rolling_average(
        truck_id=truck_id,
        event_timestamp=latest_timestamp,
        window_minutes=WINDOW_MINUTES,
    )

    return {
        "truck_id": truck_id,
        "average_temperature": result["average_temperature"],
        "reading_count": result["reading_count"],
        "window_minutes": WINDOW_MINUTES,
    }


@router.post("/worker/process")
def process_pending_events(limit: int = 100):
    """Process a batch of pending temperature events."""
    if limit < 1 or limit > 1000:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=422,
            detail="limit must be between 1 and 1000",
        )

    worker = EventWorker(
        repository=repository,
        window_minutes=WINDOW_MINUTES,
    )

    return worker.process_pending_events(limit=limit)



@router.get("/worker/stats")
def get_worker_stats():
    """Return event counts and the latest worker batch metrics."""
    stats = repository.get_event_status_counts()
    batch_metrics = get_batch_metrics()

    stats["latest_batch"] = batch_metrics
    return stats

from fastapi import APIRouter

from backend.app.engine.aggregator import RollingTemperatureAggregator
from backend.app.schemas.events import TemperatureEvent

router = APIRouter(prefix="/api/v1", tags=["Temperature Events"])

# Initial in-memory implementation for the prototype.
aggregator = RollingTemperatureAggregator()
latest_results = {}


@router.post("/events")
def submit_event(event: TemperatureEvent):
    result = aggregator.process(event)

    if not result["accepted"]:
        return result

    latest_results[event.truck_id] = result
    return result


@router.get("/trucks/{truck_id}/temperature")
def get_truck_temperature(truck_id: str):
    result = latest_results.get(truck_id)

    if result is None:
        return {
            "truck_id": truck_id,
            "message": "No temperature events found for this truck",
        }

    return result
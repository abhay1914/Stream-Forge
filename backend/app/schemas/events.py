
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class TemperatureEvent(BaseModel):
    event_id: str = Field(min_length=1)
    truck_id: str = Field(min_length=1)
    temperature: float = Field(ge=-80, le=100)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

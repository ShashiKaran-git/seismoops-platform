from datetime import datetime

from pydantic import BaseModel, Field


class EarthquakeEvent(BaseModel):
    event_id: str = Field(min_length=1)
    magnitude: float | None = Field(default=None, ge=-10, le=15)
    place: str | None = None

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    depth_km: float = Field(ge=0)

    timestamp: datetime
    source: str = "USGS"
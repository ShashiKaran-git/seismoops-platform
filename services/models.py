from datetime import datetime

from pydantic import BaseModel, Field


class EarthquakeEvent(BaseModel):
    event_id: str = Field(min_length=1)
    magnitude: float | None = Field(default=None, ge=-10, le=15)
    place: str | None = None

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    # USGS allows negative depths in its earthquake catalog.
    # We allow the documented catalog range of -100 km to 1000 km.
    depth_km: float = Field(ge=-100, le=1000)

    timestamp: datetime
    source: str = "USGS"
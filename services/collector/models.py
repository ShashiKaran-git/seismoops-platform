from dataclasses import dataclass
from datetime import datetime


@dataclass
class EarthquakeEvent:
    event_id: str
    magnitude: float | None
    place: str | None
    latitude: float
    longitude: float
    depth_km: float
    timestamp: datetime
    source: str = "USGS"
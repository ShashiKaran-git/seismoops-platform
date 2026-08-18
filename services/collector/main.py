import requests
from datetime import datetime, timezone

from models import EarthquakeEvent


USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def fetch_earthquakes():
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()

    return response.json()


def parse_earthquake(feature):
    properties = feature["properties"]
    coordinates = feature["geometry"]["coordinates"]

    timestamp = datetime.fromtimestamp(
        properties["time"] / 1000,
        tz=timezone.utc,
    )

    return EarthquakeEvent(
        event_id=feature["id"],
        magnitude=properties["mag"],
        place=properties["place"],
        latitude=coordinates[1],
        longitude=coordinates[0],
        depth_km=coordinates[2],
        timestamp=timestamp,
    )


if __name__ == "__main__":
    data = fetch_earthquakes()

    events = [
        parse_earthquake(feature)
        for feature in data["features"][:5]
    ]

    for event in events:
        print(event)
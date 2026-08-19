import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from models import EarthquakeEvent
from redis_client import create_redis_client, publish_earthquake


USGS_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def fetch_earthquakes():
    import requests

    logger.info("Fetching earthquake data from USGS")

    try:
        response = requests.get(USGS_URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        logger.info(
            "Successfully fetched earthquake data | events=%d",
            len(data.get("features", [])),
        )

        return data

    except requests.exceptions.Timeout:
        logger.error("USGS request timed out")
        return None

    except requests.exceptions.RequestException as error:
        logger.error("USGS request failed | error=%s", error)
        return None

    except ValueError:
        logger.error("USGS returned invalid JSON")
        return None


def parse_earthquake(feature):
    try:
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

    except ValidationError as error:
        logger.warning(
            "Earthquake validation failed | event_id=%s | errors=%s",
            feature.get("id"),
            error.errors(),
        )
        return None

    except (KeyError, TypeError, IndexError, ValueError) as error:
        logger.warning(
            "Invalid earthquake data | event_id=%s | error=%s",
            feature.get("id"),
            error,
        )
        return None


def main():
    data = fetch_earthquakes()

    if data is None:
        logger.error("Collector stopped because USGS data could not be fetched")
        raise SystemExit(1)

    redis_client = create_redis_client()

    if redis_client is None:
        logger.error("Collector stopped because Redis is unavailable")
        raise SystemExit(1)

    published_count = 0

    for feature in data.get("features", [])[:5]:
        event = parse_earthquake(feature)

        if event is None:
            continue

        if publish_earthquake(redis_client, event):
            published_count += 1

    logger.info(
        "Collector completed | events_published=%d",
        published_count,
    )


if __name__ == "__main__":
    main()
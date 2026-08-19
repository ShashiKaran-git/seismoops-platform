import logging

from models import EarthquakeEvent
from redis_client import create_redis_client, publish_earthquake


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


if __name__ == "__main__":
    redis_client = create_redis_client()

    if redis_client is None:
        raise SystemExit(1)

    test_event = EarthquakeEvent(
        event_id="test-event-001",
        magnitude=5.2,
        place="Test Location",
        latitude=17.385,
        longitude=78.486,
        depth_km=10.0,
        timestamp="2026-08-19T11:00:00Z",
    )

    success = publish_earthquake(
        redis_client,
        test_event,
    )

    if not success:
        raise SystemExit(1)

    print("Earthquake event published successfully")
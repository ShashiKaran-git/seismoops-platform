import json
import redis
from services.collector.redis_client import (
    EARTHQUAKE_STREAM,
    PROCESSED_EVENTS,
    publish_earthquake,
)
from services.models import EarthquakeEvent


def test_publish_earthquake_success(redis_client):
    event = EarthquakeEvent(
        event_id="test-publisher-001",
        magnitude=5.2,
        place="Test Location",
        latitude=17.385,
        longitude=78.486,
        depth_km=10.0,
        timestamp="2026-08-19T11:00:00Z",
    )

    result = publish_earthquake(
        redis_client,
        event,
    )

    assert result is True

    assert redis_client.sismember(
        PROCESSED_EVENTS,
        event.event_id,
    )

    messages = redis_client.xrange(
        EARTHQUAKE_STREAM,
        "-",
        "+",
    )

    matching_messages = []

    for _, data in messages:
        if data.get("event_id") == event.event_id:
            matching_messages.append(data)

    assert len(matching_messages) == 1

    stored_event = json.loads(
        matching_messages[0]["event_data"]
    )

    assert stored_event["event_id"] == event.event_id
    assert stored_event["magnitude"] == event.magnitude
    assert stored_event["place"] == event.place

def test_publish_earthquake_duplicate(redis_client):
    event = EarthquakeEvent(
        event_id="test-publisher-duplicate-001",
        magnitude=4.8,
        place="Duplicate Test Location",
        latitude=17.385,
        longitude=78.486,
        depth_km=12.0,
        timestamp="2026-08-19T12:00:00Z",
    )

    first_result = publish_earthquake(
        redis_client,
        event,
    )

    second_result = publish_earthquake(
        redis_client,
        event,
    )

    assert first_result is True
    assert second_result is False

    messages = redis_client.xrange(
        EARTHQUAKE_STREAM,
        "-",
        "+",
    )

    matching_messages = []

    for _, data in messages:
        if data.get("event_id") == event.event_id:
            matching_messages.append(data)

    assert len(matching_messages) == 1

def test_publish_earthquake_redis_failure():
    event = EarthquakeEvent(
        event_id="test-publisher-failure-001",
        magnitude=4.5,
        place="Redis Failure Test Location",
        latitude=17.385,
        longitude=78.486,
        depth_km=15.0,
        timestamp="2026-08-19T13:00:00Z",
    )

    class FailingRedisClient:
        def eval(self, *args, **kwargs):
            raise redis.RedisError("Redis connection failed")

    client = FailingRedisClient()

    result = publish_earthquake(
        client,
        event,
    )

    assert result is False
import json
import logging

import redis

from services.models import EarthquakeEvent


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

EARTHQUAKE_STREAM = "seismoops:earthquake-stream"
PROCESSED_EVENTS = "seismoops:processed_events"


logger = logging.getLogger(__name__)


def create_redis_client():
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )

    try:
        client.ping()
        logger.info("Successfully connected to Redis")
        return client

    except redis.RedisError as error:
        logger.error("Redis connection failed | error=%s", error)
        return None


def publish_earthquake(client, event: EarthquakeEvent):
    event_data = event.model_dump(mode="json")

    script = """
    if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then
        return 0
    end

    local stream_id = redis.call(
        "XADD",
        KEYS[1],
        "*",
        "event_id",
        ARGV[1],
        "event_data",
        ARGV[2]
    )

    redis.call("SADD", KEYS[2], ARGV[1])

    return stream_id
    """

    try:
        result = client.eval(
            script,
            2,
            EARTHQUAKE_STREAM,
            PROCESSED_EVENTS,
            event.event_id,
            json.dumps(event_data),
        )

        if result == 0:
            logger.info(
                "Duplicate earthquake skipped | event_id=%s",
                event.event_id,
            )
            return False

        logger.info(
            "Published earthquake event to Redis Stream | event_id=%s | stream_id=%s",
            event.event_id,
            result,
        )

        return True

    except redis.RedisError as error:
        logger.error(
            "Failed to publish earthquake event | event_id=%s | error=%s",
            event.event_id,
            error,
        )
        return False
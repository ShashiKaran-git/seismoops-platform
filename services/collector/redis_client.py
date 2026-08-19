import json
import logging

import redis

from models import EarthquakeEvent


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

EARTHQUAKE_QUEUE = "seismoops:earthquakes"


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
    try:
        event_data = event.model_dump(mode="json")

        client.rpush(
            EARTHQUAKE_QUEUE,
            json.dumps(event_data),
        )

        logger.info(
            "Published earthquake event to Redis | event_id=%s",
            event.event_id,
        )

        return True

    except redis.RedisError as error:
        logger.error(
            "Failed to publish earthquake event | event_id=%s | error=%s",
            event.event_id,
            error,
        )
        return False
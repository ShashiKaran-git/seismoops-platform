import json
import logging

import redis
from pydantic import ValidationError

from services.models import EarthquakeEvent


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

EARTHQUAKE_STREAM = "seismoops:earthquake-stream"
CONSUMER_GROUP = "seismoops-processors"
CONSUMER_NAME = "seismoops-processor-1"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

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


def consume_earthquake(client):
    try:
        messages = client.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            streams={
                EARTHQUAKE_STREAM: ">"
            },
            count=1,
            block=1000,
        )

        if not messages:
            return None

        for stream_name, stream_messages in messages:
            for message_id, fields in stream_messages:

                try:
                    event_data = fields.get("event_data")

                    if event_data is None:
                        logger.error(
                            "Missing event_data field | stream_id=%s",
                            message_id,
                        )
                        return None

                    data = json.loads(event_data)

                    event = EarthquakeEvent.model_validate(data)

                    logger.info(
                        "Consumed earthquake event | "
                        "stream_id=%s | event_id=%s | magnitude=%s | place=%s",
                        message_id,
                        event.event_id,
                        event.magnitude,
                        event.place,
                    )

                    client.xack(
                        EARTHQUAKE_STREAM,
                        CONSUMER_GROUP,
                        message_id,
                    )

                    logger.info(
                        "Acknowledged earthquake event | "
                        "stream_id=%s | event_id=%s",
                        message_id,
                        event.event_id,
                    )

                    return event

                except json.JSONDecodeError as error:
                    logger.error(
                        "Invalid JSON in stream message | "
                        "stream_id=%s | error=%s",
                        message_id,
                        error,
                    )
                    return None

                except ValidationError as error:
                    logger.error(
                        "Earthquake validation failed during processing | "
                        "stream_id=%s | errors=%s",
                        message_id,
                        error.errors(),
                    )
                    return None

        return None

    except redis.RedisError as error:
        logger.error(
            "Failed to consume earthquake stream | error=%s",
            error,
        )
        return None


def main():
    redis_client = create_redis_client()

    if redis_client is None:
        raise SystemExit(1)

    event = consume_earthquake(redis_client)

    if event is None:
        logger.info("No new earthquake events available")
        return

    logger.info(
        "Processor successfully handled event | event_id=%s",
        event.event_id,
    )


if __name__ == "__main__":
    main()
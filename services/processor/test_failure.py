import json
import logging

import redis


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

EARTHQUAKE_STREAM = "seismoops:earthquake-stream"
CONSUMER_GROUP = "seismoops-processors"
CONSUMER_NAME = "failure-test-consumer"


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
        socket_timeout=None,
    )

    client.ping()
    logger.info("Successfully connected to Redis")

    return client


def main():
    client = create_redis_client()

    try:
        messages = client.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            streams={
                EARTHQUAKE_STREAM: ">"
            },
            count=1,
            block=5000,
        )

        if not messages:
            logger.info("No new earthquake events available")
            return

        for stream_name, stream_messages in messages:
            for message_id, fields in stream_messages:

                event_data = fields.get("event_data")

                logger.info(
                    "Received event without acknowledging it | "
                    "stream_id=%s",
                    message_id,
                )

                if event_data:
                    data = json.loads(event_data)

                    logger.info(
                        "Event received | event_id=%s | magnitude=%s",
                        data.get("event_id"),
                        data.get("magnitude"),
                    )

                logger.error(
                    "Simulating processor failure before XACK | "
                    "stream_id=%s",
                    message_id,
                )

                raise RuntimeError(
                    "Simulated processing failure"
                )

    finally:
        client.close()
        logger.info("Failure test stopped")


if __name__ == "__main__":
    main()
import json
import logging

import redis
from pydantic import ValidationError

from services.database.postgres import (
    create_postgres_connection,
    save_earthquake_event,
)
from services.models import EarthquakeEvent
from services.observability.metrics import (
    PROCESSOR_DLQ_MESSAGES,
    PROCESSOR_FAILURES,
    PROCESSOR_MESSAGES_PROCESSED,
    PROCESSOR_RECOVERED_MESSAGES,
    PROCESSOR_RETRIES,
)


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

EARTHQUAKE_STREAM = "seismoops:earthquake-stream"
CONSUMER_GROUP = "seismoops-processors"
CONSUMER_NAME = "seismoops-processor-1"

RECOVERY_IDLE_TIME_MS = 30_000
RECOVERY_BATCH_SIZE = 10

MAX_RETRIES = 3
RETRY_KEY_PREFIX = "seismoops:retry:"
DEAD_LETTER_STREAM = "seismoops:earthquake-dlq"


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

    try:
        client.ping()
        logger.info("Successfully connected to Redis")
        return client

    except redis.RedisError as error:
        logger.error(
            "Redis connection failed | error=%s",
            error,
        )
        return None


def increment_retry_count(client, message_id):
    retry_key = f"{RETRY_KEY_PREFIX}{message_id}"

    try:
        retry_count = client.incr(retry_key)

        client.expire(
            retry_key,
            86_400,
        )

        PROCESSOR_RETRIES.inc()

        logger.warning(
            "Incremented retry count | stream_id=%s | retry=%s/%s",
            message_id,
            retry_count,
            MAX_RETRIES,
        )

        return retry_count

    except redis.RedisError as error:
        logger.error(
            "Failed to update retry count | "
            "stream_id=%s | error=%s",
            message_id,
            error,
        )

        return None


def move_to_dead_letter_queue(
    client,
    message_id,
    fields,
    retry_count,
):
    try:
        dlq_fields = dict(fields)

        dlq_fields["original_stream_id"] = message_id
        dlq_fields["retry_count"] = str(retry_count)

        client.xadd(
            DEAD_LETTER_STREAM,
            dlq_fields,
        )

        client.xack(
            EARTHQUAKE_STREAM,
            CONSUMER_GROUP,
            message_id,
        )

        client.delete(
            f"{RETRY_KEY_PREFIX}{message_id}"
        )

        PROCESSOR_DLQ_MESSAGES.inc()

        logger.error(
            "Moved earthquake event to dead-letter queue | "
            "stream_id=%s | retries=%s",
            message_id,
            retry_count,
        )

        return True

    except redis.RedisError as error:
        logger.error(
            "Failed to move event to dead-letter queue | "
            "stream_id=%s | error=%s",
            message_id,
            error,
        )

        return False


def process_message(
    client,
    postgres_connection,
    message_id,
    fields,
):
    try:
        event_data = fields.get("event_data")

        if event_data is None:
            logger.error(
                "Missing event_data field | stream_id=%s",
                message_id,
            )
            raise ValueError("Missing event_data field")

        data = json.loads(event_data)

        event = EarthquakeEvent.model_validate(data)

        logger.info(
            "Processed earthquake event | "
            "stream_id=%s | event_id=%s | magnitude=%s | place=%s",
            message_id,
            event.event_id,
            event.magnitude,
            event.place,
        )

        if not save_earthquake_event(
            postgres_connection,
            event,
        ):
            raise RuntimeError(
                f"Failed to persist earthquake event | "
                f"event_id={event.event_id}"
            )

        client.xack(
            EARTHQUAKE_STREAM,
            CONSUMER_GROUP,
            message_id,
        )

        PROCESSOR_MESSAGES_PROCESSED.inc()

        logger.info(
            "Acknowledged earthquake event | "
            "stream_id=%s | event_id=%s",
            message_id,
            event.event_id,
        )

        return True

    except (
        json.JSONDecodeError,
        ValidationError,
        ValueError,
        RuntimeError,
    ) as error:

        PROCESSOR_FAILURES.inc()

        logger.error(
            "Message processing failed | "
            "stream_id=%s | error=%s",
            message_id,
            error,
        )

        retry_count = increment_retry_count(
            client,
            message_id,
        )

        if retry_count is not None and retry_count >= MAX_RETRIES:
            move_to_dead_letter_queue(
                client,
                message_id,
                fields,
                retry_count,
            )

        return False

    except redis.RedisError as error:

        PROCESSOR_FAILURES.inc()

        logger.error(
            "Redis error while processing message | "
            "stream_id=%s | error=%s",
            message_id,
            error,
        )

        retry_count = increment_retry_count(
            client,
            message_id,
        )

        if retry_count is not None and retry_count >= MAX_RETRIES:
            move_to_dead_letter_queue(
                client,
                message_id,
                fields,
                retry_count,
            )

        return False


def recover_pending_messages(
    client,
    postgres_connection,
):
    try:
        result = client.xautoclaim(
            name=EARTHQUAKE_STREAM,
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            min_idle_time=RECOVERY_IDLE_TIME_MS,
            start_id="0-0",
            count=RECOVERY_BATCH_SIZE,
        )

        next_start_id, messages, deleted_ids = result

        if not messages:
            return 0

        recovered_count = 0

        for message_id, fields in messages:
            PROCESSOR_RECOVERED_MESSAGES.inc()

            logger.info(
                "Recovered pending earthquake event | "
                "stream_id=%s",
                message_id,
            )

            if process_message(
                client,
                postgres_connection,
                message_id,
                fields,
            ):
                recovered_count += 1

        logger.info(
            "Pending message recovery completed | recovered=%s",
            recovered_count,
        )

        return recovered_count

    except redis.RedisError as error:
        logger.error(
            "Failed to recover pending messages | error=%s",
            error,
        )

        return 0


def consume_new_messages(
    client,
    postgres_connection,
):
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
            return 0

        processed_count = 0

        for stream_name, stream_messages in messages:
            for message_id, fields in stream_messages:

                if process_message(
                    client,
                    postgres_connection,
                    message_id,
                    fields,
                ):
                    processed_count += 1

        return processed_count

    except redis.RedisError as error:
        logger.error(
            "Failed to consume earthquake stream | error=%s",
            error,
        )

        return 0


def main():
    redis_client = create_redis_client()

    if redis_client is None:
        raise SystemExit(1)

    postgres_connection = create_postgres_connection()

    if postgres_connection is None:
        logger.error(
            "Unable to start processor without PostgreSQL"
        )
        redis_client.close()
        raise SystemExit(1)

    logger.info(
        "Starting earthquake processor | "
        "stream=%s | group=%s | consumer=%s",
        EARTHQUAKE_STREAM,
        CONSUMER_GROUP,
        CONSUMER_NAME,
    )

    try:
        while True:
            recover_pending_messages(
                redis_client,
                postgres_connection,
            )

            consume_new_messages(
                redis_client,
                postgres_connection,
            )

    except KeyboardInterrupt:
        logger.info("Processor shutdown requested")

    finally:
        postgres_connection.close()
        redis_client.close()

        logger.info("Processor stopped")


if __name__ == "__main__":
    main()
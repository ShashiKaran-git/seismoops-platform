import pytest

from services.collector.redis_client import (
    EARTHQUAKE_STREAM,
    PROCESSED_EVENTS,
    create_redis_client,
)
from services.processor.main import (
    DEAD_LETTER_STREAM,
    RETRY_KEY_PREFIX,
)


@pytest.fixture
def redis_client():
    client = create_redis_client()

    if client is None:
        pytest.fail("Unable to connect to Redis")

    retry_keys = client.keys(
        f"{RETRY_KEY_PREFIX}*"
    )

    client.delete(
        EARTHQUAKE_STREAM,
        PROCESSED_EVENTS,
        DEAD_LETTER_STREAM,
        *retry_keys,
    )

    yield client

    retry_keys = client.keys(
        f"{RETRY_KEY_PREFIX}*"
    )

    client.delete(
        EARTHQUAKE_STREAM,
        PROCESSED_EVENTS,
        DEAD_LETTER_STREAM,
        *retry_keys,
    )
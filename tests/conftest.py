import pytest

from services.collector.redis_client import (
    EARTHQUAKE_STREAM,
    PROCESSED_EVENTS,
    create_redis_client,
)

@pytest.fixture
def redis_client():
    client = create_redis_client()

    if client is None:
        pytest.fail("Unable to connect to Redis")

    client.delete(
        EARTHQUAKE_STREAM,
        PROCESSED_EVENTS,
    )

    yield client

    client.delete(
        EARTHQUAKE_STREAM,
        PROCESSED_EVENTS,
    )


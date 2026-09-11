import json
from unittest.mock import Mock, patch

from services.observability.metrics import (
    PROCESSOR_DLQ_MESSAGES,
    PROCESSOR_FAILURES,
    PROCESSOR_MESSAGES_PROCESSED,
    PROCESSOR_RECOVERED_MESSAGES,
    PROCESSOR_RETRIES,
)
from services.processor.main import (
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    EARTHQUAKE_STREAM,
    RETRY_KEY_PREFIX,
    consume_new_messages,
    increment_retry_count,
    move_to_dead_letter_queue,
    process_message,
    recover_pending_messages,
)

def test_process_message_increments_processed_metric():
    before = PROCESSOR_MESSAGES_PROCESSED._value.get()

    client = Mock()
    postgres_connection = Mock()

    event_data = json.dumps(
        {
            "event_id": "processed-metric-test",
            "magnitude": 2.5,
            "place": "Processed Metric Test",
            "latitude": 10.0,
            "longitude": 20.0,
            "depth_km": 5.0,
            "timestamp": "2026-09-10T10:00:00Z",
            "source": "USGS",
        }
    )

    with patch(
        "services.processor.main.save_earthquake_event",
        return_value=True,
    ):
        result = process_message(
            client,
            postgres_connection,
            "1-0",
            {
                "event_id": "processed-metric-test",
                "event_data": event_data,
            },
        )

    after = PROCESSOR_MESSAGES_PROCESSED._value.get()

    assert result is True
    assert after - before == 1
    client.xack.assert_called_once()


def test_increment_retry_count(redis_client):
    message_id = "1003-0"

    retry_count = increment_retry_count(
        redis_client,
        message_id,
    )

    assert retry_count == 1

    retry_key = f"{RETRY_KEY_PREFIX}{message_id}"

    assert redis_client.get(retry_key) == "1"

    ttl = redis_client.ttl(retry_key)

    assert ttl > 0
    assert ttl <= 86_400


@patch(
    "services.processor.main.save_earthquake_event",
    return_value=True,
)
def test_process_message_success(mock_save):
    redis_client = Mock()
    postgres_connection = Mock()

    message_id = "1000-0"

    event_data = {
        "event_id": "test-processor-001",
        "magnitude": 5.2,
        "place": "Test Location",
        "latitude": 17.385,
        "longitude": 78.486,
        "depth_km": 10.0,
        "timestamp": "2026-08-19T11:00:00Z",
        "source": "USGS",
    }

    fields = {
        "event_id": event_data["event_id"],
        "event_data": json.dumps(event_data),
    }

    result = process_message(
        redis_client,
        postgres_connection,
        message_id,
        fields,
    )

    assert result is True

    mock_save.assert_called_once()

    redis_client.xack.assert_called_once_with(
        EARTHQUAKE_STREAM,
        CONSUMER_GROUP,
        message_id,
    )

@patch(
    "services.processor.main.save_earthquake_event",
    return_value=False,
)
@patch(
    "services.processor.main.increment_retry_count",
    return_value=1,
)
def test_process_message_failure_increments_retry(
    mock_increment_retry,
    mock_save,
):
    redis_client = Mock()
    postgres_connection = Mock()

    message_id = "1001-0"

    event_data = {
        "event_id": "test-processor-failure-001",
        "magnitude": 4.5,
        "place": "Processor Failure Test",
        "latitude": 17.385,
        "longitude": 78.486,
        "depth_km": 15.0,
        "timestamp": "2026-08-19T12:00:00Z",
        "source": "USGS",
    }

    fields = {
        "event_id": event_data["event_id"],
        "event_data": json.dumps(event_data),
    }

    result = process_message(
        redis_client,
        postgres_connection,
        message_id,
        fields,
    )

    assert result is False

    mock_save.assert_called_once()

    mock_increment_retry.assert_called_once_with(
        redis_client,
        message_id,
    )

    redis_client.xack.assert_not_called()

@patch(
    "services.processor.main.save_earthquake_event",
    return_value=False,
)
@patch(
    "services.processor.main.increment_retry_count",
    return_value=3,
)
@patch(
    "services.processor.main.move_to_dead_letter_queue",
    return_value=True,
)
def test_process_message_moves_to_dlq_after_max_retries(
    mock_move_to_dlq,
    mock_increment_retry,
    mock_save,
):
    redis_client = Mock()
    postgres_connection = Mock()

    message_id = "1002-0"

    event_data = {
        "event_id": "test-processor-dlq-001",
        "magnitude": 6.0,
        "place": "Processor DLQ Test",
        "latitude": 17.385,
        "longitude": 78.486,
        "depth_km": 20.0,
        "timestamp": "2026-08-19T13:00:00Z",
        "source": "USGS",
    }

    fields = {
        "event_id": event_data["event_id"],
        "event_data": json.dumps(event_data),
    }

    result = process_message(
        redis_client,
        postgres_connection,
        message_id,
        fields,
    )

    assert result is False

    mock_save.assert_called_once()

    mock_increment_retry.assert_called_once_with(
        redis_client,
        message_id,
    )

    mock_move_to_dlq.assert_called_once_with(
        redis_client,
        message_id,
        fields,
        3,
    )

def test_move_to_dead_letter_queue(redis_client):
    message_id = "1004-0"

    fields = {
        "event_id": "test-dlq-001",
        "event_data": json.dumps(
            {
                "event_id": "test-dlq-001",
                "magnitude": 6.5,
                "place": "DLQ Test Location",
                "latitude": 17.385,
                "longitude": 78.486,
                "depth_km": 25.0,
                "timestamp": "2026-08-19T14:00:00Z",
                "source": "USGS",
            }
        ),
    }

    retry_count = 3

    retry_key = f"{RETRY_KEY_PREFIX}{message_id}"

    redis_client.set(
        retry_key,
        retry_count,
    )

    result = move_to_dead_letter_queue(
        redis_client,
        message_id,
        fields,
        retry_count,
    )

    assert result is True

    dlq_messages = redis_client.xrange(
        DEAD_LETTER_STREAM,
        "-",
        "+",
    )

    assert len(dlq_messages) == 1

    dlq_message_id, dlq_fields = dlq_messages[0]

    assert dlq_fields["event_id"] == "test-dlq-001"
    assert dlq_fields["original_stream_id"] == message_id
    assert dlq_fields["retry_count"] == "3"

    assert redis_client.exists(retry_key) == 0

def test_recover_pending_messages(redis_client):
    redis_client.xgroup_create(
        name=EARTHQUAKE_STREAM,
        groupname=CONSUMER_GROUP,
        id="0-0",
        mkstream=True,
    )

    message_id = redis_client.xadd(
        EARTHQUAKE_STREAM,
        {
            "event_id": "recovery-test",
            "event_data": json.dumps(
                {
                    "event_id": "recovery-test",
                    "magnitude": 2.5,
                    "place": "Recovery Test",
                    "latitude": 10.0,
                    "longitude": 20.0,
                    "depth_km": 5.0,
                    "timestamp": "2026-09-10T10:00:00Z",
                    "source": "USGS",
                }
            ),
        },
    )

    messages = redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername="test-consumer",
        streams={EARTHQUAKE_STREAM: ">"},
        count=1,
    )

    assert messages
    assert messages[0][1][0][0] == message_id

    postgres_connection = Mock()

    with patch(
        "services.processor.main.CONSUMER_NAME",
        "seismoops-processor-1",
    ), patch(
        "services.processor.main.RECOVERY_IDLE_TIME_MS",
        0,
    ), patch(
        "services.processor.main.process_message",
        return_value=True,
    ) as mock_process:

        recovered_count = recover_pending_messages(
            redis_client,
            postgres_connection,
        )

    assert recovered_count == 1

    mock_process.assert_called_once()

    call_args = mock_process.call_args.args

    assert call_args[0] is redis_client
    assert call_args[1] is postgres_connection
    assert call_args[2] == message_id
    assert call_args[3]["event_id"] == "recovery-test"

def test_consume_new_messages(redis_client):
    redis_client.xgroup_create(
        name=EARTHQUAKE_STREAM,
        groupname=CONSUMER_GROUP,
        id="0-0",
        mkstream=True,
    )

    message_id = redis_client.xadd(
        EARTHQUAKE_STREAM,
        {
            "event_id": "consume-test",
            "event_data": json.dumps(
                {
                    "event_id": "consume-test",
                    "magnitude": 3.2,
                    "place": "Consume Test",
                    "latitude": 15.0,
                    "longitude": 25.0,
                    "depth_km": 8.0,
                    "timestamp": "2026-09-10T10:00:00Z",
                    "source": "USGS",
                }
            ),
        },
    )

    postgres_connection = Mock()

    with patch(
        "services.processor.main.process_message",
        return_value=True,
    ) as mock_process:

        processed_count = consume_new_messages(
            redis_client,
            postgres_connection,
        )

    assert processed_count == 1

    mock_process.assert_called_once()

    call_args = mock_process.call_args.args

    assert call_args[0] is redis_client
    assert call_args[1] is postgres_connection
    assert call_args[2] == message_id
    assert call_args[3]["event_id"] == "consume-test"

def test_increment_retry_count_increments_metric(redis_client):
    before = PROCESSOR_RETRIES._value.get()

    message_id = "metric-retry-test"

    retry_count = increment_retry_count(
        redis_client,
        message_id,
    )

    after = PROCESSOR_RETRIES._value.get()

    assert retry_count == 1
    assert after - before == 1

def test_process_message_increments_failure_metric():
    before = PROCESSOR_FAILURES._value.get()

    client = Mock()
    postgres_connection = Mock()

    event_data = json.dumps(
        {
            "event_id": "failure-metric-test",
            "magnitude": 2.5,
            "place": "Failure Metric Test",
            "latitude": 10.0,
            "longitude": 20.0,
            "depth_km": 5.0,
            "timestamp": "2026-09-10T10:00:00Z",
            "source": "USGS",
        }
    )

    with patch(
        "services.processor.main.save_earthquake_event",
        return_value=False,
    ), patch(
        "services.processor.main.increment_retry_count",
        return_value=1,
    ):
        result = process_message(
            client,
            postgres_connection,
            "2-0",
            {
                "event_id": "failure-metric-test",
                "event_data": event_data,
            },
        )

    after = PROCESSOR_FAILURES._value.get()

    assert result is False
    assert after - before == 1
    client.xack.assert_not_called()

def test_process_message_increments_failure_metric():
    before = PROCESSOR_FAILURES._value.get()

    client = Mock()
    postgres_connection = Mock()

    event_data = json.dumps(
        {
            "event_id": "failure-metric-test",
            "magnitude": 2.5,
            "place": "Failure Metric Test",
            "latitude": 10.0,
            "longitude": 20.0,
            "depth_km": 5.0,
            "timestamp": "2026-09-10T10:00:00Z",
            "source": "USGS",
        }
    )

    with patch(
        "services.processor.main.save_earthquake_event",
        return_value=False,
    ), patch(
        "services.processor.main.increment_retry_count",
        return_value=1,
    ):
        result = process_message(
            client,
            postgres_connection,
            "2-0",
            {
                "event_id": "failure-metric-test",
                "event_data": event_data,
            },
        )

    after = PROCESSOR_FAILURES._value.get()

    assert result is False
    assert after - before == 1
    client.xack.assert_not_called()

def test_recover_pending_messages_increments_metric(redis_client):
    before = PROCESSOR_RECOVERED_MESSAGES._value.get()

    redis_client.xgroup_create(
        name=EARTHQUAKE_STREAM,
        groupname=CONSUMER_GROUP,
        id="0-0",
        mkstream=True,
    )

    message_id = redis_client.xadd(
        EARTHQUAKE_STREAM,
        {
            "event_id": "metric-recovery-test",
            "event_data": json.dumps(
                {
                    "event_id": "metric-recovery-test",
                    "magnitude": 2.5,
                    "place": "Recovery Metric Test",
                    "latitude": 10.0,
                    "longitude": 20.0,
                    "depth_km": 5.0,
                    "timestamp": "2026-09-10T10:00:00Z",
                    "source": "USGS",
                }
            ),
        },
    )

    messages = redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername="test-consumer",
        streams={EARTHQUAKE_STREAM: ">"},
        count=1,
    )

    assert messages
    assert messages[0][1][0][0] == message_id

    postgres_connection = Mock()

    with patch(
        "services.processor.main.CONSUMER_NAME",
        "seismoops-processor-1",
    ), patch(
        "services.processor.main.RECOVERY_IDLE_TIME_MS",
        0,
    ), patch(
        "services.processor.main.process_message",
        return_value=True,
    ):

        recovered_count = recover_pending_messages(
            redis_client,
            postgres_connection,
        )

    after = PROCESSOR_RECOVERED_MESSAGES._value.get()

    assert recovered_count == 1
    assert after - before == 1


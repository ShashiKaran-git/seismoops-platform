from services.models import EarthquakeEvent

from services.observability.metrics import (
    COLLECTOR_EVENTS_FETCHED,
    COLLECTOR_EVENTS_PUBLISHED,
)

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import requests 

from services.collector.main import (
    fetch_earthquakes,
    main,
    parse_earthquake,
)
def test_parse_earthquake_valid_feature():
    feature = {
        "id": "test-event-001",
        "properties": {
            "mag": 5.2,
            "place": "10 km north of Hyderabad",
            "time": 1770001200000,
        },
        "geometry": {
            "coordinates": [
                78.486,
                17.385,
                10.0,
            ]
        },
    }

    event = parse_earthquake(feature)

    assert event is not None
    assert event.event_id == "test-event-001"
    assert event.magnitude == 5.2
    assert event.place == "10 km north of Hyderabad"

    assert event.latitude == 17.385
    assert event.longitude == 78.486
    assert event.depth_km == 10.0

    assert event.timestamp == datetime(
        2026,
        2,
        2,
        3,
        0,
        tzinfo=timezone.utc,
    )

    assert event.source == "USGS"


def test_parse_earthquake_invalid_coordinates():
    feature = {
        "id": "test-event-invalid",
        "properties": {
            "mag": 4.0,
            "place": "Test Location",
            "time": 1770001200000,
        },
        "geometry": {
            "coordinates": [
                200.0,
                17.385,
                10.0,
            ]
        },
    }

    event = parse_earthquake(feature)

    assert event is None


def test_parse_earthquake_missing_required_field():
    feature = {
        "id": "test-event-missing",
        "properties": {
            "mag": 4.0,
            "place": "Test Location",
        },
        "geometry": {
            "coordinates": [
                78.486,
                17.385,
                10.0,
            ]
        },
    }

    event = parse_earthquake(feature)

    assert event is None

@patch("services.collector.main.requests.get")
def test_fetch_earthquakes_success(mock_get):
    mock_response = Mock()

    mock_response.json.return_value = {
        "type": "FeatureCollection",
        "features": [
            {
                "id": "test-event-001",
                "properties": {
                    "mag": 5.2,
                    "place": "Test Location",
                    "time": 1770001200000,
                },
                "geometry": {
                    "coordinates": [
                        78.486,
                        17.385,
                        10.0,
                    ]
                },
            }
        ],
    }

    mock_get.return_value = mock_response

    result = fetch_earthquakes()

    assert result is not None
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 1
    assert result["features"][0]["id"] == "test-event-001"

    mock_get.assert_called_once()

@patch("services.collector.main.requests.get")
def test_fetch_earthquakes_request_failure(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException("Connection failed")

    result = fetch_earthquakes()

    assert result is None

@patch("services.collector.main.requests.get")
def test_fetch_earthquakes_timeout(mock_get):
    mock_get.side_effect = requests.exceptions.Timeout(
        "Request timed out"
    )

    result = fetch_earthquakes()

    assert result is None

@patch("services.collector.main.requests.get")
def test_fetch_earthquakes_invalid_json(mock_get):
    mock_response = Mock()

    mock_response.json.side_effect = ValueError(
        "Invalid JSON"
    )

    mock_get.return_value = mock_response

    result = fetch_earthquakes()

    assert result is None

def test_fetch_earthquakes_increments_events_fetched_metric():
    before = COLLECTOR_EVENTS_FETCHED._value.get()

    mock_response = Mock()

    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "features": [
            {"id": "event-1"},
            {"id": "event-2"},
            {"id": "event-3"},
        ]
    }

    with patch(
        "services.collector.main.requests.get",
        return_value=mock_response,
    ):
        result = fetch_earthquakes()

    after = COLLECTOR_EVENTS_FETCHED._value.get()

    assert result is not None
    assert after - before == 3

def test_main_increments_events_published_metric():
    before = COLLECTOR_EVENTS_PUBLISHED._value.get()

    event = EarthquakeEvent(
        event_id="published-metric-test",
        magnitude=2.5,
        place="Published Metric Test",
        latitude=10.0,
        longitude=20.0,
        depth_km=5.0,
        timestamp=datetime(
            2026,
            9,
            10,
            10,
            0,
            tzinfo=timezone.utc,
        ),
    )

    with patch(
        "services.collector.main.fetch_earthquakes",
        return_value={
            "features": [
                {
                    "id": "published-metric-test",
                    "properties": {},
                    "geometry": {},
                }
            ]
        },
    ), patch(
        "services.collector.main.create_redis_client",
        return_value=Mock(),
    ), patch(
        "services.collector.main.parse_earthquake",
        return_value=event,
    ), patch(
        "services.collector.main.publish_earthquake",
        return_value=True,
    ):

        main()

    after = COLLECTOR_EVENTS_PUBLISHED._value.get()

    assert after - before == 1
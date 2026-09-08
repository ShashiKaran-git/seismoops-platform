from datetime import datetime, timezone
from unittest.mock import Mock, patch

import requests 

from services.collector.main import (
    fetch_earthquakes,
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
from fastapi.testclient import TestClient

from services.api.main import app
from services.observability.metrics import API_REQUESTS

client = TestClient(app)


def test_health_check():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok"
    }

def test_get_earthquakes():
    response = client.get("/earthquakes")

    assert response.status_code == 200

    events = response.json()

    assert isinstance(events, list)
    assert len(events) == 5

def test_get_earthquakes_pagination():
    response = client.get(
        "/earthquakes?limit=2&offset=2"
    )

    assert response.status_code == 200

    events = response.json()

    assert len(events) == 2
    assert events[0]["event_id"] == "nc75428692"
    assert events[1]["event_id"] == "nc75428687"

def test_get_earthquakes_min_magnitude():
    response = client.get(
        "/earthquakes?min_magnitude=1.0"
    )

    assert response.status_code == 200

    events = response.json()

    assert len(events) == 3

    for event in events:
        assert event["magnitude"] >= 1.0

def test_get_earthquakes_place():
    response = client.get(
        "/earthquakes?place=Hayward"
    )

    assert response.status_code == 200

    events = response.json()

    assert len(events) == 1
    assert events[0]["event_id"] == "nc75428692"
    assert "Hayward" in events[0]["place"]

def test_get_earthquakes_combined_filters():
    response = client.get(
        "/earthquakes?place=CA&min_magnitude=1.0"
    )

    assert response.status_code == 200

    events = response.json()

    assert len(events) == 2

    event_ids = {
        event["event_id"]
        for event in events
    }

    assert event_ids == {
        "nc75428692",
        "nc75428687",
    }

def test_get_earthquake_by_id():
    response = client.get(
        "/earthquakes/nc75428702"
    )

    assert response.status_code == 200

    event = response.json()

    assert event["event_id"] == "nc75428702"
    assert event["magnitude"] == 0.94
    assert event["place"] == "10 km NW of The Geysers, CA"

def test_get_earthquake_not_found():
    response = client.get(
        "/earthquakes/does-not-exist"
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "Earthquake event not found"
    }

def test_get_earthquakes_invalid_limit():
    response = client.get(
        "/earthquakes?limit=0"
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "limit must be between 1 and 100"
    }

def test_get_earthquakes_limit_too_large():
    response = client.get(
        "/earthquakes?limit=101"
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "limit must be between 1 and 100"
    }

def test_get_earthquakes_invalid_offset():
    response = client.get(
        "/earthquakes?offset=-1"
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "offset must be 0 or greater"
    }

def test_api_request_increments_metric():
    before = API_REQUESTS._value.get()

    response = client.get("/health")

    after = API_REQUESTS._value.get()

    assert response.status_code == 200
    assert after - before == 1

def test_metrics_endpoint_exposes_custom_metrics():
    response = client.get("/metrics")

    assert response.status_code == 200

    metrics = response.text

    assert "seismoops_api_requests_total" in metrics
    assert "seismoops_collector_events_fetched_total" in metrics
    assert "seismoops_collector_events_published_total" in metrics
    assert "seismoops_processor_messages_processed_total" in metrics
    assert "seismoops_processor_failures_total" in metrics
    assert "seismoops_processor_retries_total" in metrics
    assert "seismoops_processor_dlq_messages_total" in metrics
    assert "seismoops_processor_recovered_messages_total" in metrics
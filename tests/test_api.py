
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.api import routes


client = TestClient(app)


def setup_function():
    """Reset in-memory state before each test."""
    routes.aggregator = routes.RollingTemperatureAggregator()
    routes.latest_results.clear()


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_submit_temperature_event():
    response = client.post(
        "/api/v1/events",
        json={
            "event_id": "event-001",
            "truck_id": "TRUCK-001",
            "temperature": 28.5,
            "timestamp": "2026-10-16T12:00:00Z",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] is True
    assert data["average_temperature"] == 28.5
    assert data["reading_count"] == 1


def test_rolling_temperature_average():
    first = client.post(
        "/api/v1/events",
        json={
            "event_id": "event-101",
            "truck_id": "TRUCK-101",
            "temperature": 28.5,
            "timestamp": "2026-10-16T12:00:00Z",
        },
    )

    second = client.post(
        "/api/v1/events",
        json={
            "event_id": "event-102",
            "truck_id": "TRUCK-101",
            "temperature": 30.5,
            "timestamp": "2026-10-16T12:01:00Z",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["average_temperature"] == 29.5
    assert second.json()["reading_count"] == 2


def test_duplicate_event_is_rejected():
    event = {
        "event_id": "event-duplicate",
        "truck_id": "TRUCK-201",
        "temperature": 29.0,
        "timestamp": "2026-10-16T12:00:00Z",
    }

    first = client.post("/api/v1/events", json=event)
    second = client.post("/api/v1/events", json=event)

    assert first.json()["accepted"] is True
    assert second.json()["accepted"] is False
    assert second.json()["reason"] == "duplicate_event"


def test_different_trucks_are_processed_independently():
    client.post(
        "/api/v1/events",
        json={
            "event_id": "event-truck-a",
            "truck_id": "TRUCK-A",
            "temperature": 20.0,
            "timestamp": "2026-10-16T12:00:00Z",
        },
    )

    response = client.post(
        "/api/v1/events",
        json={
            "event_id": "event-truck-b",
            "truck_id": "TRUCK-B",
            "temperature": 40.0,
            "timestamp": "2026-10-16T12:00:00Z",
        },
    )

    assert response.status_code == 200
    assert response.json()["truck_id"] == "TRUCK-B"
    assert response.json()["average_temperature"] == 40.0
    assert response.json()["reading_count"] == 1

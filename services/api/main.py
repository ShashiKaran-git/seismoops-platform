from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.database.postgres import (
    create_postgres_connection,
    get_earthquake_events,
    get_earthquake_event_by_id,
)


app = FastAPI(
    title="SeismoOps Query API",
    description="API for querying persisted earthquake events.",
    version="1.0.0",
)


class EarthquakeResponse(BaseModel):
    event_id: str
    magnitude: float | None
    place: str | None
    latitude: float
    longitude: float
    depth_km: float
    timestamp: str
    source: str


@app.get("/health")
def health_check():
    return {
        "status": "ok"
    }


@app.get("/earthquakes")
def get_earthquakes(
    limit: int = 100,
    offset: int = 0,
):
    if limit < 1 or limit > 100:
        raise HTTPException(
            status_code=400,
            detail="limit must be between 1 and 100",
        )

    if offset < 0:
        raise HTTPException(
            status_code=400,
            detail="offset must be 0 or greater",
        )

    connection = create_postgres_connection()

    if connection is None:
        raise HTTPException(
            status_code=500,
            detail="Unable to connect to PostgreSQL",
        )

    try:
        events = get_earthquake_events(
            connection,
            limit=limit,
            offset=offset,
        )

        response = []

        for event in events:
            response.append(
                EarthquakeResponse(
                    event_id=event["event_id"],
                    magnitude=event["magnitude"],
                    place=event["place"],
                    latitude=event["latitude"],
                    longitude=event["longitude"],
                    depth_km=event["depth_km"],
                    timestamp=event["timestamp"].isoformat(),
                    source=event["source"],
                )
            )

        return response

    finally:
        connection.close()

@app.get("/earthquakes/{event_id}")
def get_earthquake(event_id: str):
    connection = create_postgres_connection()

    if connection is None:
        raise HTTPException(
            status_code=500,
            detail="Unable to connect to PostgreSQL",
        )

    try:
        event = get_earthquake_event_by_id(
            connection,
            event_id,
        )

        if event is None:
            raise HTTPException(
                status_code=404,
                detail="Earthquake event not found",
            )

        return EarthquakeResponse(
            event_id=event["event_id"],
            magnitude=event["magnitude"],
            place=event["place"],
            latitude=event["latitude"],
            longitude=event["longitude"],
            depth_km=event["depth_km"],
            timestamp=event["timestamp"].isoformat(),
            source=event["source"],
        )

    finally:
        connection.close()
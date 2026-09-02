import logging

import psycopg2

from services.models import EarthquakeEvent


POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "seismoops"
POSTGRES_USER = "seismoops"
POSTGRES_PASSWORD = "seismoops_dev"

logger = logging.getLogger(__name__)


def create_postgres_connection():
    try:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        logger.info("Successfully connected to PostgreSQL")

        return connection

    except psycopg2.Error as error:
        logger.error(
            "PostgreSQL connection failed | error=%s",
            error,
        )

        return None


def save_earthquake_event(connection, event: EarthquakeEvent):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO earthquake_events (
                    event_id,
                    magnitude,
                    place,
                    latitude,
                    longitude,
                    depth_km,
                    timestamp,
                    source
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event.event_id,
                    event.magnitude,
                    event.place,
                    event.latitude,
                    event.longitude,
                    event.depth_km,
                    event.timestamp,
                    event.source,
                ),
            )

        connection.commit()

        logger.info(
            "Stored earthquake event | event_id=%s",
            event.event_id,
        )

        return True

    except psycopg2.Error as error:
        connection.rollback()

        logger.error(
            "Failed to store earthquake event | "
            "event_id=%s | error=%s",
            event.event_id,
            error,
        )

        return False
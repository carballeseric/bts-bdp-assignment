import sqlite3
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

# Ruta a la base de datos SQLite generada por prepare_s8_data.py
DB_PATH = Path(settings.local_dir).parent / "s8_aircraft.db"


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database not found at {DB_PATH}. Run prepare_s8_data.py first.",
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    offset = page * num_results
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT icao, registration, aircraft_type AS type, owner, manufacturer, model
            FROM aircraft
            ORDER BY icao ASC
            LIMIT ? OFFSET ?
            """,
            (num_results, offset),
        ).fetchall()
    finally:
        conn.close()

    return [
        AircraftReturn(
            icao=row["icao"],
            registration=row["registration"],
            type=row["type"],
            owner=row["owner"],
            manufacturer=row["manufacturer"],
            model=row["model"],
        )
        for row in rows
    ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day.

    Computation:
    - Each row in tracking = 5-second observation
    - hours_flown = (observations * 5) / 3600
    - fuel_used_kg = hours_flown * galph * 3.04
    - co2_tons = (fuel_used_kg * 3.15) / 907.185
    - If no fuel rate available, co2 = None
    """
    conn = get_conn()
    try:
        # Contar observaciones para este avión en ese día
        row = conn.execute(
            "SELECT COUNT(*) AS obs FROM tracking WHERE icao = ? AND day = ?",
            (icao.upper(), day),
        ).fetchone()
        observations = row["obs"] if row else 0

        # Calcular horas voladas
        hours_flown = (observations * 5) / 3600

        # Obtener tipo de avión
        ac = conn.execute(
            "SELECT aircraft_type FROM aircraft WHERE icao = ?",
            (icao.upper(),),
        ).fetchone()
        aircraft_type = ac["aircraft_type"] if ac else None

        # Buscar tasa de consumo de combustible
        co2 = None
        if aircraft_type:
            fuel_row = conn.execute(
                "SELECT galph FROM fuel_rates WHERE aircraft_type = ?",
                (aircraft_type,),
            ).fetchone()
            if fuel_row and fuel_row["galph"] is not None:
                galph = float(fuel_row["galph"])
                fuel_used_kg = hours_flown * galph * 3.04
                co2 = (fuel_used_kg * 3.15) / 907.185
    finally:
        conn.close()

    return AircraftCO2Return(icao=icao.upper(), hours_flown=hours_flown, co2=co2)

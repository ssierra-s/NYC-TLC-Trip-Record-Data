"""Reglas de calidad de dato (Capa Plata).

Los nombres de columna son neutros respecto al dataset de origen
(`pickup_datetime` / `dropoff_datetime`): el prefijo propio de cada dataset de
la TLC (`tpep_` para Yellow Taxi, `lpep_` para Green Taxi) ya se normaliza en
la proyeccion de DuckDB (ver `etl/transform/cleaner.py`).
"""

import pandas as pd

PICKUP_COLUMN = "pickup_datetime"
DROPOFF_COLUMN = "dropoff_datetime"
DISTANCE_COLUMN = "trip_distance"
FARE_COLUMN = "fare_amount"

REJECTION_INVALID_DISTANCE = "INVALID_DISTANCE: Distance <= 0"
REJECTION_INVALID_FARE = "INVALID_FARE: Fare < 0"
REJECTION_INVALID_DATES = "INVALID_DATES: Dropoff <= Pickup"


def build_rejection_reason(row) -> str:
    """Construye la razon de descarte. Un mismo registro puede violar varias
    reglas a la vez; todas quedan registradas separadas por ' | '."""
    reasons = []
    if row[DISTANCE_COLUMN] <= 0:
        reasons.append(REJECTION_INVALID_DISTANCE)
    if row[FARE_COLUMN] < 0:
        reasons.append(REJECTION_INVALID_FARE)
    if row[DROPOFF_COLUMN] <= row[PICKUP_COLUMN]:
        reasons.append(REJECTION_INVALID_DATES)
    return " | ".join(reasons)


def validate_trip_records(df: pd.DataFrame):
    """Separa un lote de viajes en (validos, rechazados).

    Los rechazados salen con una columna extra `rejection_reason` que alimenta
    la Dead Letter Queue (`silver.rejected_trip_records`).
    """
    df = df.copy()

    invalid_distance = df[DISTANCE_COLUMN] <= 0
    invalid_dates = df[DROPOFF_COLUMN] <= df[PICKUP_COLUMN]
    invalid_fare = df[FARE_COLUMN] < 0

    invalid_mask = invalid_distance | invalid_dates | invalid_fare

    rejected = df[invalid_mask].copy()
    valid = df[~invalid_mask].copy()

    if not rejected.empty:
        rejected["rejection_reason"] = rejected.apply(build_rejection_reason, axis=1)

    return valid, rejected

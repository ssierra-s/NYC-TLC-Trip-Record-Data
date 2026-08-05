"""Pruebas de las reglas de transformacion (mapeo crudo -> tabla de hechos)."""

import datetime

import pandas as pd
import pytest

from etl.transform.cleaner import (
    FACT_SOURCE_COLUMNS,
    build_fact_records,
    build_select_query,
)

EXECUTION_ID = "11111111-2222-3333-4444-555555555555"
SOURCE_FILE = "yellow_tripdata_2024-01.parquet"


def build_valid_df(**overrides):
    row = {
        "vendor_id": 2,
        "pu_location_id": 132,
        "do_location_id": 230,
        "payment_type_id": 1,
        "ratecode_id": 2,
        "pickup_datetime": pd.to_datetime("2024-01-15 08:00:00"),
        "dropoff_datetime": pd.to_datetime("2024-01-15 08:30:00"),
        "trip_distance": 17.5,
        "passenger_count": 1,
        "fare_amount": 70.0,
        "extra": 1.75,
        "mta_tax": 0.5,
        "tip_amount": 14.0,
        "tolls_amount": 6.94,
        "total_amount": 93.19,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_select_query_uses_tpep_columns_for_yellow():
    query = build_select_query("yellow")

    assert "tpep_pickup_datetime AS pickup_datetime" in query
    assert "tpep_dropoff_datetime AS dropoff_datetime" in query
    assert "lpep_" not in query


def test_select_query_uses_lpep_columns_for_green():
    """Green Taxi publica las fechas como lpep_*; si el pipeline no lo
    contempla, procesar green revienta al leer el Parquet."""
    query = build_select_query("green")

    assert "lpep_pickup_datetime AS pickup_datetime" in query
    assert "lpep_dropoff_datetime AS dropoff_datetime" in query
    assert "tpep_" not in query


def test_select_query_rejects_unsupported_taxi_type():
    with pytest.raises(ValueError, match="fhv"):
        build_select_query("fhv")


def test_trip_duration_is_computed_in_seconds():
    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, build_valid_df())

    assert records[0][-1] == 30 * 60


def test_trip_duration_handles_trips_crossing_midnight():
    df = build_valid_df(
        pickup_datetime=pd.to_datetime("2024-01-15 23:50:00"),
        dropoff_datetime=pd.to_datetime("2024-01-16 00:20:00"),
    )

    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, df)

    assert records[0][-1] == 30 * 60


def test_record_starts_with_audit_traceability_columns():
    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, build_valid_df())

    assert records[0][0] == EXECUTION_ID
    assert records[0][1] == SOURCE_FILE


def test_record_length_matches_insert_contract():
    """execution_id + source_file + columnas proyectadas + duracion."""
    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, build_valid_df())

    assert len(records[0]) == len(FACT_SOURCE_COLUMNS) + 3


def test_column_order_matches_insert_statement():
    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, build_valid_df())
    values = records[0]

    assert values[2] == 2       # vendor_id
    assert values[3] == 132     # pickup_location_id
    assert values[4] == 230     # dropoff_location_id
    assert values[5] == 1       # payment_type_id
    assert values[6] == 2       # rate_code_id
    assert values[9] == 17.5    # trip_distance
    assert values[16] == 93.19  # total_amount


def test_values_are_native_python_types():
    """psycopg2 no sabe adaptar numpy.int64 (lanza 'can't adapt type') y adapta
    mal numpy.float64. Toda la tupla debe salir con tipos nativos."""
    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, build_valid_df())

    for value in records[0]:
        assert type(value) in (str, int, float, datetime.datetime, pd.Timestamp), \
            f"Tipo no adaptable por psycopg2: {type(value)}"


def test_builds_one_record_per_row():
    df = pd.concat([build_valid_df(), build_valid_df(vendor_id=1)], ignore_index=True)

    records = build_fact_records(EXECUTION_ID, SOURCE_FILE, df)

    assert len(records) == 2
    assert [record[2] for record in records] == [2, 1]

"""Pruebas de las reglas de calidad de dato (Fase 2 de auditoria)."""

import pandas as pd

from etl.transform.validators import validate_trip_records


def build_trip(distance=3.5, fare=20.0, pickup='2024-01-01 10:00:00', dropoff='2024-01-01 10:20:00'):
    """Constructor de un lote de un solo viaje: cada test altera solo el campo
    que quiere poner a prueba."""
    return pd.DataFrame({
        'trip_distance': [distance],
        'fare_amount': [fare],
        'pickup_datetime': [pd.to_datetime(pickup)],
        'dropoff_datetime': [pd.to_datetime(dropoff)],
    })


def test_valid_trip_record_is_accepted():
    valid, rejected = validate_trip_records(build_trip())

    assert len(valid) == 1
    assert len(rejected) == 0


def test_negative_distance_is_rejected():
    valid, rejected = validate_trip_records(build_trip(distance=-5.0))

    assert len(valid) == 0
    assert "INVALID_DISTANCE" in rejected.iloc[0]["rejection_reason"]


def test_zero_distance_is_rejected():
    valid, rejected = validate_trip_records(build_trip(distance=0.0))

    assert len(valid) == 0
    assert "INVALID_DISTANCE" in rejected.iloc[0]["rejection_reason"]


def test_negative_fare_is_rejected():
    valid, rejected = validate_trip_records(build_trip(fare=-10.0))

    assert len(valid) == 0
    assert "INVALID_FARE" in rejected.iloc[0]["rejection_reason"]


def test_zero_fare_is_accepted():
    """Una tarifa de 0 es valida (viaje sin cargo, payment_type 'No charge');
    la regla del negocio descarta solo montos NEGATIVOS."""
    valid, rejected = validate_trip_records(build_trip(fare=0.0))

    assert len(valid) == 1
    assert len(rejected) == 0


def test_invalid_dates_are_rejected():
    valid, rejected = validate_trip_records(
        build_trip(pickup='2024-01-01 10:30:00', dropoff='2024-01-01 10:10:00')
    )

    assert len(valid) == 0
    assert "INVALID_DATES" in rejected.iloc[0]["rejection_reason"]


def test_same_pickup_and_dropoff_is_rejected():
    """Duracion cero: no es un viaje real y ademas distorsiona el calculo de la
    duracion promedio en la Capa Oro."""
    valid, rejected = validate_trip_records(
        build_trip(pickup='2024-01-01 10:00:00', dropoff='2024-01-01 10:00:00')
    )

    assert len(valid) == 0
    assert "INVALID_DATES" in rejected.iloc[0]["rejection_reason"]


def test_multiple_violations_are_all_reported():
    valid, rejected = validate_trip_records(
        build_trip(distance=-1.0, fare=-5.0, pickup='2024-01-01 10:30:00', dropoff='2024-01-01 10:00:00')
    )

    reason = rejected.iloc[0]["rejection_reason"]
    assert "INVALID_DISTANCE" in reason
    assert "INVALID_FARE" in reason
    assert "INVALID_DATES" in reason


def test_batch_is_split_without_losing_records():
    """Ningun registro puede desaparecer: validos + rechazados == crudos leidos.
    Es la invariante que sostiene los conteos de la tabla de auditoria."""
    batch = pd.concat([
        build_trip(),
        build_trip(distance=-2.0),
        build_trip(fare=-1.0),
        build_trip(pickup='2024-01-01 12:00:00', dropoff='2024-01-01 11:00:00'),
    ], ignore_index=True)

    valid, rejected = validate_trip_records(batch)

    assert len(valid) == 1
    assert len(rejected) == 3
    assert len(valid) + len(rejected) == len(batch)


def test_input_dataframe_is_not_mutated():
    """El validador no debe alterar el lote original (no agrega la columna
    `rejection_reason` al DataFrame de entrada)."""
    batch = build_trip(distance=-1.0)

    validate_trip_records(batch)

    assert "rejection_reason" not in batch.columns

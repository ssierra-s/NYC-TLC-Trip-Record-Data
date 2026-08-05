"""Pruebas de la generacion de periodos consecutivos a descargar."""

import pytest

from etl.pipeline import build_periods


def test_generates_three_consecutive_months():
    assert build_periods(2024, 1, 3) == [(2024, 1), (2024, 2), (2024, 3)]


def test_rolls_over_into_the_next_year():
    """Sin salto de anio, un arranque en noviembre pedia el 'mes 13' y el
    pipeline intentaba descargar yellow_tripdata_2024-13.parquet (404)."""
    assert build_periods(2024, 11, 3) == [(2024, 11), (2024, 12), (2025, 1)]


def test_spans_more_than_a_full_year():
    periods = build_periods(2023, 12, 14)

    assert len(periods) == 14
    assert periods[0] == (2023, 12)
    assert periods[-1] == (2025, 1)
    assert all(1 <= month <= 12 for _, month in periods)


def test_rejects_invalid_start_month():
    with pytest.raises(ValueError):
        build_periods(2024, 13, 3)


def test_rejects_zero_months():
    with pytest.raises(ValueError):
        build_periods(2024, 1, 0)

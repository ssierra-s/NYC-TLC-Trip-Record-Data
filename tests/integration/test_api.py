"""Pruebas de contrato de los endpoints Flask.

Se mockea el acceso a datos (`db_cursor`), asi que no requieren PostgreSQL
levantado: validan el contrato de respuesta del API, no la base de datos.
"""

from contextlib import contextmanager

import pytest

from api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as test_client:
        yield test_client


class FakeCursor:
    """Cursor de mentira que responde segun la consulta ejecutada y registra
    los parametros recibidos, para poder afirmar sobre ellos."""

    def __init__(self, responses):
        self.responses = responses
        self.executed = []
        self._last_key = None

    def execute(self, query, params=None):
        self.executed.append((query, params))
        self._last_key = next(
            (key for key in self.responses if key in query), None
        )

    def fetchall(self):
        return self.responses.get(self._last_key, [])


def patch_db_cursor(monkeypatch, module, cursor):
    @contextmanager
    def fake_db_cursor():
        yield cursor

    monkeypatch.setattr(f'api.routes.{module}.db_cursor', fake_db_cursor)


EXECUTION_ROW = {
    "execution_id": "test-uuid-999",
    "pipeline_name": "nyc_taxi_batch_pipeline",
    "source_file": None,
    "status": "SUCCESS",
    "started_at": "2026-08-03 10:00:00",
    "finished_at": "2026-08-03 10:05:00",
    "raw_records_read": 1000,
    "valid_records": 988,
    "rejected_records": 12,
    "loaded_records": 988,
    "duration_seconds": 300.0,
    "error_message": None
}

FILE_LOG_ROW = {
    "file_log_id": 1,
    "execution_id": "test-uuid-999",
    "source_file": "yellow_tripdata_2024-01.parquet",
    "status": "SUCCESS",
    "started_at": "2026-08-03 10:00:00",
    "finished_at": "2026-08-03 10:05:00",
    "raw_records_read": 1000,
    "valid_records": 988,
    "rejected_records": 12,
    "loaded_records": 988,
    "error_message": None
}

METRIC_ROW = {
    "year": 2024,
    "month": 1,
    "location_id": 132,
    "zone": "JFK Airport",
    "borough": "Queens",
    "total_trips": 153240,
    "total_revenue": 4278390.50,
    "average_trip_duration_seconds": 1842.35,
    "peak_hour": 17,
    "tip_percentage": 18.42,
    "updated_at": "2026-08-03 10:05:00"
}


def health_cursor():
    return FakeCursor({
        "etl_execution_log": [EXECUTION_ROW],
        "etl_file_log": [FILE_LOG_ROW],
        "rejected_trip_records": [
            {"rejection_reason": "INVALID_DISTANCE: Distance <= 0", "total": 9},
            {"rejection_reason": "INVALID_FARE: Fare < 0", "total": 3},
        ],
    })


# --------------------------------------------------------------------------
# /api/v1/health
# --------------------------------------------------------------------------

def test_health_contract(client, monkeypatch):
    patch_db_cursor(monkeypatch, 'health', health_cursor())

    response = client.get('/api/v1/health')

    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "SUCCESS"
    assert data["database"] == "UP"
    assert data["last_execution"]["execution_id"] == "test-uuid-999"
    assert data["recent_file_logs"][0]["source_file"] == "yellow_tripdata_2024-01.parquet"


def test_health_reports_dlq_total_and_breakdown(client, monkeypatch):
    patch_db_cursor(monkeypatch, 'health', health_cursor())

    data = client.get('/api/v1/health').get_json()

    assert data["total_rejected_in_dlq"] == 12
    assert len(data["dlq_breakdown_by_reason"]) == 2
    assert data["dlq_breakdown_by_reason"][0]["total"] == 9


def test_health_caps_the_limit_parameter(client, monkeypatch):
    """Un `limit` desmedido no puede traducirse en una consulta sin techo."""
    cursor = health_cursor()
    patch_db_cursor(monkeypatch, 'health', cursor)

    client.get('/api/v1/health?limit=99999')

    assert cursor.executed[0][1] == (100,)


def test_health_returns_503_when_database_is_down(client, monkeypatch):
    @contextmanager
    def failing_cursor():
        raise RuntimeError("connection refused: password=secret host=10.0.0.5")
        yield  # pragma: no cover

    monkeypatch.setattr('api.routes.health.db_cursor', failing_cursor)

    response = client.get('/api/v1/health')

    assert response.status_code == 503
    body = response.get_json()
    assert body["database"] == "DOWN"
    # El detalle interno no se filtra al cliente.
    assert "password" not in str(body)


# --------------------------------------------------------------------------
# /api/v1/metrics
# --------------------------------------------------------------------------

def test_metrics_contract(client, monkeypatch):
    patch_db_cursor(monkeypatch, 'metrics', FakeCursor({"monthly_zone_metrics": [METRIC_ROW]}))

    response = client.get('/api/v1/metrics?year=2024&month=1')

    assert response.status_code == 200
    data = response.get_json()
    assert data["year"] == 2024
    assert data["month"] == 1
    assert data["count"] == 1
    assert data["records"][0]["zone"] == "JFK Airport"
    assert data["records"][0]["peak_hour"] == 17


def test_metrics_filters_are_sent_as_bound_parameters(client, monkeypatch):
    """Los filtros viajan como parametros ligados (%s), nunca interpolados en
    el SQL: es la defensa contra inyeccion."""
    cursor = FakeCursor({"monthly_zone_metrics": [METRIC_ROW]})
    patch_db_cursor(monkeypatch, 'metrics', cursor)

    client.get('/api/v1/metrics?year=2024&month=1&location_id=132')

    query, params = cursor.executed[0]
    assert params[:3] == [2024, 1, 132]
    assert "2024" not in query


def test_metrics_rejects_invalid_month(client, monkeypatch):
    patch_db_cursor(monkeypatch, 'metrics', FakeCursor({"monthly_zone_metrics": []}))

    response = client.get('/api/v1/metrics?month=13')

    assert response.status_code == 400
    assert "month" in response.get_json()["error"]


def test_metrics_caps_pagination_limit(client, monkeypatch):
    cursor = FakeCursor({"monthly_zone_metrics": [METRIC_ROW]})
    patch_db_cursor(monkeypatch, 'metrics', cursor)

    data = client.get('/api/v1/metrics?limit=100000').get_json()

    assert data["limit"] == 500
    assert cursor.executed[0][1][-2:] == [500, 0]


def test_metrics_works_without_filters(client, monkeypatch):
    patch_db_cursor(monkeypatch, 'metrics', FakeCursor({"monthly_zone_metrics": [METRIC_ROW]}))

    response = client.get('/api/v1/metrics')

    assert response.status_code == 200
    data = response.get_json()
    assert data["year"] is None
    assert data["count"] == 1

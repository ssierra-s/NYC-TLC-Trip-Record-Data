"""Pruebas de la descarga por lotes (Capa Bronce)."""

import pytest
import requests

from etl.extract.downloader import build_file_name, download_month


def test_build_file_name_pads_month_with_zero():
    assert build_file_name("yellow", 2024, 1) == "yellow_tripdata_2024-01.parquet"
    assert build_file_name("green", 2024, 12) == "green_tripdata_2024-12.parquet"


def test_download_month_skips_when_file_already_exists(tmp_path, monkeypatch):
    existing_file = tmp_path / "yellow_tripdata_2024-01.parquet"
    existing_file.write_bytes(b"fake-parquet-content")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("requests.get should not be called when file already exists")

    monkeypatch.setattr("etl.extract.downloader.requests.get", fail_if_called)

    result_path = download_month("yellow", 2024, 1, output_directory=str(tmp_path))

    assert result_path == existing_file
    assert result_path.read_bytes() == b"fake-parquet-content"


class FakeResponse:
    """Doble de prueba de la respuesta en streaming de requests."""

    def __init__(self, chunks, error=None):
        self._chunks = chunks
        self._error = error

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for chunk in self._chunks:
            yield chunk
        if self._error:
            raise self._error


def test_download_month_writes_file_and_removes_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "etl.extract.downloader.requests.get",
        lambda *args, **kwargs: FakeResponse([b"PAR1", b"-data"])
    )

    result_path = download_month("yellow", 2024, 3, output_directory=str(tmp_path))

    assert result_path.read_bytes() == b"PAR1-data"
    assert list(tmp_path.glob("*.part")) == []


def test_interrupted_download_leaves_no_partial_file(tmp_path, monkeypatch):
    """Una descarga cortada no puede dejar un Parquet truncado en disco: la
    siguiente ejecucion lo daria por valido y saltaria la descarga."""
    monkeypatch.setattr(
        "etl.extract.downloader.requests.get",
        lambda *args, **kwargs: FakeResponse([b"PAR1"], error=requests.ConnectionError("boom"))
    )

    with pytest.raises(requests.ConnectionError):
        download_month("yellow", 2024, 4, output_directory=str(tmp_path))

    assert list(tmp_path.iterdir()) == []

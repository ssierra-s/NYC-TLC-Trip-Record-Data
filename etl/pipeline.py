"""Orquestacion del pipeline batch: Bronce -> Plata -> Oro, con auditoria
transversal en las tres fases."""

import logging
import time
from uuid import uuid4

from etl.extract.downloader import download_month
from etl.transform.cleaner import process_parquet
from etl.load.audit import (
    create_execution,
    finish_execution,
    fail_execution,
    start_file_log,
    finish_file_log,
    fail_file_log,
    get_db_connection
)

logger = logging.getLogger(__name__)


def build_periods(start_year: int, start_month: int, months_to_process: int) -> list[tuple[int, int]]:
    """Genera N periodos (anio, mes) consecutivos con salto de anio correcto.

    Ej: (2024, 11, 3) -> [(2024, 11), (2024, 12), (2025, 1)]
    """
    if months_to_process < 1:
        raise ValueError("months_to_process debe ser >= 1")
    if not 1 <= start_month <= 12:
        raise ValueError("start_month debe estar entre 1 y 12")

    periods = []
    for offset in range(months_to_process):
        absolute_month = (start_month - 1) + offset
        periods.append((start_year + absolute_month // 12, absolute_month % 12 + 1))
    return periods


def run_pipeline(taxi_type: str = "yellow", periods: list[tuple[int, int]] | None = None) -> None:
    """Ejecuta el pipeline completo para los periodos indicados.

    Toda la ejecucion queda bajo un unico `execution_id`: Fase 1 al inicio
    (y por cada archivo), Fase 2 en la validacion de calidad, Fase 3 al cerrar
    tras el procedimiento almacenado de la Capa Oro.
    """
    periods = periods or build_periods(2024, 1, 3)
    execution_id = uuid4()
    start_time = time.time()

    create_execution(
        execution_id=execution_id,
        pipeline_name="nyc_taxi_batch_pipeline",
    )

    try:
        totals = {
            "raw": 0,
            "valid": 0,
            "rejected": 0,
            "loaded": 0,
        }

        for year, month in periods:
            print(f"--- Processing {taxi_type} {year}-{month:02d} ---")
            source_file = download_month(
                taxi_type=taxi_type,
                year=year,
                month=month,
            )

            file_log_id = start_file_log(execution_id, source_file.name)
            try:
                result = process_parquet(
                    file_path=str(source_file),
                    execution_id=execution_id,
                    taxi_type=taxi_type,
                )
            except Exception as file_exc:
                fail_file_log(file_log_id, str(file_exc))
                raise

            finish_file_log(
                file_log_id=file_log_id,
                raw_records=result.raw_records,
                valid_records=result.valid_records,
                rejected_records=result.rejected_records,
                loaded_records=result.loaded_records,
            )

            totals["raw"] += result.raw_records
            totals["valid"] += result.valid_records
            totals["rejected"] += result.rejected_records
            totals["loaded"] += result.loaded_records

        # Capa Oro: el procedimiento almacenado recalcula las metricas de negocio
        print("Executing Gold Layer Stored Procedure: gold.sp_generate_monthly_zone_metrics()...")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("CALL gold.sp_generate_monthly_zone_metrics();")
        conn.commit()
        cursor.close()
        conn.close()

        duration = round(time.time() - start_time, 3)

        finish_execution(
            execution_id=execution_id,
            raw_records=totals["raw"],
            valid_records=totals["valid"],
            rejected_records=totals["rejected"],
            loaded_records=totals["loaded"],
            duration_seconds=duration
        )
        print(f"Pipeline executed successfully! Execution ID: {execution_id}")

    except Exception as exc:
        duration = round(time.time() - start_time, 3)
        logger.exception("Pipeline execution failed")
        fail_execution(
            execution_id=execution_id,
            error_message=str(exc),
            duration_seconds=duration
        )
        raise

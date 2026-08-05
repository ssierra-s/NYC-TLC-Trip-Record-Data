"""Lectura por lotes del Parquet crudo, validacion y carga a la Capa Plata."""

import os

import psycopg2
import duckdb
from psycopg2.extras import execute_values

from etl.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from etl.transform.validators import validate_trip_records
from etl.load.audit import log_rejected_records

# Filas leidas por lote desde el Parquet. Mantiene el uso de memoria acotado
# sin importar cuantos millones de registros tenga el archivo del mes.
CHUNK_SIZE = 250_000

# Prefijo de las columnas de fecha segun el dataset publicado por la TLC:
# Yellow Taxi -> tpep_pickup_datetime / tpep_dropoff_datetime
# Green Taxi  -> lpep_pickup_datetime / lpep_dropoff_datetime
DATETIME_COLUMN_PREFIX = {
    "yellow": "tpep",
    "green": "lpep",
}

# Orden de las columnas proyectadas por DuckDB. Coincide exactamente con el
# orden de INSERT_FACT_TRIP_SQL (despues de execution_id y source_file) para
# poder construir las tuplas de insercion de forma vectorizada.
FACT_SOURCE_COLUMNS = (
    "vendor_id",
    "pu_location_id",
    "do_location_id",
    "payment_type_id",
    "ratecode_id",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "passenger_count",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "total_amount",
)

INSERT_FACT_TRIP_SQL = """
    INSERT INTO silver.fact_trip (
        execution_id, source_file, vendor_id, pickup_location_id, dropoff_location_id,
        payment_type_id, rate_code_id, pickup_datetime, dropoff_datetime,
        trip_distance, passenger_count, fare_amount, extra_amount,
        mta_tax, tip_amount, tolls_amount, total_amount, trip_duration_seconds
    ) VALUES %s;
"""


class ProcessResult:
    def __init__(self, raw_records, valid_records, rejected_records, loaded_records, purged_records=0):
        self.raw_records = raw_records
        self.valid_records = valid_records
        self.rejected_records = rejected_records
        self.loaded_records = loaded_records
        self.purged_records = purged_records


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def build_select_query(taxi_type: str) -> str:
    """Proyeccion normalizada del Parquet crudo.

    Se seleccionan solo las columnas que alimentan el modelo relacional (evita
    leer del Parquet columnas que no se usan) y se normalizan los nombres de
    fecha para que Yellow y Green compartan el mismo pipeline. Los COALESCE
    garantizan que ningun valor nulo rompa las llaves foraneas hacia las
    dimensiones: los codigos desconocidos se redirigen a los miembros
    'Unknown' de cada dimension (vendor -1, ratecode 99, location 264).
    """
    prefix = DATETIME_COLUMN_PREFIX.get(taxi_type.lower())
    if prefix is None:
        raise ValueError(
            f"Tipo de taxi no soportado: '{taxi_type}'. "
            f"Valores validos: {sorted(DATETIME_COLUMN_PREFIX)}"
        )

    return f"""
SELECT
    COALESCE(CAST(VendorID AS INT), -1) AS vendor_id,
    {prefix}_pickup_datetime AS pickup_datetime,
    {prefix}_dropoff_datetime AS dropoff_datetime,
    COALESCE(CAST(passenger_count AS INT), 0) AS passenger_count,
    COALESCE(CAST(trip_distance AS DOUBLE), 0.0) AS trip_distance,
    COALESCE(CAST(RatecodeID AS INT), 99) AS ratecode_id,
    COALESCE(CAST(PULocationID AS INT), 264) AS pu_location_id,
    COALESCE(CAST(DOLocationID AS INT), 264) AS do_location_id,
    COALESCE(CAST(payment_type AS INT), 0) AS payment_type_id,
    COALESCE(CAST(fare_amount AS DOUBLE), 0.0) AS fare_amount,
    COALESCE(CAST(extra AS DOUBLE), 0.0) AS extra,
    COALESCE(CAST(mta_tax AS DOUBLE), 0.0) AS mta_tax,
    COALESCE(CAST(tip_amount AS DOUBLE), 0.0) AS tip_amount,
    COALESCE(CAST(tolls_amount AS DOUBLE), 0.0) AS tolls_amount,
    COALESCE(CAST(total_amount AS DOUBLE), 0.0) AS total_amount
FROM read_parquet(?)
"""


def build_fact_records(execution_id, source_file: str, valid_df):
    """Construye las tuplas de insercion de forma vectorizada.

    Se usa `Series.tolist()` columna por columna (y no `iterrows()`) por dos
    razones: es un orden de magnitud mas rapido sobre millones de filas, y
    devuelve tipos nativos de Python. Esto ultimo es obligatorio: psycopg2 no
    sabe adaptar `numpy.int64` (lanza 'can't adapt type') y adapta mal
    `numpy.float64`.
    """
    duration_seconds = (
        (valid_df["dropoff_datetime"] - valid_df["pickup_datetime"])
        .dt.total_seconds()
        .astype("int64")
    )

    columns = [valid_df[column].tolist() for column in FACT_SOURCE_COLUMNS]
    columns.append(duration_seconds.tolist())

    execution_id_str = str(execution_id)
    return [(execution_id_str, source_file) + row for row in zip(*columns)]


def purge_previous_load(cursor, source_file: str) -> int:
    """Borra la carga anterior del mismo archivo (idempotencia).

    Sin esto, reprocesar un mes duplicaria millones de hechos y dispararia al
    doble las metricas de la Capa Oro. La FK de `fact_trip` hacia
    `audit.etl_execution_log` no se ve afectada: el log de la ejecucion
    anterior se conserva intacto como evidencia de auditoria.
    """
    cursor.execute("DELETE FROM silver.fact_trip WHERE source_file = %s;", (source_file,))
    purged = cursor.rowcount or 0
    cursor.execute(
        "DELETE FROM silver.rejected_trip_records WHERE source_file = %s;",
        (source_file,)
    )
    return purged


def process_parquet(file_path: str, execution_id, taxi_type: str = "yellow",
                    chunk_size: int = CHUNK_SIZE) -> ProcessResult:
    """Procesa un archivo Parquet mensual en lotes (streaming) para evitar
    cargar millones de registros completos en memoria RAM."""
    duck_conn = duckdb.connect()
    arrow_reader = duck_conn.execute(
        build_select_query(taxi_type), [file_path]
    ).to_arrow_reader(chunk_size)
    source_file_name = os.path.basename(file_path)

    conn = get_db_connection()
    cursor = conn.cursor()

    totals = {"raw": 0, "valid": 0, "rejected": 0, "loaded": 0}
    try:
        purged = purge_previous_load(cursor, source_file_name)
        conn.commit()
        if purged:
            print(f"Reproceso detectado: {purged:,} hechos previos de {source_file_name} eliminados.")

        for batch in arrow_reader:
            df = batch.to_pandas()
            totals["raw"] += len(df)

            valid_df, rejected_df = validate_trip_records(df)
            totals["valid"] += len(valid_df)
            totals["rejected"] += len(rejected_df)

            if not rejected_df.empty:
                log_rejected_records(execution_id, source_file_name, rejected_df)

            if not valid_df.empty:
                records_to_insert = build_fact_records(execution_id, source_file_name, valid_df)
                execute_values(cursor, INSERT_FACT_TRIP_SQL, records_to_insert, page_size=5000)
                conn.commit()
                totals["loaded"] += len(records_to_insert)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        duck_conn.close()

    return ProcessResult(
        raw_records=totals["raw"],
        valid_records=totals["valid"],
        rejected_records=totals["rejected"],
        loaded_records=totals["loaded"],
        purged_records=purged
    )

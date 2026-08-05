import datetime
import decimal
import json

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

from etl.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_execution(execution_id, pipeline_name="nyc_taxi_batch_pipeline", source_file=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO audit.etl_execution_log (
            execution_id, pipeline_name, source_file, status, started_at
        ) VALUES (%s, %s, %s, %s, %s);
    """, (str(execution_id), pipeline_name, source_file, 'RUNNING', datetime.datetime.now(datetime.timezone.utc)))
    conn.commit()
    cursor.close()
    conn.close()

def update_execution_progress(execution_id, raw_records_read):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE audit.etl_execution_log
        SET raw_records_read = %s
        WHERE execution_id = %s;
    """, (raw_records_read, str(execution_id)))
    conn.commit()
    cursor.close()
    conn.close()

def finish_execution(execution_id, raw_records, valid_records, rejected_records, loaded_records, duration_seconds):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE audit.etl_execution_log
        SET finished_at = %s,
            status = 'SUCCESS',
            raw_records_read = %s,
            valid_records = %s,
            rejected_records = %s,
            loaded_records = %s,
            duration_seconds = %s
        WHERE execution_id = %s;
    """, (datetime.datetime.now(datetime.timezone.utc), raw_records, valid_records, rejected_records, loaded_records, duration_seconds, str(execution_id)))
    conn.commit()
    cursor.close()
    conn.close()

def fail_execution(execution_id, error_message, duration_seconds=0):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE audit.etl_execution_log
        SET finished_at = %s,
            status = 'FAILED',
            error_message = %s,
            duration_seconds = %s
        WHERE execution_id = %s;
    """, (datetime.datetime.now(datetime.timezone.utc), error_message, duration_seconds, str(execution_id)))
    conn.commit()
    cursor.close()
    conn.close()

def start_file_log(execution_id, source_file):
    """Registra el inicio del procesamiento de un archivo (Fase 1: nombre de
    archivo, fecha/hora de inicio). Devuelve el file_log_id para cerrarlo luego."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO audit.etl_file_log (
            execution_id, source_file, status, started_at
        ) VALUES (%s, %s, %s, %s)
        RETURNING file_log_id;
    """, (str(execution_id), source_file, 'RUNNING', datetime.datetime.now(datetime.timezone.utc)))
    file_log_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return file_log_id


def finish_file_log(file_log_id, raw_records, valid_records, rejected_records, loaded_records):
    """Cierra el registro de auditoria de un archivo con el estado de la ingesta
    (Exito) y la cantidad de registros crudos leidos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE audit.etl_file_log
        SET finished_at = %s,
            status = 'SUCCESS',
            raw_records_read = %s,
            valid_records = %s,
            rejected_records = %s,
            loaded_records = %s
        WHERE file_log_id = %s;
    """, (datetime.datetime.now(datetime.timezone.utc), raw_records, valid_records, rejected_records, loaded_records, file_log_id))
    conn.commit()
    cursor.close()
    conn.close()


def fail_file_log(file_log_id, error_message):
    """Cierra el registro de auditoria de un archivo con estado de Error."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE audit.etl_file_log
        SET finished_at = %s,
            status = 'FAILED',
            error_message = %s
        WHERE file_log_id = %s;
    """, (datetime.datetime.now(datetime.timezone.utc), error_message, file_log_id))
    conn.commit()
    cursor.close()
    conn.close()


def _json_default(value):
    """Serializa a JSON los tipos que trae pandas/numpy y que `json` no conoce.

    Se convierten los escalares de numpy a su equivalente nativo (`.item()`)
    en vez de a texto, para que el payload de la DLQ conserve los tipos
    originales (numeros como numeros) y sea consultable con operadores JSONB.
    """
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return str(value)


def log_rejected_records(execution_id, source_file, rejected_df):
    """Envia a la Dead Letter Queue los registros descartados por calidad,
    guardando el payload original completo y la razon del descarte."""
    if rejected_df is None or rejected_df.empty:
        return

    payloads = rejected_df.drop(columns=["rejection_reason"]).to_dict(orient="records")
    reasons = rejected_df["rejection_reason"].tolist()

    records = [
        (str(execution_id), source_file, json.dumps(payload, default=_json_default), reason)
        for payload, reason in zip(payloads, reasons)
    ]

    conn = get_db_connection()
    cursor = conn.cursor()
    execute_values(cursor, """
        INSERT INTO silver.rejected_trip_records (
            execution_id, source_file, record_data, rejection_reason
        ) VALUES %s;
    """, records, page_size=5000)
    conn.commit()
    cursor.close()
    conn.close()

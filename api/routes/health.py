"""Endpoint de salud / telemetria del pipeline (auditoria expuesta)."""

import logging

from flask import Blueprint, request, jsonify

from api.database import db_cursor

logger = logging.getLogger(__name__)

health_bp = Blueprint('health', __name__)

DEFAULT_LIMIT = 10
MAX_LIMIT = 100

EXECUTIONS_QUERY = """
    SELECT execution_id, pipeline_name, source_file, status,
           started_at, finished_at, raw_records_read, valid_records,
           rejected_records, loaded_records, duration_seconds, error_message
    FROM audit.etl_execution_log
    ORDER BY started_at DESC
    LIMIT %s;
"""

FILE_LOGS_QUERY = """
    SELECT file_log_id, execution_id, source_file, status,
           started_at, finished_at, raw_records_read, valid_records,
           rejected_records, loaded_records, error_message
    FROM audit.etl_file_log
    ORDER BY started_at DESC
    LIMIT %s;
"""

# Se agrupa por razon ademas de contar: el total suelto no es accionable,
# mientras que el desglose muestra que regla de calidad esta descartando mas
# registros. El GROUP BY corre sobre una tabla chica (solo los descartes).
DLQ_SUMMARY_QUERY = """
    SELECT rejection_reason, COUNT(*) AS total
    FROM silver.rejected_trip_records
    GROUP BY rejection_reason
    ORDER BY total DESC;
"""


@health_bp.route('/api/v1/health', methods=['GET'])
def get_health():
    limit = request.args.get('limit', default=DEFAULT_LIMIT, type=int) or DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    try:
        with db_cursor() as cursor:
            cursor.execute(EXECUTIONS_QUERY, (limit,))
            executions = cursor.fetchall()

            cursor.execute(FILE_LOGS_QUERY, (limit,))
            file_logs = cursor.fetchall()

            cursor.execute(DLQ_SUMMARY_QUERY)
            dlq_breakdown = cursor.fetchall()

        total_rejected = sum(row["total"] for row in dlq_breakdown)

        return jsonify({
            "status": executions[0]["status"] if executions else "UNKNOWN",
            "database": "UP",
            "last_execution": executions[0] if executions else None,
            "recent_executions": executions,
            "recent_file_logs": file_logs,
            "total_rejected_in_dlq": total_rejected,
            "dlq_breakdown_by_reason": dlq_breakdown
        }), 200
    except Exception:
        # El detalle tecnico va al log del servidor, no al cliente: el mensaje de
        # una excepcion de psycopg2 puede filtrar host, usuario o esquema.
        logger.exception("Health check failed")
        return jsonify({
            "status": "ERROR",
            "database": "DOWN",
            "error": "No fue posible consultar la telemetria del pipeline"
        }), 503

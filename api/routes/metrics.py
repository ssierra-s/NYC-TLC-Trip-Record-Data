"""Endpoint analitico: metricas agregadas por zona (Capa Oro)."""

import logging

from flask import Blueprint, request, jsonify

from api.database import db_cursor

logger = logging.getLogger(__name__)

metrics_bp = Blueprint('metrics', __name__)

DEFAULT_LIMIT = 100
MAX_LIMIT = 500

# La consulta ataca directamente la tabla pre-agregada de la Capa Oro (una fila
# por anio/mes/zona, ~265 filas por mes) y nunca la tabla de hechos: el trabajo
# pesado ya lo hizo el procedimiento almacenado. El JOIN a dim_location es
# contra una dimension de 265 filas, y los filtros usan idx_gold_metrics_year_month.
BASE_QUERY = """
    SELECT m.metric_year AS year, m.metric_month AS month,
           m.pickup_location_id AS location_id, l.zone, l.borough,
           m.total_trips, m.total_revenue, m.average_trip_duration_seconds,
           m.peak_hour, m.tip_percentage, m.updated_at
    FROM gold.monthly_zone_metrics m
    LEFT JOIN silver.dim_location l ON m.pickup_location_id = l.location_id
    WHERE 1=1
"""


def _validate_filters(year, month, location_id):
    """Valida los filtros antes de tocar la base de datos. Un filtro absurdo
    debe costar un 400 inmediato, no una consulta."""
    if year is not None and not 2009 <= year <= 2100:
        return "El parametro 'year' debe estar entre 2009 y 2100."
    if month is not None and not 1 <= month <= 12:
        return "El parametro 'month' debe estar entre 1 y 12."
    if location_id is not None and not 1 <= location_id <= 265:
        return "El parametro 'location_id' debe estar entre 1 y 265."
    return None


@metrics_bp.route('/api/v1/metrics', methods=['GET'])
def get_metrics():
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    location_id = request.args.get('location_id', type=int)
    limit = request.args.get('limit', default=DEFAULT_LIMIT, type=int) or DEFAULT_LIMIT
    offset = request.args.get('offset', default=0, type=int) or 0

    validation_error = _validate_filters(year, month, location_id)
    if validation_error:
        return jsonify({"error": validation_error}), 400

    # Techo duro de paginacion: ninguna peticion puede pedir un resultado
    # ilimitado y tumbar la memoria del proceso.
    limit = max(1, min(limit, MAX_LIMIT))
    offset = max(0, offset)

    query = BASE_QUERY
    params = []

    # Los filtros se concatenan como placeholders (%s) y nunca por interpolacion
    # de strings: es lo que evita la inyeccion SQL.
    if year is not None:
        query += " AND m.metric_year = %s"
        params.append(year)
    if month is not None:
        query += " AND m.metric_month = %s"
        params.append(month)
    if location_id is not None:
        query += " AND m.pickup_location_id = %s"
        params.append(location_id)

    query += """
        ORDER BY m.metric_year DESC, m.metric_month DESC, m.total_revenue DESC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    try:
        with db_cursor() as cursor:
            cursor.execute(query, params)
            records = cursor.fetchall()

        return jsonify({
            "year": year,
            "month": month,
            "location_id": location_id,
            "limit": limit,
            "offset": offset,
            "count": len(records),
            "records": records
        }), 200
    except Exception:
        logger.exception("Metrics query failed")
        return jsonify({"error": "No fue posible consultar las metricas agregadas"}), 503

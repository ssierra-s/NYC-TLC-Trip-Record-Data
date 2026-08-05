"""Acceso a PostgreSQL para la API.

Se usa un pool de conexiones en vez de abrir/cerrar una conexion por request:
establecer una conexion nueva a PostgreSQL cuesta ~5-20 ms (TCP + handshake +
autenticacion) y, bajo concurrencia, agota el `max_connections` del servidor.
El pool acota el maximo de conexiones simultaneas que la API puede consumir,
que es justo lo que protege la disponibilidad del servicio.
"""

import os
from contextlib import contextmanager

from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_MIN_CONNECTIONS = int(os.getenv("DB_POOL_MIN", "1"))
DB_MAX_CONNECTIONS = int(os.getenv("DB_POOL_MAX", "10"))

_connection_pool = None


def get_connection_pool():
    """Crea el pool de forma perezosa (en el primer request, no al importar),
    para que la API pueda arrancar aunque PostgreSQL todavia no este listo."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=DB_MIN_CONNECTIONS,
            maxconn=DB_MAX_CONNECTIONS,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "taxi_dw"),
            user=os.getenv("POSTGRES_USER", "taxi_user"),
            password=os.getenv("POSTGRES_PASSWORD", "taxi_password"),
            cursor_factory=RealDictCursor
        )
    return _connection_pool


@contextmanager
def db_cursor():
    """Entrega un cursor del pool y garantiza la devolucion de la conexion.

    El `finally` con `putconn` es lo que evita la fuga de conexiones cuando una
    consulta lanza excepcion: sin el, cada error dejaria una conexion retenida
    hasta agotar el pool.
    """
    connection_pool = get_connection_pool()
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cursor:
            yield cursor
        # Consultas de solo lectura: el commit cierra la transaccion abierta para
        # que la conexion vuelva limpia al pool (evita 'idle in transaction').
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        connection_pool.putconn(conn)

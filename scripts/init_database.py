"""Aplica todas las migraciones SQL sobre un PostgreSQL ya existente.

Alternativa a Docker: `docker compose up -d postgres` ejecuta estos mismos
archivos automaticamente en el primer arranque via `db/init.sql`.

Las migraciones se descubren ordenadas por nombre (001_, 002_, ...) en vez de
listarse a mano, para que agregar una nueva no requiera tocar este script.
Todas son idempotentes (`CREATE ... IF NOT EXISTS`, `ON CONFLICT DO NOTHING`,
`CREATE OR REPLACE`), asi que volver a ejecutarlo es seguro.
"""

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "db" / "migrations"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "taxi_dw"),
        user=os.getenv("POSTGRES_USER", "taxi_user"),
        password=os.getenv("POSTGRES_PASSWORD", "taxi_password")
    )


def run_migrations():
    migrations = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migrations:
        raise SystemExit(f"No se encontraron migraciones en {MIGRATIONS_DIR}")

    conn = get_db_connection()
    try:
        # `with conn` hace COMMIT al salir sin excepcion y ROLLBACK si algo falla:
        # las migraciones se aplican como una unica transaccion atomica.
        with conn, conn.cursor() as cursor:
            for migration in migrations:
                print(f"Applying migration: {migration.name}")
                cursor.execute(migration.read_text(encoding="utf-8"))
        print(f"{len(migrations)} migraciones aplicadas correctamente.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_migrations()

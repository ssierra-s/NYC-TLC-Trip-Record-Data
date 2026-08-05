# NYC Taxi & Mobility Data Pipeline - Simón Movilidad

Pipeline de Ingeniería de Datos modular en Python y PostgreSQL con arquitectura por capas (**Bronce, Plata, Oro, Auditoría y DLQ**), API Flask y contenedores Docker. Procesa datos públicos de [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Quickstart

```bash
docker compose up -d --build
docker compose --profile etl run --rm etl python -m scripts.run_pipeline --taxi-type yellow --year 2024 --months 1 2 3
curl "http://localhost:5000/api/v1/metrics?year=2024&month=1&limit=5"
curl "http://localhost:5000/api/v1/health"
```

No hace falta crear el `.env`: el `docker-compose.yml` aplica defaults. Cópialo
(`cp .env.example .env`) solo si quieres cambiar credenciales o períodos.

Ver la guía completa (ejecución local sin Docker, variables de entorno, backup y
troubleshooting) en [SETUP.md](SETUP.md).

## Estructura del Proyecto

```text
nyc-taxi-data-pipeline/
├── api/                  # Servicio Flask (app, pool de conexiones, endpoints)
├── etl/                  # Módulos del pipeline (extract, transform, load)
├── db/migrations/        # Migraciones SQL: esquemas, auditoría, dimensiones, hechos, Oro, procedimiento
├── tests/                # 38 pruebas unitarias y de integración (pytest)
├── data/                 # Caché local de Parquet por capas
├── scripts/              # Runner del pipeline e inicializador de base de datos
└── docker/               # Dockerfiles de API (Gunicorn) y ETL
```

## Pruebas

```bash
pytest -v     # 38 pruebas, no requieren PostgreSQL levantado
```

## Documentación

- **[DOCUMENTACION.md](DOCUMENTACION.md)**: documento maestro — recorrido completo del proyecto, cada librería y por qué se eligió, cada decisión técnica y el guion del video.
- [DESIGN.md](DESIGN.md): decisiones de arquitectura, diagrama Entidad-Relación, estrategia de migración a streaming.
- [SETUP.md](SETUP.md): guía paso a paso de instalación, variables de entorno, backup/restore y troubleshooting.

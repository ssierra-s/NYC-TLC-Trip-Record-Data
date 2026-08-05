# Guía de Instalación y Ejecución (SETUP.md)

> Todos los comandos de esta guía se ejecutan desde la raíz del repositorio.
> Los módulos internos se invocan como paquete (`python -m ...`) y no como
> script suelto (`python api/app.py`), porque los imports son absolutos
> (`from api...`, `from etl...`) y Python solo los resuelve si el directorio
> raíz del proyecto está en `sys.path`. Al ejecutar un script directamente,
> Python agrega el directorio del script (no la raíz) a `sys.path`, por lo que
> `python api/app.py` o `python scripts/run_pipeline.py` fallan con
> `ModuleNotFoundError`. Usar `-m` evita ese problema.

## 0. Requisitos Previos

- Docker y Docker Compose (recomendado), o Python 3.10+ y PostgreSQL 14+ en local.
- Conexión a internet para descargar los archivos Parquet del portal TLC
  (~50 MB comprimidos por mes de Yellow Taxi).

---

## 1. Despliegue con Docker Compose (Recomendado)

### Paso 0 (opcional): configuración
```bash
cp .env.example .env      # Windows PowerShell: copy .env.example .env
```
No es obligatorio: `docker-compose.yml` usa interpolación `${VAR:-default}`, así
que el stack levanta igual sin `.env`. Copia el archivo solo si quieres cambiar
credenciales, puertos o los períodos por defecto.

### Paso 1: Levantar PostgreSQL y la API Flask
```bash
docker compose up -d --build
```
Esto crea:
- `taxi_postgres`: aplica automáticamente todas las migraciones de `db/migrations/`
  en el primer arranque (incluye las 265 zonas del diccionario oficial TLC en
  `dim_location`).
- `taxi_api_flask`: API servida con **Gunicorn** en `http://localhost:5000`.

Verifica que ambos estén sanos:
```bash
docker compose ps
curl http://localhost:5000/api/v1/health
```

### Paso 2: Ejecutar el Pipeline ETL
El servicio `etl` está bajo el perfil `etl` para que **no** se levante con
`up -d` (evita descargas no deseadas en cada arranque del stack):
```bash
docker compose --profile etl run --rm etl python -m scripts.run_pipeline --taxi-type yellow --year 2024 --months 1 2 3
```

Otras formas de indicar los períodos:
```bash
# Sin argumentos: toma TAXI_TYPE / START_YEAR / START_MONTH / MONTHS_TO_PROCESS del entorno
docker compose --profile etl run --rm etl

# 3 meses consecutivos con salto de año automático (2024-11, 2024-12, 2025-01)
docker compose --profile etl run --rm etl python -m scripts.run_pipeline --start 2024-11 --months-count 3

# Green Taxi (el pipeline resuelve solo las columnas lpep_* del dataset)
docker compose --profile etl run --rm etl python -m scripts.run_pipeline --taxi-type green --year 2024 --months 1 2 3
```

Los Parquet descargados se cachean en `./data/bronze` (volumen montado), por lo
que no se vuelven a descargar si ya existen.

**Reprocesar un mes es seguro:** antes de insertar, el pipeline borra los hechos
y los descartes previos de ese mismo archivo (`source_file`), así que volver a
ejecutar el mismo período no duplica registros ni infla las métricas de la Capa
Oro. Los logs de auditoría de la ejecución anterior se conservan intactos.

### Paso 3 (opcional): Ver logs
```bash
docker compose logs -f api
docker compose logs -f postgres
```

---

## 2. Ejecución Local (Sin Docker)

1. Crear el entorno virtual e instalar dependencias:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate      # Windows
   # source .venv/bin/activate # Linux/Mac
   pip install -r requirements.txt
   ```
2. Copiar `.env.example` a `.env` (aquí sí es necesario) y dejar
   `POSTGRES_HOST=localhost`.
3. Levantar solo la base de datos, o inicializar un PostgreSQL propio:
   ```bash
   docker compose up -d postgres        # opción A
   python -m scripts.init_database      # opción B: aplica db/migrations/*.sql
   ```
4. Ejecutar el pipeline ETL:
   ```bash
   python -m scripts.run_pipeline --taxi-type yellow --year 2024 --months 1 2 3
   ```
5. Levantar la API Flask (servidor de desarrollo):
   ```bash
   python -m api.app
   ```

---

## 3. Variables de Entorno

| Variable | Descripción | Default |
|---|---|---|
| `POSTGRES_HOST` | Host de PostgreSQL (`postgres` dentro de Compose, `localhost` en local) | `localhost` |
| `POSTGRES_PORT` | Puerto de PostgreSQL | `5432` |
| `POSTGRES_DB` | Nombre de la base de datos | `taxi_dw` |
| `POSTGRES_USER` | Usuario de la base de datos | `taxi_user` |
| `POSTGRES_PASSWORD` | Contraseña de la base de datos | `taxi_password` |
| `TAXI_TYPE` | Dataset por defecto (`yellow`/`green`) | `yellow` |
| `START_YEAR` | Año inicial por defecto | `2024` |
| `START_MONTH` | Mes inicial por defecto | `1` |
| `MONTHS_TO_PROCESS` | Cantidad de meses consecutivos | `3` |
| `API_PORT` | Puerto publicado de la API | `5000` |
| `API_WORKERS` | Procesos worker de Gunicorn | `2` |
| `DB_POOL_MIN` / `DB_POOL_MAX` | Tamaño del pool de conexiones de la API | `1` / `10` |

Ninguna credencial va hardcodeada: todo se parametriza vía entorno
(`etl/config.py`, `api/database.py`, `scripts/init_database.py`). `.env` está en
`.gitignore`; solo se versiona `.env.example`.

---

## 4. Pruebas Automatizadas (pytest)

```bash
pytest -v
```
38 pruebas, sin necesidad de PostgreSQL levantado:

| Archivo | Qué cubre |
|---|---|
| `tests/unit/test_validators.py` | Reglas de calidad: distancia ≤ 0, tarifa < 0, fechas inconsistentes, violaciones múltiples, invariante `válidos + rechazados = crudos` |
| `tests/unit/test_cleaner.py` | Reglas de transformación: prefijo `tpep_`/`lpep_` según dataset, cálculo de duración, orden de columnas del INSERT y tipos adaptables por psycopg2 |
| `tests/unit/test_downloader.py` | Nombre de archivo, caché, escritura atómica y limpieza tras descarga interrumpida |
| `tests/unit/test_periods.py` | Generación de meses consecutivos con salto de año |
| `tests/integration/test_api.py` | Contratos de ambos endpoints, tope de paginación, filtros como parámetros ligados, 400 en filtro inválido y 503 sin filtrar detalles internos |

---

## 5. Endpoints del API

### `GET /api/v1/metrics` — métricas agregadas por zona (Capa Oro)
Parámetros opcionales: `year`, `month`, `location_id`, `limit` (máx. 500), `offset`.
```bash
curl "http://localhost:5000/api/v1/metrics?year=2024&month=1&limit=5"
```
```json
{
  "year": 2024, "month": 1, "location_id": null,
  "limit": 5, "offset": 0, "count": 1,
  "records": [{
    "year": 2024, "month": 1, "location_id": 132,
    "zone": "JFK Airport", "borough": "Queens",
    "total_trips": 153240, "total_revenue": 4278390.50,
    "average_trip_duration_seconds": 1842.35,
    "peak_hour": 17, "tip_percentage": 18.42
  }]
}
```
Un filtro fuera de rango (`month=13`) devuelve `400` sin tocar la base de datos.

### `GET /api/v1/health` — telemetría de auditoría del pipeline
Parámetro opcional: `limit` (default 10, máx. 100).
```bash
curl "http://localhost:5000/api/v1/health?limit=5"
```
Retorna estado de la última ejecución, últimas N ejecuciones
(`recent_executions`), detalle por archivo procesado (`recent_file_logs`), total
en la Dead Letter Queue y el desglose por razón de descarte
(`dlq_breakdown_by_reason`). Si la base de datos no responde, devuelve `503` con
`"database": "DOWN"`.

---

## 6. Backup y Recuperación de la Base de Datos

Con el contenedor `taxi_postgres` corriendo:

**Backup completo (esquema + datos):**
```bash
docker exec taxi_postgres pg_dump -U taxi_user -d taxi_dw -F c -f /tmp/taxi_dw.dump
docker cp taxi_postgres:/tmp/taxi_dw.dump ./taxi_dw.dump
```

**Backup solo del esquema (útil para versionar la estructura):**
```bash
docker exec taxi_postgres pg_dump -U taxi_user -d taxi_dw --schema-only -f /tmp/schema.sql
```

**Restaurar:**
```bash
docker cp ./taxi_dw.dump taxi_postgres:/tmp/taxi_dw.dump
docker exec taxi_postgres pg_restore -U taxi_user -d taxi_dw --clean --if-exists /tmp/taxi_dw.dump
```

**Persistencia:** los datos sobreviven a `docker compose down` gracias al volumen
nombrado `postgres_data`. Solo se pierden con `docker compose down -v`.

---

## 7. Troubleshooting

- **`ModuleNotFoundError: No module named 'api'` o `'etl'`**: estás ejecutando un
  script directo (`python api/app.py`) en vez del módulo (`python -m api.app`).
- **`ForeignKeyViolation` en `pickup_location_id`/`dropoff_location_id`**:
  `silver.dim_location` no tiene las 265 zonas cargadas. Fuerza la
  reinicialización con `docker compose down -v && docker compose up -d`.
- **`docker compose run --rm etl ...` dice `no such service`**: falta el perfil.
  Usa `docker compose --profile etl run --rm etl ...`.
- **La API responde 503**: PostgreSQL no está accesible. Revisa
  `docker compose ps` y `docker compose logs postgres`.
- **`column "tpep_pickup_datetime" not found` al procesar Green**: estás
  forzando `--taxi-type yellow` sobre un archivo Green. El tipo de taxi debe
  coincidir con el archivo descargado.

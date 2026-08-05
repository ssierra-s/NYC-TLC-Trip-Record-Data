# Documentación Técnica Completa
### Pipeline ETL por lotes · NYC TLC Trip Record Data · Simón Movilidad

Este documento es el recorrido completo del proyecto: qué se construyó, con qué
herramientas, **por qué se eligió cada una** y qué se decidió en cada punto donde
había más de un camino posible.

Documentos complementarios: [DESIGN.md](DESIGN.md) (arquitectura y modelo E-R),
[SETUP.md](SETUP.md) (instalación y operación).

---

## Índice

1. [Resumen ejecutivo](#1-resumen-ejecutivo)
2. [Mapa: requisito de la prueba → dónde está resuelto](#2-mapa-requisito-de-la-prueba--dónde-está-resuelto)
3. [Arquitectura general](#3-arquitectura-general)
4. [Stack tecnológico: cada librería y por qué](#4-stack-tecnológico-cada-librería-y-por-qué)
5. [Recorrido del código, módulo por módulo](#5-recorrido-del-código-módulo-por-módulo)
6. [Modelo de datos](#6-modelo-de-datos)
7. [Auditoría en tres fases](#7-auditoría-en-tres-fases)
8. [Calidad del dato y Dead Letter Queue](#8-calidad-del-dato-y-dead-letter-queue)
9. [Capa Oro: el procedimiento almacenado](#9-capa-oro-el-procedimiento-almacenado)
10. [La API Flask](#10-la-api-flask)
11. [Contenedores y despliegue](#11-contenedores-y-despliegue)
12. [Estrategia de pruebas](#12-estrategia-de-pruebas)
13. [Rendimiento y optimización](#13-rendimiento-y-optimización)
14. [Seguridad y gobierno del dato](#14-seguridad-y-gobierno-del-dato)
15. [Evolución de Batch a Streaming](#15-evolución-de-batch-a-streaming)
16. [Correcciones aplicadas en la revisión final](#16-correcciones-aplicadas-en-la-revisión-final)
17. [Limitaciones conocidas](#17-limitaciones-conocidas)
18. [Guion del video (5 minutos)](#18-guion-del-video-5-minutos)

---

## 1. Resumen ejecutivo

El sistema descarga de forma programática los archivos Parquet mensuales de la
NYC TLC, los procesa **en lotes acotados en memoria**, los valida contra reglas
de negocio, los persiste en un modelo relacional dimensional en PostgreSQL con
integridad referencial, agrega las métricas de negocio con un procedimiento
almacenado PL/pgSQL, y expone el resultado por una API Flask contenerizada.
Todo el recorrido queda registrado en un sistema de auditoría de tres fases, y
los registros que no pasan las reglas de calidad se conservan en una Dead Letter
Queue con la razón exacta del descarte.

**Cifras del dataset de referencia** (Yellow Taxi 2024-01, medidas sobre el
archivo real): 2.964.624 viajes en un solo mes; ~3,4% de registros descartados
por reglas de calidad. Tres meses superan los 9 millones de registros, que es
justo el volumen que obliga a no cargar el archivo completo en memoria.

---

## 2. Mapa: requisito de la prueba → dónde está resuelto

| Requisito | Implementación | Archivo |
|---|---|---|
| Descarga batch programática, ≥3 meses | Iteración sobre períodos (año, mes) con URL predecible del CDN oficial | `etl/extract/downloader.py`, `etl/pipeline.py` |
| Dimensiones desde el diccionario de datos, relacionadas por FK | 4 dimensiones sembradas con los códigos oficiales TLC | `db/migrations/003_dimensions.sql` |
| Procesamiento eficiente (Big Data) | DuckDB + Arrow `RecordBatchReader`, lotes de 250.000 filas | `etl/transform/cleaner.py` |
| Auditoría Fase 1 (ID, inicio, archivo, crudos, estado) | Dos niveles: por ejecución y por archivo | `db/migrations/002`, `007`, `etl/load/audit.py` |
| Modelo relacional con integridad referencial | Tabla de hechos + 4 dimensiones + CHECK constraints | `db/migrations/004_fact_trips.sql` |
| Calidad Fase 2 + DLQ con razón de descarte | Validación vectorizada; descartes a tabla dedicada con payload JSONB | `etl/transform/validators.py`, `etl/load/audit.py` |
| Procedimiento almacenado PL/pgSQL | Recaudo, duración promedio, hora pico, % propina, por zona y mes | `db/migrations/006_procedures.sql` |
| Auditoría Fase 3 (cierre, cargados, duración) | `finish_execution()` tras ejecutar el procedimiento | `etl/pipeline.py`, `etl/load/audit.py` |
| API Flask en Docker, 2 endpoints | Métricas agregadas + salud/telemetría | `api/routes/metrics.py`, `api/routes/health.py` |
| Optimización de consultas | Consulta contra tabla pre-agregada, índices dirigidos, paginación acotada, pool | `db/migrations/005`, `api/database.py` |
| Pruebas automatizadas | 38 pruebas: transformación, calidad, descarga, períodos y contratos del API | `tests/` |
| Documentación técnica | DESIGN + SETUP + este documento | `*.md` |
| Propuesta Batch → Streaming | Sección 15 de este documento y sección 5 de DESIGN.md | — |

---

## 3. Arquitectura general

Se adoptó una **arquitectura de medallón** (Bronce → Plata → Oro), materializada
como esquemas separados dentro de PostgreSQL.

```
NYC TLC (CDN oficial, Parquet mensual)
        │  descarga batch, streaming a disco, atómica
        ▼
┌─────────────────────────────────────────────┐
│ BRONCE — dato crudo tal cual llega          │
│ Archivo Parquet en ./data/bronze            │
│ Auditoría: audit.etl_file_log (por archivo) │
└──────────────────┬──────────────────────────┘
                   │  DuckDB: lectura en lotes de 250k filas
                   ▼
┌─────────────────────────────────────────────┐
│ PLATA — dato limpio, validado y modelado    │
│ silver.fact_trip + 4 dimensiones (FK)       │
│ DLQ: silver.rejected_trip_records           │
└──────────────────┬──────────────────────────┘
                   │  CALL gold.sp_generate_monthly_zone_metrics()
                   ▼
┌─────────────────────────────────────────────┐
│ ORO — dato agregado listo para consumo      │
│ gold.monthly_zone_metrics                   │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
        API Flask + Gunicorn (Docker)
```

### Por qué separar en tres capas y no cargar directo a una tabla final

1. **Reprocesabilidad**: si mañana cambia una regla de negocio (por ejemplo, que
   una distancia mayor a 200 millas también sea inválida), la capa Bronce está
   intacta en disco y se reprocesa sin volver a descargar 500 MB por mes.
2. **Aislamiento de fallos**: un error en el cálculo de una métrica de la Capa
   Oro no compromete los hechos de la Capa Plata. Se vuelve a llamar el
   procedimiento y listo.
3. **Rendimiento de consulta**: la API nunca toca la tabla de hechos (millones de
   filas). Consulta la tabla pre-agregada (~265 filas por mes). La diferencia
   entre responder en milisegundos o en decenas de segundos está exactamente ahí.

### Por qué los esquemas viven dentro de una sola base de datos

Separar por esquemas (`bronze`, `silver`, `gold`, `audit`) y no por bases de
datos distintas permite que el procedimiento almacenado haga JOIN entre capas y
que las FK crucen esquemas (`silver.fact_trip` → `audit.etl_execution_log`), lo
que sería imposible entre bases separadas. Además, un único `pg_dump` respalda
todo el sistema.

---

## 4. Stack tecnológico: cada librería y por qué

### 4.1 DuckDB — lectura y proyección del Parquet

**Rol:** leer los archivos Parquet, proyectar solo las columnas necesarias,
normalizar tipos y entregar los datos por lotes.

**Por qué DuckDB y no pandas directo:**
`pandas.read_parquet()` materializa el archivo completo en memoria. Un mes de
Yellow Taxi son ~3 millones de filas × 19 columnas; tres meses son más de 9
millones. Eso son varios GB de RAM y el proceso muere en cualquier máquina
modesta o en un contenedor con límite de memoria.

DuckDB, en cambio:
- Es un motor OLAP embebido: no requiere servidor, se instala con `pip` y corre
  dentro del mismo proceso Python (cero infraestructura adicional).
- Lee Parquet de forma **columnar y con predicate/projection pushdown**: al pedir
  15 de las 19 columnas, las otras 4 ni siquiera se leen del disco.
- Expone `to_arrow_reader(chunk_size)`, que devuelve un iterador de lotes. La
  memoria pico queda acotada al tamaño de un lote (250.000 filas), sin importar
  si el archivo tiene 3 millones o 300 millones de filas.
- Permite hacer la limpieza de tipos y los `COALESCE` en SQL, que es más rápido y
  más legible que hacerlo en Python fila por fila.

**Alternativas consideradas:**
- **Polars**: excelente y con streaming nativo. Se descartó porque DuckDB
  permite expresar la proyección en SQL, lo cual encaja mejor con un proyecto
  donde el resto de la lógica pesada también es SQL (procedimiento almacenado).
- **PySpark**: sobredimensionado. Levantar una JVM y un cluster para 9 millones
  de filas es más costo operativo que beneficio.
- **pyarrow puro** (`ParquetFile.iter_batches`): habría funcionado, pero obliga a
  escribir la limpieza de tipos y los COALESCE a mano en Python.

### 4.2 pandas — validación de calidad por lote

**Rol:** recibir cada lote de Arrow, aplicar las reglas de calidad de forma
vectorizada y separar válidos de rechazados.

**Por qué:** las reglas de calidad son máscaras booleanas
(`df["trip_distance"] <= 0`), que pandas resuelve de forma vectorizada sobre
todo el lote en una sola operación en C, en lugar de iterar 250.000 veces en
Python. Trabajar sobre lotes ya acotados evita el problema de memoria que tendría
pandas si se le entregara el archivo completo.

### 4.3 PyArrow — el puente entre DuckDB y pandas

**Rol:** formato columnar en memoria compartido por ambos.

**Por qué:** es lo que permite que `batch.to_pandas()` sea casi gratuito: DuckDB
produce Arrow y pandas consume Arrow, sin serializar ni copiar los datos por el
camino. Es una dependencia de infraestructura, no una elección de estilo: sin
ella el `RecordBatchReader` no existe.

### 4.4 psycopg2 — driver de PostgreSQL

**Rol:** toda la comunicación con la base de datos.

**Por qué:**
- Es el driver más maduro y estable del ecosistema Python-PostgreSQL.
- `psycopg2.extras.execute_values` inserta miles de filas en **una sola sentencia
  SQL** (`INSERT ... VALUES (...), (...), (...)`). Frente a un `executemany`
  clásico, que hace un round-trip de red por fila, la diferencia sobre millones
  de registros es de órdenes de magnitud.
- `RealDictCursor` devuelve cada fila como diccionario, listo para `jsonify` en
  la API sin una capa de mapeo manual.
- `psycopg2.pool.ThreadedConnectionPool` da el pool de conexiones de la API sin
  dependencias extra.

Se usa la variante `psycopg2-binary` (con las librerías ya compiladas) para que
`pip install` funcione sin necesidad de tener `libpq` y un compilador de C en la
máquina del evaluador.

**Por qué no un ORM (SQLAlchemy / Django ORM):** el proyecto es un pipeline
analítico, no una aplicación transaccional. La carga masiva se hace con
`execute_values`, la agregación vive en un procedimiento almacenado y las
consultas de la API son SQL analítico con JOIN a dimensiones. Un ORM añadiría
una capa de abstracción que aquí solo estorba y esconde el plan de ejecución.

### 4.5 Flask — la API

**Rol:** exponer las métricas de la Capa Oro y la telemetría de auditoría.

**Por qué:** lo pide explícitamente la prueba, y encaja: son dos endpoints de
lectura. Flask aporta enrutamiento, parseo de query params y serialización JSON
sin imponer estructura. Se usan **Blueprints** (`health_bp`, `metrics_bp`) para
que cada endpoint viva en su módulo y la app se componga en un `create_app()`,
que es también lo que permite instanciar la aplicación en las pruebas sin
levantar un servidor.

### 4.6 Gunicorn — servidor WSGI

**Rol:** servir la aplicación Flask dentro del contenedor.

**Por qué:** `app.run()` es el servidor de desarrollo de Werkzeug: monohilo, sin
manejo de concurrencia real, y el propio Flask advierte en consola que no debe
usarse en producción. Gunicorn levanta varios procesos worker independientes,
reinicia los que fallan y maneja timeouts. En el contenedor corre con
`--workers 2 --threads 4`, parametrizable con `API_WORKERS`.

### 4.7 requests — descarga HTTP

**Rol:** bajar los Parquet del CDN de la TLC.

**Por qué:** `stream=True` + `iter_content(chunk_size=1MB)` escribe a disco por
bloques sin cargar el archivo entero en memoria. Con `urllib` de la librería
estándar se podría, pero requests da manejo de reintentos, timeouts y
`raise_for_status()` de forma mucho más legible.

### 4.8 python-dotenv — configuración

**Rol:** cargar las variables de entorno desde `.env`.

**Por qué:** la prueba exige parametrizar credenciales por variables de entorno.
`dotenv` permite que el mismo código funcione en local (leyendo `.env`) y en
Docker (leyendo las variables inyectadas por Compose), sin ninguna rama
condicional. **Ninguna credencial está hardcodeada** y `.env` está en
`.gitignore`; solo se versiona `.env.example`.

### 4.9 pytest — pruebas

**Rol:** validar reglas de transformación, filtros de calidad y contratos del API.

**Por qué:** fixtures y `monkeypatch` permiten probar los endpoints **sin
PostgreSQL levantado**, sustituyendo la capa de acceso a datos por un doble de
prueba. Eso hace que `pytest -v` corra en segundos en la máquina de cualquier
evaluador, que es exactamente lo que se necesita para que las pruebas se
ejecuten de verdad.

### 4.10 PostgreSQL 16 — la base de datos

**Por qué PostgreSQL y no otro motor:**
- Lo sugiere la prueba y soporta **PL/pgSQL** para el procedimiento almacenado.
- Integridad referencial real (FK con verificación), `CHECK` constraints, tipo
  `JSONB` para el payload de la DLQ y `UUID` nativo para los identificadores de
  ejecución.
- Funciones de ventana (`ROW_NUMBER() OVER (PARTITION BY ...)`), que es lo que
  resuelve el cálculo de la hora pico en una sola pasada.
- `INSERT ... ON CONFLICT DO UPDATE`, que hace idempotente el procedimiento de
  la Capa Oro.

### 4.11 Docker y Docker Compose

**Por qué:** la prueba valora que el entorno se levante con un comando. Compose
orquesta los tres servicios (base de datos, API, ETL), gestiona el orden de
arranque con `depends_on: condition: service_healthy` y monta las migraciones en
`/docker-entrypoint-initdb.d/` para que la base se inicialice sola. El servicio
`etl` está bajo un **perfil**, para que no se dispare una descarga de cientos de
megas cada vez que alguien levanta el stack.

---

## 5. Recorrido del código, módulo por módulo

### `etl/config.py`
Punto único de lectura de configuración. Todo lo demás importa desde aquí, así
que cambiar de fuente de configuración (por ejemplo, a un secret manager) es
tocar un solo archivo.

### `etl/extract/downloader.py` — Capa Bronce
Construye la URL (`{tipo}_tripdata_{año}-{mes:02d}.parquet`) y descarga.

Tres decisiones deliberadas:
1. **Se ataca el CDN oficial, no se hace scraping del HTML del portal.** La URL
   es predecible y estable; depender de la maqueta de una página web es frágil.
2. **Idempotencia**: si el archivo ya está en `data/bronze/`, no se vuelve a
   descargar. Reejecutar el pipeline no cuesta ancho de banda.
3. **Escritura atómica**: se baja a un archivo `.part` y solo al completarse se
   renombra. Sin esto, una descarga cortada (red caída, Ctrl+C) dejaría un
   Parquet truncado en disco que la siguiente ejecución daría por válido y
   saltaría, corrompiendo silenciosamente la carga. Es un fallo difícil de
   diagnosticar después, y evitarlo cuesta tres líneas.

### `etl/transform/cleaner.py` — el corazón del procesamiento
- `build_select_query(taxi_type)`: arma la proyección SQL. Resuelve el prefijo de
  las columnas de fecha según el dataset (`tpep_` en Yellow, `lpep_` en Green) y
  las normaliza a `pickup_datetime`/`dropoff_datetime`, de modo que **un solo
  pipeline sirve para los dos datasets**. Los `COALESCE` redirigen los valores
  nulos a los miembros "desconocido" de cada dimensión (vendor `-1`, ratecode
  `99`, location `264`), que es lo que impide que un nulo rompa una FK.
- `purge_previous_load()`: borra la carga anterior de ese mismo archivo antes de
  insertar. **Esto es lo que hace idempotente la recarga**: sin ello, reprocesar
  2024-01 insertaría 3 millones de hechos duplicados y la Capa Oro reportaría el
  doble de recaudo.
- `build_fact_records()`: construye las tuplas de inserción de forma vectorizada,
  con `Series.tolist()` columna por columna. Además de ser mucho más rápido que
  `iterrows()`, es **un requisito de corrección**: psycopg2 no sabe adaptar
  `numpy.int64` (lanza `can't adapt type`) y adapta mal `numpy.float64`;
  `tolist()` devuelve `int` y `float` nativos.
- `process_parquet()`: el bucle principal. Por cada lote: contar crudos → validar
  → mandar rechazados a la DLQ → insertar válidos → commit. El commit por lote es
  deliberado: en un archivo de 3 millones de filas, una única transacción gigante
  haría crecer el WAL sin control y un fallo al 90% perdería todo el trabajo.

### `etl/transform/validators.py` — reglas de calidad
Función pura, sin base de datos ni entrada/salida: recibe un DataFrame y
devuelve `(válidos, rechazados)`. Por eso es trivial de probar y por eso las
mismas reglas podrán reutilizarse tal cual en un motor de streaming.

### `etl/load/audit.py` — la gobernanza
Todas las escrituras a las tablas de auditoría y a la DLQ. Cada función abre y
cierra su propia conexión: son operaciones puntuales y de vida corta, y así el
registro de auditoría queda comprometido (`commit`) **con independencia de la
transacción de datos**. Es intencional: si la carga falla y hace rollback, el
log del fallo debe sobrevivir.

`_json_default()` serializa el payload de la DLQ conservando los tipos (los
escalares de numpy se convierten con `.item()`, no a texto), para que el `JSONB`
resultante sea consultable con operadores de PostgreSQL.

### `etl/pipeline.py` — la orquestación
- `build_periods()`: genera N meses consecutivos manejando el salto de año
  (2024-11 + 3 meses = 2024-11, 2024-12, **2025-01**).
- `run_pipeline()`: abre la auditoría, itera períodos, ejecuta el procedimiento
  de la Capa Oro y cierra la auditoría. El `try/except` global garantiza que
  **ninguna excepción deja la ejecución en estado `RUNNING` para siempre**: se
  marca `FAILED` con el mensaje de error y se relanza la excepción para que el
  contenedor termine con código distinto de cero (importante para un
  orquestador tipo Airflow o un cron con alertas).

### `api/` — el servicio
`create_app()` (patrón factory) + Blueprints + `db_cursor()` como context
manager sobre el pool. El context manager es lo que garantiza que la conexión
vuelva al pool **incluso si la consulta lanza excepción**; sin ese `finally`,
cada error dejaría una conexión retenida hasta agotar el pool y tumbar el
servicio.

---

## 6. Modelo de datos

### Esquema estrella

Se eligió un **modelo estrella** (una tabla de hechos rodeada de dimensiones
desnormalizadas) y no un copo de nieve. Razón: las consultas analíticas hacen
JOIN de hechos contra dimensiones pequeñas; normalizar `dim_location` en
`borough` → `zone` → `service_zone` añadiría dos JOIN más para ahorrar unos
kilobytes. En analítica, el estrella es el estándar precisamente por eso.

### Tabla de hechos: `silver.fact_trip`

| Columna | Tipo | Razón |
|---|---|---|
| `trip_id` | `BIGSERIAL` PK | Clave sustituta: el dataset de la TLC no trae identificador de viaje |
| `execution_id` | `UUID` FK → auditoría | Linaje: de qué ejecución del pipeline salió cada fila |
| `source_file` | `VARCHAR` | Linaje de archivo + criterio de purga para recargas idempotentes |
| `vendor_id`, `pickup_location_id`, `dropoff_location_id`, `payment_type_id`, `rate_code_id` | FK a dimensiones | Integridad referencial |
| `pickup_datetime`, `dropoff_datetime` | `TIMESTAMP` | Base de las agregaciones temporales |
| `trip_distance`, `fare_amount`, `tip_amount`, `total_amount`, ... | `NUMERIC` | **Nunca `FLOAT` para dinero**: el binario de punto flotante no representa exactamente valores decimales y los errores se acumulan al sumar millones de filas |
| `trip_duration_seconds` | `INTEGER` | Métrica pre-calculada en la carga, para no recalcular la resta de fechas en cada agregación |

Y tres `CHECK` constraints (`trip_distance > 0`, `dropoff > pickup`,
`fare_amount >= 0`) que son la **última línea de defensa**: aunque alguien
insertara datos saltándose el pipeline, la base de datos rechaza lo que viola las
reglas de negocio.

### Dimensiones

Las cuatro se siembran con los códigos del **diccionario oficial de datos** de la
TLC, no con valores inventados: `dim_vendor`, `dim_rate_code`,
`dim_payment_type` y `dim_location` (las 265 zonas del `taxi_zone_lookup`).

Cada dimensión incluye un **miembro "desconocido"** (vendor `-1`, ratecode `99`,
location `264` = "Unknown"). Esta es una decisión de diseño importante: sin él,
un registro con `VendorID` nulo obligaría a elegir entre descartar el viaje
(perder un hecho válido por un atributo secundario) o poner un `NULL` que rompe
la lógica de los JOIN. Con el miembro desconocido, el hecho se conserva y la
incertidumbre queda explícita y contable.

*Verificación real:* se comprobó contra `yellow_tripdata_2024-01.parquet` que
todos los valores presentes en el archivo (`RatecodeID` 1-6 y 99, `payment_type`
0-4, `VendorID` 1, 2 y 6, `PULocationID` 1-265) están cubiertos por las
dimensiones sembradas, es decir, la carga no produce violaciones de FK.

### Capa Oro: `gold.monthly_zone_metrics`
Granularidad: una fila por (año, mes, zona de recogida), con
`UNIQUE (metric_year, metric_month, pickup_location_id)`. Esa restricción única
es lo que habilita el `ON CONFLICT DO UPDATE` del procedimiento y, por tanto, su
idempotencia.

---

## 7. Auditoría en tres fases

Se implementó en **dos niveles** de granularidad, porque una ejecución puede
abarcar varios archivos y ambos datos son necesarios:

- **`audit.etl_execution_log`** — una fila por ejecución del pipeline.
- **`audit.etl_file_log`** — una fila por archivo Parquet procesado, ligada por
  FK a la ejecución.

| Fase | Momento | Qué se registra |
|---|---|---|
| **Fase 1 — Ingesta** | Al arrancar la ejecución y al abrir cada archivo | `execution_id` (UUID), `started_at`, `source_file`, estado `RUNNING`; al cerrar el archivo, los **registros crudos leídos** y el estado (`SUCCESS`/`FAILED`) |
| **Fase 2 — Calidad** | Durante la validación de cada lote | Conteo de válidos y rechazados; cada descarte va a la DLQ con su razón |
| **Fase 3 — Cierre** | Tras ejecutar el procedimiento de la Capa Oro | `finished_at`, total de registros cargados en la capa final, `duration_seconds` y estado final |

**Por qué un UUID y no un serial** como identificador de ejecución: permite que
varias instancias del contenedor ETL corran en paralelo (por ejemplo, un mes
cada una) generando identificadores únicos sin coordinarse contra la base de
datos.

**Por qué la auditoría usa su propia transacción:** si la carga de datos falla y
hace rollback, el registro del fallo tiene que sobrevivir. Una auditoría que
desaparece junto con el error que documenta no sirve de nada.

---

## 8. Calidad del dato y Dead Letter Queue

### Reglas aplicadas
| Regla | Criterio | Razón de negocio |
|---|---|---|
| `INVALID_DISTANCE` | `trip_distance <= 0` | Un viaje sin recorrido no es un viaje; distorsiona la distancia media |
| `INVALID_FARE` | `fare_amount < 0` | Un monto negativo es una anulación mal registrada, no un ingreso |
| `INVALID_DATES` | `dropoff <= pickup` | Error de captura; produciría duraciones nulas o negativas |

Un mismo registro puede violar varias reglas: se registran **todas**, unidas por
`|`. Diagnosticar con "esta fila estaba mal por A, B y C" es mucho más útil que
saber solo la primera causa encontrada.

Nota sobre el criterio: una tarifa de **cero** sí se acepta (viaje sin cargo,
`payment_type = 'No charge'`), solo se descartan las negativas. Está cubierto por
una prueba explícita para que la decisión quede documentada en el código.

### La DLQ: `silver.rejected_trip_records`
Guarda el **payload original completo** en `JSONB` más la razón, el archivo de
origen y el `execution_id`. Se decidió guardar el registro entero, y no solo su
identificador, porque así el descarte es auditable y reprocesable sin volver a
abrir el Parquet original: es la diferencia entre poder decir "descartamos
102.000 registros" y poder demostrar exactamente cuáles y por qué.

*Medición real sobre el primer lote de 50.000 registros de 2024-01:* 1.688
descartes (3,4%), repartidos en 856 por distancia, 720 por tarifa negativa, 99
por ambas y 13 por distancia + fechas inconsistentes.

El endpoint de salud expone el desglose agregado por razón, que es la métrica que
permite detectar una degradación en la fuente (si de pronto el 40% de los
registros se descarta por fechas, algo cambió aguas arriba).

---

## 9. Capa Oro: el procedimiento almacenado

`gold.sp_generate_monthly_zone_metrics()` calcula, por año, mes y zona de
recogida: total de viajes, recaudo total, duración promedio, **hora pico** y
**porcentaje de propina**.

**Por qué la agregación vive en la base de datos y no en Python:** mover 9
millones de filas por la red hacia el proceso Python para sumarlas allí es el
antipatrón clásico. PostgreSQL agrega donde viven los datos, usa los índices y
devuelve solo el resultado (unos cientos de filas). Además, lo pide la prueba.

**Estructura:** se apoya en CTEs encadenadas para que la lógica sea legible:
`base_trips` (extrae año, mes, hora) → `hourly_counts` (cuenta viajes por hora y
los rankea con `ROW_NUMBER()`) → `peak_hours` (se queda con el rango 1) →
`aggregated` (las métricas restantes) → INSERT final con JOIN.

**Detalles de cálculo:**
- La **hora pico** se resuelve con una función de ventana en lugar de con una
  subconsulta correlacionada por zona: una sola pasada en vez de una consulta
  por cada una de las 265 zonas.
- El **porcentaje de propina** se calcula como
  `SUM(tip_amount) / SUM(fare_amount)` (razón de totales) y **no** como el
  promedio de los porcentajes individuales. Son cosas distintas: la segunda le
  daría el mismo peso a una carrera de 5 dólares que a una de 200. Se protege la
  división con un `CASE WHEN SUM(fare_amount) > 0`.
- El `ON CONFLICT ... DO UPDATE` permite volver a llamar el procedimiento tras
  cargar más meses sin duplicar filas ni tener que vaciar la tabla.

---

## 10. La API Flask

### `GET /api/v1/metrics` — consumo analítico
Filtros opcionales y combinables: `year`, `month`, `location_id`, más paginación
(`limit`, `offset`).

Decisiones de optimización y disponibilidad:
- **Consulta contra la tabla pre-agregada de la Capa Oro**, nunca contra la tabla
  de hechos. El trabajo pesado ya lo hizo el procedimiento almacenado.
- **Índice `idx_gold_metrics_year_month`** alineado con el filtro principal.
- **Techo duro de paginación** (`LIMIT` máximo 500 impuesto por el servidor, no
  por el cliente): ninguna petición puede pedir un resultado ilimitado y agotar
  la memoria del proceso.
- **Validación de filtros antes de consultar**: `month=13` devuelve `400` sin
  llegar a abrir una conexión. Un filtro absurdo debe costar barato.
- **Parámetros ligados** (`%s`) en todos los filtros, nunca interpolación de
  strings: es la defensa contra inyección SQL.

### `GET /api/v1/health` — telemetría del pipeline
Devuelve estado de la última ejecución, las últimas N ejecuciones, el detalle por
archivo procesado, el total en la DLQ y **el desglose por razón de descarte**.
Sirve como health check real (si la base no responde, `503` con
`"database": "DOWN"`) y como panel de control del ETL.

Los mensajes de excepción **no se devuelven al cliente**: el detalle va al log
del servidor y la respuesta es genérica. El texto de una excepción de psycopg2
puede contener host, usuario y esquema de la base de datos.

---

## 11. Contenedores y despliegue

**Tres servicios:** `postgres` (con volumen nombrado para persistencia y
healthcheck con `pg_isready`), `api` (Gunicorn, usuario no-root, healthcheck
HTTP) y `etl` (perfil `etl`, se invoca a demanda).

**Dockerfiles separados para API y ETL:** la API no necesita DuckDB ni pandas
para responder consultas; separar mantiene la imagen del servicio expuesto más
pequeña y con menos superficie de ataque.

**Inicialización automática de la base de datos:** las migraciones se montan en
`/docker-entrypoint-initdb.d/` y PostgreSQL las ejecuta en orden en el primer
arranque. El evaluador no tiene que correr ningún script de esquema a mano.

**Configuración sin `.env` obligatorio:** el `docker-compose.yml` usa
interpolación `${VAR:-default}`. Compose lee automáticamente el `.env` del
proyecto si existe y aplica los defaults si no. Esto importa porque `.env` está
en `.gitignore`: con `env_file: .env`, un clon limpio del repositorio fallaba al
levantar con *"env file .env not found"* — el evaluador se estrellaba en el
primer comando.

**Caché de descargas:** `./data` se monta como volumen, así que los Parquet
sobreviven entre ejecuciones del contenedor efímero.

---

## 12. Estrategia de pruebas

38 pruebas, todas ejecutables **sin PostgreSQL levantado**.

| Archivo | Qué valida |
|---|---|
| `test_validators.py` | Cada regla de calidad, el caso límite de tarifa cero, violaciones múltiples, que el validador no mute la entrada y la invariante `válidos + rechazados = crudos leídos` |
| `test_cleaner.py` | Reglas de transformación: prefijo correcto por dataset, cálculo de duración (incluido un viaje que cruza medianoche), orden de columnas del INSERT y que los tipos sean adaptables por psycopg2 |
| `test_downloader.py` | Construcción del nombre, caché, escritura correcta y que una descarga interrumpida no deje archivo residual |
| `test_periods.py` | Generación de meses consecutivos con salto de año y validación de entradas |
| `test_api.py` | Contratos de ambos endpoints, tope de paginación, filtros como parámetros ligados, `400` ante filtro inválido y `503` sin filtrar detalles internos |

**Por qué mockear la base de datos y no usar una real:** una prueba que necesita
infraestructura no se ejecuta. Sustituyendo la capa de acceso a datos por un
doble de prueba, `pytest` corre en ~2 segundos en cualquier máquina, que es la
condición para que las pruebas se corran de verdad y no solo existan.

La invariante `válidos + rechazados = crudos` merece mención aparte: es la
propiedad que sostiene la confiabilidad de toda la tabla de auditoría. Si alguna
vez deja de cumplirse, los conteos reportados serían mentira.

---

## 13. Rendimiento y optimización

| Técnica | Dónde | Efecto |
|---|---|---|
| Lectura por lotes (250k filas) | `cleaner.py` | Memoria acotada e independiente del tamaño del archivo |
| Proyección de columnas en DuckDB | `build_select_query` | Solo se leen del Parquet las columnas que se usan |
| Construcción vectorizada de tuplas | `build_fact_records` | Evita el bucle Python fila por fila; además garantiza tipos nativos |
| `execute_values` con `page_size=5000` | `cleaner.py` | Una sentencia por cada 5.000 filas en vez de un round-trip por fila |
| Commit por lote | `cleaner.py` | Evita transacciones gigantes y crecimiento descontrolado del WAL |
| Agregación en el motor SQL | Procedimiento almacenado | No se mueven millones de filas por la red |
| Índice compuesto `(pickup_location_id, pickup_datetime)` | `004` | Alineado con el `GROUP BY` del procedimiento |
| Índice `(metric_year, metric_month)` | `005` | Alineado con el filtro principal del endpoint |
| Índice sobre `source_file` | `008` | Hace barata la purga del reproceso |
| Consulta contra tabla pre-agregada | `metrics.py` | Respuesta en milisegundos: ~265 filas por mes en lugar de millones |
| Pool de conexiones | `api/database.py` | Elimina el coste de conexión por request y acota el consumo de `max_connections` |
| Paginación con techo de servidor | `metrics.py` | Protege memoria y disponibilidad |
| Caché de descargas | `downloader.py` | Reejecutar no vuelve a bajar cientos de megas |

---

## 14. Seguridad y gobierno del dato

- **Credenciales fuera del código**: todo por variables de entorno; `.env` en
  `.gitignore`, solo se versiona `.env.example` con valores de ejemplo.
- **Sin inyección SQL**: todos los parámetros de usuario viajan como parámetros
  ligados de psycopg2, nunca concatenados en la cadena SQL.
- **Sin filtración de detalles internos**: las excepciones se registran en el
  servidor; el cliente recibe un mensaje genérico.
- **Contenedor de la API sin root**: la aplicación corre como `appuser`.
- **Validación de entrada en el borde**: rangos verificados antes de tocar la
  base de datos.
- **Linaje completo**: cada hecho sabe de qué ejecución (`execution_id`) y de qué
  archivo (`source_file`) proviene; cada descarte también. Se puede responder
  "¿de dónde salió este número?" hasta el archivo de origen.
- **Trazabilidad de calidad**: la DLQ conserva la evidencia de todo lo que se
  descartó y por qué, que es lo que permite auditar decisiones de calidad
  después del hecho.

---

## 15. Evolución de Batch a Streaming

La prueba pide proponer cómo migrar a streaming manteniendo calidad, gobierno y
seguridad. La propuesta, en orden de implementación:

**1. Ingesta.** Sustituir la descarga mensual por un tópico de **Apache Kafka**
(o Redpanda) `taxi-events`, donde cada viaje finalizado se publica como evento
individual. La clave de partición debe ser `PULocationID`: garantiza que todos
los eventos de una zona lleguen ordenados al mismo consumidor, que es lo que
necesitan las agregaciones por zona.

**2. Contratos de esquema.** **Schema Registry** con Avro o Protobuf, en modo de
compatibilidad `BACKWARD`. Hoy el contrato lo impone implícitamente el `CAST` de
DuckDB sobre el Parquet; en streaming, el contrato debe ser explícito y
verificado en el productor, para que un cambio de esquema aguas arriba se
detecte al publicar y no tres capas más abajo.

**3. Procesamiento.** **Apache Flink** (o Spark Structured Streaming) aplicando
**las mismas reglas de `validators.py`**, evento por evento en vez de por lote.
Este es el punto clave del diseño actual: al ser una función pura sin
dependencias de base de datos ni de entrada/salida, la lógica de calidad se
traslada tal cual, sin reescribirla ni arriesgar divergencias entre el mundo
batch y el streaming. Las dimensiones se materializan como *lookup tables* en el
motor de streaming, alimentadas por CDC (Debezium) desde PostgreSQL.

**4. DLQ como tópico.** Los eventos que no pasan la validación se publican en
`taxi-events-dlq` con la razón del descarte en la cabecera del mensaje: misma
semántica de auditoría que hoy, pero desacoplada y reprocesable con solo mover
el offset del consumidor.

**5. Auditoría continua.** El concepto de "ejecución con inicio y fin" desaparece;
lo reemplazan métricas continuas (throughput, *consumer lag*, tasa de rechazo por
regla) hacia Prometheus/Grafana, con alertas por umbral. Las agregaciones pasan a
ventanas temporales (*tumbling windows*) que alimentan incrementalmente el
equivalente de `monthly_zone_metrics`.

**6. Manejo de datos tardíos.** Un viaje puede llegar minutos u horas después de
haber terminado. Se procesa por **event time** (no *processing time*), con
*watermarks* y una tolerancia explícita de datos tardíos; lo que llegue fuera de
esa ventana va a un canal de corrección, nunca se descarta en silencio.

**7. Transición sin corte (arquitectura Lambda).** Durante la migración conviven
las dos rutas: el histórico servido por la ruta batch actual y lo reciente por la
ruta de streaming, unificados en una vista. Cuando la ruta de streaming demuestre
paridad de resultados contra el batch durante un período de validación, se retira
la ruta antigua.

**8. Seguridad y gobierno.** Cifrado en tránsito (TLS 1.3) y en reposo (KMS),
autenticación mTLS o SASL/SCRAM entre productores y consumidores, ACLs por
tópico con principio de mínimo privilegio, y un catálogo de datos
(OpenMetadata / DataHub) que mantenga el linaje y las definiciones de calidad
consistentes entre ambos mundos durante la convivencia.

---

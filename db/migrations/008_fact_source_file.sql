-- Migration 008: Trazabilidad de origen e idempotencia de recarga
-- `source_file` permite saber de que archivo Parquet proviene cada hecho y,
-- sobre todo, reprocesar un mes sin duplicar millones de filas: el pipeline
-- borra por `source_file` antes de insertar (ver etl/transform/cleaner.py).
ALTER TABLE silver.fact_trip
    ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_fact_trip_source_file
    ON silver.fact_trip(source_file);

-- La DLQ se purga con el mismo criterio al reprocesar un archivo.
CREATE INDEX IF NOT EXISTS idx_rejected_source_file
    ON silver.rejected_trip_records(source_file);

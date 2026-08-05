-- Migration 007: Auditoria por archivo (Fase 1 - Trazabilidad de ingesta)
-- Complementa a audit.etl_execution_log (que resume la ejecucion completa,
-- potencialmente de varios meses) con un registro por CADA archivo Parquet
-- procesado: nombre de archivo, registros crudos leidos y estado individual.
CREATE TABLE IF NOT EXISTS audit.etl_file_log (
    file_log_id BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES audit.etl_execution_log(execution_id),
    source_file VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    raw_records_read BIGINT DEFAULT 0,
    valid_records BIGINT DEFAULT 0,
    rejected_records BIGINT DEFAULT 0,
    loaded_records BIGINT DEFAULT 0,
    error_message TEXT,
    CONSTRAINT chk_file_log_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_file_log_execution ON audit.etl_file_log(execution_id);

-- Migration 002: Audit Tables & DLQ
CREATE TABLE IF NOT EXISTS audit.etl_execution_log (
    execution_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_file VARCHAR(255),
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    raw_records_read BIGINT DEFAULT 0,
    valid_records BIGINT DEFAULT 0,
    rejected_records BIGINT DEFAULT 0,
    loaded_records BIGINT DEFAULT 0,
    duration_seconds NUMERIC(14, 3),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_execution_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'))
);

CREATE TABLE IF NOT EXISTS silver.rejected_trip_records (
    rejected_id BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES audit.etl_execution_log(execution_id),
    source_file VARCHAR(255) NOT NULL,
    record_data JSONB NOT NULL,
    rejection_reason VARCHAR(500) NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

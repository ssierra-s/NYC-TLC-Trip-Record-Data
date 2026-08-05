-- Migration 005: Gold Metrics
CREATE TABLE IF NOT EXISTS gold.monthly_zone_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_year SMALLINT NOT NULL,
    metric_month SMALLINT NOT NULL,
    pickup_location_id INTEGER NOT NULL REFERENCES silver.dim_location(location_id),
    total_trips BIGINT NOT NULL,
    total_revenue NUMERIC(18, 2) NOT NULL,
    average_trip_duration_seconds NUMERIC(14, 2) NOT NULL,
    peak_hour SMALLINT,
    tip_percentage NUMERIC(8, 4),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metric_year, metric_month, pickup_location_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_metrics_year_month ON gold.monthly_zone_metrics(metric_year, metric_month);

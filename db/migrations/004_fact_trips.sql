-- Migration 004: Fact Trips
CREATE TABLE IF NOT EXISTS silver.fact_trip (
    trip_id BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES audit.etl_execution_log(execution_id),
    vendor_id SMALLINT REFERENCES silver.dim_vendor(vendor_id),
    pickup_location_id INTEGER REFERENCES silver.dim_location(location_id),
    dropoff_location_id INTEGER REFERENCES silver.dim_location(location_id),
    payment_type_id SMALLINT REFERENCES silver.dim_payment_type(payment_type_id),
    rate_code_id SMALLINT REFERENCES silver.dim_rate_code(rate_code_id),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    trip_distance NUMERIC(12, 3) NOT NULL,
    passenger_count SMALLINT,
    fare_amount NUMERIC(14, 2) NOT NULL,
    extra_amount NUMERIC(14, 2) DEFAULT 0,
    mta_tax NUMERIC(14, 2) DEFAULT 0,
    tip_amount NUMERIC(14, 2) DEFAULT 0,
    tolls_amount NUMERIC(14, 2) DEFAULT 0,
    total_amount NUMERIC(14, 2) NOT NULL,
    trip_duration_seconds INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_positive_distance CHECK (trip_distance > 0),
    CONSTRAINT chk_valid_dates CHECK (dropoff_datetime > pickup_datetime),
    CONSTRAINT chk_non_negative_fare CHECK (fare_amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_dt ON silver.fact_trip(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_trip_pu_loc ON silver.fact_trip(pickup_location_id);
CREATE INDEX IF NOT EXISTS idx_fact_trip_pu_loc_pickup ON silver.fact_trip(pickup_location_id, pickup_datetime);

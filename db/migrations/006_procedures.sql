-- Migration 006: Stored Procedures
CREATE OR REPLACE PROCEDURE gold.sp_generate_monthly_zone_metrics()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gold.monthly_zone_metrics (
        metric_year, metric_month, pickup_location_id, total_trips, total_revenue,
        average_trip_duration_seconds, peak_hour, tip_percentage, updated_at
    )
    WITH base_trips AS (
        SELECT 
            EXTRACT(YEAR FROM pickup_datetime)::SMALLINT AS m_year,
            EXTRACT(MONTH FROM pickup_datetime)::SMALLINT AS m_month,
            pickup_location_id,
            EXTRACT(HOUR FROM pickup_datetime)::SMALLINT AS hr,
            trip_duration_seconds,
            fare_amount,
            tip_amount,
            total_amount
        FROM silver.fact_trip
    ),
    hourly_counts AS (
        SELECT 
            m_year, m_month, pickup_location_id, hr, COUNT(*) AS trips_in_hour,
            ROW_NUMBER() OVER(PARTITION BY m_year, m_month, pickup_location_id ORDER BY COUNT(*) DESC) AS rnk
        FROM base_trips
        GROUP BY m_year, m_month, pickup_location_id, hr
    ),
    peak_hours AS (
        SELECT m_year, m_month, pickup_location_id, hr AS peak_hr
        FROM hourly_counts
        WHERE rnk = 1
    ),
    aggregated AS (
        SELECT 
            bt.m_year,
            bt.m_month,
            bt.pickup_location_id,
            COUNT(*) AS total_trips,
            SUM(bt.total_amount) AS total_revenue,
            AVG(bt.trip_duration_seconds) AS avg_duration,
            ROUND(
                (CASE WHEN SUM(bt.fare_amount) > 0 THEN (SUM(bt.tip_amount) / SUM(bt.fare_amount)) * 100 ELSE 0 END)::NUMERIC, 
                4
            ) AS tip_pct
        FROM base_trips bt
        GROUP BY bt.m_year, bt.m_month, bt.pickup_location_id
    )
    SELECT 
        a.m_year,
        a.m_month,
        a.pickup_location_id,
        a.total_trips,
        ROUND(a.total_revenue, 2),
        ROUND(a.avg_duration, 2),
        ph.peak_hr,
        a.tip_pct,
        CURRENT_TIMESTAMP
    FROM aggregated a
    JOIN peak_hours ph ON a.m_year = ph.m_year AND a.m_month = ph.m_month AND a.pickup_location_id = ph.pickup_location_id
    ON CONFLICT (metric_year, metric_month, pickup_location_id) DO UPDATE SET
        total_trips = EXCLUDED.total_trips,
        total_revenue = EXCLUDED.total_revenue,
        average_trip_duration_seconds = EXCLUDED.average_trip_duration_seconds,
        peak_hour = EXCLUDED.peak_hour,
        tip_percentage = EXCLUDED.tip_percentage,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;

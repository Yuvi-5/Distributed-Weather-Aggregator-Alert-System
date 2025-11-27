CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw observations collected from edge nodes and external APIs.
CREATE TABLE IF NOT EXISTS observations (
    id BIGSERIAL,
    city_id TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temp_c DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_kph DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    rain_mm DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, observed_at)
);

SELECT create_hypertable('observations', 'observed_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_observations_city_time ON observations (city_id, observed_at DESC);

-- Precomputed windowed aggregates for fast dashboard queries.
CREATE TABLE IF NOT EXISTS aggregates (
    id BIGSERIAL,
    city_id TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_width INTERVAL NOT NULL,
    temp_avg DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    temp_max DOUBLE PRECISION,
    humidity_avg DOUBLE PRECISION,
    wind_avg DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, bucket_start),
    UNIQUE (city_id, bucket_start, bucket_width)
);

SELECT create_hypertable('aggregates', 'bucket_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_aggregates_city_bucket ON aggregates (city_id, bucket_start DESC);

-- Alerts emitted by the rule engine.
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    city_id TEXT NOT NULL,
    level TEXT NOT NULL,
    rule TEXT NOT NULL,
    message TEXT NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_city_time ON alerts (city_id, triggered_at DESC);

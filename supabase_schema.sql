-- ============================================================
-- Fischer Data App – Supabase Schema
-- Run this in the Supabase SQL Editor (one-time setup).
-- Idempotent: safe to re-run; will not drop existing data.
-- ============================================================


-- ============================================================
-- TABLE 1: combined_raw
-- Minute-level merged sensor readings (long / EAV format).
-- ============================================================
CREATE TABLE IF NOT EXISTS combined_raw (
    bbl             VARCHAR     NOT NULL,        -- Borough-Block-Lot (normalized: no dashes)
    timestamp       TIMESTAMPTZ NOT NULL,        -- Original sensor timestamp, UTC stored
    sensor_name     VARCHAR     NOT NULL,        -- Column name from the uploaded file
    reading_value   DOUBLE PRECISION,            -- Sensor reading (NULL = no data for interval)
    data_year       INTEGER,                     -- Year extracted from timestamp (for filtering)

    -- Composite primary key enables upsert on re-processing
    PRIMARY KEY (bbl, timestamp, sensor_name)
);

COMMENT ON TABLE combined_raw IS
    'Minute-level (raw) sensor readings, one row per (building, timestamp, sensor).';
COMMENT ON COLUMN combined_raw.bbl IS
    'Normalized BBL – spaces and dashes removed, leading zeros preserved.';
COMMENT ON COLUMN combined_raw.reading_value IS
    'NULL when no sensor reading exists for this interval.';


-- ============================================================
-- TABLE 2: resampled_15min
-- Quarter-hour resampled sensor readings with quality flags.
-- ============================================================
CREATE TABLE IF NOT EXISTS resampled_15min (
    bbl             VARCHAR     NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    sensor_name     VARCHAR     NOT NULL,
    reading_value   DOUBLE PRECISION,
    data_year       INTEGER,
    is_stale        BOOLEAN     NOT NULL DEFAULT FALSE,  -- Sensor had 3+ consecutive identical non-zero readings
    is_inexact      BOOLEAN     NOT NULL DEFAULT FALSE,  -- Original timestamp was not on a 15-min boundary
    zero_flag       VARCHAR     NOT NULL DEFAULT 'Clear', -- 'Clear' | 'Single' | 'Repeated'

    PRIMARY KEY (bbl, timestamp, sensor_name)
);

COMMENT ON TABLE resampled_15min IS
    'Quarter-hour resampled sensor readings with quality flags, one row per (building, timestamp, sensor).';
COMMENT ON COLUMN resampled_15min.is_stale IS
    'TRUE when this sensor had 3 or more consecutive identical non-zero readings at this interval.';
COMMENT ON COLUMN resampled_15min.is_inexact IS
    'TRUE when the source timestamp was not exactly on a 15-minute boundary (:00/:15/:30/:45).';
COMMENT ON COLUMN resampled_15min.zero_flag IS
    'Per-row zero-reading flag: Clear = no zeros, Single = isolated zero, Repeated = 2+ consecutive zeros.';


-- ============================================================
-- INDEXES
-- The PRIMARY KEY already creates a unique B-tree index on
-- (bbl, timestamp, sensor_name).  These additional indexes
-- support common query patterns without partitioning.
-- ============================================================

-- Filter / aggregate by building
CREATE INDEX IF NOT EXISTS idx_combined_raw_bbl
    ON combined_raw (bbl);

CREATE INDEX IF NOT EXISTS idx_resampled_15min_bbl
    ON resampled_15min (bbl);

-- Time-range queries within a building
CREATE INDEX IF NOT EXISTS idx_combined_raw_bbl_ts
    ON combined_raw (bbl, timestamp);

CREATE INDEX IF NOT EXISTS idx_resampled_15min_bbl_ts
    ON resampled_15min (bbl, timestamp);

-- Annual slicing (data_year used in WHERE clauses for year filtering)
CREATE INDEX IF NOT EXISTS idx_combined_raw_data_year
    ON combined_raw (data_year);

CREATE INDEX IF NOT EXISTS idx_resampled_15min_data_year
    ON resampled_15min (data_year);

-- Quality flag filtering on resampled table
CREATE INDEX IF NOT EXISTS idx_resampled_15min_flags
    ON resampled_15min (bbl, is_stale, zero_flag);

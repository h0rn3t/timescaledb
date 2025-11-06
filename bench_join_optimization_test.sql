-- TimescaleDB Join Optimization Test Script
-- This script provides several optimized approaches for joining large hypertables
-- with time-range conditions from a dimension table.

-- ============================================================================
-- SETUP: Verify Settings
-- ============================================================================

-- Ensure chunk exclusion features are enabled
SHOW timescaledb.enable_chunk_skipping;
SHOW timescaledb.enable_qual_propagation;
SHOW timescaledb.enable_constraint_exclusion;
SHOW timescaledb.enable_chunk_append;

-- Enable if needed:
-- SET timescaledb.enable_chunk_skipping = ON;
-- SET timescaledb.enable_qual_propagation = ON;
-- SET timescaledb.enable_constraint_exclusion = ON;

-- ============================================================================
-- APPROACH 1: Dynamic SQL with Batching (RECOMMENDED)
-- ============================================================================
-- This approach generates OR conditions that TimescaleDB can optimize

CREATE OR REPLACE FUNCTION bench.query_sensor_data_batched(
    p_sensor_ids int4[],
    p_start_times timestamptz[],
    p_end_times timestamptz[],
    p_batch_size int DEFAULT 1000
)
RETURNS TABLE(
    sensor_id int4,
    measurement_time timestamptz,
    measurement_value int4,
    period_start_time timestamptz,
    period_end_time timestamptz
)
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query text;
    or_conditions text[];
    i int;
    batch_start int;
    batch_end int;
    num_periods int;
    global_time_min timestamptz;
    global_time_max timestamptz;
BEGIN
    num_periods := array_length(p_sensor_ids, 1);

    IF num_periods IS NULL OR num_periods = 0 THEN
        RETURN;
    END IF;

    -- Process in batches to keep query size reasonable
    FOR batch_start IN 1..num_periods BY p_batch_size LOOP
        batch_end := LEAST(batch_start + p_batch_size - 1, num_periods);

        -- Reset for this batch
        or_conditions := ARRAY[]::text[];
        global_time_min := NULL;
        global_time_max := NULL;

        -- Build OR conditions and track time bounds
        FOR i IN batch_start..batch_end LOOP
            or_conditions := array_append(
                or_conditions,
                format('(s.sensor_id = %s AND s.measurement_time >= %L AND s.measurement_time < %L)',
                       p_sensor_ids[i],
                       p_start_times[i],
                       p_end_times[i])
            );

            -- Track global time range for additional pruning
            IF global_time_min IS NULL OR p_start_times[i] < global_time_min THEN
                global_time_min := p_start_times[i];
            END IF;
            IF global_time_max IS NULL OR p_end_times[i] > global_time_max THEN
                global_time_max := p_end_times[i];
            END IF;
        END LOOP;

        -- Build query with proper chunk exclusion via OR conditions
        sql_query := format($query$
            SELECT
                s.sensor_id,
                s.measurement_time,
                s.measurement_value,
                p.start_time,
                p.end_time
            FROM bench.sensor_data s
            JOIN bench.active_periods p
              ON s.sensor_id = p.sensor_id
             AND s.measurement_time >= p.start_time
             AND s.measurement_time < p.end_time
            WHERE (%s)
              AND s.measurement_time >= %L
              AND s.measurement_time < %L
              AND p.status = 'DONE'
        $query$,
            array_to_string(or_conditions, ' OR '),
            global_time_min,
            global_time_max
        );

        RAISE NOTICE 'Processing batch % to % (% periods)', batch_start, batch_end, batch_end - batch_start + 1;

        -- Execute and return results
        RETURN QUERY EXECUTE sql_query;
    END LOOP;
END;
$$;

-- Example usage:
-- SELECT * FROM bench.query_sensor_data_batched(
--     ARRAY(SELECT sensor_id FROM bench.active_periods WHERE status = 'DONE' LIMIT 100),
--     ARRAY(SELECT start_time FROM bench.active_periods WHERE status = 'DONE' LIMIT 100),
--     ARRAY(SELECT end_time FROM bench.active_periods WHERE status = 'DONE' LIMIT 100),
--     50  -- batch size
-- );

-- ============================================================================
-- APPROACH 2: Pre-Aggregated Time Bounds (SIMPLE)
-- ============================================================================
-- Force chunk exclusion by providing explicit time range per sensor

-- Test query:
EXPLAIN (ANALYZE, BUFFERS)
WITH time_bounds AS (
    SELECT
        sensor_id,
        min(start_time) AS global_start,
        max(end_time) AS global_end
    FROM bench.active_periods
    WHERE status = 'DONE'
    GROUP BY sensor_id
),
filtered_sensor_data AS (
    -- This CTE will have proper chunk exclusion
    SELECT s.*
    FROM bench.sensor_data s
    JOIN time_bounds tb
      ON s.sensor_id = tb.sensor_id
     AND s.measurement_time >= tb.global_start
     AND s.measurement_time < tb.global_end
)
SELECT
    s.sensor_id,
    s.measurement_time,
    s.measurement_value,
    p.start_time,
    p.end_time
FROM filtered_sensor_data s
JOIN bench.active_periods p
  ON s.sensor_id = p.sensor_id
 AND s.measurement_time >= p.start_time
 AND s.measurement_time < p.end_time
WHERE p.status = 'DONE'
LIMIT 1000;

-- ============================================================================
-- APPROACH 3: Per-Sensor Processing
-- ============================================================================
-- Query one sensor at a time with explicit bounds

CREATE OR REPLACE FUNCTION bench.query_single_sensor(
    p_sensor_id int4,
    p_start_time timestamptz,
    p_end_time timestamptz
)
RETURNS TABLE(
    sensor_id int4,
    measurement_time timestamptz,
    measurement_value int4,
    period_start_time timestamptz,
    period_end_time timestamptz
)
STABLE
LANGUAGE SQL
AS $$
    SELECT
        s.sensor_id,
        s.measurement_time,
        s.measurement_value,
        p.start_time,
        p.end_time
    FROM bench.sensor_data s
    JOIN bench.active_periods p
      ON s.sensor_id = p.sensor_id
     AND s.measurement_time >= p.start_time
     AND s.measurement_time < p.end_time
    WHERE s.sensor_id = p_sensor_id
      AND s.measurement_time >= p_start_time
      AND s.measurement_time < p_end_time
      AND p.status = 'DONE';
$$;

-- Use from application with parallel execution:
-- SELECT * FROM bench.query_single_sensor(123, '2025-01-01', '2025-12-31');

-- ============================================================================
-- APPROACH 4: Materialized Lookup Table
-- ============================================================================
-- Create a helper table with explicit date keys for better planning

-- One-time setup:
CREATE TABLE IF NOT EXISTS bench.active_periods_daily (
    date_key date NOT NULL,
    sensor_id int4 NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    period_id int4 NOT NULL,
    status text
);

-- Populate (run when active_periods changes):
TRUNCATE bench.active_periods_daily;

INSERT INTO bench.active_periods_daily
SELECT
    d.date_key,
    ap.sensor_id,
    ap.start_time,
    ap.end_time,
    ap.id,
    ap.status
FROM bench.active_periods ap
CROSS JOIN LATERAL (
    SELECT generate_series(
        ap.start_time::date,
        ap.end_time::date,
        '1 day'::interval
    )::date AS date_key
) d
WHERE ap.status = 'DONE';

-- Create indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_active_periods_daily_lookup
    ON bench.active_periods_daily (date_key, sensor_id, start_time, end_time);

CREATE INDEX IF NOT EXISTS idx_active_periods_daily_sensor
    ON bench.active_periods_daily (sensor_id, date_key);

ANALYZE bench.active_periods_daily;

-- Query with date equality (better for planner):
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT ON (s.sensor_id, s.measurement_time)
    s.sensor_id,
    s.measurement_time,
    s.measurement_value,
    p.start_time,
    p.end_time
FROM bench.sensor_data s
JOIN bench.active_periods_daily p
  ON s.sensor_id = p.sensor_id
 AND s.measurement_time::date = p.date_key
 AND s.measurement_time >= p.start_time
 AND s.measurement_time < p.end_time
WHERE s.measurement_time >= '2025-01-01'
  AND s.measurement_time < '2025-12-31'
LIMIT 1000;

-- ============================================================================
-- DIAGNOSTIC QUERIES
-- ============================================================================

-- Check chunk distribution
SELECT
    hypertable_name,
    chunk_name,
    range_start,
    range_end,
    is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sensor_data'
ORDER BY range_start DESC
LIMIT 20;

-- Check if chunks are being excluded (compare these numbers):
-- 1. Total chunks
SELECT count(*) AS total_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sensor_data';

-- 2. Chunks that should match your query (with explicit WHERE)
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*)
FROM bench.sensor_data
WHERE measurement_time >= '2025-07-01'
  AND measurement_time < '2025-07-10'
  AND sensor_id = 11204312;

-- If "actual rows" is much larger than expected, chunk exclusion isn't working

-- ============================================================================
-- TESTING METHODOLOGY
-- ============================================================================

-- 1. Test with explicit conditions (baseline - should work):
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM bench.sensor_data s
WHERE
  (s.sensor_id = 123 AND s.measurement_time >= '2025-01-01' AND s.measurement_time < '2025-01-02')
  OR
  (s.sensor_id = 124 AND s.measurement_time >= '2025-01-03' AND s.measurement_time < '2025-01-04')
LIMIT 100;

-- 2. Test with JOIN (problematic):
EXPLAIN (ANALYZE, BUFFERS)
SELECT s.*
FROM bench.sensor_data s
JOIN bench.active_periods p
  ON s.sensor_id = p.sensor_id
 AND s.measurement_time >= p.start_time
 AND s.measurement_time < p.end_time
WHERE p.sensor_id IN (123, 124)
  AND p.status = 'DONE'
LIMIT 100;

-- 3. Test with batched approach:
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM bench.query_sensor_data_batched(
    ARRAY[123, 124],
    ARRAY['2025-01-01'::timestamptz, '2025-01-03'::timestamptz],
    ARRAY['2025-01-02'::timestamptz, '2025-01-04'::timestamptz],
    10
);

-- ============================================================================
-- PERFORMANCE COMPARISON TEMPLATE
-- ============================================================================

-- Measure execution time for different approaches:
\timing on

-- Baseline (explicit OR):
-- [Run your query]

-- Approach 1 (batched):
-- [Run your query]

-- Approach 2 (time bounds):
-- [Run your query]

-- Approach 3 (per-sensor):
-- [Run your query]

\timing off

-- ============================================================================
-- RECOMMENDATIONS
-- ============================================================================
/*
For your 100B row / 20M period use case, I recommend:

1. **Short-term (immediate)**: Use APPROACH 2 (Pre-Aggregated Time Bounds)
   - Simple to implement
   - No schema changes
   - Will provide significant improvement by enabling chunk exclusion

2. **Medium-term (best performance)**: Use APPROACH 1 (Dynamic SQL Batching)
   - Process 1000-10000 periods at a time
   - Parallel execution from application
   - Full chunk exclusion support

3. **Long-term (scalable architecture)**:
   - Implement APPROACH 4 (Materialized Lookup Table)
   - Add application-level caching of frequently accessed periods
   - Consider time-based partitioning of active_periods
   - Use parallel workers: SET max_parallel_workers_per_gather = 4;

4. **Additional optimizations**:
   - Increase work_mem for large sorts: SET work_mem = '256MB';
   - Tune columnar scan settings:
     SET timescaledb.max_tuples_decompressed_per_dml = 100000;
   - Monitor with: SELECT * FROM timescaledb_information.job_stats;

5. **Avoid**:
   - Generic JOINs with BETWEEN on hypertables (doesn't prune chunks)
   - Functions called per-period (8x slower due to repeated decompression)
   - Overly complex CTEs (planner can't optimize through them)
*/

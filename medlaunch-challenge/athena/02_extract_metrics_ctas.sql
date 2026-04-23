-- =============================================================================
-- Stage 1: Extract Facility Metrics — CTAS to Parquet
-- Healthcare Facility Accreditation Data Pipeline
-- =============================================================================
-- Output columns (6): facility_id, facility_name, employee_count,
--   number_of_offered_services, expiry_date_of_first_accreditation, state
-- Why Parquet? Columnar + Snappy compression reduces Athena scan cost by
-- 10-100x vs raw JSON. Athena charges $5/TB scanned.
-- Replace medlaunch-techchallenge-rasagyna before running.
-- Run AFTER 01_create_table.sql has executed successfully.
-- =============================================================================

CREATE TABLE facility_metrics
WITH (
    format              = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location   = 's3://medlaunch-techchallenge-rasagyna/output/facility_metrics/'
)
AS
SELECT
    facility_id,
    facility_name,
    employee_count,
    CARDINALITY(services)                       AS number_of_offered_services,
    CAST(
        MIN(accreditation.valid_until) AS DATE
    )                                           AS expiry_date_of_first_accreditation,
    location.state                              AS state

FROM healthcare_facilities
CROSS JOIN UNNEST(accreditations) AS t(accreditation)
WHERE CARDINALITY(accreditations) > 0
GROUP BY
    facility_id,
    facility_name,
    employee_count,
    location.state,
    services;

-- Verification query (run after CTAS):
-- SELECT * FROM facility_metrics ORDER BY expiry_date_of_first_accreditation ASC;

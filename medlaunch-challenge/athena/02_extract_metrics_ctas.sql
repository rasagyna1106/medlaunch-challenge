-- =============================================================================
-- Stage 1: Extract Facility Metrics — CTAS to Parquet
-- Healthcare Facility Accreditation Data Pipeline
-- =============================================================================
-- Purpose : Read from the external table, compute per-facility KPIs, and
--           persist the results as compressed Parquet in S3.
--
-- Why Parquet?
--   • Columnar format — downstream queries scan only the columns they need.
--   • Snappy compression reduces S3 storage cost and Athena scan cost.
--   • Athena charges $5 per TB scanned; Parquet typically gives 10-100x
--     reduction in bytes scanned vs. raw JSON.
--
-- Fields produced:
--   facility_id                     — primary key
--   facility_name                   — display name
--   employee_count                  — headcount
--   number_of_offered_services      — CARDINALITY of the services array
--   expiry_date_of_first_accreditation — earliest valid_until across all
--                                        accreditation records for this facility
--
-- Run AFTER 01_create_table.sql has executed successfully.
-- =============================================================================

CREATE TABLE facility_metrics
WITH (
    format            = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://medlaunch-techchallenge-rasagyna/output/facility_metrics/'
)
AS
SELECT
    facility_id,
    facility_name,
    employee_count,

    -- Count how many services this facility offers
    CARDINALITY(services)                                           AS number_of_offered_services,

    -- Find the soonest-expiring accreditation across all accreditation records
    -- CAST to DATE so MIN() comparison is chronological, not lexicographic
    CAST(
        MIN(accreditation.valid_until)
        AS DATE
    )                                                               AS expiry_date_of_first_accreditation,

    -- Expose state for downstream filtering / grouping
    location.state                                                  AS state

FROM healthcare_facilities

-- Unnest the accreditations array so we can aggregate across all entries
CROSS JOIN UNNEST(accreditations) AS t(accreditation)

-- Only include facilities that actually have accreditation records
WHERE CARDINALITY(accreditations) > 0

GROUP BY
    facility_id,
    facility_name,
    employee_count,
    location.state,
    services    -- required because CARDINALITY(services) is not an aggregate
;


-- =============================================================================
-- Verification query — run this after the CTAS to confirm results
-- =============================================================================
-- SELECT *
-- FROM facility_metrics
-- ORDER BY expiry_date_of_first_accreditation ASC;

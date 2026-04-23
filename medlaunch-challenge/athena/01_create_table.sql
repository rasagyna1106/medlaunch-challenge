-- =============================================================================
-- Stage 1: Athena External Table Definition
-- Healthcare Facility Accreditation Data Pipeline
-- =============================================================================
-- Purpose : Define an external table over raw NDJSON files in S3 so that
--           Athena can query nested facility records without any ETL step.
-- SerDe   : org.openx.data.jsonserde.JsonSerDe  (OpenX JsonSerDe)
--           Handles nested objects and arrays natively.
-- Cost tip: Partition the S3 prefix by date (e.g. /year=2025/month=04/) to
--           enable partition pruning and minimize data scanned per query.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_facilities (
    facility_id     STRING          COMMENT 'Unique facility identifier',
    facility_name   STRING          COMMENT 'Human-readable facility name',

    location STRUCT<
        address : STRING,
        city    : STRING,
        state   : STRING,
        zip     : STRING
    >                               COMMENT 'Physical location of the facility',

    employee_count  INT             COMMENT 'Total number of employees',

    services        ARRAY<STRING>   COMMENT 'List of medical services offered',

    labs ARRAY<STRUCT<
        lab_name       : STRING,
        certifications : ARRAY<STRING>
    >>                              COMMENT 'Lab units and their certifications',

    accreditations ARRAY<STRUCT<
        accreditation_body : STRING,
        accreditation_id   : STRING,
        valid_until        : STRING  -- stored as STRING; cast to DATE in queries
    >>                              COMMENT 'Accreditation records with expiry dates'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true',   -- skip unparseable rows, do not fail job
    'case.insensitive'      = 'true'    -- tolerate mixed-case field names in source
)
STORED AS TEXTFILE
LOCATION 's3://medlaunch-techchallenge-rasagyna/raw/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'skip.header.line.count' = '0'
);

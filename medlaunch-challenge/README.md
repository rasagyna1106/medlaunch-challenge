# MedLaunch AWS Data Engineering Challenge
**Submitted by:** Rasagyna Peddapalli | rasagyna.p@gmail.com

---

## Stage Selection Rationale

All four stages were completed. Stages 1 and 2 were chosen as the primary stages because they directly reflect the core data engineering responsibilities in the MedLaunch role — SQL analytics on cloud data lakes and Python-based ETL with proper validation and error handling. Stage 1 demonstrates Athena-specific patterns like CTAS, JsonSerDe, and Parquet output for cost optimization. Stage 2 demonstrates production Python practices including structured logging, boto3 exception handling, and resilient JSON parsing. Stages 3 and 4 were added to show the full serverless architecture — Lambda for event-driven ingestion and Step Functions for workflow orchestration with async Athena polling, S3 result promotion, and SNS failure alerting.

---

## Architecture

```
S3 raw/
  |
  |-- Athena (Stage 1) --> CTAS Parquet --> S3 output/facility_metrics/
  |
  |-- Python (Stage 2) --> Filter expiring --> S3 output/expiring/
  |
  |-- S3 Upload Event
        |
        Lambda: medlaunch-facility-processor (Stage 3)
          |
          Start Athena query --> return queryExecutionId
          |
          Step Functions: medlaunch-accreditation-pipeline (Stage 4)
            |
            Wait 10s --> Check status --> Succeeded: copy to production/
                                      --> Failed: SNS alert email
```

---

## Repository Structure

```
medlaunch-challenge/
  README.md
  data/
    sample_facilities.ndjson       8 records (5 core + 3 edge cases)
  athena/
    01_create_table.sql            External table over S3 JSON
    02_extract_metrics_ctas.sql    CTAS to Parquet with facility KPIs
  python/
    filter_expiring_accreditations.py
    requirements.txt
  lambda/
    handler.py                     Stage 3 - S3 triggered Lambda
    athena_status_checker.py       Stage 4 - Athena polling Lambda
    requirements.txt
  stepfunctions/
    pipeline.json                  Stage 4 - State machine definition
  iam/
    least_privilege_policy.json    Minimal IAM permissions
```

---

## Stage 1 — Athena SQL

Creates an external table over raw NDJSON in S3 using the OpenX JsonSerDe, then runs a CTAS query to extract facility metrics and write Parquet output.

**Why Parquet:** Columnar and Snappy-compressed. Reduces Athena scan cost by 10-100x versus raw JSON. Athena charges $5 per TB scanned so format choice matters at scale.

**Why external table:** Raw data stays in S3 untouched. Athena queries it directly with no ETL preprocessing step, no data movement, no extra cost.

**Why ignore.malformed.json:** A single bad record in source data does not abort the entire query. Malformed rows are skipped and logged.

Fields extracted: facility_id, facility_name, employee_count, number_of_offered_services, expiry_date_of_first_accreditation, state.

**Run:**
```sql
-- Run in Athena Query Editor with database set to default
-- Run 01_create_table.sql first, then 02_extract_metrics_ctas.sql
-- Verify: SELECT * FROM facility_metrics ORDER BY expiry_date_of_first_accreditation;
```

---

## Stage 2 — Python

Reads all NDJSON files from S3, filters facilities with any accreditation expiring within 6 months, writes filtered records back to a separate S3 prefix.

**Why relativedelta not timedelta:** relativedelta(months=6) advances by calendar months correctly. timedelta(days=180) gives wrong results at month boundaries and leap years.

**Why dual JSON parser:** Source data arrives as either strict NDJSON or glued JSON where objects are concatenated without a wrapper array. The script handles both transparently without requiring pre-normalization.

**Why structured logging:** Every S3 call, record count, and filter result is logged with timestamps. Makes debugging straightforward in CloudWatch if deployed as Lambda.

**Run:**
```bash
cd python
pip install -r requirements.txt
python filter_expiring_accreditations.py \
  --source-bucket medlaunch-techchallenge-rasagyna \
  --source-prefix raw/ \
  --dest-bucket   medlaunch-techchallenge-rasagyna \
  --dest-prefix   output/expiring/ \
  --months        6 \
  --region        us-east-1
```

---

## Stage 3 — Lambda

Triggered automatically when a new JSON or NDJSON file lands in the raw/ S3 prefix. Starts an Athena query counting accredited facilities per state and returns the QueryExecutionId.

**Why not poll inside Lambda:** Athena queries take 5-60+ seconds. Polling inside Lambda burns execution time and risks the 15-minute timeout. Lambda starts the query and exits immediately. Step Functions handles the polling.

**Why prefix scoped trigger:** The trigger fires only on raw/ prefix to prevent recursive invocations when the Lambda writes output files to the same bucket.

**Deploy:**
```
Function name:   medlaunch-facility-processor
Runtime:         Python 3.12
Handler:         lambda_function.handler
Environment:
  ATHENA_DATABASE        = default
  ATHENA_RESULTS_BUCKET  = medlaunch-techchallenge-rasagyna
  ATHENA_WORKGROUP       = primary
Trigger:         S3 - bucket: medlaunch-techchallenge-rasagyna, prefix: raw/
```

---

## Stage 4 — Step Functions

Orchestrates the full pipeline: invoke Lambda, wait for Athena, copy results to production on success, send SNS alert on failure.

**Why Step Functions over polling in Lambda:** Native Wait state pauses execution without consuming compute. The workflow resumes automatically when the wait completes. This is the correct serverless pattern for async service calls.

**Why every state has a Catch block:** No error can silently pass through. Any failure at any stage routes to SendFailureAlert which publishes the full error details to SNS before terminating.

**State machine flow:**
```
StartAthenaQuery --> ExtractQueryId --> WaitForQuery (10s)
  --> CheckQueryStatus --> EvaluateStatus
        --> SUCCEEDED: CopyResultsToProduction --> PipelineSucceeded
        --> FAILED:    SendFailureAlert --> PipelineFailed
        --> RUNNING:   back to WaitForQuery
```

**Deploy:** Create state machine using stepfunctions/pipeline.json. Attach role with Lambda invoke, S3 CopyObject, and SNS publish permissions.

---

## Error Handling Test Cases

Three edge case records were added to data/sample_facilities.ndjson to demonstrate robust error handling:

**FAC33333 - blank valid_until:** Python logs a warning and skips the accreditation without crashing. The facility is excluded from the expiring list.

**FAC44444 - empty accreditations array:** Python returns False immediately on the empty array check. Athena CTAS excludes this facility via the CARDINALITY > 0 filter.

**FAC55555 - expired in 2020:** Python correctly catches past-expiry dates within the 6-month window. Demonstrates the filter works for backlogged historical data, not just future dates.

---

## IAM — Least Privilege

Policy in iam/least_privilege_policy.json. No wildcards. No admin permissions.

| Permission | Scope |
|---|---|
| s3:GetObject | raw/ prefix only |
| s3:PutObject | output/ and athena-results/ only |
| athena:StartQueryExecution | primary workgroup only |
| glue:GetTable | two specific tables only |
| logs:PutLogEvents | specific Lambda log group only |

---

## Cost

Everything ran within AWS Free Tier limits. Total cost: $0.00.

| Service | Usage | Free Tier |
|---|---|---|
| S3 | Less than 1MB | 5GB/month |
| Athena | Less than 1MB scanned | 1TB/month |
| Lambda | Less than 20 invocations | 1M/month |
| Step Functions | Less than 30 transitions | 4000/month |
| SNS | Less than 5 notifications | 1M/month |

---

## Cleanup

```bash
aws s3 rb s3://medlaunch-techchallenge-rasagyna --force
aws lambda delete-function --function-name medlaunch-facility-processor
aws lambda delete-function --function-name medlaunch_athena_status_checker
aws logs delete-log-group --log-group-name /aws/lambda/medlaunch-facility-processor
aws logs delete-log-group --log-group-name /aws/lambda/medlaunch_athena_status_checker
```

```sql
DROP TABLE IF EXISTS healthcare_facilities;
DROP TABLE IF EXISTS facility_metrics;
```

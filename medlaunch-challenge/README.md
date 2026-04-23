# MedLaunch AWS Data Engineering Challenge
**Submitted by:** Rasagyna Peddapalli | rasagyna.p@gmail.com | github.com/rasagyna1106

---

## Problem Statement

Healthcare facilities must maintain active accreditations from bodies like Joint Commission and NCQA. When accreditations expire, facilities lose operating licenses and insurance reimbursements. Accreditation data sits in raw JSON in S3 with no automated system to extract metrics, flag expiring records, or trigger processing on new uploads. Manual tracking does not scale.

---

## Solution

A four-stage AWS pipeline that ingests raw facility JSON, extracts KPIs via Athena SQL, filters expiring accreditations via Python, and automates the workflow with Lambda and Step Functions. Runs end to end without manual intervention. Handles malformed data gracefully. Sends SNS alerts on failure.

---

## Stage Selection Rationale

All four stages were completed. Stages 1 and 2 were the primary stages - SQL analytics on a cloud data lake and Python ETL with production-grade error handling, which maps directly to the MedLaunch data engineering role. Stage 3 added event-driven ingestion so new uploads trigger processing automatically. Stage 4 completed the pipeline with async Athena polling, result promotion to production, and SNS failure alerting turning a collection of scripts into a reliable automated workflow.

---

## Architecture

```
S3 raw/
  |-- Athena (Stage 1) --> CTAS Parquet --> S3 output/facility_metrics/
  |-- Python (Stage 2) --> Filter expiring --> S3 output/expiring/
  |-- S3 Upload Event
        |
        Lambda (Stage 3) --> Start Athena query --> return queryExecutionId
        |
        Step Functions (Stage 4)
          |-- Wait 10s
          |-- Check Athena status
          |-- SUCCEEDED --> CopyResultsToProduction --> PipelineSucceeded
          |-- FAILED    --> SendFailureAlert (SNS email) --> PipelineFailed
          |-- RUNNING   --> loop back to Wait
```

---

## Repository Structure

```
medlaunch-challenge/
├── README.md
├── data/
│   └── sample_facilities.ndjson       8 records (5 core + 3 edge cases)
├── athena/
│   ├── 01_create_table.sql            External table over S3 JSON
│   └── 02_extract_metrics_ctas.sql    CTAS to Parquet with facility KPIs
├── python/
│   ├── filter_expiring_accreditations.py
│   └── requirements.txt
├── lambda/
│   ├── handler.py                     Stage 3 - S3 triggered Lambda
│   ├── athena_status_checker.py       Stage 4 - Athena polling Lambda
│   └── requirements.txt
├── stepfunctions/
│   └── pipeline.json                  Stage 4 - State machine definition
└── iam/
    └── least_privilege_policy.json    Minimal IAM permissions
```

---

## Stage 1 - Athena SQL

**Problem:** Raw facility JSON has nested arrays for services, labs, and accreditations. Need to extract 5 metrics per facility including earliest expiry across multiple accreditation records.

**Solution:** Athena external table over S3 raw prefix using OpenX JsonSerDe. CTAS query extracts the 5 fields and writes Parquet output to S3.

**Why:**
- External table means zero data movement. Raw files stay untouched in S3.
- JsonSerDe handles nested structs and arrays natively. No preprocessing needed.
- CTAS writes Parquet in a single SQL statement. No extra infrastructure.
- Parquet reduces Athena scan cost by 10-100x versus raw JSON.
- ignore.malformed.json skips bad records without aborting the query.

**Result:** facility_metrics table created with 5 rows. Parquet output in s3://medlaunch-techchallenge-rasagyna/output/facility_metrics/

```sql
-- External table reads JSON directly from S3 using OpenX JsonSerDe
-- CTAS output is Parquet + Snappy - reduces scan cost 10-100x vs raw JSON
-- CROSS JOIN UNNEST flattens accreditations array for MIN(valid_until)
-- WHERE CARDINALITY > 0 excludes facilities with no accreditation records
-- ignore.malformed.json skips bad rows without failing the entire query
```

---

## Stage 2 - Python

**Problem:** Filtering by expiry window is cleaner in Python than SQL. Source data arrives in inconsistent formats from different upstream systems.

**Solution:** boto3 script reads all files from raw S3 prefix, filters on 6-month expiry window, writes matching records as NDJSON to a separate prefix.

**Why:**
- relativedelta(months=6) gives correct calendar month arithmetic. timedelta(days=180) breaks at month boundaries.
- Dual parser handles both strict NDJSON and glued JSON transparently.
- ClientError handlers on every boto3 call. Non-zero exit code on failure.
- Output as NDJSON means Athena can query it immediately without transformation.

**Result:** 8 records loaded. 5 facilities filtered. Edge cases handled with warning logs. Output in s3://medlaunch-techchallenge-rasagyna/output/expiring/

```python
# relativedelta used over timedelta - correct at month boundaries and leap years
# Dual JSON parser: tries strict NDJSON first, falls back to streaming decoder
# All boto3 calls wrapped in ClientError - exits non-zero on AWS failures
# Output as NDJSON — immediately Athena-queryable without transformation
# Structured logging on every S3 call and filter result for CloudWatch analysis
```

---

## Stage 3 - Lambda

**Problem:** Running the script manually on every upload is not production behaviour. New data should trigger processing automatically.

**Solution:** Lambda triggered by S3 ObjectCreated events on raw/ prefix. Starts Athena query and returns queryExecutionId for Step Functions to poll.

**Why:**
- S3 event notification is the correct trigger pattern. No polling, no cron.
- Lambda exits immediately after starting the query. Polling inside Lambda risks the 15-minute timeout.
- Trigger scoped to raw/ only. Prevents recursive invocations when output files land in the same bucket.
- boto3 client initialized outside handler for reuse across warm invocations.

**Result:** Lambda deployed with S3 trigger. Test execution returned queryExecutionId confirming Athena query started.

```python
# boto3 client outside handler - reused across warm Lambda invocations
# Trigger scoped to raw/ prefix - prevents recursive S3 invocation loop
# Lambda starts query and exits - polling handled by Step Functions (Stage 4)
# Environment variables for all config — no hardcoded bucket or database names
# is_processable_file() filters .json and .ndjson only - ignores other file types
```

---

## Stage 4 - Step Functions

**Problem:** Athena queries are async. Something needs to wait, check the result, copy output to production on success, and alert on failure. Building this inside Lambda is brittle and expensive.

**Solution:** Step Functions state machine that waits, polls, evaluates, and routes to success or failure with full SNS alerting.

**Why:**
- Wait state pauses execution without consuming compute. Correct serverless pattern for async calls.
- Separate status checker Lambda keeps concerns clean. One function starts queries, one checks them.
- Every state has a Catch block. No failure can pass through silently.
- SNS publishes full error payload so the alert email has enough context to diagnose without opening CloudWatch.
- CopyObject to production/ separates raw Athena output from curated data.

**State machine flow:**

```
StartAthenaQuery --> ExtractQueryId --> WaitForQuery (10s)
  --> CheckQueryStatus --> EvaluateStatus
        --> SUCCEEDED: CopyResultsToProduction --> PipelineSucceeded
        --> FAILED:    SendFailureAlert --> PipelineFailed
        --> RUNNING:   loop back to WaitForQuery
```

**Result:** State machine executed successfully. Full green flow confirmed. SNS alert tested and working.

```json
// Wait state pauses without compute cost — correct pattern for async Athena
// Catch on every state routes all failures to SendFailureAlert
// Choice state reads boolean flags from status checker — clean routing logic
// CopyObject promotes results to production/ only after SUCCEEDED confirmation
// SNS message includes full error payload for immediate diagnosis
```

---

## Error Handling Test Cases

Three edge case records added to demonstrate robust error handling:

**FAC33333 - blank valid_until:** Python logs a warning and skips the record. Does not crash.

**FAC44444 - empty accreditations array:** Python returns False immediately. Athena excludes via CARDINALITY > 0 filter.

**FAC55555 - expired in 2020:** Correctly caught by the 6-month filter. Demonstrates the filter works on historical backlog data too.

---

## IAM — Least Privilege

No wildcards. No admin permissions. Every permission scoped to exact resource.

| Permission | Scope |
|---|---|
| s3:GetObject | raw/ prefix only |
| s3:PutObject | output/ and athena-results/ only |
| athena:StartQueryExecution | primary workgroup only |
| glue:GetTable | two specific tables only |
| logs:PutLogEvents | specific Lambda log group only |

---

## Cost

Total cost: $0.00 — all within AWS Free Tier.

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

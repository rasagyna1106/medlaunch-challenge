# MedLaunch AWS Data Engineering Challenge
### Healthcare Facility Accreditation Pipeline

**Submitted by:** Rasagyna Peddapalli  
**Email:** rasagyna.p@gmail.com  
**GitHub:** [rasagyna1106](https://github.com/rasagyna1106)

---

## Overview

This project implements an automated AWS data pipeline that processes healthcare facility JSON records stored in S3, extracts accreditation metrics using Athena SQL, filters facilities with expiring accreditations using Python/boto3, and triggers automated processing via a Lambda function on new data uploads.

**Stages completed:** Stage 1 (Athena SQL), Stage 2 (Python/boto3), Stage 3 (Lambda — bonus), Stage 4 (Step Functions)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Architecture                         │
└─────────────────────────────────────────────────────────────────┘

  New file upload
       │
       ▼
┌─────────────┐     S3 Event        ┌──────────────────┐
│  S3 Bucket  │ ──────────────────► │  Lambda Function │
│  raw/*.json │                     │  (Stage 3)       │
└─────────────┘                     └────────┬─────────┘
       │                                     │
       │  Athena External Table              │ StartQueryExecution
       ▼                                     ▼
┌─────────────────────────┐        ┌──────────────────┐
│  Athena (Stage 1)       │        │  Athena Query    │
│  01_create_table.sql    │        │  facilities/     │
│  02_extract_metrics.sql │        │  state count     │
└────────────┬────────────┘        └────────┬─────────┘
             │ CTAS → Parquet               │
             ▼                              ▼
┌─────────────────────────┐     ┌──────────────────────┐
│  S3 output/             │     │  S3 athena-results/  │
│  facility_metrics/      │     │  query_results.csv   │
│  (Snappy Parquet)       │     └──────────────────────┘
└─────────────────────────┘

  Python Script (Stage 2)
  filter_expiring_accreditations.py
       │
       ├── Reads:  s3://bucket/raw/*.ndjson
       ├── Filter: accreditations expiring within 6 months
       └── Writes: s3://bucket/output/expiring/expiring_accreditations.ndjson
```

---

## Repository Structure

```
medlaunch-challenge/
├── README.md                               # This file
├── .gitignore                              # Python / AWS exclusions
├── data/
│   └── sample_facilities.ndjson           # 5 test facility records (NDJSON)
├── athena/
│   ├── 01_create_table.sql                # External table over S3 JSON
│   └── 02_extract_metrics_ctas.sql        # CTAS → Parquet with KPIs
├── python/
│   ├── filter_expiring_accreditations.py  # Stage 2 — boto3 filter script
│   └── requirements.txt                   # boto3, python-dateutil
├── lambda/
│   ├── handler.py                         # Stage 3 — event-driven Lambda
│   ├── athena_status_checker.py           # Stage 4 — Athena query status checker
│   └── requirements.txt                   # boto3
├── stepfunctions/
│   └── pipeline.json                      # Stage 4 — Step Functions state machine (ASL)
└── iam/
    └── least_privilege_policy.json        # Minimal IAM policy
```

---

## Stage Selection Rationale

I chose **Stage 1 (Athena SQL)** and **Stage 2 (Python/boto3)** as my primary stages because they directly mirror the core data engineering responsibilities in the MedLaunch role — building ETL pipelines, writing production SQL against cloud data lakes, and processing healthcare records with proper validation and error handling.

Stage 1 demonstrates SQL fluency with nested JSON schemas and Athena-specific patterns like CTAS, JsonSerDe, and Parquet output for cost optimization. Stage 2 demonstrates clean Python engineering: modular functions, structured logging, graceful error handling, and support for both NDJSON and glued JSON formats.

I added **Stage 3 (Lambda)** as a bonus because event-driven ingestion is the first responsibility listed in the MedLaunch job description, and I wanted to demonstrate how the pipeline extends beyond batch processing into real-time serverless patterns — specifically, how a new file upload automatically triggers downstream analytics without manual intervention.

---

## Setup & Deployment

### Prerequisites

- AWS account (Free Tier)
- AWS CLI configured (`aws configure`)
- Python 3.11+
- IAM user or role with permissions from `iam/least_privilege_policy.json`

### Step 1 — Create S3 Bucket and Upload Data

```bash
BUCKET=medlaunch-techchallenge-rasagyna
REGION=us-east-1

# Create bucket
aws s3 mb s3://$BUCKET --region $REGION

# Upload sample data
aws s3 cp data/sample_facilities.ndjson s3://$BUCKET/raw/
```

### Step 2 — Configure Athena Workgroup

In the AWS Console:
1. Open **Athena → Settings → Manage**
2. Set query result location: `s3://medlaunch-techchallenge-rasagyna/athena-results/`
3. Save

### Step 3 — Run Stage 1 (Athena SQL)

Open **Athena Query Editor** → database: `default`

Run `athena/01_create_table.sql` first:
```sql
-- Creates external table: healthcare_facilities
-- Reads directly from s3://medlaunch-techchallenge-rasagyna/raw/
```

Then run `athena/02_extract_metrics_ctas.sql`:
```sql
-- Creates: facility_metrics table
-- Output:  s3://medlaunch-techchallenge-rasagyna/output/facility_metrics/
-- Format:  Parquet + Snappy compression
```

The CTAS writes **six** analytics columns: `facility_id`, `facility_name`, `employee_count`, `number_of_offered_services` (from `CARDINALITY(services)`), `expiry_date_of_first_accreditation` (earliest `valid_until` across accreditations), and `state` (from `location.state`).

Verify results:
```sql
SELECT * FROM facility_metrics ORDER BY expiry_date_of_first_accreditation;
```

### Step 4 — Run Stage 2 (Python)

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

Verify output:
```bash
aws s3 cp s3://medlaunch-techchallenge-rasagyna/output/expiring/expiring_accreditations.ndjson -
```

### Step 5 — Deploy Stage 3 (Lambda — bonus)

**Package and deploy:**
```bash
cd lambda
pip install -r requirements.txt -t package/
cp handler.py package/
cd package && zip -r ../lambda_package.zip . && cd ..

aws lambda create-function \
    --function-name medlaunch-facility-processor \
    --runtime python3.11 \
    --role YOUR_LAMBDA_ROLE_ARN \
    --handler handler.handler \
    --zip-file fileb://lambda_package.zip \
    --timeout 60 \
    --memory-size 256 \
    --environment Variables="{
        ATHENA_DATABASE=default,
        ATHENA_RESULTS_BUCKET=medlaunch-techchallenge-rasagyna,
        ATHENA_WORKGROUP=primary
    }" \
    --region us-east-1
```

**Add S3 trigger in AWS Console:**
- Lambda → Configuration → Triggers → Add trigger
- Source: S3
- Bucket: `medlaunch-techchallenge-rasagyna`
- Event type: `s3:ObjectCreated:*`
- Prefix: `raw/`
- Suffix (optional): restrict to `.json` / `.ndjson` if your console supports it — the function also ignores non-`raw/` keys in code.

**Handler name:** If the deployment zip has `handler.py` at the root (as in the packaging steps above), set the Lambda **handler** to `handler.handler` (module `handler`, function `handler`). Use `lambda_function.handler` only if you rename the file to `lambda_function.py`.

---

## Design Decisions

### Why NDJSON for raw storage?
Athena's JsonSerDe processes one JSON object per line. Storing raw data as NDJSON means Athena can query records directly without preprocessing, reducing both pipeline complexity and cost.

### Why Parquet for CTAS output?
Parquet is columnar and Snappy-compressed. Downstream Athena queries that select only a few columns scan a fraction of the data vs raw JSON — typically 10-100x less data scanned, directly reducing the $5/TB Athena scan cost.

### Why not poll Athena inside Lambda?
Athena queries can take 10-60+ seconds. Polling inside Lambda wastes execution time and risks the 15-minute timeout on large datasets. The Lambda starts the query and returns the `QueryExecutionId` — a Step Functions state machine (Stage 4) or CloudWatch Events rule handles polling asynchronously, keeping Lambda execution time under 1 second per invocation.

### Why `relativedelta` for the 6-month window?
`timedelta(days=180)` gives incorrect results around month boundaries and leap years. `relativedelta(months=6)` correctly advances by calendar months regardless of days-per-month.

### Why glued JSON fallback in the Python parser?
The sample data provided arrives as multiple JSON objects concatenated without a wrapper array. Rather than requiring pre-normalization, the script handles both strict NDJSON and glued JSON transparently — making it resilient to different upstream data formats.

---

## Stage 4: Workflow Orchestration with Step Functions

**What was done:**

A Step Functions state machine was built to orchestrate the complete pipeline end to end. The workflow chains together Lambda invocation, Athena query polling, S3 result copying, and SNS failure alerts into a single automated workflow that runs whenever triggered.

The state machine has six states: StartAthenaQuery invokes the facility processor Lambda, ExtractQueryId pulls the query ID from the response, WaitForQuery pauses for 10 seconds, CheckQueryStatus invokes the status checker Lambda to poll Athena, EvaluateStatus routes to either success or failure based on the query result, CopyResultsToProduction copies the output file to the production S3 prefix, and SendFailureAlert publishes an SNS notification if anything goes wrong.

**Why this approach:**

Step Functions was the right tool for orchestrating an async workflow because Athena queries do not complete instantly. Rather than building a polling loop inside a single Lambda function which would burn execution time and risk the 15-minute timeout, Step Functions handles the wait natively using a Wait state. The workflow pauses, then resumes to check status, retrying in a loop until the query finishes. This is the correct serverless pattern for async AWS service calls.

Every state has a Catch block that routes failures to SendFailureAlert so no error can silently pass through. The SNS topic sends an email notification with the full error details, which is exactly the kind of observability a production healthcare data pipeline needs.

**Result:**

State machine deployed successfully. Execution ran through all states, correctly detected query completion, and routed through the full workflow. SNS failure alerts fired correctly during error testing, confirming the failure handling works as designed.

---

## IAM — Least Privilege

The policy in `iam/least_privilege_policy.json` follows least-privilege principles:

| Permission | Scope |
|-----------|-------|
| `s3:GetObject` | `raw/` prefix only |
| `s3:PutObject` | `output/` and `athena-results/` only |
| `athena:StartQueryExecution` | `primary` workgroup only |
| `glue:GetTable` | Two specific tables only |
| `logs:PutLogEvents` | Specific Lambda log group only |

No `s3:*` wildcards. No `*` resource ARNs. No admin permissions.

---

## Cost Estimate

| Service | Usage in this project | Free Tier | Estimated Cost |
|---------|----------------------|-----------|----------------|
| S3 Storage | < 1 MB | 5 GB/month | $0.00 |
| S3 Requests | < 50 requests | 20,000 GET / 2,000 PUT | $0.00 |
| Athena | < 1 MB scanned | 1 TB/month | $0.00 |
| Lambda | < 10 invocations | 1M requests/month | $0.00 |
| CloudWatch Logs | < 1 MB | 5 GB/month | $0.00 |
| **Total** | | | **$0.00** |

---

## Sample Data

The `data/sample_facilities.ndjson` file contains 5 healthcare facility records:

| Facility | State | Services | Earliest accreditation expiry |
|---------|-------|----------|------------------------------|
| City Hospital | TX | 5 | 2025-06-30 ⚠️ |
| Green Valley Clinic | CA | 2 | 2024-09-30 ⚠️ |
| Lakeside Medical Center | FL | 4 | 2025-12-31 ⚠️ |
| Sunrise Health Center | TX | 3 | 2025-08-15 ⚠️ |
| Pineview Regional Hospital | FL | 5 | 2027-01-31 |

⚠️ = at least one accreditation expires on or before the Stage 2 cutoff (`today` + 6 calendar months). With a run date in **April 2026**, four of five facilities match; Pineview’s earliest expiry (2027-01-31) is outside that window.

---

## Cleanup

Run after submission review to avoid any AWS charges:

```bash
# Delete all S3 content and bucket
aws s3 rb s3://medlaunch-techchallenge-rasagyna --force

# Delete Athena tables (run in Athena console)
# DROP TABLE IF EXISTS healthcare_facilities;
# DROP TABLE IF EXISTS facility_metrics;

# Delete Lambda function
aws lambda delete-function --function-name medlaunch-facility-processor

# Delete CloudWatch log group
aws logs delete-log-group \
    --log-group-name /aws/lambda/medlaunch-facility-processor
```

---

## Technologies Used

| Category | Technology |
|----------|-----------|
| Cloud | AWS (S3, Athena, Lambda, CloudWatch, Glue) |
| Languages | Python 3.11, SQL |
| Libraries | boto3, python-dateutil |
| Data Formats | NDJSON (input), Parquet/Snappy (output) |
| Infrastructure | IAM least-privilege, S3 event notifications |
| Version Control | Git / GitHub |

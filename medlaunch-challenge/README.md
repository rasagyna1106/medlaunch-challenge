# MedLaunch AWS Data Engineering Challenge
### Healthcare Facility Accreditation Pipeline

**Submitted by:** Rasagyna Peddapalli
**Email:** rasagyna.p@gmail.com
**GitHub:** github.com/rasagyna1106

---

## Problem Statement

Healthcare facilities are required to maintain active accreditations from regulatory bodies like the Joint Commission and NCQA. When these accreditations expire, facilities risk losing their ability to operate, receive insurance reimbursements, and serve patients. The problem is that accreditation data is stored in raw JSON files in S3, and there is no automated system to extract key metrics, identify which facilities are approaching expiry, or trigger processing when new data arrives.

This challenge required building a data pipeline on AWS that solves exactly this: ingest raw healthcare facility JSON, extract meaningful accreditation metrics, filter facilities with expiring accreditations, and automate the processing pipeline end to end when new data is uploaded.

---

## Solution Overview

The solution is a four-stage AWS data pipeline built with Athena SQL, Python with boto3, Lambda, and Step Functions. Each stage handles a specific part of the problem: querying the data, filtering it, automating the trigger, and orchestrating the full workflow end to end.

---

## Architecture

```
Raw JSON in S3 (raw/)
        |
        |-----> Athena External Table (Stage 1)
        |            |
        |            |-----> CTAS Query -> Parquet output (output/facility_metrics/)
        |
        |-----> Python Script (Stage 2)
        |            |
        |            |-----> Filter expiring accreditations
        |            |-----> Write NDJSON to S3 (output/expiring/)
        |
        |-----> S3 Upload Event
                     |
                     |-----> Lambda: medlaunch-facility-processor (Stage 3)
                                  |
                                  |-----> Start Athena query
                                  |-----> Return queryExecutionId
                                  |
                                  |-----> Step Functions (Stage 4)
                                               |
                                               |-----> Wait 10s
                                               |-----> Check query status
                                               |-----> Success: copy to production/
                                               |-----> Failure: SNS email alert
```

---

## Repository Structure

```
medlaunch-challenge/
    README.md
    data/
        sample_facilities.ndjson        8 facility records including 3 edge case test records
    athena/
        01_create_table.sql             Creates external table over raw S3 JSON
        02_extract_metrics_ctas.sql     Extracts KPIs and writes Parquet to S3
    python/
        filter_expiring_accreditations.py   Filters facilities with expiring accreditations
        requirements.txt
    lambda/
        handler.py                      Stage 3 Lambda triggered by S3 uploads
        athena_status_checker.py        Stage 4 Lambda that polls Athena query status
        requirements.txt
    stepfunctions/
        pipeline.json                   Stage 4 Step Functions state machine definition
    iam/
        least_privilege_policy.json     Minimal IAM permissions for the pipeline
```

---

## Stage 1: Data Extraction with Athena SQL

**What was done:**

An external table called healthcare_facilities was created in Athena pointing to the raw NDJSON files in S3. This table uses the OpenX JsonSerDe to handle the nested JSON structure including arrays for services, labs, and accreditations. A second query uses CTAS to extract five key metrics per facility and write the results as Parquet files to S3.

Fields extracted: facility_id, facility_name, employee_count, number_of_offered_services (CARDINALITY of services array), and expiry_date_of_first_accreditation (MIN of valid_until across all accreditation records).

**Why this approach:**

Athena reads directly from S3 without any data movement or ETL preprocessing. CTAS was chosen for the output because it writes Parquet in a single SQL statement. Parquet was chosen over JSON because it is columnar and compressed, reducing Athena scan costs by 10 to 100 times. The OpenX JsonSerDe was configured with ignore.malformed.json set to true so a single bad record does not abort the entire query.

**Result:**

Query ran successfully returning all 5 core facilities with correct metrics ordered by expiry date. Parquet output landed in s3://medlaunch-techchallenge-rasagyna/output/facility_metrics/.

---

## Stage 2: Data Processing with Python

**What was done:**

A Python script using boto3 reads all JSON and NDJSON files from the raw S3 prefix, parses facility records, filters facilities with any accreditation expiring within 6 months, and writes filtered records as NDJSON to a separate S3 location.

**Why this approach:**

Python was the right tool because the filtering logic involves date arithmetic that is cleaner in Python than SQL. The script uses relativedelta from python-dateutil to calculate the 6-month window correctly. timedelta(days=180) gives wrong results around month boundaries, so relativedelta(months=6) was used instead.

The script handles both strict NDJSON and glued JSON where multiple objects are concatenated without a wrapper array. boto3 calls are wrapped in specific exception handlers so the script surfaces clear errors and exits with a non-zero status code on failure.

**Result:**

Processed 8 records, identified facilities with expiring accreditations, and wrote them to s3://medlaunch-techchallenge-rasagyna/output/expiring/expiring_accreditations.ndjson. Edge case records with missing dates and empty accreditations were handled gracefully without crashing.

---

## Stage 3: Event-Driven Processing with Lambda

**What was done:**

A Lambda function was deployed that triggers automatically whenever a new JSON or NDJSON file is uploaded to the raw/ prefix in S3. When triggered it starts an Athena query counting accredited facilities per US state and returns the QueryExecutionId for downstream processing.

**Why this approach:**

Lambda was chosen because event-driven ingestion is the first responsibility in the MedLaunch job description. The function intentionally does not poll for Athena completion inside itself because polling would burn execution time and risk the 15-minute timeout. Instead it starts the query and returns immediately. Step Functions handles the polling in Stage 4.

The S3 trigger is scoped to raw/ only and fires only on .json and .ndjson files to prevent recursive invocations when output files are written to the same bucket.

**Result:**

Lambda deployed successfully with S3 trigger configured. Test execution succeeded and returned a queryExecutionId confirming the Athena query started correctly.

---

## Stage 4: Workflow Orchestration with Step Functions

**What was done:**

A Step Functions state machine was built to orchestrate the complete pipeline end to end. The workflow chains together Lambda invocation, Athena query polling, S3 result copying, and SNS failure alerts into a single automated workflow.

The state machine has seven states: StartAthenaQuery invokes the facility processor Lambda, ExtractQueryId pulls the query ID from the response, WaitForQuery pauses 10 seconds, CheckQueryStatus invokes the status checker Lambda to poll Athena, EvaluateStatus routes to success or failure, CopyResultsToProduction copies output to the production S3 prefix, and SendFailureAlert publishes an SNS notification if anything goes wrong.

**Why this approach:**

Step Functions was the right tool for orchestrating an async workflow because Athena queries do not complete instantly. Rather than building a polling loop inside a Lambda which would burn execution time, Step Functions handles the wait natively using a Wait state that pauses and resumes without consuming compute. This is the correct serverless pattern for async AWS service calls.

Every state has a Catch block routing failures to SendFailureAlert so no error can silently pass through. The SNS topic sends an email with full error details, which is exactly the kind of observability a production healthcare pipeline needs.

**Result:**

State machine deployed and executed successfully. All states ran in correct order. SNS failure alerts fired correctly during error testing confirming failure handling works as designed.

---

## Test Cases for Error Handling

Beyond the 5 core facility records, 3 additional edge case records were added to demonstrate robust error handling:

**Test Case 1 - Missing accreditation date (FAC33333)**
A facility with a blank valid_until field. Tests that the Python date parser skips unparseable dates with a warning log rather than crashing the entire run. Tests that Athena handles empty string dates gracefully via the ignore.malformed.json SerDe property.

**Test Case 2 - No accreditations (FAC44444)**
A facility with an empty accreditations array. Tests the CARDINALITY > 0 filter in the Athena CTAS query excludes this facility correctly. Tests that the Python has_expiring_accreditation function returns False on empty arrays without errors.

**Test Case 3 - Long-expired accreditation (FAC55555)**
A facility whose accreditation expired in 2020. Tests that the 6-month window filter correctly catches accreditations that expired in the past, not just future expiries. This is realistic because pipelines processing backlogged data would encounter historical expiry dates.

All three edge cases are processed without exceptions. The Python script logs warnings for the missing date and continues processing remaining records.

---

## Security: IAM Least Privilege

The IAM policy in iam/least_privilege_policy.json follows least privilege throughout. Each permission is scoped to the exact resource it needs.

S3 read access is limited to raw/ prefix only. S3 write access is limited to output/ and athena-results/ prefixes only. Athena permissions are scoped to the primary workgroup only. Glue catalog permissions are scoped to the two specific tables. CloudWatch Logs permissions are scoped to the specific Lambda log group. No wildcards on resource ARNs. No admin permissions.

---

## Sample Data Summary

| Facility | State | Services | Earliest Expiry | Category |
|---|---|---|---|---|
| Green Valley Clinic | CA | 2 | 2024-09-30 | Core test |
| City Hospital | TX | 5 | 2025-06-30 | Core test |
| Sunrise Health Center | TX | 3 | 2025-08-15 | Core test |
| Lakeside Medical Center | FL | 4 | 2025-12-31 | Core test |
| Pineview Regional Hospital | FL | 5 | 2027-01-31 | Core test |
| Test Missing Date Clinic | TX | 1 | blank | Edge case 1 |
| Test No Accreditation Hospital | TX | 2 | none | Edge case 2 |
| Test Expired Long Ago Clinic | AZ | 1 | 2020-01-01 | Edge case 3 |

---

## How to Run

**Prerequisites:** AWS account, AWS CLI configured, Python 3.11+

**Step 1: Upload data**
```
aws s3 mb s3://your-bucket-name --region us-east-1
aws s3 cp data/sample_facilities.ndjson s3://your-bucket-name/raw/
```

**Step 2: Configure Athena**

In Athena console go to Query settings and set result location to s3://your-bucket-name/athena-results/

**Step 3: Run Stage 1**

Run athena/01_create_table.sql then athena/02_extract_metrics_ctas.sql in Athena Query Editor with database set to default.

**Step 4: Run Stage 2**
```
cd python
pip install -r requirements.txt
python filter_expiring_accreditations.py --source-bucket your-bucket-name --source-prefix raw/ --dest-bucket your-bucket-name --dest-prefix output/expiring/ --months 6 --region us-east-1
```

**Step 5: Deploy Stage 3**

Create Lambda function medlaunch-facility-processor with Python 3.12, paste lambda/handler.py, set handler to lambda_function.handler, add environment variables, add S3 trigger on raw/ prefix.

**Step 6: Deploy Stage 4**

Create Lambda function medlaunch_athena_status_checker with Python 3.12, paste lambda/athena_status_checker.py, set handler to lambda_function.handler, attach AmazonAthenaFullAccess.

Create SNS topic medlaunch-pipeline-alerts, add email subscription.

Create Step Functions state machine using stepfunctions/pipeline.json, attach role with Lambda, SNS, and S3 permissions.

---

## Cost

All resources fall within AWS Free Tier limits. Total cost for this submission is zero dollars.

| Service | Usage | Free Tier | Cost |
|---|---|---|---|
| S3 | Less than 1MB | 5GB/month | 0.00 |
| Athena | Less than 1MB scanned | 1TB/month | 0.00 |
| Lambda | Less than 20 invocations | 1M/month | 0.00 |
| Step Functions | Less than 30 state transitions | 4000/month | 0.00 |
| SNS | Less than 5 notifications | 1M/month | 0.00 |
| CloudWatch | Less than 1MB logs | 5GB/month | 0.00 |

---

## Conclusion

This pipeline shows how a small set of AWS services can be combined to solve a real healthcare compliance problem efficiently. Athena handles SQL analytics without servers to manage. Python handles filtering with proper error handling and logging. Lambda makes the pipeline event-driven so new data triggers processing automatically. Step Functions ties everything together into a reliable orchestrated workflow with built-in retry logic, polling, and failure alerting.

The design prioritized cost efficiency through Parquet output and minimal IAM permissions, reliability through malformed JSON tolerance and specific exception handling, and extensibility through the QueryExecutionId pattern that makes it straightforward to extend the workflow further without changing existing code.

---

## Cleanup

```
aws s3 rb s3://medlaunch-techchallenge-rasagyna --force
aws lambda delete-function --function-name medlaunch-facility-processor
aws lambda delete-function --function-name medlaunch_athena_status_checker
aws logs delete-log-group --log-group-name /aws/lambda/medlaunch-facility-processor
aws logs delete-log-group --log-group-name /aws/lambda/medlaunch_athena_status_checker
```

Drop Athena tables in query editor:
```sql
DROP TABLE IF EXISTS healthcare_facilities;
DROP TABLE IF EXISTS facility_metrics;
```

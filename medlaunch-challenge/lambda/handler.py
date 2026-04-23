"""
lambda/handler.py
==================
Stage 3 — Event-Driven Processing with Lambda (bonus stage)

Triggered by S3 ObjectCreated events on the raw/ prefix.
Starts an Athena query counting accredited facilities per state.
Returns QueryExecutionId for async polling — does NOT poll inside Lambda
to avoid timeout risk on large datasets.

Environment variables required:
  ATHENA_DATABASE        — Glue database name (e.g. default)
  ATHENA_RESULTS_BUCKET  — S3 bucket for Athena result files
  ATHENA_WORKGROUP       — Athena workgroup (default: primary)

Deploy: set Lambda handler to handler.handler when this file is packaged as
handler.py at the zip root (use lambda_function.handler only if you rename
the module to lambda_function.py).
"""

import json
import logging
import os
import urllib.parse

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

ATHENA_DATABASE       = os.environ.get("ATHENA_DATABASE", "default")
ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]
ATHENA_WORKGROUP      = os.environ.get("ATHENA_WORKGROUP", "primary")

athena_client = boto3.client("athena")

FACILITIES_PER_STATE_QUERY = """
SELECT
    location.state                                      AS state,
    COUNT(DISTINCT facility_id)                         AS total_facilities,
    COUNT(DISTINCT
        CASE WHEN CARDINALITY(accreditations) > 0
             THEN facility_id END
    )                                                   AS accredited_facilities,
    ROUND(
        COUNT(DISTINCT CASE WHEN CARDINALITY(accreditations) > 0
                            THEN facility_id END)
        * 100.0 / NULLIF(COUNT(DISTINCT facility_id), 0), 2
    )                                                   AS accreditation_rate_pct
FROM healthcare_facilities
GROUP BY location.state
ORDER BY total_facilities DESC;
"""


def start_athena_query(query, database, results_bucket, workgroup):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": f"s3://{results_bucket}/athena-results/"},
            WorkGroup=workgroup,
        )
        qid = response["QueryExecutionId"]
        log.info("Started Athena query — QueryExecutionId: %s", qid)
        return qid
    except ClientError as exc:
        log.error("Failed to start Athena query: %s", exc)
        raise


def extract_s3_event_details(event):
    details = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        details.append((bucket, key))
    return details


def is_processable_file(key):
    lower = key.lower()
    return lower.startswith("raw/") and (lower.endswith(".json") or lower.endswith(".ndjson"))


def handler(event, context):
    log.info("Event: %s", json.dumps(event))

    s3_objects = extract_s3_event_details(event)
    processable = [(b, k) for b, k in s3_objects if is_processable_file(k)]

    if not processable:
        return {"statusCode": 200, "message": "No JSON files to process"}

    for bucket, key in processable:
        log.info("Triggered by s3://%s/%s", bucket, key)

    try:
        qid = start_athena_query(
            FACILITIES_PER_STATE_QUERY,
            ATHENA_DATABASE,
            ATHENA_RESULTS_BUCKET,
            ATHENA_WORKGROUP,
        )
    except ClientError as exc:
        return {"statusCode": 500, "error": str(exc)}

    return {
        "statusCode": 200,
        "queryExecutionId": qid,
        "triggeredBy": [f"s3://{b}/{k}" for b, k in processable],
    }

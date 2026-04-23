"""
filter_expiring_accreditations.py
==================================
Stage 2 — Data Processing with Python (boto3)

Purpose
-------
Read healthcare facility JSON records from S3, identify facilities that have
at least one accreditation expiring within a configurable window (default: 6
months), and write those records as NDJSON to a separate S3 destination.

Usage
-----
    python filter_expiring_accreditations.py \
        --source-bucket  medlaunch-techchallenge-rasagyna \
        --source-prefix  raw/ \
        --dest-bucket    medlaunch-techchallenge-rasagyna \
        --dest-prefix    output/expiring/ \
        --months         6

Design decisions
----------------
- Handles both NDJSON (one object per line) and glued JSON (multiple root
  objects concatenated without a wrapper array) so the script works with
  either the raw sample file or pre-normalized files.
- Uses Python's standard logging module — no third-party logging required.
- All boto3 calls are wrapped in specific exception handlers so the script
  surfaces actionable error messages and exits with a non-zero code on
  unrecoverable failures.
- Writes results as NDJSON (one JSON object per line) so the output is
  immediately Athena-queryable without additional transformation.
"""

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from io import StringIO

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_date

# ---------------------------------------------------------------------------
# Logging setup — structured enough for CloudWatch ingestion if run in Lambda
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def list_s3_objects(s3_client, bucket: str, prefix: str) -> list[dict]:
    """Return all object metadata under bucket/prefix (handles pagination)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(page.get("Contents", []))
    except ClientError as exc:
        log.error("Failed to list s3://%s/%s — %s", bucket, prefix, exc)
        raise
    log.info("Found %d object(s) under s3://%s/%s", len(objects), bucket, prefix)
    return objects


def read_s3_object(s3_client, bucket: str, key: str) -> str:
    """Download a single S3 object and return its body as a string."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        log.debug("Read %d bytes from s3://%s/%s", len(body), bucket, key)
        return body
    except ClientError as exc:
        log.error("Failed to read s3://%s/%s — %s", bucket, key, exc)
        raise


def write_s3_object(s3_client, bucket: str, key: str, content: str) -> None:
    """Write a UTF-8 string to S3."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        log.info("Wrote output to s3://%s/%s", bucket, key)
    except ClientError as exc:
        log.error("Failed to write s3://%s/%s — %s", bucket, key, exc)
        raise


# ---------------------------------------------------------------------------
# JSON parsing — handles both NDJSON and glued/concatenated JSON
# ---------------------------------------------------------------------------

def parse_records(raw_text: str) -> list[dict]:
    """
    Parse raw text that may be:
      1. Strict NDJSON  — one valid JSON object per line.
      2. Glued JSON     — multiple root objects written back-to-back with
                          no enclosing array, separated only by whitespace.

    Returns a list of parsed facility dicts. Malformed lines are skipped with
    a warning so one bad record does not abort the entire file.
    """
    records = []

    # --- attempt strict NDJSON first (fast path) ---
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    ndjson_failures = 0
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            ndjson_failures += 1

    if ndjson_failures == 0:
        log.debug("Parsed %d records as strict NDJSON", len(records))
        return records

    # --- fall back to streaming decoder for glued JSON ---
    log.debug("NDJSON parse had %d failures; retrying with streaming decoder", ndjson_failures)
    records = []
    decoder = json.JSONDecoder()
    text = raw_text.strip()
    idx = 0
    while idx < len(text):
        # skip whitespace between objects
        while idx < len(text) and text[idx] in " \t\n\r":
            idx += 1
        if idx >= len(text):
            break
        try:
            obj, end_idx = decoder.raw_decode(text, idx)
            records.append(obj)
            idx = end_idx
        except json.JSONDecodeError as exc:
            log.warning("Skipping unparseable chunk at position %d: %s", idx, exc)
            # advance past the offending character to avoid infinite loop
            idx += 1

    log.debug("Parsed %d records via streaming decoder", len(records))
    return records


# ---------------------------------------------------------------------------
# Business logic — accreditation expiry filter
# ---------------------------------------------------------------------------

def has_expiring_accreditation(facility: dict, cutoff_date: date) -> bool:
    """
    Return True if any accreditation in this facility expires on or before
    cutoff_date (i.e. within the alerting window).

    Treats missing or unparseable valid_until values as non-expiring so that
    malformed records do not generate false positives.
    """
    accreditations = facility.get("accreditations", [])
    if not accreditations:
        return False

    for acc in accreditations:
        raw_date = acc.get("valid_until", "")
        if not raw_date:
            continue
        try:
            expiry = parse_date(raw_date).date()
        except (ValueError, OverflowError):
            log.warning(
                "Facility %s: unparseable valid_until '%s' — skipping accreditation",
                facility.get("facility_id", "UNKNOWN"),
                raw_date,
            )
            continue

        if expiry <= cutoff_date:
            log.debug(
                "Facility %s has accreditation '%s' expiring %s (cutoff %s)",
                facility.get("facility_id"),
                acc.get("accreditation_id"),
                expiry,
                cutoff_date,
            )
            return True

    return False


def filter_expiring_facilities(records: list[dict], months: int) -> list[dict]:
    """Return only facilities with at least one accreditation expiring within `months` months."""
    today = date.today()
    cutoff = today + relativedelta(months=months)
    log.info(
        "Filtering for accreditations expiring between %s and %s (%d-month window)",
        today,
        cutoff,
        months,
    )
    expiring = [r for r in records if has_expiring_accreditation(r, cutoff)]
    log.info(
        "%d of %d facilities have expiring accreditations",
        len(expiring),
        len(records),
    )
    return expiring


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    months: int,
    aws_region: str,
) -> int:
    """
    Orchestrate the full filter pipeline. Returns the number of facilities
    written to the destination, or raises on unrecoverable error.
    """
    session = boto3.Session(region_name=aws_region)
    s3 = session.client("s3")

    # -- discover source files --
    objects = list_s3_objects(s3, source_bucket, source_prefix)
    json_objects = [
        o for o in objects
        if o["Key"].endswith(".json") or o["Key"].endswith(".ndjson")
    ]
    if not json_objects:
        log.warning(
            "No .json or .ndjson files found under s3://%s/%s — nothing to process",
            source_bucket,
            source_prefix,
        )
        return 0

    # -- read and parse all source files --
    all_records: list[dict] = []
    for obj in json_objects:
        log.info("Processing s3://%s/%s", source_bucket, obj["Key"])
        raw = read_s3_object(s3, source_bucket, obj["Key"])
        records = parse_records(raw)
        all_records.extend(records)

    log.info("Total records loaded: %d", len(all_records))

    if not all_records:
        log.warning("No records parsed — check source files")
        return 0

    # -- apply business filter --
    expiring = filter_expiring_facilities(all_records, months)

    if not expiring:
        log.info("No facilities matched the expiry filter — output will be empty")
        return 0

    # -- write results as NDJSON --
    ndjson_output = "\n".join(json.dumps(r, default=str) for r in expiring) + "\n"

    dest_key = dest_prefix.rstrip("/") + "/expiring_accreditations.ndjson"
    write_s3_object(s3, dest_bucket, dest_key, ndjson_output)

    log.info(
        "Pipeline complete — wrote %d expiring facilities to s3://%s/%s",
        len(expiring),
        dest_bucket,
        dest_key,
    )
    return len(expiring)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter healthcare facilities with soon-to-expire accreditations."
    )
    parser.add_argument("--source-bucket", required=True, help="S3 bucket containing raw JSON files")
    parser.add_argument("--source-prefix", default="raw/", help="S3 prefix (folder) to read from")
    parser.add_argument("--dest-bucket", required=True, help="S3 bucket for filtered output")
    parser.add_argument("--dest-prefix", default="output/expiring/", help="S3 prefix to write results")
    parser.add_argument("--months", type=int, default=6, help="Alert window in months (default: 6)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        count = run(
            source_bucket=args.source_bucket,
            source_prefix=args.source_prefix,
            dest_bucket=args.dest_bucket,
            dest_prefix=args.dest_prefix,
            months=args.months,
            aws_region=args.region,
        )
        sys.exit(0)
    except (BotoCoreError, ClientError) as exc:
        log.critical("AWS error — aborting: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.critical("Unexpected error — aborting: %s", exc, exc_info=True)
        sys.exit(1)

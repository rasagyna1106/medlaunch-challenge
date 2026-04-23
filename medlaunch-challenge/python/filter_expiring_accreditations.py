"""
filter_expiring_accreditations.py
==================================
Stage 2 — Data Processing with Python (boto3)

Reads healthcare facility JSON from S3, filters facilities with any
accreditation expiring within N months (default: 6), writes filtered
records as NDJSON to a separate S3 destination.

Usage:
    python filter_expiring_accreditations.py \
        --source-bucket  medlaunch-techchallenge-rasagyna \
        --source-prefix  raw/ \
        --dest-bucket    medlaunch-techchallenge-rasagyna \
        --dest-prefix    output/expiring/ \
        --months         6
"""

import argparse
import json
import logging
import sys
from datetime import date
from io import StringIO

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


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
        return response["Body"].read().decode("utf-8")
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


def parse_records(raw_text: str) -> list[dict]:
    """
    Parse raw text as either strict NDJSON or glued/concatenated JSON.
    Malformed lines are skipped with a warning.
    """
    records = []
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    ndjson_failures = 0
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            ndjson_failures += 1

    if ndjson_failures == 0:
        return records

    # Fall back to streaming decoder for glued JSON
    log.debug("Retrying with streaming decoder (%d NDJSON failures)", ndjson_failures)
    records = []
    decoder = json.JSONDecoder()
    text = raw_text.strip()
    idx = 0
    while idx < len(text):
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
            idx += 1

    return records


def has_expiring_accreditation(facility: dict, cutoff_date: date) -> bool:
    """Return True if any accreditation expires on or before cutoff_date."""
    for acc in facility.get("accreditations", []):
        raw_date = acc.get("valid_until", "")
        if not raw_date:
            continue
        try:
            expiry = parse_date(raw_date).date()
        except (ValueError, OverflowError):
            log.warning(
                "Facility %s: unparseable valid_until '%s' — skipping",
                facility.get("facility_id", "UNKNOWN"),
                raw_date,
            )
            continue
        if expiry <= cutoff_date:
            return True
    return False


def filter_expiring_facilities(records: list[dict], months: int) -> list[dict]:
    """Return only facilities with at least one accreditation expiring within months."""
    today = date.today()
    cutoff = today + relativedelta(months=months)
    log.info("Filter window: %s → %s (%d months)", today, cutoff, months)
    expiring = [r for r in records if has_expiring_accreditation(r, cutoff)]
    log.info("%d / %d facilities have expiring accreditations", len(expiring), len(records))
    return expiring


def run(source_bucket, source_prefix, dest_bucket, dest_prefix, months, aws_region) -> int:
    session = boto3.Session(region_name=aws_region)
    s3 = session.client("s3")

    objects = list_s3_objects(s3, source_bucket, source_prefix)
    json_objects = [o for o in objects if o["Key"].endswith((".json", ".ndjson"))]
    if not json_objects:
        log.warning("No JSON files found — nothing to process")
        return 0

    all_records = []
    for obj in json_objects:
        log.info("Processing s3://%s/%s", source_bucket, obj["Key"])
        raw = read_s3_object(s3, source_bucket, obj["Key"])
        all_records.extend(parse_records(raw))

    log.info("Total records loaded: %d", len(all_records))
    if not all_records:
        return 0

    expiring = filter_expiring_facilities(all_records, months)
    if not expiring:
        log.info("No facilities matched — output will be empty")
        return 0

    ndjson_output = "\n".join(json.dumps(r, default=str) for r in expiring) + "\n"
    dest_key = dest_prefix.rstrip("/") + "/expiring_accreditations.ndjson"
    write_s3_object(s3, dest_bucket, dest_key, ndjson_output)

    log.info("Done — wrote %d facilities to s3://%s/%s", len(expiring), dest_bucket, dest_key)
    return len(expiring)


def parse_args():
    parser = argparse.ArgumentParser(description="Filter healthcare facilities with expiring accreditations.")
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--source-prefix", default="raw/")
    parser.add_argument("--dest-bucket", required=True)
    parser.add_argument("--dest-prefix", default="output/expiring/")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        run(args.source_bucket, args.source_prefix, args.dest_bucket, args.dest_prefix, args.months, args.region)
        sys.exit(0)
    except (BotoCoreError, ClientError) as exc:
        log.critical("AWS error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.critical("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)

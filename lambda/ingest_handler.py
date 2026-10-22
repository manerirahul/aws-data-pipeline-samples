"""S3 landing validator (AWS Lambda pattern sample).

Triggered by an S3 ObjectCreated event on the raw landing bucket. Reads
the CSV, applies the validation rules, and:
  - copies the file to the `validated/` prefix when every record passes
  - writes failing records to `quarantine/` with a reason report when
    any record fails, and flags the batch in the returned summary

In production this sits behind a Step Functions orchestration; the
returned summary is what the state machine branches on.
"""
from __future__ import annotations

import csv
import io
import json

import boto3

s3 = boto3.client("s3")

REQUIRED_FIELDS = ["order_id", "customer_id", "order_date", "quantity", "unit_price"]
VALID_STATES = {"TX", "CA", "WA", "NY", "FL", "AZ"}


def validate_row(row: dict) -> list[str]:
    reasons = []
    for field in REQUIRED_FIELDS:
        if not (row.get(field) or "").strip():
            reasons.append(f"MISSING_REQUIRED:{field}")
    qty = (row.get("quantity") or "").strip()
    if qty and (not qty.lstrip("-").isdigit() or int(qty) <= 0):
        reasons.append(f"BAD_QUANTITY:{qty}")
    state = (row.get("state") or "").strip().upper()
    if state and state not in VALID_STATES:
        reasons.append(f"DOMAIN_VIOLATION:state={state}")
    return reasons


def handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(body)))

    rejects = []
    for row in rows:
        reasons = validate_row(row)
        if reasons:
            rejects.append({**row, "reject_reason": ";".join(reasons)})

    summary = {
        "source_key": key,
        "records_in": len(rows),
        "records_rejected": len(rejects),
        "status": "QUARANTINED" if rejects else "VALIDATED",
    }

    if rejects:
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(rejects[0].keys()))
        writer.writeheader()
        writer.writerows(rejects)
        s3.put_object(Bucket=bucket, Key=f"quarantine/{key}.rejects.csv",
                      Body=out.getvalue().encode("utf-8"))
    else:
        s3.copy_object(Bucket=bucket,
                       CopySource={"Bucket": bucket, "Key": key},
                       Key=f"validated/{key}")

    s3.put_object(Bucket=bucket, Key=f"manifests/{key}.summary.json",
                  Body=json.dumps(summary, indent=2).encode("utf-8"))
    return summary

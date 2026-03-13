import json
import os
import sys
import time

# Make sure local modules are importable inside Lambda
sys.path.insert(0, os.path.dirname(__file__))

import boto3
from datetime import datetime, timedelta
from fetch_weather import fetch_weather
from fetch_energy  import fetch_energy
from upload_to_s3  import upload_weather, upload_energy


# ── AWS clients ───────────────────────────────────────────────────────────────
dynamodb   = boto3.resource("dynamodb")
cloudwatch = boto3.client("cloudwatch")
sns        = boto3.client("sns")

PIPELINE_TABLE  = os.environ.get("PIPELINE_TABLE",  "pipeline_runs")
SNS_ALERT_ARN   = os.environ.get("SNS_ALERT_ARN",   "")   # set in Lambda env vars
CW_NAMESPACE    = "WeatherEnergyPipeline"


# ── Observability helpers ─────────────────────────────────────────────────────

def log_run(date_str: str, status: str, weather_records: int = 0,
            energy_records: int = 0, duration_ms: int = 0, error: str = ""):
    """Write one item to DynamoDB pipeline_runs table."""
    try:
        table = dynamodb.Table(PIPELINE_TABLE)
        table.put_item(Item={
            "run_date":        date_str,
            "run_ts":          datetime.utcnow().isoformat(),
            "status":          status,
            "weather_records": weather_records,
            "energy_records":  energy_records,
            "total_records":   weather_records + energy_records,
            "duration_ms":     duration_ms,
            "error":           error,
        })
        print(f"📋 Run logged to DynamoDB: {status}")
    except Exception as e:
        # Never let observability code kill the handler
        print(f"⚠️  DynamoDB log failed (non-fatal): {e}")


def emit_metrics(weather_records: int, energy_records: int,
                 duration_ms: int, success: bool):
    """Emit custom CloudWatch metrics."""
    try:
        cloudwatch.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "WeatherRecordsIngested",
                    "Value": weather_records,
                    "Unit": "Count",
                },
                {
                    "MetricName": "EnergyRecordsIngested",
                    "Value": energy_records,
                    "Unit": "Count",
                },
                {
                    "MetricName": "TotalRecordsIngested",
                    "Value": weather_records + energy_records,
                    "Unit": "Count",
                },
                {
                    "MetricName": "IngestionDurationMs",
                    "Value": duration_ms,
                    "Unit": "Milliseconds",
                },
                {
                    "MetricName": "PipelineSuccess",
                    "Value": 1 if success else 0,
                    "Unit": "Count",
                },
            ],
        )
        print("📊 CloudWatch metrics emitted")
    except Exception as e:
        print(f"⚠️  CloudWatch emit failed (non-fatal): {e}")


def send_alert(subject: str, message: str):
    """Send SNS alert email on hard failure."""
    if not SNS_ALERT_ARN:
        print("⚠️  SNS_ALERT_ARN not set — skipping alert")
        return
    try:
        sns.publish(
            TopicArn=SNS_ALERT_ARN,
            Subject=subject,
            Message=message,
        )
        print(f"🚨 Alert sent: {subject}")
    except Exception as e:
        print(f"⚠️  SNS alert failed (non-fatal): {e}")


# ── Main handler ──────────────────────────────────────────────────────────────

def handler(event, context):
    """
    AWS Lambda entry point.
    Fetches yesterday's weather + energy data and uploads to S3.

    Failure contract:
      - Partial failure (one source fails): logs WARNING, still returns 200
        so EventBridge doesn't retry and double-upload the good source.
      - Total failure (both sources fail): raises exception so Lambda marks
        the invocation FAILED → EventBridge retries up to 2x → SNS alert.
    """
    date_str  = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_ts  = time.time()

    print(f"🚀 Lambda triggered. Fetching data for {date_str}")

    weather_records = 0
    energy_records  = 0
    errors          = []

    # ── Weather ───────────────────────────────────────────────────────────────
    try:
        weather_data    = fetch_weather(date_str)
        weather_records = len(weather_data)
        if weather_records == 0:
            raise ValueError("fetch_weather returned 0 records — possible API issue")
        upload_weather(weather_data, date_str)
        print(f"✅ Weather: {weather_records} records uploaded")
    except Exception as e:
        msg = f"Weather pipeline failed for {date_str}: {e}"
        print(f"❌ {msg}")
        errors.append(msg)

    # ── Energy ────────────────────────────────────────────────────────────────
    try:
        energy_data    = fetch_energy(date_str)
        energy_records = len(energy_data)
        if energy_records == 0:
            raise ValueError("fetch_energy returned 0 records — possible API issue")
        upload_energy(energy_data, date_str)
        print(f"✅ Energy: {energy_records} records uploaded")
    except Exception as e:
        msg = f"Energy pipeline failed for {date_str}: {e}"
        print(f"❌ {msg}")
        errors.append(msg)

    # ── Observability ─────────────────────────────────────────────────────────
    duration_ms = int((time.time() - start_ts) * 1000)
    total_ok    = len(errors) == 0
    status      = "SUCCESS" if total_ok else ("PARTIAL" if len(errors) < 2 else "FAILED")

    log_run(
        date_str        = date_str,
        status          = status,
        weather_records = weather_records,
        energy_records  = energy_records,
        duration_ms     = duration_ms,
        error           = "; ".join(errors),
    )
    emit_metrics(weather_records, energy_records, duration_ms, success=total_ok)

    # ── Failure contract ──────────────────────────────────────────────────────
    if len(errors) == 2:
        # Both sources failed — raise so Lambda marks invocation FAILED
        # EventBridge will retry (configure max 2 retries in EventBridge rule)
        error_summary = "\n".join(errors)
        send_alert(
            subject = f"[PIPELINE FAILURE] weather-energy-pipeline {date_str}",
            message = (
                f"Both ingestion sources failed for {date_str}.\n\n"
                f"Errors:\n{error_summary}\n\n"
                f"Duration: {duration_ms}ms\n"
                f"Check CloudWatch logs for full stack trace."
            ),
        )
        raise RuntimeError(f"Total pipeline failure for {date_str}: {error_summary}")

    if len(errors) == 1:
        # Partial failure — warn but don't raise (avoid double-uploading good source on retry)
        send_alert(
            subject = f"[PIPELINE WARNING] weather-energy-pipeline {date_str}",
            message = (
                f"Partial failure for {date_str}.\n\n"
                f"Error: {errors[0]}\n\n"
                f"Weather records: {weather_records}\n"
                f"Energy records:  {energy_records}\n"
                f"Duration: {duration_ms}ms"
            ),
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date":            date_str,
            "status":          status,
            "weather_records": weather_records,
            "energy_records":  energy_records,
            "duration_ms":     duration_ms,
            "errors":          errors,
        }),
    }
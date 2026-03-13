"""
One-time setup script.
Run once from your terminal to create the DynamoDB table and SNS alert topic.

Usage:
    python setup_infra.py
"""
import boto3
import json

AWS_REGION     = "us-east-1"   # change if your stack is in a different region
PIPELINE_TABLE = "pipeline_runs"
SNS_TOPIC_NAME = "weather-energy-pipeline-alerts"
ALERT_EMAIL    = input("Enter your alert email address: ").strip()

dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)
sns      = boto3.client("sns",      region_name=AWS_REGION)
iam      = boto3.client("iam",      region_name=AWS_REGION)


# ── 1. DynamoDB table ─────────────────────────────────────────────────────────
print("\n📋 Creating DynamoDB table...")
try:
    dynamodb.create_table(
        TableName            = PIPELINE_TABLE,
        KeySchema            = [{"AttributeName": "run_date", "KeyType": "HASH"}],
        AttributeDefinitions = [{"AttributeName": "run_date", "AttributeType": "S"}],
        BillingMode          = "PAY_PER_REQUEST",   # on-demand, no capacity planning
        Tags                 = [{"Key": "Project", "Value": "weather-energy-pipeline"}],
    )
    print(f"  ✅ Table '{PIPELINE_TABLE}' created (PAY_PER_REQUEST billing)")
except dynamodb.exceptions.ResourceInUseException:
    print(f"  ⏭️  Table '{PIPELINE_TABLE}' already exists — skipping")


# ── 2. SNS topic + email subscription ────────────────────────────────────────
print("\n🔔 Creating SNS alert topic...")
topic = sns.create_topic(
    Name = SNS_TOPIC_NAME,
    Tags = [{"Key": "Project", "Value": "weather-energy-pipeline"}],
)
topic_arn = topic["TopicArn"]
print(f"  ✅ Topic ARN: {topic_arn}")

sns.subscribe(
    TopicArn = topic_arn,
    Protocol = "email",
    Endpoint = ALERT_EMAIL,
)
print(f"  ✅ Subscription created for {ALERT_EMAIL}")
print(f"  ⚠️  Check your inbox and confirm the subscription before alerts will work!")


# ── 3. Print Lambda environment variables to set ─────────────────────────────
print("\n" + "="*60)
print("Add these environment variables to your Lambda function:")
print("="*60)
print(f"  PIPELINE_TABLE = {PIPELINE_TABLE}")
print(f"  SNS_ALERT_ARN  = {topic_arn}")
print()
print("Run these AWS CLI commands to set them:")
print()
print(f"""aws lambda update-function-configuration \\
  --function-name weather-energy-ingestion \\
  --environment 'Variables={{
    "EIA_API_KEY":     "<your-key>",
    "S3_BUCKET_NAME":  "weather-energy-pipeline-dchau",
    "PIPELINE_TABLE":  "{PIPELINE_TABLE}",
    "SNS_ALERT_ARN":   "{topic_arn}"
  }}'""")


# ── 4. IAM: add DynamoDB + CloudWatch + SNS permissions to Lambda role ────────
print("\n🔐 Adding permissions to Lambda IAM role...")

LAMBDA_ROLE = "weather-energy-lambda-role"

inline_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid":    "DynamoDBPipelineRuns",
            "Effect": "Allow",
            "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
            "Resource": f"arn:aws:dynamodb:{AWS_REGION}:*:table/{PIPELINE_TABLE}",
        },
        {
            "Sid":    "CloudWatchMetrics",
            "Effect": "Allow",
            "Action": ["cloudwatch:PutMetricData"],
            "Resource": "*",
        },
        {
            "Sid":    "SNSAlerts",
            "Effect": "Allow",
            "Action": ["sns:Publish"],
            "Resource": topic_arn,
        },
    ],
}

try:
    iam.put_role_policy(
        RoleName       = LAMBDA_ROLE,
        PolicyName     = "pipeline-observability",
        PolicyDocument = json.dumps(inline_policy),
    )
    print(f"  ✅ Inline policy 'pipeline-observability' attached to {LAMBDA_ROLE}")
except Exception as e:
    print(f"  ❌ IAM policy attachment failed: {e}")
    print(f"  Attach manually in AWS Console → IAM → {LAMBDA_ROLE}")

print("\n✅ Infrastructure setup complete!")
print("\nNext steps:")
print("  1. Confirm your SNS email subscription")
print("  2. Update Lambda env vars (commands above)")
print("  3. Deploy updated lambda_handler.py")
print("  4. Upload glue_transform.py to S3 and run a test Glue job")
print("  5. Set up CloudWatch alarm: PipelineSuccess < 1 for 1 datapoint → SNS")
import json
import boto3
from analysis import run_selected_analyses

s3 = boto3.client("s3")

OUTPUT_BUCKET = "comm034-output-spoorti"  # change ONLY if your bucket name is different

def lambda_handler(event, context):
    """
    This Lambda supports:
    - API Gateway calls (A2)
    - Later: S3-triggered calls (A3)
    """

    bucket = event.get("bucket")
    key = event.get("key")
    analyses = event.get("analyses", {})

    if not bucket or not key:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "bucket and key are required"})
        }

    # 1. Read file from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8", errors="ignore")

    # 2. Run selected analyses
    result = {"file": key}
    result.update(run_selected_analyses(text, analyses))

    # 3. Write result to output bucket
    output_key = key.replace("uploads/", "results/").rsplit(".", 1)[0] + "_analysis.json"

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=output_key,
        Body=json.dumps(result, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Analysis complete",
            "output_key": output_key
        })
    }

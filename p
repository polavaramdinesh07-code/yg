import boto3
import os

# ---------- SOURCE BUCKET CREDENTIALS ----------
SOURCE_ACCESS_KEY = "YOUR_SOURCE_ACCESS_KEY"
SOURCE_SECRET_KEY = "YOUR_SOURCE_SECRET_KEY"
SOURCE_REGION = "us-east-1"  # change if needed

SOURCE_BUCKET = "source-bucket-name"
SOURCE_PREFIX = "images/2024/"   # prefix to copy

# ---------- DESTINATION BUCKET ----------
DEST_BUCKET = "destination-bucket-name"
DEST_PREFIX = "backup/images/2024/"

DEST_REGION = "us-east-1"

# ---------- SOURCE S3 CLIENT ----------
source_s3 = boto3.client(
    "s3",
    aws_access_key_id=SOURCE_ACCESS_KEY,
    aws_secret_access_key=SOURCE_SECRET_KEY,
    region_name=SOURCE_REGION
)

# ---------- DESTINATION S3 CLIENT ----------
# Uses default credentials (IAM role / ~/.aws/credentials)
dest_s3 = boto3.client("s3", region_name=DEST_REGION)

# ---------- COPY OBJECTS ----------
paginator = source_s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        source_key = obj["Key"]

        # Skip folders
        if source_key.endswith("/"):
            continue

        # Destination key
        dest_key = source_key.replace(SOURCE_PREFIX, DEST_PREFIX, 1)

        copy_source = {
            "Bucket": SOURCE_BUCKET,
            "Key": source_key
        }

        print(f"Copying {source_key} → {dest_key}")

        dest_s3.copy(
            copy_source,
            DEST_BUCKET,
            dest_key
        )

print("Copy completed")

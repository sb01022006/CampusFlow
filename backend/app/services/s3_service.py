import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from app.config import settings

LOCAL_UPLOADS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "uploads")

def is_aws_configured() -> bool:
    key = settings.aws_access_key_id.strip() if settings.aws_access_key_id else ""
    return bool(key and not key.startswith("your_aws_"))

def get_s3():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

def upload_file(file_bytes: bytes, key: str, content_type: str = "application/octet-stream") -> str:
    if is_aws_configured():
        try:
            s3 = get_s3()
            s3.put_object(
                Bucket=settings.aws_s3_bucket,
                Key=key,
                Body=file_bytes,
                ContentType=content_type,
            )
            return f"s3://{settings.aws_s3_bucket}/{key}"
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"S3 upload failed ({e}), using local filesystem fallback")

    file_path = os.path.join(LOCAL_UPLOADS_DIR, key)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(file_bytes)
    return f"local://{key}"

def get_file(key: str) -> bytes:
    if is_aws_configured():
        try:
            s3 = get_s3()
            response = s3.get_object(Bucket=settings.aws_s3_bucket, Key=key)
            return response["Body"].read()
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"S3 get_file failed ({e}), using local filesystem fallback")

    file_path = os.path.join(LOCAL_UPLOADS_DIR, key)
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return f.read()
    return b""

def list_files(prefix: str) -> list:
    if is_aws_configured():
        try:
            s3 = get_s3()
            response = s3.list_objects_v2(Bucket=settings.aws_s3_bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"S3 list_files failed ({e}), using local filesystem fallback")

    results = []
    prefix_dir = os.path.join(LOCAL_UPLOADS_DIR, prefix)
    if os.path.exists(prefix_dir):
        for root, _, files in os.walk(prefix_dir):
            for file in files:
                rel = os.path.relpath(os.path.join(root, file), LOCAL_UPLOADS_DIR)
                results.append(rel)
    return results

def delete_file(key: str):
    if is_aws_configured():
        try:
            s3 = get_s3()
            s3.delete_object(Bucket=settings.aws_s3_bucket, Key=key)
            return
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"S3 delete_file failed ({e}), using local filesystem fallback")

    file_path = os.path.join(LOCAL_UPLOADS_DIR, key)
    if os.path.exists(file_path):
        os.remove(file_path)

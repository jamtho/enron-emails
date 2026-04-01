"""Upload generated Parquet files to S3-compatible storage."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


def _get_client() -> boto3.client:
    """Create an S3 client from environment variables."""
    load_dotenv()
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint_url:
        raise RuntimeError("S3_ENDPOINT_URL not set in .env")
    return boto3.client("s3", endpoint_url=endpoint_url)


def _get_bucket() -> str:
    """Return the configured S3 bucket name."""
    load_dotenv()
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set in .env")
    return bucket


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    """Create the bucket if it doesn't already exist."""
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)


def upload_parquet(data_dir: Path) -> list[str]:
    """Upload all Parquet files from data_dir/parquet/ to S3.

    Returns a list of S3 keys that were uploaded.
    """
    client = _get_client()
    bucket = _get_bucket()
    ensure_bucket(client, bucket)

    parquet_dir = data_dir / "parquet"
    if not parquet_dir.exists():
        raise FileNotFoundError(f"No parquet directory at {parquet_dir}")

    files = sorted(parquet_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No .parquet files found in {parquet_dir}")

    uploaded: list[str] = []
    for path in files:
        key = str(path.relative_to(parquet_dir))
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  {key} ({size_mb:.1f} MB)")
        client.upload_file(str(path), bucket, key)
        uploaded.append(key)

    return uploaded

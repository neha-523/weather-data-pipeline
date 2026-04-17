"""
Run this once after `docker compose up` to create the MinIO buckets.

Usage:
    python scripts/init_minio_buckets.py
"""

from minio import Minio

MINIO_ENDPOINT  = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKETS = [
    "raw",        # bronze: raw JSON from API
    "processed",  # silver: cleaned Parquet files
]


def main():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"  Created bucket: {bucket}")
        else:
            print(f"  Bucket already exists: {bucket}")

    print("Done.")


if __name__ == "__main__":
    main()

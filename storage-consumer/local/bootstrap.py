"""Create only the named local fixture bucket/topics; credentials are Compose-only."""

import time
from urllib.parse import urlsplit

from botocore.exceptions import ClientError
from maintai_messaging.admin import create_local_topics

from maintai_storage.settings import Settings

settings = Settings.from_env()
if (
    not settings.kafka.local_mode
    or settings.bucket != "maintai-storage-demo"
    or urlsplit(settings.endpoint_url or "").hostname not in {"minio", "127.0.0.1", "localhost"}
):
    raise RuntimeError("Bootstrap is restricted to the isolated Storage demo")
client = settings.create_s3_client()
try:
    for attempt in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            if attempt == 59:
                raise RuntimeError("Local object store did not become ready") from None
            time.sleep(1)
    try:
        client.create_bucket(Bucket=settings.bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise
    client.put_bucket_versioning(
        Bucket=settings.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    create_local_topics(settings.kafka)
finally:
    client.close()
print("Local fixture topics and versioned bucket ready")

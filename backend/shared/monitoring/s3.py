"""Versioned bounded object reads; credentials come from the standard SDK chain."""

from maintai_messaging.contracts import strict_json

from backend.shared.alerts.domain import AlertError


class S3Reports:
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def _get(self, key, version=None, *, maximum=2097152):
        options = {"Bucket": self.bucket, "Key": key}
        if version:
            options["VersionId"] = version
        try:
            response = self.client.get_object(**options)
            stream = response["Body"]
            try:
                if version and response.get("VersionId") != version:
                    raise ValueError("Object version differs")
                if response.get("ContentLength", maximum + 1) > maximum:
                    raise ValueError("Object exceeds bound")
                body = stream.read(maximum + 1)
                if len(body) > maximum:
                    raise ValueError("Object exceeds bound")
                return body
            finally:
                stream.close()
        except Exception:
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE") from None

    def read(self, ref):
        if ref["bucket"] != self.bucket or not ref["version_id"]:
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
        return self._get(ref["key"], ref["version_id"])

    def page(self, prefix, continuation=None):
        options = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 25}
        if continuation:
            options["ContinuationToken"] = continuation
        response = self.client.list_objects_v2(**options)
        entries = []
        for entry in response.get("Contents", []):
            key = entry["Key"]
            if not key.endswith(".json"):
                continue
            try:
                value = strict_json(self._get(key, maximum=262144))
                expected = (
                    "drift-report-manifests/published/"
                    + value["publication_at"][:10]
                    + "/"
                    + value["report_id"]
                    + ".json"
                )
                if expected != key:
                    raise ValueError("Manifest locator differs")
                entries.append((key, value))
            except Exception:
                entries.append((key, None))
        token = response.get("NextContinuationToken") if response.get("IsTruncated") else None
        if response.get("IsTruncated") and (not token or token == continuation):
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
        return entries, token


def build(config):
    import boto3
    from botocore.config import Config

    return S3Reports(
        boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            region_name=config.region,
            config=Config(
                connect_timeout=2,
                read_timeout=3,
                retries={"total_max_attempts": 1},
                s3={"addressing_style": "path"},
            ),
        ),
        config.bucket,
    )

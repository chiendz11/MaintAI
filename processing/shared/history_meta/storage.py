"""Bounded exact-version object reads and conditional private export writes."""

import hashlib
import time

from .security import HistoryError


class Objects:
    def __init__(self, client, *, raw_bucket, export_bucket):
        if not raw_bucket or not export_bucket:
            raise ValueError("Explicit raw and private export buckets are required")
        self.client, self.raw_bucket, self.export_bucket = client, raw_bucket, export_bucket

    def _authorize(self, reference):
        bucket, key = reference.get("bucket"), reference.get("key", "")
        allowed = (
            bucket == self.raw_bucket and key.startswith(("raw-sensor/", "raw-catalog/v1/"))
        ) or (bucket == self.export_bucket and key.startswith("history-exports/"))
        if not allowed or not reference.get("version_id") or reference["version_id"] == "null":
            raise HistoryError(503, "HISTORY_UNAVAILABLE")

    def read(self, reference, *, max_bytes=16 * 1024 * 1024):
        self._authorize(reference)
        if (
            type(reference.get("size_bytes")) is not int
            or not 0 <= reference["size_bytes"] <= max_bytes
        ):
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        raw, digest, started = bytearray(), hashlib.sha256(), time.monotonic()
        try:
            response = self.client.get_object(
                Bucket=reference["bucket"], Key=reference["key"], VersionId=reference["version_id"]
            )
            body = response["Body"]
            try:
                while chunk := body.read(65536):
                    if len(raw) + len(chunk) > max_bytes or time.monotonic() - started > 10:
                        raise HistoryError(503, "HISTORY_UNAVAILABLE")
                    raw.extend(chunk)
                    digest.update(chunk)
            finally:
                body.close()
            if (
                len(raw) != reference["size_bytes"]
                or digest.hexdigest() != reference["sha256"]
                or response.get("VersionId") != reference["version_id"]
            ):
                raise HistoryError(503, "HISTORY_UNAVAILABLE")
            return bytes(raw)
        except HistoryError:
            raise
        except Exception:
            raise HistoryError(503, "HISTORY_UNAVAILABLE") from None

    def pointer(self):
        # Only this configured discovery key may be read without a pre-pinned ref.
        try:
            response = self.client.get_object(
                Bucket=self.raw_bucket, Key="raw-catalog/v1/current.json"
            )
            body = response["Body"]
            try:
                raw = body.read(65537)
                if (
                    len(raw) > 65536
                    or not response.get("VersionId")
                    or response["VersionId"] == "null"
                ):
                    raise ValueError("Invalid catalog pointer")
                return raw
            finally:
                body.close()
        except Exception:
            raise HistoryError(503, "HISTORY_UNAVAILABLE") from None

    def put_export(self, key, file, size, sha256, content_type):
        if not key.startswith("history-exports/") or size > 512 * 1024 * 1024:
            raise HistoryError(422, "QUERY_TOO_LARGE")
        try:
            file.seek(0)
            try:
                response = self.client.put_object(
                    Bucket=self.export_bucket,
                    Key=key,
                    Body=file,
                    ContentLength=size,
                    ContentType=content_type,
                    IfNoneMatch="*",
                    Metadata={"sha256": sha256},
                )
            except Exception:
                # A timeout may have committed. Reconcile only this deterministic key.
                response = self.client.head_object(Bucket=self.export_bucket, Key=key)
            reference = {
                "bucket": self.export_bucket,
                "key": key,
                "version_id": response.get("VersionId"),
                "sha256": sha256,
                "size_bytes": size,
            }
            self.verify(reference)
            return reference
        except HistoryError:
            raise
        except Exception:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE") from None

    def verify(self, reference):
        self._authorize(reference)
        digest, size, started = hashlib.sha256(), 0, time.monotonic()
        response = self.client.get_object(
            Bucket=reference["bucket"], Key=reference["key"], VersionId=reference["version_id"]
        )
        body = response["Body"]
        try:
            while chunk := body.read(65536):
                digest.update(chunk)
                size += len(chunk)
                if size > reference["size_bytes"] or time.monotonic() - started > 60:
                    raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")
        finally:
            body.close()
        if size != reference["size_bytes"] or digest.hexdigest() != reference["sha256"]:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")

    def signed_url(self, reference):
        self.verify(reference)
        return self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": reference["bucket"],
                "Key": reference["key"],
                "VersionId": reference["version_id"],
            },
            ExpiresIn=300,
        )

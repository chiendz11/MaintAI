"""Nạp nhiều tài liệu PDF từ danh sách JSON."""

import argparse
import json
from pathlib import Path

from ingestion.ingest import ingest


def ingest_manifest(
    manifest_path: str,
    *,
    continue_on_error: bool = False,
) -> dict:

    manifest = Path(manifest_path).resolve(strict=True)

    records = json.loads(manifest.read_text(encoding="utf-8"))

    if not isinstance(records, list) or not records:
        raise ValueError("Manifest must be a nonempty JSON array")

    results = []
    seen = set()
    validated_records = []

    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise TypeError(f"Entry {index} must be an object")

        required = (
            "path",
            "document_id",
            "document_version",
            "section",
        )

        if any(
            not isinstance(record.get(key), str) or not record[key].strip()
            for key in required
        ):
            raise ValueError(f"Entry {index} has invalid metadata")

        document_id = record["document_id"].strip()
        version = record["document_version"].strip()

        identity = (document_id, version)

        if identity in seen:
            raise ValueError(f"Duplicate document/version: {identity}")

        seen.add(identity)

        pdf = (manifest.parent / record["path"]).resolve()

        if not pdf.is_relative_to(manifest.parent):
            raise ValueError(f"Entry {index}: PDF outside documents directory")

        if not pdf.is_file() or pdf.suffix.lower() != ".pdf":
            raise ValueError(f"Entry {index}: invalid PDF path")
        validated_records.append(
            {
                "pdf": pdf,
                "document_id": document_id,
                "version": version,
                "section": record["section"].strip(),
            }
        )

    for record in validated_records:
        try:
            inserted = ingest(
                str(record["pdf"]),
                record["document_id"],
                record["version"],
                record["section"],
            )

            results.append(
                {
                    "document_id": record["document_id"],
                    "version": record["version"],
                    "path": str(record["pdf"]),
                    "inserted": inserted,
                    "status": "ok",
                }
            )

        except Exception as exc:
            if not continue_on_error:
                raise

            results.append(
                {
                    "document_id": record["document_id"],
                    "version": record["version"],
                    "path": str(record["pdf"]),
                    "status": "failed",
                    "error_type": type(exc).__name__,
                }
            )

    return {
        "total": len(results),
        "succeeded": sum(item["status"] == "ok" for item in results),
        "failed": sum(item["status"] == "failed" for item in results),
        "inserted_chunks": sum(item.get("inserted", 0) for item in results),
        "documents": results,
    }


def main():
    parser = argparse.ArgumentParser(description="Batch PDF ingestion for MaintAI RAG")

    parser.add_argument(
        "--manifest",
        required=True,
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
    )

    args = parser.parse_args()

    report = ingest_manifest(
        args.manifest,
        continue_on_error=args.continue_on_error,
    )

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        )
    )

    if report["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

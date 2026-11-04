"""Explicit, restartable knowledge preparation; never calls Groq generation."""

import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg

from ingestion.embedding import Embedder
from ingestion.ingest import ingest
from knowledge.vector_store import PostgresVectorStore

ROOT = Path(__file__).resolve().parents[1]


def read_manifest(path, root=ROOT):
    value = json.loads(Path(path).read_text())
    if value.get("schema_version") != "1.0.0" or not value.get("documents"):
        raise ValueError("A versioned nonempty knowledge manifest is required")
    lock = json.loads((root / "embedding-lock.json").read_text())
    if value.get("embedding") != lock:
        raise ValueError("Embedding manifest differs from the application lock")
    seen = set()
    for item in value["documents"]:
        key = (item["document_id"], item["version"])
        if key in seen or not all(
            isinstance(item[k], str) and item[k].strip()
            for k in ("document_id", "version", "section", "query")
        ):
            raise ValueError("Invalid or duplicate knowledge document")
        seen.add(key)
        source = (root / item["path"]).resolve()
        if (
            not source.is_relative_to((root / "documents").resolve())
            or source.suffix.lower() != ".pdf"
        ):
            raise ValueError("Knowledge source must be an application PDF")
        if hashlib.sha256(source.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("Knowledge PDF differs from its selected checksum")
    return value


def prepare(path, *, root=ROOT):
    manifest = read_manifest(path, root)
    embedding = manifest["embedding"]
    for key, value in (
        ("EMBEDDING_MODEL", embedding["model"]),
        ("EMBEDDING_REVISION", embedding["revision"]),
    ):
        if os.getenv(key) and os.environ[key] != value:
            raise ValueError("Runtime embedding differs from the selected manifest")
        os.environ[key] = value
    # A single owner Job prepares schema, model, documents and verifies retrieval.
    # Session-level lock spans ingestion's own short DB transactions.
    with psycopg.connect(os.environ["DATABASE_URL"]) as owner:
        owner.execute("SELECT pg_advisory_lock(643926105)")
        owner.execute((root / "migrations/init.sql").read_text(encoding="utf-8-sig"))
        owner.commit()
        embedder = Embedder()
        if embedder.encode(["ready"]).shape != (1, embedding["dimensions"]):
            raise ValueError("Embedding dimension differs from the knowledge schema")
        os.environ["MODEL_LOCAL_FILES_ONLY"] = "true"
        store = PostgresVectorStore()
        reports = []
        for item in manifest["documents"]:
            selector = json.dumps(
                {
                    "document_id": item["document_id"],
                    "document_version": item["version"],
                }
            )
            with owner.cursor() as cursor:
                cursor.execute(
                    "SELECT metadata FROM knowledge_chunks WHERE metadata @> %s::jsonb",
                    (selector,),
                )
                existing = cursor.fetchall()
            if any(
                row[0].get("source_pdf_sha256") != item["sha256"]
                or row[0].get("embedding_model") != embedding["model"]
                or row[0].get("embedding_revision") != embedding["revision"]
                for row in existing
            ):
                raise ValueError(
                    "Existing document/version has different or unverified provenance; use a new version"
                )
            inserted = ingest(
                str(root / item["path"]),
                item["document_id"],
                item["version"],
                item["section"],
            )
            documents = store.search(
                embedder.encode([item["query"]])[0],
                top_k=3,
                filters={
                    "document_id": item["document_id"],
                    "document_version": item["version"],
                },
            )
            if not documents or any(
                not all(
                    doc.metadata.get(k)
                    for k in (
                        "chunk_id",
                        "document_id",
                        "document_version",
                        "section",
                        "page",
                    )
                )
                for doc in documents
            ):
                raise ValueError(
                    "Knowledge retrieval/citation metadata verification failed"
                )
            with owner.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM knowledge_chunks WHERE metadata @> %s::jsonb",
                    (selector,),
                )
                count = cursor.fetchone()[0]
            reports.append(
                {
                    "document_id": item["document_id"],
                    "version": item["version"],
                    "inserted": inserted,
                    "stored_chunks": count,
                    "retrieved_chunk_ids": [
                        doc.metadata["chunk_id"] for doc in documents
                    ],
                }
            )
    result = {
        "schema_version": "1.0.0",
        "embedding": embedding,
        "documents": reports,
        "groq_generation_tested": False,
    }
    print(json.dumps(result, ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(ROOT / "knowledge-demo.json"))
    prepare(parser.parse_args().manifest)


if __name__ == "__main__":
    main()

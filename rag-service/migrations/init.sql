CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS knowledge_chunks (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    embedding VECTOR(384) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Enforce unique chunk identity for versioned documents.
-- Existing legacy chunks without these fields are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS uq_knowledge_document_chunk
ON knowledge_chunks (
    (metadata->>'document_id'),
    (metadata->>'document_version'),
    (metadata->>'chunk_index')
)
WHERE
    NULLIF(BTRIM(metadata->>'document_id'), '') IS NOT NULL
    AND NULLIF(BTRIM(metadata->>'document_version'), '') IS NOT NULL
    AND NULLIF(BTRIM(metadata->>'chunk_index'), '') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_metadata
ON knowledge_chunks
USING GIN (metadata);

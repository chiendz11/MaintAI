import argparse
import psycopg
from knowledge.vector_store import PostgresVectorStore
from ingestion.ingest import ingest

def main():
    parser = argparse.ArgumentParser(description='Ingest PDF into knowledge DB')
    parser.add_argument('--pdf', required=True)
    parser.add_argument('--document-id', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--section', required=True)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if not value.strip():
            parser.error(name + ' cannot be blank')
    inserted = ingest(args.pdf, args.document_id, args.version, args.section)
    store = PostgresVectorStore()
    with psycopg.connect(store.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM knowledge_chunks WHERE metadata @> %s::jsonb', (__import__('json').dumps({'document_id': args.document_id, 'document_version': args.version}),))
            count = cur.fetchone()[0]
    print(f'Inserted this run: {inserted}; stored for document/version: {count}')

if __name__ == '__main__':
    main()

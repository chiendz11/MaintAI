from ingestion.embedding import Embedder

def main():
    model = Embedder()
    assert model.encode(['ready']).shape == (1, 384)
    print('Embedding model cached and ready')

if __name__ == '__main__':
    main()

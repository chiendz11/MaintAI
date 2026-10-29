import os
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv()

def validate_runtime_config():
    required = ('DATABASE_URL', 'OLLAMA_BASE_URL', 'OLLAMA_MODEL', 'EMBEDDING_MODEL')
    missing = [key for key in required if not os.getenv(key, '').strip()]
    if missing:
        raise ValueError('Missing runtime configuration: ' + ', '.join(missing))
    if urlparse(os.environ['DATABASE_URL']).scheme not in ('postgresql', 'postgres'):
        raise ValueError('DATABASE_URL must use postgresql:// or postgres://')
    if urlparse(os.environ['OLLAMA_BASE_URL']).scheme not in ('http', 'https'):
        raise ValueError('OLLAMA_BASE_URL must be HTTP(S)')
    try:
        if float(os.getenv('OLLAMA_TIMEOUT', '120')) <= 0:
            raise ValueError()
    except ValueError:
        raise ValueError('OLLAMA_TIMEOUT must be a positive number') from None

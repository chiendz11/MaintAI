import argparse
import os
from pathlib import Path
import psycopg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['up'])
    parser.parse_args()
    url = os.getenv('DATABASE_URL')
    if not url:
        raise SystemExit('DATABASE_URL is required')
    sql = (Path(__file__).parent / 'init.sql').read_text(encoding='utf-8')
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print('Knowledge schema ready')

if __name__ == '__main__':
    main()

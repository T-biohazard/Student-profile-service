import os
import psycopg2
from contextlib import contextmanager

TIMESCALE_URL = os.getenv("TIMESCALE_URL", "postgres://ts:ts@timescale:5432/ts")

@contextmanager
def get_conn():
    conn = psycopg2.connect(TIMESCALE_URL)
    conn.autocommit = True 
    try:
        yield conn
    finally:
        conn.close()

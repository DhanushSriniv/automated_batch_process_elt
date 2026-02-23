# utils/db.py
import os
import psycopg2
from contextlib import contextmanager

# Read connection info from environment variables
# e.g. POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT
def _get_pg_params():
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "dbname": os.getenv("POSTGRES_DB", "toronto_bike_share_project"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "Admin"),
        "port": int(os.getenv("POSTGRES_PORT", "5433")),
    }

@contextmanager
def get_pg_connection():
    """
    Usage:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                conn.commit()
    """
    params = _get_pg_params()
    conn = psycopg2.connect(**params)  # opens connection[web:27][web:29]
    try:
        yield conn
    finally:
        conn.close()


# utils/io.py
import os

def ensure_dir(path: str) -> None:
    """
    Ensure that a directory exists.
    Creates all parent folders if needed and does nothing if it already exists.
    """
    os.makedirs(path, exist_ok=True)  # robust recursive create[web:28][web:36]


if __name__ == "__main__":
    get_pg_connection()  # test connection on script run
    ensure_dir
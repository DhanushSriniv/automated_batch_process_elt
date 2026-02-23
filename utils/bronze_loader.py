# utils/bronze_loader.py
import os
import sys
from typing import Dict, Any
from psycopg2.extras import Json
# Dynamically find the project root by looking for the 'utils' folder
current_dir = os.path.dirname(os.path.abspath(__file__))
while not os.path.exists(os.path.join(current_dir, 'utils')):
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        raise RuntimeError("Could not find project root (utils folder not found)")
    current_dir = parent
sys.path.insert(0, current_dir)
from utils.db import get_pg_connection

INSERT_SQL = """
INSERT INTO bronze.gbfs_feed_raw (
    feed_type,
    source_name,
    load_batch_id,
    file_name,
    api_url,
    version,
    raw_payload
)
VALUES (%(feed_type)s,
        %(source_name)s,
        %(load_batch_id)s,
        %(file_name)s,
        %(api_url)s,
        %(version)s,
        %(raw_payload)s);
"""

def load_feed_to_bronze(
    *,
    feed_name: str,
    source_name: str,
    batch_id: str,
    api_url: str,
    payload: Dict[str, Any],
) -> None:
    version = payload.get("version")  # GBFS root version if present[web:22]
    params = {
        "feed_type": feed_name,
        "source_name": source_name,
        "load_batch_id": batch_id,
        "file_name": f"{feed_name}.json",
        "api_url": api_url,
        "version": version,
        "raw_payload": Json(payload),
    }
    with get_pg_connection() as conn, conn.cursor() as cur:
        cur.execute(INSERT_SQL, params)
        conn.commit()

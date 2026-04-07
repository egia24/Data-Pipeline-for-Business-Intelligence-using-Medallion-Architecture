"""
ingest.py
Load semua CSV Olist ke PostgreSQL schema: bronze (as-is, tanpa transformasi)

Jalankan via pyworker (recommended):
  docker exec pyworker_olist python ingestion/ingest.py

Atau dari lokal (set PG_HOST dulu):
  $env:PG_HOST="localhost"; python ingestion/ingest.py
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import psycopg2
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, CSV_PATH, CSV_TABLE_MAP


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )


def ingest():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    for filename, table_name in CSV_TABLE_MAP.items():
        path = os.path.join(CSV_PATH, filename)

        if not os.path.exists(path):
            print(f"[ingest] SKIP {filename} (tidak ditemukan)")
            continue

        print(f"[ingest] Loading {filename} → bronze.{table_name} ...")
        df = pd.read_csv(path, dtype=str).fillna("")

        # Drop & recreate tabel
        cur.execute(f'DROP TABLE IF EXISTS bronze."{table_name}"')

        # Buat kolom dari header CSV
        cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS bronze."{table_name}" ({cols})')

        # Insert data
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query  = f'INSERT INTO bronze."{table_name}" VALUES ({placeholders})'
        cur.executemany(insert_query, df.values.tolist())

        print(f"[ingest] Done. Rows: {len(df):,}")

    cur.close()
    conn.close()
    print("\n[ingest] Selesai! Semua CSV masuk ke schema bronze.")


if __name__ == "__main__":
    ingest()
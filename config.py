"""
config.py
Satu tempat untuk semua konfigurasi project.
Semua file lain import dari sini — tidak ada hardcode di mana-mana.

Catatan:
  - PG_HOST default "postgres" → untuk dijalankan dari dalam Docker (pyworker, Spark)
  - Kalau mau jalankan dari lokal: set env PG_HOST=localhost
    contoh PowerShell: $env:PG_HOST="localhost"; python ingestion/ingest.py
"""

import os

# ════════════════════════════════════════════════════════
# PostgreSQL
# ════════════════════════════════════════════════════════
PG_HOST     = os.getenv("PG_HOST",     "postgres")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_DB       = os.getenv("PG_DB",       "dwh")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

JDBC_URL   = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

SQLALCHEMY_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

CSV_PATH   = os.getenv("CSV_PATH",   "data/")
OUTPUT_PDF = os.getenv("OUTPUT_PDF", "profiling_report.pdf")

# ════════════════════════════════════════════════════════
# Spark
# ════════════════════════════════════════════════════════
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_PG_JAR = "org.postgresql:postgresql:42.6.0"

# ════════════════════════════════════════════════════════
# Mapping CSV → tabel bronze
# ════════════════════════════════════════════════════════
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":               "orders",
    "olist_order_items_dataset.csv":          "order_items",
    "olist_order_payments_dataset.csv":       "order_payments",
    "olist_order_reviews_dataset.csv":        "order_reviews",
    "olist_customers_dataset.csv":            "customers",
    "olist_products_dataset.csv":             "products",
    "olist_sellers_dataset.csv":              "sellers",
    "product_category_name_translation.csv":  "product_category_name_translation",
}
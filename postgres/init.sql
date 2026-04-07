-- ════════════════════════════════════════════════════════
-- init.sql
-- Jalankan manual jika volume postgres sudah ada:
--   docker exec -i postgres_olist psql -U postgres < postgres/init.sql
-- ════════════════════════════════════════════════════════

CREATE DATABASE airflow;

\c dwh;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ════════════════════════════════════════════════════════
-- BRONZE (semua TEXT, as-is dari CSV)
-- ════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id                        TEXT,
    customer_id                     TEXT,
    order_status                    TEXT,
    order_purchase_timestamp        TEXT,
    order_approved_at               TEXT,
    order_delivered_carrier_date    TEXT,
    order_delivered_customer_date   TEXT,
    order_estimated_delivery_date   TEXT
);

CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_id             TEXT,
    order_item_id        TEXT,
    product_id           TEXT,
    seller_id            TEXT,
    shipping_limit_date  TEXT,
    price                TEXT,
    freight_value        TEXT
);

CREATE TABLE IF NOT EXISTS bronze.order_payments (
    order_id              TEXT,
    payment_sequential    TEXT,
    payment_type          TEXT,
    payment_installments  TEXT,
    payment_value         TEXT
);

CREATE TABLE IF NOT EXISTS bronze.order_reviews (
    review_id               TEXT,
    order_id                TEXT,
    review_score            TEXT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TEXT,
    review_answer_timestamp TEXT
);

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id               TEXT,
    customer_unique_id        TEXT,
    customer_zip_code_prefix  TEXT,
    customer_city             TEXT,
    customer_state            TEXT
);

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id                  TEXT,
    product_category_name       TEXT,
    product_name_lenght         TEXT,
    product_description_lenght  TEXT,
    product_photos_qty          TEXT,
    product_weight_g            TEXT,
    product_length_cm           TEXT,
    product_height_cm           TEXT,
    product_width_cm            TEXT
);

CREATE TABLE IF NOT EXISTS bronze.sellers (
    seller_id               TEXT,
    seller_zip_code_prefix  TEXT,
    seller_city             TEXT,
    seller_state            TEXT
);

CREATE TABLE IF NOT EXISTS bronze.geolocation (
    geolocation_zip_code_prefix  TEXT,
    geolocation_lat              TEXT,
    geolocation_lng              TEXT,
    geolocation_city             TEXT,
    geolocation_state            TEXT
);

CREATE TABLE IF NOT EXISTS bronze.product_category_name_translation (
    product_category_name         TEXT,
    product_category_name_english TEXT
);

-- ════════════════════════════════════════════════════════
-- SILVER (star schema)
-- ════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS silver.dim_customer (
    customer_id       TEXT PRIMARY KEY,
    customer_city     TEXT,
    customer_state    TEXT,
    customer_zip_code TEXT
);

CREATE TABLE IF NOT EXISTS silver.dim_product (
    product_id          TEXT PRIMARY KEY,
    product_category    TEXT,
    product_category_en TEXT,
    product_weight_g    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS silver.dim_seller (
    seller_id       TEXT PRIMARY KEY,
    seller_city     TEXT,
    seller_state    TEXT,
    seller_zip_code TEXT
);

CREATE TABLE IF NOT EXISTS silver.dim_geolocation (
    zip_code TEXT,
    lat      DOUBLE PRECISION,
    lng      DOUBLE PRECISION,
    city     TEXT,
    state    TEXT
);

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_id              TEXT PRIMARY KEY,
    order_id             TEXT,
    purchase_date        DATE,
    delivered_date       DATE,
    estimated_delivery   DATE,
    month                INTEGER,
    year                 INTEGER,
    delivery_delay_days  INTEGER
);

CREATE TABLE IF NOT EXISTS silver.fact_orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT,
    seller_id      TEXT,
    product_id     TEXT,
    date_id        TEXT,
    payment_value  DOUBLE PRECISION,
    freight_value  DOUBLE PRECISION,
    review_score   INTEGER,
    order_status   TEXT
);

-- ════════════════════════════════════════════════════════
-- GOLD (tabel analitik untuk Metabase)
-- ════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.monthly_revenue (
    year             INTEGER,
    month            INTEGER,
    total_revenue    NUMERIC(14,2),
    total_freight    NUMERIC(14,2),
    total_orders     INTEGER,
    avg_order_value  NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS gold.order_status_summary (
    order_status  TEXT,
    total_orders  INTEGER
);

CREATE TABLE IF NOT EXISTS gold.top_products (
    product_id          TEXT,
    product_category_en TEXT,
    total_orders        INTEGER,
    total_revenue       NUMERIC(14,2),
    avg_order_value     NUMERIC(14,2),
    avg_rating          NUMERIC(4,2)
);

CREATE TABLE IF NOT EXISTS gold.review_monthly (
    year          INTEGER,
    month         INTEGER,
    avg_rating    NUMERIC(4,2),
    total_reviews INTEGER
);

CREATE TABLE IF NOT EXISTS gold.review_score_distribution (
    review_score  INTEGER,
    total_reviews INTEGER
);

CREATE TABLE IF NOT EXISTS gold.customer_by_city (
    customer_city   TEXT,
    customer_state  TEXT,
    total_customers INTEGER,
    total_orders    INTEGER,
    total_revenue   NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS gold.delivery_performance (
    year            INTEGER,
    month           INTEGER,
    total_delivered INTEGER,
    avg_delay_days  NUMERIC(6,2),
    total_late      INTEGER,
    total_ontime    INTEGER
);

CREATE TABLE IF NOT EXISTS gold.seller_performance (
    seller_id       TEXT,
    seller_city     TEXT,
    seller_state    TEXT,
    total_orders    INTEGER,
    total_revenue   NUMERIC(14,2),
    avg_order_value NUMERIC(14,2),
    avg_rating      NUMERIC(4,2),
    avg_delay_days  NUMERIC(6,2)
);
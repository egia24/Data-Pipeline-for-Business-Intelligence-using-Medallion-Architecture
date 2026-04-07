# Data Pipeline for Business Intelligence using Medallion Architecture

Pipeline data end-to-end yang mengolah data transaksi e-commerce dari **Olist** (marketplace terbesar di Brazil) menggunakan arsitektur **Medallion (Bronze → Silver → Gold)** dengan Apache Airflow, Apache Spark, PostgreSQL, dan Metabase — seluruhnya berjalan di atas Docker.

---
LINK PPT = https://canva.link/2rlhl3acfnjwhai
LINK LOGBOOK = https://docs.google.com/spreadsheets/d/1xMoLw9GzghXDG2Vu7kLhQKujbdWC939DmnYLPL1OQhs/edit?usp=sharing

## Latar Belakang

Industri e-commerce menghasilkan data dalam jumlah sangat besar. Tanpa pengelolaan yang baik, data tersebut menjadi sia-sia. Proyek ini hadir untuk mengubah data mentah transaksi, ulasan pelanggan, dan performa seller menjadi insight bisnis yang actionable — mulai dari tren penjualan, revenue, produk terlaris per kategori dan wilayah, hingga pemantauan kepuasan pelanggan dan performa seller.

---

## Tujuan Proyek

- Memberikan insight bisnis yang mudah diakses kapan saja oleh tim non-teknis maupun manajemen, tanpa perlu skill SQL.
- Mendukung keputusan berbasis data terkait promosi, stok, kategori produk, dan manajemen seller.

Insight yang dihasilkan:
- Tren revenue bulanan
- Top kategori produk terlaris
- Rata-rata skor review per seller
- Distribusi order per wilayah Brazil
- Rata-rata waktu pengiriman per kota

---

## Tech Stack

| Tool | Layer | Fungsi |
|---|---|---|
| Docker | Infra | Membungkus seluruh komponen dalam environment yang terisolasi dan konsisten |
| Apache Airflow | Orkestrasi | Mengatur urutan eksekusi pipeline secara otomatis (DAG: `olist_pipeline`) |
| Python + psycopg2 | Bronze | Membaca file CSV dan memuat raw data ke Bronze layer |
| Apache Spark (PySpark) | Silver | Membersihkan data, memperbaiki tipe data, deduplikasi, transformasi ke star schema |
| Apache Spark (PySpark) | Gold | Agregasi data Silver menjadi tabel analitik siap pakai |
| PostgreSQL | Serving | Menyimpan data Bronze, Silver, dan Gold |
| Metabase | Visualisasi | Dashboard interaktif yang terhubung ke PostgreSQL |

---

## Arsitektur

```
Data Source (CSV Lokal)
        │
        ▼
  ┌─────────────────────────────────────────────┐
  │              Apache Airflow (DAG)            │
  │                                             │
  │  [Bronze] ──Spark──> [Silver] ──Spark──> [Gold]  │
  │      Raw Data       Star Schema      Aggregated  │
  └─────────────────────────────────────────────┘
        │
        ▼
   PostgreSQL  ──>  Metabase Dashboard
```

Jadwal pipeline: setiap hari pukul 06:00 (`0 6 * * *`), write mode: overwrite.

---

## Sumber Data

**Brazilian E-Commerce Public Dataset by Olist**
- Platform: [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Periode: 2016 – 2018
- Total order: 100.000+

| File | Deskripsi | Rows |
|---|---|---|
| `olist_orders_dataset` | Status order & timestamp tiap tahap pengiriman | 99,441 |
| `olist_order_items_dataset` | Detail item per order (produk, seller, harga, ongkir) | 112,650 |
| `olist_order_payments_dataset` | Metode dan nilai pembayaran per order | 103,886 |
| `olist_order_reviews_dataset` | Ulasan pelanggan setelah menerima pesanan | 99,224 |
| `olist_customers_dataset` | Data pelanggan unik beserta lokasi domisili | 99,441 |
| `olist_sellers_dataset` | Data seller terdaftar beserta lokasi (3,095 seller) | 3,095 |
| `olist_products_dataset` | Katalog produk dengan dimensi fisik dan kategori | 32,951 |
| `product_category_name_translation` | Mapping nama kategori dari Portugis ke Inggris (71 kategori) | 71 |

---

## Data Modeling

### Bronze Layer
Raw data dari CSV dimuat tanpa perubahan apapun ke PostgreSQL. Berfungsi sebagai sumber kebenaran agar data asli tetap aman jika terjadi kesalahan di tahap berikutnya.

### Silver Layer — Star Schema
Data dibersihkan dan ditransformasi ke dalam star schema:

- **Fact Table:** `fact_orders` — order_id, customer_id, seller_id, product_id, date_id, payment_value, freight_value, review_score, order_status
- **Dimension Tables:**
  - `dim_customer` — customer_id, city, state, zip_code
  - `dim_seller` — seller_id, city, state, zip_code
  - `dim_product` — product_id, category, category_en, weight_g
  - `dim_date` — date_id, purchase_date, delivered_date, estimated_delivery, month, year, delivery_delay_days

### Gold Layer — Aggregated
Tabel analitik siap pakai untuk Metabase:

| Tabel | Isi |
|---|---|
| `monthly_revenue` | Revenue, freight, total order, dan avg order value per bulan |
| `top_products` | Produk terlaris berdasarkan kategori, qty, revenue, dan rating |
| `seller_performance` | Performa seller: total order, revenue, rating, dan delay |
| `customer_by_city` | Distribusi pelanggan unik, order, dan revenue per kota |
| `delivery_performance` | Rata-rata delay, on-time, dan late delivery per bulan |
| `review_monthly` | Rata-rata rating, review positif/negatif per bulan |
| `review_score_distribution` | Distribusi skor ulasan |
| `order_status_summary` | Ringkasan jumlah order per status |

---

## Cara Menjalankan

**Prerequisites**
- Docker & Docker Compose terinstall
- Dataset Olist sudah diunduh dari Kaggle

**Langkah-langkah**

1. Clone repository
   ```bash
   git clone <repo-url>
   cd <repo-name>
   ```

2. Letakkan file CSV dataset ke dalam folder `data/`

3. Jalankan seluruh service
   ```bash
   docker-compose up -d
   ```

4. Akses Airflow UI di `http://localhost:8081`, aktifkan DAG `olist_pipeline`

5. Akses Metabase di `http://localhost:3000`, hubungkan ke PostgreSQL untuk melihat dashboard

---

## Dashboard

Dashboard Metabase mencakup minimal 5 visualisasi aktif: tren revenue bulanan, top kategori produk terlaris, rata-rata skor review per seller, distribusi order per wilayah Brazil, dan rata-rata waktu pengiriman per kota.

---

## Success Metrics

- Seluruh tabel berhasil diproses dari Bronze hingga Gold tanpa error
- Total revenue dan jumlah order di Gold konsisten dengan data Bronze
- Minimal 5 visualisasi aktif dan akurat di Metabase

---
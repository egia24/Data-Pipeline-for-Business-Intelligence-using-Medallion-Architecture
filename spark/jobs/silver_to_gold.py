"""
silver_to_gold.py
Baca dari PostgreSQL schema silver (star schema), agregasi, simpan ke schema gold

Gold tables:
  1. monthly_revenue          → Revenue, freight, items per bulan
  2. order_status_summary     → Distribusi status pesanan
  3. top_products             → Produk terlaris + rating + installments
  4. review_monthly           → Trend rating per bulan
  5. review_score_distribution → Distribusi bintang 1-5
  6. customer_by_city         → Sebaran unique customer + revenue per kota
  7. delivery_performance     → Ontime vs terlambat per bulan
  8. seller_performance       → Ranking seller by revenue, rating & items

Jalankan:
  docker exec spark_master_olist /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    /app/jobs/silver_to_gold.py
"""

import sys
sys.path.insert(0, "/opt/project")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, round, desc,
    countDistinct, when, max, min
)
from config import JDBC_URL, JDBC_PROPS, SPARK_MASTER, SPARK_PG_JAR

spark = SparkSession.builder \
    .appName("silver_to_gold") \
    .master(SPARK_MASTER) \
    .config("spark.jars.packages", SPARK_PG_JAR) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_silver(table):
    return spark.read.jdbc(JDBC_URL, f"silver.{table}", properties=JDBC_PROPS)

def write_gold(df, table):
    df.write.jdbc(JDBC_URL, f"gold.{table}", mode="overwrite", properties=JDBC_PROPS)
    print(f"[silver→gold] gold.{table} selesai. Rows: {df.count():,}")


print("[silver→gold] Loading silver tables...")
fact_orders  = read_silver("fact_orders")
dim_customer = read_silver("dim_customer")
dim_product  = read_silver("dim_product")
dim_seller   = read_silver("dim_seller")
dim_date     = read_silver("dim_date")

base = fact_orders \
    .join(dim_customer, on="customer_id", how="left") \
    .join(dim_product,  on="product_id",  how="left") \
    .join(dim_seller,   on="seller_id",   how="left") \
    .join(dim_date.drop("order_id"), on="date_id", how="left")

print("[silver→gold] monthly_revenue...")
df = base.filter(col("order_status") == "delivered") \
    .groupBy("year", "month") \
    .agg(
        round(sum("payment_value"),   2).alias("total_revenue"),
        round(sum("freight_value"),   2).alias("total_freight"),
        round(sum("item_total_price"),2).alias("total_item_price"),
        countDistinct("order_id")       .alias("total_orders"),
        sum("item_quantity")            .alias("total_items_sold"),
        round(avg("payment_value"),   2).alias("avg_order_value"),
    ) \
    .orderBy("year", "month")
write_gold(df, "monthly_revenue")

print("[silver→gold] order_status_summary...")
df = fact_orders \
    .groupBy("order_status") \
    .agg(count("order_id").alias("total_orders")) \
    .orderBy(desc("total_orders"))
write_gold(df, "order_status_summary")

print("[silver→gold] top_products...")
df = base.filter(col("order_status") == "delivered") \
    .groupBy("product_id", "product_category_en") \
    .agg(
        count("order_id")                    .alias("total_orders"),
        sum("item_quantity")                 .alias("total_qty_sold"),
        round(sum("item_total_price"),    2) .alias("total_revenue"),
        round(avg("payment_value"),       2) .alias("avg_order_value"),
        round(avg("review_score"),        2) .alias("avg_rating"),
        round(avg("payment_installments"),2) .alias("avg_installments"),
    ) \
    .orderBy(desc("total_orders"))
write_gold(df, "top_products")

print("[silver→gold] review_monthly...")
df = base.filter(col("review_score").isNotNull()) \
    .groupBy("year", "month") \
    .agg(
        round(avg("review_score"), 2).alias("avg_rating"),
        count("review_score")        .alias("total_reviews"),
        count(when(col("review_score") >= 4, 1)).alias("positive_reviews"),
        count(when(col("review_score") <= 2, 1)).alias("negative_reviews"),
    ) \
    .orderBy("year", "month")
write_gold(df, "review_monthly")

print("[silver→gold] review_score_distribution...")
df = fact_orders.filter(col("review_score").isNotNull()) \
    .groupBy("review_score") \
    .agg(count("order_id").alias("total_reviews")) \
    .orderBy("review_score")
write_gold(df, "review_score_distribution")

print("[silver→gold] customer_by_city...")
df = base \
    .groupBy("customer_city", "customer_state") \
    .agg(
        countDistinct("customer_unique_id") .alias("total_unique_customers"),
        count("order_id")                   .alias("total_orders"),
        round(sum("payment_value"),      2) .alias("total_revenue"),
        round(avg("payment_value"),      2) .alias("avg_order_value"),
    ) \
    .orderBy(desc("total_unique_customers"))
write_gold(df, "customer_by_city")

print("[silver→gold] delivery_performance...")
df = base.filter(col("order_status") == "delivered") \
    .groupBy("year", "month") \
    .agg(
        count("order_id")                               .alias("total_delivered"),
        round(avg("delivery_delay_days"),            2) .alias("avg_delay_days"),
        count(when(col("delivery_delay_days") > 0,  1)) .alias("total_late"),
        count(when(col("delivery_delay_days") <= 0, 1)) .alias("total_ontime"),
    ) \
    .orderBy("year", "month")
write_gold(df, "delivery_performance")

print("[silver→gold] seller_performance...")
df = base.filter(col("order_status") == "delivered") \
    .groupBy("seller_id", "seller_city", "seller_state") \
    .agg(
        count("order_id")                        .alias("total_orders"),
        sum("item_quantity")                     .alias("total_items_sold"),
        round(sum("payment_value"),          2)  .alias("total_revenue"),
        round(avg("payment_value"),          2)  .alias("avg_order_value"),
        round(avg("review_score"),           2)  .alias("avg_rating"),
        round(avg("delivery_delay_days"),    2)  .alias("avg_delay_days"),
        count(when(col("review_score") >= 4, 1)) .alias("positive_reviews"),
    ) \
    .orderBy(desc("total_revenue"))
write_gold(df, "seller_performance")


spark.stop()
print("\n[silver→gold] Semua tabel gold selesai! Siap di Metabase.")
"""
bronze_to_silver.py
Baca dari PostgreSQL schema bronze, cleaning lebih dalam, bentuk star schema,
simpan ke schema silver

Star Schema:
  Fact  : fact_orders
  Dims  : dim_customer, dim_product, dim_seller, dim_date

Jalankan:
  docker exec spark_master_olist /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    /app/jobs/bronze_to_silver.py
"""

import sys
sys.path.insert(0, "/opt/project")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, trim, lower, upper, when, lit,
    month, year, datediff, concat_ws, md5, regexp_replace, initcap
)
from config import JDBC_URL, JDBC_PROPS, SPARK_MASTER, SPARK_PG_JAR

spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .master(SPARK_MASTER) \
    .config("spark.jars.packages", SPARK_PG_JAR) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_bronze(table):
    return spark.read.jdbc(JDBC_URL, f"bronze.{table}", properties=JDBC_PROPS)

def write_silver(df, table):
    df.write.jdbc(JDBC_URL, f"silver.{table}", mode="overwrite", properties=JDBC_PROPS)
    print(f"[bronze→silver] silver.{table} selesai. Rows: {df.count():,}")

def clean_city(c):
    """Normalisasi nama kota: hapus karakter spesial, Title Case"""
    return initcap(trim(regexp_replace(regexp_replace(lower(c), r"[^a-z0-9\s]", " "), r"\s+", " ")))


print("[bronze→silver] Loading bronze tables...")
df_orders    = read_bronze("orders")
df_items     = read_bronze("order_items")
df_payments  = read_bronze("order_payments")
df_reviews   = read_bronze("order_reviews")
df_customers = read_bronze("customers")
df_products  = read_bronze("products")
df_sellers   = read_bronze("sellers")
df_cat       = read_bronze("product_category_name_translation")

print("[bronze→silver] Building dim_customer...")
dim_customer = df_customers \
    .dropna(subset=["customer_id", "customer_unique_id"]) \
    .dropDuplicates(["customer_unique_id"]) \
    .withColumn("customer_city",
        when(col("customer_city").isNull() | (trim(col("customer_city")) == ""), lit("Unknown"))
        .otherwise(clean_city(col("customer_city")))) \
    .withColumn("customer_state",
        when(col("customer_state").isNull() | (trim(col("customer_state")) == ""), lit("Unknown"))
        .otherwise(trim(upper(col("customer_state"))))) \
    .withColumn("customer_zip_code",
        when(col("customer_zip_code_prefix").isNull(), lit("00000"))
        .otherwise(trim(col("customer_zip_code_prefix")))) \
    .select("customer_id", "customer_unique_id", "customer_city", "customer_state", "customer_zip_code")
write_silver(dim_customer, "dim_customer")


print("[bronze→silver] Building dim_product...")
dim_product = df_products \
    .dropDuplicates(["product_id"]) \
    .dropna(subset=["product_id"]) \
    .join(df_cat, on="product_category_name", how="left") \
    .withColumn("product_category",
        when(col("product_category_name").isNull() | (trim(col("product_category_name")) == ""), lit("uncategorized"))
        .otherwise(trim(lower(col("product_category_name"))))) \
    .withColumn("product_category_en",
        when(col("product_category_name_english").isNotNull() & (trim(col("product_category_name_english")) != ""),
             initcap(trim(col("product_category_name_english"))))
        .when(col("product_category_name").isNotNull(), initcap(trim(col("product_category_name"))))
        .otherwise(lit("Uncategorized"))) \
    .withColumn("product_weight_g",
        when(col("product_weight_g").isNull() | (col("product_weight_g") == ""), lit(None))
        .otherwise(col("product_weight_g").cast("double"))) \
    .withColumn("product_photos_qty",
        when(col("product_photos_qty").isNull(), lit(0))
        .otherwise(col("product_photos_qty").cast("integer"))) \
    .select("product_id", "product_category", "product_category_en", "product_weight_g", "product_photos_qty")
write_silver(dim_product, "dim_product")


print("[bronze→silver] Building dim_seller...")
dim_seller = df_sellers \
    .dropDuplicates(["seller_id"]) \
    .dropna(subset=["seller_id"]) \
    .withColumn("seller_city",
        when(col("seller_city").isNull() | (trim(col("seller_city")) == ""), lit("Unknown"))
        .otherwise(clean_city(col("seller_city")))) \
    .withColumn("seller_state",
        when(col("seller_state").isNull() | (trim(col("seller_state")) == ""), lit("Unknown"))
        .otherwise(trim(upper(col("seller_state"))))) \
    .withColumn("seller_zip_code",
        when(col("seller_zip_code_prefix").isNull(), lit("00000"))
        .otherwise(trim(col("seller_zip_code_prefix")))) \
    .select("seller_id", "seller_city", "seller_state", "seller_zip_code")
write_silver(dim_seller, "dim_seller")

print("[bronze→silver] Building dim_date...")
dim_date = df_orders \
    .withColumn("purchase_date",      to_date(to_timestamp("order_purchase_timestamp"))) \
    .withColumn("delivered_date",     to_date(to_timestamp("order_delivered_customer_date"))) \
    .withColumn("estimated_delivery", to_date(to_timestamp("order_estimated_delivery_date"))) \
    .dropna(subset=["purchase_date"]) \
    .withColumn("date_id", md5(concat_ws("_", col("order_id"), col("purchase_date").cast("string")))) \
    .withColumn("month", month(col("purchase_date"))) \
    .withColumn("year",  year(col("purchase_date"))) \
    .withColumn("delivery_delay_days",
        when(col("delivered_date").isNotNull() & col("estimated_delivery").isNotNull(),
             datediff(col("delivered_date"), col("estimated_delivery")))
        .otherwise(lit(None))) \
    .select("date_id", "purchase_date", "delivered_date", "estimated_delivery", "month", "year", "delivery_delay_days") \
    .dropDuplicates(["date_id"])
write_silver(dim_date, "dim_date")

print("[bronze→silver] Building fact_orders...")

# Payment per order
df_payment_agg = df_payments \
    .withColumn("payment_value",        col("payment_value").cast("double")) \
    .withColumn("payment_installments", col("payment_installments").cast("integer")) \
    .filter(col("payment_value") > 0) \
    .groupBy("order_id") \
    .agg({"payment_value": "sum", "payment_installments": "max"}) \
    .withColumnRenamed("sum(payment_value)",        "payment_value") \
    .withColumnRenamed("max(payment_installments)", "payment_installments")

# Review per order (filter score 1-5 saja)
df_review_agg = df_reviews \
    .withColumn("review_score", col("review_score").cast("integer")) \
    .filter(col("review_score").isNotNull()) \
    .filter(col("review_score").between(1, 5)) \
    .dropDuplicates(["order_id"]) \
    .select("order_id", "review_score")

# Items per order
df_items_agg = df_items \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .withColumn("price",         col("price").cast("double")) \
    .filter(col("price") > 0) \
    .groupBy("order_id", "product_id", "seller_id") \
    .agg({"freight_value": "sum", "price": "sum", "order_item_id": "count"}) \
    .withColumnRenamed("sum(freight_value)",   "freight_value") \
    .withColumnRenamed("sum(price)",           "item_total_price") \
    .withColumnRenamed("count(order_item_id)", "item_quantity") \
    .dropDuplicates(["order_id"])

# Date id referensi
df_date_ref = df_orders \
    .withColumn("purchase_date", to_date(to_timestamp("order_purchase_timestamp"))) \
    .withColumn("date_id", md5(concat_ws("_", col("order_id"), col("purchase_date").cast("string")))) \
    .select("order_id", "date_id")

# Gabung semua
fact_orders = df_orders \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id", "customer_id"]) \
    .withColumn("order_status",
        when(col("order_status").isNull() | (trim(col("order_status")) == ""), lit("unknown"))
        .otherwise(trim(lower(col("order_status"))))) \
    .join(df_items_agg,   on="order_id", how="left") \
    .join(df_payment_agg, on="order_id", how="left") \
    .join(df_review_agg,  on="order_id", how="left") \
    .join(df_date_ref,    on="order_id", how="left") \
    .withColumn("payment_value",
        when(col("payment_value").isNull(), lit(0.0)).otherwise(col("payment_value"))) \
    .withColumn("freight_value",
        when(col("freight_value").isNull(), lit(0.0)).otherwise(col("freight_value"))) \
    .withColumn("item_quantity",
        when(col("item_quantity").isNull(), lit(1)).otherwise(col("item_quantity"))) \
    .select("order_id", "customer_id", "seller_id", "product_id", "date_id",
            "payment_value", "payment_installments", "freight_value",
            "item_quantity", "item_total_price", "review_score", "order_status")
write_silver(fact_orders, "fact_orders")

spark.stop()
print("\n[bronze→silver] Star schema selesai!")
print("Tables: fact_orders, dim_customer, dim_product, dim_seller, dim_date")
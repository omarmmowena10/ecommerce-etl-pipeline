import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('EcommerceETL_Extract') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

print("✅ Spark Connected!")

HDFS_RAW = "hdfs://hadoop-namenode:9000/raw/ecommerce"
BRONZE = "hdfs://hadoop-namenode:9000/datalake/bronze"


orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])


customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])


items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])


products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
])


sellers_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])


payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

try:
    tables = {
        "orders": (orders_schema, "olist_orders_dataset"),
        "customers": (customers_schema, "olist_customers_dataset"),
        "order_items": (items_schema, "olist_order_items_dataset"),
        "products": (products_schema, "olist_products_dataset"),
        "sellers": (sellers_schema, "olist_sellers_dataset"),
        "payments": (payments_schema, "olist_order_payments_dataset"),
    }

    for table_name, (schema, folder) in tables.items():
        input_path = f"{HDFS_RAW}/{folder}/"
        output_path = f"{BRONZE}/{table_name}/"

        print(f"📥 Reading {table_name}...")
        df = spark.read \
            .schema(schema) \
            .option("header", "true") \
            .csv(input_path)

        print(f"⚡ {df.count()} rows found!")

        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)

        print(f"✅ {table_name} saved to Bronze Layer!")

    print("\n🎉 Extract complete!")

except Exception as e:
    print(f"❌ Error: {e}")
    raise e
finally:
    spark.stop()
    print("🛑 Spark Stopped.")
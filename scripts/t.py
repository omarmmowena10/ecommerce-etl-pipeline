import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('EcommerceETL_Transform') \
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

BRONZE = "hdfs://hadoop-namenode:9000/datalake/bronze"
GOLD = "hdfs://hadoop-namenode:9000/datalake/gold"

try:
    
    orders = spark.read.parquet(f"{BRONZE}/orders/")
    customers = spark.read.parquet(f"{BRONZE}/customers/")
    order_items = spark.read.parquet(f"{BRONZE}/order_items/")
    products = spark.read.parquet(f"{BRONZE}/products/")
    sellers = spark.read.parquet(f"{BRONZE}/sellers/")
    payments = spark.read.parquet(f"{BRONZE}/payments/")

    print("✅ Bronze Layer loaded!")

    orders = orders.withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    ).dropna(subset=["order_id", "customer_id"])

    try:
        spark.read.parquet(f"{GOLD}/dim_date")
        print("✅ dim_date already exists!")
    except:
        print("📅 Creating dim_date...")
        df_dates = spark.sql("""
            SELECT CAST('2016-01-01' AS DATE) as start, 
                   CAST('2020-12-31' AS DATE) as end
        """)
        dim_date = df_dates.select(
            F.explode(
                F.sequence(F.to_date("start"), F.to_date("end"),
                          F.expr("interval 1 day"))
            ).alias("date")
        ).select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.month("date").alias("month"),
            F.dayofmonth("date").alias("day"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.quarter("date").alias("quarter"),
            F.when(F.dayofweek("date").isin(1, 7), True)
             .otherwise(False).alias("is_weekend")
        )
        dim_date.write.mode("overwrite").parquet(f"{GOLD}/dim_date")
        print("✅ dim_date created!")

    
    dim_customer = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    ).dropDuplicates(["customer_id"])

    dim_customer.write.mode("overwrite").parquet(f"{GOLD}/dim_customer")
    print("✅ dim_customer created!")

    
    dim_product = products.select(
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ).dropDuplicates(["product_id"])

    dim_product.write.mode("overwrite").parquet(f"{GOLD}/dim_product")
    print("✅ dim_product created!")

    dim_seller = sellers.select(
        "seller_id",
        "seller_city",
        "seller_state"
    ).dropDuplicates(["seller_id"])

    dim_seller.write.mode("overwrite").parquet(f"{GOLD}/dim_seller")
    print("✅ dim_seller created!")

    
    fact_orders = orders \
        .join(order_items, "order_id", "left") \
        .join(payments, "order_id", "left") \
        .withColumn("date_key",
            F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int")
        ) \
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_status",
            "order_purchase_timestamp",
            "price",
            "freight_value",
            "payment_type",
            "payment_value",
            "date_key"
        )

    fact_orders.write.mode("overwrite").parquet(f"{GOLD}/fact_orders")
    print("✅ fact_orders created!")

    print("\n🎉 Transform complete! Gold Layer ready!")

except Exception as e:
    print(f"❌ Error: {e}")
    raise e
finally:
    spark.stop()
    print("🛑 Spark Stopped.")
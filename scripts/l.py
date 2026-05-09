import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('EcommerceETL_Load') \
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

sf_options = {
    "sfURL": "yjmwqyd-am21544.snowflakecomputing.com",
    "sfUser": "OMAR10",
    "sfPassword": "2vcpv4vfNkav3UH",
    "sfDatabase": "ECOMMERCE_DB",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "BANK_WH"
}

GOLD = "hdfs://hadoop-namenode:9000/datalake/gold"

def load_to_snowflake(table_name, mode="overwrite"):
    print(f"📤 Loading {table_name} to Snowflake...")
    df = spark.read.parquet(f"{GOLD}/{table_name}")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name.upper()) \
        .mode(mode) \
        .save()
    print(f"✅ {table_name} loaded successfully!")

try:
    load_to_snowflake("dim_date", mode="overwrite")
    load_to_snowflake("dim_customer", mode="overwrite")
    load_to_snowflake("dim_product", mode="overwrite")
    load_to_snowflake("dim_seller", mode="overwrite")
    load_to_snowflake("fact_orders", mode="append")

    print("\n🎉 ALL TABLES LOADED TO SNOWFLAKE!")

except Exception as e:
    print(f"❌ Error: {e}")
    raise e
finally:
    spark.stop()
    print("🛑 Spark Stopped.")
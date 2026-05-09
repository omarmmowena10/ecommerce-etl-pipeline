import pandas as pd
import requests
import math
import os


DATA_DIR = "/tmp/ecommerce_data"
HDFS_HOST = "http://hadoop-namenode:9870"
HDFS_RAW_DIR = "/raw/ecommerce"
BATCH_SIZE = 1000

FILES = [

    "olist_orders_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
]

def create_hdfs_dir(path):
    url = f"{HDFS_HOST}/webhdfs/v1{path}?op=MKDIRS&user.name=root"
    requests.put(url)
    print(f"📁 Created HDFS dir: {path}")

def upload_to_hdfs(local_path, hdfs_path):
    url = f"{HDFS_HOST}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    r = requests.put(url, allow_redirects=False)
    redirect_url = r.headers['Location']
    with open(local_path, 'rb') as f:
        requests.put(redirect_url, data=f)


create_hdfs_dir(HDFS_RAW_DIR)


for file_name in FILES:
    file_path = f"{DATA_DIR}/{file_name}"
    print(f"\n📥 Processing: {file_name}")

    df = pd.read_csv(file_path)
    print(f"✅ Loaded {len(df)} rows")

    total_batches = math.ceil(len(df) / BATCH_SIZE)
    file_dir = f"{HDFS_RAW_DIR}/{file_name.replace('.csv', '')}"
    create_hdfs_dir(file_dir)

    for i in range(total_batches):
        batch = df[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
        local_path = f"/tmp/batch_{i+1}.csv"
        batch.to_csv(local_path, index=False)

        hdfs_path = f"{file_dir}/batch_{i+1}.csv"
        upload_to_hdfs(local_path, hdfs_path)
        print(f"✅ Batch {i+1}/{total_batches} uploaded")

print("\n🎉 All files uploaded to HDFS!")
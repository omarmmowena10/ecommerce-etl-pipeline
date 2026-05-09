# 🛒 E-Commerce ETL Pipeline

An end-to-end data engineering project that processes Brazilian e-commerce data through a complete ETL pipeline using modern big data tools.

## 🏗️ Architecture

```
Kaggle Dataset → Batch Pipeline → HDFS (Raw Layer) → Spark Extract → Bronze Layer → Spark Transform → Gold Layer → Snowflake → Airflow (Daily Schedule)
```

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Hadoop (HDFS) | Distributed file storage |
| Apache Spark | Data processing & transformation |
| Apache Airflow | Pipeline orchestration |
| Snowflake | Cloud data warehouse |
| Docker | Containerization |
| Python | Scripting & automation |

## 📊 Dataset

Brazilian E-Commerce dataset from Kaggle (Olist):
- 100,000+ real orders
- Multiple CSV files: Orders, Customers, Products, Sellers, Payments

## 🔄 Pipeline Steps

1. **Ingestion** - Download dataset from Kaggle and load into HDFS in batches (1000 rows/batch)
2. **Extract** - Convert raw CSV files to Parquet format (Bronze Layer)
3. **Transform** - Clean data and build Star Schema (Gold Layer)
4. **Load** - Push final tables to Snowflake Data Warehouse
5. **Orchestrate** - Airflow DAG runs the full pipeline daily automatically

## ⭐ Star Schema (Gold Layer)

```
        FACT_ORDERS
            |
    ┌───────┼───────┐───────┐
    ↓       ↓       ↓       ↓
DIM_DATE DIM_CUSTOMER DIM_PRODUCT DIM_SELLER
```

## 🚀 How to Run

```bash
# 1. Start all services
docker-compose up -d

# 2. Upload data to HDFS
docker exec spark-jupyter python3 /tmp/batch_pipeline.py

# 3. Run ETL pipeline manually
docker exec spark-jupyter spark-submit --master yarn /tmp/e.py
docker exec spark-jupyter spark-submit --master yarn /tmp/t.py
docker exec spark-jupyter spark-submit --master yarn --jars /opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar,/opt/spark/jars/snowflake-jdbc-3.13.30.jar /tmp/l.py
```

## 📁 Project Structure

```
├── scripts/
│   ├── batch_pipeline.py  # Uploads raw data to HDFS in batches
│   ├── e.py               # Extract: CSV → Parquet (Bronze Layer)
│   ├── t.py               # Transform: Build Star Schema (Gold Layer)
│   └── l.py               # Load: Gold Layer → Snowflake
├── dags/
│   └── ecommerce_etl_dag.py  # Airflow DAG (daily schedule)
└── docker-compose.yml         # All services configuration
```

## 🌐 Services & Ports

| Service | URL |
|---------|-----|
| Hadoop UI | http://localhost:9870 |
| Airflow UI | http://localhost:18080 |
| Spark UI | http://localhost:4040 |
| YARN UI | http://localhost:8088 |

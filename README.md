# Customer Churn Prediction – Big Data Streaming Pipeline

This project demonstrates a **production-style Big Data pipeline**
for predicting customer churn using both **real-time streaming**
and **batch processing**.

The system is designed to reflect how churn prediction is handled
in large-scale data platforms, where ingestion, processing,
and model training are **decoupled for scalability and reliability**.

---

## Problem Statement

Customer churn directly impacts business revenue.
To proactively identify high-risk customers, organizations require
systems capable of processing **high-volume event data** and
generating predictive insights at scale.

This project demonstrates an **end-to-end Big Data pipeline**
combining real-time streaming and batch analytics for churn prediction.



---

## Architecture Overview

The pipeline follows a **Lambda-style architecture**:

- **Streaming Layer** for real-time ingestion
- **Batch Layer** for feature engineering and ML training
- **Orchestration Layer** for scheduling and dependency management

### High-Level Flow


- Kafka → Spark Structured Streaming → HDFS (raw & curated data)
- HDFS → Spark Batch Processing → ML Model training (MLlib)
- Airflow → Orchestrates batch processing and model training


---

## Technology Stack

- **Apache Kafka** – event ingestion and buffering
- **Apache Spark (PySpark)** – distributed processing
- **Spark Structured Streaming** – real-time analytics
- **Apache Airflow** – workflow orchestration
- **HDFS** – durable distributed storage
- **Spark MLlib** – scalable machine learning
- **Python** – implementation language

---

## Project Structure

Customer-Churn-Prediction/
├── airflow/
│ └── dags/
│ └── churn_pipeline_dag.py
├── kafka/
│ └── producer.py
├── spark/
│ ├── stream_ingest.py
│ ├── batch_processing.py
│ └── train_model.py
├── data/
│ └── README.md
├── hdfs/
├── hive/
├── logs/
├── README.md
└── .gitignore

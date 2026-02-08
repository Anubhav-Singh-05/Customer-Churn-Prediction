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
To proactively identify high-risk customers, organizations
need systems capable of processing **high-volume event data**
and generating predictive insights at scale.

This project addresses that challenge using distributed
streaming and batch analytics.


---

## System Architecture

The pipeline follows a **Lambda-style architecture**:

- **Streaming Layer** for real-time ingestion
- **Batch Layer** for feature engineering and ML training
- **Orchestration Layer** for scheduling and dependency management

### High-Level Flow

Kafka → Spark Structured Streaming → HDFS  
HDFS → Spark Batch Processing → ML Model (MLlib)  
Airflow → Orchestrates batch & training workflows

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

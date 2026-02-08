\# Customer Churn Prediction – Big Data Streaming Pipeline



An end-to-end \*\*Big Data project\*\* demonstrating real-time and batch data processing

to predict customer churn using \*\*Kafka, Apache Spark, Airflow, HDFS, and Spark MLlib\*\*.



This project follows \*\*production-style design principles\*\* where only source code

and orchestration logic are versioned, while runtime data and artifacts are generated dynamically.



---



\## 🏗 Architecture Overview



Kafka → Spark Structured Streaming → HDFS → Spark Batch Processing → ML Model  

---



\## 🧱 Architecture Diagram



> \*(Diagram shows the end-to-end data flow and orchestration)\*



Kafka Producer

↓

Spark Structured Streaming

↓

HDFS (Raw / Curated Data)

↓

Spark Batch Processing

↓

ML Model Training (MLlib)

↑

Airflow DAG (Scheduling \& Orchestration)


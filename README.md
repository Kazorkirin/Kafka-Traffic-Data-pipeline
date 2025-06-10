# Real-Time Traffic Data Pipeline

This project demonstrates a real-time traffic monitoring system using **Apache Kafka** and **Apache Spark Structured Streaming**, outputting processed traffic data to a single consolidated **CSV file**.

## 📌 Project Overview

A Python Kafka producer simulates traffic data (timestamp, location, vehicle count, and average speed) and streams it to a Kafka topic named `traffic_topic`. Apache Spark then consumes this data in real-time, processes it using Structured Streaming, and appends it to a unified CSV file for further analysis or dashboarding.

## 🔄 Architecture Flow

1. **Kafka Producer**: Sends JSON-formatted simulated traffic data.
2. **Kafka Topic (`traffic_topic`)**: Acts as the message queue buffer.
3. **Spark Structured Streaming**:
   - Reads streaming data from Kafka.
   - Deserializes and processes JSON.
   - Appends output to a CSV file in `traffic_output/`.

See the flowchart below for architecture clarity:

![Traffic Data Pipeline](A_flowchart_presents_a_Traffic_Data_Pipeline,_illu.png)

## 🧰 Technologies Used

- Python 3.12
- Apache Kafka (via Docker)
- Apache Spark 3.4.1
- PySpark
- Docker Compose

## 🚀 Getting Started

### 1. Start Kafka & Zookeeper
```bash
docker-compose up -d


2. Run Kafka Producer
bash
Copy
Edit
python traffic_producer.py
3. Run Spark Consumer
bash
Copy
Edit
spark-submit spark_consumer.py
Make sure Spark has access to pyspark and Kafka dependencies.

📁 Output
A single continuously updating CSV file at:

bash
Copy
Edit
traffic_output/traffic_data.csv
📈 Sample Data Format
timestamp	location	vehicle_count	average_speed
2025-06-10 00:47:00	B2	56	50.09

📌 Notes
Ensure Kafka topic name in both producer and consumer is traffic_topic.

Only one Kafka topic should be used to avoid routing issues.

CSV file is append-only to maintain historical logs.

📜 License
MIT License

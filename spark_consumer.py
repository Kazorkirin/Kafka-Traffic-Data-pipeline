from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import pandas as pd
import os


# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToSingleCSV") \
    .getOrCreate()

# Kafka message schema
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("average_speed", DoubleType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .load()

# Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Custom function to append batch to single CSV
def append_to_single_csv(batch_df, batch_id):
    pdf = batch_df.toPandas()
    pdf.to_csv("traffic_output/final_output.csv", mode='a', index=False, header=not os.path.exists("traffic_output/final_output.csv"))

# Stream with foreachBatch
query = parsed_df.writeStream \
    .foreachBatch(append_to_single_csv) \
    .outputMode("append") \
    .option("checkpointLocation", "traffic_output/_checkpoint") \
    .start()

query.awaitTermination()

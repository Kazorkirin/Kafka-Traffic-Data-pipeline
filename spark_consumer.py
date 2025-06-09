from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("TrafficDataPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the schema of incoming Kafka data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("average_speed", DoubleType())

# 3. Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_topic") \
    .load()

# 4. Parse the JSON messages from Kafka
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Optional: Print to console for debugging
# parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# 6. Write to CSV output
query = parsed_df.writeStream \
    .format("csv") \
    .option("path", "output/csv_with_header/") \
    .option("checkpointLocation", "output/checkpoint_csv/") \
    .option("header", True) \
    .outputMode("append") \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ✅ Create a Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# ✅ Define Kafka parameters
KAFKA_BROKER = "kafka-1:9092"
BIKE_STATUS_TOPIC = "bike_station_status"
BIKE_INFO_TOPIC = "bike_station_information"
WEATHER_TOPIC = "weather_data"

# ✅ Define schema for JSON data
bike_status_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("station_id", StringType()) \
    .add("num_bikes_available", IntegerType()) \
    .add("num_docks_available", IntegerType())

bike_info_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("capacity", IntegerType()) \
    .add("lon", DoubleType()) \
    .add("lat", DoubleType())

weather_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("temperature", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("precipitation", DoubleType()) \
    .add("cloudiness", DoubleType())

# ✅ Read data from Kafka topics
bike_status_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", BIKE_STATUS_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

bike_info_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", BIKE_INFO_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", WEATHER_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ✅ Convert Kafka messages (JSON) into structured DataFrames
bike_status_df = bike_status_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bike_status_schema).alias("data")) \
    .select("data.*")

bike_info_df = bike_info_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bike_info_schema).alias("data")) \
    .select("data.*")

weather_df = weather_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*")

# ✅ Join station information with real-time status
bike_joined_df = bike_status_df.join(bike_info_df, "station_id", "left") \
    .withColumn("utilization_rate", expr("(num_bikes_available / capacity) * 100"))

# ✅ Join bike data with weather data on timestamp
final_df = bike_joined_df.join(weather_df, "timestamp", "left")

# ✅ Write the processed data to console (for debugging)
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# ✅ Wait for the streaming process to complete
query.awaitTermination()

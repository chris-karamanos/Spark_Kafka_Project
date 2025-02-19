
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp, from_unixtime, trim, lower
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, ArrayType

# Create a Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

#  Define Kafka parameters
KAFKA_BROKER = "kafka-1:9092"
WEATHER_TOPIC = "weather_data"
STATION_INFO_TOPIC = "station_information"
STATION_STATUS_TOPIC = "station_status"

#  Define schema for JSON weather data
weather_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("temperature", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("precipitation", DoubleType()) \
    .add("cloudiness", DoubleType())

#  Define schema for JSON bike station information
station_info_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("capacity", IntegerType())

# Define schema for JSON bike station status (Real-time availability)
station_status_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("stations", ArrayType(
        StructType()
        .add("station_id", StringType())
        .add("num_bikes_available", IntegerType())
        .add("num_docks_available", IntegerType())
    ))

#  Read data from Kafka (weather_data topic)
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", WEATHER_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka messages (JSON) into structured DataFrame
weather_df = weather_df.selectExpr("CAST(value AS STRING)") \
    .filter(col("value").isNotNull()) \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*")

# Read data from Kafka (station_information topic)
station_info_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", STATION_INFO_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .filter(col("value").isNotNull()) \
    .select(from_json(col("value"), station_info_schema).alias("data")) \
    .select("data.*") \
    .withColumn("station_id", trim(lower(col("station_id"))))  # Standardize `station_id`


#  Read data from Kafka (station_status topic - real-time availability)
station_status_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", STATION_STATUS_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

#  Convert Kafka messages (JSON) into structured DataFrame for bike station status
station_status_df = station_status_df.selectExpr("CAST(value AS STRING)") \
    .filter(col("value").isNotNull()) \
    .select(from_json(col("value"), station_status_schema).alias("data")) \
    .select("data.timestamp", "data.stations")

#  Explode the nested 'stations' array into separate rows
station_status_df = station_status_df.withColumn("station", explode(col("stations"))) \
    .select(
        col("timestamp"),
        col("station.station_id"),
        col("station.num_bikes_available"),
        col("station.num_docks_available")
    )

#  Convert timestamp to proper format
station_status_df = station_status_df.withColumn(
    "timestamp", from_unixtime(col("timestamp")).cast("timestamp")
)


# Write the processed data to console (for debugging)
query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/weather_checkpoint") \
    .start()

#  Write the processed station info data to console (for debugging)
station_info_query = station_info_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/station_info_checkpoint") \
    .start()

#  Write the processed station status data to console (for debugging)
station_status_query = station_status_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/station_status_checkpoint") \
    .start()

#  Keep the streaming process running
query.awaitTermination()
station_info_query.awaitTermination()
station_status_query.awaitTermination()
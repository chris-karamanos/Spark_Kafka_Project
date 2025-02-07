import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, \
    min as spark_min, max as spark_max, avg as spark_avg, stddev as spark_stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
#SparkSession
spark = SparkSession.builder \
    .appName("BikeSharingAnalytics") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()


#Kafka topics
station_info_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_information") \
    .option("startingOffsets", "earliest") \
    .load()

station_status_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_status") \
    .option("startingOffsets", "earliest") \
    .load()

weather_raw = spark.read\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "earliest") \
    .load()

# ΔΗΜΙΟΥΡΓΟΥΜΕ ΤΑ SCHEMAS

# station_information
info_schema = StructType([
    StructField("station_id", StringType()),
    StructField("name", StringType()),
    StructField("capacity", IntegerType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
])

#station_status
status_schema = StructType([
    StructField("station_id", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("timestamp", LongType())
])

#weather_data
weather_schema = StructType([
    StructField("timestamp", LongType()),
    StructField("temperature", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("cloudiness", DoubleType())
])

# Μετατρέπουμε την τιμή (binary -> string -> json)
station_info_df = station_info_raw.selectExpr("CAST(value AS STRING) as jsonStr") \
    .select(from_json(col("jsonStr"), info_schema).alias("data")) \
    .select("data.*")

station_status_df = station_status_raw.selectExpr("CAST(value AS STRING) as jsonStr") \
    .select(from_json(col("jsonStr"), status_schema).alias("data")) \
    .select("data.*")

weather_df = weather_raw.selectExpr("CAST(value AS STRING) as jsonStr") \
    .select(from_json(col("jsonStr"), weather_schema).alias("data")) \
    .select("data.*")


# Join info/status βάσει station_id ΕΡΩΤΗΜΑ 1
stations_joined = station_info_df.alias("i") \
    .join(station_status_df.alias("s"), col("i.station_id") == col("s.station_id")) \
    .select(
        col("i.station_id").alias("station_id"),
        col("s.timestamp").alias("timestamp"),
        col("i.capacity"),
        col("s.num_bikes_available"),
        col("s.num_docks_available")
    )


# Υπολόγισε utilization ΕΡΩΤΗΜΑ 2
# Βλέπουμε το ποσό των ποδηλάτων που χρησιμοποιούνται
# καινούργιο dataframe με την εξτρα πληροφορια
stations_util_df = stations_joined.withColumn(
    "docking_station_utilisation",
    expr("(capacity - num_bikes_available) / capacity")
)

#ΕΡΩΤΗΜΑ 3
#μερντζαρουμε weather με ποδηλατα με βάση τον χρονο
#άρα όλα είναι σε 1 dataframe
all_joined_df = stations_util_df.alias("st") \
    .join(
        weather_df.alias("w"),
        (col("st.timestamp") == col("w.timestamp")),
        "inner"
    ) \
    .select(
        col("st.timestamp"),
        col("w.temperature"),
        col("w.wind_speed"),
        col("w.precipitation"),
        col("w.cloudiness"),
        col("st.docking_station_utilisation")
    )

# Ερώτημα 4 Δεν είναι έτοιμο πρέπει να δούμε πως θα το κάνουμε ανα ώρα
final_df = all_joined_df.groupBy("timestamp") \
    .agg(
    spark_avg("docking_station_utilisation").alias("average_docking_station_utilisation"),
    spark_min("docking_station_utilisation").alias("min_docking_station_utilisation"),
    spark_max("docking_station_utilisation").alias("max_docking_station_utilisation"),
    spark_stddev("docking_station_utilisation").alias("std_dev_docking_station_utilisation"),

    # Παίρνουμε μια «αντιπροσωπευτική» τιμή καιρού (π.χ. ίδιο timestamp - μπορούμε να πάρουμε το μέσο όρο αν διαφέρει)
    spark_avg("temperature").alias("temperature"),
    spark_avg("wind_speed").alias("wind_speed"),
    spark_avg("precipitation").alias("precipitation"),
    spark_avg("cloudiness").alias("cloudiness")
)
#Ερώτημα 3
corr_usage_temp = all_joined_df.stat.corr("docking_station_utilisation", "temperature")
print("Correlation between usage and temperature:", corr_usage_temp)

#μπορούμε να προσθέσουμε τα plots αν και δεν είμαι σίγουρος ότι χρειάζεται (θα γίναι πολύ αργό)
#import matplotlib.pyplot as plt

# pdf = all_joined_df.select("docking_station_utilisation", "temperature").sample(False, 0.05).toPandas()
#
# plt.scatter(pdf["temperature"], pdf["docking_station_utilisation"])
# plt.xlabel("Temperature (C)")
# plt.ylabel("Docking Station Utilization")
# plt.title("Correlation between Temperature and Bike Usage")
# plt.show()


final_df.write.mode("overwrite").parquet("apotelesma.parquet")

# query = final_df.writeStream \
#     .outputMode("append" or "complete") \
#     .format("console") \
#     .start()
# query.awaitTermination()

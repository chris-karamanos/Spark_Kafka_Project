import spark
from pyspark.sql.functions import col, expr
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

#Διαβάζουμε το αποτέλεσμα του προηγούμενου βήματος
final_df = spark.read.parquet("apotelesma.parquet")

# Δημιουργούμε την στήλη "next_hour_ts" που περιέχει το timestamp της επόμενης ώρας
# Αυτή η στήλη θα χρησιμοποιηθεί για την πρόβλεψη που θέλουμε να κάνουμε (είναι ο χρόνος+1 ώρα)
df_offset = final_df.withColumn("next_hour_ts", col("timestamp") + expr("3600"))

# Κάνουμε self join για να έχουμε τα features και το label στο ίδιο row
model_df = df_offset.alias("curr").join(
    final_df.alias("nxt"),
    (col("curr.next_hour_ts") == col("nxt.timestamp")),
    "inner"
).select(
    col("curr.timestamp").alias("timestamp_current"),
    col("curr.temperature").alias("temp_current"),
    col("curr.wind_speed").alias("wind_current"),
    col("curr.precipitation").alias("prec_current"),
    col("curr.cloudiness").alias("cloud_current"),
    col("curr.average_docking_station_utilisation").alias("usage_current"),

    # To label για την επόμενη ώρα
    col("nxt.average_docking_station_utilisation").alias("usage_next_hour")
)

# 1) Ορισμός feature columns
feature_cols = [
    "temp_current",
    "wind_current",
    "prec_current",
    "cloud_current",
    "usage_current"
]

# 2) VectorAssembler
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# 3) Χρησιμοποιούμε RandomForestRegressor που προτείνεται
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="usage_next_hour",  # δηλώνουμε ως label το usage_next_hour
    numTrees=20
)

# 4) Pipeline
pipeline = Pipeline(stages=[assembler, rf])
train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)

predictions = model.transform(test_df)
predictions.select("features", "usage_next_hour", "prediction").show(10, False)


evaluator = RegressionEvaluator(
    labelCol="usage_next_hour",
    predictionCol="prediction",
    metricName="rmse"  # ή "mae", "r2"
)

rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(
    labelCol="usage_next_hour",
    predictionCol="prediction",
    metricName="r2"
).evaluate(predictions)

print("Test RMSE:", rmse)
print("Test R2:", r2)

# Task 2: Advanced DF Analysis by Phalguna Avalagunta
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg

# Initialize Spark session
spark = SparkSession.builder.appName("PhalgunaAvalagunta_Analysis").getOrCreate()

# Sample DataFrame
data = [
    ("2024-12-10 10:00:00", "192.168.1.1", 200, 1234),
    ("2024-12-10 10:01:00", "192.168.1.2", 404, 2345),
]
schema = ["timestamp", "ip", "status", "size"]
df = spark.createDataFrame(data, schema)

# Convert timestamp column to timestamp type
df = df.withColumn("timestamp", df["timestamp"].cast("timestamp"))

# Rolling hourly traffic analysis
df = df.withColumn("hour", window("timestamp", "1 hour").alias("hour_window"))
df.groupBy("hour").count().show()

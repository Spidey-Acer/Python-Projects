# Task 3: Optimization by Sai Kishore Dodda
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SaiKishoreDodda_Optimization").getOrCreate()

# Sample DataFrame
data = [
    ("2024-12-10 10:00:00", "192.168.1.1", 200, 1234),
    ("2024-12-10 10:01:00", "192.168.1.2", 404, 2345),
]
schema = ["timestamp", "ip", "status", "size"]
df = spark.createDataFrame(data, schema)

# Apply caching
df.cache()
df.show()

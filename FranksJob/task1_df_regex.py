
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("DF Creation").getOrCreate()

# Custom schema definition
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("method", StringType(), True)
])

# Sample data
data = [
    "127.0.0.1 - - [10/Dec/2024:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326"
]

# Apply REGEX
log_regex = r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>.*?)\] "(?P<method>[A-Z]+)'

# Create DataFrame (implementation specific to extracted schema)

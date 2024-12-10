# Task 1: DF Creation with REGEX by Navya Athoti
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import re

# Initialize Spark session
spark = SparkSession.builder.appName("NavyaAthoti_Regex").getOrCreate()

# Sample log data
data = [
    "127.0.0.1 - - [10/Dec/2024:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326",
]

# Define schema and REGEX
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("method", StringType(), True)
])

log_regex = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)'

# Function to parse logs
def parse_log(line):
    match = re.search(log_regex, line)
    if match:
        return match.groups()
    return None

# Create DataFrame
rdd = spark.sparkContext.parallelize(data)
parsed_rdd = rdd.map(parse_log).filter(lambda x: x is not None)
df = spark.createDataFrame(parsed_rdd, schema)
df.show()

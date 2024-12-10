from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import re
import pandas as pd

# Define the custom schema
custom_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("message", StringType(), True),
    StructField("http_status_code", IntegerType(), True)
])

# Define the log parsing function
def parse_log_line(line):
    match = re.search(r'\[(.*?)\]\s+(\d+)\s+(\w+)', line)
    if match:
        timestamp_str, http_status_code, message = match.groups()
        timestamp = pd.to_datetime(timestamp_str)
        http_status_code = int(http_status_code)
        return (timestamp, message, http_status_code)
    else:
        return (None, None, None)

# Create the PySpark DataFrame
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    spark.sparkContext.textFile('./FranksJob/log/web.log')
        .map(parse_log_line),
    schema=custom_schema
)

# Preview the DataFrame
print("DataFrame Schema:")
df.printSchema()

print("DataFrame Head:")
df.show(5)

# Perform DataFrame Analysis
# 1. SQL Query using Window Functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window = Window.partitionBy("http_status_code").orderBy("timestamp")
df_with_metrics = df.withColumn("rolling_count", F.count("message").over(window))

# 2. Complex SQL Aggregation
df_aggregated = df.groupBy("http_status_code") \
    .agg(
        F.count("message").alias("total_messages"),
        F.avg("http_status_code").alias("avg_http_status_code")
    )

# Data Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Visualize rolling count by HTTP status code
plt.figure(figsize=(12, 6))
sns.lineplot(data=df_with_metrics, x="timestamp", y="rolling_count", hue="http_status_code")
plt.title("Rolling Message Count by HTTP Status Code")
plt.xlabel("Timestamp")
plt.ylabel("Rolling Count")
plt.show()

# 2. Visualize aggregated metrics
plt.figure(figsize=(8, 6))
df_aggregated.toPandas().plot(kind="bar", x="http_status_code", y=["total_messages", "avg_http_status_code"])
plt.title("Aggregated Metrics by HTTP Status Code")
plt.xlabel("HTTP Status Code")
plt.ylabel("Metric Value")
plt.show()
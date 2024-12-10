
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("DF Analysis").getOrCreate()

# Load data into DataFrame
df = spark.read.csv("weblogs.csv", header=True, inferSchema=True)

# Example Window Function: Rolling Hourly Traffic
df.createOrReplaceTempView("logs")
query = '''
SELECT ip, COUNT(*) as request_count
FROM logs
GROUP BY ip
ORDER BY request_count DESC
LIMIT 10
'''
spark.sql(query).show()

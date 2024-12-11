import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Initialize Spark session
conf = SparkConf().setAppName('CN7031_Group136_2024')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Load the dataset
data = spark.read.text("web.log")

# Task 1 - Student 1: Extract IP Address, Timestamp, HTTP Method
regex_student1 = r"(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)"
df_student1 = data.select(
    regexp_extract('value', regex_student1, 1).alias('IP Address'),
    regexp_extract('value', regex_student1, 2).alias('Timestamp'),
    regexp_extract('value', regex_student1, 3).alias('HTTP Method')
)

# Show DataFrame for Student 1
print("Student 1 DataFrame:")
df_student1.show()

# Advanced Analysis for Student 1
# 1. Count of requests per IP
print("Count of requests per IP for Student 1:")
df_student1.groupBy('IP Address').count().show()

# 2. Count of requests per HTTP Method
print("Count of requests per HTTP Method for Student 1:")
df_student1.groupBy('HTTP Method').count().show()

# Task 1 - Student 2: Extract HTTP Status Code, Response Size, Timestamp
regex_student2 = r"\".*\" (\d+) (\d+) \[(.*?)\]"
df_student2 = data.select(
    regexp_extract('value', regex_student2, 1).alias('HTTP Status Code'),
    regexp_extract('value', regex_student2, 2).alias('Response Size'),
    regexp_extract('value', regex_student2, 3).alias('Timestamp')
)

# Show DataFrame for Student 2
print("Student 2 DataFrame:")
df_student2.show()

# Advanced Analysis for Student 2
# 1. Count of responses by status code
print("Count of responses by HTTP Status Code for Student 2:")
df_student2.groupBy('HTTP Status Code').count().show()

# 2. Average response size
print("Average Response Size for Student 2:")
df_student2.agg({'Response Size': 'avg'}).show()

# Task 1 - Student 3: Extract URL Path, IP Address, Response Size
regex_student3 = r"\"[A-Z]+ (\/.*?) HTTP.* (\d+\.\d+\.\d+\.\d+) (\d+)"
df_student3 = data.select(
    regexp_extract('value', regex_student3, 1).alias('URL Path'),
    regexp_extract('value', regex_student3, 2).alias('IP Address'),
    regexp_extract('value', regex_student3, 3).alias('Response Size')
)

# Show DataFrame for Student 3
print("Student 3 DataFrame:")
df_student3.show()

# Advanced Analysis for Student 3
# 1. Count of requests per URL Path
print("Count of requests per URL Path for Student 3:")
df_student3.groupBy('URL Path').count().show()

# 2. Total response size per IP Address
print("Total Response Size per IP Address for Student 3:")
df_student3.groupBy('IP Address').agg({'Response Size': 'sum'}).show()

# Task 1 - Student 4: Extract Log Message, HTTP Status Code, Timestamp
regex_student4 = r"\".*\" (\d+) .*? \[(.*?)\] (.*)"
df_student4 = data.select(
    regexp_extract('value', regex_student4, 1).alias('HTTP Status Code'),
    regexp_extract('value', regex_student4, 2).alias('Timestamp'),
    regexp_extract('value', regex_student4, 3).alias('Log Message')
)

# Show DataFrame for Student 4
print("Student 4 DataFrame:")
df_student4.show()

# Advanced Analysis for Student 4
# 1. Count of log messages by status code
print("Count of Log Messages by HTTP Status Code for Student 4:")
df_student4.groupBy('HTTP Status Code').count().show()

# 2. Count of log messages per timestamp
print("Count of Log Messages per Timestamp for Student 4:")
df_student4.groupBy('Timestamp').count().show()

# Stop the Spark context
sc.stop()
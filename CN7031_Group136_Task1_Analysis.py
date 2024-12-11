import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
conf = SparkConf().setAppName('CN7031_Group136_2024')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Load the dataset
data = spark.read.text("web.log")  # Adjust the path as needed

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

# Convert to Pandas for analysis and visualization
df1 = df_student1.toPandas()

# Convert Timestamp to datetime
df1['Timestamp'] = pd.to_datetime(df1['Timestamp'], format='%d/%b/%Y:%H:%M:%S', errors='coerce')

# Advanced Analysis for Student 1
# 1. Count of requests per IP
print("Count of requests per IP for Student 1:")
df_student1.groupBy('IP Address').count().show()

# 2. Count of requests per HTTP Method
print("Count of requests per HTTP Method for Student 1:")
df_student1.groupBy('HTTP Method').count().show()

# Visualization for Student 1: Rolling hourly traffic per IP
df1.set_index('Timestamp', inplace=True)
rolling_traffic = df1.resample('H').count()['IP Address']

plt.figure(figsize=(10, 5))
rolling_traffic.plot()
plt.title('Rolling Hourly Traffic per IP (Student 1)')
plt.xlabel('Time')
plt.ylabel('Traffic Count')
plt.grid()
plt.savefig('student1_traffic.png')
plt.close()

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

# Convert to Pandas for analysis
df2 = df_student2.toPandas()

# Check for empty values in df2
print("Student 2 DataFrame after conversion to Pandas:")
print(df2.head())

# Convert Response Size to numeric, forcing errors to NaN
df2['Response Size'] = pd.to_numeric(df2['Response Size'], errors='coerce')

# Advanced Analysis for Student 2
# 1. Count of responses by status code
print("Count of responses by HTTP Status Code for Student 2:")
df_student2.groupBy('HTTP Status Code').count().show()

# 2. Average response size
print("Average Response Size for Student 2:")
df_student2.agg({'Response Size': 'avg'}).show()

# Visualization for Student 2: Top 10 failed requests by size
top_failed_requests = df2[df2['HTTP Status Code'] != '200'].nlargest(10, 'Response Size')

plt.figure(figsize=(10, 5))
sns.barplot(x='Response Size', y='HTTP Status Code', data=top_failed_requests)
plt.title('Top 10 Failed Requests by Size (Student 2)')
plt.xlabel('Response Size')
plt.ylabel('HTTP Status Code')
plt.savefig('student2_failed_requests.png')
plt.close()

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

# Convert to Pandas for analysis
df3 = df_student3.toPandas()

# Advanced Analysis for Student 3
# 1. Count of requests per URL Path
print("Count of requests per URL Path for Student 3:")
df_student3.groupBy('URL Path').count().show()

# 2. Total response size per IP Address
print("Total Response Size per IP Address for Student 3:")
df_student3.groupBy('IP Address').agg({'Response Size': 'sum'}).show()

# Visualization for Student 3: Unique visitors per hour
unique_visitors = df3.groupby(df3['URL Path']).size().reset_index(name='Counts')

plt.figure(figsize=(10, 5))
sns.heatmap(unique_visitors.pivot("URL Path", "Counts"), cmap="YlGnBu")
plt.title('Hourly Unique Visitors (Student 3)')
plt.xlabel('URL Path')
plt.ylabel('Counts')
plt.savefig('student3_unique_visitors.png')
plt.close()

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

# Convert to Pandas for analysis
df4 = df_student4.toPandas()

# Advanced Analysis for Student 4
# 1. Count of log messages by status code
print("Count of Log Messages by HTTP Status Code for Student 4:")
df_student4.groupBy('HTTP Status Code').count().show()

# 2. Count of log messages per timestamp
print("Count of Log Messages per Timestamp for Student 4:")
df_student4.groupBy('Timestamp').count().show()

# Visualization for Student 4: Response Code Distribution
response_distribution = df4['HTTP Status Code'].value_counts()

plt.figure(figsize=(10, 5))
plt.pie(response_distribution, labels=response_distribution.index, autopct='%1.1f%%')
plt.title('Response Code Distribution (Student 4)')
plt.savefig('student4_response_distribution.png')
plt.close()

# Stop the Spark context
sc.stop()
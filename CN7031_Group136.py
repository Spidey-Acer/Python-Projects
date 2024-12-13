# Cell 1 [Markdown]:
'''
# Big Data Analytics [CN7031] CRWK 2024-25
# Group ID: CN7031_Group136_2024

1. Student 1: Navya Athoti u2793047@uel.ac.uk
2. Student 2: Phalguna Avalagunta u2811669@uel.ac.uk
3. Student 3: Nikhil Sai Damera u2810262@uel.ac.uk
4. Student 4: Sai Kishore Dodda u2773584@uel.ac.uk

---
'''

# Cell 2 [Markdown]:
'''
# Initiate and Configure Spark
---
'''

# Cell 3 [Code]:
!pip3 install pyspark

# Cell 4 [Code]:
# Import required libraries
import os
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
from datetime import datetime

# Initialize Spark
def initialize_spark():
    conf = SparkConf().setAppName('CN7031_Group136_2024') \
        .set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "4g") \
        .set("spark.sql.shuffle.partitions", "100")
    
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    return sc, spark

sc, spark = initialize_spark()

# Cell 5 [Markdown]:
'''
# Load Unstructured Data
---
'''

# Cell 6 [Code]:
def load_data(spark, path="web.log"):
    try:
        data = spark.read.text(path)
        print(f"Successfully loaded {data.count()} log entries")
        return data
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

data = load_data(spark)

# Cell 7 [Markdown]:
'''
# Task 1: Data Processing using PySpark DF [40 marks]
---
'''

# Cell 8 [Markdown]:
'''
# Student 1 (Navya Athoti u2793047)
- DF Creation with REGEX (10 marks)
- Two advanced DF Analysis (20 marks)
- Utilize data visualization (10 marks)
'''

# Cell 9 [Code]:
print("\nStudent 1 Analysis - Web Traffic Pattern Analysis")
print("=" * 50)

# DF Creation with REGEX (10 marks)
regex_student1 = r"(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)"
df_student1 = data.select(
    regexp_extract('value', regex_student1, 1).alias('IP_Address'),
    regexp_extract('value', regex_student1, 2).alias('Timestamp'),
    regexp_extract('value', regex_student1, 3).alias('HTTP_Method')
).cache()  # Cache for performance

# Validate extracted data
print("\nData Quality Check:")
print(f"Total Records: {df_student1.count()}")
print(f"Null Values: {df_student1.filter(col('IP_Address') == '').count()}")

# Advanced Analysis 1: Rolling Window Analysis (10 marks)
windowed_traffic = df_student1 \
    .withColumn('timestamp', unix_timestamp('Timestamp', 'dd/MMM/yyyy:HH:mm:ss').cast('timestamp')) \
    .withWatermark('timestamp', '1 hour') \
    .groupBy(
        window('timestamp', '1 hour'),
        'IP_Address'
    ).agg(
        count('*').alias('request_count')
    ).orderBy('window.start')

print("\nHourly Traffic Pattern Sample:")
windowed_traffic.show(5)

# Advanced Analysis 2: HTTP Method Distribution (10 marks)
method_distribution = df_student1 \
    .groupBy('HTTP_Method') \
    .agg(
        count('*').alias('total_requests'),
        countDistinct('IP_Address').alias('unique_ips')
    ).orderBy(col('total_requests').desc())

print("\nHTTP Method Distribution:")
method_distribution.show()

# Visualization (10 marks)
# For Student 1's visualization
def create_traffic_visualization(df):
    # Convert to pandas and prepare data
    df_pandas = df.toPandas()
    
    # Convert window struct to datetime
    df_pandas['time'] = df_pandas['window'].apply(lambda x: x.start)
    
    plt.figure(figsize=(12, 6))
    
    # Create time series plot with proper column names
    sns.lineplot(data=df_pandas, 
                x='time', 
                y='request_count',
                marker='o')
    
    plt.title('Hourly Web Traffic Pattern')
    plt.xlabel('Time')
    plt.ylabel('Request Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save visualization
    plt.savefig('student1_analysis.png')
    plt.close()

# Modify the windowed traffic query
windowed_traffic = df_student1 \
    .withColumn('timestamp', unix_timestamp('Timestamp', 'dd/MMM/yyyy:HH:mm:ss').cast('timestamp')) \
    .groupBy(
        window('timestamp', '1 hour')
    ).agg(
        count('*').alias('request_count')
    ).orderBy('window')

# Create visualization
create_traffic_visualization(windowed_traffic)

# Student 2 (Phalguna Avalagunta u2811669)
print("\nStudent 2 Analysis - Response Analysis")
print("=" * 50)

# DF Creation with REGEX (10 marks)
regex_student2 = r"\".*\" (\d+) (\d+) \[(.*?)\]"
df_student2 = data.select(
    regexp_extract('value', regex_student2, 1).alias('Status_Code'),
    regexp_extract('value', regex_student2, 2).alias('Response_Size'),
    regexp_extract('value', regex_student2, 3).alias('Timestamp')
).cache()

# Student 3 (Nikhil Sai Damera u2810262)
print("\nStudent 3 Analysis - URL Pattern Analysis")
print("=" * 50)
# DF Creation with REGEX (10 marks)
regex_student3 = r"\"[A-Z]+ (\/.*?) HTTP.* (\d+\.\d+\.\d+\.\d+) (\d+)"
df_student3 = data.select(
    regexp_extract('value', regex_student3, 1).alias('URL_Path'),
    regexp_extract('value', regex_student3, 2).alias('IP_Address'),
    regexp_extract('value', regex_student3, 3).alias('Response_Size')
).cache()
# Verify DataFrame creation
print("\nVerifying Student 3 DataFrame structure:")
df_student3.printSchema()
print("\nSample data:")
df_student3.show(5)

# Student 4 (Sai Kishore Dodda u2773584)
print("\nStudent 4 Analysis - Log Message Analysis")
print("=" * 50)
# DF Creation with REGEX (10 marks)
regex_student4 = r"\".*\" (\d+) .*? \[(.*?)\] (.*)"
df_student4 = data.select(
    regexp_extract('value', regex_student4, 1).alias('HTTP_Status_Code'),
    regexp_extract('value', regex_student4, 2).alias('Timestamp'),
    regexp_extract('value', regex_student4, 3).alias('Log_Message')
).cache()
# Verify DataFrame creation
print("\nVerifying Student 4 DataFrame structure:")
df_student4.printSchema()
print("\nSample data:")
df_student4.show(5)

# Advanced Analysis 1: Session Analysis (10 marks)
session_analysis = df_student2 \
    .withColumn('timestamp', unix_timestamp('Timestamp', 'dd/MMM/yyyy:HH:mm:ss').cast('long')) \
    .withColumn(
        'session_requests',
        count('*').over(
            Window.orderBy('timestamp')
            .rangeBetween(-1800, 0)  # 30-minute window in seconds
        )
    ) \
    .withColumn(
        'avg_response_size',
        avg('Response_Size').over(
            Window.orderBy('timestamp')
            .rangeBetween(-1800, 0)
        )
    )

print("\nSession Analysis Sample:")
session_analysis.select('timestamp', 'session_requests', 'avg_response_size').show(5)

# Advanced Analysis 2: Response Size Analysis (10 marks)
response_analysis = df_student2 \
    .groupBy('Status_Code') \
    .agg(
        count('*').alias('request_count'),
        avg('Response_Size').alias('avg_response_size'),
        spark_max('Response_Size').alias('max_response_size')  # Use spark_max instead of max
    ).orderBy('Status_Code')

print("\nResponse Size Analysis:")
response_analysis.show()

# Visualization (10 marks)
def create_response_visualization(df):
    # Convert to pandas
    df_pandas = df.toPandas()
    
    # Convert Status_Code to string for better plotting
    df_pandas['Status_Code'] = df_pandas['Status_Code'].astype(str)
    
    plt.figure(figsize=(12, 6))
    
    # Create bar plot
    sns.barplot(
        data=df_pandas,
        x='Status_Code',
        y='avg_response_size',
        palette='viridis'
    )
    
    plt.title('Average Response Size by Status Code')
    plt.xlabel('HTTP Status Code')
    plt.ylabel('Average Response Size (bytes)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save visualization
    plt.savefig('student2_analysis.png')
    plt.close()

create_response_visualization(response_analysis)


# Cell 10 [Markdown]:
'''
# Task 2: Data Processing using PySpark RDD [40 marks]
---
'''

[Continue with RDD processing sections...]

# Cell 11 [Markdown]:
'''
# Task 3: Optimization and LSEPI Considerations [10 marks]
---
'''

[Continue with optimization sections...]

# Final Cell [Code]:
# Convert notebook to HTML
!jupyter nbconvert --to html CN7031_Group136_2024.ipynb
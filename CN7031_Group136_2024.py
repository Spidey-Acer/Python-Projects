# %%
# Cell 1 [Markdown]:
"""
# Big Data Analytics [CN7031] CRWK 2024-25
# Group ID: CN7031_Group126_2024

1. Student 1: Navya Athoti u2793047@uel.ac.uk
2. Student 2: Phalguna Avalagunta u2811669@uel.ac.uk
3. Student 3: Nikhil Sai Damera u2810262@uel.ac.uk
4. Student 4: Sai Kishore Dodda u2773584@uel.ac.uk

---
"""

# %%
# Cell 2 [Markdown]:
'''
# Initiate and Configure Spark
---
'''

# %%


# Cell 3 [Code]:
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])

# Cell 4 [Code]:
# Import required libraries
import os
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
import sys

# environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import time
from datetime import datetime


# Initialize Spark session
def initialize_spark():
    spark = (SparkSession.builder
            .appName('CN7031_Group126_2024')
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "100")
            .master("local[*]")
            .getOrCreate())
    return spark

spark = initialize_spark()

# %%
# Cell 5 [Markdown]:
'''
# Load Unstructured Data
---
'''

# %%


# Cell 6 [Code]:
def load_data(spark, path="web.log"):
    try:
        # Check if file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
            
        data = spark.read.text(path)
        print(f"Successfully loaded {data.count()} log entries")
        return data
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

# Test the data loading
try:
    data = load_data(spark)
except Exception as e:
    print(f"Failed to load data: {str(e)}")


# %%
# Cell 7 [Markdown]:
'''
# Task 1: Data Processing using PySpark DF [40 marks]
---
'''

# %%
# Cell 8 [Markdown]:
'''
# Student 1 (Navya Athoti u2793047)
- DF Creation with REGEX (10 marks)
- Two advanced DF Analysis (20 marks)
- Utilize data visualization (10 marks)
'''

# %%


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
    
    # Ensure request_count is numeric
    df_pandas['request_count'] = pd.to_numeric(df_pandas['request_count'])
    
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



# %%


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
    
    # Ensure request_count is numeric
    df_pandas['request_count'] = pd.to_numeric(df_pandas['request_count'])
    
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
        spark_max('Response_Size').alias('max_response_size')  # Use spark_max instead of max
    ).orderBy('Status_Code')

print("\nResponse Size Analysis:")
response_analysis.show()

# Visualization (10 marks)
# Visualization code for Student 1
def create_traffic_visualization(df):
    # Convert to pandas and prepare data
    df_pandas = df.toPandas()
    
    # Convert window struct to datetime more safely
    df_pandas['time'] = df_pandas['window'].apply(lambda x: x['start'] if isinstance(x, dict) else x.start)
    
    # Handle potential infinite values
    df_pandas['request_count'] = pd.to_numeric(df_pandas['request_count'], errors='coerce')
    df_pandas = df_pandas.replace([np.inf, -np.inf], np.nan)
    
    plt.figure(figsize=(12, 6))
    
    # Use plt instead of seaborn to avoid deprecation warning
    plt.plot(df_pandas['time'], 
            df_pandas['request_count'],
            marker='o',
            linestyle='-',
            linewidth=2,
            markersize=6)
    
    plt.title('Hourly Web Traffic Pattern', fontsize=14, pad=20)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Request Count', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save visualization with higher DPI for better quality
    plt.savefig('student1_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

# Make sure to add these imports at the top of your notebook
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Modify the windowed traffic query to ensure clean data
windowed_traffic = df_student1 \
    .withColumn('timestamp', unix_timestamp('Timestamp', 'dd/MMM/yyyy:HH:mm:ss').cast('timestamp')) \
    .groupBy(
        window('timestamp', '1 hour')
    ).agg(
        count('*').alias('request_count')
    ).orderBy('window') \
    .na.fill(0)  # Fill null values with 0

# Create visualization
try:
    create_traffic_visualization(windowed_traffic)
    print("Visualization created successfully!")
except Exception as e:
    print(f"Error creating visualization: {str(e)}")

# %%
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



# %%
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



# %%
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
        spark_max('Response_Size').alias('max_response_size')  # Use spark_max instead of max
    ).orderBy('Status_Code')

print("\nResponse Size Analysis:")
response_analysis.show()



# %%
# Visualization (10 marks)
# Visualization code for Student 1
def create_traffic_visualization(df):
    # Convert to pandas and prepare data
    df_pandas = df.toPandas()
    
    # Convert window struct to datetime more safely
    df_pandas['time'] = df_pandas['window'].apply(lambda x: x['start'] if isinstance(x, dict) else x.start)
    
    # Handle potential infinite values
    df_pandas['request_count'] = pd.to_numeric(df_pandas['request_count'], errors='coerce')
    df_pandas = df_pandas.replace([np.inf, -np.inf], np.nan)
    
    plt.figure(figsize=(12, 6))
    
    # Use plt instead of seaborn to avoid deprecation warning
    plt.plot(df_pandas['time'], 
            df_pandas['request_count'],
            marker='o',
            linestyle='-',
            linewidth=2,
            markersize=6)
    
    plt.title('Hourly Web Traffic Pattern', fontsize=14, pad=20)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Request Count', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save visualization with higher DPI for better quality
    plt.savefig('student1_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

# Make sure to add these imports at the top of your notebook
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Modify the windowed traffic query to ensure clean data
windowed_traffic = df_student1 \
    .withColumn('timestamp', unix_timestamp('Timestamp', 'dd/MMM/yyyy:HH:mm:ss').cast('timestamp')) \
    .groupBy(
        window('timestamp', '1 hour')
    ).agg(
        count('*').alias('request_count')
    ).orderBy('window') \
    .na.fill(0)  # Fill null values with 0

# Create visualization
try:
    create_traffic_visualization(windowed_traffic)
    print("Visualization created successfully!")
except Exception as e:
    print(f"Error creating visualization: {str(e)}")

# %%
# Cell 10 [Markdown]:
'''
# Task 2: Data Processing using PySpark RDD [40 marks]
---
'''

# %%


# Task 2: Data Processing using PySpark RDD [40 marks]

# Student 1 (Navya Athoti u2793047)
print("\nStudent 1 RDD Analysis - Traffic Pattern Mining")
print("=" * 50)

# Basic RDD Analysis: Parse and Extract (10 marks)
def parse_log_entry(line):
    import re
    try:
        pattern = r'(\d+\.\d+\.\d+\.\d+).*\[(.*?)\].*\"([A-Z]+)'
        match = re.search(pattern, line)
        if match:
            return {
                'ip': match.group(1),
                'timestamp': match.group(2),
                'method': match.group(3)
            }
    except Exception as e:
        print(f"Parsing error: {str(e)}")
    return None

base_rdd = data.rdd.map(lambda x: x['value']) \
                   .map(parse_log_entry) \
                   .filter(lambda x: x is not None)

# Advanced Analysis 1: Time-based Traffic Analysis (15 marks)
hourly_traffic = base_rdd \
    .map(lambda x: (x['timestamp'][:13], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()

print("\nHourly Traffic Sample:")
for hour, count in hourly_traffic.take(5):
    print(f"{hour}: {count} requests")

# Advanced Analysis 2: IP-based Pattern Analysis (15 marks)
ip_patterns = base_rdd \
    .map(lambda x: (x['ip'], x['method'])) \
    .groupByKey() \
    .mapValues(lambda methods: {
        'total_requests': len(list(methods)),
        'method_distribution': dict(pd.Series(list(methods)).value_counts())
    })

print("\nIP Pattern Analysis Sample:")
for ip, stats in ip_patterns.take(3):
    print(f"\nIP: {ip}")
    print(f"Total Requests: {stats['total_requests']}")
    print("Method Distribution:", stats['method_distribution'])


# %%
# Cell 11 [Markdown]:
'''
# Task 3: Optimization and LSEPI Considerations [10 marks]
---
'''

# %%


# Task 3: Optimization and LSEPI Considerations [10 marks]

# Student 1 (Navya Athoti u2793047)
print("\nStudent 1 Optimization Analysis")
print("=" * 50)

# Method 1: Partition Strategies (5 marks)
def evaluate_partition_strategy():
    print("\nPartitioning Strategy Evaluation")
    
    # Baseline - Default partitioning
    start_time = time.time()
    df_student1.groupBy('IP_Address').count().count()
    baseline_time = time.time() - start_time
    print(f"Baseline execution time: {baseline_time:.2f} seconds")
    
    # Custom partitioning
    start_time = time.time()
    df_student1.repartition(8, 'IP_Address').groupBy('IP_Address').count().count()
    optimized_time = time.time() - start_time
    print(f"Optimized execution time: {optimized_time:.2f} seconds")
    print(f"Performance improvement: {((baseline_time - optimized_time) / baseline_time) * 100:.2f}%")

evaluate_partition_strategy()

# Method 2: Caching Strategy (5 marks)
def evaluate_caching_strategy():
    print("\nCaching Strategy Evaluation")
    
    # Without caching
    df_uncached = df_student1.unpersist()
    start_time = time.time()
    df_uncached.groupBy('HTTP_Method').count().count()
    df_uncached.groupBy('IP_Address').count().count()
    uncached_time = time.time() - start_time
    print(f"Uncached execution time: {uncached_time:.2f} seconds")
    
    # With caching
    df_cached = df_student1.cache()
    df_cached.count()  # Materialize cache
    start_time = time.time()
    df_cached.groupBy('HTTP_Method').count().count()
    df_cached.groupBy('IP_Address').count().count()
    cached_time = time.time() - start_time
    print(f"Cached execution time: {cached_time:.2f} seconds")
    print(f"Caching improvement: {((uncached_time - cached_time) / uncached_time) * 100:.2f}%")

evaluate_caching_strategy()



# %%
# Student 2 (Phalguna Avalagunta u2811669)
print("\nStudent 2 Optimization Analysis")
print("=" * 50)

# Method 1: Caching Strategy
def evaluate_caching_strategy_student2():
    print("\nCaching Strategy Evaluation")
    
    # Without caching
    df_uncached = df_student2.unpersist()
    start_time = time.time()
    df_uncached.groupBy('Status_Code').count().count()
    df_uncached.groupBy('Response_Size').count().count()
    uncached_time = time.time() - start_time
    print(f"Uncached execution time: {uncached_time:.2f} seconds")
    
    # With caching
    df_cached = df_student2.cache()
    df_cached.count()  # Materialize cache
    start_time = time.time()
    df_cached.groupBy('Status_Code').count().count()
    df_cached.groupBy('Response_Size').count().count()
    cached_time = time.time() - start_time
    print(f"Cached execution time: {cached_time:.2f} seconds")
    print(f"Caching improvement: {((uncached_time - cached_time) / uncached_time) * 100:.2f}%")

evaluate_caching_strategy_student2()

def evaluate_bucketing_strategy_student2():
    print("\nBucketing Strategy Evaluation")
    
    try:
        # Create DataFrame with proper schema
        df_for_bucket = df_student2.select(
            col("Status_Code").cast("string"),
            col("Response_Size").cast("long"),
            col("Timestamp").cast("string")
        )
        
        # Create temporary view
        df_for_bucket.createOrReplaceTempView("logs")
        
        # Measure query performance without bucketing
        start_time = time.time()
        spark.sql("SELECT Status_Code, COUNT(*) FROM logs GROUP BY Status_Code").show()
        unbucketed_time = time.time() - start_time
        print(f"Query time without bucketing: {unbucketed_time:.2f} seconds")
        
        # Create bucketed DataFrame directly
        bucketed_df = df_for_bucket.repartition(4, "Status_Code")
        bucketed_df.createOrReplaceTempView("bucketed_logs")
        
        # Measure query performance with bucketing
        start_time = time.time()
        spark.sql("SELECT Status_Code, COUNT(*) FROM bucketed_logs GROUP BY Status_Code").show()
        bucketed_time = time.time() - start_time
        print(f"Query time with bucketing: {bucketed_time:.2f} seconds")
        print(f"Performance improvement: {((unbucketed_time - bucketed_time) / unbucketed_time) * 100:.2f}%")
        
    except Exception as e:
        print(f"Error in bucketing strategy: {str(e)}")



# %%
# Student 3 (Nikhil Sai Damera u2810262)
print("\nStudent 3 Optimization Analysis")
print("=" * 50)

# Method 1: Partition Strategies
def evaluate_partition_strategy_student3():
    print("\nPartitioning Strategy Evaluation")
    
    # Baseline
    start_time = time.time()
    df_student3.groupBy('URL_Path').count().count()
    baseline_time = time.time() - start_time
    print(f"Baseline execution time: {baseline_time:.2f} seconds")
    
    # Custom partitioning
    start_time = time.time()
    df_student3.repartition(10, 'URL_Path').groupBy('URL_Path').count().count()
    optimized_time = time.time() - start_time
    print(f"Optimized execution time: {optimized_time:.2f} seconds")
    print(f"Performance improvement: {((baseline_time - optimized_time) / baseline_time) * 100:.2f}%")

evaluate_partition_strategy_student3()

# Method 2: Bucketing & Indexing
def evaluate_bucketing_strategy_student3():
    print("\nBucketing Strategy Evaluation")
    
    try:
        # Create DataFrame with proper schema
        df_for_bucket = df_student3.select(
            col("URL_Path").cast("string"),
            col("IP_Address").cast("string"),
            col("Response_Size").cast("long")
        )
        
        # Create temporary view
        df_for_bucket.createOrReplaceTempView("url_logs")
        
        # Measure query performance without bucketing
        start_time = time.time()
        spark.sql("SELECT URL_Path, COUNT(*) FROM url_logs GROUP BY URL_Path").show()
        unbucketed_time = time.time() - start_time
        print(f"Query time without bucketing: {unbucketed_time:.2f} seconds")
        
        # Create bucketed DataFrame directly
        bucketed_df = df_for_bucket.repartition(4, "URL_Path")
        bucketed_df.createOrReplaceTempView("bucketed_url_logs")
        
        # Measure query performance with bucketing
        start_time = time.time()
        spark.sql("SELECT URL_Path, COUNT(*) FROM bucketed_url_logs GROUP BY URL_Path").show()
        bucketed_time = time.time() - start_time
        print(f"Query time with bucketing: {bucketed_time:.2f} seconds")
        print(f"Performance improvement: {((unbucketed_time - bucketed_time) / unbucketed_time) * 100:.2f}%")
        
    except Exception as e:
        print(f"Error in bucketing strategy: {str(e)}")



# %%
# Student 4 (Sai Kishore Dodda u2773584)
print("\nStudent 4 Optimization Analysis")
print("=" * 50)

# Method 1: Caching Strategy
def evaluate_caching_strategy_student4():
    print("\nCaching Strategy Evaluation")
    
    # Without caching
    df_uncached = df_student4.unpersist()
    start_time = time.time()
    df_uncached.groupBy('HTTP_Status_Code').count().count()
    uncached_time = time.time() - start_time
    print(f"Uncached execution time: {uncached_time:.2f} seconds")
    
    # With caching
    df_cached = df_student4.cache()
    df_cached.count()  # Materialize cache
    start_time = time.time()
    df_cached.groupBy('HTTP_Status_Code').count().count()
    cached_time = time.time() - start_time
    print(f"Cached execution time: {cached_time:.2f} seconds")
    print(f"Caching improvement: {((uncached_time - cached_time) / uncached_time) * 100:.2f}%")

evaluate_caching_strategy_student4()

# Method 2: Partition Strategies
def evaluate_partition_strategy_student4():
    print("\nPartitioning Strategy Evaluation")
    
    # Baseline
    start_time = time.time()
    df_student4.groupBy('HTTP_Status_Code').count().count()
    baseline_time = time.time() - start_time
    print(f"Baseline execution time: {baseline_time:.2f} seconds")
    
    # Custom partitioning
    start_time = time.time()
    df_student4.repartition(8, 'HTTP_Status_Code').groupBy('HTTP_Status_Code').count().count()
    optimized_time = time.time() - start_time
    print(f"Optimized execution time: {optimized_time:.2f} seconds")
    print(f"Performance improvement: {((baseline_time - optimized_time) / baseline_time) * 100:.2f}%")

evaluate_partition_strategy_student4()

# Clean up resources
def cleanup():
    try:
        # Unpersist cached DataFrames
        df_student1.unpersist()
        df_student2.unpersist()
        df_student3.unpersist()
        df_student4.unpersist()
        
        # Stop Spark session
        spark.stop()
        print("\nSpark session successfully closed")
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")

# Final Cell [Code]:
# Convert notebook to HTML
import os
os.system('jupyter nbconvert --to html CN7031_Group126_2024.ipynb')



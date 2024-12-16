# %% [markdown]
# # Big Data Analytics [CN7031] CRWK 2024-25
# 
# ## Group ID: CN7031_Group126_2024
# 
# ### Group Members:
# 
# 1. **Navya Athoti**  
#     Email: u2793047@uel.ac.uk
# 2. **Phalguna Avalagunta**  
#     Email: u2811669@uel.ac.uk
# 3. **Nikhil Sai Damera**  
#     Email: u2810262@uel.ac.uk
# 4. **Sai Kishore Dodda**  
#     Email: u2773584@uel.ac.uk
# 
# ---
# 
# ## Initiate and Configure Spark and Helper Functions
# 
# In this section, we will initiate and configure Apache Spark, a powerful open-source processing engine for big data. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.
# 
# We will also define some helper functions that will be used throughout the notebook.
# 
# ---
# 
# 
# 

# %%
# System and Core Python Imports
import os
import sys
import time
import re
import builtins
from datetime import datetime, timedelta

# Data Analysis and Visualization
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Notebook Processing
import nbformat
from nbconvert import HTMLExporter

# Configure PySpark Environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Findspark
import findspark
findspark.init()

# PySpark Imports
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    to_timestamp,
    max as spark_max,
    
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    
)

def create_sample_data():
    """
    Create sample log data with two different datasets
    
    Returns:
    --------
    tuple
        Two pandas DataFrames containing sample log data
    """
    base_time = datetime.now()
    
    # First dataset
    data1 = [{
        'timestamp': base_time + timedelta(hours=i % 24),
        'ip_address': f"192.168.1.{np.random.randint(1, 255)}",
        'status_code': np.random.choice([200, 404, 500], p=[0.8, 0.15, 0.05]),
        'response_size': np.random.randint(100, 10000)
    } for i in range(1000)]
    
    # Second dataset
    data2 = [{
        'timestamp': base_time + timedelta(hours=i % 24),
        'ip_address': f"192.168.2.{np.random.randint(1, 255)}",
        'status_code': np.random.choice([200, 301, 404, 500], p=[0.7, 0.1, 0.15, 0.05]),
        'response_size': np.random.randint(100, 10000)
    } for i in range(1000)]
    
    return pd.DataFrame(data1), pd.DataFrame(data2)

def initialize_spark(app_name="Advanced_Log_Analysis", memory="4g", partitions=100):
    """
    Initialize a Spark session with configurable parameters and handle existing sessions
    
    Parameters:
    -----------
    app_name : str
        Name of the Spark application
    memory : str
        Amount of memory to allocate for driver and executor
    partitions : int
        Number of shuffle partitions
    
    Returns:
    --------
    SparkSession
        Configured Spark session
    """
    # Stop any existing Spark sessions
    SparkSession.builder.getOrCreate().stop()
    
    # Create new session with configurations
    spark = (SparkSession.builder
            .appName(app_name)
            .config("spark.driver.memory", memory)
            .config("spark.executor.memory", memory)
            .config("spark.sql.shuffle.partitions", str(partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
            .master("local[*]")
            .getOrCreate())
    
    # Print debugging information
    print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
    print(f"Spark Application ID: {spark.sparkContext.applicationId}")
    
    return spark

# Initialize Spark Session
try:
    # Stop any existing sessions first
    if SparkSession._instantiatedSession is not None:
        print("Stopping existing Spark session...")
        SparkSession._instantiatedSession.stop()
    
    print("Initializing new Spark session...")
    spark = initialize_spark()
    print("Spark session successfully initialized!")
except Exception as e:
    print(f"Error initializing Spark: {str(e)}")
    raise

# Create sample datasets
df1, df2 = create_sample_data()
print("Sample datasets created successfully!")

# %% [markdown]
# # Load Unstructured Data
# 
# In this section, we will load log data from a text file into a Spark DataFrame with error handling. This process involves reading the log file, handling potential errors, and converting the data into a structured format for further analysis.
# 
# ### Function: `load_data`
# 
# 

# %%
def load_data(spark, path="web.log"):
    
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
            
        data = spark.read.text(path)
        print(f"Successfully loaded {data.count()} log entries")
        return data
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

# Load the log data
try:
    data = load_data(spark)
except Exception as e:
    print(f"Failed to load data: {str(e)}")

# %% [markdown]
# # Task 1: Data Processing using PySpark DataFrame [40 marks]
# 
# ---
# 
# ## DataFrame Creation with REGEX (10 marks)
# 
# Each member will define a custom schema using REGEX to extract specific metrics from the dataset.
# 
# ### Student Metrics to Extract
# 
# - **Student 1: IP Address, Timestamp, HTTP Method**
# 
#   - **REGEX Example:** `(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)`
# 
# - **Student 2: HTTP Status Code, Response Size, Timestamp**
# 
#   - **REGEX Example:** `\".*\" (\d+) (\d+) \[(.*?)\]`
# 
# - **Student 3: URL Path, IP Address, Response Size**
# 
#   - **REGEX Example:** `\"[A-Z]+ (\/.*?) HTTP.* (\d+\.\d+\.\d+\.\d+) (\d+)`
# 
# - **Student 4: Log Message, HTTP Status Code, Timestamp**
#   - **REGEX Example:** `\".*\" (\d+) .* \[(.*?)\] (.*)`
# 

# %% [markdown]
# ## Navya Athoti - IP Address, Timestamp, and HTTP Method Analysis
# 

# %%

def create_student1_df():
    student1_df = data.select(
        regexp_extract(col("value"), r"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip_address"),
        to_timestamp(
            regexp_extract(col("value"), r"\[(.*?)\]", 1),
            "dd/MMM/yyyy:HH:mm:ss"
        ).alias("timestamp"),
        regexp_extract(col("value"), r'"(\w+)', 1).alias("http_method"),
        regexp_extract(col("value"), r'"[A-Z]+ (.*?) HTTP', 1).alias("url_path")  # Added this line
    )
    student1_df.createOrReplaceTempView("student1_logs")
    return student1_df

student1_df = create_student1_df()

print("Student 1 DataFrame Schema:")
student1_df.printSchema()
print("\nSample Data:")
student1_df.show(5, truncate=False)

# %% [markdown]
# ## Phalguna Avalagunta - HTTP Status Code, Response Size, and Timestamp Analysis
# 

# %%
# Extract HTTP Status Code, Response Size, and Timestamp
def create_student2_df():
    student2_df = data.select(
        regexp_extract(col("value"), r'" (\d{3})', 1).alias("status_code"),
        regexp_extract(col("value"), r'" \d{3} (\d+)', 1).cast(IntegerType()).alias("response_size"),
        to_timestamp(
            regexp_extract(col("value"), r"\[(.*?)\]", 1),
            "dd/MMM/yyyy:HH:mm:ss"
        ).alias("timestamp")
    )
    
    
    student2_df.createOrReplaceTempView("student2_logs")
    return student2_df

# Validate Student 2 DataFrame
student2_df = create_student2_df()
print("Student 2 (Phalguna Avalagunta) DataFrame Schema:")
student2_df.printSchema()
print("\nSample Data:")
student2_df.show(5, truncate=False)

# %% [markdown]
# ## Nikhil Sai Damera - URL Path, IP Address, and Response Size Analysis
# 

# %%
# Nikhil Sai Damera - Modified to include timestamp
def create_student3_df():
    student3_df = data.select(
        regexp_extract(col("value"), r'"[A-Z]+ (.*?) HTTP', 1).alias("url_path"),
        regexp_extract(col("value"), r"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip_address"),
        regexp_extract(col("value"), r'" \d{3} (\d+)', 1).cast(IntegerType()).alias("response_size"),
        regexp_extract(col("value"), r'" (\d{3})', 1).alias("status_code"),
        # Add timestamp column
        to_timestamp(
            regexp_extract(col("value"), r"\[(.*?)\]", 1),
            "dd/MMM/yyyy:HH:mm:ss"
        ).alias("timestamp")
    )
    student3_df.createOrReplaceTempView("student3_logs")
    return student3_df


student3_df = create_student3_df()

print("Student 3 DataFrame Schema:")
student3_df.printSchema()


def nikhil_window_analysis():
    query = """
    SELECT 
        date_trunc('hour', timestamp) as hour,
        COUNT(DISTINCT ip_address) as unique_visitors,
        COUNT(DISTINCT ip_address) - LAG(COUNT(DISTINCT ip_address)) 
            OVER (ORDER BY date_trunc('hour', timestamp)) as visitor_change
    FROM student3_logs
    GROUP BY date_trunc('hour', timestamp)
    ORDER BY hour
    """
    return spark.sql(query)

print("\nNikhil's Window Function Analysis - Hourly Unique Visitors:")
nikhil_window_analysis().show(5)

# %% [markdown]
# ## Sai Kishore Dodda - Log Message, HTTP Status Code, and Timestamp Analysis
# 

# %%
# Sai Kishore Dodda - Modified to include ip_address
def create_student4_df():
    student4_df = data.select(
        regexp_extract(col("value"), r'"(.*?)"', 1).alias("log_message"),
        regexp_extract(col("value"), r'" (\d{3})', 1).alias("status_code"),
        to_timestamp(
            regexp_extract(col("value"), r"\[(.*?)\]", 1),
            "dd/MMM/yyyy:HH:mm:ss"
        ).alias("timestamp"),
        regexp_extract(col("value"), r'" \d{3} (\d+)', 1).cast(IntegerType()).alias("response_size"),
        regexp_extract(col("value"), r"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip_address")  # Added this line
    )
    student4_df.createOrReplaceTempView("student4_logs")
    return student4_df


student4_df = create_student4_df()


print("Student 4 DataFrame Schema:")
student4_df.printSchema()


def sai_aggregation_analysis():
    query = """
    SELECT 
        date_trunc('day', timestamp) as day,
        COUNT(DISTINCT ip_address) as unique_visitors,
        COUNT(*) as total_requests
    FROM student4_logs
    GROUP BY date_trunc('day', timestamp)
    ORDER BY day
    """
    return spark.sql(query)

# Execute and show results
print("\nSai's Aggregation Analysis - Daily Unique Visitors:")
sai_aggregation_analysis().show(5)

# %% [markdown]
# ## Validation Function (For debugging)
# 

# %%
def validate_dataframe(df, student_name):
    print(f"\n{student_name} DataFrame Validation:")
    # Count non-null values for each column
    df.select([
        sum(col(c).isNotNull().cast("int")).alias(f"{c}_count")
        for c in df.columns
    ]).show()

# %% [markdown]
# # Task 2: Two Advanced DataFrame Analysis (20 marks)
# 

# %% [markdown]
# ## Navya Athoti
# 
# ### Query 1: Rolling Hourly Traffic per IP (Window Function)
# 

# %%
def navya_window_analysis():
    query = """
    WITH hourly_counts AS (
        SELECT 
            ip_address,
            date_trunc('hour', timestamp) as hour,
            COUNT(*) as traffic_count,
            LAG(COUNT(*)) OVER (PARTITION BY ip_address ORDER BY date_trunc('hour', timestamp)) as prev_hour_count
        FROM student1_logs
        GROUP BY ip_address, date_trunc('hour', timestamp)
    )
    SELECT 
        ip_address,
        hour,
        traffic_count,
        prev_hour_count,
        COALESCE(traffic_count - prev_hour_count, 0) as traffic_change
    FROM hourly_counts
    ORDER BY hour, ip_address
    """
    return spark.sql(query)

# Execute and show results
print("Navya's Window Function Analysis - Rolling Hourly Traffic:")
navya_window_analysis().show(5)

# %% [markdown]
# ### Query 2: Traffic Patterns by URL Path (Aggregation)
# 

# %%
def navya_aggregation_analysis():
    query = """
    SELECT 
        url_path,
        date_trunc('hour', timestamp) as hour,
        COUNT(*) as visit_count,
        COUNT(DISTINCT ip_address) as unique_visitors
    FROM student1_logs
    GROUP BY url_path, date_trunc('hour', timestamp)
    ORDER BY hour, visit_count DESC
    """
    return spark.sql(query)

# Execute and show results
print("\nNavya's Aggregation Analysis - URL Traffic Patterns:")
navya_aggregation_analysis().show(5)

# %% [markdown]
# ## Phalguna Avalagunta
# 
# ### Query 1: Session Identification (Window Function)
# 

# %%
def phalguna_window_analysis():
    query = """
    WITH time_gaps AS (
        SELECT 
            timestamp,
            LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
            status_code,
            CASE 
                WHEN (unix_timestamp(timestamp) - unix_timestamp(LAG(timestamp) OVER (ORDER BY timestamp))) > 1800 
                OR LAG(timestamp) OVER (ORDER BY timestamp) IS NULL 
                THEN 1 
                ELSE 0 
            END as new_session
        FROM student2_logs
    )
    SELECT 
        timestamp,
        status_code,
        new_session,
        SUM(new_session) OVER (ORDER BY timestamp) as session_id
    FROM time_gaps
    ORDER BY timestamp
    """
    return spark.sql(query)

# Execute and show results
print("Phalguna's Window Function Analysis - Session Identification:")
phalguna_window_analysis().show(5)

# %% [markdown]
# ### Query 2: Top 10 Failed Requests by Size (Aggregation)
# 

# %%
def phalguna_aggregation_analysis():
    query = """
    SELECT 
        status_code,
        response_size,
        timestamp
    FROM student2_logs
    WHERE status_code >= '400'
    ORDER BY response_size DESC
    LIMIT 10
    """
    return spark.sql(query)

# Execute and show results
print("Phalguna's Aggregation Analysis - Top Failed Requests:")
phalguna_aggregation_analysis().show(5)

# %% [markdown]
# ## Student 3: Nikhil Sai Damera
# 
# ### Query 1: Unique Visitors per Hour (Window Function)
# 

# %%
def nikhil_window_analysis():
    query = """
    SELECT 
        date_trunc('hour', timestamp) as hour,
        COUNT(DISTINCT ip_address) as unique_visitors,
        COUNT(DISTINCT ip_address) - LAG(COUNT(DISTINCT ip_address)) 
            OVER (ORDER BY date_trunc('hour', timestamp)) as visitor_change
    FROM student3_logs
    GROUP BY date_trunc('hour', timestamp)
    ORDER BY hour
    """
    return spark.sql(query)

# Execute and show results
print("Nikhil's Window Function Analysis - Hourly Unique Visitors:")
nikhil_window_analysis().show(5)

# %% [markdown]
# ### Query 2: Response Size Distribution by Status (Aggregation)
# 

# %%
def nikhil_aggregation_analysis():
    query = """
    SELECT 
        status_code,
        MIN(response_size) as min_size,
        MAX(response_size) as max_size,
        AVG(response_size) as avg_size,
        PERCENTILE_APPROX(response_size, 0.5) as median_size,
        COUNT(*) as request_count
    FROM student3_logs
    GROUP BY status_code
    ORDER BY status_code
    """
    return spark.sql(query)

# Execute and show results
print("Nikhil's Aggregation Analysis - Response Size Distribution:")
nikhil_aggregation_analysis().show(5)

# %% [markdown]
# ## Student 4: Sai Kishore Dodda
# 
# ### Query 1: Average Response Size per Status Code (Window Function)
# 

# %%
def sai_window_analysis():
    query = """
    SELECT 
        status_code,
        AVG(response_size) OVER (
            PARTITION BY status_code 
            ORDER BY timestamp 
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) as rolling_avg_size,
        response_size,
        timestamp
    FROM student4_logs
    ORDER BY timestamp
    """
    return spark.sql(query)

# Execute and show results
print("Sai's Window Function Analysis - Rolling Average Response Size:")
sai_window_analysis().show(5)

# %% [markdown]
# ### Query 2: Daily Unique Visitors (Aggregation)
# 

# %%
def sai_aggregation_analysis():
    query = """
    SELECT 
        date_trunc('day', timestamp) as day,
        COUNT(DISTINCT ip_address) as unique_visitors,
        COUNT(*) as total_requests
    FROM student4_logs
    GROUP BY date_trunc('day', timestamp)
    ORDER BY day
    """
    return spark.sql(query)

# Execute and show results
print("Sai's Aggregation Analysis - Daily Unique Visitors:")
sai_aggregation_analysis().show(5)

# %% [markdown]
# ## Execute All Analyses
# 

# %%
def run_all_analyses():
    try:
        print("\n=== Running All Analyses ===\n")
        
        # Student 1: Navya
        print("\nStudent 1 (Navya) Analyses:")
        navya_window_analysis().show(5)
        navya_aggregation_analysis().show(5)
        
        # Student 2: Phalguna
        print("\nStudent 2 (Phalguna) Analyses:")
        phalguna_window_analysis().show(5)
        phalguna_aggregation_analysis().show(5)
        
        # Student 3: Nikhil
        print("\nStudent 3 (Nikhil) Analyses:")
        nikhil_window_analysis().show(5)
        nikhil_aggregation_analysis().show(5)
        
        # Student 4: Sai
        print("\nStudent 4 (Sai) Analyses:")
        sai_window_analysis().show(5)
        sai_aggregation_analysis().show(5)
        
        print("\n=== All Analyses Completed Successfully ===")
        
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("\nDebug Information:")
        for view in ["student1_logs", "student2_logs", "student3_logs", "student4_logs"]:
            print(f"\nSample data from {view}:")
            spark.sql(f"SELECT * FROM {view} LIMIT 3").show()

# Run all analyses
run_all_analyses()

# %% [markdown]
# # Task 3: Data Visualization (10 marks)
# 
# Each member will visualize the results of their unique SQL queries using different chart types.
# 
# ### Student Visualization Type Examples
# 
# - **Student 1: Line Chart (Hourly Traffic)**
# 
#   - **Tool:** Matplotlib for rolling traffic visualization.
# 
#   ![student1_traffic.png](attachment:student1_traffic.png)
# 
#   - **Student 2: Bar Chart (Top 10 Failed Requests)**
# 
#   - **Tool:** Seaborn for aggregated failure counts.
# 
#   ![student2_failures.png](attachment:student2_failures.png)
# 
# - **Student 3: Heatmap (Hourly Unique Visitors)**
# 
#   - **Tool:** Seaborn for visualizing traffic density.
# 
#   ![student3_heatmap.png](attachment:student3_heatmap.png)
# 
# - **Student 4: Pie Chart (Response Code Distribution)**
# 
#   - **Tool:** Matplotlib for status code proportions.
# 
#   ![student4_distribution.png](attachment:student4_distribution.png)
# 

# %% [markdown]
# ## Data Visualization & Shared Imports and Utilities
# 

# %%
# %pip install

# import matplotlib.pyplot as plt
# import seaborn as sns
# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta

# def create_sample_data():
#     """Create sample log data"""
 
#     base_time = datetime.now()
#     data = []
    
   
#     for i in range(1000):
#         timestamp = base_time + timedelta(hours=i % 24)
#         ip = f"192.168.1.{np.random.randint(1, 255)}"
#         status = np.random.choice([200, 404, 500], p=[0.8, 0.15, 0.05])
#         size = np.random.randint(100, 10000)
#         data.append({
#             'timestamp': timestamp,
#             'ip_address': ip,
#             'status_code': status,
#             'response_size': size
#         })
    
#     df1 = pd.DataFrame(data)
    
    
#     data = []
#     for i in range(1000):
#         timestamp = base_time + timedelta(hours=i % 24)
#         ip = f"192.168.2.{np.random.randint(1, 255)}"
#         status = np.random.choice([200, 301, 404, 500], p=[0.7, 0.1, 0.15, 0.05])
#         size = np.random.randint(100, 10000)
#         data.append({
#             'timestamp': timestamp,
#             'ip_address': ip,
#             'status_code': status,
#             'response_size': size
#         })
    
#     df2 = pd.DataFrame(data)
#     return df1, df2



# %% [markdown]
# ## Navya Athoti - Hourly Traffic Analysis
# 

# %%
# ===============================================
# Student 1: Navya Athoti (u2793047@uel.ac.uk)
# Hourly Traffic Pattern Analysis
# ===============================================

def navya_traffic_analysis():
    """Generate hourly traffic pattern visualization"""
    try:
        # Get sample data
        df1, _ = create_sample_data()
        
        # Extract hour and count traffic
        df1['hour'] = df1['timestamp'].dt.hour
        hourly_traffic = df1.groupby('hour').size().reset_index(name='traffic_count')
        
        # Create visualization
        plt.figure(figsize=(10, 6))
        plt.plot(hourly_traffic['hour'], hourly_traffic['traffic_count'], marker='o')
        plt.title('Hourly Traffic Pattern Analysis')
        plt.xlabel('Hour of Day')
        plt.ylabel('Traffic Count')
        plt.grid(True)
        plt.savefig('navya_traffic_analysis.png')
        plt.close()
        
        return "Analysis completed successfully"
    except Exception as e:
        return f"Error in analysis: {str(e)}"


# %% [markdown]
# ## Phalguna Avalagunta - Failed Requests Analysis
# 

# %%
# ===============================================
# Student 2: Phalguna Avalagunta (u2811669@uel.ac.uk)
# Failed Requests Analysis
# ===============================================

def phalguna_error_analysis():
    """Analyze failed requests by response size"""
    try:
        spark = initialize_spark()
        create_sample_data(spark)  # Create sample data first
        
        # Query for error analysis
        query = """
        SELECT status_code, 
               AVG(response_size) as avg_size 
        FROM student2_logs 
        WHERE CAST(status_code AS INT) >= 400 
        GROUP BY status_code 
        ORDER BY avg_size DESC 
        LIMIT 10
        """
        df = spark.sql(query)
        
        # Create visualization
        pdf = df.toPandas()
        plt.figure(figsize=(10, 6))
        sns.barplot(x='status_code', y='avg_size', data=pdf)
        plt.title('Top Failed Requests Analysis')
        plt.xlabel('Status Code')
        plt.ylabel('Average Response Size')
        plt.savefig('phalguna_error_analysis.png')
        plt.close()
        
        return "Analysis completed successfully"
    except Exception as e:
        return f"Error in analysis: {str(e)}"
    finally:
        if 'spark' in locals():
            spark.stop()



# %% [markdown]
# ## Nikhil Sai Damera - Traffic Density Analysis
# 

# %%
# ===============================================
# Student 3: Nikhil Sai Damera (u2810262@uel.ac.uk)
# Traffic Density Heatmap
# ===============================================

def nikhil_traffic_density():
    """Generate traffic density heatmap"""
    try:
        spark = initialize_spark()
        create_sample_data(spark)  # Create sample data first
        
        # Query for traffic density
        query = """
        SELECT 
            hour(timestamp) as hour_of_day,
            to_date(timestamp) as date,
            COUNT(DISTINCT ip_address) as unique_visitors
        FROM student1_logs
        GROUP BY hour_of_day, date
        ORDER BY date, hour_of_day
        """
        df = spark.sql(query)
        
        # Create visualization
        pdf = df.toPandas()
        pivot_data = pdf.pivot(index='date', columns='hour_of_day', values='unique_visitors')
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_data, cmap='YlOrRd', annot=True, fmt='g')
        plt.title('Traffic Density Analysis')
        plt.xlabel('Hour of Day')
        plt.ylabel('Date')
        plt.savefig('nikhil_density_analysis.png')
        plt.close()
        
        return "Analysis completed successfully"
    except Exception as e:
        return f"Error in analysis: {str(e)}"
    finally:
        if 'spark' in locals():
            spark.stop()



# %% [markdown]
# ## Sai Kishore Dodda - Response Code Distribution
# 

# %%

# ===============================================
# Student 4: Sai Kishore Dodda (u2773584@uel.ac.uk)
# Response Code Distribution
# ===============================================

def sai_response_distribution():
    """Analyze distribution of response codes"""
    try:
        # Get sample data
        _, df2 = create_sample_data()
        
        # Calculate response code distribution
        status_dist = df2['status_code'].value_counts()
        
        # Create visualization
        plt.figure(figsize=(10, 10))
        plt.pie(status_dist.values, labels=status_dist.index, autopct='%1.1f%%')
        plt.title('Response Code Distribution Analysis')
        plt.axis('equal')
        plt.savefig('sai_distribution_analysis.png')
        plt.close()
        
        return "Analysis completed successfully"
    except Exception as e:
        return f"Error in analysis: {str(e)}"

# Main execution function to run all analyses
def run_all_analyses():
    """Execute all student analyses"""
    results = {
        "Navya's Analysis": navya_traffic_analysis(),
        "Phalguna's Analysis": phalguna_error_analysis(),
        "Nikhil's Analysis": nikhil_traffic_density(),
        "Sai's Analysis": sai_response_distribution()
    }
    
    for name, result in results.items():
        print(f"{name}: {result}")

if __name__ == "__main__":
    run_all_analyses()

# %% [markdown]
# # Data Processing using PySpark RDD
# 
# ## Task 1: Basic RDD Analysis (10 marks)
# 
# ### Setup and Configuration
# 

# %%
# Initialize Spark (Common Setup)
import findspark
findspark.init()

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create new SparkSession
spark = SparkSession.builder \
    .appName("LogAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# %% [markdown]
# ### =================================================================
# 
# ### Student 1: Navya Athoti (u2793047@uel.ac.uk)
# 
# ### Task: Extract Timestamp and IP
# 
# ### =================================================================
# 

# %%
# =================================================================
# Student 1: Navya Athoti (u2793047@uel.ac.uk)
# Task: Extract Timestamp and IP
# =================================================================

def extract_timestamp_ip(log_line):
    """Extract timestamp and IP address from log entry"""
    if isinstance(log_line, str):
        text = log_line
    else:
        text = log_line.value  # For DataFrame rows
        
    ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
    timestamp_pattern = r'\[([^\]]+)\]'
    
    try:
        ip = re.search(ip_pattern, text).group(1)
        timestamp = re.search(timestamp_pattern, text).group(1)
        return (ip, timestamp)
    except Exception as e:
        return ("Error", "Error")


# %% [markdown]
# ### =================================================================
# 
# ##
# 
# ### Student 2: Phalguna Avalagunta (u2811669@uel.ac.uk)
# 
# ##
# 
# ### Task: Extract URL and HTTP Method
# 
# ##
# 
# ### =================================================================
# 
# ##
# 

# %%
# =================================================================
# Student 2: Phalguna Avalagunta (u2811669@uel.ac.uk)
# Task: Extract URL and HTTP Method
# =================================================================

def extract_url_method(log_line):
    """Extract URL path and HTTP method from log entry"""
    if isinstance(log_line, str):
        text = log_line
    else:
        text = log_line.value  # For DataFrame rows
        
    pattern = r'"(GET|POST|PUT|DELETE)\s+([^"\s]+)'
    
    try:
        match = re.search(pattern, text)
        if match:
            return (match.group(1), match.group(2))
        return ("Not Found", "Not Found")
    except Exception as e:
        return ("Error", "Error")

# %% [markdown]
# ### =================================================================
# 
# ### Student 3: Nikhil Sai Damera (u2810262@uel.ac.uk)
# 
# ### Task: Extract Status Code and Response Size
# 
# ### =================================================================
# 

# %%
# =================================================================
# Student 3: Nikhil Sai Damera (u2810262@uel.ac.uk)
# Task: Extract Status Code and Response Size
# =================================================================

def extract_status_size(log_line):
    """Extract HTTP status code and response size from log entry"""
    if isinstance(log_line, str):
        text = log_line
    else:
        text = log_line.value  # For DataFrame rows
        
    pattern = r'HTTP/[\d.]+" (\d{3}) (\d+)'
    
    try:
        match = re.search(pattern, text)
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)
    except Exception as e:
        return (0, 0)

# %% [markdown]
# ### =================================================================
# 
# ### Student 4: Sai Kishore Dodda (u2773584@uel.ac.uk)
# 
# ### Task: Extract Log Message and IP Address
# 
# ### =================================================================
# 

# %%
# =================================================================
# Student 4: Sai Kishore Dodda (u2773584@uel.ac.uk)
# Task: Extract Log Message and IP Address
# =================================================================

def extract_message_ip(log_line):
    """Extract log message and IP address from log entry"""
    if isinstance(log_line, str):
        text = log_line
    else:
        text = log_line.value  # For DataFrame rows
        
    ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
    # Updated pattern to better match log messages
    message_pattern = r'"([^"]+)"'  # This will capture text between quotes
    
    try:
        ip = re.search(ip_pattern, text).group(1)
        message = re.search(message_pattern, text)
        if message:
            return (ip, message.group(1))
        return (ip, "No message found")
    except Exception as e:
        return ("Error", f"Error processing: {str(e)}")

# %% [markdown]
# ### =================================================================
# 
# ### Main Analysis Function
# 
# ### =================================================================
# 

# %%
# =================================================================
# Main Analysis Function
# =================================================================

def analyze_logs(data):
    """Analyze logs using the existing Spark session"""
    print("\nProcessing log entries...")
    print("\n" + "="*50 + "\n")
    
    try:
        # Convert DataFrame to RDD of log lines
        logs_rdd = data.rdd.map(lambda row: row[0])
        
        # Navya's Analysis
        print("NAVYA ATHOTI - TIMESTAMP AND IP EXTRACTION")
        print("-" * 40)
        rdd1_results = logs_rdd.map(extract_timestamp_ip).collect()
        for ip, timestamp in rdd1_results[:5]:
            print(f"IP Address: {ip}")
            print(f"Timestamp:  {timestamp}\n")

        print("\n" + "="*50 + "\n")
        
        # Phalguna's Analysis
        print("PHALGUNA AVALAGUNTA - URL AND HTTP METHOD EXTRACTION")
        print("-" * 40)
        rdd2_results = logs_rdd.map(extract_url_method).collect()
        for method, url in rdd2_results[:5]:
            print(f"HTTP Method: {method}")
            print(f"URL Path:    {url}\n")

        print("\n" + "="*50 + "\n")
        
        # Nikhil's Analysis
        print("NIKHIL SAI DAMERA - STATUS CODE AND RESPONSE SIZE EXTRACTION")
        print("-" * 40)
        rdd3_results = logs_rdd.map(extract_status_size).collect()
        for status, size in rdd3_results[:5]:
            print(f"Status Code:   {status}")
            print(f"Response Size: {size} bytes\n")

        print("\n" + "="*50 + "\n")
        
        # Sai Kishore's Analysis
        print("SAI KISHORE DODDA - LOG MESSAGE AND IP EXTRACTION")
        print("-" * 40)
        rdd4_results = logs_rdd.map(extract_message_ip).collect()
        for ip, message in rdd4_results[:5]:
            print(f"IP Address:  {ip}")
            print(f"Log Message: {message}\n")
            
    except Exception as e:
        print(f"Error during processing: {str(e)}")

# Main execution
if __name__ == "__main__":
    try:
        # Load the log file
        data = spark.read.text("web.log")
        
        # Execute the analysis
        analyze_logs(data)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        # Stop SparkSession
        spark.stop()

# %% [markdown]
# ## Task 2: Two Advanced RDD Analysis (30 marks)
# 
# Each member will perform unique advanced processing tasks.
# 
# ### Student Advanced Analysis Examples
# 
# - **Student 1**: Calculate hourly visit counts per IP  
#    **Description**: Count visits grouped by hour and IP.
# - **Student 2**: Identify top 10 URLs by visit count  
#    **Description**: Aggregate visit counts and rank top URLs.
# - **Student 3**: Find average response size per URL  
#    **Description**: Compute average response size for each URL.
# - **Student 4**: Detect failed requests per IP  
#    **Description**: Identify IPs with the most failed requests.
# 

# %%

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import re


spark = SparkSession.builder \
    .appName("AdvancedLogAnalysis") \
    .master("local[*]") \
    .getOrCreate()

def advanced_rdd_analysis(data):
   
    logs_rdd = data.rdd.map(lambda row: row[0])
    
    print("\nAdvanced RDD Analysis Results")
    print("=" * 50)

   
    def student1_analysis(logs_rdd):
        print("\nStudent 1 - Hourly Visit Counts per IP")
        print("-" * 40)
        
        def extract_hour_ip(log_line):
           
            ip_pattern = r'(\d+\.\d+\.\d+\.\d+)'
            timestamp_pattern = r'\[([\d\/\w:]+\s[+\-]\d{4})\]'
            
            try:
                ip = re.search(ip_pattern, log_line).group(1)
                timestamp_str = re.search(timestamp_pattern, log_line).group(1)
               
                dt = datetime.strptime(timestamp_str, '%d/%b/%Y:%H:%M:%S %z')
                hour = dt.strftime('%H:00')
                return ((ip, hour), 1)
            except Exception as e:
                return (("Error", "Error"), 0)

        hourly_counts = logs_rdd \
            .map(extract_hour_ip) \
            .filter(lambda x: x[0] != ("Error", "Error")) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[0][0], x[0][1], x[1])) \
            .sortBy(lambda x: (x[0], x[1]))

        print("\nSample of Hourly Visit Counts:")
        for ip, hour, count in hourly_counts.take(10):
            print(f"IP: {ip}, Hour: {hour}, Visits: {count}")

        total_visits = hourly_counts \
            .map(lambda x: (x[0], x[2])) \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)

        print("\nTop 5 IPs by Total Visits:")
        for ip, total in total_visits.take(5):
            print(f"IP: {ip}, Total Visits: {total}")


    def student2_analysis(logs_rdd):
        print("\nStudent 2 - Top 10 URLs by Visit Count")
        print("-" * 40)
        
        def extract_url(log_line):
            pattern = r'"(?:GET|POST|PUT|DELETE)\s+([^"\s]+)'
            try:
                url = re.search(pattern, log_line).group(1)
                return (url, 1)
            except Exception:
                return ("Error", 0)

        url_counts = logs_rdd \
            .map(extract_url) \
            .filter(lambda x: x[0] != "Error") \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)

        print("\nTop 10 Most Visited URLs:")
        for url, count in url_counts.take(10):
            print(f"URL: {url}")
            print(f"Visit Count: {count}\n")

        total_visits = url_counts.map(lambda x: x[1]).sum()
        print("\nTraffic Distribution:")
        for url, count in url_counts.take(5):
            percentage = (count / total_visits) * 100
            print(f"URL: {url}")
            print(f"Percentage of Total Traffic: {percentage:.2f}%\n")

    # Student 3: Response size analysis (Fixed statistics calculation)
    def student3_analysis(logs_rdd):
        print("\nStudent 3 - Average Response Size per URL")
        print("-" * 40)
        
        def extract_url_size(log_line):
            url_pattern = r'"(?:GET|POST|PUT|DELETE)\s+([^"\s]+)'
            size_pattern = r'HTTP/[\d.]+" \d{3} (\d+)'
            try:
                url = re.search(url_pattern, log_line).group(1)
                size = int(re.search(size_pattern, log_line).group(1))
                return (url, (size, 1))
            except Exception:
                return ("Error", (0, 0))

        avg_sizes = logs_rdd \
            .map(extract_url_size) \
            .filter(lambda x: x[0] != "Error") \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .mapValues(lambda x: x[0] / x[1] if x[1] > 0 else 0) \
            .sortBy(lambda x: x[1], ascending=False)

        print("\nURL Response Size Analysis:")
        for url, avg_size in avg_sizes.take(10):
            print(f"URL: {url}")
            print(f"Average Response Size: {avg_size:.2f} bytes\n")

        # Fixed statistics calculation
        sizes_data = avg_sizes.collect()
        if sizes_data:
            sizes = [size for _, size in sizes_data]
            print("\nResponse Size Statistics:")
            print(f"Maximum Average Size: {max(sizes):.2f} bytes")
            print(f"Minimum Average Size: {min(sizes):.2f} bytes")
            print(f"Overall Average Size: {sum(sizes)/len(sizes):.2f} bytes")

    # Execute the analyses
    try:
        student1_analysis(logs_rdd)
        print("\n" + "="*50)
        student2_analysis(logs_rdd)
        print("\n" + "="*50)
        student3_analysis(logs_rdd)
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

# Main execution
if __name__ == "__main__":
    try:
        # Load the log file 
        data = spark.read.text("web.log")
        
        # Execute the analysis
        advanced_rdd_analysis(data)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        # Stop SparkSession
        spark.stop()

# %% [markdown]
# - **Student 4**: Detect failed requests per IP  
#    **Description**: Identify IPs with the most failed requests.
# 

# %%
import findspark
findspark.init()

from pyspark.sql import SparkSession
import re


spark = SparkSession.builder \
    .appName("FailedRequestsAnalysis") \
    .master("local[*]") \
    .getOrCreate()

def analyze_failed_requests(data):
    """
    Analyze failed requests from web server logs.
    """
    try:
        # Convert DataFrame to RDD
        logs_rdd = data.rdd.map(lambda row: row[0])
        
        def extract_ip_status(log_line):
            ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
            status_pattern = r'HTTP/[\d.]+" (\d{3})'
            try:
                ip = re.search(ip_pattern, log_line).group(1)
                status = int(re.search(status_pattern, log_line).group(1))
                is_failed = 1 if status >= 400 else 0
                return (ip, (is_failed, 1))
            except Exception:
                return ("Error", (0, 0))

        
        ip_stats = logs_rdd \
            .map(extract_ip_status) \
            .filter(lambda x: x[0] != "Error") \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .collect()
        
       
        if ip_stats:
           
            results = []
            for ip, (failed, total) in ip_stats:
                failure_rate = (failed / total * 100) if total > 0 else 0
                results.append({
                    'ip': ip,
                    'failed': failed,
                    'total': total,
                    'failure_rate': failure_rate
                })
            
           
            results.sort(key=lambda x: (-x['failed'], x['ip']))
            
            print("\nTop 10 IPs by Failed Requests:")
            for stat in results[:10]:
                print(f"IP: {stat['ip']}")
                print(f"Failed Requests: {stat['failed']}")
                print(f"Total Requests: {stat['total']}")
                print(f"Failure Rate: {stat['failure_rate']:.2f}%\n")

          
            total_failed = sum(stat['failed'] for stat in results)
            total_requests = sum(stat['total'] for stat in results)
            overall_rate = (total_failed / total_requests * 100) if total_requests > 0 else 0

            print("Overall Statistics:")
            print(f"Total Failed Requests: {total_failed}")
            print(f"Total Requests: {total_requests}")
            print(f"Overall Failure Rate: {overall_rate:.2f}%\n")

            high_failure_ips = [stat for stat in results if stat['total'] >= 10]
            high_failure_ips.sort(key=lambda x: (-x['failure_rate'], x['ip']))

            print("Top 5 IPs with Highest Failure Rates (min 10 requests):")
            for stat in high_failure_ips[:5]:
                print(f"IP: {stat['ip']}")
                print(f"Failure Rate: {stat['failure_rate']:.2f}%")
                print(f"Failed: {stat['failed']}/{stat['total']} requests\n")
        else:
            print("No valid data found for analysis")
            
    except Exception as e:
        print(f"Error during analysis: {str(e)}")


if __name__ == "__main__":
    try:
      
        data = spark.read.text("web.log")
        
    
        analyze_failed_requests(data)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        spark.stop()

# %% [markdown]
# ## Task 3: Optimization and LSEPI Considerations (10 marks)
# 
# Each member chooses two unique methods for optimization.
# 
# ### Student Optimization Methods
# 
# - **Student 1**: Partition Strategies, Caching
# - **Student 2**: Caching, Bucketing & Indexing
# - **Student 3**: Partition Strategies, Bucketing & Indexing
# - **Student 4**: Caching, Partition Strategies
# 

# %%
# Common Imports and Setup
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import builtins
import time
import re
import os

def initialize_spark():
    spark = (SparkSession.builder
            .appName("LogAnalysis")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.sql.execution.arrow.enabled", "true")
            .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.2f} seconds")
        return result, execution_time
    return wrapper

# =================================================================
# Student 1: Navya Athoti (u2793047@uel.ac.uk)
# Tasks: 
# 1. Basic - Extract Timestamp and IP
# 2. Optimization - Partition Strategies and Caching
# =================================================================

class NavyaAnalysis:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def extract_data(log_line):
        """Extract IP and timestamp for analysis"""
        if isinstance(log_line, str):
            text = log_line
        else:
            text = log_line.value
            
        ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
        timestamp_pattern = r'\[([^\]]+)\]'
        
        try:
            ip = re.search(ip_pattern, text).group(1)
            timestamp = re.search(timestamp_pattern, text).group(1)
            return (ip, timestamp)
        except:
            return ("error", "error")

    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nNavya - Baseline Analysis")
        extract_fn = NavyaAnalysis.extract_data  # Use static method
        rdd = self.data.rdd.map(lambda row: row[0])
        return rdd.map(extract_fn).groupByKey().mapValues(list).collect()

# =================================================================
# Student 2: Phalguna Avalagunta (u2811669@uel.ac.uk)
# Tasks: 
# 1. Basic - Extract URL and HTTP Method
# 2. Optimization - Caching and Bucketing
# =================================================================

class PhalgunaAnalysis:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def extract_url_method(log_line):
        """Extract URL and HTTP method from log entry"""
        if isinstance(log_line, str):
            text = log_line
        else:
            text = log_line.value
            
        pattern = r'"(GET|POST|PUT|DELETE)\s+([^"\s]+)'
        
        try:
            match = re.search(pattern, text)
            if match:
                return (match.group(1), match.group(2))
            return ("Not Found", "Not Found")
        except:
            return ("Error", "Error")

    @staticmethod
    def extract_url_info(log_line):
        """Extract URL and additional info for analysis"""
        if isinstance(log_line, str):
            text = log_line
        else:
            text = log_line.value
            
        method_url = PhalgunaAnalysis.extract_url_method(text)
        return (method_url[1], method_url[0])

    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nPhalguna - Baseline Analysis")
        extract_fn = PhalgunaAnalysis.extract_url_info  # Use static method
        rdd = self.data.rdd.map(lambda row: row[0])
        return rdd.map(extract_fn).groupByKey().mapValues(list).collect()

# =================================================================
# Student 3: Nikhil Sai Damera (u2810262@uel.ac.uk)
# Tasks: 
# 1. Basic - Extract Status Code and Response Size
# 2. Optimization - Partition Strategies and Bucketing
# =================================================================

class NikhilAnalysis:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def extract_status_size(log_line):
        """Extract status code and response size"""
        if isinstance(log_line, str):
            text = log_line
        else:
            text = log_line.value
            
        pattern = r'HTTP/[\d.]+" (\d{3}) (\d+)'
        
        try:
            match = re.search(pattern, text)
            if match:
                return (int(match.group(1)), int(match.group(2)))
            return (0, 0)
        except:
            return (0, 0)

    @staticmethod
    def extract_status_info(log_line):
        """Extract status code and size for analysis"""
        return NikhilAnalysis.extract_status_size(log_line)

    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nNikhil - Baseline Analysis")
        extract_fn = NikhilAnalysis.extract_status_info  # Use static method
        rdd = self.data.rdd.map(lambda row: row[0])
        return rdd.map(extract_fn).groupByKey().mapValues(list).collect()

# =================================================================
# Student 4: Sai Kishore Dodda (u2773584@uel.ac.uk)
# Tasks: 
# 1. Basic - Extract Log Message and IP Address
# 2. Optimization - Caching and Partition Strategies
# =================================================================

class SaiKishoreAnalysis:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def extract_message_ip(log_line):
        """Extract message and IP"""
        if isinstance(log_line, str):
            text = log_line
        else:
            text = log_line.value
            
        ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
        message_pattern = r'"([^"]+)"'
        
        try:
            ip = re.search(ip_pattern, text).group(1)
            message = re.search(message_pattern, text)
            if message:
                return (ip, message.group(1))
            return (ip, "No message found")
        except:
            return ("Error", "Error processing")

    @staticmethod
    def extract_ip_info(log_line):
        """Extract IP and message for analysis"""
        return SaiKishoreAnalysis.extract_message_ip(log_line)

    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nSai Kishore - Baseline Analysis")
        extract_fn = SaiKishoreAnalysis.extract_ip_info 
        rdd = self.data.rdd.map(lambda row: row[0])
        return rdd.map(extract_fn).groupByKey().mapValues(list).collect()

def main():
    try:
        # Initialize Spark
        spark = initialize_spark()
        print("Spark initialized successfully!")
        
        # Load data
        data = spark.read.text("web.log")
        print(f"Loaded {data.count()} log entries")
        
        # Initialize analyses
        analyses = {
            "Navya": NavyaAnalysis(data),
            "Phalguna": PhalgunaAnalysis(data),
            "Nikhil": NikhilAnalysis(data),
            "Sai Kishore": SaiKishoreAnalysis(data)
        }
        
        # Run analyses
        for name, analysis in analyses.items():
            print(f"\n{'='*50}")
            print(f"Running analysis for {name}")
            print(f"{'='*50}")
            
            try:
                baseline, time1 = analysis.baseline_analysis()
                
                if isinstance(analysis, PhalgunaAnalysis):
                    optimization1, time2 = analysis.optimized_with_caching()
                    optimization2, time3 = analysis.optimized_with_bucketing()
                    method2_name = "Bucketing"
                else:
                    optimization1, time2 = analysis.optimized_with_caching()
                    optimization2, time3 = analysis.optimized_with_partitioning()
                    method2_name = "Partitioning"
                
                print(f"\nPerformance Improvements:")
                print(f"Caching: {((time1 - time2)/time1)*100:.2f}% faster")
                print(f"{method2_name}: {((time1 - time3)/time1)*100:.2f}% faster")
                
            
                
            except Exception as e:
                print(f"Error in analysis: {str(e)}")
                import traceback
                print(traceback.format_exc())
                
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# %% [markdown]
# ## Convert ipynb to HTML for Turnitin submission. (5 marks)
# 

# %%
import nbformat
from nbconvert import HTMLExporter
import os

def convert_notebook_to_html(notebook_path, output_path=None):
  
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = nbformat.read(f, as_version=4)
        
        html_exporter = HTMLExporter()
        html_exporter.template_name = 'classic'  # Use classic template
        
        body, _ = html_exporter.from_notebook_node(notebook)
    
        if output_path is None:
            output_path = os.path.splitext(notebook_path)[0] + '.html'
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(body)
            
        print(f"Successfully converted '{notebook_path}' to HTML")
        print(f"Output saved to: '{output_path}'")
        return output_path
        
    except FileNotFoundError:
        print(f"Error: Could not find notebook file '{notebook_path}'")
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
    
    return None

def batch_convert_notebooks(directory_path, output_directory=None):

    try:
        if output_directory and not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        converted_files = []
        
        for filename in os.listdir(directory_path):
            if filename.endswith('.ipynb'):
                notebook_path = os.path.join(directory_path, filename)
                
                if output_directory:
                    output_path = os.path.join(output_directory, 
                                             os.path.splitext(filename)[0] + '.html')
                else:
                    output_path = None
                    
                result = convert_notebook_to_html(notebook_path, output_path)
                if result:
                    converted_files.append(result)
        
        return converted_files
        
    except Exception as e:
        print(f"Error during batch conversion: {str(e)}")
        return []

if __name__ == "__main__":
    # Convert a single notebook
    notebook_path = "CN7031_Group126_2024.ipynb"
    html_path = convert_notebook_to_html(notebook_path)



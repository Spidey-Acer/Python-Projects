# %% [markdown]
# # Big Data Analytics [CN7031] CRWK 2024-25
# 
# ## Group ID: CN7031_Group126_2024
# 
# ### Group Members:
# 
# 1. **Navya Athoti**  
#    Email: u2793047@uel.ac.uk
# 2. **Phalguna Avalagunta**  
#    Email: u2811669@uel.ac.uk
# 3. **Nikhil Sai Damera**  
#    Email: u2810262@uel.ac.uk
# 4. **Sai Kishore Dodda**  
#    Email: u2773584@uel.ac.uk
# 
# ---
# 

# %% [markdown]
# ## Initiate and Configure Spark
# 
# In this section, we will initiate and configure Apache Spark, which is a powerful open-source processing engine for big data. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.
# 

# %%
!pip3 install pyspark

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

# %% [markdown]
# # Load Unstructured Data
# 
# In this section, we will load and process unstructured data. Unstructured data refers to information that does not have a predefined data model or is not organized in a predefined manner. This type of data is typically text-heavy, but may also contain data such as dates, numbers, and facts.
# 
# We will explore various techniques to handle and analyze unstructured data, including tokenization, vectorization, and the use of embeddings to capture semantic information.
# 

# %%
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
# ## Common Imports and Spark Initialization
# 

# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import regexp_extract, to_timestamp, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Log Analysis") \
    .getOrCreate()

# Read the log file
logs_df = spark.read.text("web.log")

# %% [markdown]
# ## Student 1: Navya Athoti - IP Address, Timestamp, and HTTP Method Analysis
# 

# %%
# Student 1: Navya Athoti - Modified to include url_path
def create_student1_df():
    student1_df = logs_df.select(
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

# Create DataFrame
student1_df = create_student1_df()

# Verify the schema
print("Student 1 DataFrame Schema:")
student1_df.printSchema()

# %% [markdown]
# ## Student 2: Phalguna Avalagunta - HTTP Status Code, Response Size, and Timestamp Analysis
# 

# %%
# Extract HTTP Status Code, Response Size, and Timestamp
def create_student2_df():
    student2_df = logs_df.select(
        regexp_extract(col("value"), r'" (\d{3})', 1).alias("status_code"),
        regexp_extract(col("value"), r'" \d{3} (\d+)', 1).cast(IntegerType()).alias("response_size"),
        to_timestamp(
            regexp_extract(col("value"), r"\[(.*?)\]", 1),
            "dd/MMM/yyyy:HH:mm:ss"
        ).alias("timestamp")
    )
    
    # Register DataFrame as view for SQL queries
    student2_df.createOrReplaceTempView("student2_logs")
    return student2_df

# Validate Student 2's DataFrame
student2_df = create_student2_df()
print("Student 2 (Phalguna Avalagunta) DataFrame Schema:")
student2_df.printSchema()
print("\nSample Data:")
student2_df.show(5, truncate=False)

# %% [markdown]
# ## Student 3: Nikhil Sai Damera - URL Path, IP Address, and Response Size Analysis
# 

# %%
# Student 3: Nikhil Sai Damera - Modified to include timestamp
def create_student3_df():
    student3_df = logs_df.select(
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

# Create the DataFrame
student3_df = create_student3_df()

# Verify the schema
print("Student 3 DataFrame Schema:")
student3_df.printSchema()

# Then keep your window analysis function:
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
print("\nNikhil's Window Function Analysis - Hourly Unique Visitors:")
nikhil_window_analysis().show(5)

# %% [markdown]
# ## Student 4: Sai Kishore Dodda - Log Message, HTTP Status Code, and Timestamp Analysis
# 

# %%
# Student 4: Sai Kishore Dodda - Modified to include ip_address
def create_student4_df():
    student4_df = logs_df.select(
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

# Create the DataFrame
student4_df = create_student4_df()

# Verify the schema
print("Student 4 DataFrame Schema:")
student4_df.printSchema()

# Then the aggregation function:
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
# ## Student 1: Navya Athoti
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
# ## Student 2: Phalguna Avalagunta
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
# Shared Imports and Utilities
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_sample_data():
    """Create sample log data"""
    # Generate sample data
    base_time = datetime.now()
    data = []
    
    # Generate 1000 records for student1_logs
    for i in range(1000):
        timestamp = base_time + timedelta(hours=i % 24)
        ip = f"192.168.1.{np.random.randint(1, 255)}"
        status = np.random.choice([200, 404, 500], p=[0.8, 0.15, 0.05])
        size = np.random.randint(100, 10000)
        data.append({
            'timestamp': timestamp,
            'ip_address': ip,
            'status_code': status,
            'response_size': size
        })
    
    df1 = pd.DataFrame(data)
    
    # Generate different data for student2_logs
    data = []
    for i in range(1000):
        timestamp = base_time + timedelta(hours=i % 24)
        ip = f"192.168.2.{np.random.randint(1, 255)}"
        status = np.random.choice([200, 301, 404, 500], p=[0.7, 0.1, 0.15, 0.05])
        size = np.random.randint(100, 10000)
        data.append({
            'timestamp': timestamp,
            'ip_address': ip,
            'status_code': status,
            'response_size': size
        })
    
    df2 = pd.DataFrame(data)
    return df1, df2



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

# %%
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import hour, to_date, count, avg, col

def process_hourly_traffic(spark_df):
    """Process hourly traffic data using Spark SQL operations"""
    traffic_data = spark_df.select(
        to_date('timestamp').alias('date'),
        hour('timestamp').alias('hour'),
        'ip_address'
    ).groupBy('date', 'hour')\
     .agg(count('ip_address').alias('total_visits'))
    
    return traffic_data.toPandas()

def main():
    """Main execution function"""
    try:
        # Initialize Spark session with specific configurations
        spark = SparkSession.builder\
            .appName("LogAnalysis")\
            .config("spark.sql.execution.arrow.enabled", "false")\
            .getOrCreate()
        
        # Create the visualizations
        print("Generating visualizations...")
        
        # Example SQL query for Student 1
        student1_query = """
        SELECT hour(timestamp) as hour, 
               COUNT(*) as traffic_count 
        FROM student1_logs 
        GROUP BY hour(timestamp) 
        ORDER BY hour
        """
        df1 = spark.sql(student1_query)
        
        # Create line plot for Student 1
        pdf1 = df1.toPandas()
        plt.figure(figsize=(10, 6))
        plt.plot(pdf1['hour'], pdf1['traffic_count'], marker='o')
        plt.title('Hourly Traffic Pattern')
        plt.xlabel('Hour of Day')
        plt.ylabel('Traffic Count')
        plt.savefig('student1_traffic.png')
        plt.close()
        
        # Example SQL query for Student 2
        student2_query = """
        SELECT status_code, 
               AVG(response_size) as avg_size 
        FROM student2_logs 
        WHERE CAST(status_code AS INT) >= 400 
        GROUP BY status_code 
        ORDER BY avg_size DESC 
        LIMIT 10
        """
        df2 = spark.sql(student2_query)
        
        # Create bar plot for Student 2
        pdf2 = df2.toPandas()
        plt.figure(figsize=(10, 6))
        sns.barplot(x='status_code', y='avg_size', data=pdf2)
        plt.title('Top Failed Requests by Average Size')
        plt.xlabel('Status Code')
        plt.ylabel('Average Response Size')
        plt.savefig('student2_failures.png')
        plt.close()
        
        # Example SQL query for Student 3
        student3_query = """
        SELECT 
            hour(timestamp) as hour_of_day,
            to_date(timestamp) as date,
            COUNT(DISTINCT ip_address) as unique_visitors
        FROM student1_logs
        GROUP BY hour_of_day, date
        ORDER BY date, hour_of_day
        """
        df3 = spark.sql(student3_query)
        
        # Create heatmap for Student 3
        pdf3 = df3.toPandas()
        pivot_data = pdf3.pivot(index='date', columns='hour_of_day', values='unique_visitors')
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_data, cmap='YlOrRd', annot=True, fmt='g')
        plt.title('Traffic Density Heatmap')
        plt.xlabel('Hour of Day')
        plt.ylabel('Date')
        plt.savefig('student3_heatmap.png')
        plt.close()
        
        # Example SQL query for Student 4
        student4_query = """
        SELECT status_code, COUNT(*) as count
        FROM student2_logs
        GROUP BY status_code
        ORDER BY count DESC
        """
        df4 = spark.sql(student4_query)
        
        # Create pie chart for Student 4
        pdf4 = df4.toPandas()
        plt.figure(figsize=(10, 10))
        plt.pie(pdf4['count'], labels=pdf4['status_code'], autopct='%1.1f%%')
        plt.title('Distribution of Response Codes')
        plt.axis('equal')
        plt.savefig('student4_distribution.png')
        plt.close()
        
        print("All visualizations have been generated successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()

# %% [markdown]
# # Data Processing using PySpark RDD
# 
# ## Task 1: Basic RDD Analysis (10 marks)
# 
# Each member will create a custom function to parse and process the log entries.
# 

# %% [markdown]
# ### Student Basic Extraction Examples
# 
# - \***\*Navya Athoti** Email: u2793047@uel.ac.uk**: Extract Timestamp and IP  
#    **Description\*\*: Parse timestamp and IP address from logs.
# 

# %% [markdown]
# - **Student 2**: Extract URL and HTTP Method  
#    **Description**: Parse URL path and HTTP method from logs.
# 

# %% [markdown]
# - **Student 3**: Extract Status Code and Response Size  
#    **Description**: Parse HTTP status and response size from logs.
# 

# %% [markdown]
# - **Student 4**: Extract Log Message and IP Address  
#    **Description**: Parse log messages and corresponding IP addresses.
# 

# %%
# Initialize Spark
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

# Student 1: Extract Timestamp and IP
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

# Student 2: Extract URL and HTTP Method
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

# Student 3: Extract Status Code and Response Size
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

# Student 4: Extract Log Message and IP Address
def extract_message_ip(log_line):
    """Extract log message and IP address from log entry"""
    if isinstance(log_line, str):
        text = log_line
    else:
        text = log_line.value  # For DataFrame rows
        
    ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
    message_pattern = r'(?:Warning|Update|Debug|Error|Info):(.*?)(?:\s*$)'
    
    try:
        ip = re.search(ip_pattern, text).group(1)
        message = re.search(message_pattern, text).group(1).strip()
        return (ip, message)
    except Exception as e:
        return ("Error", "Error")

def analyze_logs(data):
    """Analyze logs using the existing Spark session"""
    print("\nProcessing log entries...")
    print("\n" + "="*50 + "\n")
    
    try:
        # Convert DataFrame to RDD of log lines
        logs_rdd = data.rdd.map(lambda row: row[0])
        
        # Student 1 Analysis
        print("STUDENT 1 - TIMESTAMP AND IP EXTRACTION")
        print("-" * 40)
        rdd1_results = logs_rdd.map(extract_timestamp_ip).collect()
        for ip, timestamp in rdd1_results[:5]:  # Show first 5 results
            print(f"IP Address: {ip}")
            print(f"Timestamp:  {timestamp}\n")

        print("\n" + "="*50 + "\n")
        
        # Student 2 Analysis
        print("STUDENT 2 - URL AND HTTP METHOD EXTRACTION")
        print("-" * 40)
        rdd2_results = logs_rdd.map(extract_url_method).collect()
        for method, url in rdd2_results[:5]:  # Show first 5 results
            print(f"HTTP Method: {method}")
            print(f"URL Path:    {url}\n")

        print("\n" + "="*50 + "\n")
        
        # Student 3 Analysis
        print("STUDENT 3 - STATUS CODE AND RESPONSE SIZE EXTRACTION")
        print("-" * 40)
        rdd3_results = logs_rdd.map(extract_status_size).collect()
        for status, size in rdd3_results[:5]:  # Show first 5 results
            print(f"Status Code:   {status}")
            print(f"Response Size: {size} bytes\n")

        print("\n" + "="*50 + "\n")
        
        # Student 4 Analysis
        print("STUDENT 4 - LOG MESSAGE AND IP EXTRACTION")
        print("-" * 40)
        rdd4_results = logs_rdd.map(extract_message_ip).collect()
        for ip, message in rdd4_results[:5]:  # Show first 5 results
            print(f"IP Address:  {ip}")
            print(f"Log Message: {message}\n")
            
    except Exception as e:
        print(f"Error during processing: {str(e)}")

# Main execution
if __name__ == "__main__":
    try:
        # Load the log file
        # Replace with your actual log file path
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
# Initialize Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import re

# Create Spark session
spark = SparkSession.builder \
    .appName("AdvancedLogAnalysis") \
    .master("local[*]") \
    .getOrCreate()

def advanced_rdd_analysis(data):
    # Convert DataFrame to RDD
    logs_rdd = data.rdd.map(lambda row: row[0])
    
    print("\nAdvanced RDD Analysis Results")
    print("=" * 50)

    # Student 1: Calculate hourly visit counts per IP
    def student1_analysis(logs_rdd):
        print("\nStudent 1 - Hourly Visit Counts per IP")
        print("-" * 40)
        
        def extract_hour_ip(log_line):
            # Updated regex patterns to be more precise
            ip_pattern = r'(\d+\.\d+\.\d+\.\d+)'
            timestamp_pattern = r'\[([\d\/\w:]+\s[+\-]\d{4})\]'
            
            try:
                ip = re.search(ip_pattern, log_line).group(1)
                timestamp_str = re.search(timestamp_pattern, log_line).group(1)
                # Parse timestamp with timezone
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

    # Student 2: Analyze URL patterns (Your existing code works fine)
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
# Initialize Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession
import re

# Create Spark session
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

        # Process and collect data
        ip_stats = logs_rdd \
            .map(extract_ip_status) \
            .filter(lambda x: x[0] != "Error") \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .collect()
        
        # Process results locally
        if ip_stats:
            # Convert to list of dictionaries with calculated stats
            results = []
            for ip, (failed, total) in ip_stats:
                failure_rate = (failed / total * 100) if total > 0 else 0
                results.append({
                    'ip': ip,
                    'failed': failed,
                    'total': total,
                    'failure_rate': failure_rate
                })
            
            # Sort by failed requests
            results.sort(key=lambda x: (-x['failed'], x['ip']))
            
            print("\nTop 10 IPs by Failed Requests:")
            for stat in results[:10]:
                print(f"IP: {stat['ip']}")
                print(f"Failed Requests: {stat['failed']}")
                print(f"Total Requests: {stat['total']}")
                print(f"Failure Rate: {stat['failure_rate']:.2f}%\n")

            # Calculate overall statistics
            total_failed = sum(stat['failed'] for stat in results)
            total_requests = sum(stat['total'] for stat in results)
            overall_rate = (total_failed / total_requests * 100) if total_requests > 0 else 0

            print("Overall Statistics:")
            print(f"Total Failed Requests: {total_failed}")
            print(f"Total Requests: {total_requests}")
            print(f"Overall Failure Rate: {overall_rate:.2f}%\n")

            # Find IPs with highest failure rates (min 10 requests)
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

# Main execution
if __name__ == "__main__":
    try:
        # Load the log file
        data = spark.read.text("web.log")
        
        # Execute the analysis
        analyze_failed_requests(data)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        # Stop SparkSession
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
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import builtins
import time
import re
import os

# Initialize Spark Session with proper configuration
def initialize_spark():
    spark = (SparkSession.builder
            .appName("LogAnalysis")
            .master("local[*]")  # Use all available cores
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.sql.execution.arrow.enabled", "true")
            .getOrCreate())
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_data(spark, path="web.log"):
    """Load and verify the data"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
        
    # Read the log file as a text file
    data = spark.read.text(path)
    print(f"Successfully loaded {data.count()} log entries")
    return data

def measure_execution_time(func):
    """Decorator to measure execution time of functions"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.2f} seconds")
        return result, execution_time
    return wrapper

class Student1Optimization:
    """Student 1: Partition Strategies and Caching"""
    
    def __init__(self, data, spark):
        self.data = data
        self.spark = spark
        
    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nStudent 1 - Baseline Analysis (No Optimization)")
        print("-" * 50)
        
        rdd = self.data.rdd.map(lambda row: row[0])
        result = rdd.map(self.extract_data).groupByKey().mapValues(len).collect()
        return result
        
    @measure_execution_time
    def optimized_with_partitioning(self):
        """Analysis with custom partitioning"""
        print("\nStudent 1 - Analysis with Custom Partitioning")
        print("-" * 50)
        
        def custom_partitioner(key):
            return builtins.sum(ord(c) for c in str(key)) % 10
            return sum(ord(c) for c in str(key)) % 10
            
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_data)
                    .partitionBy(10, custom_partitioner)
                    .groupByKey()
                    .mapValues(len)
                    .collect())
        return result
        
    @measure_execution_time
    def optimized_with_caching(self):
        """Analysis with caching"""
        print("\nStudent 1 - Analysis with Caching")
        print("-" * 50)
        
        rdd = self.data.rdd.map(lambda row: row[0])
        cached_rdd = rdd.map(self.extract_data).cache()
        result = cached_rdd.groupByKey().mapValues(len).collect()
        cached_rdd.unpersist()
        return result
        
    @staticmethod
    def extract_data(log_line):
        """Extract IP and timestamp from log line"""
        ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
        try:
            ip = re.search(ip_pattern, log_line).group(1)
            return (ip, 1)
        except:
            return ("error", 1)

class Student2Optimization:
    """Student 2: Caching and Bucketing & Indexing"""
    
    def __init__(self, data, spark):
        self.data = data
        self.spark = spark
        
    @staticmethod
    def extract_url_status(log_line):
        """Extract URL and status code from log line"""
        url_pattern = r'"(?:GET|POST|PUT|DELETE)\s+([^"\s]+)'
        status_pattern = r'HTTP/[\d.]+" (\d{3})'
        try:
            url = re.search(url_pattern, log_line).group(1)
            status = re.search(status_pattern, log_line).group(1)
            return (url, status)
        except:
            return ("error", "0")
            
    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nStudent 2 - Baseline Analysis (No Optimization)")
        print("-" * 50)
        
        def count_values(iterator):
            # Convert iterator to list and get its length
            return len(list(iterator))
            
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_url_status)
                    .groupByKey()
                    .mapValues(count_values)  # Using our custom counting function
                    .collect())
        return result
        
    @measure_execution_time
    def optimized_with_caching(self):
        """Analysis with caching"""
        print("\nStudent 2 - Analysis with Caching")
        print("-" * 50)
        
        def count_values(iterator):
            return len(list(iterator))
            
        rdd = self.data.rdd.map(lambda row: row[0])
        cached_rdd = rdd.map(self.extract_url_status).cache()
        result = (cached_rdd.groupByKey()
                          .mapValues(count_values)
                          .collect())
        cached_rdd.unpersist()
        return result
        
    @measure_execution_time
    def optimized_with_bucketing(self):
        """Analysis with bucketing"""
        print("\nStudent 2 - Analysis with Bucketing")
        print("-" * 50)
        
        # Create temporary view
        df = self.data.selectExpr("value as log_line")
        df.createOrReplaceTempView("logs")
        
        # SQL query with bucketing
        bucketed_df = self.spark.sql("""
            SELECT 
                regexp_extract(log_line, '"(?:GET|POST|PUT|DELETE)\\s+([^"\\s]+)', 1) as url,
                count(*) as count
            FROM logs
            GROUP BY url
            ORDER BY count DESC
        """)
        
        return bucketed_df.collect()
        
    @staticmethod
    def extract_url_status(log_line):
        """Extract URL and status code from log line"""
        url_pattern = r'"(?:GET|POST|PUT|DELETE)\s+([^"\s]+)'
        status_pattern = r'HTTP/[\d.]+" (\d{3})'
        try:
            url = re.search(url_pattern, log_line).group(1)
            status = re.search(status_pattern, log_line).group(1)
            return (url, status)
        except:
            return ("error", "0")

class Student3Optimization:
    """Student 3: Partition Strategies and Bucketing & Indexing"""
    
    def __init__(self, data, spark):
        self.data = data
        self.spark = spark
        
    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nStudent 3 - Baseline Analysis (No Optimization)")
        print("-" * 50)
        
        def calculate_average(iterator):
            total = 0
            count = 0
            for value in iterator:
                total += value
                count += 1
            return total / count if count > 0 else 0
            
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_url_size)
                    .groupByKey()
                    .mapValues(calculate_average)
                    .collect())
        return result
        
    @measure_execution_time
    def optimized_with_partitioning(self):
        """Analysis with custom partitioning"""
        print("\nStudent 3 - Analysis with Custom Partitioning")
        print("-" * 50)
        
        def url_partitioner(url):
            return hash(str(url)) % 8
            
        def calculate_average(iterator):
            total = 0
            count = 0
            for value in iterator:
                total += value
                count += 1
            return total / count if count > 0 else 0
            
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_url_size)
                    .partitionBy(8, url_partitioner)
                    .groupByKey()
                    .mapValues(calculate_average)
                    .collect())
        return result
        
    @measure_execution_time
    def optimized_with_bucketing(self):
        """Analysis with bucketing"""
        print("\nStudent 3 - Analysis with Bucketing")
        print("-" * 50)
        
        # Create temporary view
        df = self.data.selectExpr("value as log_line")
        df.createOrReplaceTempView("logs")
        
        # SQL query with bucketing
        bucketed_df = self.spark.sql("""
            SELECT 
                regexp_extract(log_line, '"(?:GET|POST|PUT|DELETE)\\s+([^"\\s]+)', 1) as url,
                AVG(CAST(regexp_extract(log_line, 'HTTP/[\\d.]+" \\d{3} (\\d+)', 1) AS INT)) as avg_size
            FROM logs
            GROUP BY url
            HAVING url != 'error'
            ORDER BY avg_size DESC
        """)
        
        return bucketed_df.collect()
        
    @staticmethod
    def extract_url_size(log_line):
        """Extract URL and response size from log line"""
        url_pattern = r'"(?:GET|POST|PUT|DELETE)\s+([^"\s]+)'
        size_pattern = r'HTTP/[\d.]+" \d{3} (\d+)'
        try:
            url = re.search(url_pattern, log_line).group(1)
            size = int(re.search(size_pattern, log_line).group(1))
            return (url, size)
        except:
            return ("error", 0)

class Student4Optimization:
    """Student 4: Caching and Partition Strategies"""
    
    def __init__(self, data, spark):
        self.data = data
        self.spark = spark
        
    @measure_execution_time
    def baseline_analysis(self):
        """Baseline analysis without optimizations"""
        print("\nStudent 4 - Baseline Analysis (No Optimization)")
        print("-" * 50)
        
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_ip_status)
                    .groupByKey()
                    .mapValues(lambda x: sum(1 for status in x if int(status) >= 400))
                    .collect())
        return result
        
    @measure_execution_time
    def optimized_with_caching(self):
        """Analysis with caching"""
        print("\nStudent 4 - Analysis with Caching")
        print("-" * 50)
        
        rdd = self.data.rdd.map(lambda row: row[0])
        cached_rdd = rdd.map(self.extract_ip_status).cache()
        result = (cached_rdd.groupByKey()
                          .mapValues(lambda x: sum(1 for status in x if int(status) >= 400))
                          .collect())
        cached_rdd.unpersist()
        return result
        
    @measure_execution_time
    def optimized_with_partitioning(self):
        """Analysis with custom partitioning"""
        print("\nStudent 4 - Analysis with Custom Partitioning")
        print("-" * 50)
        
        def ip_partitioner(ip):
            try:
                first_octet = sum(ord(c) for c in str(ip.split('.')[0]))
                return first_octet % 8
            except:
                return 0
            
        rdd = self.data.rdd.map(lambda row: row[0])
        result = (rdd.map(self.extract_ip_status)
                    .partitionBy(8, ip_partitioner)
                    .groupByKey()
                    .mapValues(lambda x: sum(1 for status in x if int(status) >= 400))
                    .collect())
        return result
        
    @staticmethod
    def extract_ip_status(log_line):
        """Extract IP and status code from log line"""
        ip_pattern = r'^(\d+\.\d+\.\d+\.\d+)'
        status_pattern = r'HTTP/[\d.]+" (\d{3})'
        try:
            ip = re.search(ip_pattern, log_line).group(1)
            status = re.search(status_pattern, log_line).group(1)
            return (ip, status)
        except:
            return ("error", "0")

def run_all_optimizations(data, spark):
    """Run all students' optimization analyses"""
    
    print("\nRunning Optimization Analyses for All Students")
    print("=" * 60)
    
    # Student 1
    try:
        print("\nStudent 1 - Partition Strategies and Caching")
        print("=" * 40)
        s1 = Student1Optimization(data, spark)
        baseline1, time1 = s1.baseline_analysis()
        partition1, time2 = s1.optimized_with_partitioning()
        cache1, time3 = s1.optimized_with_caching()
        print(f"\nPerformance Improvement:")
        print(f"Partitioning: {((time1 - time2)/time1)*100:.2f}% faster")
        print(f"Caching: {((time1 - time3)/time1)*100:.2f}% faster")
    except Exception as e:
        print(f"Error in Student 1 analysis: {str(e)}")
    
    # Student 2
    try:
        print("\nStudent 2 - Caching and Bucketing")
        print("=" * 40)
        s2 = Student2Optimization(data, spark)
        baseline2, time1 = s2.baseline_analysis()
        cache2, time2 = s2.optimized_with_caching()
        bucket2, time3 = s2.optimized_with_bucketing()
        print(f"\nPerformance Improvement:")
        print(f"Caching: {((time1 - time2)/time1)*100:.2f}% faster")
        print(f"Bucketing: {((time1 - time3)/time1)*100:.2f}% faster")
    except Exception as e:
        print(f"Error in Student 2 analysis: {str(e)}")
    
    # Student 3
    try:
        print("\nStudent 3 - Partition Strategies and Bucketing")
        print("=" * 40)
        s3 = Student3Optimization(data, spark)
        baseline3, time1 = s3.baseline_analysis()
        partition3, time2 = s3.optimized_with_partitioning()
        bucket3, time3 = s3.optimized_with_bucketing()
        print(f"\nPerformance Improvement:")
        print(f"Partitioning: {((time1 - time2)/time1)*100:.2f}% faster")
        print(f"Bucketing: {((time1 - time3)/time1)*100:.2f}% faster")
    except Exception as e:
        print(f"Error in Student 3 analysis: {str(e)}")
    
    # Student 4
    try:
        print("\nStudent 4 - Caching and Partition Strategies")
        print("=" * 40)
        s4 = Student4Optimization(data, spark)
        baseline4, time1 = s4.baseline_analysis()
        cache4, time2 = s4.optimized_with_caching()
        partition4, time3 = s4.optimized_with_partitioning()
        print(f"\nPerformance Improvement:")
        print(f"Caching: {((time1 - time2)/time1)*100:.2f}% faster")
        print(f"Partitioning: {((time1 - time3)/time1)*100:.2f}% faster")
    except Exception as e:
        print(f"Error in Student 4 analysis: {str(e)}")

def main():
    try:
        # Initialize Spark
        spark = initialize_spark()
        
        try:
            # Load data
            data = load_data(spark)
            
            # Run optimizations
            run_all_optimizations(data, spark)
            
        except FileNotFoundError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error during analysis: {str(e)}")
        finally:
            # Clean up
            spark.stop()
            
    except Exception as e:
        print(f"Error initializing Spark: {str(e)}")

if __name__ == "__main__":
    main()

# %%
import nbformat
from nbconvert import HTMLExporter
import os

def convert_notebook_to_html(notebook_path, output_path=None):
  
    try:
        # Read the notebook
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = nbformat.read(f, as_version=4)
        
        # Create HTML exporter
        html_exporter = HTMLExporter()
        html_exporter.template_name = 'classic'  # Use classic template
        
        # Convert notebook to HTML
        body, _ = html_exporter.from_notebook_node(notebook)
        
        # Determine output path
        if output_path is None:
            output_path = os.path.splitext(notebook_path)[0] + '.html'
        
        # Write HTML to file
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
    """
    Convert all .ipynb files in a directory to HTML format.
    
    Parameters:
    directory_path (str): Path to directory containing notebooks
    output_directory (str): Optional path for output HTML files
    
    Returns:
    list: Paths to all generated HTML files
    """
    try:
        # Create output directory if it doesn't exist
        if output_directory and not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        converted_files = []
        
        # Iterate through all files in directory
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
    notebook_path = "CN7031_Group126_2024_Draft.ipynb"
    html_path = convert_notebook_to_html(notebook_path)



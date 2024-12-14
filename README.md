# Data Processing using PySpark

## Task 1: DataFrame Creation with REGEX (10 marks)
Each member will define a custom schema using REGEX to extract specific metrics from the dataset.

### Student Metrics to Extract
- **Student 1**: IP Address, Timestamp, HTTP Method  
    **REGEX Example**: `(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)`
- **Student 2**: HTTP Status Code, Response Size, Timestamp  
    **REGEX Example**: `\".*\" (\d+) (\d+) \[(.*?)\]`
- **Student 3**: URL Path, IP Address, Response Size  
    **REGEX Example**: `\"[A-Z]+ (\/.*?) HTTP.* (\d+\.\d+\.\d+\.\d+) (\d+)`
- **Student 4**: Log Message, HTTP Status Code, Timestamp  
    **REGEX Example**: `\".*\" (\d+) .* \[(.*?)\] (.*)`

## Task 2: Two Advanced DataFrame Analysis (20 marks)
Each member will write unique SQL queries for the analysis:

### SQL Query 1: Window Functions
- **Student 1**: Rolling hourly traffic per IP  
    **Description**: Calculate traffic count per IP over a sliding window.
- **Student 2**: Session identification  
    **Description**: Identify sessions based on timestamp gaps.
- **Student 3**: Unique visitors per hour  
    **Description**: Count distinct IPs for each hour.
- **Student 4**: Average response size per status code  
    **Description**: Compute averages grouped by status codes.

### SQL Query 2: Aggregation Functions
- **Student 1**: Traffic patterns by URL path  
    **Description**: Analyze URL visits by hour.
- **Student 2**: Top 10 failed requests by size  
    **Description**: Identify the largest failed requests.
- **Student 3**: Response size distribution by status  
    **Description**: Show min, max, and avg sizes for each status.
- **Student 4**: Daily unique visitors  
    **Description**: Count unique IPs per day.

## Task 3: Data Visualization (10 marks)
Each member will visualize the results of their unique SQL queries using different chart types.

### Student Visualization Type Examples
- **Student 1**: Line Chart (Hourly Traffic)  
    **Tool**: Matplotlib for rolling traffic visualization.
- **Student 2**: Bar Chart (Top 10 Failed Requests)  
    **Tool**: Seaborn for aggregated failure counts.
- **Student 3**: Heatmap (Hourly Unique Visitors)  
    **Tool**: Seaborn for visualizing traffic density.
- **Student 4**: Pie Chart (Response Code Distribution)  
    **Tool**: Matplotlib for status code proportions.

# Data Processing using PySpark RDD

## Task 1: Basic RDD Analysis (10 marks)
Each member will create a custom function to parse and process the log entries.

### Student Basic Extraction Examples
- **Student 1**: Extract Timestamp and IP  
    **Description**: Parse timestamp and IP address from logs.
- **Student 2**: Extract URL and HTTP Method  
    **Description**: Parse URL path and HTTP method from logs.
- **Student 3**: Extract Status Code and Response Size  
    **Description**: Parse HTTP status and response size from logs.
- **Student 4**: Extract Log Message and IP Address  
    **Description**: Parse log messages and corresponding IP addresses.

## Task 2: Two Advanced RDD Analysis (30 marks)
Each member will perform unique advanced processing tasks.

### Student Advanced Analysis Examples
- **Student 1**: Calculate hourly visit counts per IP  
    **Description**: Count visits grouped by hour and IP.
- **Student 2**: Identify top 10 URLs by visit count  
    **Description**: Aggregate visit counts and rank top URLs.
- **Student 3**: Find average response size per URL  
    **Description**: Compute average response size for each URL.
- **Student 4**: Detect failed requests per IP  
    **Description**: Identify IPs with the most failed requests.

## Optimization and LSEPI Considerations (10 marks)
Each member chooses two unique methods for optimization.

### Student Optimization Methods
- **Student 1**: Partition Strategies, Caching
- **Student 2**: Caching, Bucketing & Indexing
- **Student 3**: Partition Strategies, Bucketing & Indexing
- **Student 4**: Caching, Partition Strategies

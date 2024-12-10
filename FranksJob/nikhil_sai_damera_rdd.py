# Task 1: Basic RDD Analysis by Nikhil Sai Damera
from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext()

# Sample data
data = [
    "127.0.0.1 - - [10/Dec/2024:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326",
]

# Parse logs
def parse_log(line):
    fields = line.split(" ")
    return fields[0], fields[3], fields[5]

rdd = sc.parallelize(data)
parsed_rdd = rdd.map(parse_log)
parsed_rdd.foreach(print)

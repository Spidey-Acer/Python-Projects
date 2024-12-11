
from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext()

# Load data into RDD
logs_rdd = sc.textFile("weblogs.txt")

# Parse log entries
def parse_log(line):
    fields = line.split()
    return fields[0], fields[3], fields[5]

parsed_logs = logs_rdd.map(parse_log)

# Sample action to trigger execution
print(parsed_logs.take(5))

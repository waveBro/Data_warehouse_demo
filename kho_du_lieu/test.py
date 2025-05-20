import os
from pyspark.sql import SparkSession

# Set Java Home (MUST match your actual JDK path!)
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-11.0.15'  # ← Fix backslashes
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 4g pyspark-shell'

# Initialize Spark
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Test read (ensure file exists)
df = spark.read.csv("UniversalBank.csv", header=True, inferSchema=True)
df.show(5)
print("Success!")
import pymysql
from pyspark.sql import SparkSession


try:    
    spark = (SparkSession \
             .builder.appName("Write_data") \
             .config("spark.hadoop.hadoop.home.dir", "C:/ProgramF iles/hadoop-3.4.1") \
             .config("spark.jars","C:/Program Files/Java/jdk-11/lib/mysql-connector-java-8.0.29.jar") \
             .getOrCreate() 
            )
    # pour data into mysql
    # data is parquet file
    df = spark.read.parquet("C:/STAGING AREA/part-00000-f8ec22e9-8cd8-4ea7-b686-4728fc02d3b7-c000.snappy.parquet")
    url = "jdbc:mysql://localhost:3306/Test"
    table_name = "Test"
    properties = {
        'user': 'root',
        'password': '123456',
        'driver': 'com.mysql.cj.jdbc.Driver' 
    }
    df.write.jdbc(url = url, table = table_name, mode="append", properties = properties)

except Exception as e:
    print(f"Error {e}")

finally:
    spark.stop()
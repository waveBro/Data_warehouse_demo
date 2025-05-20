from pyspark.sql import SparkSession
import pandas as pd

# import data from data souce into mysql staging area
try:
    spark = SparkSession \
            .builder.appName("Spark_test") \
            .config("spark.hadoop.hadoop.home.dir", "C:/ProgramF iles/hadoop-3.4.1") \
            .config("spark.jars","C:/Program Files/Java/jdk-11/lib/mysql-connector-java-8.0.29.jar")\
            .getOrCreate()

    giang_vien = spark.read.option("header", True).option("inferSchema", True).csv("giangvien1.csv")

    output_path = "C:/STAGING AREA"
    #giang_vien.write.mode("append").parquet(output_path)
        # overwrite --> delete all file at target foler 

    # write data from giangvien to mysql
    url = "jdbc:mysql://localhost:3306/Test"
    table_name = "giangvien"
    # dictionary hold user, password
    properties = {
        "user" : "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver" # class Name of  JDBC driver -> 
    }
    giang_vien.write.jdbc(url = url,table = table_name, mode="append", properties=properties)
    
except Exception as e:
    print(f"Error{str(e)}")  #
#spark = SparkSession.builder.appName("spark_test").config("spark.hadoop.hadoop.home.dir", "C:/Program Files/hadoop-3.4.1").getOrCreate()


finally:
    spark.stop()
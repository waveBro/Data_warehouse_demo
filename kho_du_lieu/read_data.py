from pyspark.sql import SparkSession
import pymysql
import os
import json

# create database connection
def get_db_connection():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',
        database='Test'
    )
    return conn

try :
    spark = SparkSession.builder \
        .appName("read_file") \
        .config("spark.hadoop.hadoop.home.dir","C:/Program Files/hadoop-3.4.1") \
        .getOrCreate()

    # read parquet file
    df = spark.read.parquet("C:/STAGING AREA/part-00000-f8ec22e9-8cd8-4ea7-b686-4728fc02d3b7-c000.snappy.parquet")
    columns =  df.columns
    print(df.count())
    """
    #create database in mysql with columns
    conn = get_db_connection()
    cursor = conn.cursor()
    
    
    table_name = "Test"
    statement = ', '.join([f"`{col}` varchar(20)" for col in columns])
    create_table = f"Create table `{table_name}` ({statement});"
    cursor.execute(create_table)
    conn.commit()"""
    
except Exception as e:
    print(f"Error: {str(e)}")

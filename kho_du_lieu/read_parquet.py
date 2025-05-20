from pyspark.sql import SparkSession
import pymysql


def get_db_connection():
    conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='Test'
    )
    return conn
    

try:
    spark = SparkSession.builder.appName("read_file").config("spark.hadoop.hadoop.home.dir","C:/ProgramF iles/hadoop-3.4.1").getOrCreate()
    
    df = spark.read.parquet("C:/STAGING AREA/part-00000-917ae792-3f69-4d7f-b42b-1edd2fa92903-c000.snappy.parquet")
    cl = df.columns
    # create database in mysql
    conn = get_db_connection()
    ex = conn.cursor()
    table_name = "giangvien"
    statement = ", ".join([f"`{col}` varchar(15)" for col in cl])
    # for col in cl -> return value of cl array
    # f" '{col}' varchar"  -> return "name_col varchar" 
    #", ".join --> join all string by ","
    create_table = f"Create table `{table_name}` ({statement});"
    ex.execute(create_table)
    conn.commit()
except Exception as e: 
    print(f"{str(e)}")


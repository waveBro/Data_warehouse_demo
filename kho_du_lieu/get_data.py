import requests
import pandas as pd
from pyspark.sql import SparkSession
import pymysql
import os
import json

apikey = 'EON2H46TBWWTELG3'
url = 'https://www.alphavantage.co/query'
# outputsize=full: get all data, datatype=json: get data in json format

param = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'IBM',
    'interval': '5min',
    'apikey': apikey,
    'outputsize': 'full',
    'datatype': 'json'
}
# request class will be read http url -> get data from url
# requests will read query parameter and return data in json format
response = requests.get(url, params=param) 
# response is object contain data from url
# data we will get from response.json is dictionary


data = response.json() # convert data to json format
# print all data
#print(data) 
# convert data to csv format
df = pd.DataFrame(data)
# convert data to parquet format

spark = (SparkSession.builder
    .appName("get_data")
    .config("spark.hadoop.hadoop.home.dir", "C:/Program Files/hadoop-3.4.1") # this is path of hadoop folder to connect to hadoop
    .config("spark.jars","C:/Program Files/Java/jdk-11/lib/mysql-connector-java-8.0.29.jar")  # this is path of mysql connector jar file to connect to mysql
    .getOrCreate()
)

# convert dic to list
# declare empty list
record = []  # save dic from data["Time Series (5min)"] 

for key, value in data["Time Series (5min)"].items():  # data["Time Series (5min)"] is a dictonary and .items() -> return tuple (key, value)
    value["timestamp"] = key   # value is a pointer to the dictonary in data["Time Series (5min)"]
    record.append(value) 
 


# convert dictonary to json file   (way 1)
with open("data.json", "w") as f:    # with is context manager -> help open temporary resource and close it after using
    json.dump(record, f, indent=None, separators=(',', ':')) 
# Spark just read json file


# save df to parquet format into staging area
parquet_df = spark.read.option("inferSchema", True).json("data.json", multiLine = True) # mutilTrue : read json with multiple lines
store_path = "C:/STAGING AREA"

# save data to parquet format into Staging area
parquet_df.write.mode("overwrite").parquet(store_path)






'''
# Instead, you can cache or save the parsed results and then send the same query.
df = spark.read.schema("timestamp STRING, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INT").json("data.json")
# filter corrupt data
df.filter(df["timestamp"].isNotNull()) # filter out all rows that have null value in timestamp column

'''

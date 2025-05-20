from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, trim, abs, lit, to_date

#1.object spark
spark = (SparkSession.builder \
    .appName('Process_Sp_data') \
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    #.config("spark.local.dir", "F:/spark-temp") \
    .getOrCreate()
)
# option2 : config("spark.jars.packages", org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.5)  # package Maven
#spark = SparkSession.builder.appName('Process_Sp_data').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.5").getOrCreate()


#2.read data from kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "SuperstoreData") \
    .load()
# .option('kafka.bootstrap.servers', 'localhost:9092')  -> địa chỉ Kafka server (spark connect to)
#.option("subscribe", "SuperstoreData") --> Subscribe to read data from topic = SuperstoreData


#3.convert data from kafka(json) -> Datafram 
# "CAST(value AS STRING) as json" -> cast  value(byte) -> string -> as json
# from_json(col("json"))  -> convert string json into Struct 
# col("json") : cột chứa string json  (json: ex: {"Row ID": 1,"Order ID": "CA-2011-100006","Order Date": "11/8/2011")
df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), """
        `Row ID` STRING,
        `Order ID` STRING,
        `Order Date` STRING,
        `Ship Date` STRING,
        `Ship Mode` STRING,
        `Customer ID` STRING,
        `Customer Name` STRING,
        `Segment` STRING,
        `City` STRING,
        `State` STRING,
        `Country` STRING,
        `Postal Code` STRING,
        `Market` STRING,
        `Region` STRING,
        `Product ID` STRING,
        `Category` STRING,
        `Sub-Category` STRING,
        `Product Name` STRING,
        `Sales` STRING,
        `Quantity` STRING,
        `Discount` STRING,
        `Profit` STRING,
        `Shipping Cost` STRING,
        `Order Priority` STRING
""").alias("data")) \
    .select("data.*")
# select("data.*") --> data.* with '*' là các trường của Struct 

# Cast datatype for each numeric column 
df = df.withColumn("Row ID", col("Row ID").cast("int")) \
       .withColumn("Sales", col("Sales").cast("double")) \
       .withColumn("Profit", col("Profit").cast("double")) \
       .withColumn("Discount", col("Discount").cast("double")) \
       .withColumn("Shipping Cost", col("Shipping Cost").cast("double")) \
       .withColumn("Quantity", col("Quantity").cast("int"))


#4. Handle Null value (String)
string_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Customer Name", 
                  "Segment", "City", "State", "Country","Postal Code", "Market", "Region", "Product ID", 
                  "Category", "Sub-Category", "Product Name", "Order Priority"]

number_columns = ["Sales", "Profit", "Discount", "Shipping Cost", "Quantity"]

for column in string_columns:
    df = df.withColumn(
        column,
        when(col(column).isNull() | (trim(col(column)) == ''), lit("NULL")).otherwise(col(column))
    )
# withColumns : tạo lại column with name = column
#when(col(column).isNull(), 'NULL')  --> if value of column = Null 0 -> change = "NULL"
# otherwise : giữ nguyên 

#4.1 Handle with number
df = df.fillna(0, subset=number_columns)
df = df.withColumn("Sales", when(col("Sales") < 0, 0).otherwise(col("Sales")))
df = df.withColumn("Profit", abs(col("Profit")))

# 8. Xử lý định dạng ngày (chuyển chuỗi thành kiểu ngày)
df = df.withColumn("Order Date", to_date(col("Order Date"), "d-M-yyyy")) \
       .withColumn("Ship Date", to_date(col("Ship Date"), "d/M/yyyy"))

# Write data into Staging area
#path = "G:/Kho Du Lieu/STAGING AREA"
#query = df.writeStream.outputMode("append").format("json").option("path", f"{path}/processed_superstore_data").option("checkpointLocation", f"{path}/checkpoint_superstore").start()
#query.awaitTermination()

#5.1 Write data into Staging Area
# set config -> connect to  Minio Server
# spark.sparkContext : core object to connect with cluster,v.v
# when use sparkContext : khi muốn cấu hình sâu hơn cho Spark (Hadoop, S3, MiniO, ...)
# Below : config to write, read with MiniO(S3)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://26.109.187.218:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# -> SparkContext là cầu nối -> access each feature of Spark core
query = df.writeStream \
    .outputMode("append")\
    .format("json")\
    .option("path", "s3a://superstoredata/processed")\
    .option("checkpointLocation", "checkpoint")\
    .start()

query.awaitTermination()  # keep spark never stop 
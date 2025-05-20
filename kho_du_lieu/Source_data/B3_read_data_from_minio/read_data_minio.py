from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

#declare sparkSesson object
spark = SparkSession.builder \
    .appName("read_data_from_Minio") \
    .getOrCreate()

# defind dataframe(spark) schema for SuperstoreData
schema = StructType([ # StructType -> define a list of field (columns)
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Postal Code", IntegerType(), True),
    StructField("Market", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Shipping Cost", DoubleType(), True),
    StructField("Order Priority", StringType(), True)    
])

#B1.read and assign (from bucket of Minio)    (use s3 protocol (Hadoop's S3 connector)
# schema is defined -> when read data -> each field is pushed into schema
df = spark.read.schema(schema).json("s3a://superstoredata/processed/*.json")

#B3: Storage to Staging area (Minio's bucket)
df.write \
    .mode("overwrite") \
    .option("header", "True") \
    .csv("s3a://staging-area/staging")

spark.stop()

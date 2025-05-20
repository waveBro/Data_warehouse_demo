from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, monotonically_increasing_id, year, month, dayofmonth, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("staging_to_dwh") \
    .getOrCreate()

# Read csv file from staging
df = spark.read.csv("s3a://staging-area/staging", header=True, inferSchema=True)

# MySQL connection properties
mysql_url = "jdbc:mysql://localhost:3306/data_warehouse?useSSL=false"
mysql_properties = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# dim_date
dim_date = df.select(
    to_date(col("Order Date"), "yyyy-MM-dd").alias("full_date"),
    year(col("Order Date")).alias("year"),
    month(col("Order Date")).alias("month"),
    dayofmonth(col("Order Date")).alias("day"),
    lit("Order").alias("order_type")
).union(
    df.select(
        to_date(col("Ship Date"), "yyyy-MM-dd").alias("full_date"),
        year(col("Ship Date")).alias("year"),
        month(col("Ship Date")).alias("month"),
        dayofmonth(col("Ship Date")).alias("day"),
        lit("Ship").alias("order_type")
    )
).distinct()
dim_date = dim_date.withColumn("date_id", monotonically_increasing_id() + 1)
dim_date.write.jdbc(
    url=mysql_url,
    table="dim_date",
    mode="overwrite",
    properties=mysql_properties
)
# dim_product (distinct on product_id)
window_spec_product = Window.partitionBy("Product ID").orderBy(col("Order Date").desc())
dim_product = df.withColumn("row_num", row_number().over(window_spec_product)).filter(col("row_num") == 1).select(
    col("Product ID").alias("product_id"),
    col("Product Name").alias("product_name"),
    col("Category").alias("category"),
    col("Sub-Category").alias("sub_category")
).drop("row_num")
dim_product.write.jdbc(
    url=mysql_url,
    table="dim_product",
    mode="overwrite",
    properties=mysql_properties
)

# dim_customer (distinct on customer_id)
window_spec_customer = Window.partitionBy("Customer ID").orderBy(col("Order Date").desc())
dim_customer = df.withColumn("row_num", row_number().over(window_spec_customer)).filter(col("row_num") == 1).select(
    col("Customer ID").alias("customer_id"),
    col("Customer Name").alias("customer_name"),
    col("Segment").alias("segment")
).drop("row_num").filter(col("customer_id").isNotNull())
dim_customer.write.jdbc(
    url=mysql_url,
    table="dim_customer",
    mode="overwrite",
    properties=mysql_properties
)



# dim_location
dim_location = df.select(
    col("City").alias("city"),
    col("State").alias("state"),
    col("Country").alias("country"),
    col("Postal Code").cast("int").alias("postal_code"),
    col("Market").alias("market"),
    col("Region").alias("region")
).distinct()
dim_location = dim_location.withColumn("location_id", monotonically_increasing_id() + 1)
dim_location.write.jdbc(
    url=mysql_url,
    table="dim_location",
    mode="overwrite",
    properties=mysql_properties
)

# dim_order (distinct on order_id)
window_spec_order = Window.partitionBy("Order ID").orderBy(col("Order Date").desc())
dim_order = df.withColumn("row_num", row_number().over(window_spec_order)).filter(col("row_num") == 1).select(
    col("Order ID").alias("order_id"),
    col("Order Priority").alias("order_priority"),
    col("Ship Mode").alias("ship_mode")
).drop("row_num").filter(col("order_id").isNotNull())
dim_order.write.jdbc(
    url=mysql_url,
    table="dim_order",
    mode="overwrite",
    properties=mysql_properties
)

# fact_sales
fact_sales = df.join(
    dim_date,
    (to_date(df["Order Date"], "yyyy-MM-dd") == dim_date["full_date"]) & (dim_date["order_type"] == "Order"),
    "left"
).join(
    dim_customer,
    df["Customer ID"] == dim_customer["customer_id"],
    "left"
).join(
    dim_product,
    df["Product ID"] == dim_product["product_id"],
    "left"
).join(
    dim_location,
    (df["City"] == dim_location["city"]) &
    (df["State"] == dim_location["state"]) &
    (df["Country"] == dim_location["country"]) &
    (df["Postal Code"].cast("int") == dim_location["postal_code"]) &
    (df["Market"] == dim_location["market"]) &
    (df["Region"] == dim_location["region"]),
    "left"
).join(
    dim_order,
    df["Order ID"] == dim_order["order_id"],
    "left"
)

fact_sales = fact_sales.select(
    dim_date["date_id"].alias("date_id"),
    dim_customer["customer_id"].alias("customer_id"),
    dim_product["product_id"].alias("product_id"),
    dim_location["location_id"].alias("location_id"),
    dim_order["order_id"].alias("order_id"),
    col("Quantity").cast("int").alias("quantity"),
    col("Discount").cast("double").alias("discount"),
    col("Profit").cast("double").alias("profit"),
    col("Shipping Cost").cast("double").alias("shipping_cost"),
    col("Sales").cast("double").alias("sales")
).filter(col("order_id").isNotNull())
fact_sales.write.jdbc(
    url=mysql_url,
    table="fact_sales",
    mode="overwrite",
    properties=mysql_properties
)

# Stop Spark session
spark.stop()
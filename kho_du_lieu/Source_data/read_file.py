from pyspark.sql import SparkSession
import pymysql

spark = (
    SparkSession.builder \
    .appName("read_file_csv") \
    .config("spark.jars","C:/Program Files/Java/jdk-11/lib/mysql-connector-java-8.0.29.jar") \
    .config("spark.local.dir", "F:/spark-temp") # where spark will store temporary files 
    .getOrCreate()
)
my_sql_url = "jdbc:mysql://localhost:3306/Test"  # standard format : jdbc:mysql://<host>:<port>/databaese_name
my_sql_properties = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"  # class Name of  JDBC driver ->
}
def get_db_connection():
    conn = pymysql.connect(
        host='localhost',
        user= 'root',
        password='123456',
        database='Test'
    )
    return conn 

def create_src_database():
    try:
        conn = get_db_connection()
        ex = conn.cursor()
        ex.execute("""
            CREATE TABLE IF NOT EXISTS Customers (
                Customer_ID VARCHAR(20) PRIMARY KEY,
                Customer_Name VARCHAR(100) NOT NULL,
                Segment VARCHAR(50)
            )
            """)
        ex.execute("""
            CREATE TABLE IF NOT EXISTS Customer_Locations (
                Customer_ID VARCHAR(20) PRIMARY KEY,
                City VARCHAR(50),
                State VARCHAR(50),
                Country VARCHAR(50),
                Postal_Code VARCHAR(10),
                Market VARCHAR(50),
                Region VARCHAR(50),
                FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
            )
            """)
        ex.execute("""
            CREATE TABLE IF NOT EXISTS Products (
                Product_ID VARCHAR(20) PRIMARY KEY,
                Product_Name VARCHAR(100) NOT NULL,
                Category VARCHAR(50),
                Sub_Category VARCHAR(50)
            )   
        """)
        ex.execute("""
            CREATE TABLE IF NOT EXISTS Orders (
                Order_ID VARCHAR(20) PRIMARY KEY,
                Order_Date DATE,
                Ship_Date DATE,
                Ship_Mode VARCHAR(50),
                Customer_ID VARCHAR(20),
                Order_Priority VARCHAR(20),
                FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
            )
        """)
        ex.execute("""
            CREATE TABLE IF NOT EXISTS Order_Details (
                Row_ID INT PRIMARY KEY,
                Order_ID VARCHAR(20),
                Product_ID VARCHAR(20),
                Quantity INT,
                Sales DECIMAL(10, 2),
                Discount DECIMAL(5, 2),
                Profit DECIMAL(10, 2),
                Shipping_Cost DECIMAL(10, 2),
                FOREIGN KEY (Order_ID) REFERENCES Orders(Order_ID),
                FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error: {str(e)}")



try: 
    # Read csv file
    df = spark.read.option("header", True).option("inferSchema", True).csv("G:/Kho Du Lieu/superstore_dataset2011-2015.csv")
    cl = df.columns


    # Transform data to each table (through method chaining)
    # get column in Table
    src_customer = df.select("Customer ID", "Customer Name", "Segment").distinct().filter(df["Customer ID"].isNotNull())
    src_customer_location = df.select("Customer ID", "City", "State", "Country", "Postal Code", "Market", "Region").distinct().filter(df["Customer ID"].isNotNull())
    src_product = df.select("Product ID", "Product Name", "Category", "Sub-Category").distinct().filter(df["Product ID"].isNotNull())
    src_order = df.select("Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Order Priority").distinct().filter(df["Order ID"].isNotNull())
    src_order_detail = df.select("Row ID", "Order ID", "Product ID","Quantity", "Sales", "Discount", "Profit", "Shipping Cost").distinct().filter(df["Row ID"].isNotNull() & df["Product ID"].isNotNull())
        
    # write.jdbc data into mysql
    src_customer.write.jdbc(url = my_sql_url,table = "Customers", mode = "append", properties = my_sql_properties)
    src_customer_location.write.jdbc(url = my_sql_url,table = "Customer_Locations", mode = "append", properties = my_sql_properties)
    src_product.write.jdbc(url = my_sql_url,table = "Products", mode = "append", properties = my_sql_properties)
    src_order.write.jdbc(url = my_sql_url,table = "Orders", mode = "append", properties = my_sql_properties)
    src_order_detail.write.jdbc(url = my_sql_url,table = "Order_Details", mode = "append", properties = my_sql_properties)
    
except Exception as e:  
    print(f"Error: {str(e)}")


#spark.stop()  # to delete all temporary files in spark local dir


import pymysql

def get_db_connection():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',
        database='data_warehouse'
    )
    return conn

# Kết nối đến MySQL
conn = get_db_connection()
cursor = conn.cursor()

# Tạo bảng dim_Date
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_Date (
        date_id INT PRIMARY KEY AUTO_INCREMENT,
        full_date DATE,
        year INT,
        month INT,
        day INT,
        order_type ENUM('Order', 'Ship')
    )
""")

# Tạo bảng dim_Product
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_Product (
        product_id VARCHAR(50) PRIMARY KEY,
        product_name VARCHAR(200),
        category VARCHAR(50),
        sub_category VARCHAR(50)
    )
""")

# Tạo bảng dim_Customer
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_Customer (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_name VARCHAR(100),
        segment VARCHAR(50)
    )
""")

# Tạo bảng dim_Order
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_Order (
        order_id VARCHAR(50) PRIMARY KEY,
        order_priority VARCHAR(50),
        ship_mode VARCHAR(20)
    )
""")


# Tạo bảng dim_Location
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_Location (
        location_id INT PRIMARY KEY AUTO_INCREMENT,
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        postal_code INT,
        market VARCHAR(50),
        region VARCHAR(50)
    )
""")

# Tạo bảng fact_Sales
cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_Sales (
        fact_id INT PRIMARY KEY AUTO_INCREMENT,
        date_id INT,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        location_id INT,
        order_id VARCHAR(50),
        quantity INT,
        discount DOUBLE,
        profit DOUBLE,
        shipping_cost DOUBLE,
        sales DOUBLE,
        FOREIGN KEY (date_id) REFERENCES dim_Date(date_id),
        FOREIGN KEY (customer_id) REFERENCES dim_Customer(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_Product(product_id),
        FOREIGN KEY (location_id) REFERENCES dim_Location(location_id),
        FOREIGN KEY (order_id) REFERENCES dim_Order(order_id)
    )
""")

# Commit các thay đổi và đóng kết nối
conn.commit()
cursor.close()
conn.close()


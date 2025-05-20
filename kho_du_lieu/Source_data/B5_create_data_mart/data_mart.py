import pymysql

def get_db_connection(database_name):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',
        database=database_name
    )
    return conn

# Kết nối đến MySQL để tạo database mới
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456'
)
cursor = conn.cursor()

# Tạo database data_mart
cursor.execute("CREATE DATABASE IF NOT EXISTS data_mart")
cursor.close()
conn.close()

# Kết nối đến database data_mart để tạo bảng
conn_data_mart = get_db_connection('data_mart')
cursor_data_mart = conn_data_mart.cursor()

# Tạo bảng dm_sales_summary trong database data_mart
cursor_data_mart.execute("""
    CREATE TABLE IF NOT EXISTS dm_sales_summary (
        id INT PRIMARY KEY AUTO_INCREMENT,
        date_id INT,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        category VARCHAR(50),
        segment VARCHAR(50),
        quantity INT,
        sales DOUBLE,
        profit DOUBLE
    )
""")

# Kết nối đến database data_warehouse để truy vấn dữ liệu
conn_warehouse = get_db_connection('data_warehouse')
cursor_warehouse = conn_warehouse.cursor()

# Xóa dữ liệu cũ trong bảng dm_sales_summary (nếu có)
cursor_data_mart.execute("TRUNCATE TABLE data_mart.dm_sales_summary")

# Chèn dữ liệu vào dm_sales_summary từ data_warehouse
cursor_warehouse.execute("""
    INSERT INTO data_mart.dm_sales_summary (date_id, customer_id, product_id, category, segment, quantity, sales, profit)
    SELECT 
        fs.date_id,
        fs.customer_id,
        fs.product_id,
        dp.category,
        dc.segment,
        SUM(fs.quantity) AS quantity,
        SUM(fs.sales) AS sales,
        SUM(fs.profit) AS profit
    FROM data_warehouse.fact_Sales fs
    JOIN data_warehouse.dim_Product dp ON fs.product_id = dp.product_id
    JOIN data_warehouse.dim_Customer dc ON fs.customer_id = dc.customer_id
    JOIN data_warehouse.dim_Date dd ON fs.date_id = dd.date_id
    GROUP BY fs.date_id, fs.customer_id, fs.product_id, dp.category, dc.segment
""")

# Commit các thay đổi
conn_warehouse.commit()
conn_data_mart.commit()

# Đóng các kết nối
cursor_warehouse.close()
conn_warehouse.close()
cursor_data_mart.close()
conn_data_mart.close()

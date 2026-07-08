# I went with SQL, refreshing my memory on it
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FindCustomersWithNoOrder") \
    .getOrCreate()

orders_df = spark.read.csv("./orders.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("./customers.csv", header=True, inferSchema=True)

orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")

spark.sql("""
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name
    FROM customers c
    LEFT JOIN orders o
        ON c.customer_id = o.customer_id
    WHERE o.customer_id IS NULL
    ORDER BY c.customer_id
""").show()
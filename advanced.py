import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, round, expr, date_sub, current_date, floor

print("Advanced PySpark example")

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("myspark")
        .enableHiveSupport()
        .getOrCreate()
)

DATA_PATH = "sample_sales"
N = 1_000_000

if os.path.exists(DATA_PATH):
    print("Loading existing data from")
    sales = spark.read.parquet(DATA_PATH)
else:
    print("Generating sample data")
    sales = (
        spark.range(N)
        .withColumn("customer_id", (col("id") % 50_000).cast("int"))
        .withColumn(
            "region",
            when(col("id") % 4 == 0, "North")
            .when(col("id") % 4 == 1, "South")
            .when(col("id") % 4 == 2, "East")
            .otherwise("West")
        )
        .withColumn(
            "product",
            when(col("id") % 5 == 0, "Laptop")
            .when(col("id") % 5 == 1, "Phone")
            .when(col("id") % 5 == 2, "Tablet")
            .when(col("id") % 5 == 3, "Monitor")
            .otherwise("Keyboard")
        )
        .withColumn("quantity", ((rand() * 5) + 1).cast("int"))
        .withColumn("unit_price", round((rand() * 900) + 50, 2))
        .withColumn("sale_amount", round(col("quantity") * col("unit_price"), 2))
        .withColumn("sale_date", date_sub(current_date(), floor(rand() * 365).cast("int")))
        .drop("id")
    )

    print("Saving data to disk...")
    sales.write.mode("overwrite").parquet(DATA_PATH)

    print("Reloading saved data...")
    sales = spark.read.parquet(DATA_PATH)

sales.cache()

print("Data loaded!")

sales.createOrReplaceTempView("sales")
print("Data saved!")

print("Data ready!")
print("Tables:")
spark.sql("SHOW TABLES").show()

print("#### API QUERIES ####")

sales.show(10)
print("Rows:", sales.count())

# 1. Total sales by region
sales.groupBy("region") \
    .sum("sale_amount") \
    .show()

# 2. Total sales by product
sales.groupBy("product") \
    .sum("sale_amount") \
    .show()

# 3. Average order value by region
sales.groupBy("region") \
    .avg("sale_amount") \
    .show()

# 4. Top 10 customers by total spend
sales.groupBy("customer_id") \
    .sum("sale_amount") \
    .orderBy(col("sum(sale_amount)").desc()) \
    .show(10)

# 5. Large orders only
sales.filter(col("sale_amount") > 3000) \
    .show(20)

# 6. Product sales by region
sales.groupBy("region", "product") \
    .sum("sale_amount") \
    .orderBy("region", "product") \
    .show()

# 7. Sales in last 30 days
sales.filter(col("sale_date") >= date_sub(current_date(), 30)) \
    .groupBy("product") \
    .sum("sale_amount") \
    .show()

# 8. Register as SQL table

print("#### SQL QUERIES ####")

spark.sql("""
    SELECT region, product, ROUND(SUM(sale_amount), 2) AS total_sales
    FROM sales
    GROUP BY region, product
    ORDER BY total_sales DESC
""").show()

spark.sql("""
    SELECT customer_id, ROUND(SUM(sale_amount), 2) AS total_spend
    FROM sales
    GROUP BY customer_id
    ORDER BY total_spend DESC
    LIMIT 10
""").show()

spark.stop()
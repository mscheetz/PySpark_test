import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, round, expr, date_sub, current_date, floor

DATA_PATH = "sample_sales"
N = 1_000_000

def create_spark_session():
        return (
        SparkSession.builder
        .master("local[*]")
        .appName("myspark")
        .getOrCreate()
    )

def generate_sample_data(spark, row_count):
    return (
        spark.range(row_count)
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

def load_or_create_data(spark, data_path=DATA_PATH, row_count=N):
    if os.path.exists(data_path):
        print(f"Loading existing data from '{data_path}'")
        return spark.read.parquet(data_path)
        
    print("Generating sample data")
    sales = generate_sample_data(spark, row_count)
    
    print("Saving data to disk...")
    sales.write.mode("overwrite").parquet(data_path)

    print("Reloading saved data...")
    sales = spark.read.parquet(data_path)

def prepare_view(sales):
    sales.cache()
    sales.createOrReplaceTempView("sales")
    return sales

def show_table_info(spark):
    print("Registered tables/views:")

    for table in spark.catalog.listTables():
        print(f"- {table.name} ({table.tableType})")

def run_api_queries(sales):
    print("#### API QUERIES ####")

    sales.show(10)
    print("Rows:", sales.count())

    print("Total sales by region")
    sales.groupBy("region") \
        .sum("sale_amount") \
        .show()

    print("Total sales by product")
    sales.groupBy("product") \
        .sum("sale_amount") \
        .show()

    print("Average order by region")
    sales.groupBy("region") \
        .avg("sale_amount") \
        .show()

    print("Top 10 customers by total spend")
    sales.groupBy("customer_id") \
        .sum("sale_amount") \
        .orderBy(col("sum(sale_amount)").desc()) \
        .show(10)

    print("Large orders")
    sales.filter(col("sale_amount") > 3000) \
        .show(20)

    print("Sales by region")
    sales.groupBy("region", "product") \
        .sum("sale_amount") \
        .orderBy("region", "product") \
        .show()

    print("Sales last 30 days")
    sales.filter(col("sale_date") >= date_sub(current_date(), 30)) \
        .groupBy("product") \
        .sum("sale_amount") \
        .show()

def run_sql_queries(spark):
    print("#### SQL QUERIES ####")

    print("Sales by region/product")
    spark.sql("""
        SELECT region, product, ROUND(SUM(sale_amount), 2) AS total_sales
        FROM sales
        GROUP BY region, product
        ORDER BY total_sales DESC
    """).show()

    print("Top 10 customers")
    spark.sql("""
        SELECT customer_id, ROUND(SUM(sale_amount), 2) AS total_spend
        FROM sales
        GROUP BY customer_id
        ORDER BY total_spend DESC
        LIMIT 10
    """).show()

def main():
    print("Advanced PySpark example")

    spark = create_spark_session()

    try:
        sales = load_or_create_data(spark)
        prepare_view(sales)

        print("Data ready!")

        show_table_info(spark)
        run_api_queries(sales)
        run_sql_queries(spark)

    finally:
        spark.stop()
        print("Doneski")

if __name__ == "__main__":
    main()

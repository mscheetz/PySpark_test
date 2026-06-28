import os
from pyspark.sql import SparkSession
from data_gen import DataGen

DATA_DIR = "multi_data"

def create_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("multi-data-spark")
        .getOrCreate()
    )

def reset_data_dir():
    DataGen.reset_data_dir(DATA_DIR)

def create_customers(spark):
    DataGen.create_customers(spark, DATA_DIR)

def create_products(spark):
    DataGen.create_products(spark, DATA_DIR)

def create_orders(spark):
    DataGen.create_orders(spark, DATA_DIR)

def create_web_events(spark):
    DataGen.create_web_events(spark, DATA_DIR)

def load_or_create_data(spark):
        if os.path.exists(DATA_DIR):
            print(f"Data already exists in '{DATA_DIR}'")
        else:
            reset_data_dir()

            create_customers(spark)
            create_products(spark)
            create_orders(spark)
            create_web_events(spark)

        print("Loading data")
        load_data(spark)

def load_data(spark):
    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{DATA_DIR}/customers_csv")
    )

    products = spark.read.json(f"{DATA_DIR}/products_json")
    orders = spark.read.parquet(f"{DATA_DIR}/orders_parquet")
    web_events = spark.read.parquet(f"{DATA_DIR}/web_events_parquet")

    customers.createOrReplaceTempView("customers")
    products.createOrReplaceTempView("products")
    orders.createOrReplaceTempView("orders")
    web_events.createOrReplaceTempView("web_events")

    return customers, products, orders, web_events


def run_queries(spark):
    print("Tables:")
    for table in spark.catalog.listTables():
        print(table.name)

    print("\n1. Revenue by customer:")
    spark.sql("""
        SELECT
            c.customer_name,
            c.segment,
            c.region,
            CAST(ROUND(SUM(o.quantity * p.list_price * (1 - o.discount_pct)), 2) AS BIGINT) AS revenue
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products p ON o.product_id = p.product_id
        GROUP BY c.customer_name, c.segment, c.region
        ORDER BY revenue DESC
    """).show(truncate=False)

    print("\n2. Revenue by product category:")
    spark.sql("""
        SELECT
            p.category,
            COUNT(*) AS order_count,
            CAST(ROUND(SUM(o.quantity * p.list_price * (1 - o.discount_pct)), 2) AS BIGINT) AS revenue
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        GROUP BY p.category
        ORDER BY revenue DESC
    """).show(truncate=False)

    print("\n3. Customer engagement vs revenue:")
    spark.sql("""
        WITH revenue AS (
            SELECT
                customer_id,
                CAST(ROUND(SUM(quantity * list_price * (1 - discount_pct)), 2) AS BIGINT) AS revenue
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            GROUP BY customer_id
        ),
        engagement AS (
            SELECT
                customer_id,
                COUNT(*) AS web_events,
                SUM(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS checkouts
            FROM web_events
            GROUP BY customer_id
        )
        SELECT
            c.customer_name,
            c.segment,
            c.industry,
            r.revenue,
            e.web_events,
            e.checkouts
        FROM customers c
        JOIN revenue r ON c.customer_id = r.customer_id
        JOIN engagement e ON c.customer_id = e.customer_id
        ORDER BY r.revenue DESC
    """).show(truncate=False)

    print("\n4. Monthly revenue:")
    spark.sql("""
        SELECT
            DATE_TRUNC('month', o.order_date) AS month,
            CAST(ROUND(SUM(o.quantity * p.list_price * (1 - o.discount_pct)), 2) AS BIGINT) AS revenue
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        GROUP BY DATE_TRUNC('month', o.order_date)
        ORDER BY month
    """).show(50, truncate=False)

    print("\n5. Top customers:")
    spark.sql("""
        SELECT
            c.customer_name,
            c.industry,
            CAST(ROUND(SUM(o.quantity * p.list_price * (1 - o.discount_pct)), 2) AS BIGINT) AS revenue
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products p ON o.product_id = p.product_id
        WHERE c.segment = 'Enterprise'
        GROUP BY c.customer_name, c.industry
        HAVING revenue > 50000000
        ORDER BY revenue DESC
    """).show(truncate=False)

def main():
    print("Multi-Data Spark Sample")

    spark = create_spark()

    try:
        load_or_create_data(spark)

        run_queries(spark)

    finally:
        spark.stop()
        print("Doneski")


if __name__ == "__main__":
    main()
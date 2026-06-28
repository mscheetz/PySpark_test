import os
import shutil
from datetime import date, timedelta
from pyspark.sql.functions import col, rand, when, round, date_sub, current_date, floor, expr

class DataGen:

    @staticmethod
    def generate_sales_data(spark, n=1_000_000):
        return (
                spark.range(n)
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

    @staticmethod
    def reset_data_dir(data_dir):
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)

        os.makedirs(data_dir, exist_ok=True)

    @staticmethod
    def create_customers(spark, data_dir):
        customers = [
            (1, "Acme Corp", "Enterprise", "North", "Manufacturing"),
            (2, "Globex", "Mid-Market", "East", "Technology"),
            (3, "Initech", "Enterprise", "West", "Finance"),
            (4, "Umbrella Corp", "Enterprise", "South", "Healthcare"),
            (5, "Soylent", "SMB", "East", "Retail"),
            (6, "Stark Industries", "Enterprise", "West", "Defense"),
            (7, "Wayne Enterprises", "Enterprise", "North", "Technology"),
            (8, "Wonka Industries", "SMB", "South", "Food"),
        ]

        df = spark.createDataFrame(
            customers,
            ["customer_id", "customer_name", "segment", "region", "industry"],
        )

        df.write.mode("overwrite").option("header", True).csv(f"{data_dir}/customers_csv")

    @staticmethod
    def create_products(spark, data_dir):
        products = [
            (101, "Laptop", "Hardware", 1200.00),
            (102, "Phone", "Hardware", 800.00),
            (103, "Monitor", "Hardware", 350.00),
            (104, "Keyboard", "Accessories", 75.00),
            (105, "Cloud Subscription", "Software", 250.00),
            (106, "Security Suite", "Software", 500.00),
        ]

        df = spark.createDataFrame(
            products,
            ["product_id", "product_name", "category", "list_price"],
        )

        df.write.mode("overwrite").json(f"{data_dir}/products_json")

    @staticmethod
    def create_orders(spark, data_dir, n=500_000):
        orders = (
            spark.range(n)
            .withColumn("order_id", col("id") + 10_000)
            .withColumn("customer_id", ((col("id") % 8) + 1).cast("int"))
            .withColumn("product_id", ((col("id") % 6) + 101).cast("int"))
            .withColumn("quantity", ((rand(seed=10) * 5) + 1).cast("int"))
            .withColumn("discount_pct", round(rand(seed=20) * 0.25, 2))
            .withColumn("order_date", date_sub(current_date(), floor(rand(seed=30) * 365).cast("int")))
            .drop("id")
        )

        orders.write.mode("overwrite").parquet(f"{data_dir}/orders_parquet")

    @staticmethod
    def create_web_events(spark, data_dir, n=1_000_000):
        events = (
            spark.range(n)
            .withColumn("event_id", col("id") + 1_000_000)
            .withColumn("customer_id", ((col("id") % 8) + 1).cast("int"))
            .withColumn(
                "event_type",
                when(col("id") % 5 == 0, "page_view")
                .when(col("id") % 5 == 1, "product_view")
                .when(col("id") % 5 == 2, "add_to_cart")
                .when(col("id") % 5 == 3, "checkout")
                .otherwise("support_visit"),
            )
            .withColumn("event_date", date_sub(current_date(), floor(rand(seed=40) * 365).cast("int")))
            .drop("id")
        )

        events.write.mode("overwrite").parquet(f"{data_dir}/web_events_parquet")

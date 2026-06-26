from pyspark.sql import SparkSession

def create_spark_session():
        return (
        SparkSession.builder
        .master("local[*]")
        .appName("myspark")
        .getOrCreate()
    )

def create_dataframe(spark):
    data = [
        ("Joe", 100),
        ("Jane", 150),
    ]

    return spark.createDataFrame(data, ["name", "quantity"])

def main():
    print("Simple PySpark example")

    spark = create_spark_session()

    try:
        df = create_dataframe(spark)

        print("Data loaded")

        print("Total quantity:")
        df.selectExpr("sum(quantity) as total").show()

        print("View Data:")
        df.show()

    finally:
        spark.stop()
        print("Doneski")

if __name__ == "__main__":
    main()
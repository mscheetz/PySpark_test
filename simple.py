from pyspark.sql import SparkSession

print("Simple PySpark example")

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("myspark")
        .getOrCreate()
)

data = [
    ("Joe", 100),
    ("Jane", 150)
]

df = spark.createDataFrame(data, ["name", "quantity"]);

print("Data loaded")

print("Total quantity:")
df.selectExpr("sum(quantity) as total").show()

print("View Data:")
df.show()

spark.stop()
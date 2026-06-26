import pytest
from pyspark.sql import SparkSession
from src.simple import create_dataframe

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .getOrCreate()
    )

    yield spark

    spark.stop()

def test_dataframe_has_two_rows(spark):
    df = create_dataframe(spark)

    assert df.count() == 2


def test_dataframe_columns(spark):
    df = create_dataframe(spark)

    assert df.columns == ["name", "quantity"]

def test_total_quantity(spark):
    df = create_dataframe(spark)
    total = df.selectExpr("sum(quantity) as total").collect()[0]["total"]

    assert total == 250 

def test_contains_joe(spark):
    df = create_dataframe(spark)

    assert df.filter(df.name == "Joe").count() == 1

def test_contains_jane(spark):
    df = create_dataframe(spark)

    assert df.filter(df.name == "Jane").count() == 1

def test_quantities_are_positive(spark):
    df = create_dataframe(spark)

    assert df.filter(df.quantity <= 0).count() == 0
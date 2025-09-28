import pytest
from pyspark.sql import SparkSession

from src.product_category import get_product_category_pairs


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("Product - Category") \
        .getOrCreate()

    yield spark
    spark.stop()


def test_product_dataframe(spark):
    data = [
        (1, "Телефон"),
        (2, "Ноутбук"),
    ]
    df = spark.createDataFrame(data, ["product_id", "product_name"])
    assert df.count() == 2

def test_category_dataframe(spark):
    data = [
        (10, "Электроника"),
        (12, "Периферия"),
    ]
    df = spark.createDataFrame(data, ["category_id", "category_name"])
    assert df.count() == 2


def test_join_products_and_categories(spark):
    products_data = [(1, "Телефон"), (2, "Ноутбук")]
    categories_data = [(10, "Электроника")]
    relations_data = [(1, 10)]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    prod_cat_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

    result_df = get_product_category_pairs(products_df, categories_df, prod_cat_df)

    rows = result_df.collect()

    assert ("Телефон", "Электроника") in [(r.product_name, r.category_name) for r in rows]
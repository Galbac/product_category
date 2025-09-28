from pyspark.sql import DataFrame


def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame,
                               product_category_df: DataFrame) -> DataFrame:
    result = products_df \
        .join(product_category_df, products_df["product_id"] == product_category_df["product_id"], "left") \
        .join(categories_df, product_category_df["category_id"] == categories_df["category_id"], "left") \
        .select(products_df["product_name"], categories_df["category_name"])
    return result


from pyspark.sql import SparkSession                             # Spark entry point
from pyspark.sql.functions import col, year, month, dayofmonth   # Date and column functions


def main():
    # Hive database name
    DB_NAME = "iphone_analytics"

    # Read product data to calculate total sales amount
    products = spark.table("silver_products")

    # Join sales with products and calculate total_amount
    fact_df = (
        sales.join(products, "product_id")  # Join on product_id
        .withColumn("total_amount",  # Calculate revenue per sale
                    col("quantity") * col("unit_price"))
        .select(
            "sale_id",
            "customer_id",
            "product_id",
            "store_id",
            col("sale_date").alias("date_key"),  # Rename sale_date for partitioning
            "quantity",
            "total_amount"
        )
    )

# Script entry point
if __name__ == "__main__":
    main()

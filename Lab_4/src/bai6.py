import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, sum, year, expr

def bai6():
    logger = logging.getLogger("bai6")
    spark = get_spark_session("lab4bai6")

    orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Orders.csv")
    items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Items.csv")
    products_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Products.csv")

    orders_df = orders_df.withColumn("Order_Purchase_Timestamp", expr("try_cast(Order_Purchase_Timestamp as timestamp)"))
    items_df = items_df.withColumn("price", expr("try_cast(price as double)")) \
                       .withColumn("freight_value", expr("try_cast(freight_value as double)"))

    report_df = (
        orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024)
        .join(items_df, "Order_ID")
        .join(products_df, "Product_ID")
        .groupBy("Product_Category_Name")
        .agg(sum(col("price") + col("freight_value")).alias("Total_Revenue"))
        .orderBy(col("Total_Revenue").desc())
    )

    results = report_df.collect()
    for row in results:
        cat_name = str(row['Product_Category_Name']) if row['Product_Category_Name'] else "Không xác định"
        revenue = row['Total_Revenue'] if row['Total_Revenue'] else 0.0

        logger.info(f"{cat_name:<35} | ${revenue:,.2f}")

    output_log_path = os.path.join(config.output_dir, "bai6.txt")


    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'Danh mục':<35} | {'Tổng doanh thu':<20}\n")
        f.write("-" * 60 + "\n")

        for row in results:
            cat_name = str(row['Product_Category_Name']) if row['Product_Category_Name'] else "Không xác định"
            revenue = row['Total_Revenue'] if row['Total_Revenue'] else 0.0

            f.write(f"{cat_name:<35} | ${revenue:,.2f}\n")

if __name__ == "__main__":
    bai6()
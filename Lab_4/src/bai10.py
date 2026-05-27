import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, countDistinct, sum, expr

def bai10():
    logger = logging.getLogger("bai10")
    spark = get_spark_session("lab4bai10")

    items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Items.csv")

    items_df = items_df.withColumn("price", expr("try_cast(price as double)"))
    report_df = (
        items_df.groupBy("Seller_ID")
        .agg(
            sum("price").alias("Total_Revenue"),
            countDistinct("Order_ID").alias("Total_Orders")
        )
        .orderBy(col("Total_Revenue").desc())
    )

    results = report_df.collect()

    for row in results[:10]:
        revenue = row['Total_Revenue'] if row['Total_Revenue'] else 0.0
        logger.info(f"{row['Seller_ID']:<35} | {row['Total_Orders']:>5,} đơn | ${revenue:,.2f}")

    output_log_path = os.path.join(config.output_dir, "bai10.txt")
    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'Mã người bán':<35} | {'Tổng đơn':<15} | {'Tổng doanh thu':<20}\n")
        f.write("-" * 80 + "\n")
        for row in results:
            revenue = row['Total_Revenue'] if row['Total_Revenue'] else 0.0
            f.write(f"{row['Seller_ID']:<35} | {row['Total_Orders']:<15,} | ${revenue:,.2f}\n")

if __name__ == "__main__":
    bai10()
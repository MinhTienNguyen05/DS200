import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, count, avg, expr

def bai7():
    logger = logging.getLogger("bai7")
    spark = get_spark_session("lab4bai7")


    items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Items.csv")
    reviews_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Reviews.csv")

    reviews_df = reviews_df.withColumn("Review_Score", expr("try_cast(Review_Score as int)"))

    report_df = (
        items_df.join(reviews_df, "Order_ID", "left")
        .groupBy("Product_ID")
        .agg(
            count("Order_ID").alias("Total_Sold"),
            avg("Review_Score").alias("Avg_Score")
        )
        .orderBy(col("Total_Sold").desc())
    )

    results = report_df.collect()


    for row in results[:10]:
        avg_score = row['Avg_Score'] if row['Avg_Score'] else 0.0
        logger.info(f"{row['Product_ID']:<35} | {row['Total_Sold']:>5,} đã bán | {avg_score:.2f}")


    output_log_path = os.path.join(config.output_dir, "bai7.txt")

    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'Mã sản phẩm':<35} | {'Đã bán':<15} | {'Điểm trung bình':<10}\n")

        for row in results:
            avg_score = row['Avg_Score'] if row['Avg_Score'] else 0.0
            f.write(f"{row['Product_ID']:<35} | {row['Total_Sold']:<15,} | {avg_score:.2f}\n")

if __name__ == "__main__":
    bai7()
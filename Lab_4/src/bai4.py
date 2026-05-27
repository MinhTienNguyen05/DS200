import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, count, year, month

def bai4():
    logger = logging.getLogger("bai4")
    spark = get_spark_session("lab4bai4")

    orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Orders.csv")

    TIME_COL = "Order_Purchase_Timestamp"

    report_df = (
        orders_df
        .withColumn("Order_Year", year(col(TIME_COL)))
        .withColumn("Order_Month", month(col(TIME_COL)))
        .filter(col("Order_Year").isNotNull())
        .groupBy("Order_Year", "Order_Month")
        .agg(count("Order_ID").alias("Total_Orders"))
        .orderBy(col("Order_Year").asc(), col("Order_Month").desc())
    )

    results = report_df.collect()

    for row in results:
        month_str = str(row['Order_Month']).zfill(2)
        logger.info(f"Năm {row['Order_Year']} - Tháng {month_str} : {row['Total_Orders']:,} đơn")

    output_log_path = os.path.join(config.output_dir, "bai4.txt")

    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'Năm':<10} | {'Tháng':<10} | {'Số lượng đơn hàng':<20}\n")

        for row in results:
            month_str = str(row['Order_Month']).zfill(2)
            f.write(f"{row['Order_Year']:<10} | {month_str:<10} | {row['Total_Orders']:,}\n")

if __name__ == "__main__":
    bai4()
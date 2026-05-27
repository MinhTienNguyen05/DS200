import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import countDistinct

def bai2():
    logger = logging.getLogger("bai2")
    spark = get_spark_session("lab4bai2")

    customers_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Customer_List.csv")
    orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Orders.csv")
    items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Items.csv")


    total_orders = orders_df.select(countDistinct("Order_ID").alias("total")).collect()[0]["total"]
    unique_customers = customers_df.select(countDistinct("Customer_Trx_ID").alias("total")).collect()[0]["total"]
    total_sellers = items_df.select(countDistinct("Seller_ID").alias("total")).collect()[0]["total"]

    logger.info(f"TỔNG SỐ ĐƠN HÀNG    : {total_orders:,}")
    logger.info(f"SỐ LƯỢNG KHÁCH HÀNG : {unique_customers:,}")
    logger.info(f"SỐ LƯỢNG NGƯỜI BÁN  : {total_sellers:,}")

    output_log_path = os.path.join(config.output_dir, "bai2.txt")
    logger.info(f"Đang ghi kết quả ra file: {output_log_path}")

    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"Tổng số đơn hàng: {total_orders:,}\n")
        f.write(f"Tổng số khách hàng: {unique_customers:,}\n")
        f.write(f"Tổng số người bán: {total_sellers:,}\n")

if __name__ == "__main__":
    bai2()
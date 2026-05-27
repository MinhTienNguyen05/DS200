import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, count

def bai3():
    logger = logging.getLogger("bai3")
    spark = get_spark_session("lab4bai3")

    logger.info("Đang đọc dữ liệu Orders và Customers...")
    customers_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Customer_List.csv")
    orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Orders.csv")

    COUNTRY_COL = "Customer_Country"

    report_df = (
        orders_df.join(customers_df, "Customer_Trx_ID", "left") # Nối để lấy thông tin địa lý
        .groupBy(COUNTRY_COL)                                   # Gom nhóm theo từng quốc gia
        .agg(count("Order_ID").alias("Total_Orders"))           # Đếm tổng số đơn hàng
        .orderBy(col("Total_Orders").desc())                    # Sắp xếp giảm dần
    )

    results = report_df.collect()

    logger.info("BẢNG XẾP HẠNG SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA")
    for row in results:
        country_name = str(row[COUNTRY_COL]) if row[COUNTRY_COL] else "Unknown (Không xác định)"
        logger.info(f"{country_name:<30} : {row['Total_Orders']:,} đơn")


    output_log_path = os.path.join(config.output_dir, "bai3.txt")

    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'Tên quốc gia':<35} | {'Số lượng đơn hàng':<20}\n")
        for row in results:
            country_name = str(row[COUNTRY_COL]) if row[COUNTRY_COL] else "Unknown"
            f.write(f"{country_name:<35} | {row['Total_Orders']:,}\n")


if __name__ == "__main__":
    bai3()
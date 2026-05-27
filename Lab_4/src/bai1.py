import logging
import os
from config import config
from spark_core import get_spark_session

def bai1():
    logger = logging.getLogger("bai1")
    spark = get_spark_session("lab4bai1")

    logger.info("Đọc dữ liệu")
    customers_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Customer_List.csv")
    orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Orders.csv")
    items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Items.csv")
    products_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Products.csv")
    reviews_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Reviews.csv")

    dataframes_dict = {
        "CUSTOMER_LIST": customers_df,
        "ORDER_ITEMS": items_df,
        "ORDER_REVIEWS": reviews_df,
        "ORDERS": orders_df,
        "PRODUCTS": products_df
    }

    logger.info(f"Đang xuất Schema của 5 bảng ra file:")

    output_log_path = os.path.join(config.output_dir, "bai1.txt")

    with open(output_log_path, "w", encoding="utf-8") as f:

        # Duyệt qua từng DataFrame trong Dictionary
        for table_name, df in dataframes_dict.items():
            # Lấy chuỗi schema dạng cây từ lõi Java của Spark
            schema_string = df._jdf.schema().treeString()

            # Ghi tên bảng và cấu trúc vào file
            f.write(f"Schema của bảng: {table_name} \n")
            f.write(f"{schema_string}\n")
            f.write("-" * 60 + "\n\n")

            print(f"Schema của bảng: {table_name}")
            print(schema_string)
            print("-" * 60 + "\n")

if __name__ == "__main__":
    bai1()

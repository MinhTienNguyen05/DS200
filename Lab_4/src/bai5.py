import logging
import os
from config import config
from spark_core import get_spark_session
from pyspark.sql.functions import col, count, avg, expr

def bai5():
    logger = logging.getLogger("bai5")
    spark = get_spark_session("lab4bai5")

    reviews_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(f"file://{config.data_dir}/Order_Reviews.csv")

    SCORE_COL = "Review_Score"

    original_count = reviews_df.count()

    reviews_df = reviews_df.withColumn(SCORE_COL, expr(f"try_cast({SCORE_COL} as int)"))

    cleaned_df = reviews_df.filter(
        col(SCORE_COL).isNotNull() &
        (col(SCORE_COL) >= 1) &
        (col(SCORE_COL) <= 5)
    )

    valid_count = cleaned_df.count()
    invalid_count = original_count - valid_count

    if invalid_count > 0:
        logger.warning(f"Phát hiện và loại bỏ {invalid_count:,} dòng dữ liệu lỗi!")
    else:
        logger.info(" Dữ liệu đầu vào hoàn toàn sạch, không có ngoại lệ.")


    avg_score_row = cleaned_df.select(avg(SCORE_COL).alias("Average_Score")).collect()[0]
    overall_avg = avg_score_row["Average_Score"]

    distribution_df = (
        cleaned_df.groupBy(SCORE_COL)
        .agg(count("*").alias("Total_Reviews"))
        .orderBy(col(SCORE_COL).desc())
    )

    results = distribution_df.collect()

    logger.info(f"Tổng số đánh giá hợp lệ : {valid_count:,} lượt")
    logger.info(f"Điểm trung bình: {overall_avg:.2f} / 5.00")
    logger.info("-" * 55)

    for row in results:
        score = int(row[SCORE_COL])
        logger.info(f"Mức {score} sao : {row['Total_Reviews']:>7,} lượt đánh giá")


    output_log_path = os.path.join(config.output_dir, "bai5.txt")

    with open(output_log_path, "w", encoding="utf-8") as f:
        f.write(f"Điểm đánh giá trung bình: {overall_avg:.2f} / 5.00\n")
        f.write(f"{'Mức điểm':<15} | {'Số lượng đánh giá':<20}\n")
        f.write("-" * 60 + "\n")

        for row in results:
            score = int(row[SCORE_COL])
            f.write(f"{f'{score}':<15} | {row['Total_Reviews']:,}\n")

if __name__ == "__main__":
    bai5()
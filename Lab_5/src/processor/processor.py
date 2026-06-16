import json
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

from src.common.config import KAFKA_BROKER, TOPIC_CAMERA, OUTPUT_JSON_DIR, DB_URL, DB_USER, DB_PASS
from src.utils.image_utils import decode_base64_to_image
from src.processor.model import PersonDetector

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

detector = PersonDetector()

def process_and_write_batch(df_batch, batch_id):
    rows = df_batch.collect()
    if not rows: return

    processed_data = []
    print(f"\nXử lý batch {batch_id} với {len(rows)} ảnh")

    for row in rows:
        try:
            data = json.loads(row['json_string'])
            frame = decode_base64_to_image(data['image_base64'])
            person_count = detector.count_people(frame)

            processed_data.append((data['frame_id'], data['timestamp'], person_count))
            print(f"    Frame {data['frame_id']}: {person_count} người.")
        except Exception as e:
            print(f"Lỗi ở frame: {e}")

    if processed_data:
        spark = SparkSession.builder.getOrCreate()
        schema = StructType([
            StructField("frame_id", IntegerType(), True),
            StructField("timestamp", DoubleType(), True),
            StructField("people_count", IntegerType(), True)
        ])
        result_df = spark.createDataFrame(processed_data, schema=schema)

        try:
            result_df.write.format("json").mode("append").save(OUTPUT_JSON_DIR)
            print("[FILE] Đã xuất ra JSON.")
        except Exception as e:
            print(f"Lỗi xuất JSON: {e}")

        try:
            result_df.write.format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "detection_results") \
                .option("user", DB_USER) \
                .option("password", DB_PASS) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append").save()
            print("[DB] Đã lưu vào PostgreSQL.")
        except Exception as e:
            print(f"Lỗi lưu PostgreSQL: {e}")

def start_streaming():
    spark = SparkSession.builder \
        .appName("PeopleCounter_Spark_Kafka") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_CAMERA) \
        .option("startingOffsets", "latest") \
        .load()

    df_string = df.selectExpr("CAST(value AS STRING) as json_string")

    query = df_string.writeStream \
        .outputMode("append") \
        .trigger(processingTime='2 seconds') \
        .foreachBatch(process_and_write_batch) \
        .start()

    print(f"Đã kết nối Kafka ({KAFKA_BROKER}). Bắt đầu Spark Streaming...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
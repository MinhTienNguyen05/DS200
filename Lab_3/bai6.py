from pyspark import SparkContext, SparkConf
import datetime  

conf = SparkConf().setAppName("MovieLens_Time_Analysis").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")

ratings_raw = ratings_1_raw.union(ratings_2_raw)

def keep_year_rating(line: str):
    parts = line.split(',')
    rating = float(parts[2])
    timestamp = int(parts[3])

    # Chuyển đổi Timestamp sang Năm
    year = datetime.datetime.fromtimestamp(timestamp).year

    # Phát hành cặp khóa-giá trị: (Năm, (Rating, 1))
    return str(year), (rating, 1)

def get_total_rating(value_1, value_2):
    total_rating_1, count_1 = value_1
    total_rating_2, count_2 = value_2
    return (total_rating_1 + total_rating_2, count_1 + count_2)

def calculate_avg(value):
    total_rating, count = value
    return (total_rating / count, count)

# Map dữ liệu thành cặp (Năm, (Điểm, 1))
year_rating_pair_rdd = ratings_raw.map(keep_year_rating)

# ReduceByKey để tính tổng điểm và số lượt cho mỗi năm
total_year_rating_rdd = year_rating_pair_rdd.reduceByKey(get_total_rating)

# Tính điểm trung bình
avg_year_rating_rdd = total_year_rating_rdd.mapValues(calculate_avg)

# Thu thập kết quả về Driver
results = avg_year_rating_rdd.collect()

print(" KẾT QUẢ BÀI 6: PHÂN TÍCH ĐÁNH GIÁ THEO NĂM ")
print("="*55)
print(f"{'NĂM':<10} | {'ĐIỂM TRUNG BÌNH':<18} | {'TỔNG SỐ LƯỢT'}")

for record in sorted(results, key=lambda x: int(x[0])):
    year = record[0]
    avg_rating = record[1][0]
    count = record[1][1]

    print(f"{year:<10} | {avg_rating:<18.2f} | {count} lượt")


csv_data = ["Year,AvgRating,TotalRatings"]
for record in sorted(results, key=lambda x: int(x[0])):
    year = record[0]
    avg_rating = record[1][0]
    count = record[1][1]

    csv_data.append(f"{year},{avg_rating:.2f},{count}")

output_rdd = sc.parallelize(csv_data)
output_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai6")

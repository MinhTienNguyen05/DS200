from pyspark import SparkContext, SparkConf

# 1. Khởi tạo SparkContext
conf = SparkConf().setAppName("MovieLens_HDFS_Lab3").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# 2. Đọc dữ liệu từ HDFS
movies_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/movies.txt")
ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")

# Gộp 2 RDD ratings
ratings_raw = ratings_1_raw.union(ratings_2_raw)

# 3. Định nghĩa các hàm xử lý
def keep_id_title(line: str):
    parts = line.split(',')
    movie_id = parts[0]
    title = parts[1]
    return movie_id, title

def keep_id_rating(line: str):
    parts = line.split(',')
    movie_id = parts[1]
    rating = float(parts[2])
    return movie_id, (rating, 1)

def get_total_rating(value_1, value_2):
    total_rating_1, count_1 = value_1
    total_rating_2, count_2 = value_2
    return (total_rating_1 + total_rating_2, count_1 + count_2)

def calculate_avg(value):
    total_rating, count = value
    return (total_rating / count, count)

# 4. Thực thi luồng biến đổi RDD
# Map dữ liệu
movies_pair_rdd = movies_raw.map(keep_id_title)
ratings_pair_rdd = ratings_raw.map(keep_id_rating)

# Reduce tính tổng và MapValues tính trung bình
total_ratings_rdd = ratings_pair_rdd.reduceByKey(get_total_rating)
avg_ratings_rdd = total_ratings_rdd.mapValues(calculate_avg)

# Lọc phim >= 5 đánh giá
MIN_RATINGS = 5
filtered_movies_rdd = avg_ratings_rdd.filter(lambda x: x[1][1] >= MIN_RATINGS)

# 5. Tìm phim điểm cao nhất và xuất kết quả
if not filtered_movies_rdd.isEmpty():
    best_movie_rdd_result = filtered_movies_rdd.max(key=lambda x: x[1][0])

    best_movie_id = best_movie_rdd_result[0]
    best_avg_rating = best_movie_rdd_result[1][0]
    best_count = best_movie_rdd_result[1][1]

    # Tra cứu tên phim
    movie_dict = movies_pair_rdd.collectAsMap()
    best_movie_title = movie_dict.get(best_movie_id, "Unknown Title")

    print(" KẾT QUẢ BÀI 1")
    print("="*40)
    print(f"- Tên phim           : {best_movie_title} (ID: {best_movie_id})")
    print(f"- Điểm trung bình    : {best_avg_rating:.2f} / 5.0")
    print(f"- Tổng lượt đánh giá : {best_count} lượt")


    header_line = "MovieID,Title,AvgRating,TotalRatings"
    csv_line = f"{best_movie_id},{best_movie_title},{best_avg_rating:.2f},{best_count}"
    best_movie_rdd = sc.parallelize([header_line, csv_line])
    best_movie_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai1")

else:
    print(f"Không có phim nào đạt đủ ngưỡng {MIN_RATINGS} lượt đánh giá.")



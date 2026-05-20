from pyspark import SparkContext, SparkConf

# 1. Khởi tạo SparkContext
conf = SparkConf().setAppName("MovieLens_Genre_Analysis").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# 2. Đọc dữ liệu từ HDFS
movies_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/movies.txt")
ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")

ratings_raw = ratings_1_raw.union(ratings_2_raw)

# 3. Định nghĩa các hàm xử lý dữ liệu
def keep_id_genres(line: str):
    parts = line.split(',')
    movie_id = parts[0]
    genres = parts[2].split('|')
    return movie_id, genres

def keep_id_rating(line: str):
    parts = line.split(',')
    movie_id = parts[1]
    rating = float(parts[2])
    return movie_id, (rating, 1)

def expand_genres(record):
    movie_id, (genres_list, rating_tuple) = record
    expanded_records = []
    for genre in genres_list:
        expanded_records.append((genre, rating_tuple))
    return expanded_records

def get_total_rating(value_1, value_2):
    total_rating_1, count_1 = value_1
    total_rating_2, count_2 = value_2
    return (total_rating_1 + total_rating_2, count_1 + count_2)

def calculate_avg(value):
    total_rating, count = value
    return (total_rating / count, count)

# 4. Thực thi luồng biến đổi RDD
# Bước 1: Tạo cặp (MovieID, List of Genres) và (MovieID, (Rating, 1))
movies_pair_rdd = movies_raw.map(keep_id_genres)
ratings_pair_rdd = ratings_raw.map(keep_id_rating)

# JOIN 2 RDD lại với nhau dựa trên key chung là MovieID
joined_rdd = movies_pair_rdd.join(ratings_pair_rdd)

# Bước 2: Dùng flatMap để phân rã danh sách thể loại thành từng dòng riêng biệt
genre_rating_rdd = joined_rdd.flatMap(expand_genres)

# Bước 3: Tính trung bình điểm cho từng thể loại
total_genre_rating_rdd = genre_rating_rdd.reduceByKey(get_total_rating)

# Tính điểm trung bình
avg_genre_rating_rdd = total_genre_rating_rdd.mapValues(calculate_avg)

sorted_genre_ratings = avg_genre_rating_rdd.sortBy(lambda x: x[1][0], ascending=False).collect()

print(" KẾT QUẢ BÀI 2: ĐIỂM TRUNG BÌNH THEO THỂ LOẠI ")
print("="*50)
print(f"{'THỂ LOẠI':<15} | {'ĐIỂM TB':<10} | {'SỐ LƯỢNG ĐÁNH GIÁ'}")
print("-" * 50)
for record in sorted_genre_ratings:
    genre = record[0]
    avg_rating = record[1][0]
    count = record[1][1]
    print(f"{genre:<15} | {avg_rating:<10.2f} | {count}")


csv_data = ["Genre,AvgRating,TotalRatings"]

for record in sorted_genre_ratings:
    genre = record[0]
    avg_rating = record[1][0]
    count = record[1][1]
    csv_data.append(f"{genre},{avg_rating:.2f},{count}")

output_rdd = sc.parallelize(csv_data)

output_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai2")

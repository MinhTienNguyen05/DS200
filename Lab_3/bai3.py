from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("MovieLens_Gender_Analysis").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

users_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/users.txt")
ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")
movies_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/movies.txt")

ratings_raw = ratings_1_raw.union(ratings_2_raw)

def keep_user_gender(line: str):
    parts = line.split(',')
    user_id = parts[0]
    gender = parts[1]
    return user_id, gender

def keep_rating_user_key(line: str):
    parts = line.split(',')
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    return user_id, (movie_id, rating)

def map_to_movie_gender_key(record):
    # Đầu vào của record sau khi JOIN: (UserID, (Gender, (MovieID, Rating)))
    user_id, (gender, (movie_id, rating)) = record
    # Tạo tổ hợp khóa mới là (MovieID, Gender) và giá trị tích lũy ban đầu là (Rating, 1)
    return (movie_id, gender), (rating, 1)

def get_total_rating(value_1, value_2):
    total_rating_1, count_1 = value_1
    total_rating_2, count_2 = value_2
    return (total_rating_1 + total_rating_2, count_1 + count_2)

def calculate_avg(value):
    total_rating, count = value
    return (total_rating / count, count)

def keep_id_title(line: str):
    parts = line.split(',')
    return parts[0], parts[1]

# Bước 1: Tạo các RDD cặp Key-Value
users_pair_rdd = users_raw.map(keep_user_gender)
ratings_pair_rdd = ratings_raw.map(keep_rating_user_key)

# Bước 2: Join hai tập dữ liệu dựa trên UserID làm khóa chung
# Kết quả trả về dạng: (UserID, (Gender, (MovieID, Rating)))
joined_user_ratings = users_pair_rdd.join(ratings_pair_rdd)

# Bước 3: Chuyển đổi cấu trúc khóa sang dạng tổ hợp (MovieID, Gender)
movie_gender_pair_rdd = joined_user_ratings.map(map_to_movie_gender_key)

# Tiến hành cộng dồn tổng số điểm và tổng số lượt đánh giá theo cặp khóa
total_movie_gender = movie_gender_pair_rdd.reduceByKey(get_total_rating)

# Tính điểm trung bình chung cho từng phim tương ứng với từng giới tính
avg_movie_gender = total_movie_gender.mapValues(calculate_avg)

# Thu thập bản đồ danh mục phim về Driver Node để ánh xạ tên phim khi in kết quả
movie_dict = movies_raw.map(keep_id_title).collectAsMap()

results = avg_movie_gender.collect()

print(" KẾT QUẢ BÀI 3: PHÂN TÍCH ĐÁNH GIÁ PHIM THEO GIỚI TÍNH ")
print("="*65)
print(f"{'MÃ PHIM':<8} | {'TÊN PHIM':<30} | {'GIỚI TÍNH':<10} | {'ĐIỂM TRUNG BÌNH'}")

for record in sorted(results, key=lambda x: x[0][0]):
    movie_id = record[0][0]
    gender = record[0][1]
    avg_rating = record[1][0]

    movie_title = movie_dict.get(movie_id, "Unknown Movie")
    if len(movie_title) > 28:
        movie_title = movie_title[:25] + "..."


    print(f"{movie_id:<8} | {movie_title:<30} | {gender:<10} | {avg_rating:.2f} / 5.0")

csv_data = ["MovieID,Title,Gender,AvgRating"]

for record in sorted(results, key=lambda x: (x[0][0], x[0][1])):
    movie_id = record[0][0]
    gender = record[0][1]
    avg_rating = record[1][0]

    full_movie_title = movie_dict.get(movie_id, "Unknown Movie")

    csv_data.append(f"{movie_id},{full_movie_title},{gender},{avg_rating:.2f}")
output_rdd = sc.parallelize(csv_data)

output_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai3")

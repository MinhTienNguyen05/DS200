from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("MovieLens_AgeGroup_Analysis").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

users_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/users.txt")
ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")
movies_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/movies.txt")

ratings_raw = ratings_1_raw.union(ratings_2_raw)

def categorize_age(age: int):
    """Hàm phân loại số tuổi thành các nhóm tuổi cụ thể"""
    if age < 18:
        return "< 18"
    elif 18 <= age <= 24:
        return "18-24"
    elif 25 <= age <= 34:
        return "25-34"
    elif 35 <= age <= 44:
        return "35-44"
    elif 45 <= age <= 54:
        return "45-54"
    else:
        return "55+"

def keep_user_age_group(line: str):
    parts = line.split(',')
    user_id = parts[0]
    age = int(parts[2])
    age_group = categorize_age(age)
    return user_id, age_group

def keep_rating_user_key(line: str):
    parts = line.split(',')
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    return user_id, (movie_id, rating)

def map_to_movie_age_key(record):
    # Dữ liệu sau JOIN: (UserID, (AgeGroup, (MovieID, Rating)))
    user_id, (age_group, (movie_id, rating)) = record
    # Tạo khóa phức hợp mới: (MovieID, AgeGroup)
    return (movie_id, age_group), (rating, 1)

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

# Map dữ liệu
users_pair_rdd = users_raw.map(keep_user_age_group)
ratings_pair_rdd = ratings_raw.map(keep_rating_user_key)

# Join dữ liệu theo UserID
joined_user_ratings = users_pair_rdd.join(ratings_pair_rdd)

# Chuyển đổi khóa và tính toán
movie_age_pair_rdd = joined_user_ratings.map(map_to_movie_age_key)
total_movie_age = movie_age_pair_rdd.reduceByKey(get_total_rating)
avg_movie_age = total_movie_age.mapValues(calculate_avg)

# Thu thập từ điển tên phim
movie_dict = movies_raw.map(keep_id_title).collectAsMap()

results = avg_movie_age.collect()


print(" KẾT QUẢ BÀI 4: PHÂN TÍCH ĐÁNH GIÁ PHIM THEO NHÓM TUỔI ")
print("="*70)
print(f"{'MÃ PHIM':<8} | {'TÊN PHIM':<30} | {'NHÓM TUỔI':<10} | {'ĐIỂM TRUNG BÌNH'}")


for record in sorted(results, key=lambda x: (x[0][0], x[0][1])):
    movie_id = record[0][0]
    age_group = record[0][1]
    avg_rating = record[1][0]

    movie_title = movie_dict.get(movie_id, "Unknown Movie")
    if len(movie_title) > 28:
        movie_title = movie_title[:25] + "..."

    print(f"{movie_id:<8} | {movie_title:<30} | {age_group:<10} | {avg_rating:.2f} / 5.0")


csv_data = ["MovieID,Title,AgeGroup,AvgRating"]
for record in sorted(results, key=lambda x: (x[0][0], x[0][1])):
    movie_id = record[0][0]
    age_group = record[0][1]
    avg_rating = record[1][0]

    full_movie_title = movie_dict.get(movie_id, "Unknown Movie")

    csv_data.append(f"{movie_id},{full_movie_title},{age_group},{avg_rating:.2f}")

output_rdd = sc.parallelize(csv_data)

output_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai4")

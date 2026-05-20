from pyspark import SparkContext, SparkConf

# 1. Khởi tạo SparkContext
conf = SparkConf().setAppName("MovieLens_Occupation_Analysis").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# 2. Đọc dữ liệu từ HDFS
users_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/users.txt")
ratings_1_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_1.txt")
ratings_2_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/ratings_2.txt")
occupation_raw = sc.textFile("hdfs://127.0.0.1:9000/lab3/occupation.txt")

ratings_raw = ratings_1_raw.union(ratings_2_raw)

# Khởi tạo dictionary: ID Nghề nghiệp -> Tên Nghề nghiệp
# Schema occupation: ID, Occupation
def map_occupation(line):
    parts = line.split(',')
    # Trả về một Tuple rõ ràng: (ID, Tên nghề nghiệp)
    return parts[0], parts[1]

occ_dict = occupation_raw.map(map_occupation).collectAsMap()

# Khởi tạo dictionary: UserID -> Tên Nghề nghiệp
def map_user_to_occ_name(line):
    parts = line.split(',')
    user_id = parts[0]
    occ_id = parts[3]
    occ_name = occ_dict.get(occ_id, "Unknown")
    return user_id, occ_name

# Dùng collectAsMap() để gom RDD thành Dictionary trên Driver
user_occ_dict = users_raw.map(map_user_to_occ_name).collectAsMap()

# Broadcast dictionary này đến tất cả các Worker Nodes để tra cứu cực nhanh
broadcast_user_occ = sc.broadcast(user_occ_dict)

def assign_occ_to_rating(line):
    parts = line.split(',')
    user_id = parts[0]
    rating = float(parts[2])

    # Với mỗi rating, lấy tên Occupation thông qua UserID từ Broadcast Dictionary
    user_occ_map = broadcast_user_occ.value
    occupation_name = user_occ_map.get(user_id, "Unknown")

    # Phát hành cặp key-value: (Occupation, (Rating, 1))
    return occupation_name, (rating, 1)

def get_total_rating(value_1, value_2):
    total_rating_1, count_1 = value_1
    total_rating_2, count_2 = value_2
    return (total_rating_1 + total_rating_2, count_1 + count_2)

def calculate_avg(value):
    total_rating, count = value
    return (total_rating / count, count)

# Gán nghề nghiệp vào từng đánh giá
occ_rating_pair_rdd = ratings_raw.map(assign_occ_to_rating)

# ReduceByKey để tính tổng điểm và số lượt cho mỗi Occupation
total_occ_rating_rdd = occ_rating_pair_rdd.reduceByKey(get_total_rating)

# MapValues để tính điểm trung bình
avg_occ_rating_rdd = total_occ_rating_rdd.mapValues(calculate_avg)

# Thu thập kết quả về Driver
results = avg_occ_rating_rdd.collect()


print(" KẾT QUẢ BÀI 5: ĐÁNH GIÁ THEO NGHỀ NGHIỆP ")
print("="*60)
print(f"{'NGHỀ NGHIỆP':<20} | {'ĐIỂM TRUNG BÌNH':<15} | {'TỔNG SỐ LƯỢT'}")


for record in sorted(results, key=lambda x: x[1][0], reverse=True):
    occupation = record[0]
    avg_rating = record[1][0]
    count = record[1][1]

    print(f"{occupation:<20} | {avg_rating:<15.2f} | {count} lượt")

csv_data = ["Occupation,AvgRating,TotalRatings"]
for record in sorted(results, key=lambda x: x[1][0], reverse=True):
    occupation = record[0]
    avg_rating = record[1][0]
    count = record[1][1]
    csv_data.append(f"{occupation},{avg_rating:.2f},{count}")

output_rdd = sc.parallelize(csv_data)
output_rdd.coalesce(1).saveAsTextFile("hdfs://127.0.0.1:9000/lab3/output/bai5")

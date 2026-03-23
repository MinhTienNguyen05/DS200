#!/bin/bash

BAI_TAP=$1
if [ -z "$BAI_TAP" ]; then
    echo "Lỗi: Vui lòng nhập tên bài tập (VD: ./run.sh bai1 hoặc ./run.sh bai2)"
    exit 1
fi

JAR_NAME="lab_1.jar"
OUT_BIN="build_classes"
LOCAL_RESULT_DIR="output"

mkdir -p $LOCAL_RESULT_DIR

# --- 1. BIÊN DỊCH & ĐÓNG GÓI ---
echo "--- Đang dọn dẹp thư mục class và file jar cũ..."
rm -rf $OUT_BIN $JAR_NAME
mkdir -p $OUT_BIN

echo "--- Đang biên dịch Java ..."
javac --release 11 -classpath $(hadoop classpath) -d $OUT_BIN *.java

if [ $? -ne 0 ]; then
    echo "--- LỖI BIÊN DỊCH! Vui lòng kiểm tra lại code Java."
    exit 1
fi

echo "--- Đang đóng gói thành file $JAR_NAME..."
jar -cvf $JAR_NAME -C $OUT_BIN/ .

# --- 2. CHẠY MAPREDUCE ---
case $BAI_TAP in
    "bai1")
        HDFS_OUT="lab_1/output_bai1"
        LOCAL_FILE="$LOCAL_RESULT_DIR/ketqua_bai1.txt"
        echo "--- Xóa output cũ trên HDFS..."
        hdfs dfs -rm -r $HDFS_OUT 2>/dev/null

        echo "--- BÀI 1: Tính Điểm Đánh Giá Trung Bình và Tổng Số Lượt Đánh Giá Cho Mỗi Phim..."
        hadoop jar $JAR_NAME bai1 -D mapreduce.framework.name=local lab_1/movies.txt lab_1/ratings_1.txt lab_1/ratings_2.txt $HDFS_OUT

        echo "--- Đang tải kết quả về máy..."
        rm -f $LOCAL_FILE
        hdfs dfs -get $HDFS_OUT/part-r-00000 $LOCAL_FILE
        ;;

    "bai2")
        HDFS_OUT="lab_1/output_bai2"
        LOCAL_FILE="$LOCAL_RESULT_DIR/ketqua_bai2.txt"
        echo "--- Xóa output cũ trên HDFS..."
        hdfs dfs -rm -r $HDFS_OUT 2>/dev/null

        echo "--- BÀI 2: Tính điểm trung bình theo Thể loại..."
        hadoop jar $JAR_NAME bai2 -D mapreduce.framework.name=local lab_1/movies.txt lab_1/ratings_1.txt lab_1/ratings_2.txt $HDFS_OUT

        echo "--- Đang tải kết quả về máy..."
        rm -f $LOCAL_FILE
        hdfs dfs -get $HDFS_OUT/part-r-00000 $LOCAL_FILE
        ;;

    "bai3")
        INPUT_USERS="lab_1/users.txt"
        INPUT_MOVIES="lab_1/movies.txt"
        HDFS_OUT="lab_1/output_bai3"
        LOCAL_FILE="$LOCAL_RESULT_DIR/ketqua_bai3.txt"

        echo "--- Xóa output cũ trên HDFS..."
        hdfs dfs -rm -r $HDFS_OUT 2>/dev/null

        echo "--- BÀI 3: Tính điểm trung bình theo Giới tính"
        # Chú ý thứ tự args: bai3 <users> <movies> <ratings_1> <ratings_2> <output>
        hadoop jar $JAR_NAME bai3 -D mapreduce.framework.name=local $INPUT_USERS $INPUT_MOVIES lab_1/ratings_1.txt lab_1/ratings_2.txt $HDFS_OUT

        echo "--- Đang tải kết quả về máy..."
        rm -f $LOCAL_FILE
        hdfs dfs -get $HDFS_OUT/part-r-00000 $LOCAL_FILE
        ;;

        "bai4")
        INPUT_USERS="lab_1/users.txt"
        INPUT_MOVIES="lab_1/movies.txt"
        HDFS_OUT="lab_1/output_bai4"
        LOCAL_FILE="$LOCAL_RESULT_DIR/ketqua_bai4.txt"

        echo "--- Xóa output cũ trên HDFS..."
        hdfs dfs -rm -r $HDFS_OUT 2>/dev/null

        echo "--- BÀI 4: Tính điểm trung bình theo Nhóm tuổi"
        hadoop jar $JAR_NAME bai4 -D mapreduce.framework.name=local $INPUT_USERS $INPUT_MOVIES lab_1/ratings_1.txt lab_1/ratings_2.txt $HDFS_OUT

        echo "--- Đang tải kết quả về máy..."
        rm -f $LOCAL_FILE
        hdfs dfs -get $HDFS_OUT/part-r-00000 $LOCAL_FILE
        ;;
    *)
        echo "Lỗi: Không tìm thấy bài tập $BAI_TAP"
        rm -rf $OUT_BIN
        exit 1
        ;;
esac

# --- 3. HIỂN THỊ KẾT QUẢ ---
if [ -f "$LOCAL_FILE" ]; then
    echo "----------------------------------------"
    echo "--- KẾT QUẢ 15 DÒNG ĐẦU TIÊN:"
    head -n 15 $LOCAL_FILE
    echo "----------------------------------------"
    echo "--- Kết quả đã được lưu tại: $LOCAL_FILE"
else
    echo "--- CẢNH BÁO: Không lấy được kết quả."
fi

rm -rf $OUT_BIN
import cv2
import json
import time
from kafka import KafkaProducer

from src.common.config import KAFKA_BROKER, TOPIC_CAMERA
from src.utils.image_utils import encode_image_to_base64

def start_camera():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    cap = cv2.VideoCapture(0)
    print(f"Đã bật Camera. Đang đẩy dữ liệu lên Kafka Topic '{TOPIC_CAMERA}'...")

    frame_id = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        frame = cv2.resize(frame, (640, 480))

        jpg_as_text = encode_image_to_base64(frame)

        payload = {
            "frame_id": frame_id,
            "timestamp": time.time(),
            "image_base64": jpg_as_text
        }

        producer.send(TOPIC_CAMERA, payload)
        print(f"[Camera] Đã gửi Frame: {frame_id}")

        frame_id += 1
        time.sleep(0.1)

    cap.release()

if __name__ == "__main__":
    start_camera()
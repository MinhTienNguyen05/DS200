import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_CAMERA = os.getenv('TOPIC_CAMERA', 'camera_frames')

OUTPUT_JSON_DIR = os.getenv('OUTPUT_JSON_DIR', 'storage_data/json_output')

DB_URL = os.getenv('DB_URL')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
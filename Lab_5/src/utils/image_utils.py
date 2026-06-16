import cv2
import base64
import numpy as np

def encode_image_to_base64(frame, quality=80):
    """Nén ảnh OpenCV thành chuỗi Base64 để truyền qua mạng nhẹ hơn"""
    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    return base64.b64encode(buffer).decode('utf-8')

def decode_base64_to_image(base64_string):
    """Giải mã chuỗi Base64 nhận được từ mạng trở lại thành ảnh OpenCV"""
    img_bytes = base64.b64decode(base64_string)
    np_arr = np.frombuffer(img_bytes, np.uint8)
    return cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
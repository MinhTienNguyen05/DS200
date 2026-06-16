import cv2
import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision

class PersonDetector:
    def __init__(self, model_path='efficientdet_lite0.tflite'):
        """
        Khởi tạo MediaPipe Object Detector
        """
        base_options = python.BaseOptions(model_asset_path=model_path)

        options = vision.ObjectDetectorOptions(base_options=base_options, score_threshold=0.4)

        self.detector = vision.ObjectDetector.create_from_options(options)

    def count_people(self, frame):
        """Trả về số lượng bounding box thuộc class 'person'"""
        # OpenCV đọc ảnh dạng BGR, nhưng MediaPipe yêu cầu dạng RGB
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb_frame)
        detection_result = self.detector.detect(mp_image)
        person_count = 0
        if detection_result.detections:
            for detection in detection_result.detections:
                category_name = detection.categories[0].category_name
                if category_name == 'person':
                    person_count += 1

        return person_count
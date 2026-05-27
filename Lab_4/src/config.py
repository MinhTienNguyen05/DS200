import os
from dataclasses import dataclass

@dataclass
class ProjectConfig:
    base_dir: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir: str = os.path.join(base_dir, "data")
    output_dir: str = os.path.join(base_dir, "output")

config = ProjectConfig()
os.makedirs(config.output_dir, exist_ok=True)
import json
import os

def load_config():
    # Dynamically get the path where this file is located
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(base_dir, "config.json")

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file {config_file} not found.")

    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)

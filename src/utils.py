import os
import yaml
import logging
from typing import Dict, Any, List
import shutil

def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        raise

def setup_logging(config: Dict[str, Any]):
    log_level = getattr(logging, config['logging']['level'].upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config['logging']['log_file']),
            logging.StreamHandler()
        ]
    )

def ensure_directories(config: Dict[str, Any]):
    directories = [
        config['pipeline']['input_directory'],
        config['pipeline']['output_directory'],
        config['pipeline']['processed_directory']
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def get_files_to_process(config: Dict[str, Any]) -> List[str]:
    input_dir = config['pipeline']['input_directory']
    extensions = config['pipeline']['file_extensions']
    
    files = []
    for file in os.listdir(input_dir):
        if any(file.lower().endswith(ext) for ext in extensions):
            file_path = os.path.join(input_dir, file)
            # Check file size
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb <= config['pipeline']['max_file_size_mb']:
                files.append(file_path)
    
    return files

def move_processed_file(original_path: str, config: Dict[str, Any]):
    filename = os.path.basename(original_path)
    destination = os.path.join(config['pipeline']['processed_directory'], filename)
    shutil.move(original_path, destination)
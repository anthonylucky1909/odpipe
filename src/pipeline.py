import os
import time
from typing import List, Dict, Any
import logging
from tqdm import tqdm
from .parser import DocumentParser
from .utils import get_files_to_process, move_processed_file, load_config

class DataPipeline:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = load_config(config_path)
        self.parser = DocumentParser(self.config)
        self.processed_count = 0
        self.failed_count = 0
    
    def run(self):
        logging.info("Starting data pipeline...")
        files = get_files_to_process(self.config)
        total_files = len(files)
        if total_files == 0:
            logging.info("No files to process.")
            return
        logging.info(f"Found {total_files} files to process.")
        batch_size = self.config['pipeline']['batch_size']
        for i in range(0, total_files, batch_size):
            batch_files = files[i:i + batch_size]
            self._process_batch(batch_files)
        
        logging.info(f"Pipeline completed. Processed: {self.processed_count}, Failed: {self.failed_count}")
    
    def _process_batch(self, files: List[str]):
        for file_path in tqdm(files, desc="Processing files"):
            try:
                # Parse the document
                result = self.parser.parse_document(file_path)
                
                if result:
                    # Generate output filename
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    output_path = os.path.join(
                        self.config['pipeline']['output_directory'],
                        base_name
                    )
                    
                    # Save results
                    self.parser.save_results(result, output_path)
                    
                    # Move processed file
                    if self.config['pipeline'].get('processed_directory'):
                        move_processed_file(file_path, self.config)
                    
                    self.processed_count += 1
                else:
                    self.failed_count += 1     
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
                self.failed_count += 1
            # Small delay to prevent overwhelming the system
            time.sleep(0.1)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        return {
            'processed': self.processed_count,
            'failed': self.failed_count,
            'success_rate': (self.processed_count / (self.processed_count + self.failed_count)) * 100 
            if (self.processed_count + self.failed_count) > 0 else 0
        }
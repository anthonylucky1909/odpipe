import os
import json
import pandas as pd
from typing import Dict, Any, Optional
import logging

class DocumentParser:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        try:
            from od_parse import parse_pdf, convert_to_markdown
            self.parse_pdf = parse_pdf
            self.convert_to_markdown = convert_to_markdown
        except ImportError:
            logging.error("od-parse is not installed. Install it with: pip install -e .[all]")
            raise

    def parse_document(self, file_path: str) -> Optional[Dict[str, Any]]:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None

        logging.info(f"Parsing document: {file_path}")
        use_deep_learning = self.config.get('parsing', {}).get('use_deep_learning', True)

        try:
            parsed_data = self.parse_pdf(file_path, use_deep_learning=use_deep_learning)
            parsed_data['file_info'] = {
                'filename': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'file_path': file_path
            }
            return parsed_data
        except Exception as e:
            logging.error(f"Failed to parse {file_path}: {e}")
            return None

    def save_results(self, results: Dict[str, Any], output_path: str):
        if not results:
            logging.warning("No results to save")
            return

        output_format = self.config.get('output', {}).get('format', 'json')
        base_name = os.path.splitext(output_path)[0]

        if output_format in ['json', 'both']:
            json_path = f"{base_name}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved JSON file: {json_path}")

        if output_format in ['csv', 'both']:
            summary_df = self._create_summary(results)
            if summary_df is not None:
                csv_path = f"{base_name}.csv"
                summary_df.to_csv(csv_path, index=False)
                logging.info(f"Saved CSV file: {csv_path}")

    def _create_summary(self, results: Dict[str, Any]) -> Optional[pd.DataFrame]:
        try:
            parsed_data = results.get('parsed_data', results)
            summary = {
                'filename': results['file_info']['filename'],
                'file_size_bytes': results['file_info']['file_size'],
                'text_length': len(parsed_data.get('text', '')),
                'page_count': len(parsed_data.get('pages', [])),
                'table_count': len(parsed_data.get('tables', [])),
                'has_metadata': bool(parsed_data.get('metadata'))
            }

            metadata = parsed_data.get('metadata', {})
            for field in ['title', 'author', 'creator', 'creation_date']:
                summary[field] = metadata.get(field, 'N/A')

            return pd.DataFrame([summary])
        except Exception as e:
            logging.error(f"Failed to create summary: {e}")
            return None

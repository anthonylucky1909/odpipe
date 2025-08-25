import argparse
import os
from src.pipeline import DataPipeline
from src.parser import DocumentParser
from src.utils import setup_logging, load_config, ensure_directories

def main():
    parser = argparse.ArgumentParser(description="odParse Data Pipeline")
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Path to the configuration file"
    )
    parser.add_argument(
        "--single-file", "-f",
        help="Process a single file instead of batch processing"
    )
    args = parser.parse_args()
    config = load_config(args.config)
    setup_logging(config)
    ensure_directories(config)
    pipeline = DataPipeline(args.config)
    if args.single_file:
        # Process a single file
        parser_instance = DocumentParser(config)
        result = parser_instance.parse_document(args.single_file)

        if result:
            base_name = os.path.splitext(os.path.basename(args.single_file))[0]
            output_dir = config['pipeline'].get('output_directory', 'output')
            output_path = os.path.join(output_dir, base_name)
            parser_instance.save_results(result, output_path)
            print(f"Successfully processed: {args.single_file}")
        else:
            print(f"Failed to process: {args.single_file}")
    else:
        pipeline.run()
        stats = pipeline.get_stats()
        print("\nPipeline Statistics:")
        print(f"Processed: {stats['processed']}")
        print(f"Failed: {stats['failed']}")
        print(f"Success Rate: {stats['success_rate']:.2f}%")

if __name__ == "__main__":
    main()

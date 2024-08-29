
import os
import json
import argparse
import time
import logging
from typing import Dict
from pathlib import Path
from openai import OpenAI

class QueryGenerator:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.config_file_path = Path('config') / 'openai_batch_config.jsonl'

    def __call__(self, input_file_path: str, openai_batch_config: Dict) -> None:
        """
        Args:
            input_file_path (str): Input file path in `jsonl` format
            openai_batch_config (Dict): Configuration for OpenAI batch processing
        """
        os.environ['OPENAI_API_KEY'] = self.api_key
        client = OpenAI()

        try:
            with open(input_file_path, 'rb') as input_file:
                batch_input_file = client.files.create(
                    file=input_file,
                    purpose='batch'
                )
        except Exception as e:
            logging.error(f"Failed to create batch input file: {e}")
            return
        
        try:
            batch_job = client.batches.create(
                input_file_id=batch_input_file.id,
                endpoint='/v1/chat/completions',
                completion_window='24h',
                metadata=openai_batch_config
            )
        except Exception as e:
            logging.error(f"Failed to create batch job: {e}")
            return

        openai_batch_config.update({'batch_job_id': batch_job.id})

        try:
            with open(self.config_file_path, 'a', encoding='utf-8') as f:
                json.dump(openai_batch_config, f, ensure_ascii=False)
                f.write('\n')
        except Exception as e:
            logging.error(f"Failed to write OpenAI batch config: {e}")

def get_last_id(jsonl_file_path):
    try:
        with open(jsonl_file_path, 'r', encoding='utf-8') as f:
            last_id = 0
            for line in f:
                data = json.loads(line.strip())
                if 'fid' in data:
                    last_id = int(data['fid'])
            return last_id
    except FileNotFoundError:
        return 0


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("execution.log"),
            logging.StreamHandler()
        ]
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', type=str, required=True)
    parser.add_argument('--data', type=str, required=True)
    parser.add_argument('--version', type=str, required=True)
    parser.add_argument('--input_file_path', type=str, default='')

    args = parser.parse_args()
    config_file_path = Path('config') / 'openai_batch_config.jsonl'

    try:
        generator = QueryGenerator(api_key=args.api_key)
    except Exception as e:
        logging.error(f"Failed to initialize QueryGenerator: {e}")
        return

    try:
        fid = str(get_last_id(config_file_path) + 1)
        openai_batch_config = {
            'fid': fid,
            'data': args.data,
            'version': args.version,
            'date': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        logging.error(f"Failed to generate OpenAI batch config: {e}")
        return

    try:
        generator(input_file_path=args.input_file_path, openai_batch_config=openai_batch_config)
    except Exception as e:
        logging.error(f"Failed to run QueryGenerator: {e}")

if __name__ == '__main__':
    setup_logging()
    main()

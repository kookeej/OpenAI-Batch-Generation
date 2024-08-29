import os
import sys
import json
import logging
import argparse
from pathlib import Path
import pandas as pd
from openai import OpenAI



def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("execution.log"),
            logging.StreamHandler()
        ]
    )


def save_batch_output(api_key, fid):
    logging.info("Loading batch configuration...")
    
    # Path
    config_path = Path('config') / 'openai_batch_config.jsonl'
    output_raw_path = Path('outputs') / 'raw'

    try:
        if not os.path.exists(output_raw_path):
            os.makedirs(output_raw_path)
    except OSError:
        print("Error: Failed to create the directory.")

    try:
        with open(config_path, 'r') as file:
            for line in file:
                json_data = json.loads(line)
                if json_data['fid'] == fid:
                    batch_job_id = json_data['batch_job_id']
                    data = json_data['data']
                    version = json_data['version']
                    date = json_data['date']

                    os.environ['OPENAI_API_KEY'] = api_key
                    client = OpenAI()

                    logging.info("Retrieving batch job...")
                    batch_job = client.batches.retrieve(batch_job_id)
                    output = client.files.content(batch_job.output_file_id).content

                    # Save output data
                    output_raw_file_path = output_raw_path / f'batch_raw_{data}_{version}_{date}.jsonl'
                    with open(output_raw_file_path, 'wb') as f:
                        f.write(output)
                    logging.info(f"Output file saved to {output_raw_file_path}")

                    return output_raw_file_path

        logging.error("Can't find the data!")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error while saving batch output: {e}")
        sys.exit(1)

def process_batch_output(output_raw_file_path):
    
    # Path
    processed_path = Path('outputs') / 'processed'

    try:
        if not os.path.exists(processed_path):
            os.makedirs(processed_path)
    except OSError:
        print("Error: Failed to create the directory.")

    logging.info("Processing batch output...")
    try:
        output_data = []
        with open(output_raw_file_path, 'r') as f:
            for line in f:
                output_data.append(json.loads(line))

        # Process the data
        generated = pd.DataFrame(output_data)
        generated['id'] = generated['custom_id']
        generated['generation'] = generated['response'].apply(lambda x: x['body']['choices'][0]['message']['content'])
        generated = generated[['id', 'custom_id', 'generation']]
        generated_json = generated.to_dict(orient='records')

        file_name = str(output_raw_file_path).split('/')[-1].split('.jsonl')[0]
        processed_file_path =  processed_path / f'processed_{file_name}.jsonl'
        with open(processed_file_path, 'w') as f:
            for line in generated_json:
                f.write(json.dumps(line))
                f.write('\n')

        logging.info(f"Processed file saved to {processed_file_path}")
    except Exception as e:
        logging.error(f"Error while processing batch output: {e}")


if __name__ == "__main__":
    setup_logging()

    parser = argparse.ArgumentParser(description='Load batch output and save as jsonl file.')
    parser.add_argument('--api_key', type=str, required=True)
    parser.add_argument('--fid', type=str, required=True, help='FID for the batch job')

    args = parser.parse_args()

    logging.info("Starting batch output processing...")
    output_file_path = save_batch_output(api_key=args.api_key, fid=args.fid)
    logging.info("Post-processing output file...")
    process_batch_output(output_file_path)
    logging.info("Batch output processing completed.")

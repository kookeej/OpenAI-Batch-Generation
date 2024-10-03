# OpenAI Batch Generation
```bash
# call.sh
API_KEY=YOUR_API_KEY
DATA=DATA_NAME
VERSION=VERSION_NAME
INPUT_FILE_PATH=PATH

python call_openai_batch.py --api_key $API_KEY\
                            --data $DATA \
                            --version $VERSION_NAME \
                            --input_file_path $INPUT_FILE_PATH
```
```bash
# save.sh
API_KEY=YOUR_API_KEY
FID=FID_FROM_CONFIG_FILE # (./config/openai_batch_config.jsonl)

python save_process_output.py --api_key $API_KEY\
                            --fid $FID
```

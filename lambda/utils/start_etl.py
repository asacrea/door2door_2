import json
import os
from etl_factory.factory.factory_etl import ETL_Factory

def lambda_handler(event, context):

    bucket_name = event['bucket_name']
    key_name = event['key_name']
    source_file_name = event['file_name']

    try:
        # path_data = os.listdir("test/data/")
        parameters = {
            "bucket_name": bucket_name,
            "key_name": key_name,
            "file_name": source_file_name,
            "load_path": "s3://dood-bucket/source/"
        }
        etl = ETL_Factory()
        etl.extract_method()
        etl.transform_method()
        etl.load_method()

        return {'Validation': 'SUCCESS'}

    except Exception as e:

        print(f'An error occurred: {str(e)}')
        return {'Validation': 'FAILURE'}

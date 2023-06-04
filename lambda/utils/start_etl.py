import json
import os
from etl_factory.factory.factory_etl import ETL_Factory

def lambda_handler(event, context):

    bucket_name = event['bucket_name']
    key_name = event['key_name']
    source_file_name = event['file_name']

    try:
        parameters = {
            "extract_method": "ExtractS3JsonData",
            "path_extract_method": "etl_factory.factory.extract",
            "transform_method": "JsonLivePositionTransform",
            "path_transform_method": "etl_factory.factory.transform",
            "load_method": "LoadDataToS3",
            "path_load_method": "etl_factory.factory.load",
            "bucket_name": bucket_name,
            "key_name": key_name,
            "file_name": source_file_name,
            "load_path": "s3://dood-bucket/stage/"
        }
        etl = ETL_Factory(parameters)
        etl.extract_method()
        etl.transform_method()
        etl.load_method()

        return {'Validation': 'SUCCESS'}

    except Exception as e:

        print(f'An error occurred: {str(e)}')
        return {'Validation': 'FAILURE'}

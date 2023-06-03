import json
import os
from etl_factory.factory.factory_etl import ETL_Factory

def lambda_handler(event, context):

    try:
        path_data = os.listdir("test/data/")
        load_path = "s3://dood-bucket-2/source/"
        print("Directorio actual:", path_data)

        etl = ETL_Factory(path_data)
        etl.extract_method()
        etl.transform_method()
        etl.load_method(load_path)

        return {'Validation': 'SUCCESS'}

    except Exception as e:

        print(f'An error occurred: {str(e)}')
        return {'Validation': 'FAILURE'}

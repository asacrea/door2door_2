import os
from factory_etl import ETL_Factory
from extract.abs_extraction import AbsExtraction

path_data = os.listdir("test/data/")
load_path = "s3://dood-bucket-2/source/"
print("Directorio actual:", path_data)

etl = ETL_Factory(path_data)
etl.extract_method()
etl.transform_method()
etl.load_method(load_path)
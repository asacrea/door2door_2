import os
from factory_etl import ETL_Factory
import loader
from extract.abs_extraction import AbsExtraction

path_data = os.listdir("test/data/")
print("Directorio actual:", path_data)

etl = ETL_Factory(path_data)
etl.extract_method()
etl.transform_method()
etl.load_method()
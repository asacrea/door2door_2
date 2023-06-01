import os
from factory_etl import ETL_Factory
import loader
from extract.abs_extraction import AbsExtraction

path_data = os.listdir("test/data/")
print("Directorio actual:", path_data)

test = ETL_Factory(path_data)
test.extract_method()
test.transform_method()
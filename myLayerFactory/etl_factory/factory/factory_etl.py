from abs_factory import AbsFactory
from loader import load_class
from transform.abs_transform import AbsTransform
from extract.abs_extraction import AbsExtraction
from load.abs_load import AbsLoad

class ETL_Factory(AbsFactory):
    '''
        This class allow you to create a ETL object based in Factory Design pattern
    '''
    def __init__(self, path) -> None:
        super().__init__()
        self.path = path
        self.data = None
        self.transformed_data = None
        self.file_name = None

    def extract_method(self):

        print("--------------------------------------")
        print("Extracting Files:", self.path, "\n")
        method = "ExtractS3JsonData"
        path_method = "extract"
        
        module = load_class(path_method, method, AbsExtraction)
        response, self.data = module.extract(str(self.path))
        
        print("Successfully extracted:", self.path)

    def transform_method(self):

        print("--------------------------------------")
        print("Transforming Files:", self.path, "\n")
        method = "JsonLivePositionTransform"
        path_method = "transform"

        module = load_class(path_method, method, AbsTransform)
        result, self.transformed_data = module.execute(self.data)

    def load_method(self, load_path):
        
        print("--------------------------------------")
        print(f"Loading Files to: {load_path}\n")
        method = "LoadDataToS3"
        path_method = "load"

        factory_load = load_class(path_method, method, AbsLoad)
        factory_load.execute(self.transformed_data, load_path)

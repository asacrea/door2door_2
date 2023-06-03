from etl_factory.factory.abs_factory import AbsFactory
from etl_factory.factory.loader import load_class
from etl_factory.factory.transform.abs_transform import AbsTransform
from etl_factory.factory.extract.abs_extraction import AbsExtraction
from etl_factory.factory.load.abs_load import AbsLoad

class ETL_Factory(AbsFactory):
    '''
        This class allow you to create a ETL object based in Factory Design pattern
    '''
    def __init__(self, parameters) -> None:
        super().__init__()
        self.parameters = parameters
        self.data = None
        self.transformed_data = None
        self.file_name = None

    def extract_method(self):

        print("--------------------------------------")
        print("Extracting Files:", self.path, "\n")
        method = "ExtractS3JsonData"
        path_method = "extract"
        
        module = load_class(path_method, method, AbsExtraction)
        response, self.data = module.extract(self.parameters)
        
        print(f"{response['Validation']}: {response['Reason']}")
        print(f"Location: {response['Location']}/ \
                          {self.parameters['bucket_name']}/ \
                          {self.parameters['key_name']}")

    def transform_method(self):

        print("--------------------------------------")
        print("Transforming Files:", self.path, "\n")
        method = "JsonLivePositionTransform"
        path_method = "transform"

        module = load_class(path_method, method, AbsTransform)
        result, self.transformed_data = module.execute(self.data)

    def load_method(self):
        
        print("--------------------------------------")
        print(f"Loading Files to: {self.parameters['load_path']}\n")
        method = "LoadDataToS3"
        path_method = "load"

        factory_load = load_class(path_method, method, AbsLoad)
        factory_load.execute(self.transformed_data, self.parameters["load_path"])

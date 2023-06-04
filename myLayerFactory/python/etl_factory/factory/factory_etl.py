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
        print("Extracting Files:\n")
        module = load_class(self.parameters["path_extract_method"], \
                            self.parameters["extract_method"], \
                            AbsExtraction)
        response, self.data = module.extract(self.parameters)
        
        print(f"{response['Validation']}: {response['Reason']}")
        print(f"Location: {response['Location']}")

    def transform_method(self):

        print("-------------------------------------")
        print("Transforming Files:\n")
        module = load_class(self.parameters["path_transform_method"], \
                            self.parameters["transform_method"], \
                            AbsTransform)
        result, self.transformed_data = module.execute(self.data)
        print(self.transformed_data)

    def load_method(self):
        
        print("--------------------------------------")
        print(f"Loading Files to: {self.parameters['load_path']}\n")
        factory_load = load_class(self.parameters["path_load_method"], \
                                  self.parameters["load_method"], \
                                  AbsLoad)
        factory_load.execute(self.transformed_data, \
                             self.parameters["load_path"], \
                             self.parameters["key_name"])

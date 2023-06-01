import os
import json
from abs_factory import AbsFactory
from loader import load_class
from transform.abs_transform import AbsTransform
from extract.abs_extraction import AbsExtraction

class ETL_Factory(AbsFactory):
    def __init__(self, path) -> None:
        super().__init__()
        self.path = path
        self.data = None
        self.transformed_data = None

    def extract_method(self):

        print("--------------------------------------")
        print("Extracting Files:", self.path, "\n")
        method = "ExtractS3JsonData"
        path_method = "extract"
        
        module = load_class(path_method, method, AbsExtraction)
        response, self.data = module.extract(str(self.path))
        
        # print(f"\nRuta: {self.path}\n")
        # self.data["Dataframes"]["self.path"] = df
        print("Successfully extracted:", self.path)
        print(self.data)


    def transform_method(self):

        print("--------------------------------------")
        print("Transforming Files:", self.path, "\n")
        method = "JsonLivePositionTransform"
        path_method = "transform"

        module = load_class(path_method, method, AbsTransform)
        result, self.transformed_data = module.execute(self.data)

        print(self.transformed_data)

    def load_method(self, load_path):
        pass
        # print("--------------------------------------")
        # print("Loading Files:", self.path, "\n")
        # method = "LoadDataToS3"
        # path_method = "transform"
        # for _class, path in config["load"]:
        #     factory_load = load_class.load_factory(_class, path)
        #     factory_load.execute()

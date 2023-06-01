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
        self.data = {
            "Dataframes":{}
        }

    def extract_method(self):
        
        # for file in self.config["Extract"].keys():
        # for file_name in self.path:
            # path = os.path.join("test/data/", file_name)
        print("--------------------------------------")
        print("Extracting Files:", self.path, "\n")
        method = "ExtractJsonData"
        path_method = "extract"
        
        module = load_class(path_method, method, AbsExtraction)
        response, df = module.extract(str(self.path))
        
        print(f"\nRuta: {self.path}\n")
        self.data["Dataframes"]["self.path"] = df
        print("Successfully extracted:", self.path)


    def transform_method(self):

        # Transform
        # method = "jsonTransform"

        # for method in self.config["Transform"][file].keys():
        #     path = "factory/transform/something/"
        #     module = load_class(method, path, AbsTransform)
        #     self.data['Dataframes'][file] = module.execute(
        #         dfs = self.data['Dataframes'],
        #         table = file,
        #         parameters = self.config["Transform"][file][method]['parameters']
        #     )

        print(self.data)

    def load_method(self, config):
        # for _class, path in config["load"]:
        #     factory_load = load_class.load_factory(_class, path)
        #     factory_load.execute()
        pass

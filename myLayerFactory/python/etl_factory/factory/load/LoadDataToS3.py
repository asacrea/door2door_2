import pandas as pd
from etl_factory.factory.load.abs_load import AbsLoad

class LoadDataToS3(AbsLoad):

    def execute(self, transformed_data, load_path, key_name):
        print("Loading information")
        file_name = key_name.split("/")[-1].split(".")[0]

        # Upload the CSV file to string to S3
        for event, df in transformed_data.items():
            target_file_name = "{}{}{}{}".format(file_name, "-", event, ".csv")
            print(load_path + target_file_name)
            transformed_key = load_path + target_file_name

            df.to_csv(transformed_key)

        print("Successfuly moved file to  : " + transformed_key)

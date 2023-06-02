import pandas as pd
from load.abs_load import AbsLoad

class LoadDataToS3(AbsLoad):

    def execute(self, transformed_data, load_path):
        bucket_name = "dood-bucket"
        key_name = "stage"
        print("Loading information")

        # Upload the CSV file to string to S3
        for name, df in transformed_data.items():
            target_file_name = "{}{}".format(name, ".csv")
            print(target_file_name)
            transformed_key = "s3://" + bucket_name + '/' + key_name + '/' + target_file_name

            df.to_csv(transformed_key, index=True)

        print("Successfuly moved file to  : " + transformed_key)

import os
import json
import boto3
import pandas as pd
from load.abs_load import AbsLoad

class LoadDataToS3(AbsLoad):

    def execute(self, transformed_data, load_path):
        bucket_name = "dood-bucket"
        key_name = "stage"
        print("Loading information")
        print(transformed_data)
        # Upload the JSON string to S3
        for name, df in transformed_data.items():
            target_file_name = "{}{}".format(name, ".csv")
            print(target_file_name)
            transformed_key = "s3://" + bucket_name + '/' + key_name + '/' + target_file_name

            df.to_csv(transformed_key, index=True)
            # s3.put_object(Bucket=bucket_name, Key=key_transform, Body=df)
            # s3_resource.Object(bucket_name, key_name).delete()
        print("Successfuly moved file to  : " + transformed_key)

    def load_s3(self):
        pass
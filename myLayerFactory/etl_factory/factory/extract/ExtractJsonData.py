import os
import json
import boto3
import pandas as pd
from extract.abs_extraction import AbsExtraction

class ExtractJsonData(AbsExtraction):

    def extract(self, file_name):
        
        result = {}
        # Create a boto3 S3 client
        s3 = boto3.client('s3')

        bucket_name = "dood-bucket"
        key_name = f"raw/{'2019-06-01-15-17-4-events.json'}"
        
        try:
            file_content = s3.get_object(Bucket=bucket_name, Key=key_name)["Body"].read().decode('utf-8').split('\n')
            # json_content = [cambiar_formato_fechas(json.loads(line), '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d %H:%M:%S') for line in file_content.splitlines()]
            
            json_content = [json.loads(line) for line in file_content if line.strip()]

            # event_list = ["create", "register", "update", "deregister"]
            event_list = ["create", "register", "update", "deregister"]
            df_data = {}
            df_dataframe = pd.DataFrame()
            for item in event_list:
                print(item)
                data = [period for period in json_content if period['event'] == item]
                if data:
                    list_periods = [pd.DataFrame.from_dict([row]) for row in data]
                    df_dataframe = pd.concat(list_periods, ignore_index=True)
                    data_column = pd.json_normalize(df_dataframe["data"])
                    df_dataframe = pd.concat([df_dataframe.drop("data", axis=1), data_column], axis=1)
                    df_data[item] = df_dataframe
                    print(df_data[item])
            print("Successfully read")
            result['Validation'] = "SUCCESS"
            return result, df_data
        except:
            result['Validation'] = "FAILURE"
            result['Reason'] = "Error while reading Json file in the source bucket"
            print('Error while reading Json')
            return result, None
        
    def connect(self):
        pass
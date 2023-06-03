import boto3
from etl_factory.factory.extract.abs_extraction import AbsExtraction

class ExtractS3JsonData(AbsExtraction):

    def extract(self, parameters):
        
        result = {}
        # Create a boto3 S3 client
        s3 = boto3.client('s3')

        bucket_name = parameters["bucket_name"]
        key_name = parameters["key_name"]
        
        try:
            #Get data S3 data from a especific folder in a bucket
            file_content = s3.get_object(Bucket=bucket_name, Key=key_name)["Body"].read().decode('utf-8').split('\n')

            print("Successfully read")
            result['Validation'] = "SUCCESS"
            result['Reason'] = ""
            return result, file_content
        except:
            result['Validation'] = "FAILURE"
            result['Reason'] = "Error while reading Json file in the source bucket"
            print('Error while reading Json ')
            return result, None
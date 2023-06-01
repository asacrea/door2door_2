import os
import json
import io
import boto3
import google.auth
import googleapiclient.http
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from botocore.exceptions import ClientError

s3  = boto3.client('s3')

scopes_list = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]

#Autenticación con las credenciales descargadas
creds_json = './credentials.json'
creds = service_account.Credentials.from_service_account_file(creds_json, scopes=scopes_list)
# Construir el cliente de la API de Google Drive
service = build('drive', 'v3', credentials=creds)
# Listar los archivos en una carpeta específica
folder_id = '1y_MFSCC3MMmhImMtQey6hF9i8xMj0FuD'

def drive_extract_load(event,  context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    bucket_name = 'dood-bucket'

    results = service.files().list(q=f"'{folder_id}' in parents and trashed=false",
                                fields="nextPageToken, files(id, name)").execute()
    files = results.get('files', [])
    validation = []
    # Print files names
    if not files:
        print('No files found.')
    else:
        # Download and upload each file
        for file in files:
            key = f"{file['name']}.json"
            validation.append(outload_to_s3(bucket_name, key, file))
        print(validation)
        
        response = {
            "statusCode": 200,
            "body": json.dumps(validation),
            "headers": {
                'Content-Type': 'application/json', 
                'Access-Control-Allow-Origin': '*'
            }
        }
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": response,
            }
        ),
    }

def outload_to_s3(bucket, key, file):
    try:
        print(f"{file['name']} ({file['id']})")
        file_content = io.BytesIO()
        request = service.files().get_media(fileId=file['id'])
        downloader = googleapiclient.http.MediaIoBaseDownload(file_content,request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f'Download {int(status.progress() * 100)}.')
        file_content.seek(0)

        key = "source/" + file["name"]
        response = s3.upload_fileobj(file_content, bucket, key)
        return f'Successfully uploaded {key} to S3 bucket {bucket}.'
    except HttpError as e:
        return f'An error occurred while processing file {file["name"]}: {e}'
    return url
import os
import json
import boto3
import pandas as pd
from datetime import datetime
from cerberus import Validator

def lambda_handler(event, context):
    print(event)
    result = {}
    # Create a boto3 S3 client
    # s3_resource = boto3.resource('s3')
    s3 = boto3.client('s3')

    bucket_name = event["bucket_name"]
    key_name = event["key_name"]
    source_file_name = event['file_name']
    
    target_file_name = print("{}{}".format(source_file_name.split(".")[0], ".json"))
    # key_transform = os.environ['stage_folder_name'] + '/' + source_file_name
    transformed_key = "s3://" + bucket_name + '/' + os.environ['stage_folder_name'] + '/' + target_file_name

    result, json_content = extract(result, s3, bucket_name, key_name, source_file_name)
    df = transform(json_content, result, source_file_name)
    
    load(df, transformed_key)
    
    return result

def extract(result, s3, bucket_name, key_name, source_file_name):
    try:
        file_content = s3.get_object(Bucket=bucket_name, Key=key_name)["Body"].read().decode('utf-8')
        # json_content = [cambiar_formato_fechas(json.loads(line), '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d %H:%M:%S') for line in file_content.splitlines()]
        json_content = [json.loads(line) for line in file_content]
        print("Successfully read")
        result['Validation'] = "SUCCESS"
        result['Location'] = os.environ['source_folder_name']
        return result, json_content
    except:
        result['Validation'] = "FAILURE"
        result['Reason'] = "Error while reading Json file in the source bucket"
        result['Location'] = os.environ['source_folder_name']
        print('Error while reading Json')
        return result, None
    

def transform(json_content, result, source_file_name):
    
    data = []
    
    periods = [period for period in json_content if json_content['event'] == "create"]
    registers = [event for event in json_content if json_content['event'] == "register" or "update" or "deregister"]
    updates = [event for event in json_content if json_content['event'] == "register" or "update" or "deregister"]
    deregisters = [event for event in json_content if json_content['event'] == "register" or "update" or "deregister"]
    
    print(period)
    print(registers)
    print(updates)
    print(deregisters)

    df_periods = pd.DataFrame()
    df_registers = pd.DataFrame()
    df_updates = pd.DataFrame()
    df_deregisters = pd.DataFrame()
    # {"event":"create","on":"operating_period","at":"2019-06-01T18:17:03.087Z","data":{"id":"op_2","start":"2019-06-01T18:17:04.079Z","finish":"2019-06-01T18:22:04.079Z"},"organization_id":"org-id"}
    for period in periods:
        df_periods.append(pd.json_normalize(period, record_path =['data']))
    data.append(df_periods)
    for register in registers:
        df_registers.append(pd.json_normalize(register, record_path =['data']))
    data.append(df_registers)
    for update in updates:
        df_updates.append(pd.json_normalize(update, record_path =['data']))
    data.append(df_updates)
    for deregister in deregisters:
        df_deregisters.append(pd.json_normalize(deregister, record_path =['data']))
    data.append(df_deregisters)

    information = information.append(pd.DataFrame(registers), ignore_index=True)
    print(information)
    return information
    

def load(json_content, transformed_key):
    # json_content.to_json(transformed_file_name,
    #               compression='gzip')
    df = pd.json_normalize(json_content)

    # Upload the JSON string to S3
    df.to_csv(transformed_key, index=True)
    #s3.put_object(Bucket=bucket_name, Key=key_transform, Body=df)
    #s3_resource.Object(bucket_name, key_name).delete()
    print("Successfuly moved file to  : " + transformed_key)

def cambiar_formato_fechas(data, formato_actual, nuevo_formato, nivel=1):
    for k, v in data.items():
        if isinstance(v, dict) and nivel < 4:
            cambiar_formato_fechas(v, formato_actual, nuevo_formato, nivel+1)
        elif isinstance(v, str):
            try:
                fecha = datetime.strptime(v, formato_actual)
                nueva_fecha = datetime.strftime(fecha, nuevo_formato)
                data[k] = nueva_fecha
            except ValueError:
                pass
    return(data)
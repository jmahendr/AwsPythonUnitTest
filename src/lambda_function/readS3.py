import boto3
import json
from datetime import datetime
import os
import re

# Receive event from S3 object create
# Check for file pattern against bucket name and pattern provided in env variables.
# If pattern matches, invoke stepfunction with the connection parameters provided


def lambda_handler(event, context):
    stateMcArn = os.environ['stateMcArn']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    vots_bucket = os.environ['vots_bucket']
    landata_bucket = os.environ['landata_bucket']
    vots_file_pattern = os.environ['vots_file_pattern']
    landata_file_pattern = os.environ['landata_file_pattern']

    landata = False
    vots = False

    input_data = {}
    print('## EVENT')
    print(event)

    if(bucket == landata_bucket):
        print("matched landata bucket")
        landata = re.match(landata_file_pattern, object_key)
    if(bucket == vots_bucket):
        print("matched vots bucket")
        vots = re.match(vots_file_pattern, object_key)

    if(landata):
        input_data['type'] = 'landata'
        input_data['user_name'] = os.environ['landata_user_name']
        input_data['private_key'] = os.environ['landata_private_key']
        input_data['passphrase'] = os.environ['landata_passphrase']
        input_data['dest_dir'] = os.environ['landata_dest_dir']
        print("Processing landata file")
    elif(vots):
        input_data['type'] = 'vots'
        input_data['user_name'] = os.environ['vots_user_name']
        input_data['private_key'] = os.environ['vots_private_key']
        input_data['passphrase'] = os.environ['vots_passphrase']
        input_data['dest_dir'] = os.environ['vots_dest_dir']
        print("Processing vots file")

    if(not vots and not landata):
        print("File pattern does not match landata/vots")
        return

    now = datetime.now()
    exeName = input_data['type'] + '-transfer-' + now.strftime("%Y%m%d_%H%M%S")

    input_data['bucket'] = bucket
    input_data['pgpPublicKey_param'] = os.environ['pgp_public_key']
    input_data['archive_dir'] = os.environ['archive_dir']
    input_data['source_object_key'] = object_key
    input_data['host_name'] = os.environ['host_name']
    input_data['message'] = bucket+'/'+object_key + \
        ' initiated for encryption and transmission. Step Function Execution: ' + exeName

    print(json.dumps(input_data))

    client = boto3.client('stepfunctions')
    client.start_execution(
        stateMachineArn=stateMcArn,
        name=exeName,
        input=json.dumps(input_data)
    )

    body_text = 'Initiated Step Function with ' + json.dumps(input_data)
    print(body_text)
    return body_text

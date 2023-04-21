import pdb
import json
import boto3


access_key = None
secret_access_key = None
region = None

# load AWS security credentials and region from JSON file
try:
    with open('aws_details.json', 'r') as f:
        aws_details = json.load(f)
        access_key = aws_details['aws_access_key_id']
        secret_access_key = aws_details['aws_secret_access_key']
        region = aws_details['region']
except:
    print("JSON file not found! Make sure that aws_details.json exists!")

# start session with AWS SDK
try:
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )
    sts_client = session.client('sts')
    identity = sts_client.get_caller_identity()
    print('Success!')
except:
    print("Error! Could not connect to AWS! Make sure the credentials are correct.")


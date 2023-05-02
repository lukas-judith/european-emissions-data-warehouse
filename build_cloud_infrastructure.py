import pdb
import json
from time import sleep
import boto3
import random

import utils

from aws_service_classes import *


# names of the AWS resources
# NOTE: if you change the names here, you must also change the corresponding resource names
# in the policy documents (JSON files in the config folder)
VPC_NAME = 'data-warehouse-vpc'
# include random strings to ensure globally unique bucket names 
BUCKET_NAME_SOURCE = 'raw-data-bucket-1405480'
BUCKET_NAME_SINK = 'processed-data-bucket-1405480'
BUCKET_NAME_SCRIPT ='script-bucket-1405480'
RDS_NAME = 'data-warehouse'
LAMBDA_NAME_RDS = 's3-to-rds-lambda'
LAMBDA_NAME_GLUE = 's3-to-glue-lambda'
GLUE_JOB_NAME = 'etl-glue-job'

# paths of the config JSON files for IAM policies and security groups
SECURITY_GROUP_RDS_PATH = 'configs/security_groups/rds_security_group.json'
LAMBDA_RDS_POLICY_PATH = 'configs/IAM_roles/lambda_S3_to_RDS_policy.json'
LAMBDA_GLUE_POLICY_PATH = 'configs/IAM_roles/lambda_S3_to_Glue_policy.json'
GLUE_JOB_POLICY_PATH = 'configs/IAM_roles/glue_job_policy.json'

# load AWS security credentials and region from JSON file
access_key = None
secret_access_key = None
region = None

try:
    with open('aws_details.json', 'r') as f:
        aws_details = json.load(f)
        access_key = aws_details['aws_access_key_id']
        secret_access_key = aws_details['aws_secret_access_key']
        region = aws_details['region']
except:
    print("JSON file not found! Make sure that aws_details.json exists!")
    exit()

# start session with AWS SDK
try:
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )
    sts_client = session.client('sts')
    account_id = sts_client.get_caller_identity()['Account']
    print(f'Successfully connected to AWS account with account ID {account_id}!')
except:
    print("Error! Could not connect to AWS! Make sure the credentials are correct.")
    exit()

# create a collection of AWS services to make it easier to delete all of them later
AWS_architecture = AWSServiceCollection()
print('\nBuilding cloud infrastructure...')

# create VPC with 2 subnets
num_subnets = 2
vpc = VPC(session, region, num_subnets, VPC_NAME, collections=[AWS_architecture])
# using multiple subnets across different availability zones (AZs) 
# is recommended for RDS instance to achieve higher availability
subnets = [comp for comp in vpc.components if isinstance(comp, Subnet)]

# create S3 buckets for data and script storage (object storage)
bucket_script = S3Bucket(session, region, BUCKET_NAME_SCRIPT, collections=[AWS_architecture])
bucket_source = S3Bucket(session, region, BUCKET_NAME_SOURCE, collections=[AWS_architecture])
bucket_sink = S3Bucket(session, region, BUCKET_NAME_SINK, collections=[AWS_architecture])

# upload the scripts for the ETL process and Lambda handlers to an S3 bucket
etl_script_object_key = 'etl_process.py'
lambda_script_object_key = 'lambda_handlers.py'
bucket_script.upload_data('scripts/etl_process.py', etl_script_object_key)
bucket_script.upload_data('scripts/lambda_handlers.py', lambda_script_object_key)
# full paths of the scripts
etl_script_location = f's3://{bucket_script.name}/{etl_script_object_key}'
lambda_script_location = f's3://{bucket_script.name}/{lambda_script_object_key}'


# create AWS Glue job with associated IAM role
glue_job_policy = IAMPolicy(session, f'{GLUE_JOB_NAME}-policy', GLUE_JOB_POLICY_PATH,
                                region, account_id, collections=[AWS_architecture])

glue_job_role = IAMRole(session, f'{GLUE_JOB_NAME}-role', 'glue.amazonaws.com',
                        policies=[glue_job_policy], collections=[AWS_architecture])

etl_glue_job = AWSGlueJob(session, region, 'etl-glue-job', glue_job_role,
                          etl_script_location, collections=[AWS_architecture])


# create security group as component of the VPC
rds_security_group = SecurityGroup(session, region, vpc.id, f'{RDS_NAME}_security_group',
                                   description=f"Security Group for {RDS_NAME} RDS Instance", 
                                   json_file=SECURITY_GROUP_RDS_PATH, collections=[vpc])


# # create PostgreSQL RDS instance as data warehouse
# data_warehouse = RDSInstance(session, region, RDS_NAME, 'aws_details.json',
#                              security_groups=[rds_security_group], subnets=subnets,
#                              collections=subnets)


# TODO: sort AWS architecture component list to ensure roles are deleted before policies etc.

print("\nFinished setting up cloud architecture!")
print("You can now upload new data by typing ...\n") 

POSSIBLE_ANSWERS =  ['delete me', 'exit']
ans = None

print("When you are ready to delete the architecture, type 'delete me'.")
print("If you want to remove the components later manually, type 'exit'.")

while ans not in POSSIBLE_ANSWERS:
    ans = input('> ')

    # delete all AWS components
    # limit for how may times the script can attempt to delete resources
    limit = 3
    c = 0
    if ans.lower() == 'delete me':
        print('')
        first_deletion_attempt = True
        # loop in case any deletion fails on first attempt
        while not AWS_architecture.empty:
            if c > limit:
                print("Could not delete all AWS resources!")
                print("Please delete the following components manually:")
                AWS_architecture.list()
                exit()
            c += 1
            if not first_deletion_attempt:
                print('\nTrying again to delete remaining components...')
            AWS_architecture.delete_components()
            first_deletion_attempt = False

        print('\nAll deleted successfully!\nDone!')
        exit()

    elif ans.lower() == 'exit':
        print("Exiting without deleting all AWS resources!")
        print("Please remember to delete the following components later:")
        AWS_architecture.list()
        exit()
    
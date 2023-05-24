import json
import boto3
from time import sleep

# wrapper classes to facilitate use of the AWS SDK
from aws_service_classes import *
from data_downloader import download_emission_data


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
DATA_OBJECT_KEY = 'emission_data.csv'
PROCESSED_DATA_OBJECT_KEY = 'processed_data'

# paths of the config JSON files for IAM policies and security groups
SECURITY_GROUP_RDS_PATH = 'configs/security_groups/rds_security_group.json'
LAMBDA_RDS_POLICY_PATH = 'configs/IAM_roles/lambda_S3_to_RDS_policy.json'
LAMBDA_GLUE_POLICY_PATH = 'configs/IAM_roles/lambda_S3_to_Glue_policy.json'
GLUE_JOB_POLICY_PATH = 'configs/IAM_roles/glue_job_policy.json'


def create_lambda_function_with_retry(function_name, creation_params, collection, max_attempts=2, delay=10):
    """
    Creates a lambda function with specified parameters, retrying several times if the first attempt fails.
    """
    c = 0
    first_attempt = True
    # while loop until Lambda function is created or maximum number of attempts is exhausted
    while not collection.contains_resource(name=function_name):
        if not first_attempt:
            print('Trying again...')
        if c >= max_attempts:
            print("Could not create Lambda function!")
            return
            # print("Aborting...")
            # collection.delete_components_with_retry()
            # exit()
        first_attempt = False
        c += 1
        sleep(delay)
        # create lambda function that is triggered upon .csv upload to an S3 bucket
        # specify name of Glue job for Lambda function's variables
        lambda_func_obj = S3LambdaFunction(*creation_params)

    return lambda_func_obj


def build_cloud_infrastructure(credentials_file='aws_details.json'):
    """
    Creates an AWS cloud infrastructure for the data warehouse, including VPC (and it's components),
    S3 buckets, RDS instance with PostgreSQL, AWS Glue ETL job, and Lambda functions for automation.
    If the process is terminated by an unknown error or KeyboardInterrupt, the infrastructure will be
    deleted automatically, so that the user does not have do this manually.

    After setting up, starts an interactive session in the command line that allows upload of data 
    and automatic deletion of the infrastructure.
    
    Args:
        credentials_file (str): path to JSON file containing AWS user credentials (access key and secret key)
        as well as self-defined username and password for the PostgreSQL database.
    """
    #######################################
    # START BUILDING CLOUD INFRASTRUCTURE
    #######################################
    # load AWS security credentials and region from JSON file
    access_key = None
    secret_access_key = None
    region = None

    try:
        with open(credentials_file, 'r') as f:
            aws_details = json.load(f)
            access_key = aws_details['aws_access_key_id']
            secret_access_key = aws_details['aws_secret_access_key']
            region = aws_details['region']
    except:
        print(f"JSON file not found! Make sure that {credentials_file} exists!")
        exit()

    # start session with AWS SDK
    try:
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
        sts_client = session.client('sts')
        account_id = sts_client.get_caller_identity()['Account']
        print(f'\nSuccessfully connected to AWS account with account ID {account_id}!')
    except:
        print("Error! Could not connect to AWS! Make sure the credentials are correct.")
        exit()

    # create a collection of AWS services to make it easier to delete all of them later
    AWS_architecture = AWSServiceCollection()
    
    print("WARNING: the current version of this account is using services not eligible for the free tier!")
    print("If you continue, you may be charged by AWS. For more information, see the README.md file.")
    
    cont = input("Continue? [yes]/no ")
    if cont.lower() in ['yes', '']:
        print('\nBuilding cloud infrastructure...')        
    else:
        print('Exiting')
        exit()

    try:
        #############
        # CREATE VPC
        #############
        # create VPC with 2 subnets
        num_subnets = 2
        vpc = VPC(session, region, num_subnets, VPC_NAME, collections=[AWS_architecture])
        # using multiple subnets across different availability zones (AZs) 
        # is recommended for RDS instance to achieve higher availability
        subnets = [comp for comp in vpc.components if isinstance(comp, Subnet)]

        # create a custom routing table and internet gateway as components of the VPC
        routing_table = RoutingTable(session, region, vpc.id, subnets, collections=[vpc])
        internet_gateway = InternetGateway(session, region, vpc_id=vpc.id, collections=[vpc])
        # allow all internet traffic into public subnets
        routing_table.add_route(internet_gateway.id, '0.0.0.0/0')


        #######################################
        # CREATE DATA WAREHOUSE (RDS INSTANCE)
        #######################################
        # Note: creating the RDS instance takes the longest, so create it first to save some time
        # create security group as component of the VPC
        rds_security_group = SecurityGroup(session, region, vpc.id, f'{RDS_NAME}_security_group',
                                        description=f"Security Group for {RDS_NAME} RDS Instance", 
                                        json_file=SECURITY_GROUP_RDS_PATH, collections=[vpc])

        # create PostgreSQL RDS instance as data warehouse
        data_warehouse = RDSInstance(session, region, RDS_NAME, 'aws_details.json',
                                    security_groups=[rds_security_group], subnets=subnets,
                                    collections=subnets)

        ####################
        # CREATE S3 BUCKETS
        ####################
        # create S3 buckets for raw and processed data and script storage (object storage)
        bucket_script = S3Bucket(session, region, BUCKET_NAME_SCRIPT, collections=[AWS_architecture])
        bucket_source = S3Bucket(session, region, BUCKET_NAME_SOURCE, collections=[AWS_architecture])
        bucket_sink = S3Bucket(session, region, BUCKET_NAME_SINK, collections=[AWS_architecture])

        # upload the scripts for the ETL process and Lambda handlers to an S3 bucket
        glue_script_etl = 'etl_process.py'
   
        # create deployment package for Lambda function, including Python script and it's dependencies
        lambda_zip_etl = 'lambda_etl_depl_pkg.zip'
        lambda_zip_warehouse = 'lambda_warehouse_depl_pkg.zip'

        # paths of the ETL script and Lambda deployment packages
        path_dict = {
            'ETL script' : glue_script_etl,
            'ETL Lambda deployment package' : lambda_zip_etl,
            'Warehouse Lambda deployment package' : lambda_zip_warehouse
        }
        # upload the files to a dedicated S3 bucket
        print("Uploading ETL script and Lambda function deployment packages to S3 bucket")
        for name, file in path_dict.items():
            full_path = f'scripts/{file}'
            if os.path.exists(full_path):
                bucket_script.upload_data(full_path, file)
                print(f'Uploaded {full_path} to S3 bucket')
            else:
                print(f"Cannot find {name} path!")
            
        ####################################
        # CREATE ETL PROCESS (AWS GLUE JOB)
        ####################################
        # full path of the ETL script
        glue_script_etl_loc = f's3://{bucket_script.name}/{glue_script_etl}'

        # create associated IAM role
        glue_job_policy = IAMPolicy(session, f'{GLUE_JOB_NAME}-policy', region,
                                    json_file=GLUE_JOB_POLICY_PATH, account_id=account_id,
                                    collections=[AWS_architecture])

        glue_job_role = IAMRole(session, f'{GLUE_JOB_NAME}-role', 'glue.amazonaws.com',
                                policies=[glue_job_policy], collections=[AWS_architecture])

        # specify variables for Glue run
        arguments_glue = {
            '--SOURCE_BUCKET_NAME' : bucket_source.name,
            '--SINK_BUCKET_NAME' : bucket_sink.name,
            # file path to the file containing the data
            '--SOURCE_FILEPATH' : DATA_OBJECT_KEY,
            '--OUTPUT_FOLDER_NAME' : PROCESSED_DATA_OBJECT_KEY
        }

        etl_glue_job = AWSGlueJob(session, region, 'etl-glue-job', glue_job_role, glue_script_etl_loc,
                                variables=arguments_glue, collections=[AWS_architecture])

        #########################################
        # CREATE LAMBDA FUNCTIONS FOR AUTOMATION
        #########################################
        # create IAM role for Lambda function to trigger the Glue job
        glue_lambda_policy = IAMPolicy(session, f'{LAMBDA_NAME_GLUE}-policy', region, 
                                    json_file=LAMBDA_GLUE_POLICY_PATH, account_id=account_id,
                                    collections=[AWS_architecture])

        glue_lambda_role = IAMRole(session, f'{LAMBDA_NAME_GLUE}-role', 'lambda.amazonaws.com',
                                policies=[glue_lambda_policy], collections=[AWS_architecture])

        # create IAM role for Lambda function to transfer processed data into data warehouse (RDS PostgreSQL database)
        rds_lambda_policy = IAMPolicy(session, f'{LAMBDA_NAME_RDS}-policy', region, 
                                    json_file=LAMBDA_RDS_POLICY_PATH, account_id=account_id,
                                    collections=[AWS_architecture])

        rds_lambda_role = IAMRole(session, f'{LAMBDA_NAME_RDS}-role', 'lambda.amazonaws.com',
                                policies=[rds_lambda_policy], collections=[AWS_architecture])


        # parameters for creating Lambda function for automatically starting ETL job
        glue_lambda_handler = 'lambda_handler_etl.start_glue_job'

        variables_etl_job = {
            'JOB_NAME' : etl_glue_job.name,
            'OUTPUT_FOLDER_NAME' : PROCESSED_DATA_OBJECT_KEY
        }
        etl_lambda_function_params = (session, region, account_id, LAMBDA_NAME_GLUE, glue_lambda_handler,
                                    lambda_zip_etl, BUCKET_NAME_SCRIPT, BUCKET_NAME_SOURCE,
                                    glue_lambda_role, variables_etl_job, [AWS_architecture])
        
        # depending on delay after creating IAM role, creation of Lambda functions may fail on the first try
        # try as many times as necessary, using sleep(...) as artificial delay
        etl_lambda_function = create_lambda_function_with_retry(LAMBDA_NAME_GLUE,
                                                                etl_lambda_function_params,
                                                                AWS_architecture)

        # parameters for creating Lambda function for automatically storing processed data in warehouse
        rds_lambda_handler = 'lambda_handler_warehouse.transfer_processed_data'

        # retrieve assigned host name for the RDS instance (the data warehouse)
        # this may cause some delay as fully booting up the RDS instance can take a while
        data_warehouse.retrieve_hostname()

        # name of a new database to be created on the RDS instance (data warehouse)
        database_name = 'greenhouse_gas_emissions'
        # use typical PostgreSQL port
        port = '5432'
        # create database
        data_warehouse.create_database(database_name, port)
        # install extension that allows RDS instance to access data from S3 bucket
        data_warehouse.install_extension('aws_s3', database_name, port)

        # environment variables for Lambda functions (S3 to warehouse/RDS instance)
        variables_warehouse = {
            'BUCKET_NAME' : bucket_sink.name, # name of bucket containing the processed data
            'OUTPUT_PATH' : PROCESSED_DATA_OBJECT_KEY, # directory that will contain parquet files
            'DB_HOSTNAME' : data_warehouse.hostname,
            'DB_NAME' : database_name,
            'DB_USERNAME' : data_warehouse.username,
            'DB_PASSWORD' : data_warehouse.password,
            'PORT' : port,
            'REGION' : region,
            'ACCESS_KEY' : access_key,
            'SECRET_KEY' : secret_access_key
        }

        # define parameters for a new Lambda function to transfer processed data to warehouse
        warehouse_lambda_function_params = (session, region, account_id, LAMBDA_NAME_RDS, rds_lambda_handler,
                                            lambda_zip_warehouse, BUCKET_NAME_SCRIPT, BUCKET_NAME_SINK,
                                            rds_lambda_role, variables_warehouse, [AWS_architecture])

        # depending on delay after creating IAM role, creation of Lambda functions may fail on the first try
        # try as many times as necessary, using sleep(...) as artificial delay
        warehouse_lambda_function = create_lambda_function_with_retry(LAMBDA_NAME_RDS,
                                                                    warehouse_lambda_function_params,
                                                                    AWS_architecture)

    except (KeyboardInterrupt, Exception) as e:
        # make sure that infrastructure is deleted before quitting the program
        if isinstance(e, KeyboardInterrupt):
            print("\nProgram interrupted! Aborting...")
        elif isinstance(e, Exception):
            print("Unexpected error occured:", e)
            print("Aborting...")
        AWS_architecture.delete_components_with_retry()
        print('\nExiting')
        exit()

    print("Finished setting up cloud infrastructure!\n")

    #######################################################################################
    # CREATE INTERACTIVE SESSION TO ALLOW INTERACTION WITH AND DELETION OF INFRASTRUCTURE
    #######################################################################################

    POSSIBLE_ANSWERS_FOR_EXIT =  ['delete', 'exit']
    ans = None

    print("You can now upload new data by typing 'upload'. This will start an automatic ETL job.\n") 
    print("You can then access the processed data in this PostgreSQL database:")
    print(f"Hostname: {data_warehouse.hostname}")
    print(f"Database name: {database_name}")
    print(f"Username: {data_warehouse.username}")
    print(f"Password: {data_warehouse.password}\n")
    print("When you are ready to delete the architecture, type 'delete'.")
    print("WARNING! This will delete all data that has not been backed up from the cloud!")
    print("If you want to remove the components later manually, type 'exit'.\n")

    # start session using while loop
    while ans not in POSSIBLE_ANSWERS_FOR_EXIT:
        ans = input('> ')

        if ans.lower() == 'delete':
            print('')
            AWS_architecture.delete_components_with_retry()
            print('\nDone!')
            exit()

        elif ans.lower() == 'exit':
            print("\nExiting without deleting all AWS resources!")
            print("Please remember to delete the following components later:")
            AWS_architecture.list()
            exit()

        elif ans.lower() == 'upload':
            # download data from EEA website
            downloaded_data = False
            print("\nDownloading data on greenhouse gas emissions...")
            try:
                filename = download_emission_data()
                downloaded_data = True
            except:
                filename = None
                print("Failed to download data!\n")
            # upload data to cloud infrastructure 
            if downloaded_data:
                print(f"Uploading {filename} to cloud infrastructure...")
                bucket_source.upload_data(filename, DATA_OBJECT_KEY)
                print("You can check the ETL job status under 'AWS Glue > ETL jobs' or 'CloudWatch > Logs' in your AWS account.\n")
                print("After the ETL job has finished, you can access the data in the PostgreSQL database.\n")
        else:
            print(f"\nUnknown command {ans}! Try again!\n")
        


if __name__ == '__main__':
    # create AWS cloud infrastructure for data warehouse 
    # and start interactive session in command line
    build_cloud_infrastructure()
import abc
import json
import os
import boto3
import botocore
import pdb

import numpy as np

from utils import find_optimal_number_of_AZs


def handle_exceptions(service_type, operation):
    """
    Decorator for handling exceptions while performing 
    different operations on AWS services.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(f"An error occured when trying to {operation} {service_type}:\n{e}")
        return wrapper
    return decorator


class AWSService(abc.ABC):
    """
    Abstract class for AWS services.
    """
    @abc.abstractmethod
    def __init__(self, session, collections=None, type=None):
        self.session = session
        # type of AWS service
        self.type = type
        # AWSServiceCollection collections to which the AWSService belongs
        self.collections = collections if collections else []
        self.name = None
        self.id = None
        # Availability Zone (AZ)
        self.az = None
        # AWS region
        self.region = None

    @abc.abstractmethod
    def create(self):
        string = f"Created {self.type} with name {self.name} and ID {self.id}"
        if self.region:
            string += f" in region {self.region}"
        if self.az:
            string += f" in Availability Zone {self.az['ZoneName']}"
        print(string)
        # ensure that this service is part of its collections' component lists
        for collection in self.collections:
            if collection and (not self in collection.components):
                collection.add_component(self)
        # flag for successful creation
        return 1

    @abc.abstractmethod
    def delete(self):
        string = f"Deleted {self.type} with name {self.name} and ID {self.id}"
        if self.region:
            string += f" in region {self.region}"
        if self.az:
            string += f" in Availability Zone {self.az['ZoneName']}"
        print(string)
        # remove this object from its collections
        for collection in self.collections:
            if collection and self in collection.components:
                collection.components.remove(self)
        # flag for successful deletion
        return 1


class AWSServiceCollection():
    """
    Class that can contain and delete several AWS services as components.
    """
    def __init__(self):
        self.components = []

    def add_component(self, component : AWSService):
        self.components.append(component)
 
    def add_components(self, components):
        for component in components:
            self.add_component(component)

    def delete_components(self):
        """
        Deletes all AWS compnents and removes them from the components list.
        """
        # loop over copy of self.components list, as the original self.components
        # object will be altered when calling component.delete()
        for component in self.components.copy():
            # first check if the component has components on its own, e.g. a VPC
            # then first delete its components
            if isinstance(component, AWSServiceCollection):
                component.delete_components()
            # if the component is a service, delete it and remove it from list
            if isinstance(component, AWSService):
                deletion_successful = component.delete()
                if deletion_successful:
                    self.components.remove(component)

    @property
    def empty(self):
        # True when list of components is empty
        return len(self.components) == 0
    
    def list(self):
        """
        List all components.
        """
        for comp in self.components:
            string = f"{comp.type} with name {comp.name} and ID {comp.id}"
            if comp.region:
                string += f" in region {comp.region}"
            if comp.az:
                string += f" in Availability Zone {comp.az['ZoneName']}"
            if isinstance(comp, AWSServiceCollection):
                comp.list()
            print(string)


class Subnet(AWSServiceCollection, AWSService):
    """
    Class for subnet of a VPC. Can contain other AWS services/instance
    and is an AWS service itself.
    """
    def __init__(self, session, region, vpc_id, subnet_cidr, az, subnet_name=None,
                 collections=None):
        AWSServiceCollection.__init__(self)
        AWSService.__init__(self, session, collections, type='subnet')
        self.region = region
        self.name = subnet_name
        self.az = az
        self.vpc_id = vpc_id
        self.cidr = subnet_cidr
        self.create()

    @handle_exceptions('subnet', 'create')
    def create(self):
        """
        Create subnet.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        response = ec2.create_subnet(
            VpcId=self.vpc_id,
            AvailabilityZone=self.az['ZoneName'], 
            CidrBlock=self.cidr
        )
        self.vpc_id = self.vpc_id
        self.cidr = self.cidr
        self.id = response['Subnet']['SubnetId']
        super().create()

    @handle_exceptions('subnet', 'delete')
    def delete(self):
        # delete all instances within subnet before deleting VPC itself
        self.delete_components()
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_subnet(SubnetId=self.id)
        super().delete()


class VPC(AWSServiceCollection, AWSService):
    """
    Class for a VPC. Can contain other AWS services/instance
    and is an AWS service itself.
    """
    def __init__(self, session, region, num_subnets, vpc_name=None, 
                 collections=None):
        AWSServiceCollection.__init__(self)
        AWSService.__init__(self, session, collections, type='VPC')
        self.name = vpc_name
        self.region = region
        self.num_subnets = num_subnets
        self.num_azs_used = None
        self.create()
        
    @handle_exceptions('vpc', 'create')
    def create(self):
        """
        Create a virtual private cloud (VPC) with an even specified number of subnets.
        """
        assert self.num_subnets <= 256 # chosen CIDR notation allows for 256 subnets

        ec2 = self.session.client('ec2', region_name=self.region)
        # create VPC with specified IP addresses
        cidr_block = '10.0.0.0/16'
        response = ec2.create_vpc(CidrBlock=cidr_block)
        vpc_id = response['Vpc']['VpcId']
        self.id = vpc_id
        super().create()

        # CREATE SUBNETS
        # retrieve available availability zones (AZs)
        az_response = ec2.describe_availability_zones()
        azs = az_response['AvailabilityZones']

        # given the number of subnets to be created and the number 
        # of AZs available, determine the best number of AZs to use
        # in order to ensure high availability (with at least two subnets per AZ)
        n_azs = find_optimal_number_of_AZs(self.num_subnets, len(azs))
        self.num_azs_used = n_azs
        max_n_subnets_per_az = np.ceil(self.num_subnets / n_azs)
        
        for i in range(self.num_subnets):
            # fill up each AZ with subnets until limit is reached
            # as calculated above
            az_idx = int(i // max_n_subnets_per_az)
            az = azs[az_idx]
            # specify CIDR block for subnet
            # note: this notation allows for up to 256 subnets
            subnet_cidr = f'10.0.{i+1}.0/24'
            # create subnet; automatically gets added to VPC's components list
            # when setting collections=[self]
            subnet = Subnet(self.session, self.region, vpc_id, subnet_cidr,
                            az, collections=[self])
        
    @handle_exceptions('vpc', 'delete')
    def delete(self):
        # delete all instances within VPC before deleting VPC itself
        self.delete_components()
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_vpc(VpcId=self.id)
        super().delete()


class IAMPolicy(AWSService):
    """
    Role for IAM policy.
    """
    def __init__(self, session, name, json_file, region=None, account_id=None, collections=None):
        super().__init__(session, collections, type='IAM policy')
        self.name = name
        # pass region and account ID if these are contained in the resource name
        # as specified in the policy document (JSON file)
        self.create(json_file, region, account_id)
        
    @handle_exceptions('IAM policy', 'create')
    def create(self, json_file, region, account_id):
        """
        Create an IAM policy from a policy document in JSON format.
        """
        iam = self.session.client('iam')

        with open(json_file, 'r') as file:
            policy_doc = json.dumps(json.load(file))

        # enter correct region and account ID into the policy document
        if 'REGION' in policy_doc:
            policy_doc = policy_doc.replace('REGION', region)
        if 'ACCOUNT_ID' in policy_doc:
            policy_doc = policy_doc.replace('ACCOUNT_ID', account_id)

        response = iam.create_policy(
            PolicyName=self.name,
            PolicyDocument=policy_doc
        )
        self.id = response['Policy']['Arn']
        super().create()

    @handle_exceptions('IAM policy', 'delete')
    def delete(self):
        iam = self.session.client('iam')
        iam.delete_policy(PolicyArn=self.id)
        super().delete()


class IAMRole(AWSService):
    """
    Role for IAM role.
    """
    def __init__(self, session, name, aws_service, policies, collections=None):
        super().__init__(session, collections, type='IAM role')
        self.name = name
        self.policies = policies
        self.aws_service = aws_service
        self.create()
        
    # TODO: write separate function allowing to add more policies to role
    @handle_exceptions('IAM role', 'create')
    def create(self):
        """
        Create an IAM role for a specified service that allows specified actions.
        """
        iam = self.session.client('iam')

        assume_role_policy_doc = {
            "Version" : "2012-10-17",
            "Statement" : [ # array of statements
                {    
                    "Action" : "sts:AssumeRole",
                    "Effect" : "Allow",
                    "Principal" : {
                        "Service": self.aws_service
                    }
                }
            ]
        }
        # make API call to IAM service to create IAM role
        response = iam.create_role(
            RoleName = self.name,
            AssumeRolePolicyDocument = json.dumps(assume_role_policy_doc)
        )
        # attach policies to the role
        for policy in self.policies:
            arn = policy.id
            iam.attach_role_policy(
                RoleName = self.name, 
                PolicyArn = arn
            )
        # get Amazon resource name (ARN) from the server's response
        self.id = response['Role']['Arn']
        super().create()

    @handle_exceptions('IAM role', 'delete')
    def delete(self):
        iam = self.session.client('iam')
        # detach policies before deleting the IAM role
        for policy in self.policies:
            arn = policy.id
            iam.detach_role_policy(
                RoleName=self.name,
                PolicyArn=arn
            )
        iam.delete_role(RoleName=self.name)
        super().delete()


class SecurityGroup(AWSService):

    def __init__(self, session, region, vpc_id, name, description, json_file=None, collections=None):
        super().__init__(session, collections, type='security group')
        self.name = name
        self.region = region
        self.vpc_id = vpc_id
        self.description = description
        self.create(json_file)

    @handle_exceptions('security group', 'create')
    def create(self, json_file=None):
        """
        Create security group within specified VPC and with specified rules.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        
        response = ec2.create_security_group(
            GroupName=self.name,
            Description=self.description,
            VpcId = self.vpc_id
        )
        security_group_id = response['GroupId']
        self.id = security_group_id

        if json_file is not None:
            # load IP permission for security group from JSON file
            with open(json_file, 'r') as file:
                rules = json.load(file)

            # attach inbound and outbound rules to the security group
            for permission, direction in zip(
                    rules["IpPermissions"], 
                    rules["directions"]
                ):
                if direction == 'inbound':
                    ec2.authorize_security_group_ingress(
                        GroupId=security_group_id,
                        IpPermissions=[permission]
                    )
                elif direction == 'outbound': 
                    ec2.authorize_security_group_egress(
                        GroupId=security_group_id,
                        IpPermissions=[permission]
                    )
        super().create()
            
    @handle_exceptions('security group', 'delete')
    def delete(self):
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_security_group(GroupId=self.id)
        super().delete()
    
    # TODO: write this method
    def add_rules(self, rules):
        pass


class S3Bucket(AWSService):

    def __init__(self, session, region, name, collections=None):
        super().__init__(session, collections, type='S3 bucket')
        self.region = region
        # bucket name is globally unique identifier for the bucket
        self.name = name
        self.create()
        
    @handle_exceptions('S3 bucket', 'create')
    def create(self):
        """
        Create S3 bucket in specified region.
        """
        s3 = self.session.client('s3', region_name=self.region)
        response = s3.create_bucket(
            Bucket=self.name,
            CreateBucketConfiguration={
                'LocationConstraint' : self.region
            }
        )
        super().create()

    @handle_exceptions('S3 bucket', 'delete data from')
    def delete_all_objects(self):
        """
        Delete all objects stored in the bucket.
        """
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(self.name)
        for object in bucket.objects.all():
            object.delete()

    @handle_exceptions('S3 bucket', 'delete')
    def delete(self):
        # before deleting the bucket, all of its objects need to be deleted
        self.delete_all_objects()
        s3 = self.session.client('s3', region_name=self.region)
        s3.delete_bucket(Bucket=self.name)
        super().delete()

    @handle_exceptions('S3 bucket', 'upload data to')
    def upload_data(self, file_path, object_key):
        """
        Upload a local file to the bucket.
        """
        if not os.path.exists(file_path):
            raise ValueError(f"File {file_path} does not exist.")

        s3 = self.session.client('s3', region_name=self.region)
        with open(file_path, 'rb') as file:
            s3.put_object(Bucket=self.name, Key=object_key, Body=file)

    @handle_exceptions('S3 bucket', 'load data from')
    def get_data(self, object_key, destination_path):
        """
        Retrieve data from the bucket and save it to a local file.
        """
        s3 = self.session.client('s3', region_name=self.region)
        with open(destination_path, 'wb') as file:
            s3.download_fileobj(self.name, object_key, file)


class RDSInstance(AWSService):

    def __init__(self, session, region, name, credentials_file, security_groups,
                 subnets, instance_class='db.t3.micro', collections=None):
        super().__init__(session, collections, type='RDS instance')
        self.name = name
        self.region = region
        # load master username and password from JSON file
        with open(credentials_file, 'r') as f:
            configs = json.load(f)
        # username and password specified in the credentials file
        self.username = configs['DB_username']
        self.password = configs['DB_password']
        self.security_groups = security_groups
        self.subnets = subnets
        self.instance_class = instance_class
        self.create()

    @handle_exceptions('RDS instance', 'create')
    def create(self):
        rds = self.session.client('rds', region_name=self.region)
        security_group_ids = [sg.id for sg in self.security_groups]
        subnet_ids = [sn.id for sn in self.subnets]

        # create group of subnets in which RDS instance resides
        db_subnet_group_name = 'data-warehouse-subnet-group'
        self.db_subnet_group_name = db_subnet_group_name

        response = rds.create_db_subnet_group(
            DBSubnetGroupName=db_subnet_group_name,
            DBSubnetGroupDescription='DB subnet group for data warehouse',
            SubnetIds=subnet_ids
        )
        response = rds.create_db_instance(
            DBInstanceIdentifier=self.name,
            AllocatedStorage=20, # 20 GB storage
            DBInstanceClass=self.instance_class,
            Engine='postgres',
            MasterUsername=self.username,
            MasterUserPassword=self.username,
            VpcSecurityGroupIds=security_group_ids,
            DBSubnetGroupName=db_subnet_group_name,
            # when subnets in different AZs are provided, 
            # create standby replica and provide automatic failover support
            MultiAZ=True, 
            StorageType='gp2', # standard general purpose storage type
            PubliclyAccessible=False,
        )
        self.id = response['DBInstance']['DBInstanceArn']
        super().create()

    @handle_exceptions('RDS instance', 'delete')
    def delete(self):
        """
        Delete RDS instance and the associated subnet group.
        """
        rds = self.session.client('rds', region_name=self.region)
        # delete RDS instance
        rds.delete_db_instance(
            DBInstanceIdentifier=self.name,
            SkipFinalSnapshot=True
        )
        # create Waiter object to make sure RDS instance is deleted
        # before proceeding
        print("Waiting for deletion of RDS instance...")
        waiter = rds.get_waiter('db_instance_deleted')
        waiter.wait(DBInstanceIdentifier=self.name)
        # after RDS instance is deleted, can remove subnet group
        rds.delete_db_subnet_group(
            DBSubnetGroupName=self.db_subnet_group_name,
        )
        super().delete()
        

class AWSGlueJob(AWSService):

    def __init__(self, session, region, name, role, script_location, collections=None):
        super().__init__(session, collections, type='AWS Glue job')
        self.name = name
        self.role = role
        self.region = region
        # S3 bucket location of the format s3://{bucket-name}/{object-key}
        self.script_location = script_location
        self.create()

    @handle_exceptions('AWS Glue job', 'create')
    def create(self):   
        glue_client = self.session.client('glue', region_name=self.region)

        glue_client.create_job(
            Name=self.name,
            Description='ETL process',
            Role=self.role.id,
            ExecutionProperty={
                'MaxConcurrentRuns': 2
            },
            Command={
                'Name': 'etl_process',
                'ScriptLocation': self.script_location
            },
            DefaultArguments={
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-disable'
            },
            GlueVersion='2.0',
            WorkerType='Standard',
            NumberOfWorkers=2,
            Timeout=300
        )
        super().create()

    @handle_exceptions('AWS Glue job', 'delete')
    def delete(self):
        glue_client = self.session.client('glue', region_name=self.region)
        glue_client.delete_job(JobName=self.name)
        super().delete()
        

class S3LambdaFunction(AWSService):
    """
    Class of Lambda function triggered by S3 bucket upload of .csv file.
    """
    def __init__(self, session, region, name, handler, role, bucket_name, variables=None,
                 collections=None):
        super().__init__(session, collections, type='Lambda function')
        self.name = name
        self.region = region
        self.handler = handler
        self.role = role
        self.bucket_name = bucket_name
        self.create(variables)

    @handle_exceptions('Lambda function', 'create')  
    def create(self, variables):
        lambda_client = self.session.client('lambda', region_name=self.region)

        # the hander object is a string of the format {script_name}.{function_name}
        script_name, function_name = self.handler.split('.')

        # save python script as bytes to pass it to the lambda function
        with open(f'{script_name}.py', 'rb') as file:
            script_in_bytes = file.read()

        environment_variables = {
            'SESSION' : self.session,
            'BUCKET' : self.bucket_name
        }
        # variable names for the Lambda function, other than session and bucket name 
        for key, value in variables:
            environment_variables[key] = value

        response = lambda_client.create_function(
            FunctionName=self.name,
            Runtime='python3.8',
            Role=self.role.id,
            Handler=self.handler,
            Environment={
                'Variables' : environment_variables
            } ,
            Code={
                'ZipFile': script_in_bytes
            },
            Timeout=3000,
            MemorySize=1024
        )
        self.id = response['FunctionArn']

        s3_client = self.session.client('s3')

        response = s3_client.put_bucket_notification_configuration(
            Bucket=self.bucket_name,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': self.id,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'suffix',
                                        'Value': '.csv'  # trigger for files with .csv extension
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
        super().create()

    @handle_exceptions('Lambda function', 'delete')  
    def delete(self):
        lambda_client = self.session.client('lambda')
        lambda_client.create_function(FunctionName=self.name)
        super().delete()



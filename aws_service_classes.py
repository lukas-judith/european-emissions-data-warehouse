import pdb
import abc
import json
import os
import numpy as np
from time import sleep, time
from sqlalchemy import create_engine, text

from utils import find_optimal_number_of_AZs, handle_exceptions


class AWSService(abc.ABC):
    """
    Abstract wrapper class for creating, deleting, and interacting with AWS services.
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
        """
        Creates AWS service. To be implemented by respective AWS service wrapper class.
        """
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
        """
        Deletes AWS service. To be implemented by respective AWS service wrapper class.
        """
        string = f"Deleted {self.type} with name {self.name} and ID {self.id}"
        if self.region:
            string += f" in region {self.region}"
        if self.az:
            string += f" in Availability Zone {self.az['ZoneName']}"
        print(string)
        # remove this object from its collections
        for collection in self.collections:
            if collection and (self in collection.components):
                collection.components.remove(self)
        # flag for successful deletion
        return 1


class AWSServiceCollection():
    """
    Class for collection that can contain and easily delete several AWS services.
    """
    def __init__(self):
        self.components = []

    def add_component(self, component : AWSService):
        """
        Adds new component (AWSService instance) to the collection and this collection
        to the list of collections of the respective component (if not already the case).
        """
        if not component in self.components:
            self.components.append(component)
        if not self in component.collections:
            component.collections.append(self)
 
    def add_components(self, components):
        """
        Adds several components to the collection.
        """
        for component in components:
            self.add_component(component)

    @property
    def empty(self):
        """
        Checks if collection is empty.
        """
        # True when list of components is empty
        return len(self.components) == 0

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
    
    def delete_components_with_retry(self, max_attempts=3):
        """
        Deletes all AWS components in the AWS service collection.
        If deletion fails upon first attempt, tries again.
        """
        c = 0
        first_deletion_attempt = True
        # loop in case any deletion fails on first attempt
        while not self.empty:
            if c > max_attempts:
                print("Could not delete all AWS resources!")
                print("Please delete the following components manually:")
                self.list()
                return
            c += 1
            if not first_deletion_attempt:
                print('\nTrying again to delete remaining components...')
            self.delete_components()
            first_deletion_attempt = False
        print("\nAll components deleted successfully!")
    
    def contains_resource(self, name=None, id=None):
        """
        Checks if the collection includes a resource with given name or ID.
        """
        if not name and not id:
            raise Exception("You must specify either ID or name of the resource!")
        for comp in self.components:
            if name and name == comp.name:
                return True
            if id and id == comp.id:
                return True
        return False

    def list(self):
        """
        Lists all components.
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
    Wrapper class for subnet of a VPC. Can contain other AWS services/instance
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
        """
        Deletes subnet and all of its components.
        """
        # delete all instances within subnet before deleting subnet itself
        self.delete_components()
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_subnet(SubnetId=self.id)
        super().delete()


class VPC(AWSServiceCollection, AWSService):
    """
    Wrapper class for a VPC. Can contain other AWS services/instance
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
        Creates a virtual private cloud (VPC) with an even specified number of subnets.
        """
        assert self.num_subnets <= 256 # chosen CIDR notation allows for 256 subnets

        ec2 = self.session.client('ec2', region_name=self.region)
        # create VPC with specified IP addresses
        cidr_block = '10.0.0.0/16'
        response = ec2.create_vpc(CidrBlock=cidr_block)
        vpc_id = response['Vpc']['VpcId']
        self.id = vpc_id

        # enable DNS hostname and DNS resolution (otherwise RDS instance cannot be public)
        ec2.modify_vpc_attribute(
            EnableDnsHostnames={
                'Value': True
            },
            VpcId=self.id,
        )
        ec2.modify_vpc_attribute(
            EnableDnsSupport={
                'Value': True
            },
            VpcId=self.id,
        )
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
        """
        Deletes VPC and all of its components.
        """
        # delete all instances within VPC before deleting VPC itself
        self.delete_components()
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_vpc(VpcId=self.id)
        super().delete()


class IAMPolicy(AWSService):
    """
    Wrapper class for IAM policy. Can be created from existing policy ARN
    or from a new JSON permissions file.
    """
    def __init__(self, session, name, region=None, arn=None, json_file=None,
                 account_id=None, collections=None):
        super().__init__(session, collections, type='IAM policy')
        self.name = name
        # pass region and account ID if these are contained in the resource name
        # as specified in the policy document (JSON file)
        if not arn and not json_file:
            raise Exception("Error! Must supply either JSON file or existing policy ARN!")
        if json_file:
            self.create(json_file, region, account_id)
        elif arn:
            self.id = arn
        
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
        """
        Deletes IAM policy.
        """
        iam = self.session.client('iam')
        iam.delete_policy(PolicyArn=self.id)
        super().delete()


class IAMRole(AWSService):
    """
    Wrapper class for IAM role.
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
        Creates an IAM role for a specified service that allows specified actions.
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
        """
        Detaches policies from IAM role and then deletes role.
        """
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
    """
    Wrapper class for security groups.
    """
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
        Creates security group within specified VPC and with specified rules.
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
        """
        Delete security group.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.delete_security_group(GroupId=self.id)
        super().delete()
    

class S3Bucket(AWSService):
    """
    Wrapper class for AWS simple storage service (S3 Bucket).
    """
    def __init__(self, session, region, name, collections=None):
        super().__init__(session, collections, type='S3 bucket')
        self.region = region
        # bucket name is globally unique identifier for the bucket
        self.name = name
        self.create()
        
    @handle_exceptions('S3 bucket', 'create')
    def create(self):
        """
        Creates S3 bucket in specified region.
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
        Deletes all objects stored in the bucket.
        """
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(self.name)
        for object in bucket.objects.all():
            object.delete()

    @handle_exceptions('S3 bucket', 'delete')
    def delete(self):
        """
        Empties and deletes S3 bucket.
        """
        # before deleting the bucket, all of its objects need to be deleted
        self.delete_all_objects()
        s3 = self.session.client('s3', region_name=self.region)
        s3.delete_bucket(Bucket=self.name)
        super().delete()

    @handle_exceptions('S3 bucket', 'upload data to')
    def upload_data(self, file_path, object_key):
        """
        Uploads a local file to the bucket.
        """
        if not os.path.exists(file_path):
            raise ValueError(f"File {file_path} does not exist.")

        s3 = self.session.client('s3', region_name=self.region)
        with open(file_path, 'rb') as file:
            s3.put_object(Bucket=self.name, Key=object_key, Body=file)

    @handle_exceptions('S3 bucket', 'load data from')
    def get_data(self, object_key, destination_path):
        """
        Retrieves data from the bucket and save it to a local file.
        """
        s3 = self.session.client('s3', region_name=self.region)
        with open(destination_path, 'wb') as file:
            s3.download_fileobj(self.name, object_key, file)


class RDSInstance(AWSService):
    """
    Wrapper class for RDS instance with PostgreSQL engine.
    """
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
        # need to explicitly retrieve host name later, after RDS instance has booted up
        self.hostname = None

    @handle_exceptions('RDS instance', 'create')
    def create(self):
        """
        Creates RDS instance and attach security groups and IAM roles (if specified).
        """
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
            MasterUserPassword=self.password,
            VpcSecurityGroupIds=security_group_ids,
            DBSubnetGroupName=db_subnet_group_name,
            # when subnets in different AZs are provided, 
            # create standby replica and provide automatic failover support
            MultiAZ=True, 
            StorageType='gp2', # standard general purpose storage type
            PubliclyAccessible=True,
        )
        self.id = response['DBInstance']['DBInstanceArn']
        super().create()

    @handle_exceptions('RDS instance', 'retrieve the host name')
    def retrieve_hostname(self):
        """
        Waits for instance to become availble and then retrieve the assigned host name.
        """
        rds = self.session.client('rds', region_name=self.region)
        # Wait until the RDS instance is available
        print("Waiting until the RDS instance is available (this may take several minutes)...")
        waiter = rds.get_waiter('db_instance_available')
        # wait for 30 minutes max.
        tic = time()
        waiter.wait(
            DBInstanceIdentifier=self.name,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        # Once available, retrieve the instance information
        response = rds.describe_db_instances(DBInstanceIdentifier=self.name)
        toc = time()
        diff = toc-tic
        mins = int(diff // 60)
        secs = diff % 60
        print(f"Completed after {mins} minutes and {secs:.2f} seconds")
        # Store the endpoint (hostname) as an attribute
        self.hostname = response['DBInstances'][0]['Endpoint']['Address']

    #@handle_exceptions('RDS instance', 'create database on')
    def create_database(self, dbname, port='5432'):
        """
        Creates new database on RDS instance.
        """
        if not self.hostname:
            raise Exception('Error! No hostname found!')
        
        # uri for connecting to default database (postgres) in order to create a new database
        db_uri = f'postgresql://{self.username}:{self.password}@{self.hostname}:{port}/postgres'
    
        # create SQLAlchemy engine and create new database
        engine = create_engine(db_uri)

        with engine.connect() as connection:
            connection.execute(text("commit"))
            connection.execute(text(f"CREATE DATABASE {dbname};"))
        print(f"Created new database {dbname}")
        
    @handle_exceptions('RDS instance', 'install extension for')
    def install_extension(self, ext_name, dbname, port='5432'):
        """
        Installs extension (e.g. for accessing S3 buckets) in PostgreSQL.
        """
        if not self.hostname:
            raise Exception('Error! No hostname found!')

        # uri for connecting to specified database in order to create a new database
        db_uri = f'postgresql://{self.username}:{self.password}@{self.hostname}:{port}/{dbname}'
    
        # create SQLAlchemy engine and create new database
        engine = create_engine(db_uri)

        with engine.connect() as connection:
            connection.execute(text("commit"))
            connection.execute(text(f"CREATE EXTENSION IF NOT EXISTS {ext_name} CASCADE;"))
        print(f'Installed {ext_name} extension for RDS instance with name {self.name}')

    @handle_exceptions('RDS instance', 'delete')
    def delete(self):
        """
        Deletes RDS instance and the associated subnet group (without backups).
        """
        rds = self.session.client('rds', region_name=self.region)
        # delete RDS instance
        rds.delete_db_instance(
            DBInstanceIdentifier=self.name,
            SkipFinalSnapshot=True
        )
        # create Waiter object to make sure RDS instance is deleted
        # before proceeding
        print("Waiting for deletion of RDS instance (this may take several minutes)...")
        waiter = rds.get_waiter('db_instance_deleted')
        tic = time()
        waiter.wait(DBInstanceIdentifier=self.name)
        # after RDS instance is deleted, can remove subnet group
        rds.delete_db_subnet_group(
            DBSubnetGroupName=self.db_subnet_group_name,
        )
        toc = time()
        diff = toc-tic
        mins = int(diff // 60)
        secs = diff % 60
        print(f"Completed after {mins} minutes and {secs:.2f} seconds")
        super().delete()
        

class AWSGlueJob(AWSService):
    """
    Wrapper class for AWS Glue jobs.
    """
    def __init__(self, session, region, name, role, script_location,
                 variables=None, collections=None):
        super().__init__(session, collections, type='AWS Glue job')
        self.name = name
        self.role = role
        self.region = region
        # S3 bucket location of the format s3://{bucket-name}/{object-key}
        self.script_location = script_location
        self.create(variables)

    @handle_exceptions('AWS Glue job', 'create')
    def create(self, variables):  
        """
        Creates Glue ETL job. 
        """ 
        glue_client = self.session.client('glue', region_name=self.region)

        # variables/arguments for the job
        job_args = {}
        if variables:
            for key, value in variables.items():
                job_args[key] = value

        glue_client.create_job(
            Name=self.name,
            Description='ETL process',
            Role=self.role.name,
            ExecutionProperty={
                'MaxConcurrentRuns': 2
            },
            Command={
                'Name': 'glueetl',
                'ScriptLocation': self.script_location,
                'PythonVersion': '3'
            },
            DefaultArguments=job_args,
            Timeout=300,
            GlueVersion='3.0',
            NumberOfWorkers=2,
            WorkerType='Standard'#|'G.1X'|'G.2X'|'G.025X'
        )
        super().create()

    @handle_exceptions('AWS Glue job', 'delete')
    def delete(self):
        """
        Deletes Glue ETL job.
        """
        glue_client = self.session.client('glue', region_name=self.region)
        glue_client.delete_job(JobName=self.name)
        super().delete()
        

class S3LambdaFunction(AWSService):
    """
    Wrapper class for AWS Lambda functions triggered by S3 bucket upload of a new file.
    """
    def __init__(self, session, region, account_id, name, handler, script_object_key,
                 bucket_name_script, bucket_name_trigger, role, variables=None,
                 collections=None, python_version='python3.8'):
        super().__init__(session, collections, type='Lambda function')
        self.name = name
        self.region = region
        self.handler = handler
        self.role = role
        self.bucket_name_script = bucket_name_script
        self.bucket_name_trigger = bucket_name_trigger
        self.create(script_object_key, account_id, variables, python_version)

    @handle_exceptions('Lambda function', 'create')  
    def create(self, script_object_key, account_id, variables, python_version='python3.8'):
        """
        Creates AWS Lambda function (x86_64 architecture) with specified deployment package.
        """
        lambda_client = self.session.client('lambda', region_name=self.region)

        environment_variables = {}
        # variable names for the Lambda function to use
        if variables:
            for key, value in variables.items():
                environment_variables[key] = value

        response = lambda_client.create_function(
            FunctionName=self.name,
            Runtime=python_version,
            Role=self.role.id,
            Handler=self.handler,
            Environment={
                'Variables' : environment_variables
            } ,
            Code={
                'S3Bucket': self.bucket_name_script,
                'S3Key' : script_object_key
            },
            Timeout=300,
            MemorySize=1024
        )
        self.id = response['FunctionArn']

        # wait util the Lambda function is set up before proceeding
        waiter = lambda_client.get_waiter('function_active')
        waiter.wait(FunctionName=self.name)

        # add permission that allows the S3 bucket to invoke the Lambda function
        source_bucket_arn = f"arn:aws:s3:::{self.bucket_name_trigger}"
        response = lambda_client.add_permission(
            FunctionName=self.name,
            StatementId='S3Invoke',  
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=source_bucket_arn,
            SourceAccount=account_id
        )

        s3_client = self.session.client('s3')

        response = s3_client.put_bucket_notification_configuration(
            Bucket=self.bucket_name_trigger,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': self.id,
                        'Events': ['s3:ObjectCreated:*'],
                    }
                ]
            }
        )
        super().create()

    @handle_exceptions('Lambda function', 'delete')  
    def delete(self):
        """
        Deletes Lambda function.
        """
        lambda_client = self.session.client('lambda', region_name=self.region)
        lambda_client.delete_function(FunctionName=self.name)
        super().delete()


class InternetGateway(AWSService):
    """
    Wrapper class for managing internet gateways in AWS.
    """
    def __init__(self, session, region, vpc_id, collections=None):
        super().__init__(session, collections, type='internet gateway')
        self.region = region
        self.vpc_id = vpc_id
        self.id = None
        self.create()
        
    @handle_exceptions('internet gateway', 'create')
    def create(self):
        """
        Creates internet gateway and attaches it to VPC.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        response = ec2.create_internet_gateway()
        self.id = response['InternetGateway']['InternetGatewayId']
        # atach to VPC
        ec2.attach_internet_gateway(InternetGatewayId=self.id, VpcId=self.vpc_id)
        super().create()

    @handle_exceptions('internet gateway', 'delete')
    def delete(self):
        """
        Detaches and deletes internet gateway.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.detach_internet_gateway(InternetGatewayId=self.id, VpcId=self.vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=self.id)
        super().delete()
        

class RoutingTable(AWSService):
    """
    Wrapper class for creating and managing routing tables in AWS.
    """
    def __init__(self, session, region, vpc_id, subnets=None, collections=None):
        super().__init__(session, collections, type='routing table')
        self.region = region
        self.vpc_id = vpc_id
        self.id = None
        self.create(subnets)

    @handle_exceptions('routing table', 'create')
    def create(self, subnets=None):
        """
        Creates routing table in VPC and associate it with the specified subnets.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        response = ec2.create_route_table(VpcId=self.vpc_id)
        self.id = response['RouteTable']['RouteTableId']

        # associate the subnets with this route table
        # (otherwise, they will be associated with the main/default route table)
        if subnets:
            for subnet in subnets:
                ec2.associate_route_table(
                SubnetId=subnet.id, 
                RouteTableId=self.id
            )
        super().create()

    @handle_exceptions('routing table', 'add route to')
    def add_route(self, gateway_id, destination_cidr):
        """
        Adds route for internet gateway.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        ec2.create_route(
            RouteTableId=self.id,
            DestinationCidrBlock=destination_cidr,
            GatewayId=gateway_id
        )

    @handle_exceptions('routing table', 'delete')
    def delete(self):
        """
        Removes all routes and associations and deletes routing table from VPC.
        """
        ec2 = self.session.client('ec2', region_name=self.region)
        response = ec2.describe_route_tables(RouteTableIds=[self.id])
        route_table = response['RouteTables'][0]

        # remove associations
        for assoc in route_table['Associations']:
            if not assoc['Main']:
                ec2.disassociate_route_table(AssociationId=assoc['RouteTableAssociationId'])

        # remove non-default routes
        for route in route_table['Routes']:
            if not route['DestinationCidrBlock'] == '0.0.0.0/0':
                ec2.delete_route(RouteTableId=self.id, DestinationCidrBlock=route['DestinationCidrBlock'])

        ec2.delete_route_table(RouteTableId=self.id)
        super().delete()
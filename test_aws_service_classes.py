import abc
import os
import json
import boto3
import botocore
import moto
import pytest
import pandas as pd
import pdb

from aws_service_classes import *

REGION = 'ap-southeast-2'
MOCK_NAME = 'test-warehouse-service'
DB_MOCK_USER = 'test-user'
DB_MOCK_PW = 'test-pw'
SECURITY_GROUP_RDS_PATH = 'configs/security_groups/rds_security_group.json'
GLUE_JOB_POLICY_PATH = 'configs/IAM_roles/glue_job_policy.json'


@pytest.fixture(scope='class')
def mock_session(request):
    """
    Fixture that creates a mock session to test the different AWS services.
    """
    # setup mock AWS credentials
    aws_access_key = 'mock_access_key'
    aws_secret_key = 'mock_secret_key'

    # nested function to create a boto3 session with fake credentials
    def set_up_session():
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=REGION
        )
        return session

    service = request.param
    
    # create mock services using the moto module for simulating API calls
    # note: sometimes several mock services are needed for the testing
    if service == 's3':
        with moto.mock_s3():            
            session = set_up_session()
            yield session
    elif service == 'ec2':
        with moto.mock_ec2():            
            session = set_up_session()
            yield session
    elif service == 'lambda':
        # for creating Lambda function, need account ID (from sts client),
        # an IAM role, and an S3 bucket
        with moto.mock_sts():
            with moto.mock_iam(): 
                with moto.mock_s3():
                    with moto.mock_lambda():            
                        session = set_up_session()
                        yield session
    elif service == 'glue':
        # for creating Glue job, need account ID (from sts client),
        # and an IAM role
        with moto.mock_sts():
            with moto.mock_iam(): 
                with moto.mock_glue():            
                    session = set_up_session()
                    yield session
    elif service == 'iam':
        # for creating IAM policies/roles, need account ID (from sts client)
        with moto.mock_sts():
            with moto.mock_iam():            
                session = set_up_session()
                yield session
    elif service == 'rds':
        # in order to create RDS instance, also use an mock EC2 service
        # for creating a VPC and subnets
        with moto.mock_rds():
            with moto.mock_ec2():
                session = set_up_session()
                yield session


###############################################
# TEST CLASSES FOR AWS SERVICE WRAPPER CLASSES
###############################################

class BaseTestAWSService(abc.ABC):
    """
    Abstract base class for testing AWS service wrapper classes. Features setup 
    and teardown methods at the class level to create and delete the respective 
    AWS service.
    """
    @abc.abstractmethod
    def create_service(self) -> AWSService:
        """
        This method is implemented by the respective test class.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def service_exists(self) -> bool:
        """
        Checks if service (still) exists. Implemented by respective class.
        """
        raise NotImplementedError

    @pytest.fixture(autouse=True)
    def setup_method(self, mock_session):
        """
        Set up important instance attributes, create AWSService object.
        Use mock session injected from a pytest fixture.
        """
        self.name = MOCK_NAME
        self.region = REGION
        # assigning mock session on instance level
        self.session = mock_session
        # this method is implemented by the respective test class
        self.aws_service = self.create_service()

    def test_service_creation(self):
        """
        Tests if the AWS services is created successfully through the setup method.
        """
        assert self.service_exists()

    def teardown_method(self):
        """
        Delete AWS service and all other AWSservices contained in the 
        AWS service collection it is part of.
        """
        if len(self.aws_service.collections) > 0:
            for collection in self.aws_service.collections:
                collection.delete_components_with_retry()
        else:
            # if no collection exists, just delete the service itself
            self.aws_service.delete()
        if self.service_exists():
            raise Exception('Deletion of service failed!')


# Note: through the use of the fixture "mock_session" (scope='class')
# the mock session object is created once per class; setup_method is run
# before every test method, but simply assigns this object
# to the instance variable "self.session"

@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['s3'], indirect=True) 
class TestS3Bucket(BaseTestAWSService):
    """
    Tests creation and deletion of S3 bucket and file upload.
    """
    def create_service(self):
        """
        Creates S3 bucket. Implements abstract method of parent class.
        """
        bucket = S3Bucket(self.session, self.region, self.name)
        return bucket
    
    def service_exists(self):
        """
        Checks existence of S3 bucket. Implements abstract method of parent class.
        """
        s3 = self.session.client('s3')
        response = s3.list_buckets()
        names = [bucket['Name'] for bucket in response['Buckets']]
        return self.aws_service.name in names

    def test_upload_data(self):
        """
        Create mock CSV file and test upload function of S3 bucket wrapper class.
        """
        filepath = 'mockfile.csv'
        mock_data = {
            'col1' : [1, 2, 3],
            'col2' : [2, 3, 4],
            'col3' : [3, 4, 5],
        }
        mock_df = pd.DataFrame(mock_data)
        mock_df.to_csv(filepath)
        self.aws_service.upload_data(filepath, filepath)
        # remove mock file from local dir
        os.remove(filepath)
        # extract files from bucket
        bucket_name = self.aws_service.name
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        files = [file.key for file in bucket.objects.all()]

        assert len(files) == 1
        assert files[0] == filepath
            

@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['rds'], indirect=True) 
class TestRDSInstance(BaseTestAWSService):
    """
    Tests RDS instance creation, deletion, retrieving of hostname.
    """
    def create_mock_credentials_file(self):
        """
        Create JSON file with mock DB credentials.
        """
        filename = 'mock_credentials.json'
        configs = {
            'DB_username' : DB_MOCK_USER,
            'DB_password' : DB_MOCK_PW,
        }
        with open(filename, 'w') as f:
            # write mock credentials to file
            json.dump(configs, f)
        return filename

    def create_service(self):
        """
        Creates RDS instance. Implements abstract method of parent class.
        """
        self.credentials_file = self.create_mock_credentials_file()

        num_subnets = 2
        vpc = VPC(self.session, self.region, num_subnets, MOCK_NAME)
        # using multiple subnets across different availability zones (AZs) 
        # is recommended for RDS instance to achieve higher availability
        subnets = [comp for comp in vpc.components if isinstance(comp, Subnet)]

        rds_security_group = SecurityGroup(
            self.session,
            self.region, 
            vpc.id, 
            'test_security_group',
            description="test", 
            json_file=SECURITY_GROUP_RDS_PATH, 
            collections=[vpc]
        )
        # create a custom routing table and internet gateway
        # as components of the VPC
        routing_table = RoutingTable(
            self.session, 
            self.region, 
            vpc.id, 
            subnets, 
            collections=[vpc]
        )

        internet_gateway = InternetGateway(
            self.session, 
            self.region, 
            vpc_id=vpc.id, 
            collections=[vpc]
        )
        # allow all internet traffic into public subnets
        routing_table.add_route(internet_gateway.id, '0.0.0.0/0')
        rds_instance = RDSInstance(
            self.session, 
            self.region, 
            self.name,
            self.credentials_file,
            [rds_security_group],
            subnets,
            'db.t3.micro',
            collections=[vpc]
        )
        # delete mock credentials file
        os.remove(self.credentials_file)
        self.credentials_file = None
        return rds_instance
    
    def service_exists(self):
        """
        Checks existence of RDS instance. Implements abstract method of parent class.
        """
        rds = self.session.client('rds')
        try:
            response = rds.describe_db_instances(DBInstanceIdentifier=self.name)
        except:
            return False
        instances = response['DBInstances']
        exists = (len(instances) == 1) and (instances[0]['DBInstanceIdentifier'] == self.name)
        return exists

    def test_retrieve_hostname(self):
        """
        Checks mock hostname of the RDS instance.
        """
        self.aws_service.retrieve_hostname()
        assert self.aws_service.hostname


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['ec2'], indirect=True)
class TestSubnet(BaseTestAWSService):
    """
    Tests wrapper class for Subnet.
    """
    def create_service(self):
        vpc = VPC(self.session, self.region, 0, MOCK_NAME)

        # get AZ
        ec2 = self.session.client('ec2', region_name=self.region)
        az_response = ec2.describe_availability_zones()
        az = az_response['AvailabilityZones'][0]

        subnet = Subnet(
            self.session, 
            self.region, 
            vpc.id, 
            '10.0.0.0/24', 
            az,
            MOCK_NAME,
            collections=[vpc]
        )
        return subnet

    def service_exists(self):
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_subnets()
        ids = [subnet['SubnetId'] for subnet in response['Subnets']]
        return self.aws_service.id in ids


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['ec2'], indirect=True)
class TestVPC(BaseTestAWSService):
    """
    Tests wrapper class for VPC.
    """
    def create_service(self):
        vpc = VPC(self.session, self.region, 4, MOCK_NAME)
        return vpc

    def service_exists(self):
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_vpcs()
        ids = [vpc['VpcId'] for vpc in response['Vpcs']]
        return self.aws_service.id in ids


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['iam'], indirect=True)
class TestIAMPolicy(BaseTestAWSService):
    """
    Tests wrapper class for IAMPolicy.
    """
    def create_service(self):
        sts_client = self.session.client('sts')
        account_id = sts_client.get_caller_identity()['Account']

        policy = IAMPolicy(
            self.session, 
            'test-policy', 
            self.region,
            json_file=GLUE_JOB_POLICY_PATH,
            account_id=account_id,
        )
        return policy

    def service_exists(self):
        iam = boto3.client('iam', region_name=self.region)
        response = iam.list_policies(Scope='Local')
        arns = [policy['Arn'] for policy in response['Policies']]
        return self.aws_service.id in arns


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['iam'], indirect=True)
class TestIAMRole(BaseTestAWSService):
    """
    Tests wrapper class for IAMRole.
    """
    def create_service(self):
        sts_client = self.session.client('sts')
        account_id = sts_client.get_caller_identity()['Account']

        # for deletion of all components, i.e. policy and role
        collection = AWSServiceCollection()
        policy = IAMPolicy(
            self.session, 
            'test-policy', 
            self.region,
            json_file=GLUE_JOB_POLICY_PATH,
            account_id=account_id,
        )
        role = IAMRole(
            self.session, 
            'test-role', 
            'glue.amazonaws.com',
            [policy],
        )             
        collection.add_components([role, policy])
        return role

    def service_exists(self):
        iam = boto3.client('iam', region_name=self.region)
        response = iam.list_roles()
        arns = [role['Arn'] for role in response['Roles']]
        return self.aws_service.id in arns


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['ec2'], indirect=True)
class TestSecurityGroup(BaseTestAWSService):
    """
    Tests wrapper class for SecurityGroup.
    """
    def create_service(self):
        vpc = VPC(self.session, self.region, 0, MOCK_NAME)
        security_group = SecurityGroup(
            self.session,
            self.region, 
            vpc.id,
            MOCK_NAME,
            description="test", 
            json_file=SECURITY_GROUP_RDS_PATH, 
            collections=[vpc])
        return security_group

    def service_exists(self):
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_security_groups()
        ids = [sg['GroupId'] for sg in response['SecurityGroups']]
        return self.aws_service.id in ids
    

@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['ec2'], indirect=True)
class TestInternetGateway(BaseTestAWSService):
    """
    Tests wrapper class for InternetGateway.
    """
    def create_service(self):
        vpc = VPC(self.session, self.region, 0, MOCK_NAME)
        igw = InternetGateway(
            self.session,
            self.region, 
            vpc.id,
            collections=[vpc]    
        )
        return igw

    def service_exists(self):
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_internet_gateways()
        ids = [igw['InternetGatewayId'] for igw in response['InternetGateways']]
        return self.aws_service.id in ids


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['ec2'], indirect=True)
class TestRoutingTable(BaseTestAWSService):
    """
    Tests wrapper class for RoutingTable.
    """
    def create_service(self):
        collection = AWSServiceCollection()
        self.vpc = VPC(self.session, self.region, 2, MOCK_NAME, collections=[collection])
        subnets = [comp for comp in self.vpc.components if isinstance(comp, Subnet)]
        rt = RoutingTable(
            self.session,
            self.region, 
            self.vpc.id,
            subnets,
            collections=[self.vpc, collection]    
        )
        return rt

    def test_add_route(self):
        """
        Tests adding routes to route table.
        """
        # create internet gatewar for route test
        self.igw = InternetGateway(
            self.session,
            self.region, 
            vpc_id=self.vpc.id, 
            collections=[self.vpc]
        )
        # allow all internet traffic into public subnets
        self.aws_service.add_route(self.igw.id, '0.0.0.0/0')
        # check existing routes
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_route_tables()
        route_tables = response['RouteTables']
        # get route table created for this test
        rt_id = self.aws_service.id
        this_rt = [rt for rt in route_tables if rt['RouteTableId'] == rt_id]
        assert len(this_rt) == 1
        this_rt = this_rt[0]
        # check if route to internet gateway exists
        gateway_ids = [route['GatewayId'] for route in this_rt['Routes']]
        assert self.igw.id in gateway_ids
                
    def service_exists(self):
        ec2 = boto3.client('ec2', region_name=self.region)
        response = ec2.describe_route_tables()
        ids = [rt['RouteTableId'] for rt in response['RouteTables']]
        return self.aws_service.id in ids


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['glue'], indirect=True)
class TestAWSGlueJob(BaseTestAWSService):
    """
    Tests wrapper class for AWSGlueJob.
    """
    def create_service(self):
        """
        Creates Glue job along with respective IAM role.
        """
        # for deleting all services created in this test function
        collection = AWSServiceCollection()
        # create IAM policy and role for Glue job
        # for testing, choose S3 full access policy
        policy = IAMPolicy(
             self.session,
             MOCK_NAME,
             self.region,
             arn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
             collections=[collection]
        )
        role = IAMRole(
            self.session, 
            MOCK_NAME,
            'lambda.amazonaws.com',
            policies=[policy], 
            collections=[collection]
        )
        glue_job = AWSGlueJob(
            self.session,
            self.region,
            self.name,
            role,
            'test.py'
        )
        return glue_job
    

    def service_exists(self):
        glue = boto3.client('glue', region_name=self.region)
        try:
            response = glue.get_job(JobName=self.name)
        except:
            return False
        name = response['Job']['Name']
        return self.aws_service.name == name


@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['lambda'], indirect=True)
class TestS3LambdaFunction(BaseTestAWSService):
    """
    Tests wrapper class for S3LambdaFunction.
    """
    def create_service(self):
        """
        Create Lambda function along with respective IAM role and S3 bucket.
        """
        sts_client = self.session.client('sts')
        account_id = sts_client.get_caller_identity()['Account']

        # for deleting all services created
        collection = AWSServiceCollection()
        # create IAM policy and role for Lambda function
        # for testing, choose S3 full access policy
        policy = IAMPolicy(
             self.session,
             MOCK_NAME,
             self.region,
             arn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
             collections=[collection]
        )
        role = IAMRole(
            self.session, 
            MOCK_NAME,
            'lambda.amazonaws.com',
            policies=[policy], 
            collections=[collection]
        )
        test_bucket_name = 'test-bucket'
        bucket = S3Bucket(
            self.session, 
            self.region, 
            test_bucket_name,
            collections=[collection]
        )
        lambda_func = S3LambdaFunction(
            self.session,
            self.region,
            account_id,
            self.name,
            handler='test.handler',
            script_object_key='test.py',
            bucket_name_script=test_bucket_name,
            bucket_name_trigger=test_bucket_name,
            role=role,
            collections=[collection]
        )
        return lambda_func

    def service_exists(self):
        """
        Checks if Lambda function exists.
        """
        lambda_client = boto3.client('lambda', region_name=self.region)
        try:
            response = lambda_client.get_function(FunctionName=self.name)
        except:
            return False
        name = response['Configuration']['FunctionName']
        return self.aws_service.name == name
    
    
# lastly, test class for collecting and collectively deleting AWS services
@pytest.mark.usefixtures("mock_session")
@pytest.mark.parametrize("mock_session", ['s3'], indirect=True) 
class TestAWSServiceCollection():
    """
    Tests the AWSServiceCollection class which can hold and simultaneously delete
    several different AWS service components.
    """
    @pytest.fixture(autouse=True)
    def setup_method(self, mock_session):
        """
        Set up important instance attributes, create AWSService object.
        Use mock session injected from a pytest fixture.
        """
        self.name = MOCK_NAME
        self.region = REGION
        # assigning mock session on instance level
        self.session = mock_session
        # create empty AWS service collection
        self.collection = AWSServiceCollection()
        assert self.collection.empty

    def test_create_and_delete_components(self):
        """
        Test adding components to the AWS service collection.
        """
        num_comp = 4
        # use a few S3 buckets as components for testing
        buckets = []
        for i in range(num_comp):
            bucket = S3Bucket(self.session, self.region, f'test-bucket{i}')
            buckets.append(bucket)
            self.collection.add_component(bucket)
        # check whether the collection contains num_comp components
        assert not self.collection.empty
        assert len(self.collection.components) == num_comp
        # check whether one of the buckets is contained in the collection
        bucket1_name = buckets[0].name
        assert self.collection.contains_resource(bucket1_name)

    def teardown_method(self):
        """
        Remove all components from collection.
        """
        self.collection.delete_components()
        s3 = self.session.client('s3')
        response = s3.list_buckets()
        names = [bucket['Name'] for bucket in response['Buckets']]
        # list of buckets and list of components of the collection should now be empty
        assert len(names) == 0
        assert self.collection.empty
        
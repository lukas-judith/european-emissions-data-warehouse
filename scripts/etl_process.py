# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job


# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# job.commit()

import os
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


def transform_data(data):
    data_trans = data.select("*")
    # do stuff
    return data_trans


# create a Spark session
spark = SparkSession.builder.appName('ETLJob').getOrCreate()

args = getResolvedOptions(
    sys.argv, 
    [
    'SOURCE_BUCKET_NAME',
    'SINK_BUCKET_NAME',
    'SOURCE_FILEPATH'
    ]
)

source_bucket = args['SOURCE_BUCKET_NAME']
sink_bucket = args['SINK_BUCKET_NAME']
source_path = args['SOURCE_FILEPATH']

# data paths
# use Hadoop connector "s3a://" for better performance
input_path = f"s3a://{source_bucket}/{source_path}"
output_path = f"s3a://{sink_bucket}/data_processed"

# read .csv file from S3 bucket
data = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(input_path)

data_trans = transform_data(data)

# store processed data in another S3 bucket
data_trans.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)


spark.stop()
import os
import boto3


def start_glue_job(event, context):
    """
    Start AWS Glue job.
    """
    job_name = os.environ['JOB_NAME']
    glue_client = boto3.client('glue')
    response = glue_client.start_job(JobName=job_name)
    return response


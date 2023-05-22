# get public Docker image for AWS Lambda
FROM public.ecr.aws/lambda/python:3.8-x86_64

# install the dependencies for the Lambda function
COPY requirements_warehouse.txt .
RUN pip3 install -r requirements_warehouse.txt --target "${LAMBDA_TASK_ROOT}"

# copy the Lambda handler code to the docker image
COPY lambda_handler_warehouse.py ${LAMBDA_TASK_ROOT}

# install zip command
RUN yum install -y zip

# set empty entry point and CMD
ENTRYPOINT []
CMD []
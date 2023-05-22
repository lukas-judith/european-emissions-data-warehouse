import os
import json
import boto3
from sqlalchemy import create_engine, text


def transfer_processed_data(event, context):
    """
    Transfer processed data from S3 bucket to RDS instance.
    """
    # Note: since Apache Spark processes the data in parallel,
    # it may generate multiple parquet files.
    # output_path variable is the name of the directory containing the files
    output_path_folder = os.environ['OUTPUT_PATH']
    hostname = os.environ['DB_HOSTNAME']
    dbname = os.environ['DB_NAME']
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASSWORD']
    port = os.environ['PORT']
    bucket_name = os.environ['BUCKET_NAME']
    region = os.environ['REGION']
    access_key = os.environ['ACCESS_KEY']
    secret_key = os.environ['SECRET_KEY']
   
    # Concatenate the individual dataframes before transfer to the database
    # Note: using fastparquet instead of pyarrow since it is more lightweight,
    # deployment package would otherwise exceed the maximum size for AWS Lambda
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # all CSV files
    csv_files = [obj.key for obj in bucket.objects.filter(Prefix=output_path_folder) 
                 if obj.key.endswith('.csv')]
    
    if len(csv_files) == 0:
        print(f"NO CSV FILES WERE FOUND IN {bucket_name}")
    else:
        print(f'Found the following CSV files:')
        for f in csv_files:
            print(f)

    # uri for connecting to default database (postgres) in order to create a new database
    db_uri = f'postgresql://{username}:{password}@{hostname}:{port}/{dbname}'
    
    table_name = 'european_ghg_projections'

    # columns and data types
    column_definitions = [
        'Country VARCHAR',
        'Year INTEGER',
        'Scenario VARCHAR',
        'Category VARCHAR',
        'Gas VARCHAR',
        'ReportedValue FLOAT',
        'Unit VARCHAR'
    ]
    columns = ['Country', 'Year', 'Scenario', 'Category', 'Gas', 'ReportedValue', 'Unit']

    try:
        # create SQLAlchemy engine and create new database
        engine = create_engine(db_uri)

        with engine.connect() as connection:
            connection.execute(text("commit"))
            connection.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"))
        print(f"Created new database table {table_name}")

        # import each CSV file into database separately
        for file in csv_files:
            s3_import_command = f"""
            SELECT aws_s3.table_import_from_s3 (
                '{table_name}', 
                '{', '.join(columns)}',
                'DELIMITER '','' CSV',
                '{bucket_name}', 
                '{file}', 
                '{region}', 
                '{access_key}', 
                '{secret_key}',
                ''
            );
            """
            with engine.connect() as connection: 
                connection.execute(text(s3_import_command))
                connection.execute(text("commit"))
            print(f"Imported {file} to database {dbname}")

    except Exception as e:
        return {
            'body': json.dumps(f'Failed to load data to PostgreSQL: {e}')
        }
    # retun response with success message if data can be transferred successfully
    return {
        'body': json.dumps('Data loaded to PostgreSQL successfully!')
    }
   

import os
import json
import boto3
from sqlalchemy import create_engine, text


def transfer_processed_data(event, context):
    """
    Transfers processed data from S3 bucket to RDS instance.
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

    try:
        # create SQLAlchemy engine and create new database
        engine = create_engine(db_uri)

        # create table with unique keyword, this allows for update of the reported value
        # upon conflict (see import_into_real_table_command below)
        create_table_command = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            Country VARCHAR,
            Year INTEGER,
            Scenario VARCHAR,
            Category VARCHAR,
            Gas VARCHAR,
            ReportedValue FLOAT,
            Unit VARCHAR,
            UNIQUE (Country, Year, Scenario, Category, Gas, Unit)
        );
        """
        # use temporary data to which the data is uploaded from the S3 bucket first
        # (will be deleted afterwards)
        create_temporary_table_command = f"""
        CREATE TEMPORARY TABLE temp_{table_name} AS TABLE {table_name} WITH NO DATA;
        """

        with engine.connect() as connection:
            connection.execute(text("commit"))
            connection.execute(text(create_table_command))
            connection.execute(text(create_temporary_table_command))
        print(f"Created new database table (if not existent) {table_name}")

        # import each CSV file into database separately
        for file in csv_files:
            s3_import_command = f"""
            SELECT aws_s3.table_import_from_s3 (
                'temp_{table_name}', 
                'Country, Year, Scenario, Category, Gas, ReportedValue, Unit',
                'DELIMITER '','' CSV',
                '{bucket_name}', 
                '{file}', 
                '{region}', 
                '{access_key}', 
                '{secret_key}',
                ''
            );
            """
            # import the data into the actual table, handling conflicts with duplicates
            # (update upon data entry with new reported value)
            import_into_real_table_command = f"""
            INSERT INTO {table_name} (Country, Year, Scenario, Category, Gas, ReportedValue, Unit)
            SELECT Country, Year, Scenario, Category, Gas, ReportedValue, Unit
            FROM temp_{table_name} 
            ON CONFLICT (Country, Year, Scenario, Category, Gas, Unit) DO UPDATE
            SET ReportedValue = EXCLUDED.ReportedValue;
            """

            with engine.connect() as connection: 
                connection.execute(text(s3_import_command))
                connection.execute(text(import_into_real_table_command))  
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
   

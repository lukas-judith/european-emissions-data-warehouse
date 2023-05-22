import os
import sys
from itertools import chain
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# create a Spark session
spark = SparkSession.builder.appName('ETLJob').getOrCreate()

args = getResolvedOptions(
    sys.argv, 
    [
    'SOURCE_BUCKET_NAME',
    'SINK_BUCKET_NAME',
    'SOURCE_FILEPATH',
    'OUTPUT_FOLDER_NAME'
    ]
)

source_bucket = args['SOURCE_BUCKET_NAME']
sink_bucket = args['SINK_BUCKET_NAME']
source_path = args['SOURCE_FILEPATH']
output_folder_name = args['OUTPUT_FOLDER_NAME']

# data paths
input_path = f"s3://{source_bucket}/{source_path}"
output_path = f"s3://{sink_bucket}/{output_folder_name}"

# dictionary to map country codes to full country names
# TODO: store as CSV or JSON for more flexibility when adding more countries
country_code_map = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "CH": "Switzerland",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "EE": "Estonia",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GR": "Greece",
    "HR": "Croatia",
    "HU": "Hungary",
    "IE": "Ireland",
    "IS": "Iceland",
    "IT": "Italy",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MT": "Malta",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia"
}

# create mapping expression for PySpark to map country codes to full country names
mapping_expr = create_map([lit(x) for x in chain(*country_code_map.items())])

try:
    # extract data from .csv file in S3 bucket
    df_raw = spark.read \
        .format('csv') \
        .options(delimiter=',', header='True') \
        .load(input_path)
except Exception as e:
    print("Error! Could not extract CSV data:", e)

try:
    # clean and transform data
    # Note: when storing data, column names my not contain any character(s) among " ,;{}()\n\t="
    df_trans = df_raw \
        .select('CountryCode', 'Year', 'Scenario', 'Category', 'Gas', 'Reported Value') \
        .dropna(subset=['CountryCode', 'Year', 'Scenario', 'Category', 'Gas', 'Reported Value'], how='any') \
        .filter((col('Gas') == 'Total GHG emissions (ktCO2e)') 
                & col('CountryCode').isin(list(country_code_map.keys()))) \
        .withColumn('Unit', when((col('Gas') == 'Total GHG emissions (ktCO2e)'), 'kt CO2 equivalent')) \
        .withColumnRenamed('Total GHG emissions (ktCO2e)', 'Total GHG emissions') \
        .withColumnRenamed('Reported Value', 'ReportedValue') \
        .withColumn('Country', mapping_expr[col("CountryCode")]) \
        .select('Country', 'Year', 'Scenario', 'Category', 'Gas', 'ReportedValue', 'Unit')
except Exception as e:
    print("Error! Could not transform data:", e)

try:
    # store processed data in another S3 bucket
    df_trans.write \
        .format("csv") \
        .mode("overwrite") \
        .save(output_path)
except Exception as e:
    print("Error! Could store transformed data:", e)

# stop Spark session
spark.stop()
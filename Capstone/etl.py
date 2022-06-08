import os
import configparser
from pandas import isnull
import pandas as pd
from pendulum import date
import pyspark
import utils.data_load as data_load
import utils.data_cleaning as data_cleaning
from models.s3BucketUtils import s3Bucket, s3Spark
import datetime
import numpy as np

def testing_function():
    config_path = 'config_file.ini'
    config = configparser.ConfigParser()
    config.read(config_path)
    print(config.sections)
    bucket = s3Bucket(access_aws_key=config['AWS']['AWS_ACCESS_KEY_ID'], access_aws_secret_key=config['AWS']['AWS_SECRET_ACCESS_KEY'], aws_region=config['AWS']['AWS_S3_REGION'])
    bucket.set_bucket_name(bucket_name=config['AWS']['AWS_S3_BUCKET'])

#-----------------------------------------------------------------------
    print(config['AWS']['AWS_ACCESS_KEY_ID'])
    #s3://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country/Country=United States/part-00000-3a411cae-bdec-4a21-bba8-71e6a71cb1b9.c000.csv
    #s3://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country/Country=United States/
    key_path = 's3a://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country/'
    key_filter = 'Country=United States'
    #print(bucket.get_bucket_keys())
    spark = s3Spark(access_aws_key=config['AWS']['AWS_ACCESS_KEY_ID'],
                    access_aws_secret_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])

    df = data_cleaning.clean_global_temperature_by_filter(spark=spark, 
                                                            s3_key_path=key_path, 
                                                            key_filter=key_filter,
                                                            create_partitioned_data=False)
    print(df.head(5))
    print(df.count())
#-----------------------------------------------------------------------
    # key='capstone_data/immigration_data_sample.csv'
    # df = data_cleaning.clean_immigration_data(bucket=bucket, filepath=key)

    # print(df.head(5))
    # print(df.count())
    # print("-"*100)
    # #df["date_allowed_stay"] = df["date_allowed_stay"].apply(lambda x: '' if pd.isnull(x) else x)
    # # df.dropna(thresh=5, inplace=True)
    # # df.reset_index(drop=True)
    # print(df['departure_date'][df['departure_date'].isnull()].head(5))
    # print(df.dtypes)
#-----------------------------------------------------------------------
#     key='capstone_data/us-cities-demographics.csv'
#     df = data_load.load_demographic_data_to_redshift(bucket=bucket, filepath=key)
    
#     print(df.head(5))
#     print(df.count())
#     print("-"*20)
#----------------------------------------------------------------------
    # key='capstone_data/airport-codes_csv.csv'

    # #only one filter available, could be extended.
    # filters = {'iso_country' : 'US'}
    # df = data_load.load_airport_codes_data_to_redshift(bucket=bucket, filepath=key, filters=filters)
    
    # print(df.head(5))
    # print(df.count())
    # print("-"*20)
    
if __name__ == "__main__":
    #run something
    testing_function()

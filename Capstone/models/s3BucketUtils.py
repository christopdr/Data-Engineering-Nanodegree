#from distutils.command.config import config
from io import StringIO
import io
from logging import exception
import logging
import boto3
import pandas as pd
#from pendulum import local
import pyspark
from pyspark.sql import SparkSession
import os
import fastparquet
import pyarrow
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[3] pyspark-shell'


class s3Bucket():

    def __init__(self, access_aws_key, access_aws_secret_key, aws_region) -> None:
        self.__s3 = boto3.resource(
                service_name='s3',
                region_name=aws_region,
                aws_access_key_id=access_aws_key,
                aws_secret_access_key=access_aws_secret_key)
        self.__bucket_name = ""

   
    def get_buckets_available(self):
        """ Return a list of available buckets in s3 boto3 object."""
        return [bucket.name for bucket in self.__s3.buckets.all()]
    

    def get_bucket_keys(self):
        """Return a list of the key objects stored in the bucket provided."""
        if not self.is_bucket_name_set():
            raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

        return [obj.key for obj in self.__s3.Bucket(self.__bucket_name).objects.all()]


    def get_bucket_object(self, key_name_bucket):

        if not self.is_bucket_name_set():
             raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

        return self.__s3.Bucket(self.__bucket_name).Object(key_name_bucket).get()

    def get_csv_data_reader(self, key_name_bucket, chunk_size=(10**4)*2):
        """ Returns data reader splitted in chunk size from s3 bucket. Useful method for huge csv files."""

        if not self.is_bucket_name_set():
            raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

        try:
            csv_obj = self.__s3.Bucket(self.__bucket_name).Object(key_name_bucket).get()
            return pd.read_csv(csv_obj['Body'], chunk_size=chunk_size)

        except Exception as e:
            raise Exception("Something went wrong retrieving data reader from bucket: \n{}\n\n{}".format("-"*10, e))
    
    def get_csv_dataframe(self, key_name_bucket, sep=','):
        """ Returns dataframe from s3 bucket key_name"""

        if not self.is_bucket_name_set():
            raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

        try:
            csv_obj = self.__s3.Bucket(self.__bucket_name).Object(key_name_bucket).get()
            return pd.read_csv(csv_obj['Body'], sep=sep)

        except Exception as e:
            raise Exception("Something went wrong retrieving dataframe from bucket: \n{}\n\n{}".format("-"*10, e))
    
    def get_snappy_parquet_dataframe(self, key_name_bucket):
        if not self.is_bucket_name_set():
            raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

        try:
            #uri = 's3://{}/{}'.format(self.get_bucket_name(), key_name_bucket)
            #print(uri)
            obj = self.__s3.Bucket(self.__bucket_name).Object(key_name_bucket).get()

            return pd.read_parquet(io.BytesIO(obj['Body'].read()))
        except Exception as e:
            raise Exception("Something went wrong retrieving dataframe from bucket: \n{}\n\n{}".format("-"*10, e))


    # def get_multiple_csv_from_folder_to_dataframe(self, key_folder, sep=','):
    #     """ """
    #     if not self.is_bucket_name_set():
    #         raise Exception("Bucket name is not provided. Please use set_bucket_name() to assign a bucket name.")

    #     try:
    #         if self.is_bucket_folder_empty():
    #             logging.info("No keys found in bucket. Nothing to process...")
            
    #         for key in self.get_bucket_keys():
    #             pass
    #         pass
    #     except Exception as e:
    #         pass


    def save_csv_to_bucket(self,dataframe, bucket_name, key_name_bucket, sep=',', index=False):
        """ Save dataframe to s3 bucket directly, storing temporal data to buffer. Returns bool value if successfull"""

        HTTP_STATUS_KEY = 'HTTPStatusCode'
        STATUS_VALUE = 200
        
        csv_buffer = StringIO()
        try:
            dataframe.to_csv(csv_buffer, sep=sep, index=index)
            response = self.__s3.Object(bucket_name, key_name_bucket).put(Body=csv_buffer.getvalue())
            if response[HTTP_STATUS_KEY] == STATUS_VALUE:
                return True
            else:
                return False

        except Exception as ex:
            raise Exception("Something went wrong saving data buffer in bucket: \n{}\n\n{}".format("-"*10, ex))
    
    def is_bucket_folder_empty(self):
        return len(self.get_bucket_keys()) == 0

    def is_bucket_name_set(self):
        return len(self.__bucket_name) > 0
    
    def set_bucket_name(self, bucket_name):
        self.__bucket_name = bucket_name
    
    def get_bucket_name(self):
        return self.__bucket_name

                    #.config("spark.jars.packages", 'com.johnsnowlabs.nlp:spark-nlp_2.11:1.8.2')\
#config('spark.jars.packagers', 'org.apache.hadoop:hadoop-aws:2.8.5,org.apache.hadoop:hadoop-common:2.8.5,com.amazonaws:aws-java-sdk-s3:1.10.6"')\
  
class s3Spark():
    
    def __init__(self , access_aws_key, access_aws_secret_key):
        self.__sparkSession = SparkSession.builder\
            .appName("CapstoneSpark")\
                .config('spark.hadoop.fs.s3a.access.key', access_aws_key)\
                .config('spark.hadoop.fs.s3a.secret.key', access_aws_secret_key)\
                .getOrCreate()
    
    def get_dataframe_from_bucket(self, s3_path, header=True):
        """ Read data from csv stored in s3 bucket. You can specify if your csv contain headers."""
        return self.__sparkSession.read.csv(s3_path, header=header)
    
    def get_csv_with_schema(self, s3_key_path, header=False, schema=None):
        """  """
        return self.__sparkSession.read.csv(s3_key_path, header=header, schema=schema)

    @staticmethod
    def save_partitioned_data_to_bucket(spark_dataframe, partition_filter, s3_key_path, mode='overwrite'):
        """ Save data in bucket partitioned by a key value in dataframe. Carefull with this method, if you don't run it in cluster it will take a lot of time to process all data and upload it.

            Parameters:
                - spark_dataframe (sql context): sql spark dataframe object created with pyspark.
                - partition_filter (str): key string value in dataframe headers to partition data.
                - s3_key_path (str): s3 path folder to store the data after partitioned
            
            Returns:
                - (bool) if successful
        """
        if partition_filter not in spark_dataframe.columns:
            return False
        try:
            spark_dataframe.write.mode(mode).partitionBy(partition_filter).csv(s3_key_path)
            return True
        except Exception as ex:
            raise Exception("Saving parquet files to s3 bucket was not possible. \n{}".format(ex))
    
    
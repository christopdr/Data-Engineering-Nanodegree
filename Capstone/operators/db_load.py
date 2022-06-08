from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from utils import data_load, data_cleaning
from models.s3BucketUtils import s3Spark


class DBImmigrationLoadOperator(BaseOperator):

    ui_color = '#53B9E9'
    

    @apply_defaults
    def __init__(self, redshift_conn_id, bucket, bucket_key, *args, **kwargs):
        super(DBImmigrationLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.bucket = bucket
        self.bucket_key = bucket_key
        self.table_name = 'public.immigration'
    
    def execute(self, context):
        self.log.info("Data loader for (immigraiton) is running to insert info into redshift.")
        
        self.log.info("Getting and cleaning (immigration) data from s3 bucket.")
        df = data_cleaning.clean_immigration_data(bucket=self.bucket, filepath=self.bucket_key)

        if df.empty == True:
            raise Exception("Unable to retrieve data for immigration history...")

        #run data load method to store data in redshift
        data_load.load_data_to_redshift(redshift_conn_id=self.redshift_conn_id, 
                                                    tablename=self.table_name,
                                                    dataframe=df)
    


class DBDemographicLoadOperator(BaseOperator):

    ui_color = '#53B9E9'

    @apply_defaults
    def __init__(self, redshift_conn_id, bucket, bucket_key, *args, **kwargs):
        super(DBDemographicLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.bucket = bucket
        self.bucket_key = bucket_key
        self.table_name = 'public.demographic'
    
    def execute(self, context):
        self.log.info("Data loader for (demographic) is running to insert info into redshift.")
        
        self.log.info("Getting and cleaning (demographic) data from s3 bucket.")
        df = data_cleaning.clean_demographic_data(bucket=self.bucket, filepath=self.bucket_key)

        if df.empty == True:
            raise Exception("Unable to retrieve data for demographic history...")

        #run data load method to store data in redshift
        data_load.load_data_to_redshift(redshift_conn_id=self.redshift_conn_id, 
                                                    tablename=self.table_name,
                                                    dataframe=df)



class DBAirportCodesLoadOperator(BaseOperator):

    ui_color = '#53B9E9'

    @apply_defaults
    def __init__(self, redshift_conn_id, bucket, bucket_key, *args, **kwargs):
        super(DBAirportCodesLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.bucket = bucket
        self.bucket_key = bucket_key
        self.table_name = 'public.airport_code'
    
    def execute(self, context):
        self.log.info("Data loader for (airport codes) is running to insert info into redshift.")
        
        self.log.info("Getting and cleaning (airport codes) data from s3 bucket.")
        df = data_cleaning.clean_demographic_data(bucket=self.bucket, filepath=self.bucket_key)

        if df.empty == True:
            raise Exception("Unable to retrieve data for airport codes...")

        #run data load method to store data in redshift
        data_load.load_data_to_redshift(redshift_conn_id=self.redshift_conn_id, 
                                                    tablename=self.table_name,
                                                    dataframe=df)



class DBTemperatureLoadOperator(BaseOperator):

    ui_color = '#53B9E9'

    @apply_defaults
    def __init__(self, redshift_conn_id, access_key, secret_key, bucket_key, key_filter, partition_data, *args, **kwargs):
        super(DBTemperatureLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.bucket_key = bucket_key
        self.table_name = 'public.GLOBAL_TEMPERATURE'
        self.key_filter = key_filter
        self.partition_data = partition_data
        self.spark = s3Spark(access_aws_key= access_key, access_aws_secret_key=secret_key)
    
    
    def execute(self, context):
        self.log.info("Data loader for (global temperature) is running to insert info into redshift.")
        
        self.log.info("Getting and cleaning (global temperature) data from s3 bucket.")
        df = data_cleaning.clean_global_temperature_by_filter(spark=self.spark, 
                                                                s3_key_path=self.bucket_key, 
                                                                key_filter=self.key_filter, 
                                                                create_partitioned_data=self.partition_data) 
        if df.empty == True:
            raise Exception("Unable to retrieve data for global temperatures...")

        #run data load method to store data in redshift
        data_load.load_data_to_redshift(redshift_conn_id=self.redshift_conn_id, 
                                                    tablename=self.table_name,
                                                    dataframe=df)


class DBSaticLabelsLoadOperator(BaseOperator):

    ui_color = '#8FF2DD'

    @apply_defaults
    def __init__(self, redshift_conn_id, file_path, labels_map, *args, **kwargs):
        super(DBSaticLabelsLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.file_path = file_path
        self.labels_map = labels_map

    def execute(self, context):
        self.log.info("Data loader for (static label tables) is running to insert info into redshift.")

        self.log.info("Getting static label tables from s3 excel file in bucket.")
        labels_df_map = data_cleaning.get_sas_labels_dataframes(file_path=self.file_path, labels_map=self.labels_map)
        # labels_df_map = data_cleaning.get_labels_from_excel_file(bucket=self.bucket, 
        #                                                             bucket_key_name=self.bucket_key)

        #run loop to insert all statitc labels data
        for key_label in labels_df_map.keys():
            data_load.load_data_to_redshift(redshift_conn_id=self.redshift_conn_id,
                                            tablename="public.{}".format(key_label),
                                            dataframe=labels_df_map[key_label])


class DBCreateStarSchemaModelOperator(BaseOperator):

    ui_color="#33FFBB"

    @apply_defaults
    def __init__(self, redshift_conn_id, sql_query, *args, **kwargs) -> None:
        super(DBCreateStarSchemaModelOperator).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
    
    def execute(self, context):
        self.log.info("DAG operator to create star schema in redshift is being executed.")

        self.log.info("+ Creating PosgresHook to run redshift query.")
        redshift = PostgresHook(self.redshift_conn_id)
        conn = redshift.get_conn()
        self.log.info("+Modifying redshift schema. Executing query...")
        try:
            redshift.run(self.sql_query, self.autocommit)

            self.log.info("Star schema query successfully executed.")
        except Exception as ex:
            self.log.info("Unable to create star schema model.")
            raise Exception("Unable to create star schema in redshift")
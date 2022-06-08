
#python3
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
import datetime
import logging
#python3
import configparser
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils import data_cleaning, data_load
from utils.data_load import load_immigration_data_to_redshift
from models.s3BucketUtils import s3Bucket
import sql.sql_commands as sql_commands
from operators.db_builder import DBBuilderOperator
from operators.db_load import DBImmigrationLoadOperator, DBDemographicLoadOperator, DBAirportCodesLoadOperator, DBTemperatureLoadOperator, DBSaticLabelsLoadOperator

from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator

#key location in s3 bucket
key_location = {'immigration' :'capstone_data/immigration_data_sample.csv',
                'airport_codes' : 'capstone_data/airport-codes_csv.csv',
                'demographic' :'capstone_data/us-cities-demographics.csv',
                'temperature' : 's3a://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country',
                'static_labels' : 'capstone_data/sas_label_description.xlsx'}


default_args = {
    'owner': 'Chris',
    'start_date' : datetime.datetime.utcnow(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


dag = DAG(
    dag_id="Capstone_Project",
    default_args=default_args,
    description="Extract Transform and Load Capstone project data.",
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,
    tags=["capstone"]
)

#create dummy operation to start execution and end execution

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
end_operator = DummyOperator(task_id="End_execution", dag=dag)

#database tables creation operators

create_db_tables = DBBuilderOperator(
    task_id="create_all_tables_in_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    queries_to_exec=sql_commands.CREATE_TABLES_QUERIES_MAP
)

#database insert operator
# config_path = 'config_file.ini'
# config = configparser.ConfigParser()
# config.read(config_path)





bucket = s3Bucket(access_aws_key=Variable.get("AWS_ACCESS_KEY_ID"), 
                    access_aws_secret_key=Variable.get("AWS_SECRET_ACCESS_KEY"), 
                    aws_region=Variable.get("AWS_S3_REGION"))

bucket.set_bucket_name(bucket_name=Variable.get("AWS_S3_BUCKET"))


clean_and_load_immigration_data = DBImmigrationLoadOperator(
    task_id="clean_and_load_immigration_data",
    dag=dag,
    redshift_conn_id="redshift",
    bucket=bucket,
    bucket_key=key_location.get('immigration')
)

clean_and_load_demographic_data = DBDemographicLoadOperator(
    task_id="clean_and_load_demographic_data",
    dag=dag,
    redshift_conn_id="redshift",
    bucket=bucket,
    bucket_key=key_location.get('demographic')
)

clean_and_load_airport_codes_data = DBAirportCodesLoadOperator(
    task_id="clean_and_load_airport_codes_data",
    dag=dag,
    redshift_conn_id="redshift",
    bucket=bucket,
    bucket_key=key_location.get('airport_codes')
)

clean_and_load_temperature_data = DBTemperatureLoadOperator(
    task_id="clean_and_load_temperature_data",
    dag=dag,
    redshift_conn_id="redshift",
    access_key=Variable.get("AWS_ACCESS_KEY_ID"),
    secret_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    bucket_key=key_location.get("temperature"),
    key_filter='Country=United States',
    partition_data=False
)

load_static_labels_data = DBSaticLabelsLoadOperator(
    task_id="laod_static_labels_data",
    dag=dag,
    redshift_conn_id="redshift",
    bucket=bucket,
    bucket_key=key_location.get("static_labels")
)



#airflow execution map

start_operator >> create_db_tables

create_db_tables >> clean_and_load_immigration_data
create_db_tables >> clean_and_load_demographic_data
create_db_tables >> clean_and_load_airport_codes_data
create_db_tables >> clean_and_load_temperature_data
create_db_tables >> load_static_labels_data

clean_and_load_immigration_data >> end_operator
clean_and_load_demographic_data >> end_operator
clean_and_load_airport_codes_data >> end_operator
clean_and_load_temperature_data >> end_operator
load_static_labels_data >> end_operator

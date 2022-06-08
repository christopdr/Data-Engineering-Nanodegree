
from models.s3BucketUtils import s3Bucket
from airflow.hooks.postgres_hook import PostgresHook
import utils.data_cleaning as data_cleaning
import pandas as pd
import logging

# def load_temperature_data_to_redshift(config, s3_key_path, key_filter):
#     """  """
#     df = data_cleaning.clean_global_temperature_by_filter(config, s3_key_path, key_filter , create_partitioned_data=False)
#     if df.empty == True:
#         raise Exception("Unable to retrieve data partition for global temperature.")
    
#     return df
#     # TODO
#     #add connection to db
#     #create insert methods to save data    


def load_data_to_redshift(redshift_conn_id, tablename: str, dataframe: pd.DataFrame, commit_every=0, ):
    """ Create a redshift connection to insert dataframe into redshift table. Dataframe headers <must> match with schema table column."""
    
    logging.info("+Creating PosgresHook to insert rows in ({}) table.".format(tablename))
    redshift = PostgresHook(redshift_conn_id)
    conn = redshift.get_conn()
    logging.info("+Inserting data into redshift table.")
    try:
        logging.info("Data to insert: [{}]\n-\n-\n-\n".format(len(dataframe)))
        redshift.insert_rows(table=tablename, 
                            rows=dataframe.values, 
                            target_fields=list(dataframe.columns), 
                            commit_every=400)

        logging.info("Information successfully stored in AWS Redhift database.")
    except Exception as ex:
        logging.info("Unable to save information in redshift table:\nError:\n{}".format(ex))
        raise Exception("Unable to save information into redshift, check data_load module.")




def load_immigration_data_to_redshift(redshift_conn_id, tablename: str, dataframe: pd.DataFrame):
    """ Create a redshift connection to insert dataframe into redshift table. """
    
    logging.info("Creating PosgresHook to insert rows in ({}) table.".format(tablename))
    redshift = PostgresHook(redshift_conn_id)
    conn = redshift.get_conn()
    logging.info("updated.....")
    try:
        #dataframe.to_sql(name=tablename, con=conn, schema="public", if_exists='replace', index=False)
        logging.info(list(dataframe.columns))
        logging.info(dataframe.values)
        redshift.insert_rows(table=tablename, 
                            rows=dataframe.values, 
                            target_fields=list(dataframe.columns), 
                            commit_every=400)

        logging.info("Information successfully stored in AWS Redhift database.")
    except Exception as ex:
        logging.info("Unable to save information in redshift table:\nError:\n{}".format(ex))
        raise Exception("Unable to save information into redshift, check data_load module.")



def load_demographic_data_to_redshift(bucket, filepath):
    """  """
    
    df = data_cleaning.clean_demographic_data(bucket=bucket, filepath=filepath)
    
    if df.empty == True:
        raise Exception("Unable to retrieve data for demographic information.")

    return df
    # TODO
    #add connection to db
    #create insert methods to save data 

def load_airport_codes_data_to_redshift(bucket, filepath, filters):
    """  """
    
    df = data_cleaning.clean_airport_codes_data(bucket=bucket, filepath=filepath, filters=filters)
    
    if df.empty == True:
        raise Exception("Unable to retrieve data for airport codes.")

    return df
    # TODO
    #add connection to db
    #create insert methods to save data 
    
    
# TODO
#missing excel sas data to be loaded.

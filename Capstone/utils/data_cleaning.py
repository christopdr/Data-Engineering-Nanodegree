
import logging
from numpy import datetime64
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType,DateType
import pandas as pd
import numpy as np
from models.s3BucketUtils import s3Bucket, s3Spark
import datetime

#lambda functions
capitalize_funct = lambda text: " ".join([t.capitalize() for t in text.split(" ")]) if len(text.split(" ")) > 0 else text.capitalize()

#General functions
def try_cast_allowed_date(_date) -> str:
    try:
        return datetime.datetime.strptime(str(_date), '%m%d%Y')
    except:
        return datetime.date(1000, 1, 1)

def try_cast_sas_date(_date) -> datetime:
    sas_initial_date = '1960-1-1'
    try:
        return (pd.to_timedelta(int(_date), unit='D') + pd.Timestamp(sas_initial_date)).date()
    except Exception as ex:
        return datetime.date(1000, 1, 1)

def try_cast_str_to_datetime(_date, d_format):
    try:
        return datetime.datetime.strptime(str(_date), d_format)
    except:
        return datetime.date(1000, 1, 1)
    
def __get_dataframe_from_bucket(bucket: s3Bucket, filepath: str, sep=',', file_type='csv') -> pd.DataFrame:
    """ Get Dataframe from s3 bucket filepath. """
    if file_type=='csv':
        df = bucket.get_csv_dataframe(key_name_bucket=filepath, sep=sep)
    if file_type=='snappy.parquet':
        df = bucket.get_snappy_parquet_dataframe(key_name_bucket=filepath)
    
    if df.empty == True:
        raise Exception("Dataframe is not present, instead empty variable found. Please check csv data for: \n{}".format(filepath))
    
    return df
    
#Global temperature functions

def __create_temperature_partitions_s3(spark, s3_key_path, key_filter) -> bool:
    """ Based on a big csv file, the function split the information and store it by key value on s3. """
    #"s3a://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country"
    df_temperature = spark.get_dataframe_from_bucket(s3_path=s3_key_path, header=True)
    return spark.save_partitioned_data_to_bucket(spark_dataframe=df_temperature, partition_filter=key_filter, s3_key_path=s3_key_path)

def __temperature_dataframe_cleaning(df_temperature):
    """ Temperature dataframe process to remove empty rows, and casts of dates, and strings """

    max_rows = len(df_temperature)
    if df_temperature["date"].count() != max_rows or df_temperature["City"].count() != max_rows:
        df_temperature.dropna(subset = ["date", "City"], inplace=True)
    
    df_temperature['date'] = df_temperature['date'].apply(try_cast_str_to_datetime, args=("%Y-%d-%M",))
    df_temperature['City'] = df_temperature['City'].apply(capitalize_funct)
    
    
    return df_temperature

def __get_temperature_dataframe(spark, s3_key_path, key_filter) -> pd.DataFrame:
    """ Return a dataframe created partition from s3 with a schema format, selected by key filter"""
    temperature_schema = StructType([StructField("date", StringType(), True),
                        StructField("AverageTemperature", FloatType(), True),
                        StructField("AverageTemperatureUncertainty", FloatType(), True),
                        StructField("City", StringType(), True),
                        StructField("Latitude", StringType(), True),
                        StructField("Longitude", StringType(), True)])
    #"s3a://udacity-practice-bucket/capstone_data/Global_Temperatures_By_Country/Country=United States"
    s3_partition_path = "{}{}".format(s3_key_path, key_filter)
    df_temperature = spark.get_csv_with_schema(s3_key_path=s3_partition_path, header=False, schema=temperature_schema).toPandas()
    
    return df_temperature

def clean_global_temperature_by_filter(spark, s3_key_path, key_filter , create_partitioned_data=False) -> pd.DataFrame:
    """ Run process to split and get partitioned data, based on the case of study."""

    if create_partitioned_data:
        status = __create_temperature_partitions_s3(spark=spark, s3_key_path=s3_key_path, key_filter=key_filter)
        if not status:
            print("Data partitioned was not saved successfully...")
            return
    
    df_temperature = __get_temperature_dataframe(spark=spark, s3_key_path=s3_key_path, key_filter=key_filter)
    if df_temperature.empty == True:
        print("Retrieving data partition from bucket was not possible...Exiting cleaning global temperature function.")
        return None

    return __temperature_dataframe_cleaning(df_temperature)

#Immigration functions

def __immigration_bucket_check(bucket, filepath) -> None:
    """ Run bucket and filepath (key) validation to check if they exist."""
    if bucket.get_bucket_name() not in bucket.get_buckets_available():
        raise Exception("The bucket name ({}) provided is not present in the bucket list available.\nAvailable buckets:\n\n{}".format(bucket.get_bucket_name(), bucket.get_buckets_available()))
    
    if filepath not in bucket.get_bucket_keys():
        for k in bucket.get_bucket_keys():
            print(k)
        raise Exception("The file location (key={}) in bucket is not present. Check path location.".format(filepath))

def __immigration_dataframe_cleaning(df_immigration: pd.DataFrame) -> pd.DataFrame:
    """ Clean immigration dataframe. Remove unwanted columns, rename columns, cast datetimes. """
    #Drop not useful columns, I made this assumption for my model
    #df_immigration = df_immigration.fillna()
    df_immigration.drop(["occup", "entdepu", "insnum", "visapost"], axis=1, inplace=True)
  
    #Rename column names to make it easier to understand
    column_rename = {'i94yr': 'arrival_year', 'i94mon':'arrival_month', 'i94cit': 'citizenship_code', 'i94res': 'residence_code', 'i94port' : 'port_code', 
                 'arrdate' : 'arrival_date', 'i94mode':'arrival_mode', 'i94addr' : 'addr_code', 'depdate': 'departure_date', 'i94bir' : 'age', 'i94visa':'visa_type', 'dtadfile': 'log_date',
                'entdepa' : 'arrival_flag', 'entdepd' : 'departure_flag', 'matflag': 'match_flag', 'biryear':'birth_year', 'dtaddto': 'date_allowed_stay',  
                 'admnum' : 'admision_num', 'fltno' : 'flight_num'}

    df_immigration = df_immigration.rename(columns=column_rename)

    #Change data types for columns objects. I made this to simplify the data manipulation
    type_change = {'port_code': 'str', 'addr_code': 'str', 'arrival_flag' : 'str', 'departure_flag': 'str', 'match_flag': 'str', 'gender': 'str', 'airline': 'str', 'visatype':'str', 'flight_num': 'str'}
    df_immigration = df_immigration.astype(type_change)

    #drop empty columns with identifier and ccid
    df_immigration.dropna(subset=['cicid'], inplace=True)

    #pandas cannot convert Nan or infinite value, setting those values to 0, in order to keep them.
    #Other possible solution could be to drop na values, but it's going to depend on customer necessities
    df_immigration['arrival_year'] = df_immigration['arrival_year'].fillna(0)
    df_immigration['arrival_month'] = df_immigration['arrival_month'].fillna(0)
    df_immigration['citizenship_code'] = df_immigration['citizenship_code'].fillna(0)
    df_immigration['residence_code'] = df_immigration['residence_code'].fillna(0)
    df_immigration['birth_year'] = df_immigration['birth_year'].fillna(0)
    df_immigration['visa_type'] = df_immigration['visa_type'].fillna(0)
    df_immigration['age'] = df_immigration['age'].fillna(0)
    df_immigration['arrival_mode'] = df_immigration['arrival_mode'].fillna(0)
    df_immigration['admision_num'] = df_immigration['admision_num'].fillna(0)

    # Cast conversion from float64 to int32 
    type_change = {'cicid': 'int32', 'arrival_year': 'int32', 'arrival_month': 'int32',
                    'citizenship_code': 'int32', 'residence_code': 'int32', 'birth_year': 'int32', 
                    'visa_type' : 'int32', 'age': 'int32',
                    'arrival_mode': 'int32', 'admision_num':'int32'}
    df_immigration = df_immigration.astype(type_change)
    
    

    #Format allowed stay date
    df_immigration['date_allowed_stay'] = df_immigration['date_allowed_stay'].apply(try_cast_allowed_date)
    df_immigration['log_date'] = df_immigration['log_date'].apply(try_cast_str_to_datetime, args=("%Y%M%d",))

    #Format and cast SAS date
    df_immigration['arrival_date'] = df_immigration['arrival_date'].apply(try_cast_sas_date)
    df_immigration['departure_date'] = df_immigration['departure_date'].apply(try_cast_sas_date)
    
    df_immigration.dropna(thresh=5, inplace=True)
    df_immigration.reset_index(drop=True)
    logging.info(df_immigration.dtypes)

    return df_immigration

def clean_immigration_data(bucket, filepath) -> pd.DataFrame:
    """ Run process to get immigration data from s3 bucket, clean and format dataframe. Ready to save on database """

    __immigration_bucket_check(bucket=bucket, filepath=filepath)
    
    df = __get_dataframe_from_bucket(bucket=bucket, filepath=filepath,file_type='snappy.parquet')

    return __immigration_dataframe_cleaning(df)


#Demographic functions

def __demographic_columns_check(df_demographic):
    """ Validate that key columns are present, avoid throwing unexpected errors if not present. """
    key_columns = ["City", "State", "State Code"]
    #df_columns = [key for key in list(df_demographic.columns)[0].split(";")]

    for key in key_columns:
        if key not in df_demographic.columns:
            raise Exception("Key ({}) is not present in demographic dataframe columns.\nDemographic columns:\n{}\nAborting cleaning...".format(key, df_demographic.columns))

def __demographic_dataframe_cleaning(df_demographic):
    """ Demographic dataframe cleaning. Standardize of Capital letters in City, State, and upper case for State Code. """
    
    __demographic_columns_check(df_demographic)
    
    df_demographic["City"] = df_demographic["City"].apply(capitalize_funct)
    df_demographic["State"] = df_demographic["State"].apply(capitalize_funct)
    df_demographic["State Code"] = df_demographic["State Code"].apply(lambda s: s.upper())
    
    return df_demographic

def clean_demographic_data(bucket, filepath):
    """ Run process to get data (demographic) from s3 bucket and give it format. Ready to upload to Redshift. """
    
    df = __get_dataframe_from_bucket(bucket=bucket, filepath=filepath, sep=';')
    
    return __demographic_dataframe_cleaning(df_demographic=df)



#Airport Codes functions

region_cleaner = lambda iso_reg: iso_reg.split('-')[1].upper() if len(iso_reg.split('-')) > 1 else iso_reg

def __airport_codes_columns_filter_check(df_codes, filters):
    """ Validate that the filters provided are included in airport codes dataframe columns. """
    for key in filters.keys():
        if key not in df_codes.columns:
            raise Exception("Key ({}) is not present in airport_codes dataframe columns.\nAirport Code columns:\n{}\nAborting cleaning...".format(key, df_codes.columns))

def __airport_codes_dataframe_cleaning(df_codes, filters):
    """ Airport codes dataframe cleaning. Filter by iso_country (only filter available). Drop columns with low rate of data, and give new structure for region column. """

    __airport_codes_columns_filter_check(df_codes=df_codes, filters=filters)

    if 'iso_country' in filters.keys():
        df_aircodes = df_codes[df_codes['iso_country'] == filters['iso_country']]

    #keep only region, remove country code and '-'
    key = "iso_region"
    df_aircodes[key] = df_aircodes[key].apply(region_cleaner)

    #rename columns
    rename_dict = {"iso_region": "state", "iso_country": "country", "ident": "identifier", "type":"ship_type"}
    df_aircodes = df_aircodes.rename(columns=rename_dict)

    #we are going to remove continent and iata_code due to lack of consistency and low rate of data showed.
    df_aircodes = df_aircodes.drop(["continent", "iata_code"], axis=1)

    return df_aircodes

def clean_airport_codes_data(bucket, filepath, filters):
    """ Run process to get data (airport codes) from s3 bucket and give it format. Ready to upload to Redshift """
    
    df = __get_dataframe_from_bucket(bucket=bucket, filepath=filepath)
    
    return __airport_codes_dataframe_cleaning(df_codes=df, filters=filters)



#get static data from sas file, parsed in excel file
#labels tables function
def get_labels_from_excel_file(bucket, bucket_key_name):
    """  """
    labels_map = {
        'I94CIT & I94RES' : 'country_residence_code',
        'I94ADDR' : 'address_code',
        'I94PORT' : 'port_code',
        'I94MODE' : 'flight_mode',
        'I94VISA' : 'visatype', 
        'Flags Meaning' : 'flag_meaning',

    }

    s3_bucket_obj = bucket.get_bucket_object(key_name_bucket=bucket_key_name)
    xls_file = pd.ExcelFile(s3_bucket_obj['Body'])

    if not xls_file:
        raise Exception("Labels excel file object not provided. Aborting with errors...")
    
    #dataframe dictionaries
    labels_map_df = {}

    for label in labels_map.keys():
        df = xls_file.parse(sheet_name=label)
        labels_map_df[labels_map[label]] = df
    
    return labels_map_df

def __get_dataframe_from_excel(file_path, sheet_name):
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as ex:
        print("Unable to get labels sheet from excel file.\n {}".format(ex))
        return None


def get_sas_labels_dataframes(file_path: str, labels_map: dict):
    """
        Returns a list of the dataframes with the sas labeles definitions depending on their sheet name.
        
        Arguments:
            - file_path (str) excel file location of labels.
            - sheet_names (list) list with the names of the sheet names.
        
        Returns:
            - (pd.DataFrame)
    """
    labels_df_map = {}
    for name in labels_map.keys():
        labels_df_map[labels_map[name]] = __get_dataframe_from_excel(file_path=file_path, sheet_name=name)
        if labels_df_map[labels_map[name]].empty == True:
            raise Exception('Unable to retrieve sas label table, please check sas excel file.')
    
    return labels_df_map
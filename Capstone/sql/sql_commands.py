CREATE_IMMIGRATION_TABLE = """
CREATE TABLE IF NOT EXISTS "immigration" (
  "id" INT IDENTITY UNIQUE,
  "cicid" int UNIQUE,
  "arrival_year" int,
  "arrival_month" int,
  "citizenship_code" int,
  "residence_code" int,
  "port_code" varchar,
  "arrival_date" date,
  "arrival_mode" int,
  "addr_code" varchar,
  "departure_date" date,
  "age" int,
  "visa_type" int,
  "count" float,
  "log_date" date,
  "arrival_flag" varchar,
  "departure_flag" varchar,
  "match_flag" varchar,
  "birth_year" int,
  "date_allowed_stay" date,
  "gender" varchar,
  "airline" varchar,
  "admision_num" int,
  "flight_num" varchar,
  "visatype" varchar,
  PRIMARY KEY ("id", "identifier", "cicid")
);
"""
CREATE_ADDR_CODE_TABLE = """
CREATE TABLE IF NOT EXISTS "address_code" (
  "id" INT IDENTITY UNIQUE,
  "code_id" varchar UNIQUE PRIMARY KEY,
  "state" varchar
);
"""

CREATE_PORT_CODE_TABLE = """
CREATE TABLE IF NOT EXISTS "port_code" (
  "id" INT IDENTITY UNIQUE,
  "code_id" varchar UNIQUE PRIMARY KEY,
  "port" varchar
);
"""

CREATE_COUNTRY_RES_TABLE = """
CREATE TABLE IF NOT EXISTS "country_residence_code" (
  "id" INT IDENTITY UNIQUE, 
  "code_id" int UNIQUE PRIMARY KEY,
  "country" varchar
);

"""

CREATE_FLIGHT_MODE_TABLE = """
CREATE TABLE IF NOT EXISTS "flight_mode" (
  "id" int UNIQUE PRIMARY KEY,
  "mode" varchar
);
"""

CREATE_VISA_TYPE_TABLE = """
CREATE TABLE IF NOT EXISTS "visatype" (
  "id" int UNIQUE PRIMARY KEY,
  "v_type" varchar
);
"""

CREATE_FLAG_MEANING_TABLE = """
CREATE TABLE IF NOT EXISTS "flag_meaning" (
  "id" varchar UNIQUE PRIMARY KEY,
  "description" varchar
);
"""

CREATE_DEMOGRAPHIC_TABLE = """
CREATE TABLE IF NOT EXISTS "demographic" (
  "id" INT IDENTITY UNIQUE PRIMARY KEY,
  "city" varchar,
  "state" varchar,
  "median_age" float,
  "male_population" int,
  "female_population" int,
  "total_population" int,
  "number_veterans" int,
  "foreign_born" int,
  "average_household_size" float,
  "state_code" varchar,
  "race" varchar,
  "count" int
);
"""

CREATE_AIRPORT_CODE_TABLE = """
CREATE TABLE IF NOT EXISTS "airport_code" (
  "id" INT IDENTITY UNIQUE
  "identifier" varchar UNIQUE PRIMARY KEY,
  "ship_type" varchar,
  "name" varchar,
  "elevation_ft" float,
  "country" varchar,
  "state" varchar,
  "municipality" varchar,
  "gps_code" varchar,
  "local_code" varchar,
  "coordinates" varchar
);
"""

CREATE_GLOBAL_TEMPERATURE_TABLE = """
CREATE TABLE IF NOT EXISTS "global_temperature" (
  "id" INT IDENTITY UNIQUE,
  "avg" float,
  "avg_uncertainty" float,
  "city" varchar,
  "country" varchar,
  "latitude" varchar,
  "longitude" varchar
);
"""

CREATE_REFERENCES_TO_TABLES = """
ALTER TABLE "immigration" ADD FOREIGN KEY ("addr_code") REFERENCES "address_code" ("code_id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("port_code") REFERENCES "port_code" ("code_id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("citizenship_code") REFERENCES "country_residence_code" ("code_id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("residence_code") REFERENCES "country_residence_code" ("code_id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("arrival_mode") REFERENCES "flight_mode" ("id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("visa_type") REFERENCES "visatype" ("id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("arrival_flag") REFERENCES "flag_meaning" ("id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("departure_flag") REFERENCES "flag_meaning" ("id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("match_flag") REFERENCES "flag_meaning" ("id");

ALTER TABLE "immigration" ADD FOREIGN KEY ("addr_code") REFERENCES "demographic" ("state_code");

ALTER TABLE "immigration" ADD FOREIGN KEY ("addr_code") REFERENCES "airport_code" ("state");

ALTER TABLE "demographic" ADD FOREIGN KEY ("city") REFERENCES "global_temperature" ("city");
"""

CREATE_TABLES_QUERIES_MAP = [
  {'table_name' : 'immigration', 'sql_query' : CREATE_IMMIGRATION_TABLE },
  {'table_name' : 'addr_code', 'sql_query' : CREATE_ADDR_CODE_TABLE },
  {'table_name' : 'port_code', 'sql_query' : CREATE_PORT_CODE_TABLE },
  {'table_name' : 'country', 'sql_query' : CREATE_COUNTRY_RES_TABLE },
  {'table_name' : 'flight_mode', 'sql_query' : CREATE_FLIGHT_MODE_TABLE },
  {'table_name' : 'visa_type', 'sql_query' : CREATE_VISA_TYPE_TABLE },
  {'table_name' : 'flag_meaning', 'sql_query' : CREATE_FLAG_MEANING_TABLE},
  {'table_name' : 'demographic', 'sql_query' : CREATE_DEMOGRAPHIC_TABLE},
  {'table_name' : 'airport_code', 'sql_query' : CREATE_AIRPORT_CODE_TABLE},
  {'table_name' : 'global_temperature', 'sql_query' : CREATE_GLOBAL_TEMPERATURE_TABLE}]

  #cluster22Con



DROP_TABLES_QUERY = """
  DROP TABLE IMMIGRATION;
  DROP TABLE ADDRESS_CODE;
  DROP TABLE PORT_CODE;
  DROP TABLE COUNTRY_RESIDENCE_CODE;
  DROP TABLE FLIGHT_MODE;
  DROP TABLE VISATYPE;
  DROP TABLE FLAG_MEANING;
  DROP TABLE DEMOGRAPHIC;
  DROP TABLE AIRPORT_CODE;
  DROP TABLE GLOBAL_TEMPERATURE;
  """
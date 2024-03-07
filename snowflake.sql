---> create databse
CREATE DATABASE if not exists finance_api;
---> create warehouse
CREATE WAREHOUSE if not exists finance_wh;
---> use database, warehouse, schema and role
USE DATABASE finance_api;
USE WAREHOUSE finance_wh;
USE SCHEMA finance_api.public;
USE ROLE accountadmin;

---> create table
CREATE OR REPLACE TABLE finance_api.public.state_crime_rates (
    state_name varchar(50),
    year INTEGER,
    population INTEGER,
    property_crime_rates_all NUMBER(10, 2),
    property_crime_rates_burglary NUMBER(10, 2),
    property_crime_rates_larceny NUMBER(10, 2),
    property_crime_rates_motor NUMBER(10, 2),
    violent_crime_rates_all NUMBER(10, 2),
    violent_crime_rates_assault NUMBER(10, 2),
    violent_crime_rates_murder NUMBER(10, 2),
    violent_crime_rates_rape NUMBER(10, 2),
    violent_crime_rates_robbery NUMBER(10, 2),
    property_crime_total_all INTEGER,
    property_crime_total_burglary INTEGER,
    property_crime_total_larceny INTEGER,
    property_crime_total_motor INTEGER,
    violent_crime_total_all INTEGER,
    violent_crime_total_assault INTEGER,
    violent_crime_total_murder INTEGER,
    violent_crime_total_rape INTEGER,
    violent_crime_total_robbery INTEGER
);

--> create stage for uploaded file
CREATE OR REPLACE STAGE finance_api.public.state_crime_rates_stage;

---> describe the format of the file to be imported
CREATE OR REPLACE FILE FORMAT crime_rate_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true
  COMPRESSION = gzip;

---> Upload your CSV file from local folder to a Snowflake stage
PUT file:///home/sutring/extra_data/state_crime.csv @finance_api.public.state_crime_rates_stage;

---> check upload status
select
  c.$1,
  c.$2,
  c.$3
from @state_crime_rates_stage (file_format => crime_rate_format) c;

---> load data as it is organized in a CSV file
-- COPY INTO finance_api.public.state_crime_rates FROM @finance_api.public.state_crime_rates_stage;

COPY INTO finance_api.public.state_crime_rates
FROM (
    SELECT
        REGEXP_REPLACE(col.$1, '[^a-zA-Z]', '')::VARCHAR(50) as state_name,
        REGEXP_REPLACE(col.$2, '[^0-9]', '')::INTEGER AS year,
        REGEXP_REPLACE(col.$3, '[^0-9]', '')::INTEGER AS population,
        REGEXP_REPLACE(col.$4, '[^0-9.]', '')::NUMBER(10, 2) AS property_crime_rates_all,
        REGEXP_REPLACE(col.$5, '[^0-9.]', '')::NUMBER(10, 2) AS property_crime_rates_burglary,
        REGEXP_REPLACE(col.$6, '[^0-9.]', '')::NUMBER(10, 2) AS property_crime_rates_larceny,
        REGEXP_REPLACE(col.$7, '[^0-9.]', '')::NUMBER(10, 2) AS property_crime_rates_motor,
        REGEXP_REPLACE(col.$8, '[^0-9.]', '')::NUMBER(10, 2) AS violent_crime_rates_all,
        REGEXP_REPLACE(col.$9, '[^0-9.]', '')::NUMBER(10, 2) AS violent_crime_rates_assault,
        REGEXP_REPLACE(col.$10, '[^0-9.]', '')::NUMBER(10, 2) AS violent_crime_rates_murder,
        REGEXP_REPLACE(col.$11, '[^0-9.]', '')::NUMBER(10, 2) AS violent_crime_rates_rape,
        REGEXP_REPLACE(col.$12, '[^0-9.]', '')::NUMBER(10, 2) AS violent_crime_rates_robbery,
        REGEXP_REPLACE(col.$13, '[^0-9]', '')::INTEGER AS property_crime_total_all,
        REGEXP_REPLACE(col.$14, '[^0-9]', '')::INTEGER AS property_crime_rates_burglary,
        REGEXP_REPLACE(col.$15, '[^0-9]', '')::INTEGER AS property_crime_total_larceny,
        REGEXP_REPLACE(col.$16, '[^0-9]', '')::INTEGER AS property_crime_total_motor,
        REGEXP_REPLACE(col.$17, '[^0-9]', '')::INTEGER AS violent_crime_total_all,
        REGEXP_REPLACE(col.$18, '[^0-9]', '')::INTEGER AS violent_crime_total_assault,
        REGEXP_REPLACE(col.$19, '[^0-9]', '')::INTEGER AS violent_crime_total_murder,
        REGEXP_REPLACE(col.$20, '[^0-9]', '')::INTEGER AS violent_crime_total_rape,
        REGEXP_REPLACE(col.$21, '[^0-9]', '')::INTEGER AS violent_crime_total_robbery
    FROM @state_crime_rates_stage (file_format => crime_rate_format) col)
    ON_ERROR = 'CONTINUE';
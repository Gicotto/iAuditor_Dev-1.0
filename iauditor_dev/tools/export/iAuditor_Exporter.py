import requests
import polars as pl
import iAuditor_Exporter_Config as Config
import configparser
from datetime import datetime
from SnowflakeDataImporter import SnowflakeDataImporter
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def read_last_call_time(api_config):
    try:
        last_call_time = api_config.get('API', 'last_call_time')
        print(f"Last call time is: {last_call_time}")
        return last_call_time
    except (configparser.NoOptionError, ValueError):
        return None


def write_last_call_time(api_config):
    formatted_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z').replace(':', '%3A')
    print(f"Formatted time is: {formatted_time}")

    """    
    set to true when ready to run as prod/cron job
    """
    # api_config.set('API', 'last_call_time', formatted_time)
    # with open(Config.exporter_config, 'w') as configfile:
    #     api_config.write(configfile)

"""
Takes the base URL and key as input and returns the parsed JSON response
"""
def call_api(base_url, api_config):

    modified_after = read_last_call_time(api_config)
    if modified_after is None:
        raise ValueError(
            "The 'modified_after' parameter is None. Ensure 'last_call_time' is correctly set in the config.")
    print(f"Base Url is: {base_url}")

    keep_url = ["https://api.safetyculture.io/feed/schedule_assignees", "https://api.safetyculture.io/feed/users"]
    if base_url in keep_url:
        url = base_url
    else:
        url = f"{base_url}?modified_after={modified_after}"
    print(f"URL is: {url}")

    api_token = api_config.get('API', 'key')

    headers = {"accept": "application/json",
               "authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers)

    parsed_json = response.json()

    # set to active to change times when ready for prod!!
    write_last_call_time(api_config)
    return parsed_json


"""
Main function to call the API and create the DataFrame
"""
def main():
    api_config = configparser.RawConfigParser()
    api_config.read(Config.exporter_config)
    snowflake_config = configparser.RawConfigParser()
    snowflake_config.read(Config.snowflake_config)

    for key in Config.urls:
        # Call API
        url = Config.urls[key]
        json_payload = call_api(base_url=url, api_config=api_config)
        table_name = Config.table_prefix + key.upper()
        data_date = read_last_call_time(api_config)

        print(f"Table name is: {table_name}")

        current_env = SnowflakeDataImporter.get_environment()
        database = SnowflakeDataImporter.get_database(current_env)

        conn = snowflake.connector.connect(
            user=snowflake_config.get(database, 'user'),
            password=snowflake_config.get(database, 'password'),
            account=snowflake_config.get(database, 'account'),
            database=snowflake_config.get(database, 'raw_database'),
            schema=snowflake_config.get(database, 'raw_schema'),
            warehouse=snowflake_config.get(database, 'warehouse'),
            role=snowflake_config.get(database, 'role')
            )

        date_executed = Config.current_time

        # Create DataFrame
        df = pl.DataFrame({
            "DATA_DATE": [data_date],
            "JSON_PAYLOAD": [json_payload],
            "RECORD_INSERTED_AT": [date_executed]
            }, strict=False)

        pd_df = df.to_pandas()

        # Write DataFrame to Snowflake
        print(f"Writing DataFrame to Snowflake for {key}")
        try:
            success, nchunks, nrows, _ = write_pandas(conn, pd_df, table_name)
            print(f"Successfully uploaded dataframe to table: {table_name}")
        except Exception as e:
            print(f"Unable to upload to table: {table_name} - due to: {e}")

if __name__ == "__main__":
    main()

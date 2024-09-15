import requests
import pandas as pd
import iAuditor_Exporter_Config as Config
import configparser
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from urllib.parse import unquote
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../sql-flatten')))
from iAuditor_Processor import IAuditorProcessor


class IAuditorExporter:
    """
    Reads the last call time from the config file to use in the current API call. Allows querying for recent data only.
    See @write_last_call_time for setting the last call time.
    """""
    @staticmethod
    def read_last_call_time(api_config):
        try:
            last_call_time = api_config.get('API', 'last_call_time')
            print(f"Last call time is: {last_call_time}")
            return last_call_time
        except (configparser.NoOptionError, ValueError):
            return None

    """
    Writes the last call time to the config file for use in the next API call. Allows querying for recent data only.
    See @read_last_call_time for reading the last call time.
    """
    @staticmethod
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
    Takes the api endpoint url as base_url and the config file for bearer token and last call time. Makes a GET request
    to the provided api endpoint, using the last call time as a query parameter to get only the data that has been
    modified since the last call. Returns the parsed json response.
    
    Note: Not all endpoints require the modified_after query parameter. Some endpoints require no query parameters.
    Defined by keep_url list.
    """
    @staticmethod
    def call_api(base_url, api_config):
        modified_after = IAuditorExporter.read_last_call_time(api_config)
        if modified_after is None:
            raise ValueError(
                "The 'modified_after' parameter is None. Ensure 'last_call_time' is correctly set in the config.")
        print(f"Base Url is: {base_url}")

        if base_url in Config.keep_url:
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
        IAuditorExporter.write_last_call_time(api_config)
        return parsed_json

    """
    Main method to run the exporter. Calls the API for each endpoint in the Config.urls dictionary, then writes the
    response to Snowflake. Prints the success percentage of the API calls.
    """
    @staticmethod
    def run():
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../export'))
        api_config_path = os.path.join(config_dir, Config.exporter_config)

        if not os.path.exists(api_config_path):
            raise FileNotFoundError(f"Configuration file not found: {api_config_path}")

        api_config = configparser.RawConfigParser()
        api_config.read(api_config_path)
        snowflake_config = configparser.RawConfigParser()
        snowflake_config.read(Config.snowflake_config)

        current_count = 1
        success_count = 0
        total_count = len(Config.urls)
        failure_count = 0

        for key in Config.urls:
            # Call API
            url = Config.urls[key]
            json_payload = IAuditorExporter.call_api(base_url=url, api_config=api_config)
            table_name = Config.table_prefix + key.upper()
            data_date = unquote(IAuditorExporter.read_last_call_time(api_config))

            print(f"Table name is: {table_name}")
            print(f"Attempting to Upload API call ~ {current_count}/{total_count}")

            conn = IAuditorProcessor.get_snowflake_connection(snowflake_config)

            date_executed = Config.current_time
            print(f"Date executed is: {date_executed}")

            # Create DataFrame directly with pandas
            pd_df = pd.DataFrame({
                "DATA_DATE": [data_date],
                "JSON_PAYLOAD": [json_payload],
                "RECORD_INSERTED_AT": [date_executed]
            })

            # Write DataFrame to Snowflake
            print(f"Writing DataFrame to Snowflake for {key}")
            try:
                success = write_pandas(conn, pd_df, table_name)
                print(f"Successfully uploaded dataframe to table: {table_name} \n")
                success_count += 1

            except Exception as e:
                print(f"Unable to upload to table: {table_name} - due to: {e} \n")
                failure_count += 1

            current_count += 1

        # Calculate and display the success percentage
        success_percentage = (success_count / total_count) * 100
        print(f"Success percentage: {success_percentage:.2f}%")

        failure_percentage = (failure_count / total_count) * 100
        print(f"Failure percentage: {failure_percentage:.2f}%")

if __name__ == "__main__":
    IAuditorExporter.run()
# iAuditor_Utils.py
import snowflake.connector
from datetime import datetime
import pandas as pd
import os
import uuid
import requests
import iAuditor_Exporter_Config as Config
import urllib.parse


class IAuditorUtils:
    batch_id = None
    logs_df = pd.DataFrame(columns=["BATCH_ID", "LOG_TYPE", "MESSAGE", "RECORD_INSERTED_AT"])

    @staticmethod
    def generate_uuid():
        IAuditorUtils.batch_id = str(uuid.uuid4())

    @staticmethod
    def get_environment():
        dir_path = os.path.dirname(os.path.realpath(__file__))

        if '_dev' in dir_path:
            return '_dev'
        if '_stage' in dir_path:
            return '_stage'
        if '_prod' in dir_path:
            return '_prod'
        else:
            return dir_path

    @staticmethod
    def get_database(env):
        if env == '_dev':
            return 'DEV_DATABASE'
        if env == '_stage':
            return 'STAGE_DATABASE'
        if env == '_prod':
            return 'PROD_DATABASE'
        else:
            IAuditorUtils.log(msg=f"Could not find database for environment: {env}", level="ERROR", log_file=True, print_msg=True)
            return False

    @staticmethod
    def get_snowflake_connection(snowflake_config, location='IAUDITOR'):
        current_env = IAuditorUtils.get_environment()
        database = IAuditorUtils.get_database(current_env)

        raw_schema_name = snowflake_config.get(database, 'trf_schema')

        if location == 'LOG':
            raw_schema_name = snowflake_config.get(database, 'log_schema')


        # Snowflake connection setup
        conn = snowflake.connector.connect(
            user=snowflake_config.get(database, 'user'),
            password=snowflake_config.get(database, 'password'),
            account=snowflake_config.get(database, 'account'),
            database=snowflake_config.get(database, 'trf_database'),
            schema=raw_schema_name,
            warehouse=snowflake_config.get(database, 'warehouse'),
            role=snowflake_config.get(database, 'role')
        )

        return conn

    @staticmethod
    def log(msg=None, level=None, log_file=True, print_msg=True):
        if print_msg:
            print(msg)
        if log_file:
            # Create a DataFrame with the new log entry
            new_log_entry = pd.DataFrame([{
                "BATCH_ID": IAuditorUtils.batch_id,
                "LOG_TYPE": level,
                "MESSAGE": msg,
                "RECORD_INSERTED_AT": datetime.now()
            }])

            # Append the new log entry to the logs_df DataFrame using pd.concat
            IAuditorUtils.logs_df = pd.concat([IAuditorUtils.logs_df, new_log_entry], ignore_index=True)
        return

    @staticmethod
    def call_api(base_url, api_config):
        modified_after = IAuditorUtils.read_last_call_time(api_config)
        if modified_after is None:
            raise ValueError(
                "The 'modified_after' parameter is None. Ensure 'last_call_time' is correctly set in the config.")

        if base_url in Config.keep_url:
            url = base_url
        else:
            url = f"{base_url}?modified_after={modified_after}"

        api_token = api_config.get('API', 'key')

        headers = {"accept": "application/json",
                   "authorization": f"Bearer {api_token}"}

        response = requests.get(url, headers=headers)
        parsed_json = response.json()

        # set to active to change times when ready for prod!!
        IAuditorUtils.write_last_call_time(api_config)
        return parsed_json

    @staticmethod
    def write_last_call_time(api_config):
        current_time = datetime.utcnow().isoformat() + 'Z'
        api_config.set('API', 'last_call_time', current_time)
        with open(Config.exporter_config, 'w') as configfile:
            api_config.write(configfile)

    @staticmethod
    def read_last_call_time(api_config):
        return api_config.get('API', 'last_call_time', fallback=None)

    @staticmethod
    def unquote(url):
        return urllib.parse.unquote(url)
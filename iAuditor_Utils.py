# iAuditor_Utils.py
import snowflake.connector
from datetime import datetime
import time
import pandas as pd
import os
import uuid
import requests
import iAuditor_Exporter_Config as Config
import urllib.parse
import json
from snowflake.connector.pandas_tools import write_pandas



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
    def get_snowflake_connection(location='IAUDITOR'):
        current_env = IAuditorUtils.get_environment()
        database = IAuditorUtils.get_database(current_env)

        schema_name = os.environ["trf_schema"]
        database = os.environ['trf_database']

        if location == 'LOG':
            schema_name = os.environ["log_schema"]
            database = os.environ["raw_database"]

        if location == 'RAW':
            schema_name = os.environ["raw_schema"]
            database = os.environ["raw_database"]

        print(f"Connecting to Snowflake with database: {database}, schema: {schema_name}")
        # Snowflake connection setup
        conn = snowflake.connector.connect(
            user=os.environ["user"],
            password=os.environ["password"],
            account=os.environ["account"],
            database=database,
            schema=schema_name,
            warehouse=os.environ['warehouse'],
            role=os.environ['role']
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
    def upload_logs_to_snowflake():
        logs_table_name = os.environ["log_table"]
        conn = IAuditorUtils.get_snowflake_connection(location='LOG')
        log_df = pd.DataFrame(IAuditorUtils.logs_df)
        try:
            success = write_pandas(conn, log_df, logs_table_name)
            if success:
                IAuditorUtils.log(msg="Logs successfully uploaded to Snowflake.", level="INFO",
                    log_file=True, print_msg=True)
            else:
                IAuditorUtils.log(msg="Failed to upload logs to Snowflake.", level="ERROR",
                    log_file=True, print_msg=True)
        except Exception as e:
            IAuditorUtils.log(msg=f"Error uploading logs to Snowflake: {e}", level="ERROR",
                log_file=True, print_msg=True)
        conn.close()


    @staticmethod
    def call_api(base_url):

        url = base_url

        api_token = os.environ["api_token"]

        headers = {"accept": "application/json",
                   "authorization": f"Bearer {api_token}"}

        response = requests.get(url, headers=headers)
        parsed_json = response.json()

        return parsed_json

    @staticmethod
    def get_duration(start_time, with_label = True):
        end_time = time.time()
        total_time = round((end_time - start_time) / 60, 2)
        if with_label:
            return f"Duration: {total_time} mins"
        return total_time


    @staticmethod
    def read_last_call_time():
        conn = IAuditorUtils.get_snowflake_connection(location='IAUDITOR')
        cursor = conn.cursor()
        table_name = os.environ["call_table"]

        query = f"""
        SELECT MAX(MODIFIED_AT) AS modified_date
        FROM {table_name}
        """

        cursor.execute(query)
        result = cursor.fetchone()

        modified_date = result[0] if result else None

        if modified_date:
            modified_date = modified_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        cursor.close()
        conn.close()

        return modified_date

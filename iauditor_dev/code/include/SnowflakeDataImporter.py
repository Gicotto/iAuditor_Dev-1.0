import requests
import configparser
import json
import snowflake.connector
from pathlib import Path
import polars as pl
import pandas as pd
from config import *
import os
import csv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import write_pandas
from Utils import *
import os

processed_location = 'files/processing/ready/'



class SnowflakeDataImporter:
    prefix = '../processing/ready'
    suffix = '.dat'


    @staticmethod
    def import_ready_files_to_snowflake(file):
        was_uploaded = False

        # Get the current directory
        current_dir = os.path.dirname(os.path.realpath(__file__))
        config_file_path = os.path.join(current_dir, 'config.ini')

        # Read config.ini file
        snowflake_config = configparser.ConfigParser()
        snowflake_config.read(config_file_path)

        Utils.log(msg=f"Importing file: {file} to Snowflake...", l_bln_print_to_screen=True, l_bln_log_to_file=True)
        Utils.log(msg="Exiting Program...", l_bln_print_to_screen=True, l_bln_log_to_file=True, l_bln_die=True)

        # Actions.dat uploading
        if '/actions.dat' in file:
            Utils.log(msg=f"Found actions.dat: {file}", l_bln_print_to_screen=True, l_bln_log_to_file=False)

            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # inspection_items.dat uploading
        if '/inspection_items.dat' in file:
            Utils.log(msg=f"Found inspection_items.dat: {file}", l_bln_print_to_screen=True,
                      l_bln_log_to_file=False)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # action_assignees.dat uploading
        if '/action_assignees.dat' in file:
            Utils.log(msg=f'Found action_assignees.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # group_users.dat uploading
        if '/group_users.dat' in file:
            Utils.log(msg=f'Found group_users.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        if '/groups.dat' in file:
            Utils.log(msg=f'Found groups.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        if '/inspections.dat' in file:
            Utils.log(msg=f'Found inspections.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        if '/schedule_assignees.dat' in file:
            Utils.log(msg=f'Found schedule_assignees.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # schedules.dat uploading
        if '/schedules.dat' in file:
            Utils.log(msg=f'Found schedules.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # sites.dat uploading
        if '/sites.dat' in file:
            Utils.log(msg=f'Found sites.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # template_permissions.dat uploading
        if '/template_permissions.dat' in file:
            Utils.log(msg=f'Found template_permissions.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # templates.dat uploading
        if '/templates.dat' in file:
            Utils.log(msg=f'Found templates.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        # users.dat uploading
        if '/users.dat' in file:
            Utils.log(msg=f'Found users.dat: {file}', l_bln_print_to_screen=False,
                      l_bln_log_to_file=True)
            was_uploaded = SnowflakeDataImporter.upload_file(file, snowflake_config)

        if was_uploaded:
            Utils.log(msg=f"Successfully imported {file} to Snowflake.", l_bln_print_to_screen=True, l_bln_log_to_file=True)
        else:
            Utils.log(msg=f"Failed to import {file} to Snowflake.", l_bln_print_to_screen=True,
                      l_bln_log_to_file=True, l_str_log_type=Utils.LOG_TYPE_ERROR)
        return

    """
    Uploads each dat file to Snowflake
    """
    @staticmethod
    def upload_file(file, snowflake_config):
        # Create Polars DataFrame from CSV, convert to Pandas
        file_config = SnowflakeDataImporter.find_file_info(file)
        table_name = file_config['table_name']
        Utils.log(msg=f"File Config: {file_config}", l_bln_print_to_screen=False, l_bln_log_to_file=True)

        date_columns = file_config['date_columns']
        inspections_df = pl.read_csv(file, dtypes=file_config['data_types'], has_header=True, separator='\t')
        pd_inspections_df = inspections_df.to_pandas()

        for col in date_columns:
            pd_inspections_df[col] = pd_inspections_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Utils.log(msg=f"Datatypes for Pandas: {pd_inspections_df.dtypes}", l_bln_print_to_screen=True, l_bln_log_to_file=True)

        # Convert column names to uppercase to match Database
        pd_inspections_df.columns = map(str.upper, pd_inspections_df.columns)

        current_env = SnowflakeDataImporter.get_environment()
        try:
            if current_env == '_dev':
                database = 'DEV_DATABASE'
            elif current_env == '_stage':
                database = 'STAGE_DATABASE'
            elif current_env == '_prod':
                database = 'PROD_DATABASE'
        except Exception as e:
            Utils.log(msg=f"Failed to get database name: {e}", l_bln_print_to_screen=True, l_bln_log_to_file=True, l_str_log_type=Utils.LOG_TYPE_ERROR, l_bln_die=True)


        # Create a SQLAlchemy Engine
        conn = snowflake.connector.connect(
            user=snowflake_config.get(database, 'user'),
            password=snowflake_config.get('database', 'password'),
            account=snowflake_config.get('DATABASE', 'account'),
            database=snowflake_config.get('DATABASE', 'trf_database'),
            schema=snowflake_config.get('DATABASE', 'trf_schema'),
            warehouse=snowflake_config.get('DATABASE', 'warehouse'),
            role=snowflake_config.get('DATABASE', 'role')
        )

        try:
            # If Test Mode is enabled, only upload a few files to database instead of all
            if config.TEST_MODE:
                pd.set_option('display.max_columns', None)
                Utils.log(msg=f"Here are the first three rows: {pd_inspections_df.head(3)}", l_bln_print_to_screen=True, l_bln_log_to_file=True, l_bln_die=False)
                pd_inspections_df = pd_inspections_df.head(3)

            # Write to Snowflake
            if config.SAVE_TO_DATABASE:
                success, nchunks, nrows, _ = write_pandas(conn, pd_inspections_df, table_name)
                Utils.log(msg=f"Success: {success}, Number of chunks: {nchunks}, Number of rows: {nrows}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
        except Exception as e:
            Utils.log(msg=f"Failed to write to table: {e} - Table Name: {table_name}", l_bln_print_to_screen=True, l_bln_log_to_file=True, l_str_log_type=Utils.LOG_TYPE_ERROR, l_bln_die=True)
            return False

        Utils.log(msg=f"Successfully uploaded {file} to Snowflake.", l_bln_print_to_screen=True, l_bln_log_to_file=True, l_bln_die=False)
        return True

    @staticmethod
    def find_file_info(file):
        if '/actions.dat' in file:
            return config.file_config['actions']
        if '/inspection_items.dat' in file:
            return config.file_config['inspection_items']
        if '/action_assignees.dat' in file:
            return config.file_config['action_assignees']
        if '/group_users.dat' in file:
            return config.file_config['group_users']
        if '/groups.dat' in file:
            return config.file_config['groups']
        if '/inspections.dat' in file:
            return config.file_config['inspections']
        if '/schedule_assignees.dat' in file:
            return config.file_config['schedule_assignees']
        if '/schedules.dat' in file:
            return config.file_config['schedules']
        if '/sites.dat' in file:
            return config.file_config['sites']
        if '/template_permissions.dat' in file:
            return config.file_config['template_permissions']
        if '/templates.dat' in file:
            return config.file_config['templates']
        if '/users.dat' in file:
            return config.file_config['users']
        Utils.log(msg=f"Could not find entry for {file}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
        return None

    @staticmethod
    def get_environment():
        dir_path = os.path.dirname(os.path.realpath(__file__))

        if dir_path.find('_dev') != -1:
            return '_dev'
        if dir_path.find('_stage') != -1:
            return '_stage'
        if dir_path.find('_prod') != -1:
            return '_prod'
        else:
            return dir_path
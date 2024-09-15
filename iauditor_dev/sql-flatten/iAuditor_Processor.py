import snowflake.connector
from SnowflakeDataImporter import SnowflakeDataImporter
import configparser
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../tools/export')))
import iAuditor_Exporter_Config as Config


class IAuditorProcessor:

    @staticmethod
    def get_snowflake_connection(snowflake_config):
        current_env = SnowflakeDataImporter.get_environment()
        database = SnowflakeDataImporter.get_database(current_env)

        # Snowflake connection setup
        conn = snowflake.connector.connect(
            user=snowflake_config.get(database, 'user'),
            password=snowflake_config.get(database, 'password'),
            account=snowflake_config.get(database, 'account'),
            database=snowflake_config.get(database, 'raw_database'),
            schema=snowflake_config.get(database, 'raw_schema'),
            warehouse=snowflake_config.get(database, 'warehouse'),
            role=snowflake_config.get(database, 'role')
        )

        return conn

    @staticmethod
    def run():
        snowflake_config = configparser.RawConfigParser()
        snowflake_config.read(Config.snowflake_config)

        # Getting the Snowflake connection
        conn = IAuditorProcessor.get_snowflake_connection(snowflake_config)

        current_count = 1
        success_count = 0
        total_count = len(Config.sql_scripts)
        failure_count = 0

        # Getting the SQL directory
        sql_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../sql-flatten'))

        # Run Each SQL Script
        for sql_script in Config.sql_scripts:
            sql_script_path = os.path.join(sql_directory, sql_script)

            print(f"Attempting to execute SQL script: {sql_script_path} ~ {current_count}/{total_count}")

            try:
                # Reading the SQL script from the file
                with open(sql_script_path, 'r') as file:
                    sql_query = file.read()

                # Executing the SQL script
                with conn.cursor() as cur:
                    cur.execute(sql_query)

                # Logging the success
                print('SQL script executed successfully')
                success_count += 1

            except Exception as e:
                print(f'Error executing the SQL script - Error: {e}')
                failure_count += 1

            current_count += 1

        # Logging the total count of SQL scripts executed
        print(f"Total SQL scripts executed: {total_count}")
        print(f"Total SQL scripts executed successfully: {success_count}")
        print(f"Total SQL scripts failed: {failure_count}")

        # Closing the connection
        conn.close()

if __name__ == "__main__":
    IAuditorProcessor.run()
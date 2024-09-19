# iAuditor_Processor.py
import os
import configparser
from iAuditor_Utils import IAuditorUtils
import iAuditor_Exporter_Config as Config

class IAuditorProcessor:

    @staticmethod
    def run():
        snowflake_config = configparser.RawConfigParser()
        snowflake_config.read(Config.snowflake_config)

        # Getting the Snowflake connection
        conn = IAuditorUtils.get_snowflake_connection(snowflake_config)

        current_count = 1
        success_count = 0
        total_count = len(Config.sql_scripts)
        failure_count = 0

        # Getting the SQL directory
        sql_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../DataTransformation'))

        # Run Each SQL Script
        for sql_script in Config.sql_scripts:
            sql_script_path = os.path.join(sql_directory, sql_script)

            IAuditorUtils.log(f"Attempting to execute SQL script: {sql_script_path} ~ {current_count}/{total_count}",
                level="INFO", log_file=True, print_msg=True)

            try:
                # Reading the SQL script from the file
                with open(sql_script_path, 'r') as file:
                    sql_query = file.read()

                # Executing the SQL script
                with conn.cursor() as cur:
                    cur.execute(sql_query)

                IAuditorUtils.log(f"SQL script executed successfully: {sql_script_path}", level="INFO",
                    log_file=True, print_msg=True)
                success_count += 1

            except Exception as e:
                IAuditorUtils.log(f"Error executing the SQL script: {sql_script_path} - Error: {e}",
                    level="ERROR", log_file=True, print_msg=True)
                failure_count += 1

            current_count += 1

        IAuditorUtils.log(msg=f"Total SQL scripts executed: {total_count} \n"
                f"Total SQL scripts executed successfully: {success_count} \n"
                f"Total SQL scripts failed: {failure_count}", level="INFO", log_file=True, print_msg=True)

        conn.close()

if __name__ == "__main__":
    IAuditorProcessor.run()
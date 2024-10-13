# iAuditor_Processor.py
import os
from iAuditor_Utils import IAuditorUtils
import iAuditor_Exporter_Config as Config

class IAuditorProcessor:

    def __init__(self):
        # Initialize metadata (count variables)
        self.current_count = 1
        self.success_count = 0
        self.total_count = len(Config.sql_scripts_dict)
        self.failure_count = 0
        self.update_current_count = 1
        self.update_success_count = 0
        self.update_failure_count = 0
        self.update_total_count = len(Config.sql_scripts_dict)

        # Initialize Snowflake connection
        self.conn = IAuditorUtils.get_snowflake_connection()

    def run(self):
        # Getting the SQL directory
        sql_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), 'DataTransformation'))

        # Run Each SQL Script
        for table, script in Config.sql_scripts_dict.items():
            sql_script_path = os.path.join(sql_directory, script["flatten"])
            update_script_path = os.path.join(sql_directory, script["update"])

            IAuditorUtils.log(f"Attempting to execute Flatten SQL script: {sql_script_path} ~ {self.current_count}/{self.total_count}",
                level="INFO", log_file=True, print_msg=True)

            try:
                # Reading the SQL script from the file
                with open(sql_script_path, 'r') as file:
                    sql_query = file.read()

                # Executing the SQL script
                with self.conn.cursor() as cur:
                    cur.execute(sql_query)

                IAuditorUtils.log(f"Flatten SQL script executed successfully: {sql_script_path}", level="INFO",
                    log_file=True, print_msg=True)
                self.success_count += 1

                try:                
                    # Reading the Update SQL script from the file
                    with open(update_script_path, 'r') as file:
                        sql_query = file.read()

                    # Executing the SQL script
                    with self.conn.cursor() as cur:
                        cur.execute(sql_query)

                    IAuditorUtils.log(f"Update SQL script executed successfully: {update_script_path}", level="INFO",
                        log_file=True, print_msg=True)
                    self.update_success_count += 1
                except Exception as e:
                    IAuditorUtils.log(f"Error executing the update SQL script: {update_script_path} - Error: {e}",
                        level="ERROR", log_file=True, print_msg=True)
                    self.update_failure_count += 1

            except Exception as e:
                IAuditorUtils.log(f"Flatten Error executing the flatten SQL script: {sql_script_path} - Error: {e}",
                    level="ERROR", log_file=True, print_msg=True)
                self.failure_count += 1

            IAuditorUtils.log(f"Attempting to execute Update SQL script: {sql_script_path} ~ {self.current_count}/{self.total_count}",
                level="INFO", log_file=True, print_msg=True)

            self.current_count += 1
            self.update_current_count += 1

        IAuditorUtils.log(msg=f"Total Flatten SQL scripts executed: {self.total_count} \n"
                f"Total Flatten SQL scripts executed successfully: {self.success_count} \n"
                f"Total Flatten SQL scripts failed: {self.failure_count}", level="INFO", log_file=True, print_msg=True)
        
        IAuditorUtils.log(msg=f"Total Update SQL scripts executed: {self.update_total_count} \n"
                f"Total Update SQL scripts executed successfully: {self.update_success_count} \n"
                f"Total Update SQL scripts failed: {self.update_failure_count}", level="INFO", log_file=True, print_msg=True)    

        # Close the Snowflake connection
        self.conn.close()

        IAuditorUtils.upload_logs_to_snowflake()

if __name__ == "__main__":
    processor = IAuditorProcessor()
    processor.run()
import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from iAuditor_Utils import IAuditorUtils
import iAuditor_Exporter_Config as Config


class IAuditorExporter:

    def __init__(self):
        # Initialize metadata (count variables)
        self.current_count = 1
        self.success_count = 0
        self.total_count = len(Config.urls)
        self.failure_count = 0

        # Generate UUID
        IAuditorUtils.generate_uuid()

        # Initialize Snowflake connection
        #if Config.upload_snowflake:
        #    self.conn = IAuditorUtils.get_snowflake_connection(location='RAW')

    def run(self, conn):
        for key in Config.urls:
            # Create Empty DataFrame
            endpoint_df = pd.DataFrame(columns=['DATA_DATE', 'JSON_PAYLOAD', 'RECORD_INSERTED_AT', 'PROCESSED'])
                        
            # Change the .read_last_call_time() to read from Snowflake tables
            data_date = IAuditorUtils.read_last_call_time()
            endpoint = Config.urls[key]['endpoint']
            url = Config.urls[key]['endpoint']

            # Delete Later! (10/2) Updating to read max modified in historical date
            if Config.historical_last_call:
                data_date = '2024-02-01T00:00:00Z'

            if endpoint not in Config.keep_url:
                url = Config.urls[key]['endpoint'] + '?modified_after=' + data_date

            for n in range(1, 80):
                json_payload = IAuditorUtils.call_api(base_url=url)
                metadata = json_payload['metadata']
                remaining_records = metadata['remaining_records']
                next_page = metadata['next_page']

                raw_table_name = Config.table_prefix + key.upper()

                IAuditorUtils.log(msg=f"Current Table: {raw_table_name} Attempting to Upload API call ~ "
                    f"{self.current_count}/{self.total_count}", level="INFO", log_file=True, print_msg=True)


                date_executed = Config.current_time

                data_date = Config.current_time

                # Create DataFrame directly with pandas
                new_entry = pd.DataFrame({
                    "DATA_DATE": [data_date],
                    "JSON_PAYLOAD": [json_payload],
                    "RECORD_INSERTED_AT": [date_executed],
                    "PROCESSED": ['f']
                })

                # Append the new log entry to the logs_df DataFrame using pd.concat
                endpoint_df = pd.concat([endpoint_df, new_entry], ignore_index=True)

                if next_page is not None:
                    url = Config.base_api_url + next_page

                if remaining_records == 0:
                    break

            # upload to Snowflake if enabled
            if Config.upload_snowflake:
                # Write DataFrame to Snowflake
                IAuditorUtils.log(msg=f"Writing DataFrame to Snowflake for {key}", level="INFO", log_file=True,
                    print_msg=True)
                IAuditorUtils.log(msg=f"Attempting to write to {raw_table_name}", level="INFO", log_file=True,
                    print_msg=True)
                try:
                    success = write_pandas(conn=conn, df=endpoint_df, table=raw_table_name, schema=os.environ("RAW_SCHEMA"), database=os.environ("RAW_DATABASE"))
                    IAuditorUtils.log(msg=f"Successfully uploaded dataframe to table: {raw_table_name}", level="INFO",
                        log_file=True, print_msg=True)
                    self.success_count += 1

                except Exception as e:
                    IAuditorUtils.log(msg=f"Unable to upload to table: {raw_table_name} - due to: {e}", level="ERROR",
                        log_file=True, print_msg=True)
                    self.failure_count += 1

            self.current_count += 1

        # Calculate and display the success percentage
        success_percentage = (self.success_count / self.total_count) * 100
        failure_percentage = (self.failure_count / self.total_count) * 100

        IAuditorUtils.log(msg=f"Success percentage: {success_percentage:.2f}% \n"
            f"Failure percentage: {failure_percentage:.2f}%", level="INFO",
            log_file=True, print_msg=True)

        # Close the connection
        if self.conn:
            self.conn.close()

        # Upload logs to Snowflake
        if Config.upload_logs:
            IAuditorUtils.upload_logs_to_snowflake()

if __name__ == "__main__":
    exporter = IAuditorExporter()
    exporter.run()
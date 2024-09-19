# iAuditor_Exporter.py
import os
import configparser
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from iAuditor_Utils import IAuditorUtils
import iAuditor_Exporter_Config as Config

class IAuditorExporter:
    # UUID

    @staticmethod
    def upload_logs_to_snowflake(snowflake_config):
        logs_table_name = Config.logs_table_name
        conn = IAuditorUtils.get_snowflake_connection(snowflake_config, location='LOG')
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
    def run():
        # Start ~ Initialize the configuration/metadata
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../include/config'))
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

        IAuditorUtils.generate_uuid()
        # End ~ Initialize the configuration/metadata

        for key in Config.urls:
            # Call API
            url = Config.urls[key]
            json_payload = IAuditorUtils.call_api(base_url=url, api_config=api_config)
            raw_table_name = Config.table_prefix + key.upper()
            data_date = IAuditorUtils.unquote(IAuditorUtils.read_last_call_time(api_config))

            IAuditorUtils.log(msg=f"Current Table: {raw_table_name} Attempting to Upload API call ~ "
                f"{current_count}/{total_count}", level="INFO", log_file=True, print_msg=True)

            conn = IAuditorUtils.get_snowflake_connection(snowflake_config)

            date_executed = Config.current_time

            # Create DataFrame directly with pandas
            pd_df = pd.DataFrame({
                "DATA_DATE": [data_date],
                "JSON_PAYLOAD": [json_payload],
                "RECORD_INSERTED_AT": [date_executed]
            })

            # Write DataFrame to Snowflake
            IAuditorUtils.log(msg=f"Writing DataFrame to Snowflake for {key}", level="INFO", log_file=True,
                print_msg=True)
            try:
                success = write_pandas(conn, pd_df, raw_table_name)
                IAuditorUtils.log(msg=f"Successfully uploaded dataframe to table: {raw_table_name}", level="INFO",
                    log_file=True, print_msg=True)
                success_count += 1

            except Exception as e:
                IAuditorUtils.log(msg=f"Unable to upload to table: {raw_table_name} - due to: {e}", level="ERROR",
                    log_file=True, print_msg=True)
                failure_count += 1

            current_count += 1

        # Calculate and display the success percentage
        success_percentage = (success_count / total_count) * 100
        failure_percentage = (failure_count / total_count) * 100

        IAuditorUtils.log(msg=f"Success percentage: {success_percentage:.2f}% \n"
            f"Failure percentage: {failure_percentage:.2f}%", level="INFO",
            log_file=True, print_msg=True)

        # Close the connection
        if conn is not None:
            conn.close()

        # Upload logs to Snowflake
        IAuditorExporter.upload_logs_to_snowflake(snowflake_config)

if __name__ == "__main__":
    IAuditorExporter.run()
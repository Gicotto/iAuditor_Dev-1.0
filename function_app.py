from datetime import datetime
import time
import azure.functions as func
import logging

from iAuditor_Exporter import IAuditorExporter
from iAuditor_Processor import IAuditorProcessor
from iAuditor_Utils import IAuditorUtils
import iAuditor_Exporter_Config as Config


app = func.FunctionApp()

@app.route(route="loader_trigger", auth_level=func.AuthLevel.FUNCTION)
def loader_trigger(req: func.HttpRequest) -> func.HttpResponse:
    IAuditorUtils.log(msg='Python HTTP trigger function processed a request.', level='INFO', log_file=True, print_msg=True)
    start_time = time.time()

    conn = IAuditorUtils.get_snowflake_connection()

    if Config.run_exporter:
        try:
            IAuditorUtils.log("Running IAuditorExporter")
            exporter = IAuditorExporter()
            exporter.run(conn=conn)

        except Exception as e:
            logging.error(f"Error: {e}")
            duration_mins = IAuditorUtils.get_duration(start_time,with_label=True)
            IAuditorUtils.log(f"Duration: {duration_mins}"
                    f"Error: {e}", level="ERROR", log_file=True, print_msg=True)
            
            return func.HttpResponse(f"Error: {e}", status_code=500)


    if Config.run_processor:
        try:
            IAuditorUtils.log("Running IAuditorProcessor")
            processor = IAuditorProcessor(conn=conn)
            processor.run()

        except Exception as e:

            logging.error(f"Error: {e}")
            duration_mins = IAuditorUtils.get_duration(start_time,with_label=True)
            IAuditorUtils.log(f"Duration: {duration_mins}"
                            f"Error: {e}", level="ERROR", log_file=True, print_msg=True)
            
            return func.HttpResponse(f"Error: {e}", status_code=500)
    
    duration_mins = IAuditorUtils.get_duration(start_time,with_label=True)
    IAuditorUtils.log(f"Duration: {duration_mins}", level="INFO", log_file=True, print_msg=True)
    IAuditorUtils.upload_logs_to_snowflake()

    return func.HttpResponse("Success", status_code=200)

#!/usr/bin/env python
import os
import sys


# Import the necessary module assuming 'MainProcessor' is similarly structured in Python
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'include'))
from iAuditor_Processor import IAuditorProcessor

"""
Cron script that runs IAuditor Processor which flattens the data and loads it into the snowflake tables.
IAuditorExporter is called in a separate cron job. Logs sent to Snowflake batch audit table.
"""
# Main processing function
if __name__ == "__main__":
    IAuditorProcessor.run()

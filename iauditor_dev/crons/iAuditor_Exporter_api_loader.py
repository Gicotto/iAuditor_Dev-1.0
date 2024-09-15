#!/usr/bin/env python
import os
import sys


# Import the necessary module assuming 'MainProcessor' is similarly structured in Python
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tools', 'export'))
# noinspection PyUnresolvedReferences
from iAuditor_Exporter import IAuditorExporter

"""
Cron script that runs IAuditorExporter which makes API calls to iAuditor and exports the data to snowflake
raw tables as a json package. The data is then processed by IAuditorProcessor to flatten the data and load it into
the snowflake tables. IAuditorProcessor is called in a separate cron job.
"""
# Main processing function
if __name__ == "__main__":
    IAuditorExporter.run()

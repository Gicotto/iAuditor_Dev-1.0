#!/usr/bin/env python
import os
import sys

# Import the necessary module assuming 'MainProcessor' is similarly structured in Python
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'code', 'include'))
import MainProcessor

"""
Cron script that runs to process iAuditor files.
Files are imported from iAuditor using the iauditor-exporter located at tools/iauditor-exporter
Files are copied to "files" directory for each table to be processed.
Files are parsed and moved to files/processing with a .dat extension.
Each reformatted file is then moved to files/processing/ready to indicate that they're ready to be loaded into the database.
When ready files are loaded into the database, they're moved to files/processed/details.

To set this cron to run in a test database, set TEST_MODE to true in ../code/include/config.py
"""
# Main processing function
if __name__ == "__main__":
    MainProcessor.MainProcessor.run()
